use std::collections::HashMap;
use std::mem;
use std::str;

use num::FromPrimitive;
use redis::{FromRedisValue, Value};

use crate::{server_type_error, Graph, RedisGraphError, RedisGraphResult};

/// Implemented by types that can be contructed from a
/// Redis [`Value`](https://docs.rs/redis/0.15.1/redis/enum.Value.html) and a [`Graph`](../graph/struct.Graph.html)
pub trait FromRedisValueWithGraph: Sized {
    fn from_redis_value_with_graph(value: Value, graph: &Graph) -> RedisGraphResult<Self>;
}

impl<T: FromRedisValue> FromRedisValueWithGraph for T {
    fn from_redis_value_with_graph(value: Value, _graph: &Graph) -> RedisGraphResult<T> {
        T::from_redis_value(&value).map_err(RedisGraphError::from)
    }
}

/// A result set returned by RedisGraph in response to a query.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSet {
    /// The columns of this result set.
    ///
    /// Empty if the response did not contain any return values.
    pub columns: Vec<Column>,
    /// Contains statistics messages from the response.
    pub statistics: Statistics,
}

/// Statistics returned by RedisGraph about a query as a list of messages.
#[derive(Debug, Clone, PartialEq)]
pub struct Statistics(pub Vec<String>);

impl ResultSet {
    /// Returns the number of rows in the result set.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns the number of columns in the result set.
    pub fn num_rows(&self) -> usize {
        match self.columns.get(0) {
            Some(first_column) => first_column.len(),
            None => 0,
        }
    }

    /// Returns the scalar at the given position.
    ///
    /// Returns an error if the value at the given position is not a scalar
    /// or if the position is out of bounds.
    pub fn get_scalar(&self, row_idx: usize, column_idx: usize) -> RedisGraphResult<&Scalar> {
        match self.columns.get(column_idx) {
            Some(column) => match column {
                Column::Scalars(cells) => match cells.get(row_idx) {
                    Some(cell) => Ok(cell),
                    None => client_type_error!(
                        "failed to get scalar: row index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
                    ),
                },
                any => client_type_error!(
                    "failed to get scalar: expected column of scalars, found {:?}",
                    any
                ),
            }
            None => client_type_error!(
                "failed to get scalar: column index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
            ),
        }
    }

    /// Returns the node at the given position.
    ///
    /// Returns an error if the value at the given position is not a node
    /// or if the position is out of bounds.
    pub fn get_node(&self, row_idx: usize, column_idx: usize) -> RedisGraphResult<&Node> {
        match self.columns.get(column_idx) {
            Some(column) => match column {
                Column::Nodes(cells) => match cells.get(row_idx) {
                    Some(cell) => Ok(cell),
                    None => client_type_error!(
                        "failed to get node: row index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
                    ),
                },
                Column::Scalars(cells) => match cells.get(row_idx) {
                    Some(cell) => match cell {
                        Scalar::Node(node) => Ok(node),
                            _ => client_type_error!(
                            "failed to get node: tried to get node in scalar column, but was actually {:?}", cell,
                        ),
                    },
                    None => client_type_error!(
                        "failed to get node: row index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
                    ),
                },
                any => client_type_error!(
                    "failed to get node: expected column of nodes, found {:?}",
                    any
                ),
            }
            None => client_type_error!(
                "failed to get node: column index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
            ),
        }
    }

    /// Returns the relation at the given position.
    ///
    /// Returns an error if the value at the given position is not a relation
    /// or if the position is out of bounds.
    pub fn get_relation(&self, row_idx: usize, column_idx: usize) -> RedisGraphResult<&Relation> {
        match self.columns.get(column_idx) {
            Some(column) => match column {
                Column::Relations(cells) => match cells.get(row_idx) {
                    Some(cell) => Ok(cell),
                    None => client_type_error!(
                        "failed to get relation: row index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
                    ),
                },
                Column::Scalars(cells) => match cells.get(row_idx) {
                    Some(cell) => match cell {
                        Scalar::Edge(relation) => Ok(relation),
                        _ => client_type_error!(
                            "failed to get relation: tried to get edge in scalar column, but was actually {:?}", cell,
                        ),
                    },
                    None => client_type_error!(
                        "failed to get relation: row index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
                    ),
                },
                any => client_type_error!(
                    "failed to get relation: expected column of relations, found {:?}",
                    any
                ),
            },
            None => client_type_error!(
                "failed to get relation: column index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
            ),
        }
    }

    /// Returns the path at the given position.
    ///
    /// Returns an error if the value at the given position is not a path
    /// or if the position is out of bounds.
    pub fn get_path(&self, row_idx: usize, column_idx: usize) -> RedisGraphResult<&Path> {
        match self.columns.get(column_idx) {
            Some(column) => match column {
                Column::Scalars(cells) => match cells.get(row_idx) {
                    Some(cell) => match cell {
                        Scalar::Path(path) => Ok(path),
                        _ => client_type_error!(
                            "failed to get path: tried to get path in scalar column, but was actually {:?}", cell,
                        ),
                    },
                    None => client_type_error!(
                        "failed to get path: row index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
                    ),
                },
                any => client_type_error!(
                    "failed to get path: expected column of relations, found {:?}",
                    any
                ),
            },
            None => client_type_error!(
                "failed to get path: column index out of bounds: the len is {:?} but the index is {:?}", self.columns.len(), column_idx,
            ),
        }
    }
}

/// A single column of the result set.
#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    Scalars(Vec<Scalar>),
    Nodes(Vec<Node>),
    Relations(Vec<Relation>),
}

impl Column {
    /// Returns the lenghth of this column.
    pub fn len(&self) -> usize {
        match self {
            Self::Scalars(cells) => cells.len(),
            Self::Nodes(cells) => cells.len(),
            Self::Relations(cells) => cells.len(),
        }
    }

    /// Returns `true` if this column is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(num_derive::FromPrimitive)]
enum ColumnType {
    Unknown = 0,
    Scalar = 1,
    Node = 2,
    Relation = 3,
}

impl FromRedisValueWithGraph for ResultSet {
    fn from_redis_value_with_graph(value: Value, graph: &Graph) -> RedisGraphResult<Self> {
        match value {
            Value::Bulk(mut values) => {
                match values.len() {
                    3 => {
                        let header_row = values[0].take();
                        let result_rows = values[1].take();
                        let statistics = values[2].take();

                        match header_row {
                            Value::Bulk(header_row) => {
                                let column_count = header_row.len();
                                let mut columns = Vec::<Column>::with_capacity(column_count);

                                // `result_table[0][1]` is row 0, column 1
                                let mut result_table: Vec<Vec<Value>> = match result_rows {
                                    Value::Bulk(rows) => rows
                                        .into_iter()
                                        .map(|row| match row {
                                            Value::Bulk(row) => Ok(row),
                                            _ => server_type_error!(
                                                "expected array as result row representation",
                                            ),
                                        })
                                        .collect::<RedisGraphResult<Vec<Vec<Value>>>>(),
                                    _ => server_type_error!(
                                        "expected array as result table representation",
                                    ),
                                }?;

                                for i in 0..column_count {
                                    match &header_row[i] {
                                        Value::Bulk(header_cell) => {
                                            let column_type_i64 = match header_cell[0] {
                                                Value::Int(column_type_i64) => column_type_i64,
                                                _ => {
                                                    return server_type_error!(
                                                        "expected integer as column type",
                                                    )
                                                }
                                            };

                                            let column = match ColumnType::from_i64(column_type_i64) {
                                                Some(ColumnType::Unknown) => server_type_error!("column type is unknown"),
                                                Some(ColumnType::Scalar) => Ok(Column::Scalars(
                                                    result_table
                                                        .iter_mut()
                                                        .map(|row| {
                                                            Scalar::from_redis_value_with_graph(row[i].take(), graph)
                                                                .map_err(RedisGraphError::from)
                                                        })
                                                        .collect::<RedisGraphResult<Vec<Scalar>>>()?,
                                                )),
                                                Some(ColumnType::Node) => Ok(Column::Nodes(
                                                    result_table
                                                        .iter_mut()
                                                        .map(|row| {
                                                            Node::from_redis_value_with_graph(row[i].take(), graph)
                                                                .map_err(RedisGraphError::from)
                                                        })
                                                        .collect::<RedisGraphResult<Vec<Node>>>()?,
                                                )),
                                                Some(ColumnType::Relation) => Ok(Column::Relations(
                                                    result_table
                                                        .iter_mut()
                                                        .map(|row| {
                                                            Relation::from_redis_value_with_graph(row[i].take(), graph)
                                                                .map_err(RedisGraphError::from)
                                                        })
                                                        .collect::<RedisGraphResult<Vec<Relation>>>()?,
                                                )),
                                                None => server_type_error!("expected integer between 0 and 3 as column type")
                                            }?;

                                            columns.push(column);
                                        }
                                        _ => {
                                            return server_type_error!(
                                                "expected array as header cell representation",
                                            )
                                        }
                                    }
                                }

                                if let Some(first_column) = columns.get(0) {
                                    if !columns
                                        .iter()
                                        .all(|column| column.len() == first_column.len())
                                    {
                                        return server_type_error!(
                                            "result columns have unequal lengths",
                                        );
                                    }
                                }

                                let statistics = parse_statistics(statistics)?;

                                Ok(Self {
                                    columns,
                                    statistics,
                                })
                            }
                            _ => server_type_error!("expected array as header row representation",),
                        }
                    }
                    1 => {
                        let statistics = parse_statistics(values[0].take())?;

                        Ok(Self {
                            columns: Vec::new(),
                            statistics,
                        })
                    }
                    _ => server_type_error!(
                        "expected array of size 3 or 1 as result set representation",
                    ),
                }
            }
            _ => server_type_error!("expected array as result set representation"),
        }
    }
}

fn parse_statistics(value: Value) -> RedisGraphResult<Statistics> {
    match value {
        Value::Bulk(statistics) => statistics
            .into_iter()
            .map(|entry| match entry {
                Value::Data(utf8) => {
                    String::from_utf8(utf8).map_err(|_| RedisGraphError::InvalidUtf8)
                }
                _ => server_type_error!("expected string as statistics entry"),
            })
            .collect::<RedisGraphResult<Vec<String>>>()
            .map(Statistics),
        _ => server_type_error!("expected array as statistics list"),
    }
}

/// A scalar value returned by RedisGraph.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Scalar {
    Nil,
    Boolean(bool),
    Integer(i64),
    Double(f64),
    String(RedisString),
    Array(Vec<Scalar>),
    Edge(Relation),
    Node(Node),
    Path(Path),
}

/// Implemented for Redis types with a nil-like variant.
pub trait Take {
    /// Takes the value, leaving the "nil" variant in its place.
    fn take(&mut self) -> Self;
}

impl Take for Value {
    fn take(&mut self) -> Self {
        mem::replace(self, Self::Nil)
    }
}

impl Take for Scalar {
    fn take(&mut self) -> Self {
        mem::replace(self, Self::Nil)
    }
}

/// A string returned by Redis.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RedisString(pub Vec<u8>);

impl From<String> for RedisString {
    fn from(string: String) -> Self {
        Self(string.into_bytes())
    }
}

impl From<Vec<u8>> for RedisString {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<RedisString> for Vec<u8> {
    fn from(redis_string: RedisString) -> Self {
        redis_string.0
    }
}

#[derive(num_derive::FromPrimitive)]
enum ScalarType {
    Unknown = 0,
    Nil = 1,
    String = 2,
    Integer = 3,
    Boolean = 4,
    Double = 5,
    Array = 6,
    Edge = 7,
    Node = 8,
    Path = 9,
}

impl FromRedisValueWithGraph for Scalar {
    fn from_redis_value_with_graph(value: Value, graph: &Graph) -> RedisGraphResult<Self> {
        match value {
            Value::Bulk(mut values) => {
                if values.len() == 2 {
                    let scalar_type = values[0].take();
                    let scalar_value = values[1].take();
                    match scalar_type {
                        Value::Int(scalar_type_int) => match ScalarType::from_i64(scalar_type_int) {
                            Some(ScalarType::Unknown) => server_type_error!("scalar type is unknown"),
                            Some(ScalarType::Nil) => Ok(Scalar::Nil),
                            Some(ScalarType::String) => match scalar_value {
                                Value::Data(string_data) => Ok(Scalar::String(RedisString(string_data))),
                                _ => server_type_error!("expected binary data as scalar value (scalar type is string)")
                            },
                            Some(ScalarType::Integer) => match scalar_value {
                                Value::Int(integer) => Ok(Scalar::Integer(integer)),
                                _ => server_type_error!("expected integer as scalar value (scalar type is integer)")
                            },
                            Some(ScalarType::Boolean) => match scalar_value {
                                Value::Data(bool_data) => match &bool_data[..] {
                                    b"true" => Ok(Scalar::Boolean(true)),
                                    b"false" => Ok(Scalar::Boolean(false)),
                                    _ => server_type_error!("expected either \"true\" or \"false\" as scalar value (scalar type is boolean)")
                                }
                                _ => server_type_error!("expected binary data as scalar value (scalar type is boolean)")
                            },
                            Some(ScalarType::Double) => match scalar_value {
                                Value::Data(double_data) => match str::from_utf8(&double_data[..]) {
                                    Ok(double_string) => match double_string.parse::<f64>() {
                                        Ok(double) => Ok(Scalar::Double(double)),
                                        Err(_) => server_type_error!("expected string representation of double as scalar value (scalar type is double)")
                                    },
                                    Err(_) => Err(RedisGraphError::InvalidUtf8),
                                }
                                _ => server_type_error!("expected string representing a double as scalar value (scalar type is double)")
                            },
                            Some(ScalarType::Array) => match scalar_value {
                                Value::Bulk(elements) => {
                                    let mut values = Vec::new();
                                    for elem in elements {
                                        match Self::from_redis_value_with_graph(elem, graph) {
                                            Ok(val) => values.push(val),
                                            Err(e) => return Err(e),
                                        }
                                    }
                                    Ok(Scalar::Array(values))
                                },
                                _ => server_type_error!("expected something for array")
                            },
                            Some(ScalarType::Node) => match Node::from_redis_value_with_graph(scalar_value, graph) {
                                Ok(node) => Ok(Scalar::Node(node)),
                                Err(e) => Err(e),
                            },
                            Some(ScalarType::Edge) => match Relation::from_redis_value_with_graph(scalar_value, graph) {
                                Ok(relation) => Ok(Scalar::Edge(relation)),
                                Err(e) => Err(e),
                            },
                            Some(ScalarType::Path) => match Path::from_redis_value_with_graph(scalar_value, graph) {
                                Ok(path) => Ok(Scalar::Path(path)),
                                Err(e) => Err(e),
                            },
                            None => server_type_error!("expected integer between 0 and 9 (scalar type) as first element of scalar array, got {}", scalar_type_int)
                        },
                        _ => server_type_error!("expected integer representing scalar type as first element of scalar array")
                    }
                } else {
                    server_type_error!("expected array of size 2 as scalar representation")
                }
            }
            _ => server_type_error!("expected array as scalar representation"),
        }
    }
}

/// A node returned by RedisGraph.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The labels attached to this node.
    pub labels: Vec<RedisString>,
    /// The properties of this node.
    pub properties: HashMap<RedisString, Scalar>,
}

impl FromRedisValueWithGraph for Node {
    fn from_redis_value_with_graph(value: Value, graph: &Graph) -> RedisGraphResult<Self> {
        match value {
            Value::Bulk(mut values) => {
                if values.len() == 3 {
                    let label_ids = values[1].take();
                    let properties = values[2].take();

                    let graph_labels = graph.labels();
                    let labels = match label_ids {
                        Value::Bulk(label_ids) => label_ids
                            .iter()
                            .map(|label_id| {
                                let label_id = match label_id {
                                    Value::Int(id) => id,
                                    _ => return server_type_error!("expected integer as label ID",),
                                };

                                graph_labels
                                    .get(*label_id as usize)
                                    .cloned()
                                    .ok_or(RedisGraphError::LabelNotFound)
                            })
                            .collect::<RedisGraphResult<Vec<RedisString>>>()?,
                        _ => return server_type_error!("expected array as label IDs"),
                    };

                    let properties = parse_properties(graph, properties)?;

                    Ok(Self { labels, properties })
                } else {
                    server_type_error!("expected array of size 3 as node representation")
                }
            }
            _ => server_type_error!("expected array as node representation"),
        }
    }
}

/// A relation returned by RedisGraph.
#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    /// The type name of this relation.
    pub type_name: RedisString,
    /// The properties of this relation.
    pub properties: HashMap<RedisString, Scalar>,
}

impl FromRedisValueWithGraph for Relation {
    fn from_redis_value_with_graph(value: Value, graph: &Graph) -> RedisGraphResult<Self> {
        match value {
            Value::Bulk(mut values) => {
                if values.len() == 5 {
                    let type_id = values[1].take();
                    let properties = values[4].take();

                    let type_name = match type_id {
                        Value::Int(id) => graph
                            .relationship_types()
                            .get(id as usize)
                            .cloned()
                            .ok_or(RedisGraphError::RelationshipTypeNotFound)?,
                        _ => return server_type_error!("expected integer as relationship type ID",),
                    };

                    let properties = parse_properties(graph, properties)?;

                    Ok(Self {
                        type_name,
                        properties,
                    })
                } else {
                    server_type_error!("expected array of size 5 as relationship representation",)
                }
            }
            _ => server_type_error!("expected array as relationship representation"),
        }
    }
}

/// A path returned by RedisGraph.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    /// Nodes in the path.
    pub nodes: Vec<Node>,
    /// Edges in the path.
    pub edges: Vec<Relation>,
}

impl FromRedisValueWithGraph for Path {
    fn from_redis_value_with_graph(value: Value, graph: &Graph) -> RedisGraphResult<Self> {
        match value {
            Value::Bulk(mut values) => {
                if values.len() == 2 {
                    let nodes = values[0].take();
                    let edges = values[1].take();

                    let nodes = match Scalar::from_redis_value_with_graph(nodes, graph)? {
                        Scalar::Array(nodes) => nodes
                            .into_iter()
                            .map(|scalar| match scalar {
                                Scalar::Node(node) => Ok(node),
                                other => server_type_error!(
                                    "unexpected non-node in path nodes array, {:?}",
                                    other
                                ),
                            })
                            .collect::<RedisGraphResult<Vec<Node>>>(),
                        other => server_type_error!(
                            "expected path nodes to be an array, not {:?}",
                            other
                        ),
                    }?;

                    let edges = match Scalar::from_redis_value_with_graph(edges, graph)? {
                        Scalar::Array(edges) => edges
                            .into_iter()
                            .map(|scalar| match scalar {
                                Scalar::Edge(edge) => Ok(edge),
                                other => server_type_error!(
                                    "unexpected non-edge in path edges array, {:?}",
                                    other
                                ),
                            })
                            .collect::<RedisGraphResult<Vec<Relation>>>(),
                        other => server_type_error!(
                            "expected path nodes to be an array, not {:?}",
                            other
                        ),
                    }?;

                    Ok(Self { nodes, edges })
                } else {
                    server_type_error!("expected array of size 2 as relationship representation")
                }
            }
            _ => server_type_error!("expected array as relationship representation"),
        }
    }
}

fn parse_properties(
    graph: &Graph,
    properties: Value,
) -> RedisGraphResult<HashMap<RedisString, Scalar>> {
    let graph_property_keys = graph.property_keys();
    match properties {
        Value::Bulk(properties) => properties
            .into_iter()
            .map(|property| match property {
                Value::Bulk(mut property) => {
                    if property.len() == 3 {
                        let property_key_id = property[0].take();
                        let property_type = property[1].take();
                        let property_value = property[2].take();

                        let property_key = match property_key_id {
                            Value::Int(id) => graph_property_keys
                                .get(id as usize)
                                .cloned()
                                .ok_or(RedisGraphError::PropertyKeyNotFound)?,
                            _ => return server_type_error!("expected integer as property key ID",),
                        };

                        let property_value = Scalar::from_redis_value_with_graph(
                            Value::Bulk(vec![property_type, property_value]),
                            graph,
                        )?;

                        Ok((property_key, property_value))
                    } else {
                        server_type_error!("expected array of size 3 as properties representation",)
                    }
                }
                _ => server_type_error!("expected array as properties representation"),
            })
            .collect::<RedisGraphResult<HashMap<RedisString, Scalar>>>(),
        _ => server_type_error!("expected array as properties representation"),
    }
}
