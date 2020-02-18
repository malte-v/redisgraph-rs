use redis::{Connection, Value};

use crate::{
    assignments::FromTable,
    result_set::{Column, FromRedisValueWithGraph, Scalar, Statistics, Take},
    server_type_error, RedisGraphError, RedisGraphResult, RedisString, ResultSet,
};

/// Represents a single graph in the database.
pub struct Graph<'c, 'n> {
    conn: &'c mut Connection,
    name: &'n str,

    labels: Vec<RedisString>,
    relationship_types: Vec<RedisString>,
    property_keys: Vec<RedisString>,
}

impl<'c, 'n> Graph<'c, 'n> {
    /// Opens the graph with the given name from the database.
    ///
    /// If the graph does not already exist, creates a new graph with the given name.
    pub fn open(conn: &'c mut Connection, name: &'n str) -> RedisGraphResult<Self> {
        let mut graph = Self {
            conn,
            name,
            labels: Vec::new(),
            relationship_types: Vec::new(),
            property_keys: Vec::new(),
        };

        // Create a dummy node and delete it again.
        // This ensures that an empty graph is created and `delete()`
        // will succeed if the graph did not already exist.
        graph.mutate("CREATE (dummy:__DUMMY_LABEL__)")?;
        graph.mutate("MATCH (dummy:__DUMMY_LABEL__) DELETE dummy")?;

        Ok(graph)
    }

    /// Executes the given query and returns its return values.
    ///
    /// Only use this for queries with a `RETURN` statement.
    pub fn query<T: FromTable>(&mut self, query: &str) -> RedisGraphResult<T> {
        self.query_with_statistics(query).map(|(value, _)| value)
    }

    /// Same as [`query`](#method.query), but also returns statistics about the query along with its return values.
    pub fn query_with_statistics<T: FromTable>(
        &mut self,
        query: &str,
    ) -> RedisGraphResult<(T, Statistics)> {
        let response: Value = self.request(query)?;
        let result_set = self.get_result_set(response)?;
        let value = T::from_table(&result_set)?;
        Ok((value, result_set.statistics))
    }

    /// Executes the given query while not returning any values.
    ///
    /// If you want to mutate the graph and retrieve values from it
    /// using one query, use [`query`](#method.query) instead.
    pub fn mutate(&mut self, query: &str) -> RedisGraphResult<()> {
        self.mutate_with_statistics(query).map(|_| ())
    }

    /// Same as [`mutate`](#method.mutate), but returns statistics about the query.
    pub fn mutate_with_statistics(&mut self, query: &str) -> RedisGraphResult<Statistics> {
        let response: Value = self.request(query)?;
        let result_set = self.get_result_set(response)?;
        Ok(result_set.statistics)
    }

    /// Deletes the entire graph from the database.
    ///
    /// *This action is not easily reversible.*
    pub fn delete(self) -> RedisGraphResult<()> {
        redis::cmd("GRAPH.DELETE")
            .arg(self.name())
            .query::<()>(self.conn)
            .map_err(RedisGraphError::from)
    }

    /// Updates the internal label names by retrieving them from the database.
    ///
    /// There is no real need to call this function manually. This implementation
    /// updates the label names automatically when they become outdated.
    pub fn update_labels(&mut self) -> RedisGraphResult<()> {
        let refresh_response = self.request("CALL db.labels()")?;
        self.labels = self.get_mapping(refresh_response)?;
        Ok(())
    }

    /// Updates the internal relationship type names by retrieving them from the database.
    ///
    /// There is no real need to call this function manually. This implementation
    /// updates the relationship type names automatically when they become outdated.
    pub fn update_relationship_types(&mut self) -> RedisGraphResult<()> {
        let refresh_response = self.request("CALL db.relationshipTypes()")?;
        self.relationship_types = self.get_mapping(refresh_response)?;
        Ok(())
    }

    /// Updates the internal property key names by retrieving them from the database.
    ///
    /// There is no real need to call this function manually. This implementation
    /// updates the property key names automatically when they become outdated.
    pub fn update_property_keys(&mut self) -> RedisGraphResult<()> {
        let refresh_response = self.request("CALL db.propertyKeys()")?;
        self.property_keys = self.get_mapping(refresh_response)?;
        Ok(())
    }

    /// Returns the name of this graph.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the graph's internal label names.
    pub fn labels(&self) -> &[RedisString] {
        &self.labels[..]
    }

    /// Returns the graph's internal relationship type names.
    pub fn relationship_types(&self) -> &[RedisString] {
        &self.relationship_types[..]
    }

    /// Returns the graph's internal property key names.
    pub fn property_keys(&self) -> &[RedisString] {
        &self.property_keys[..]
    }

    fn request(&mut self, query: &str) -> RedisGraphResult<Value> {
        redis::cmd("GRAPH.QUERY")
            .arg(self.name())
            .arg(query)
            .arg("--compact")
            .query(self.conn)
            .map_err(RedisGraphError::from)
    }

    fn get_result_set(&mut self, response: Value) -> RedisGraphResult<ResultSet> {
        match ResultSet::from_redis_value_with_graph(response.clone(), self) {
            Ok(result_set) => Ok(result_set),
            Err(RedisGraphError::LabelNotFound) => {
                self.update_labels()?;
                self.get_result_set(response)
            }
            Err(RedisGraphError::RelationshipTypeNotFound) => {
                self.update_relationship_types()?;
                self.get_result_set(response)
            }
            Err(RedisGraphError::PropertyKeyNotFound) => {
                self.update_property_keys()?;
                self.get_result_set(response)
            }
            any_err => any_err,
        }
    }

    fn get_mapping(&self, response: Value) -> RedisGraphResult<Vec<RedisString>> {
        let mut result_set = ResultSet::from_redis_value_with_graph(response, self)?;
        match &mut result_set.columns[0] {
            Column::Scalars(scalars) => scalars
                .iter_mut()
                .map(|scalar| match scalar.take() {
                    Scalar::String(string) => Ok(string),
                    _ => server_type_error!("expected strings in first column of result set"),
                })
                .collect::<RedisGraphResult<Vec<RedisString>>>(),
            _ => server_type_error!("expected scalars as first column in result set"),
        }
    }
}
