mod common;

use redisgraph::Graph;
use serial_test::serial;

use common::*;

#[test]
#[serial]
fn test_open_delete() {
    let mut conn = get_connection();

    let graph = Graph::open(&mut conn, "test_open_delete_graph").unwrap();
    graph.delete().unwrap();
}
