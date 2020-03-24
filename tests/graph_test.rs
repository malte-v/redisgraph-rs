mod common;

use redisgraph::Graph;
use serial_test::serial;

use common::*;

#[test]
#[serial]
fn test_open_delete() {
    let conn = get_connection();

    let graph = Graph::open(conn, "test_open_delete_graph".to_string()).unwrap();
    graph.delete().unwrap();
}
