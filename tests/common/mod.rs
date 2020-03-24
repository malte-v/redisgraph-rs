use redis::{Client, Connection};
use redisgraph::graph::Graph;

pub fn get_connection() -> Connection {
    let client = Client::open("redis://127.0.0.1").expect("Failed to open client!");
    client.get_connection().expect("Failed to get connection!")
}

pub fn with_graph<F: FnOnce(&mut Graph) -> ()>(action: F) {
    let conn = get_connection();
    let mut graph = Graph::open(conn, "test_graph".to_string()).unwrap();

    action(&mut graph);

    graph.delete().unwrap();
}
