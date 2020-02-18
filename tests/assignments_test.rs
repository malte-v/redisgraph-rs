mod common;

use redisgraph::{RedisGraphResult, RedisString, result_set::{Node, Relation, Scalar}};
use serial_test::serial;
use maplit::hashmap;

use common::*;

#[test]
#[serial]
fn test_single() {
    with_graph(|graph| {
        let single: i64 = graph.query("RETURN 42").unwrap();
        assert_eq!(single, 42);
    });
}

#[test]
#[serial]
fn test_tuple() {
    with_graph(|graph| {
        let tuple: (i64, String, bool) = graph.query("RETURN 42, 'Hello, world!', true").unwrap();
        assert_eq!(tuple.0, 42);
        assert_eq!(tuple.1, "Hello, world!");
        assert_eq!(tuple.2, true);
    });
}

#[test]
#[serial]
fn test_vec() {
    with_graph(|graph| {
        graph.mutate("CREATE (n1 { prop: 1 }), (n2 { prop: 2 }), (n3 { prop: 3 })");
        let vec: Vec<i64> = graph.query("MATCH (n) RETURN n.prop ORDER BY n.prop").unwrap();
        assert_eq!(vec[0], 1);
        assert_eq!(vec[1], 2);
        assert_eq!(vec[2], 3);
    });
}

#[test]
#[serial]
fn test_tuple_vec() {
    with_graph(|graph| {
        graph.mutate("CREATE (n1 { num: 1, word: 'foo' }), (n2 { num: 2, word: 'bar' }), (n3 { num: 3, word: 'baz' })");
        let tuple_vec: Vec<(i64, String)> = graph.query("MATCH (n) RETURN n.num, n.word ORDER BY n.num").unwrap();
        assert_eq!(tuple_vec[0], (1, "foo".to_string()));
        assert_eq!(tuple_vec[1], (2, "bar".to_string()));
        assert_eq!(tuple_vec[2], (3, "baz".to_string()));
    });
}

#[test]
#[serial]
fn test_out_of_bounds() {
    with_graph(|graph| {
        let out_of_bounds_result: RedisGraphResult<(i64, String, bool)> = graph.query("RETURN 42, 'Hello, world!'");
        assert!(out_of_bounds_result.is_err());
    });
}
