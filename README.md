[![githubactions](https://github.com/malte-v/redisgraph-rs/workflows/elixir/badge.svg)](https://github.com/malte-v/redisgraph-rs/actions)

# redisgraph-rs

`redisgraph-rs` is an idiomatic Rust client for RedisGraph, the graph database by Redis.

This crate parses responses from RedisGraph and converts them into ordinary Rust values.
It exposes a very flexible API that allows you to retrieve a single value, a single record
or multiple records using only one function: [`Graph::query`](https://docs.rs/redisgraph/0.1.0/redisgraph/graph/struct.Graph.html#method.query).

If you want to use this crate, add this to your Cargo.toml:

```ini
[dependencies]
redis = "0.15.1"
redisgraph = "0.1.0"
```

**Warning**: This library has not been thoroughly tested yet and some features are still missing.
Expect bugs and breaking changes.

## Resources

- RedisGraph documentation: [redisgraph.io][]
- API Reference: [docs.rs/redisgraph]

## Example

First, run RedisGraph on your machine using

```
$ docker run --name redisgraph-test -d --rm -p 6379:6379 redislabs/redisgraph
```

Then, try out this code:

```rust
use redis::Client;
use redisgraph::{Graph, RedisGraphResult};

fn main() -> RedisGraphResult<()> {
    let client = Client::open("redis://127.0.0.1")?;
    let mut connection = client.get_connection()?;

    let mut graph = Graph::open(&mut connection, "MotoGP")?;

    // Create six nodes (three riders, three teams) and three relationships between them.
    graph.mutate("CREATE (:Rider {name: 'Valentino Rossi', birth_year: 1979})-[:rides]->(:Team {name: 'Yamaha'}), \
        (:Rider {name:'Dani Pedrosa', birth_year: 1985, height: 1.58})-[:rides]->(:Team {name: 'Honda'}), \
        (:Rider {name:'Andrea Dovizioso', birth_year: 1986, height: 1.67})-[:rides]->(:Team {name: 'Ducati'})")?;

    // Get the names and birth years of all riders in team Yamaha.
    let results: Vec<(String, u32)> = graph.query("MATCH (r:Rider)-[:rides]->(t:Team) WHERE t.name = 'Yamaha' RETURN r.name, r.birth_year")?;
    // Since we know just one rider in our graph rides for team Yamaha,
    // we can also write this and only get the first record:
    let (name, birth_year): (String, u32) = graph.query("MATCH (r:Rider)-[:rides]->(t:Team) WHERE t.name = 'Yamaha' RETURN r.name, r.birth_year")?;
    // Let's now get all the data about the riders we have.
    // Be aware of that we only know the height of some riders, and therefore we use an `Option`:
    let results: Vec<(String, u32, Option<f32>)> = graph.query("MATCH (r:Rider) RETURN r.name, r.birth_year, r.height")?;

    // That was just a demo; we don't need this graph anymore. Let's delete it from the database:
    graph.delete()?;

    Ok(())
}
```

[redisgraph.io]:https://redisgraph.io
[docs.rs/redisgraph]:https://docs.rs/redisgraph
