# SQL query compiler playground

This is a SQL query compiler written in Rust mainly for learning and blogging purposes.

There is no SQL parser yet and its overall functionality is very limited, although the
logical optimizer is getting real.

## Blog posts

* [Part one, the query plan representation](https://andres.senac.es/posts/query-compiler-part-one/)
* [Part two, the query rewrite driver](https://andres.senac.es/posts/query-compiler-part-two-rule-driver/)

## Visualizing query plans

`JsonSerializer` utility can be used to dump the query plan in JSON format that can be
rendered with any of the utilities in `tools` folder, using different graph rendering
libraries.

![Query plan][query-plan-1]

[query-plan-1]: .images/query-plan1.png