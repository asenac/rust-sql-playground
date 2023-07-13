use rust_sql::query_graph::optimizer::OptimizerContext;
use rust_sql::query_graph::optimizer::DEFAULT_OPTIMIZER;
use rust_sql::query_graph::*;
use rust_sql::scalar_expr::*;

fn main() {
    let mut query_graph = {
        let mut query_graph = QueryGraph::new();
        // select col0, col2 from (select col0, col9, col2 || col4 from (select * from table_1 where col0 = 'hello') where col5 = 'world')
        let table_scan_1 = query_graph.table_scan(1, 10);
        let filter_1 = query_graph.filter(
            table_scan_1,
            vec![ScalarExpr::input_ref(0)
                .binary(
                    BinaryOp::Eq,
                    ScalarExpr::string_literal("hello".to_string()).into(),
                )
                .into()],
        );
        let filter_2 = query_graph.filter(
            filter_1,
            vec![ScalarExpr::input_ref(5)
                .binary(
                    BinaryOp::Eq,
                    ScalarExpr::string_literal("world".to_string()).into(),
                )
                .into()],
        );
        let project_1 = query_graph.project(
            filter_2,
            vec![
                ScalarExpr::input_ref(0).into(),
                ScalarExpr::input_ref(9).into(),
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(2).into(),
                        ScalarExpr::input_ref(4).into(),
                    ],
                )
                .into(),
            ],
        );
        let project_2 = query_graph.project(
            project_1,
            vec![
                ScalarExpr::input_ref(0).into(),
                ScalarExpr::input_ref(2).into(),
            ],
        );
        query_graph.set_entry_node(project_2);
        query_graph
    };

    let optimizer = &DEFAULT_OPTIMIZER;

    println!("Before:\n\n{}", query_graph.fully_annotated_explain());

    println!("Before:\n\n{}", query_graph.explain());
    let mut opt_context = OptimizerContext::new();
    optimizer.optimize(&mut opt_context, &mut query_graph);
    println!("After:\n\n{}", query_graph.explain());

    query_graph.garbage_collect();
    println!("After:\n\n{}", query_graph.explain());
}
