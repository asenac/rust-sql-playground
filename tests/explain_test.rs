use std::collections::{BTreeSet, HashMap, HashSet};

use datadriven::walk;
use rust_sql::query_graph::explain::Explainer;
use rust_sql::query_graph::json::JsonSerializer;
use rust_sql::query_graph::optimizer::{
    build_rule, Optimizer, OptimizerContext, OptimizerListener, Replacement, DEFAULT_OPTIMIZER,
};
use rust_sql::query_graph::{JoinType, QueryGraph, QueryNode};
use rust_sql::scalar_expr::NaryOp;
use rust_sql::scalar_expr::ScalarExpr;
use rust_sql::scalar_expr::{BinaryOp, ToRef};

mod test_queries {
    use itertools::Itertools;
    use rust_sql::{
        data_type::DataType,
        scalar_expr::{AggregateExpr, AggregateOp, ToRef},
    };

    use super::*;

    pub(crate) fn aggregate_project_transpose(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("aggregate_project_transpose_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let project_1 = query_graph.project(
                table_scan_1,
                (0..5)
                    .rev()
                    .map(|i| ScalarExpr::input_ref(i).to_ref())
                    .collect_vec(),
            );
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Min,
                        operands: vec![4],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                ],
                input: project_1,
            });
            query_graph.set_entry_node(aggregate_1);
            query_graph
        });
    }

    pub(crate) fn aggregate_remove(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("redundant_aggregate".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: aggregate_1,
            });
            query_graph.set_entry_node(aggregate_2);
            query_graph
        });
    }

    pub(crate) fn aggregate_pruning(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("aggregate_pruning_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 6);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Min,
                        operands: vec![3],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![5],
                    }
                    .to_ref(),
                ],
                input: table_scan_1,
            });
            let project_1 = query_graph.project(
                aggregate_1,
                vec![
                    ScalarExpr::input_ref(0).to_ref(),
                    ScalarExpr::input_ref(4).to_ref(),
                ],
            );
            let project_2 = query_graph.project(
                aggregate_1,
                vec![
                    ScalarExpr::input_ref(1).to_ref(),
                    ScalarExpr::input_ref(5).to_ref(),
                ],
            );
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![project_1, project_2],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        // Nothing can be pruned
        queries.insert("aggregate_pruning_2".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 6);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Min,
                        operands: vec![3],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![5],
                    }
                    .to_ref(),
                ],
                input: table_scan_1,
            });
            let project_1 = query_graph.project(
                aggregate_1,
                vec![
                    ScalarExpr::input_ref(3).to_ref(),
                    ScalarExpr::input_ref(4).to_ref(),
                ],
            );
            let project_2 = query_graph.project(
                aggregate_1,
                vec![
                    ScalarExpr::input_ref(3).to_ref(),
                    ScalarExpr::input_ref(5).to_ref(),
                ],
            );
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![project_1, project_2],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        queries.insert("aggregate_pruning_3".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 6);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Min,
                        operands: vec![3],
                    }
                    .to_ref(),
                    // This one could be pruned as it is redundant
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                ],
                input: table_scan_1,
            });
            let project_1 = query_graph.project(
                aggregate_1,
                vec![
                    ScalarExpr::input_ref(3).to_ref(),
                    ScalarExpr::input_ref(4).to_ref(),
                ],
            );
            let project_2 = query_graph.project(
                aggregate_1,
                vec![
                    ScalarExpr::input_ref(3).to_ref(),
                    ScalarExpr::input_ref(5).to_ref(),
                ],
            );
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![project_1, project_2],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
    }

    pub(crate) fn filter_merge(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("filter_merge_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_id = query_graph.table_scan(0, 10);
            let filter_1 = ScalarExpr::input_ref(0)
                .binary(BinaryOp::Eq, ScalarExpr::input_ref(1).to_ref())
                .to_ref();
            let filter_id_1 = query_graph.filter(table_scan_id, vec![filter_1.clone()]);
            let filter_2 = ScalarExpr::input_ref(2)
                .binary(BinaryOp::Gt, ScalarExpr::input_ref(3).to_ref())
                .to_ref();
            let filter_id_2 = query_graph.filter(filter_id_1, vec![filter_2.clone()]);
            query_graph.set_entry_node(filter_id_2);
            query_graph
        });
        queries.insert("filter_merge_2".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_id = query_graph.table_scan(0, 10);
            let filter_1 = ScalarExpr::input_ref(0)
                .binary(BinaryOp::Eq, ScalarExpr::input_ref(1).to_ref())
                .to_ref();
            let filter_id_1 = query_graph.filter(table_scan_id, vec![filter_1]);
            let filter_2 = ScalarExpr::input_ref(2)
                .binary(BinaryOp::Gt, ScalarExpr::input_ref(3).to_ref())
                .to_ref();
            let filter_id_2 = query_graph.filter(filter_id_1, vec![filter_2]);
            let filter_3 = ScalarExpr::input_ref(4)
                .binary(BinaryOp::Lt, ScalarExpr::input_ref(5).to_ref())
                .to_ref();
            let filter_id_3 = query_graph.filter(filter_id_2, vec![filter_3]);
            query_graph.set_entry_node(filter_id_3);
            query_graph
        });
        queries.insert("mergeable_filters".to_string(), {
            let mut query_graph = QueryGraph::new();
            // select col0, col2 from (select col0, col9, col2 || col4 from (select * from table_1 where col0 = 'hello') where col5 = 'world')
            let table_scan_1 = query_graph.table_scan(1, 10);
            let filter_1 = query_graph.filter(
                table_scan_1,
                vec![ScalarExpr::input_ref(0)
                    .binary(
                        BinaryOp::Eq,
                        ScalarExpr::string_literal("hello".to_string()).to_ref(),
                    )
                    .to_ref()],
            );
            let filter_2 = query_graph.filter(
                filter_1,
                vec![ScalarExpr::input_ref(5)
                    .binary(
                        BinaryOp::Eq,
                        ScalarExpr::string_literal("world".to_string()).to_ref(),
                    )
                    .to_ref()],
            );
            let project_1 = query_graph.project(
                filter_2,
                vec![
                    ScalarExpr::input_ref(0).to_ref(),
                    ScalarExpr::input_ref(9).to_ref(),
                    ScalarExpr::nary(
                        NaryOp::Concat,
                        vec![
                            ScalarExpr::input_ref(2).to_ref(),
                            ScalarExpr::input_ref(4).to_ref(),
                        ],
                    )
                    .to_ref(),
                ],
            );
            let project_2 = query_graph.project(
                project_1,
                vec![
                    ScalarExpr::input_ref(0).to_ref(),
                    ScalarExpr::input_ref(2).to_ref(),
                ],
            );
            query_graph.set_entry_node(project_2);
            query_graph
        });
    }

    pub(crate) fn keys_join(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("join_between_keyless_aggregations".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let aggregate = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::new(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let join = query_graph.inner_join(aggregate, aggregate, Vec::new());
            query_graph.set_entry_node(join);
            query_graph
        });
        queries.insert("join_keys_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::new(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let join = query_graph.inner_join(aggregate_2, aggregate_1, Vec::new());
            let project = query_graph.project(
                join,
                (0..3)
                    .rev()
                    .map(|col| ScalarExpr::input_ref(col).to_ref())
                    .collect(),
            );
            query_graph.set_entry_node(project);
            query_graph
        });
        queries.insert("join_keys_2".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::new(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let join = query_graph.inner_join(aggregate_1, aggregate_2, Vec::new());
            let project = query_graph.project(
                join,
                (0..3)
                    .rev()
                    .map(|col| ScalarExpr::input_ref(col).to_ref())
                    .collect(),
            );
            query_graph.set_entry_node(project);
            query_graph
        });
        queries.insert("join_keys_3".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let aggregate = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let join = query_graph.inner_join(
                aggregate,
                aggregate,
                (0..3)
                    .map(|i| {
                        ScalarExpr::input_ref(i)
                            .binary(BinaryOp::Eq, ScalarExpr::input_ref(i + 3).to_ref())
                            .to_ref()
                    })
                    .collect(),
            );
            let project = query_graph.project(
                join,
                (0..3)
                    .rev()
                    .map(|col| ScalarExpr::input_ref(col).to_ref())
                    .collect(),
            );
            query_graph.set_entry_node(project);
            query_graph
        });
        queries.insert("left_outer_join_keys_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::new(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let join = query_graph.join(JoinType::LeftOuter, aggregate_1, aggregate_2, Vec::new());
            let project = query_graph.project(
                join,
                (0..3)
                    .rev()
                    .map(|col| ScalarExpr::input_ref(col).to_ref())
                    .collect(),
            );
            query_graph.set_entry_node(project);
            query_graph
        });
        queries.insert("left_outer_join_keys_2".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let table_scan_2 = query_graph.table_scan(1, 5);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..2).collect(),
                aggregates: Vec::new(),
                input: table_scan_2,
            });
            let join = query_graph.join(
                JoinType::LeftOuter,
                aggregate_1,
                aggregate_2,
                vec![
                    ScalarExpr::input_ref(0)
                        .binary(BinaryOp::Eq, ScalarExpr::input_ref(3).to_ref())
                        .to_ref(),
                    ScalarExpr::input_ref(1)
                        .binary(BinaryOp::Eq, ScalarExpr::input_ref(4).to_ref())
                        .to_ref(),
                    ScalarExpr::input_ref(2)
                        .binary(BinaryOp::Eq, ScalarExpr::input_ref(4).to_ref())
                        .to_ref(),
                ],
            );
            query_graph.set_entry_node(join);
            query_graph
        });
        queries.insert("right_outer_join_keys_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let table_scan_2 = query_graph.table_scan(1, 5);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..2).collect(),
                aggregates: Vec::new(),
                input: table_scan_2,
            });
            let join = query_graph.join(
                JoinType::RightOuter,
                aggregate_1,
                aggregate_2,
                vec![
                    ScalarExpr::input_ref(0)
                        .binary(BinaryOp::Eq, ScalarExpr::input_ref(3).to_ref())
                        .to_ref(),
                    ScalarExpr::input_ref(1)
                        .binary(BinaryOp::Eq, ScalarExpr::input_ref(4).to_ref())
                        .to_ref(),
                    ScalarExpr::input_ref(2)
                        .binary(BinaryOp::Eq, ScalarExpr::input_ref(4).to_ref())
                        .to_ref(),
                ],
            );
            query_graph.set_entry_node(join);
            query_graph
        });
    }

    pub(crate) fn filter_aggregate_transpose(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("filter_aggregate".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let filter_1 = query_graph.filter(
                aggregate_1,
                vec![
                    ScalarExpr::input_ref(1)
                        .binary(
                            BinaryOp::Lt,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref(),
                    ScalarExpr::input_ref(0)
                        .binary(
                            BinaryOp::Gt,
                            ScalarExpr::string_literal("world".to_string()).to_ref(),
                        )
                        .to_ref(),
                ],
            );
            let filter_2 = query_graph.filter(
                aggregate_1,
                vec![ScalarExpr::input_ref(0)
                    .binary(
                        BinaryOp::Gt,
                        ScalarExpr::string_literal("world".to_string()).to_ref(),
                    )
                    .to_ref()],
            );
            let union_ = query_graph.add_node(QueryNode::Union {
                inputs: vec![filter_2, filter_1],
            });
            query_graph.set_entry_node(union_);
            query_graph
        });
    }

    pub(crate) fn filter_project_transpose(queries: &mut HashMap<String, QueryGraph>) {
        // filter_project_transpose.test
        queries.insert("filter_project_transpose_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_id = query_graph.table_scan(0, 5);
            let project_outputs = vec![
                ScalarExpr::input_ref(4).to_ref(),
                ScalarExpr::input_ref(2).to_ref(),
                ScalarExpr::input_ref(3).to_ref(),
            ];
            let project_id = query_graph.project(table_scan_id, project_outputs);
            let filter_2 = ScalarExpr::input_ref(2)
                .binary(BinaryOp::Gt, ScalarExpr::input_ref(1).to_ref())
                .to_ref();
            let filter_id_2 = query_graph.filter(project_id, vec![filter_2]);
            query_graph.set_entry_node(filter_id_2);
            query_graph
        });
    }

    pub(crate) fn filter_join_transpose(queries: &mut HashMap<String, QueryGraph>) {
        for (suffix, join_type) in [
            ("inner", JoinType::Inner),
            ("left", JoinType::LeftOuter),
            ("right", JoinType::RightOuter),
            ("full", JoinType::FullOuter),
        ] {
            queries.insert("filter_join_transpose_".to_string() + suffix, {
                let mut query_graph = QueryGraph::new();
                let table_scan_1 = query_graph.table_scan(1, 5);
                let table_scan_2 = query_graph.table_scan(2, 5);
                let join = query_graph.join(
                    join_type,
                    table_scan_1,
                    table_scan_2,
                    vec![ScalarExpr::input_ref(0)
                        .binary(BinaryOp::Eq, ScalarExpr::input_ref(5).to_ref())
                        .to_ref()],
                );
                let filter_1 = query_graph.filter(
                    join,
                    vec![
                        ScalarExpr::input_ref(1)
                            .binary(
                                BinaryOp::Lt,
                                ScalarExpr::string_literal("hello".to_string()).to_ref(),
                            )
                            .to_ref(),
                        ScalarExpr::input_ref(2)
                            .binary(
                                BinaryOp::Eq,
                                ScalarExpr::string_literal("hello".to_string()).to_ref(),
                            )
                            .to_ref(),
                        ScalarExpr::input_ref(6)
                            .binary(
                                BinaryOp::Gt,
                                ScalarExpr::string_literal("world".to_string()).to_ref(),
                            )
                            .to_ref(),
                    ],
                );
                let filter_2 = query_graph.filter(
                    join,
                    vec![
                        ScalarExpr::input_ref(6)
                            .binary(
                                BinaryOp::Gt,
                                ScalarExpr::string_literal("world".to_string()).to_ref(),
                            )
                            .to_ref(),
                        ScalarExpr::input_ref(2)
                            .binary(
                                BinaryOp::Eq,
                                ScalarExpr::string_literal("hello".to_string()).to_ref(),
                            )
                            .to_ref(),
                    ],
                );
                let union_ = query_graph.add_node(QueryNode::Union {
                    inputs: vec![filter_2, filter_1],
                });
                query_graph.set_entry_node(union_);
                query_graph
            });
        }
        for (suffix, join_type) in [("semi", JoinType::Semi), ("anti", JoinType::Anti)] {
            queries.insert("filter_join_transpose_".to_string() + suffix, {
                let mut query_graph = QueryGraph::new();
                let table_scan_1 = query_graph.table_scan(1, 5);
                let table_scan_2 = query_graph.table_scan(2, 5);
                let join = query_graph.join(
                    join_type,
                    table_scan_1,
                    table_scan_2,
                    vec![ScalarExpr::input_ref(0)
                        .binary(BinaryOp::Eq, ScalarExpr::input_ref(5).to_ref())
                        .to_ref()],
                );
                let filter_1 = query_graph.filter(
                    join,
                    vec![
                        ScalarExpr::input_ref(1)
                            .binary(
                                BinaryOp::Lt,
                                ScalarExpr::string_literal("hello".to_string()).to_ref(),
                            )
                            .to_ref(),
                        ScalarExpr::input_ref(2)
                            .binary(
                                BinaryOp::Eq,
                                ScalarExpr::string_literal("hello".to_string()).to_ref(),
                            )
                            .to_ref(),
                    ],
                );
                let filter_2 = query_graph.filter(
                    join,
                    vec![ScalarExpr::input_ref(2)
                        .binary(
                            BinaryOp::Eq,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref()],
                );
                let union_ = query_graph.add_node(QueryNode::Union {
                    inputs: vec![filter_2, filter_1],
                });
                query_graph.set_entry_node(union_);
                query_graph
            });
        }
    }

    pub(crate) fn union_merge(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("union_merge".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![table_scan_1, table_scan_1],
            });
            let union_2 = query_graph.add_node(QueryNode::Union {
                inputs: vec![union_1, union_1, table_scan_1],
            });
            let union_3 = query_graph.add_node(QueryNode::Union {
                inputs: vec![union_1, union_2],
            });
            query_graph.set_entry_node(union_3);
            query_graph
        });
    }

    pub(crate) fn union_pruning(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("union_pruning".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![table_scan_1, table_scan_1],
            });
            let project_1 = query_graph.project(
                union_1,
                vec![
                    ScalarExpr::input_ref(0).to_ref(),
                    ScalarExpr::input_ref(2).to_ref(),
                ],
            );
            let project_2 = query_graph.project(
                union_1,
                vec![
                    ScalarExpr::input_ref(3).to_ref(),
                    ScalarExpr::input_ref(2).to_ref(),
                ],
            );
            let union_3 = query_graph.add_node(QueryNode::Union {
                inputs: vec![project_1, project_2],
            });
            query_graph.set_entry_node(union_3);
            query_graph
        });
    }

    pub(crate) fn join_project_transpose(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("join_project_transpose_left".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let project_1 = query_graph.project(
                table_scan_1,
                (0..5)
                    .rev()
                    .map(|col| ScalarExpr::input_ref(col).to_ref())
                    .collect(),
            );
            let join = query_graph.inner_join(
                project_1,
                table_scan_1,
                vec![ScalarExpr::input_ref(3)
                    .binary(BinaryOp::Eq, ScalarExpr::input_ref(5).to_ref())
                    .to_ref()],
            );
            query_graph.set_entry_node(join);
            query_graph
        });
        queries.insert("join_project_transpose_right".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let project_1 = query_graph.project(
                table_scan_1,
                (0..5)
                    .rev()
                    .map(|col| ScalarExpr::input_ref(col).to_ref())
                    .collect(),
            );
            let join = query_graph.inner_join(
                table_scan_1,
                project_1,
                vec![ScalarExpr::input_ref(3)
                    .binary(BinaryOp::Eq, ScalarExpr::input_ref(5).to_ref())
                    .to_ref()],
            );
            query_graph.set_entry_node(join);
            query_graph
        });
    }

    pub(crate) fn join_pruning(queries: &mut HashMap<String, QueryGraph>) {
        // join_pruning.test
        queries.insert("join_pruning_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let join = query_graph.inner_join(
                table_scan_1,
                table_scan_1,
                vec![ScalarExpr::input_ref(4)
                    .binary(BinaryOp::Eq, ScalarExpr::input_ref(15).to_ref())
                    .to_ref()],
            );
            let project_1 = query_graph.project(
                join,
                vec![
                    ScalarExpr::input_ref(0).to_ref(),
                    ScalarExpr::input_ref(18).to_ref(),
                ],
            );
            let project_2 = query_graph.project(
                join,
                vec![
                    ScalarExpr::input_ref(3).to_ref(),
                    ScalarExpr::input_ref(12).to_ref(),
                ],
            );
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![project_1, project_2],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        queries.insert("join_pruning_2".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let join = query_graph.inner_join(
                table_scan_1,
                table_scan_1,
                vec![ScalarExpr::input_ref(4)
                    .binary(BinaryOp::Eq, ScalarExpr::input_ref(15).to_ref())
                    .to_ref()],
            );
            let filter_1 = query_graph.filter(
                join,
                vec![ScalarExpr::input_ref(2)
                    .binary(BinaryOp::Eq, ScalarExpr::input_ref(16).to_ref())
                    .to_ref()],
            );
            let project_1 = query_graph.project(
                filter_1,
                vec![
                    ScalarExpr::input_ref(0).to_ref(),
                    ScalarExpr::input_ref(18).to_ref(),
                ],
            );
            let filter_2 = query_graph.filter(
                join,
                vec![ScalarExpr::input_ref(3)
                    .binary(BinaryOp::Eq, ScalarExpr::input_ref(15).to_ref())
                    .to_ref()],
            );
            let project_2 = query_graph.project(
                filter_2,
                vec![
                    ScalarExpr::input_ref(3).to_ref(),
                    ScalarExpr::input_ref(12).to_ref(),
                ],
            );
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![project_1, project_2],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        queries.insert("join_pruning_3".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 4);
            let table_scan_2 = query_graph.table_scan(1, 5);
            let join = query_graph.inner_join(
                table_scan_1,
                table_scan_2,
                vec![ScalarExpr::input_ref(0)
                    .binary(BinaryOp::Eq, ScalarExpr::input_ref(4).to_ref())
                    .to_ref()],
            );
            let filter_1 = query_graph.filter(
                join,
                vec![ScalarExpr::input_ref(2)
                    .binary(BinaryOp::Eq, ScalarExpr::input_ref(1).to_ref())
                    .to_ref()],
            );
            let agg_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::from([0, 1]),
                aggregates: Vec::new(),
                input: filter_1,
            });
            let agg_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::from([2, 5]),
                aggregates: Vec::new(),
                input: join,
            });
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![agg_1, agg_2],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
    }

    pub(crate) fn project_normalization(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("project_normalization_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let filter_1 = query_graph.filter(
                table_scan_1,
                vec![
                    ScalarExpr::input_ref(1)
                        .binary(
                            BinaryOp::Lt,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref(),
                    ScalarExpr::input_ref(2)
                        .binary(
                            BinaryOp::Eq,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref(),
                ],
            );
            let project_1 = query_graph.project(
                filter_1,
                vec![
                    ScalarExpr::input_ref(1).to_ref(),
                    ScalarExpr::input_ref(2).to_ref(),
                    ScalarExpr::input_ref(2)
                        .binary(
                            BinaryOp::Eq,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref(),
                ],
            );
            query_graph.set_entry_node(project_1);
            query_graph
        });
    }

    pub(crate) fn filter_normalization(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("filter_normalization_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let filter_1 = query_graph.filter(
                table_scan_1,
                vec![
                    ScalarExpr::input_ref(1)
                        .binary(
                            BinaryOp::Lt,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref(),
                    ScalarExpr::input_ref(1)
                        .binary(
                            BinaryOp::Lt,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref(),
                    ScalarExpr::input_ref(2)
                        .binary(
                            BinaryOp::Eq,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref(),
                ],
            );
            query_graph.set_entry_node(filter_1);
            query_graph
        });
    }

    pub(crate) fn pulled_up_predicates(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("union_predicates".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 10);
            let filter_1 = query_graph.filter(
                table_scan_1,
                vec![ScalarExpr::input_ref(0)
                    .binary(
                        BinaryOp::Eq,
                        ScalarExpr::string_literal("world".to_string()).to_ref(),
                    )
                    .to_ref()],
            );
            let filter_2 = query_graph.filter(
                table_scan_1,
                vec![
                    ScalarExpr::input_ref(1)
                        .binary(
                            BinaryOp::Eq,
                            ScalarExpr::string_literal("hello".to_string()).to_ref(),
                        )
                        .to_ref(),
                    ScalarExpr::input_ref(0)
                        .binary(
                            BinaryOp::Eq,
                            ScalarExpr::string_literal("world".to_string()).to_ref(),
                        )
                        .to_ref(),
                ],
            );
            let union_ = query_graph.add_node(QueryNode::Union {
                inputs: vec![filter_1, filter_2, filter_1],
            });
            query_graph.set_entry_node(union_);
            query_graph
        });
    }

    pub(crate) fn keys_filter(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("filter_keys_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let filter_1 =
                query_graph.filter(table_scan_1, vec![ScalarExpr::false_literal().to_ref()]);
            query_graph.set_entry_node(filter_1);
            query_graph
        });
        queries.insert("filter_keys_2".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let filter_1 = query_graph.filter(
                table_scan_1,
                vec![ScalarExpr::null_literal(DataType::Bool).to_ref()],
            );
            query_graph.set_entry_node(filter_1);
            query_graph
        });
    }

    pub(crate) fn keys_union(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("union_keys_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::from([2, 4]),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![aggregate_1, aggregate_1],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        queries.insert("union_keys_2".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::new(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![aggregate_1, aggregate_1],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        queries.insert("union_keys_3".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::from([2, 4]),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let project_1 = query_graph.project(
                table_scan_1,
                [2, 4]
                    .iter()
                    .map(|i| ScalarExpr::input_ref(*i).to_ref())
                    .collect_vec(),
            );
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![aggregate_1, project_1],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        // TODO(asenac) the lower bound for the empty key is known: 1
        queries.insert("union_keys_4".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::from([2, 4]),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: BTreeSet::new(),
                aggregates: Vec::new(),
                input: table_scan_1,
            });
            let project_1 = query_graph.project(
                aggregate_2,
                vec![
                    ScalarExpr::string_literal("hello".to_owned()).to_ref(),
                    ScalarExpr::string_literal("world".to_owned()).to_ref(),
                ],
            );
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![aggregate_1, project_1],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
    }

    pub(crate) fn common_aggregate_discovery(queries: &mut HashMap<String, QueryGraph>) {
        queries.insert("common_aggregate_discovery_1".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Min,
                        operands: vec![4],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                ],
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![4],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                ],
                input: table_scan_1,
            });
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![aggregate_1, aggregate_2],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        queries.insert("common_aggregate_discovery_2".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Min,
                        operands: vec![4],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                ],
                input: table_scan_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![4],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Max,
                        operands: vec![3],
                    }
                    .to_ref(),
                ],
                input: table_scan_1,
            });
            let aggregate_3 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![
                    AggregateExpr {
                        op: AggregateOp::Min,
                        operands: vec![4],
                    }
                    .to_ref(),
                    AggregateExpr {
                        op: AggregateOp::Min,
                        operands: vec![3],
                    }
                    .to_ref(),
                ],
                input: table_scan_1,
            });
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![aggregate_1, aggregate_2, aggregate_3],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
        queries.insert("common_aggregate_discovery_3".to_string(), {
            let mut query_graph = QueryGraph::new();
            let table_scan_1 = query_graph.table_scan(1, 5);
            let project_1 = query_graph.project(
                table_scan_1,
                vec![
                    ScalarExpr::input_ref(0).to_ref(),
                    ScalarExpr::input_ref(1).to_ref(),
                    ScalarExpr::input_ref(2).to_ref(),
                    ScalarExpr::nary(
                        NaryOp::Concat,
                        vec![
                            ScalarExpr::input_ref(3).to_ref(),
                            ScalarExpr::input_ref(4).to_ref(),
                        ],
                    )
                    .to_ref(),
                ],
            );
            let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![AggregateExpr {
                    op: AggregateOp::Min,
                    operands: vec![3],
                }
                .to_ref()],
                input: project_1,
            });
            let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
                group_key: (0..3).collect(),
                aggregates: vec![AggregateExpr {
                    op: AggregateOp::Max,
                    operands: vec![4],
                }
                .to_ref()],
                input: table_scan_1,
            });
            let union_1 = query_graph.add_node(QueryNode::Union {
                inputs: vec![aggregate_1, aggregate_2],
            });
            query_graph.set_entry_node(union_1);
            query_graph
        });
    }
}

fn static_queries() -> HashMap<String, QueryGraph> {
    let mut queries = HashMap::new();
    queries.insert("redundant_key".to_string(), {
        let mut query_graph = QueryGraph::new();
        let table_scan_1 = query_graph.table_scan(1, 10);
        let project = query_graph.project(
            table_scan_1,
            vec![
                ScalarExpr::input_ref(0).to_ref(),
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(0).to_ref(),
                        ScalarExpr::input_ref(2).to_ref(),
                    ],
                )
                .to_ref(),
                ScalarExpr::input_ref(2).to_ref(),
            ],
        );
        let aggregate = query_graph.add_node(QueryNode::Aggregate {
            group_key: (0..3).collect(),
            aggregates: Vec::new(),
            input: project,
        });
        query_graph.set_entry_node(aggregate);
        query_graph
    });
    queries.insert("redundant_key_2".to_string(), {
        let mut query_graph = QueryGraph::new();
        let table_scan_1 = query_graph.table_scan(1, 10);
        let project = query_graph.project(
            table_scan_1,
            vec![
                ScalarExpr::input_ref(0).to_ref(),
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(0).to_ref(),
                        ScalarExpr::input_ref(2).to_ref(),
                    ],
                )
                .to_ref(),
                ScalarExpr::input_ref(2).to_ref(),
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(0).to_ref(),
                        ScalarExpr::input_ref(2).to_ref(),
                    ],
                )
                .to_ref(),
            ],
        );
        let aggregate = query_graph.add_node(QueryNode::Aggregate {
            group_key: (0..3).collect(),
            aggregates: Vec::new(),
            input: project,
        });
        query_graph.set_entry_node(aggregate);
        query_graph
    });
    queries.insert("redundant_key_3".to_string(), {
        let mut query_graph = QueryGraph::new();
        let table_scan_1 = query_graph.table_scan(1, 10);
        let project = query_graph.project(
            table_scan_1,
            vec![
                ScalarExpr::input_ref(0).to_ref(),
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(0).to_ref(),
                        ScalarExpr::input_ref(2).to_ref(),
                    ],
                )
                .to_ref(),
                ScalarExpr::input_ref(2).to_ref(),
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(0).to_ref(),
                        ScalarExpr::input_ref(2).to_ref(),
                    ],
                )
                .to_ref(),
            ],
        );
        let aggregate = query_graph.add_node(QueryNode::Aggregate {
            group_key: (0..4).collect(),
            aggregates: Vec::new(),
            input: project,
        });
        query_graph.set_entry_node(aggregate);
        query_graph
    });
    queries.insert("constant_key".to_string(), {
        let mut query_graph = QueryGraph::new();
        let table_scan_1 = query_graph.table_scan(1, 10);
        let project = query_graph.project(
            table_scan_1,
            vec![
                ScalarExpr::string_literal("hello".to_string()).to_ref(),
                ScalarExpr::string_literal("world".to_string()).to_ref(),
                ScalarExpr::string_literal("bla".to_string()).to_ref(),
            ],
        );
        let aggregate = query_graph.add_node(QueryNode::Aggregate {
            group_key: (0..3).collect(),
            aggregates: Vec::new(),
            input: project,
        });
        query_graph.set_entry_node(aggregate);
        query_graph
    });
    queries.insert("recurrent_node".to_string(), {
        let mut query_graph = QueryGraph::new();
        let table_scan_1 = query_graph.table_scan(1, 10);
        let aggregate_1 = query_graph.add_node(QueryNode::Aggregate {
            group_key: (0..3).collect(),
            aggregates: Vec::new(),
            input: table_scan_1,
        });
        let aggregate_2 = query_graph.add_node(QueryNode::Aggregate {
            group_key: (0..3).collect(),
            aggregates: Vec::new(),
            input: table_scan_1,
        });
        let filter_1 = query_graph.filter(
            aggregate_2,
            vec![ScalarExpr::input_ref(0)
                .binary(
                    BinaryOp::Eq,
                    ScalarExpr::string_literal("world".to_string()).to_ref(),
                )
                .to_ref()],
        );
        let union_ = query_graph.add_node(QueryNode::Union {
            inputs: vec![aggregate_1, filter_1],
        });
        query_graph.set_entry_node(union_);
        query_graph
    });

    test_queries::aggregate_project_transpose(&mut queries);
    test_queries::aggregate_pruning(&mut queries);
    test_queries::aggregate_remove(&mut queries);
    test_queries::common_aggregate_discovery(&mut queries);
    test_queries::filter_aggregate_transpose(&mut queries);
    test_queries::filter_join_transpose(&mut queries);
    test_queries::filter_merge(&mut queries);
    test_queries::filter_normalization(&mut queries);
    test_queries::filter_project_transpose(&mut queries);
    test_queries::join_project_transpose(&mut queries);
    test_queries::join_pruning(&mut queries);
    test_queries::keys_filter(&mut queries);
    test_queries::keys_join(&mut queries);
    test_queries::keys_union(&mut queries);
    test_queries::project_normalization(&mut queries);
    test_queries::pulled_up_predicates(&mut queries);
    test_queries::union_merge(&mut queries);
    test_queries::union_pruning(&mut queries);

    queries
}

struct DebugOptimizerListener {}

impl OptimizerListener for DebugOptimizerListener {
    fn node_replacements(
        &mut self,
        rule: &dyn rust_sql::query_graph::optimizer::Rule,
        query_graph: &QueryGraph,
        replacements: &Vec<Replacement>,
    ) {
        for (old_node_id, new_node_id) in replacements.iter() {
            let old_nodes = query_graph.collect_nodes_under(*old_node_id);
            let new_nodes = query_graph.collect_nodes_under(*new_node_id);
            let common_nodes = old_nodes
                .intersection(&new_nodes)
                .cloned()
                .collect::<HashSet<_>>();
            let explain = Explainer::new(query_graph)
                .with_leaves(common_nodes)
                .with_all_annotators()
                .with_entry_point(*old_node_id);
            println!("Before {}:\n{}", rule.name(), explain.explain());
            let explain = explain.with_entry_point(*new_node_id);
            println!("After {}:\n{}", rule.name(), explain.explain());
        }

        let mut serializer = JsonSerializer::new_with_all_annotators();
        serializer.add_subgraph(query_graph, query_graph.entry_node);
        for (old_node_id, new_node_id) in replacements.iter() {
            serializer.add_node_replacement(
                query_graph,
                *old_node_id,
                *new_node_id,
                rule.name().to_string(),
            );
        }
        println!("{}", serializer.serialize().unwrap());
    }
}

struct FullGraphCollector<'a> {
    serializer: JsonSerializer<'a>,
    replacement_count: usize,
}

impl<'a> FullGraphCollector<'a> {
    fn new() -> Self {
        Self {
            serializer: JsonSerializer::new_with_all_annotators(),
            replacement_count: 0,
        }
    }
}

impl<'a> OptimizerListener for FullGraphCollector<'a> {
    fn node_replacements(
        &mut self,
        rule: &dyn rust_sql::query_graph::optimizer::Rule,
        query_graph: &QueryGraph,
        replacements: &Vec<Replacement>,
    ) {
        self.replacement_count += 1;
        for (old_node_id, new_node_id) in replacements.iter() {
            self.serializer.add_subgraph(query_graph, *old_node_id);
            self.serializer.add_subgraph(query_graph, *new_node_id);
            self.serializer.add_node_replacement(
                query_graph,
                *old_node_id,
                *new_node_id,
                format!("{}: {}", self.replacement_count, rule.name()),
            );
        }
    }
}

#[test]
fn test_explain_properties() {
    let static_queries = static_queries();

    let optimizer = &DEFAULT_OPTIMIZER;
    walk("tests/testdata/explain", |f| {
        f.run(|test_case| -> String {
            println!("{}", test_case.input);
            let query_graph = static_queries
                .get(&test_case.input.trim().to_string())
                .unwrap();

            let mut serializer = JsonSerializer::new_with_all_annotators();
            serializer.add_subgraph(query_graph, query_graph.entry_node);
            println!("{}", serializer.serialize().unwrap());

            let mut cloned_query_graph = query_graph.clone();
            let mut listener2 = FullGraphCollector::new();
            listener2
                .serializer
                .add_subgraph(&cloned_query_graph, cloned_query_graph.entry_node);
            let mut listener = DebugOptimizerListener {};
            let mut opt_context = OptimizerContext::new();
            opt_context.append_listener(&mut listener);
            opt_context.append_listener(&mut listener2);

            if let Some(rules) = test_case.args.get("rules") {
                let optimizer =
                    Optimizer::new(rules.iter().map(|rule| build_rule(rule).unwrap()).collect());
                optimizer.optimize(&mut opt_context, &mut cloned_query_graph);
            } else {
                optimizer.optimize(&mut opt_context, &mut cloned_query_graph);
            }

            let mut serializer = JsonSerializer::new_with_all_annotators();
            serializer.add_subgraph(&cloned_query_graph, cloned_query_graph.entry_node);
            println!("{}", serializer.serialize().unwrap());

            // Use the tools in `tools` folder to visualize this graphs.
            println!("full:\n{}", listener2.serializer.serialize().unwrap());

            format!(
                "{}\n\nOptimized:\n{}",
                query_graph.fully_annotated_explain(),
                cloned_query_graph.fully_annotated_explain()
            )
        })
    });
}
