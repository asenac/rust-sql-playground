use std::{collections::HashMap, rc::Rc};

use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::{column_provenance, row_type, ColumnProvenanceInfo},
        JoinType, NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        reduction::reduce_expr_recursively, rewrite::rewrite_expr_post, ScalarExpr, ScalarExprRef,
    },
    value::{Literal, Value},
    visitor_utils::PreOrderVisitationResult,
};

/// Rule that converts an outer join into an inner join if all the paths leading to the
/// join end up discarding nulls from the columns from the non preserving side of the
/// outer join node.
pub struct OuterToInnerJoinRule;

impl SingleReplacementRule for OuterToInnerJoinRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        match query_graph.node(node_id) {
            QueryNode::Join {
                join_type: JoinType::LeftOuter,
                conditions,
                left,
                right,
            } => {
                if do_all_parents_reject_null_from_non_preserving(query_graph, node_id, *right, 1) {
                    return Some(query_graph.add_node(QueryNode::Join {
                        join_type: JoinType::Inner,
                        conditions: conditions.clone(),
                        left: *left,
                        right: *right,
                    }));
                }
            }
            QueryNode::Join {
                join_type: JoinType::RightOuter,
                conditions,
                left,
                right,
            } => {
                if do_all_parents_reject_null_from_non_preserving(query_graph, node_id, *left, 0) {
                    return Some(query_graph.add_node(QueryNode::Join {
                        join_type: JoinType::Inner,
                        conditions: conditions.clone(),
                        left: *left,
                        right: *right,
                    }));
                }
            }
            _ => (),
        }
        None
    }
}

fn do_all_parents_reject_null_from_non_preserving(
    query_graph: &QueryGraph,
    join_node_id: NodeId,
    non_preserving_node_id: NodeId,
    non_preserving_side: usize,
) -> bool {
    if let Some(Ok(())) = query_graph.get_parents(join_node_id).map(|parents| {
        // Check that every parent is a chain of 0 or more filters leading to a projection
        parents.iter().try_for_each(|parent| {
            let mut rejects_null_from_non_preserving = false;
            query_graph.visit_subgraph_upwards_pre(
                &mut |query_graph, node_id| match query_graph.node(node_id) {
                    QueryNode::Project { .. } => PreOrderVisitationResult::VisitInputs,
                    QueryNode::Filter { conditions, input } => {
                        // For filters:
                        // 1. find the provenance information of input from the non_preserving_node_id
                        // 2. rewrite the column expressions replacing all input refs with nulls
                        // 3. reduce the resulting column expressions
                        // 4. replace the input refs in the conditions with the resulting column
                        //    that are literals expressions
                        // 5. reduce the conditions and check whether any of them results in either
                        //    FALSE OR NULL
                        // 1.)
                        if let Some(prov) = find_path_to_non_preserving_side(
                            query_graph,
                            *input,
                            non_preserving_node_id,
                            join_node_id,
                            non_preserving_side,
                        ) {
                            let input_row_type = row_type(query_graph, *input);
                            let non_prev_row_type = row_type(query_graph, non_preserving_node_id);
                            let rewrite_map = prov
                                .column_expressions
                                .iter()
                                .map(|e| {
                                    // 2.)
                                    if let Some(e) = e {
                                        return Some(rewrite_expr_post(
                                            &mut |curr_expr: &ScalarExprRef| {
                                                if let ScalarExpr::InputRef { index } =
                                                    curr_expr.as_ref()
                                                {
                                                    return Some(
                                                        ScalarExpr::null_literal(
                                                            non_prev_row_type[*index].clone(),
                                                        )
                                                        .into(),
                                                    );
                                                }
                                                None
                                            },
                                            e,
                                        ));
                                    }
                                    None
                                })
                                .enumerate()
                                .filter_map(|(i, e)| {
                                    // 3.)
                                    if let Some(e) = e {
                                        let reduced_expr =
                                            reduce_expr_recursively(&e, &input_row_type);
                                        if reduced_expr.is_literal() {
                                            return Some((i, reduced_expr));
                                        }
                                    }
                                    None
                                })
                                .collect::<HashMap<_, _>>();
                            for condition in conditions.iter() {
                                // 4.)
                                let rewritten_expr = rewrite_expr_post(
                                    &mut |curr_expr: &ScalarExprRef| {
                                        if let ScalarExpr::InputRef { index } = curr_expr.as_ref() {
                                            return rewrite_map.get(index).cloned();
                                        }
                                        None
                                    },
                                    condition,
                                );
                                // 5.)
                                let reduced_expr =
                                    reduce_expr_recursively(&rewritten_expr, &input_row_type);
                                match reduced_expr.as_ref() {
                                    ScalarExpr::Literal(Literal {
                                        value: Value::Bool(false),
                                        data_type: _,
                                    })
                                    | ScalarExpr::Literal(Literal {
                                        value: Value::Null,
                                        data_type: _,
                                    }) => {
                                        rejects_null_from_non_preserving = true;
                                        return PreOrderVisitationResult::Abort;
                                    }
                                    _ => {}
                                }
                            }
                            PreOrderVisitationResult::VisitInputs
                        } else {
                            PreOrderVisitationResult::Abort
                        }
                    }
                    QueryNode::Join {
                        conditions: _,
                        join_type: _,
                        left: _,
                        right: _,
                    } => {
                        // TODO(asenac) for joins, how do we know which one is the input node that
                        // leads to the non-preserving node?. Otherwise, the process is the same
                        // as for filters.
                        PreOrderVisitationResult::VisitInputs
                    }
                    // TODO(asenac) for aggregates we could do something
                    QueryNode::Aggregate { .. } | _ => PreOrderVisitationResult::Abort,
                },
                *parent,
            );
            if rejects_null_from_non_preserving {
                Ok(())
            } else {
                Err(())
            }
        })
    }) {
        true
    } else {
        false
    }
}

fn find_path_to_non_preserving_side(
    query_graph: &QueryGraph,
    from: NodeId,
    to: NodeId,
    join_id: NodeId,
    non_preserving_side: usize,
) -> Option<Rc<ColumnProvenanceInfo>> {
    let prov = column_provenance(query_graph, from);
    prov.iter()
        .find(|prov_info| {
            prov_info.source_node == to
                && prov_info.inverse_path.first() == Some(&non_preserving_side)
                && last_two_nodes_in_inverse_path(query_graph, from, &prov_info.inverse_path)
                    == (join_id, to)
        })
        .cloned()
}

fn last_two_nodes_in_inverse_path(
    query_graph: &QueryGraph,
    start_node: NodeId,
    inverse_path: &Vec<usize>,
) -> (NodeId, NodeId) {
    let (mut parent, mut current) = (start_node, start_node);
    for input in inverse_path.iter().rev() {
        parent = current;
        current = query_graph.node(current).get_input(*input);
    }
    (parent, current)
}
