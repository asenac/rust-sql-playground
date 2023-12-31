use std::{collections::HashMap, rc::Rc};

use crate::{
    data_type::DataType,
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::{
            column_provenance, cross_product_row_type, num_columns, row_type, ColumnProvenanceInfo,
        },
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
                            // 2.) and 3.)
                            let rewrite_map =
                                build_rewrite_map(query_graph, &prov, non_preserving_node_id, 0);
                            let input_row_type = row_type(query_graph, *input);
                            // 4.) and 5.)
                            if any_condition_rejects_nulls(
                                query_graph,
                                &rewrite_map,
                                &input_row_type,
                                conditions,
                            ) {
                                rejects_null_from_non_preserving = true;
                                PreOrderVisitationResult::Abort
                            } else {
                                PreOrderVisitationResult::VisitInputs
                            }
                        } else {
                            PreOrderVisitationResult::Abort
                        }
                    }
                    QueryNode::Join {
                        conditions,
                        join_type,
                        left,
                        right,
                    } => {
                        let left_num_columns = num_columns(query_graph, *left);
                        // Null-rejecting predicates in inner joins reject nulls from both inputs,
                        // but for outer joins only nulls from their non-preserving side are rejected.
                        let paths = match join_type {
                            JoinType::Inner => vec![(*left, 0), (*right, left_num_columns)],
                            JoinType::RightOuter => vec![(*left, 0)],
                            JoinType::LeftOuter => vec![(*right, left_num_columns)],
                            _ => vec![],
                        };
                        for (input, column_offset) in paths {
                            if let Some(prov) = find_path_to_non_preserving_side(
                                query_graph,
                                input,
                                non_preserving_node_id,
                                join_node_id,
                                non_preserving_side,
                            ) {
                                // 2.) and 3.)
                                let rewrite_map = build_rewrite_map(
                                    query_graph,
                                    &prov,
                                    non_preserving_node_id,
                                    column_offset,
                                );
                                let input_row_type =
                                    cross_product_row_type(query_graph, node_id).unwrap();
                                // 4.) and 5.)
                                if any_condition_rejects_nulls(
                                    query_graph,
                                    &rewrite_map,
                                    &input_row_type,
                                    conditions,
                                ) {
                                    rejects_null_from_non_preserving = true;
                                    return PreOrderVisitationResult::Abort;
                                } else {
                                    return PreOrderVisitationResult::VisitInputs;
                                }
                            }
                        }
                        PreOrderVisitationResult::Abort
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

/// Build the replacement map to replace any reference to the column reference that
/// comes from the given provenance that can be reduced to a literal in a condition
/// on top of the node the provenance info is associated with.
fn build_rewrite_map(
    query_graph: &QueryGraph,
    prov: &ColumnProvenanceInfo,
    non_preserving_node_id: NodeId,
    // The initial offset used to refer the columns from the given provenance.
    output_column_offset: usize,
) -> HashMap<usize, ScalarExprRef> {
    let non_prev_row_type = row_type(query_graph, non_preserving_node_id);
    // column_expressions are written in terms of the output of non_preserving_node_id.
    prov.column_expressions
        .iter()
        // Replace the input refs with nulls (properly typed).
        .map(|e| {
            e.as_ref()
                .map(|e| replace_with_nulls(e, &non_prev_row_type))
        })
        .enumerate()
        .filter_map(|(i, e)| {
            if let Some(e) = e {
                // Reduce the expression containing nulls instead of input refs
                let reduced_expr = reduce_expr_recursively(&e, &query_graph, &non_prev_row_type);
                // If the expression can be reduced to a literal, we can add it to
                // the replacement map.
                if reduced_expr.is_literal() {
                    return Some((output_column_offset + i, reduced_expr));
                }
            }
            None
        })
        .collect::<HashMap<_, _>>()
}

/// Replace the input references with nulls of the corresponding type
fn replace_with_nulls(expr: &ScalarExprRef, row_type: &[DataType]) -> ScalarExprRef {
    rewrite_expr_post(
        &mut |curr_expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = curr_expr.as_ref() {
                return Some(ScalarExpr::null_literal(row_type[*index].clone()).into());
            }
            None
        },
        expr,
    )
}

/// Check whether any of the given conditions evaluates to null or false after
/// applying the replacements in the given map.
fn any_condition_rejects_nulls(
    query_graph: &QueryGraph,
    rewrite_map: &HashMap<usize, ScalarExprRef>,
    row_type: &[DataType],
    conditions: &Vec<ScalarExprRef>,
) -> bool {
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
        let reduced_expr = reduce_expr_recursively(&rewritten_expr, query_graph, row_type);
        match reduced_expr.as_ref() {
            ScalarExpr::Literal(Literal {
                value: Value::Bool(false),
                data_type: _,
            })
            | ScalarExpr::Literal(Literal {
                value: Value::Null,
                data_type: _,
            }) => {
                return true;
            }
            _ => {}
        }
    }
    return false;
}
