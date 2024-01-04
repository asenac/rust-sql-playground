use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    query_graph::{
        cloner::deep_clone,
        optimizer::{
            utils::{
                apply_map_to_parents_and_replace_input, required_columns_from_parents,
                required_columns_to_column_map,
            },
            OptRuleType, Rule,
        },
        properties::{num_columns, subgraph_correlated_input_refs, subgraph_subqueries},
        CorrelationId, NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        rewrite::rewrite_expr_post,
        rewrite_utils::{apply_subquery_map, update_correlated_reference},
        ScalarExpr, ScalarExprRef,
    },
};

/// Rule that given a shared apply where all its parents are pruning projections, computes
/// the superset of columns required by all its parents, and prunes the columns not used
/// by any of them, replacing the parents of the apply with projections over the pruned
/// join.
pub struct ApplyPruningRule {}

impl Rule for ApplyPruningRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(
        &self,
        query_graph: &mut QueryGraph,
        node_id: NodeId,
    ) -> Option<Vec<(NodeId, NodeId)>> {
        if let QueryNode::Apply {
            apply_type,
            left,
            right,
            correlation_id,
        } = query_graph.node(node_id)
        {
            if let Some(mut required_columns) = required_columns_from_parents(query_graph, node_id)
            {
                // Add the columns from the LHS referenced by the RHS
                if let Some(columns) =
                    subgraph_correlated_input_refs(query_graph, *right).get(correlation_id)
                {
                    required_columns.extend(columns.iter());
                }
                if required_columns.len() == num_columns(&query_graph, node_id) {
                    // All columns are referenced, nothing to prune
                    return None;
                }
                let column_map = required_columns_to_column_map(&required_columns);
                let left_num_columns = num_columns(query_graph, *left);
                let (left_columns, right_columns): (Vec<usize>, Vec<usize>) = required_columns
                    .iter()
                    .sorted()
                    .partition(|col| **col < left_num_columns);
                let left_outputs = left_columns
                    .iter()
                    .map(|i| ScalarExpr::InputRef { index: *i }.into())
                    .collect::<Vec<_>>();
                let right_outputs = right_columns
                    .iter()
                    .map(|i| {
                        ScalarExpr::InputRef {
                            index: *i - left_num_columns,
                        }
                        .into()
                    })
                    .collect::<Vec<_>>();
                let correlation_id = *correlation_id;
                let apply_type = *apply_type;
                let left = *left;
                let mut right = *right;
                if left_columns.len() != left_num_columns {
                    right = update_correlated_references(
                        query_graph,
                        right,
                        correlation_id,
                        &column_map,
                    );
                }
                let new_left = query_graph.project(left, left_outputs);
                let new_right = query_graph.project(right, right_outputs);
                let new_apply = query_graph.add_node(QueryNode::Apply {
                    correlation_id,
                    left: new_left,
                    right: new_right,
                    apply_type,
                });

                // Rewrite the parent projections
                return Some(apply_map_to_parents_and_replace_input(
                    query_graph,
                    node_id,
                    &column_map,
                    new_apply,
                ));
            }
        }
        None
    }
}

/// Rewrites the given subplan under `node_id`, which is known to be correlated
/// wrt `correlation_id`, so that all correlated references pointing to `correlation_id`
/// are updated according to the given `column_map`.
fn update_correlated_references(
    query_graph: &mut QueryGraph,
    node_id: NodeId,
    correlation_id: CorrelationId,
    column_map: &HashMap<usize, usize>,
) -> NodeId {
    let stack = subqueries_in_dependency_order(query_graph, node_id, correlation_id);
    let mut subquery_map = HashMap::new();
    for subquery_root_id in stack.iter().rev() {
        let is_subquery = *subquery_root_id != node_id;
        let subquery_plan = if is_subquery {
            // Skip the subquery root
            query_graph.node(*subquery_root_id).get_input(0)
        } else {
            *subquery_root_id
        };
        let new_subquery_plan =
            deep_clone(query_graph, subquery_plan, &|_, _| false, &mut |expr| {
                rewrite_expr_post(
                    &mut |expr: &ScalarExprRef| {
                        update_correlated_reference(expr, correlation_id, column_map)
                            .or_else(|| apply_subquery_map(expr, &subquery_map))
                    },
                    expr,
                )
            });
        let new_subquery_root_id = if is_subquery {
            query_graph.add_subquery(new_subquery_plan)
        } else {
            new_subquery_plan
        };
        subquery_map.insert(*subquery_root_id, new_subquery_root_id);
    }
    *subquery_map.get(&node_id).unwrap()
}

fn subqueries_in_dependency_order(
    query_graph: &mut QueryGraph,
    node_id: NodeId,
    correlation_id: CorrelationId,
) -> Vec<NodeId> {
    // TODO(asenac) remove duplicates
    let mut stack = vec![node_id];
    let mut i = 0;
    while i < stack.len() {
        stack.extend(
            subgraph_subqueries(query_graph, stack[i])
                .iter()
                .filter(|subquery_root_id| {
                    subgraph_correlated_input_refs(query_graph, **subquery_root_id)
                        .contains_key(&correlation_id)
                })
                .cloned(),
        );
        i = i + 1;
    }
    stack
}
