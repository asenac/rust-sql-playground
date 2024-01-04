use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    query_graph::{
        cloner::deep_clone,
        properties::{subgraph_correlated_input_refs, subgraph_subqueries, subqueries},
        CorrelationId, NodeId, QueryGraph,
    },
    scalar_expr::{rewrite::rewrite_expr_post, rewrite_utils::apply_subquery_map, ScalarExprRef},
};

/// Recursively rewrite the subquery plans hanging from the given node that
/// are correlated wrt the `old_correlation_id`, to make the correlated references
/// point to `new_correlation_id` instead.
pub(crate) fn update_correlated_references_in_subqueries<F>(
    query_graph: &mut QueryGraph,
    node_id: NodeId,
    correlation_id: CorrelationId,
    update_func: F,
) -> HashMap<NodeId, NodeId>
where
    F: Fn(&ScalarExprRef) -> Option<ScalarExprRef>,
{
    let stack = node_subqueries_in_dependency_order(query_graph, node_id, correlation_id);
    let mut subquery_map = HashMap::new();
    for subquery_root_id in stack.iter().rev() {
        // Skip the subquery root
        let subquery_plan = query_graph.node(*subquery_root_id).get_input(0);
        let new_subquery_plan =
            deep_clone(query_graph, subquery_plan, &|_, _| false, &mut |expr| {
                rewrite_expr_post(
                    &mut |expr: &ScalarExprRef| {
                        update_func(expr).or_else(|| apply_subquery_map(expr, &subquery_map))
                    },
                    expr,
                )
            });
        let new_subquery_root_id = query_graph.add_subquery(new_subquery_plan);
        subquery_map.insert(*subquery_root_id, new_subquery_root_id);
    }
    subquery_map
}

/// Collect the subqueries in the given node, that are correlated wrt the given
/// correlation ID, recursively, returning them in dependency order.
fn node_subqueries_in_dependency_order(
    query_graph: &mut QueryGraph,
    node_id: NodeId,
    correlation_id: CorrelationId,
) -> Vec<NodeId> {
    // TODO(asenac) remove duplicates
    let mut stack = subqueries(query_graph, node_id)
        .iter()
        .filter(|subquery_root_id| {
            subgraph_correlated_input_refs(query_graph, **subquery_root_id)
                .contains_key(&correlation_id)
        })
        .cloned()
        .collect_vec();
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
