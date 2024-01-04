use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::{
    query_graph::{
        cloner::deep_clone,
        properties::{subgraph_correlated_input_refs, subgraph_subqueries, subqueries},
        CorrelationId, NodeId, QueryGraph,
    },
    scalar_expr::{
        rewrite::rewrite_expr_post,
        rewrite_utils::{
            apply_column_map_to_correlated_reference, apply_column_map_to_input_ref,
            apply_subquery_map,
        },
        visitor::visit_expr_pre,
        ScalarExpr, ScalarExprRef,
    },
    visitor_utils::PreOrderVisitationResult,
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

pub(crate) fn store_input_dependencies_in_possibly_correlated_node(
    query_graph: &QueryGraph,
    expr: &ScalarExprRef,
    correlation_id: Option<CorrelationId>,
    dependencies: &mut HashSet<usize>,
) {
    visit_expr_pre(expr, &mut |curr_expr: &ScalarExprRef| {
        match **curr_expr {
            ScalarExpr::InputRef { index } => {
                dependencies.insert(index);
            }
            ScalarExpr::ExistsSubquery { subquery, .. }
            | ScalarExpr::ScalarSubquery { subquery, .. }
            | ScalarExpr::ScalarSubqueryCmp { subquery, .. } => {
                if let Some(correlation_id) = correlation_id {
                    if let Some(columns) =
                        subgraph_correlated_input_refs(query_graph, subquery).get(&correlation_id)
                    {
                        dependencies.extend(columns.iter());
                    }
                }
            }
            _ => (),
        }
        PreOrderVisitationResult::VisitInputs
    });
}

pub(crate) fn apply_column_map_to_possibly_correlated_filter(
    query_graph: &mut QueryGraph,
    filter_node_id: NodeId,
    conditions: Vec<ScalarExprRef>,
    input: NodeId,
    correlation_id: Option<CorrelationId>,
    column_map: &HashMap<NodeId, NodeId>,
) -> NodeId {
    let mut conditions = conditions;
    // For correlated filters, we need to update the correlated references in
    // the contained subqueries.
    let subquery_map = if let Some(correlation_id) = correlation_id {
        update_correlated_references_in_subqueries(
            query_graph,
            filter_node_id,
            correlation_id,
            |e| apply_column_map_to_correlated_reference(e, correlation_id, column_map),
        )
    } else {
        HashMap::new()
    };
    conditions.iter_mut().for_each(|e| {
        *e = rewrite_expr_post(
            &mut |e| {
                apply_column_map_to_input_ref(e, column_map)
                    .or_else(|| apply_subquery_map(e, &subquery_map))
            },
            e,
        )
    });
    let new_filter = query_graph.possibly_correlated_filter(input, conditions, correlation_id);
    new_filter
}
