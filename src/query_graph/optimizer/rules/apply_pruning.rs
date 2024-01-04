use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{
            utils::{
                apply_map_to_parents_and_replace_input, required_columns_from_parents,
                required_columns_to_column_map,
            },
            OptRuleType, Rule,
        },
        properties::num_columns,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{visitor::store_input_dependencies, ScalarExpr},
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
            correlation,
        } = query_graph.node(node_id)
        {
            if let Some(mut required_columns) = required_columns_from_parents(query_graph, node_id)
            {
                // Add the columns from the LHS referenced by the RHS
                for parameter in correlation.parameters.iter() {
                    store_input_dependencies(parameter, &mut required_columns);
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
                let correlation = correlation.clone();
                let apply_type = *apply_type;
                let left = *left;
                let right = *right;
                let new_left = query_graph.project(left, left_outputs);
                let new_right = query_graph.project(right, right_outputs);
                let new_apply = query_graph.add_node(QueryNode::Apply {
                    correlation,
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
