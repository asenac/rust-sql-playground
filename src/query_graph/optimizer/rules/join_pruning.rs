use std::collections::HashSet;

use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{
            utils::{
                apply_map_to_parent_projections_and_replace_input,
                required_columns_from_parent_projections, required_columns_to_column_map,
            },
            OptRuleType, Rule,
        },
        properties::num_columns,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        rewrite::{apply_column_map, rewrite_expr_vec},
        visitor::store_input_dependencies,
        ScalarExpr,
    },
};

/// Rule that given a shared join where all its parents are pruning projections, computes
/// the superset of columns required by all its parents, and prunes the columns not used
/// by any of them, replacing the parents of the join with projections over the pruned
/// join.
pub struct JoinPruningRule {}

impl Rule for JoinPruningRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(
        &self,
        query_graph: &mut QueryGraph,
        node_id: NodeId,
    ) -> Option<Vec<(NodeId, NodeId)>> {
        if let QueryNode::Join {
            join_type,
            left,
            right,
            conditions,
        } = query_graph.node(node_id)
        {
            if let Some(required_columns) =
                required_columns_from_parent_projections(query_graph, node_id)
            {
                // Rewrite conditions
                let column_map = required_columns_to_column_map(&required_columns);
                let mut required_columns_including_join = column_map
                    .iter()
                    .map(|(col, _)| *col)
                    .collect::<HashSet<_>>();
                for condition in conditions.iter() {
                    store_input_dependencies(condition, &mut required_columns_including_join);
                }
                if required_columns_including_join.len() == num_columns(&query_graph, node_id) {
                    return None;
                }
                let join_column_map =
                    required_columns_to_column_map(&required_columns_including_join);
                let new_conditions = rewrite_expr_vec(conditions, &mut |e| {
                    apply_column_map(e, &join_column_map).unwrap()
                });

                // Prune the branches
                let left_num_columns = num_columns(query_graph, *left);
                let (left_columns, right_columns): (Vec<usize>, Vec<usize>) =
                    required_columns_including_join
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
                let left = *left;
                let right = *right;
                let join_type = *join_type;
                let new_left = query_graph.project(left, left_outputs);
                let new_right = query_graph.project(right, right_outputs);
                let new_join = query_graph.join(join_type, new_left, new_right, new_conditions);

                // Prune the columns used by the join conditions but not by the parents
                let pruning_proj_outputs = required_columns_including_join
                    .iter()
                    .sorted()
                    .enumerate()
                    .filter(|(_, orig_col)| required_columns.contains(&orig_col))
                    .map(|(i, _)| ScalarExpr::input_ref(i).into())
                    .collect();
                let pruning_proj = query_graph.project(new_join, pruning_proj_outputs);

                // Rewrite the parent projections
                return Some(apply_map_to_parent_projections_and_replace_input(
                    query_graph,
                    node_id,
                    &column_map,
                    pruning_proj,
                ));
            }
        }
        None
    }
}
