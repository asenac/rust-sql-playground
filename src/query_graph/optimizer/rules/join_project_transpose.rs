use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{utils::sort_projection, OptRuleType, SingleReplacementRule},
        properties::num_columns,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{rewrite::apply_column_map, ScalarExpr, ToRef},
};

/// Given a non-sorted projection at the input of a join, it creates a new join with
/// a sorted projection and adds a reordering projection on top of the new join to
/// leave the columns in the same order as before.
///
/// This is a normalization rule for lifting column reordering towards the root of
/// the query graph.
pub struct JoinProjectTransposeRule {}

impl SingleReplacementRule for JoinProjectTransposeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::BottomUp
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Join {
            join_type,
            left,
            right,
            conditions,
        } = query_graph.node(node_id)
        {
            // Lifting projection from the LHS
            if let QueryNode::Project {
                outputs,
                input: proj_input,
            } = query_graph.node(*left)
            {
                if let Some((reorder_map, sorted_proj)) = sort_projection(outputs) {
                    let left_num_columns = num_columns(query_graph, *left);
                    let right_num_columns = num_columns(query_graph, *right);
                    let column_map = reorder_map
                        .iter()
                        .enumerate()
                        .map(|(i, e)| (*e, i))
                        .chain(
                            (left_num_columns..left_num_columns + right_num_columns)
                                .map(|i| (i, i)),
                        )
                        .collect::<HashMap<_, _>>();
                    let new_conditions = conditions
                        .iter()
                        .map(|c| apply_column_map(c, &column_map).unwrap())
                        .collect_vec();

                    let final_project = column_map
                        .iter()
                        .sorted_by_key(|(_, j)| *j)
                        .map(|(i, _)| ScalarExpr::input_ref(*i).to_ref())
                        .collect_vec();

                    let join_type = *join_type;
                    let right = *right;
                    let new_left = query_graph.project(*proj_input, sorted_proj);
                    let new_join = query_graph.add_node(QueryNode::Join {
                        join_type,
                        conditions: new_conditions,
                        left: new_left,
                        right,
                    });

                    return Some(query_graph.project(new_join, final_project));
                }
            }
            // Lifting projection from the RHS
            if let QueryNode::Project {
                outputs,
                input: proj_input,
            } = query_graph.node(*right)
            {
                if let Some((reorder_map, sorted_proj)) = sort_projection(outputs) {
                    let left_num_columns = num_columns(query_graph, *left);
                    let column_map = reorder_map
                        .iter()
                        .enumerate()
                        .map(|(i, e)| (left_num_columns + *e, left_num_columns + i))
                        .chain((0..left_num_columns).map(|i| (i, i)))
                        .collect::<HashMap<_, _>>();
                    let new_conditions = conditions
                        .iter()
                        .map(|c| apply_column_map(c, &column_map).unwrap())
                        .collect_vec();

                    let final_project = column_map
                        .iter()
                        .sorted_by_key(|(_, j)| *j)
                        .map(|(i, _)| ScalarExpr::input_ref(*i).to_ref())
                        .collect_vec();

                    let join_type = *join_type;
                    let left = *left;
                    let new_right = query_graph.project(*proj_input, sorted_proj);
                    let new_join = query_graph.add_node(QueryNode::Join {
                        join_type,
                        conditions: new_conditions,
                        left,
                        right: new_right,
                    });

                    return Some(query_graph.project(new_join, final_project));
                }
            }
        }
        None
    }
}
