use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::row_type,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::reduction::reduce_and_prune_exists_subplans_recursively,
};

pub struct ExpressionReductionRule;

impl SingleReplacementRule for ExpressionReductionRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        let new_node = match query_graph.node(node_id) {
            QueryNode::Project { outputs, input } => {
                let row_type = row_type(query_graph, *input);
                let input = *input;
                let mut outputs = outputs.clone();
                outputs.iter_mut().for_each(|e| {
                    *e = reduce_and_prune_exists_subplans_recursively(e, query_graph, &row_type)
                });
                query_graph.project(input, outputs)
            }
            QueryNode::Filter { conditions, input } => {
                let row_type = row_type(query_graph, *input);
                let input = *input;
                let mut conditions = conditions.clone();
                conditions.iter_mut().for_each(|e| {
                    *e = reduce_and_prune_exists_subplans_recursively(e, query_graph, &row_type)
                });
                query_graph.filter(input, conditions)
            }
            QueryNode::Join {
                join_type,
                conditions,
                left,
                right,
            } => {
                let left_row_type = row_type(query_graph, *left);
                let right_row_type = row_type(query_graph, *right);
                let row_type = left_row_type
                    .iter()
                    .chain(right_row_type.iter())
                    .cloned()
                    .collect_vec();
                let left = *left;
                let right = *right;
                let join_type = join_type.clone();
                let mut conditions = conditions.clone();
                conditions.iter_mut().for_each(|e| {
                    *e = reduce_and_prune_exists_subplans_recursively(e, query_graph, &row_type)
                });
                query_graph.add_node(QueryNode::Join {
                    join_type,
                    conditions,
                    left,
                    right,
                })
            }
            _ => node_id,
        };
        // Note: the graph may contain duplicated nodes as a result of input
        // replacements.
        if new_node != node_id && query_graph.node(new_node) != query_graph.node(node_id) {
            Some(new_node)
        } else {
            None
        }
    }
}
