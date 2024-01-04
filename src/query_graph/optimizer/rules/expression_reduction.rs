use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::row_type,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::reduction::reduce_expr_recursively,
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
                query_graph.project(
                    *input,
                    outputs
                        .iter()
                        .map(|e| reduce_expr_recursively(e, &query_graph, &row_type))
                        .collect_vec(),
                )
            }
            QueryNode::Filter { conditions, input } => {
                let row_type = row_type(query_graph, *input);
                query_graph.filter(
                    *input,
                    conditions
                        .iter()
                        .map(|e| reduce_expr_recursively(e, &query_graph, &row_type))
                        .collect_vec(),
                )
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
                query_graph.add_node(QueryNode::Join {
                    join_type: join_type.clone(),
                    conditions: conditions
                        .iter()
                        .map(|e| reduce_expr_recursively(e, &query_graph, &row_type))
                        .collect_vec(),
                    left: *left,
                    right: *right,
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
