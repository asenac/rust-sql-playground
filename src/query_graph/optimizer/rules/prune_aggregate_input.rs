use std::collections::{BTreeSet, HashMap};

use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::{input_dependencies, num_columns},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::ScalarExpr,
};

pub struct PruneAggregateInputRule {}

impl SingleReplacementRule for PruneAggregateInputRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Aggregate { group_key, input } = query_graph.node(node_id) {
            let num_columns = num_columns(query_graph, *input);
            let input_dependencies = input_dependencies(query_graph, node_id);
            if num_columns != input_dependencies.len() {
                let column_map = input_dependencies
                    .iter()
                    .sorted()
                    .enumerate()
                    .map(|(i, j)| (*j, i))
                    .collect::<HashMap<_, _>>();
                let new_group_key = group_key
                    .iter()
                    .map(|k| *column_map.get(k).unwrap())
                    .collect::<BTreeSet<_>>();
                let project_outputs = input_dependencies
                    .iter()
                    .sorted()
                    .map(|i| ScalarExpr::input_ref(*i).to_ref())
                    .collect();

                let pruning_project = query_graph.project(*input, project_outputs);
                return Some(query_graph.add_node(QueryNode::Aggregate {
                    group_key: new_group_key,
                    input: pruning_project,
                }));
            }
        }
        None
    }
}
