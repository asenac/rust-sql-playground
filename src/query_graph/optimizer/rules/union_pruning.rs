use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{
            utils::{
                apply_map_to_parent_projections_and_replace_input,
                required_columns_from_parent_projections,
            },
            OptRuleType, Rule,
        },
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::ScalarExpr,
};

/// Rule that given a shared union where all its parents are pruning projections, computes
/// the superset of columns required by all its parents, and prunes the columns not used
/// by any of them, replacing the parents of the union with projections over the pruned
/// union. A pruning projection is inserted under each branch of the pruned union.
pub struct UnionPruningRule {}

impl Rule for UnionPruningRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(
        &self,
        query_graph: &mut QueryGraph,
        node_id: NodeId,
    ) -> Option<Vec<(NodeId, NodeId)>> {
        if let QueryNode::Union { inputs } = query_graph.node(node_id) {
            if let Some(required_columns) =
                required_columns_from_parent_projections(query_graph, node_id)
            {
                // Prune the branches
                let proj = required_columns
                    .iter()
                    .map(|(i, _)| *i)
                    .sorted()
                    .map(|i| ScalarExpr::InputRef { index: i }.to_ref())
                    .collect::<Vec<_>>();
                let new_inputs = inputs
                    .clone() // clone to make the borrow checker happy
                    .iter()
                    .map(|input| query_graph.project(*input, proj.clone()))
                    .collect();
                let new_union = query_graph.add_node(QueryNode::Union { inputs: new_inputs });

                // Rewrite the parent projections
                return Some(apply_map_to_parent_projections_and_replace_input(
                    query_graph,
                    node_id,
                    &required_columns,
                    new_union,
                ));
            }
        }
        None
    }
}
