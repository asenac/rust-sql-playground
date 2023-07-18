use itertools::Itertools;

use crate::query_graph::{
    optimizer::{OptRuleType, Rule},
    NodeId, QueryGraph,
};

/// Finds duplicated nodes in the query graph and replaces them with the equivalent node
/// with the lowest node ID.
pub struct CteDiscoveryRule {}

impl Rule for CteDiscoveryRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::RootOnly
    }

    fn apply(&self, query_graph: &mut QueryGraph, _: NodeId) -> Option<Vec<(NodeId, NodeId)>> {
        let mut node_ids = query_graph.nodes.keys().cloned().collect_vec();
        node_ids.sort();
        let replacements = node_ids
            .iter()
            .enumerate()
            .filter_map(|(i, orig_node_id)| {
                let node = query_graph.node(*orig_node_id);
                if let Some(replacement_node_id) = node_ids
                    .iter()
                    .take(i)
                    .find(|replacement_node_id| query_graph.node(**replacement_node_id) == node)
                {
                    Some((*orig_node_id, *replacement_node_id))
                } else {
                    None
                }
            })
            .collect_vec();
        if replacements.is_empty() {
            None
        } else {
            Some(replacements)
        }
    }
}
