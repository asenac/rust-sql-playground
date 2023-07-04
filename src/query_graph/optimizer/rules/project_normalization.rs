use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::equivalence_classes,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::rewrite::normalize_scalar_expr,
};

/// Replace sub-expressions in a projection with the representative of the equivalence
/// class they belong to.
pub struct ProjectNormalizationRule {}

impl SingleReplacementRule for ProjectNormalizationRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Project { outputs, input } = query_graph.node(node_id) {
            let classes = equivalence_classes(query_graph, *input);
            let new_outputs = outputs
                .iter()
                .map(|expr| normalize_scalar_expr(expr, &classes))
                .collect::<Vec<_>>();

            if new_outputs.iter().zip(outputs.iter()).any(|(x, y)| x != y) {
                return Some(query_graph.project(*input, new_outputs));
            }
        }
        None
    }
}
