use crate::query_graph::{
    optimizer::{OptRuleType, SingleReplacementRule},
    properties::{keys::empty_key, num_columns},
    JoinType, NodeId, QueryGraph, QueryNode,
};

pub struct IdentityJoinRule;

impl SingleReplacementRule for IdentityJoinRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Join {
            join_type: JoinType::Inner,
            conditions,
            left,
            right,
        } = query_graph.node(node_id)
        {
            let left_num_columns = num_columns(query_graph, *left);
            let right_num_columns = num_columns(query_graph, *right);
            let left_is_identity = left_num_columns == 0
                && empty_key(query_graph, *left)
                    .and_then(|key| Some(key.lower_bound == 1 && key.upper_bound == Some(1)))
                    .unwrap_or(false);
            let right_is_identity = right_num_columns == 0
                && empty_key(query_graph, *right)
                    .and_then(|key| Some(key.lower_bound == 1 && key.upper_bound == Some(1)))
                    .unwrap_or(false);
            let non_identity_relation = match (left_is_identity, right_is_identity) {
                (true, _) => Some(*right),
                (_, true) => Some(*left),
                _ => None,
            };
            if let Some(non_identity_relation) = non_identity_relation {
                return Some(query_graph.filter(non_identity_relation, conditions.clone()));
            }
        }
        None
    }
}
