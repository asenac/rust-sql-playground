use crate::{
    query_graph::{
        optimizer::{utils::common_parent_filters, OptRuleType, SingleReplacementRule},
        properties::{num_columns, pulled_up_predicates},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{rewrite::shift_left_input_refs, visitor::collect_input_dependencies},
};

/// Rule that pushes filters through join.
///
/// Collects the common filter among all the parents of the join and, pushes down those
/// only referring to one join input.
pub struct FilterJoinTransposeRule {}

impl SingleReplacementRule for FilterJoinTransposeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Join {
            left,
            right,
            conditions,
        } = query_graph.node(node_id)
        {
            if let Some(common_conditions) = common_parent_filters(query_graph, node_id) {
                let left_num_columns = num_columns(query_graph, *left);
                let mut left_predicates = Vec::new();
                let mut right_predicates = Vec::new();

                let known_predicates = pulled_up_predicates(query_graph, node_id);

                for condition in common_conditions.iter() {
                    if known_predicates.contains(condition) {
                        // Skip those already known to be enforced either
                        // by the join or any descendent node.
                        continue;
                    }
                    let dependencies = collect_input_dependencies(condition);
                    if !dependencies.is_empty() {
                        if dependencies.iter().all(|x| *x < left_num_columns) {
                            left_predicates.push(condition.clone());
                        } else if dependencies.iter().all(|x| *x >= left_num_columns) {
                            right_predicates
                                .push(shift_left_input_refs(condition, left_num_columns));
                        }
                    }
                }

                if !left_predicates.is_empty() || !right_predicates.is_empty() {
                    let conditions = conditions.clone();
                    let left = *left;
                    let right = *right;
                    let left = query_graph.filter(left, left_predicates);
                    let right = query_graph.filter(right, right_predicates);

                    return Some(query_graph.join(left, right, conditions));
                }
            }
        }
        None
    }
}
