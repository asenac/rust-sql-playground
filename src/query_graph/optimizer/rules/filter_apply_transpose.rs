use crate::{
    query_graph::{
        optimizer::{utils::common_parent_filters, OptRuleType, SingleReplacementRule},
        properties::{num_columns, pulled_up_predicates},
        ApplyType, NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{rewrite::shift_left_input_refs, visitor::collect_input_dependencies},
};

/// Rule that pushes filters through apply.
///
/// Collects the common filter among all the parents of the apply and, pushes down those
/// only referring to one apply input.
pub struct FilterApplyTransposeRule {}

impl SingleReplacementRule for FilterApplyTransposeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Apply {
            apply_type,
            left,
            right,
            correlation,
        } = query_graph.node(node_id)
        {
            if let Some(common_conditions) = common_parent_filters(query_graph, node_id) {
                let left_num_columns = num_columns(query_graph, *left);
                let mut left_predicates = Vec::new();
                let mut right_predicates = Vec::new();

                let known_predicates = pulled_up_predicates(query_graph, node_id);
                let allowed_right_pushdown = match apply_type {
                    ApplyType::Inner => true,
                    ApplyType::LeftOuter => false,
                };

                for condition in common_conditions.iter() {
                    if known_predicates.contains(condition) {
                        // Skip those already known to be enforced either
                        // by any descendent node.
                        continue;
                    }
                    let dependencies = collect_input_dependencies(condition);
                    if !dependencies.is_empty() {
                        if dependencies.iter().all(|x| *x < left_num_columns) {
                            left_predicates.push(condition.clone());
                        } else if allowed_right_pushdown
                            && dependencies.iter().all(|x| *x >= left_num_columns)
                        {
                            right_predicates
                                .push(shift_left_input_refs(condition, left_num_columns));
                        }
                    }
                }

                if !left_predicates.is_empty() || !right_predicates.is_empty() {
                    let correlation = correlation.clone();
                    let left = *left;
                    let right = *right;
                    let apply_type = *apply_type;
                    let left = query_graph.filter(left, left_predicates);
                    let right = query_graph.filter(right, right_predicates);

                    return Some(query_graph.add_node(QueryNode::Apply {
                        correlation,
                        left,
                        right,
                        apply_type,
                    }));
                }
            }
        }
        None
    }
}
