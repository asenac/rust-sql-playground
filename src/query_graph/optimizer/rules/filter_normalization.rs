use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::{equivalence_classes, pulled_up_predicates},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        equivalence_class::to_replacement_map, rewrite::replace_sub_expressions_pre, ScalarExpr,
    },
};

pub struct FilterNormalizationRule {}

impl SingleReplacementRule for FilterNormalizationRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Filter { conditions, input } = query_graph.node(node_id) {
            let classes = equivalence_classes(query_graph, *input);
            let predicates = pulled_up_predicates(query_graph, *input);
            let mut replacement_map = to_replacement_map(&classes);
            let true_literal = ScalarExpr::true_literal().to_ref();
            replacement_map.extend(
                predicates
                    .iter()
                    .map(|predicate| (predicate.clone(), true_literal.clone())),
            );
            let new_conditions = conditions
                .iter()
                .map(|expr| replace_sub_expressions_pre(expr, &replacement_map))
                .filter(|expr| *expr != true_literal)
                .collect::<Vec<_>>();

            if new_conditions != *conditions {
                return Some(query_graph.filter(*input, new_conditions));
            }
        }
        None
    }
}
