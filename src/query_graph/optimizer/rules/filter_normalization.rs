use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::{equivalence_classes, pulled_up_predicates},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        equivalence_class::to_replacement_map, rewrite::replace_sub_expressions_pre, ScalarExpr,
        ScalarExprRef,
    },
};

/// Rule that, among other things, removes filter nodes, either partially or fully, enforcing
/// predicates that are already enforced by some descendent node.
///
/// Expressions are normalized so that each sub-expression is replaced with the representative
/// of their class, if any. For example, if we know that `'hello'` and `ref_1` belong to the
/// same equivalence class, then we can replace any appearance of `ref_1` with `'hello'` literal
/// as literals come before input references.
///
/// Finally, it removes TRUE conditions from filter nodes.
pub struct FilterNormalizationRule {}

impl SingleReplacementRule for FilterNormalizationRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Filter {
            conditions,
            input,
            correlation_id,
        } = query_graph.node(node_id)
        {
            let classes = equivalence_classes(query_graph, *input);
            let predicates = pulled_up_predicates(query_graph, *input);
            let mut replacement_map = to_replacement_map(&classes);
            let true_literal: ScalarExprRef = ScalarExpr::true_literal().into();
            // Anything that is already enforced by a descendent node, can be assumed
            // to be true.
            replacement_map.extend(
                predicates
                    .iter()
                    .map(|predicate| (predicate.clone(), true_literal.clone())),
            );
            // [A = 1, B = 1 OR A = 1] results in [A = 1, B = 1 OR TRUE] which will
            // be later reduced to just [A = 1].
            let mut new_conditions = conditions.clone();
            for i in 0..new_conditions.len() {
                let mut replacement_map = replacement_map.clone();
                replacement_map.extend(
                    new_conditions
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| i != *j)
                        .map(|(_, e)| (e.clone(), true_literal.clone())),
                );
                new_conditions[i] =
                    replace_sub_expressions_pre(&new_conditions[i], &replacement_map);
            }
            // TODO(asenac) reduce expressions after applying the replacements. All of the above
            // could be part of the reduction of AND expressions.
            let new_conditions = new_conditions
                .into_iter()
                .filter(|e| *e != true_literal)
                .sorted()
                .dedup()
                .collect_vec();

            if new_conditions != *conditions {
                return Some(query_graph.possibly_correlated_filter(
                    *input,
                    new_conditions,
                    *correlation_id,
                ));
            }
        }
        None
    }
}
