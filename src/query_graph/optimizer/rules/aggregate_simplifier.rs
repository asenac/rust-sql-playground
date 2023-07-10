use std::collections::BTreeSet;

use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::{equivalence_classes, num_columns},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        equivalence_class::{find_class, EquivalenceClasses},
        rewrite::{rewrite_expr_post, rewrite_expr_vec},
        ScalarExpr, ScalarExprRef, ToRef,
    },
};

/// Optimization rule that removes grouping key elements from an Aggregate node that
/// are either constants or that can be computed from the remaining ones.
///
/// Note that the last constant element cannot be removed if it's the only grouping
/// key element, as that would make the aggregate always return a row.
pub struct AggregateSimplifierRule {}

impl SingleReplacementRule for AggregateSimplifierRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Aggregate {
            group_key,
            aggregates,
            input,
        } = query_graph.node(node_id)
        {
            if group_key.len() < 2 {
                return None;
            }
            let classes = equivalence_classes(query_graph, node_id);
            if classes.is_empty() {
                return None;
            }
            if let Some((out_col, in_col, expr)) = find_redundant_key(group_key, &classes) {
                let num_columns = num_columns(query_graph, node_id);
                let new_aggregate = query_graph.add_node(QueryNode::Aggregate {
                    group_key: group_key
                        .iter()
                        .filter(|i| **i != in_col)
                        .cloned()
                        .collect(),
                    aggregates: aggregates.clone(),
                    input: *input,
                });
                let project = (0..num_columns)
                    .map(|i| {
                        if i != out_col {
                            ScalarExpr::input_ref(i).to_ref()
                        } else {
                            expr.clone()
                        }
                    })
                    .collect::<Vec<_>>();
                let project = update_project_after_pruning_column(project, out_col);
                return Some(query_graph.project(new_aggregate, project));
            }
        }
        None
    }
}

/// Finds an element of the grouping key that can be written in terms of the rest or
/// it's a constant.
///
/// Returns a triple with the position in which that element is projected by the Aggregate
/// operator, the input column and the expression it is equivalent to.
fn find_redundant_key(
    group_key: &BTreeSet<usize>,
    classes: &EquivalenceClasses,
) -> Option<(usize, usize, ScalarExprRef)> {
    group_key.iter().enumerate().find_map(|(out_col, in_col)| {
        let input_ref = ScalarExpr::input_ref(out_col).to_ref();
        if let Some(class_id) = find_class(&classes, &input_ref) {
            let class = &classes[class_id];
            // TODO(asenac) verify that other doesn't reference input_ref
            if let Some(other) = class.members.iter().find(|x| **x != input_ref).cloned() {
                return Some((out_col, *in_col, other));
            }
        }
        None
    })
}

/// Rewrites the expressions in `project` so that all input refs after the pruned column
/// are shifted one position.
fn update_project_after_pruning_column(
    project: Vec<ScalarExprRef>,
    pruned_col: usize,
) -> Vec<ScalarExprRef> {
    rewrite_expr_vec(&project, &mut |expr| {
        rewrite_expr_post(
            &mut |e: &ScalarExprRef| {
                if let ScalarExpr::InputRef { index } = e.as_ref() {
                    if *index > pruned_col {
                        return Some(ScalarExpr::input_ref(index - 1).to_ref());
                    }
                }
                None
            },
            expr,
        )
    })
}
