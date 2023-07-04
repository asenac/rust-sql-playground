use std::collections::HashMap;

use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::{num_columns, pulled_up_predicates},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        equivalence_class::extract_equivalence_classes,
        rewrite::{
            rewrite_scalar_expr_pre, rewrite_scalar_expr_vec, shift_left_input_refs,
            shift_right_input_refs,
        },
        visitor::collect_input_dependencies,
        ScalarExprRef,
    },
};

/// Optimization rule that uses the equality predicates in a join node to propagate predicates that
/// are known to be true from each side in order to be enforced on the other side.
pub struct EqualityPropagationRule {}

impl SingleReplacementRule for EqualityPropagationRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Join {
            conditions,
            left,
            right,
        } = query_graph.node(node_id)
        {
            let left_num_columns = num_columns(&query_graph, *left);

            let (left_to_right, right_to_left) =
                Self::translation_maps(conditions, left_num_columns);

            let left_predicates = pulled_up_predicates(query_graph, *left);
            let right_predicates =
                rewrite_scalar_expr_vec(&pulled_up_predicates(query_graph, *right), &mut |e| {
                    shift_right_input_refs(e, left_num_columns)
                });

            let new_left_predicates = Self::propagate_predicates(
                &right_predicates,
                &right_to_left,
                &left_predicates,
                &|c| c < left_num_columns,
            );
            let new_right_predicates = Self::propagate_predicates(
                &left_predicates,
                &left_to_right,
                &right_predicates,
                &|c| c >= left_num_columns,
            );
            let new_right_predicates = new_right_predicates
                .iter()
                .map(|e| shift_left_input_refs(e, left_num_columns))
                .collect();

            if !left_predicates.is_empty() || !right_predicates.is_empty() {
                let join_conditions = conditions.clone();
                let left = *left;
                let right = *right;
                return Some(Self::build_new_join(
                    query_graph,
                    join_conditions,
                    left,
                    new_left_predicates,
                    right,
                    new_right_predicates,
                ));
            }
        }
        None
    }
}

impl EqualityPropagationRule {
    fn translation_maps(
        conditions: &Vec<ScalarExprRef>,
        left_num_columns: usize,
    ) -> (
        HashMap<ScalarExprRef, ScalarExprRef>,
        HashMap<ScalarExprRef, ScalarExprRef>,
    ) {
        let classes = extract_equivalence_classes(conditions);

        let mut left_to_right = HashMap::new();
        let mut right_to_left = HashMap::new();

        for class in classes.iter() {
            let mut left_expr: Option<ScalarExprRef> = None;
            let mut right_expr: Option<ScalarExprRef> = None;

            for member_expr in class.members.iter() {
                let dependencies = collect_input_dependencies(member_expr);
                let references_left = dependencies
                    .iter()
                    .any(|col_idx| *col_idx < left_num_columns);
                let references_right = dependencies
                    .iter()
                    .any(|col_idx| *col_idx >= left_num_columns);

                match (references_left, references_right) {
                    (true, false) if left_expr.is_none() => left_expr = Some(member_expr.clone()),
                    (false, true) if right_expr.is_none() => right_expr = Some(member_expr.clone()),
                    _ => {}
                };

                if left_expr.is_some() && right_expr.is_some() {
                    break;
                }
            }
            if left_expr.is_some() && right_expr.is_some() {
                left_to_right.insert(left_expr.clone().unwrap(), right_expr.clone().unwrap());
                right_to_left.insert(right_expr.unwrap(), left_expr.unwrap());
            }
        }

        (left_to_right, right_to_left)
    }

    /// Tries to rewrite the predicates in `predicates` using `translation_map` and validates that
    /// the rewritten predicate only references columns from the other side using `validate_input_ref`
    /// and that the resulting predicate is not already known.
    fn propagate_predicates<F>(
        predicates: &Vec<ScalarExprRef>,
        translation_map: &HashMap<ScalarExprRef, ScalarExprRef>,
        other_side_predicates: &Vec<ScalarExprRef>,
        validate_input_ref: &F,
    ) -> Vec<ScalarExprRef>
    where
        F: Fn(usize) -> bool,
    {
        let mut propagated_predicates = Vec::new();
        for predicate in predicates.iter() {
            let rewritten_predicate = rewrite_scalar_expr_pre(
                &mut |e| {
                    if let Some(other_side_expr) = translation_map.get(e) {
                        Ok(Some(other_side_expr.clone()))
                    } else {
                        Ok(None)
                    }
                },
                predicate,
            )
            .unwrap();

            if !other_side_predicates.contains(&rewritten_predicate)
                && collect_input_dependencies(&rewritten_predicate)
                    .iter()
                    .all(|col| validate_input_ref(*col))
            {
                propagated_predicates.push(rewritten_predicate);
            }
        }
        propagated_predicates
    }

    fn build_new_join(
        query_graph: &mut QueryGraph,
        join_conditions: Vec<ScalarExprRef>,
        left: NodeId,
        left_predicates: Vec<ScalarExprRef>,
        right: NodeId,
        right_predicates: Vec<ScalarExprRef>,
    ) -> NodeId {
        let mut new_left = left;
        let mut new_right = right;
        let join_conditions = join_conditions.clone();

        new_left = query_graph.filter(new_left, left_predicates);
        new_right = query_graph.filter(new_right, right_predicates);

        query_graph.join(new_left, new_right, join_conditions)
    }
}
