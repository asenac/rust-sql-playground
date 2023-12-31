use std::{any::TypeId, rc::Rc};

use itertools::Itertools;

use crate::{
    query_graph::{visitor::QueryGraphPrePostVisitor, *},
    scalar_expr::{rewrite::*, BinaryOp, ScalarExpr},
    visitor_utils::PreOrderVisitationResult,
};

use super::num_columns;

/// Returns the predicates that are known to evaluate to true on top of the given node.
///
/// Example:
///
/// Given the following sub-plan:
///
/// ```txt
/// [1] Project [ref_1, ref_1, ref_2, ref_1 + ref_2, 'hello']
///    [2] Filter ref_1 > 10
///       [3] TableScan t
/// ```
///
/// It returns the following list of predicates:
///
/// ```txt
/// pulled_up_predicates(3) = []
/// pulled_up_predicates(2) = [ref_1 > 10]
/// pulled_up_predicates(1) = [ref_0 > 10, ref_0 raw= ref_1, ref_3 raw= ref_0 + ref_2, ref_4 raw= 'hello']
/// ```
///
/// Node that when CHECK constraints are supported `pulled_up_predicates(3)` will include
/// the CHECK constraints from table `t` if it had any.
///
/// Filter nodes append the predicates they enforced to the list of predicates that are
/// known to be true from its input.
///
/// The predicates known to be true from the input of a Project node need to be lifted
/// through the projection, to rewrite them in terms of the output of the projection.
/// Also, we can also infer some extra predicates when two columns project the same
/// expression or a projected expression can be computed from other expressions in the
/// projection. These extra predicates we infer use the non-null-rejecting comparison
/// operator `RawEq` to indicate that the equivalence is still true in the presence of
/// null values.
pub fn pulled_up_predicates(query_graph: &QueryGraph, node_id: NodeId) -> Rc<Vec<ScalarExprRef>> {
    PulledUpPredicates::predicates(query_graph, node_id)
}

/// Helper function to include predicate information when explaining the plan.
pub fn pulled_up_predicates_annotator(query_graph: &QueryGraph, node_id: NodeId) -> Option<String> {
    let predicates = pulled_up_predicates(query_graph, node_id);
    if !predicates.is_empty() {
        Some(format!(
            "Pulled Up Predicates: {}",
            predicates
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        ))
    } else {
        None
    }
}

struct PulledUpPredicates {}

impl PulledUpPredicates {
    fn predicates(query_graph: &QueryGraph, node_id: NodeId) -> Rc<Vec<ScalarExprRef>> {
        let mut visitor = PulledUpPredicates {};
        query_graph.visit_subgraph(&mut visitor, node_id);
        visitor.predicates_unchecked(query_graph, node_id)
    }

    fn predicates_unchecked(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<Vec<ScalarExprRef>> {
        query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .get(&Self::metadata_type_id())
            .unwrap()
            .downcast_ref::<Rc<Vec<ScalarExprRef>>>()
            .unwrap()
            .clone()
    }

    fn metadata_type_id() -> TypeId {
        TypeId::of::<Self>()
    }

    fn compute_predicates_for_node(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<Vec<ScalarExprRef>> {
        let mut predicates = Vec::new();
        match query_graph.node(node_id) {
            QueryNode::QueryRoot { input } => {
                if let Some(input) = input {
                    predicates.extend(
                        self.predicates_unchecked(query_graph, *input)
                            .iter()
                            .cloned(),
                    );
                }
            }
            QueryNode::Project { outputs, input } => {
                predicates.extend(
                    self.predicates_unchecked(query_graph, *input)
                        .iter()
                        .filter_map(|expr| lift_scalar_expr(expr, outputs)),
                );
                predicates.extend(outputs.iter().enumerate().filter_map(|(i, expr)| {
                    let proj = outputs
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| i != *j)
                        .map(|(j, expr)| (expr.clone(), j))
                        .collect::<HashMap<_, _>>();
                    lift_scalar_expr_2(expr, &proj).and_then(|rewritten_expr| {
                        Some(
                            ScalarExpr::input_ref(i)
                                .binary(BinaryOp::RawEq, rewritten_expr)
                                .into(),
                        )
                    })
                }));
                // consider [ref_0, concat(ref_0, ref_1), ref_1, concat(ref_0, ref_1)]
                // without this extract pass we miss to generate these two: [
                //   raw_eq(ref_1, concat(ref_0, ref_2)),
                //   raw_eq(ref_3, concat(ref_0, ref_2)),
                // ]
                // Note that this extra pass may generate predicates already covered
                // in the previous pass, so deduplication is needed afterwards
                predicates.extend(outputs.iter().enumerate().filter_map(|(i, expr)| {
                    let proj = outputs
                        .iter()
                        .enumerate()
                        .filter(|(j, other)| i != *j && **other != *expr)
                        .map(|(j, expr)| (expr.clone(), j))
                        .collect::<HashMap<_, _>>();
                    lift_scalar_expr_2(expr, &proj).and_then(|rewritten_expr| {
                        Some(
                            ScalarExpr::input_ref(i)
                                .binary(BinaryOp::RawEq, rewritten_expr)
                                .into(),
                        )
                    })
                }));
            }
            QueryNode::Filter { conditions, input } => {
                predicates.extend(
                    self.predicates_unchecked(query_graph, *input)
                        .iter()
                        .cloned(),
                );
                predicates.extend(conditions.iter().cloned());
            }
            QueryNode::TableScan { .. } => {}
            QueryNode::Join {
                join_type,
                conditions,
                left,
                right,
            } => {
                let left_size = num_columns(query_graph, *left);
                if let JoinType::Inner = join_type {
                    predicates.extend(conditions.iter().cloned());
                }
                let left_predicates_filter = match join_type {
                    JoinType::Semi | JoinType::Anti | JoinType::LeftOuter | JoinType::Inner => {
                        always_true
                    }
                    JoinType::RightOuter => is_raw_column_equivalence,
                    JoinType::FullOuter => always_false,
                };
                predicates.extend(
                    self.predicates_unchecked(query_graph, *left)
                        .iter()
                        .filter(|x| left_predicates_filter(x))
                        .cloned(),
                );
                let right_predicates_filter = match join_type {
                    JoinType::Semi | JoinType::Anti | JoinType::RightOuter | JoinType::Inner => {
                        always_true
                    }
                    JoinType::LeftOuter => is_raw_column_equivalence,
                    JoinType::FullOuter => always_false,
                };
                predicates.extend(
                    self.predicates_unchecked(query_graph, *right)
                        .iter()
                        .filter(|x| right_predicates_filter(x))
                        .map(|x| shift_right_input_refs(x, left_size)),
                );
            }
            QueryNode::Aggregate {
                group_key,
                aggregates,
                input,
            } => {
                // TODO(asenac) we could infer some predicates from the aggregate expressions
                let column_map = to_column_map_for_expr_lifting(group_key);
                predicates.extend(
                    self.predicates_unchecked(query_graph, *input)
                        .iter()
                        .filter_map(|expr| apply_column_map(expr, &column_map)),
                );
                // Equivalent aggregates
                let group_key_len = group_key.len();
                for (i, j) in aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, a)| {
                        (0..i).filter_map(move |j| {
                            if *a == aggregates[j] {
                                Some((i, j))
                            } else {
                                None
                            }
                        })
                    })
                    .flatten()
                {
                    predicates.push(
                        ScalarExpr::input_ref(group_key_len + j)
                            .binary(
                                BinaryOp::RawEq,
                                ScalarExpr::input_ref(group_key_len + i).into(),
                            )
                            .into(),
                    );
                }
            }
            QueryNode::Union { inputs } => {
                predicates.extend(
                    inputs
                        .iter()
                        .map(|input| self.predicates_unchecked(query_graph, *input))
                        .fold(None, |acc: Option<HashSet<ScalarExprRef>>, predicates| {
                            let set: HashSet<ScalarExprRef> = predicates.iter().cloned().collect();
                            acc.map(|common| common.intersection(&set).cloned().collect())
                                .or(Some(set))
                        })
                        .unwrap_or_else(|| HashSet::new())
                        .into_iter(),
                );
            }
            QueryNode::SubqueryRoot { input } => predicates.extend(
                self.predicates_unchecked(query_graph, *input)
                    .iter()
                    .cloned(),
            ),
            QueryNode::Apply {
                left,
                right,
                apply_type,
                ..
            } => {
                let left_size = num_columns(query_graph, *left);
                predicates.extend(
                    self.predicates_unchecked(query_graph, *left)
                        .iter()
                        .cloned(),
                );
                let right_predicates_filter = match apply_type {
                    ApplyType::Inner => always_true,
                    ApplyType::LeftOuter => is_raw_column_equivalence,
                };
                predicates.extend(
                    self.predicates_unchecked(query_graph, *right)
                        .iter()
                        .filter(|x| right_predicates_filter(x))
                        .map(|x| shift_right_input_refs(x, left_size)),
                );
            }
        };

        predicates
            .into_iter()
            // Remove literals: NULL is not TRUE, FALSE is not TRUE
            .filter(|e| !e.is_literal())
            .sorted()
            .dedup()
            .collect_vec()
            .into()
    }
}

impl QueryGraphPrePostVisitor for PulledUpPredicates {
    fn visit_pre(&mut self, query_graph: &QueryGraph, node_id: NodeId) -> PreOrderVisitationResult {
        if query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .contains_key(&Self::metadata_type_id())
        {
            PreOrderVisitationResult::DoNotVisitInputs
        } else {
            PreOrderVisitationResult::VisitInputs
        }
    }

    fn visit_post(&mut self, query_graph: &QueryGraph, node_id: NodeId) {
        if !query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .contains_key(&Self::metadata_type_id())
        {
            let predicates = self.compute_predicates_for_node(query_graph, node_id);
            query_graph
                .property_cache
                .borrow_mut()
                .node_bottom_up_properties(node_id)
                .insert(Self::metadata_type_id(), Box::new(predicates));
        }
    }
}

fn always_true(_: &ScalarExprRef) -> bool {
    true
}

fn always_false(_: &ScalarExprRef) -> bool {
    false
}

/// Whether the given predicate is a raw equality among columns.
/// These predicates can still be forwarded when coming from the non-preserving
/// side of outer joins.
fn is_raw_column_equivalence(predicate: &ScalarExprRef) -> bool {
    if let ScalarExpr::BinaryOp {
        op: BinaryOp::RawEq,
        left,
        right,
    } = predicate.as_ref()
    {
        if let ScalarExpr::InputRef { .. } = left.as_ref() {
            if let ScalarExpr::InputRef { .. } = right.as_ref() {
                return true;
            }
        }
    }
    false
}
