use core::fmt;
use std::{any::TypeId, cmp::min, collections::HashSet, rc::Rc};

use itertools::Itertools;

use crate::{
    query_graph::{visitor::QueryGraphPrePostVisitor, JoinType, NodeId, QueryGraph, QueryNode},
    scalar_expr::{
        equivalence_class::{extract_equivalence_classes, find_class},
        rewrite::{lift_scalar_expr, shift_right_input_refs},
        ScalarExpr, ScalarExprRef,
    },
    value::Value,
    visitor_utils::PreOrderVisitationResult,
};

use super::num_columns;

/// Bounds associated with a key.
///
/// Contains the minimum and maximum number of distinct values that are guaranteed to be
/// produced for the key by some given relation.
///
/// An empty key indicates that the bounds are the minimum and the maximum number of rows
/// that will be produced.
#[derive(Clone)]
pub struct KeyBounds {
    pub key: Rc<Vec<ScalarExprRef>>,
    pub lower_bound: usize,
    /// The upper bound may be unknown indicating the key is unbounded.
    /// An upper bound equal to 1, indicates the key is a unique key.
    /// An upper bound equal to 0, indicates the relation is empty.
    pub upper_bound: Option<usize>,
}

impl fmt::Display for KeyBounds {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let key = self
            .key
            .iter()
            .map(|e| format!("{}", e))
            .collect::<Vec<String>>()
            .join(", ");
        write!(
            f,
            "[key: [{}], lower_bound: {}, upper_bound: {}]",
            key,
            self.lower_bound,
            if let Some(i) = self.upper_bound {
                i.to_string()
            } else {
                "unknown".to_string()
            }
        )
    }
}

/// Property computed in a bottom-up manner indicating the keys associated with
/// the given relation.
pub fn keys(query_graph: &QueryGraph, node_id: NodeId) -> Rc<Vec<KeyBounds>> {
    Keys::keys(query_graph, node_id)
}

/// Helper function to include keys information when explaining the plan.
pub fn keys_annotator(query_graph: &QueryGraph, node_id: NodeId) -> Option<String> {
    let keys = keys(query_graph, node_id);
    if !keys.is_empty() {
        Some(format!(
            "Keys: {}",
            keys.iter()
                .map(|e| { format!("{}", e) })
                .collect::<Vec<String>>()
                .join(", ")
        ))
    } else {
        None
    }
}

/// Extract the unique key if any among the keys of the given relation.
pub fn unique_key(query_graph: &QueryGraph, node_id: NodeId) -> Option<Rc<Vec<ScalarExprRef>>> {
    keys(query_graph, node_id).iter().find_map(|key| {
        if let Some(1) = key.upper_bound {
            Some(key.key.clone())
        } else {
            None
        }
    })
}

struct Keys {}

impl Keys {
    fn keys(query_graph: &QueryGraph, node_id: NodeId) -> Rc<Vec<KeyBounds>> {
        let mut visitor = Keys {};
        query_graph.visit_subgraph(&mut visitor, node_id);
        visitor.keys_unchecked(query_graph, node_id)
    }

    /// Warning: only use this method when the metadata is already guaranteed to be
    /// cached in the query graph.
    fn keys_unchecked(&self, query_graph: &QueryGraph, node_id: NodeId) -> Rc<Vec<KeyBounds>> {
        query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .get(&Self::metadata_type_id())
            .unwrap()
            .downcast_ref::<Rc<Vec<KeyBounds>>>()
            .unwrap()
            .clone()
    }

    /// Used to tag the metadata in `QueryGraph::metadata_cache`
    fn metadata_type_id() -> TypeId {
        TypeId::of::<Self>()
    }

    /// Invoked for every node within the current sub-graph whose information is not already
    /// cached in the query graph. When calling this method, the property for its inputs
    /// is guaranteed to be cached in the query graph, so `keys_unchecked` can be used.
    fn compute_keys_for_node(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<Vec<KeyBounds>> {
        let mut keys = Vec::new();
        match query_graph.node(node_id) {
            QueryNode::Project { outputs, input } => {
                let input_keys = self.keys_unchecked(query_graph, *input);
                // Lift the input keys through the projection expressions.
                keys.extend(input_keys.iter().filter_map(|key| {
                    // TODO(asenac) use try_for_each instead
                    let mut lifted_key = key
                        .key
                        .iter()
                        .filter_map(|e| lift_scalar_expr(e, outputs))
                        .collect::<Vec<_>>();
                    if lifted_key.len() == key.key.len() {
                        lifted_key.sort();
                        Some(KeyBounds {
                            key: Rc::new(lifted_key),
                            lower_bound: key.lower_bound,
                            upper_bound: key.upper_bound,
                        })
                    } else {
                        None
                    }
                }));
            }
            QueryNode::Filter { input, conditions } => {
                if conditions.iter().any(|c| match c.as_ref() {
                    // FALSE/NULL predicate -> empty relation
                    ScalarExpr::Literal(literal) => match literal.value {
                        Value::Null | Value::Bool(false) => true,
                        _ => false,
                    },
                    _ => false,
                }) {
                    keys.push(KeyBounds {
                        key: Rc::new(Vec::new()),
                        lower_bound: 0,
                        upper_bound: Some(0),
                    });
                } else {
                    keys.extend(self.keys_unchecked(query_graph, *input).iter().map(|key| {
                        KeyBounds {
                            key: key.key.clone(),
                            // By definition the filter relation is no longer guaranteed to produce
                            // the same number of rows as its input. All of them may be filtered out,
                            // hence, set the lower bound to 0.
                            lower_bound: 0,
                            // For sure, the filter won't produce any extra rows.
                            upper_bound: key.upper_bound,
                        }
                    }));
                }
            }
            QueryNode::TableScan { .. } => {
                // Here we could add the unique keys of the table if we had such information
                // in some system catalog.
            }
            QueryNode::Join {
                join_type,
                conditions,
                left,
                right,
            } => {
                let left_num_columns = num_columns(query_graph, *left);
                let classes = extract_equivalence_classes(conditions);
                let left_keys = self.keys_unchecked(query_graph, *left);
                // We need to rewrite the keys from the RHS to shift the input refs
                // by `left_num_columns` positions.
                let right_keys = self
                    .keys_unchecked(query_graph, *right)
                    .iter()
                    .map(|key| KeyBounds {
                        key: Rc::new(
                            key.key
                                .iter()
                                .map(|e| shift_right_input_refs(e, left_num_columns))
                                .collect(),
                        ),
                        lower_bound: key.lower_bound,
                        upper_bound: key.upper_bound,
                    })
                    .collect::<Vec<_>>();

                if let JoinType::FullOuter = join_type {
                    // TODO(asenac) the empty key can be known by computing the maximum
                } else if match join_type {
                    JoinType::Semi | JoinType::Anti => true,
                    _ => false,
                } {
                    keys.extend(left_keys.iter().map(|k| KeyBounds {
                        key: k.key.clone(),
                        lower_bound: 0,
                        upper_bound: k.upper_bound,
                    }));
                } else {
                    let preserve_left_keys = match join_type {
                        JoinType::Inner | JoinType::LeftOuter => true,
                        JoinType::RightOuter => false,
                        JoinType::Semi | JoinType::Anti | JoinType::FullOuter => unreachable!(),
                    };
                    let preserve_right_keys = match join_type {
                        JoinType::Inner | JoinType::RightOuter => true,
                        JoinType::LeftOuter => false,
                        JoinType::Semi | JoinType::Anti | JoinType::FullOuter => unreachable!(),
                    };
                    // Find pairs of keys from each side joined together.
                    for (left_key, right_key) in
                        left_keys.iter().cartesian_product(right_keys.iter())
                    {
                        let mut seen_right = HashSet::new();

                        let mut all_left_keys_joined = true;
                        for left_key_item in left_key.key.iter() {
                            if let Some(class_id) = find_class(&classes, left_key_item) {
                                let class = &classes[class_id];
                                seen_right.extend(right_key.key.iter().enumerate().filter_map(
                                    |(i, e)| {
                                        if class.members.contains(e) {
                                            Some(i)
                                        } else {
                                            None
                                        }
                                    },
                                ));
                                continue;
                            }
                            all_left_keys_joined = false;
                            break;
                        }
                        // 1. Check the keys are joined
                        // 2. Preserve keys if the other relation projects a single row at most
                        if (all_left_keys_joined && seen_right.len() == right_key.key.len())
                            || (left_key.key.is_empty() && left_key.upper_bound == Some(1))
                            || (right_key.key.is_empty() && right_key.upper_bound == Some(1))
                        {
                            let lower_bound = if conditions.is_empty() {
                                // It's a cross join
                                left_key.lower_bound * right_key.lower_bound
                            } else {
                                0 // Otherwise, the join may filter all the rows out.
                            };
                            let upper_bound = match (left_key.upper_bound, right_key.upper_bound) {
                                (Some(i), Some(j)) => Some(i * j),
                                _ => None,
                            };

                            if preserve_left_keys
                                && (!left_key.key.is_empty() || right_key.key.is_empty())
                            {
                                keys.push(KeyBounds {
                                    key: left_key.key.clone(),
                                    lower_bound,
                                    upper_bound,
                                });
                            }
                            if preserve_right_keys && !right_key.key.is_empty() {
                                keys.push(KeyBounds {
                                    key: right_key.key.clone(),
                                    lower_bound,
                                    upper_bound,
                                });
                            }
                        }
                    }
                }
                // TODO(asenac) remove duplicated keys
            }
            QueryNode::Aggregate { group_key, input } => keys.push(KeyBounds {
                // TODO(asenac) use input keys
                key: Rc::new(
                    group_key
                        .iter()
                        .enumerate()
                        .map(|(out_col, _)| ScalarExpr::input_ref(out_col).to_ref())
                        .collect(),
                ),
                lower_bound: if group_key.is_empty() { 1 } else { 0 },
                upper_bound: Some(1),
            }),
            QueryNode::Union { inputs } => {
                let input_keys = inputs
                    .iter()
                    .map(|input| self.keys_unchecked(query_graph, *input))
                    .collect::<Vec<_>>();
                if let Some(first) = input_keys.first() {
                    for mut key in first.iter().cloned() {
                        let mut all = true;
                        for other in input_keys.iter().skip(1) {
                            if let Some(other_key) = other.iter().find(|k| k.key == key.key) {
                                key.lower_bound = if key.key.is_empty() {
                                    key.lower_bound + other_key.lower_bound
                                } else {
                                    // Does this make sense?
                                    min(key.lower_bound, other_key.lower_bound)
                                };
                                key.upper_bound = match (key.upper_bound, other_key.upper_bound) {
                                    (Some(i), Some(j)) => Some(i + j),
                                    _ => None,
                                };
                            } else {
                                all = false;
                                break;
                            }
                        }
                        if all {
                            keys.push(key);
                        }
                    }
                }
            }
        };
        Rc::new(keys)
    }
}

impl QueryGraphPrePostVisitor for Keys {
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
            let keys = self.compute_keys_for_node(query_graph, node_id);
            query_graph
                .property_cache
                .borrow_mut()
                .node_bottom_up_properties(node_id)
                .insert(Self::metadata_type_id(), Box::new(keys));
        }
    }
}
