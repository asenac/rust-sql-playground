//! Module containing utilities used by several optimization rules.

use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use crate::query_graph::{NodeId, QueryGraph, QueryNode};
use crate::scalar_expr::rewrite::{apply_column_map, rewrite_expr_vec};
use crate::scalar_expr::visitor::store_input_dependencies;
use crate::visitor_utils::PreOrderVisitationResult;
use crate::{
    query_graph::{properties::num_columns, visitor::QueryGraphPrePostVisitor},
    scalar_expr::{AggregateExpr, ScalarExprRef},
};

/// Get the filters that are common to all parents on the given node.
/// None if there is at least a parent that is not a filter node or
/// if there are no common predicates to all filter parents.
pub(crate) fn common_parent_filters(
    query_graph: &QueryGraph,
    node_id: NodeId,
) -> Option<Vec<ScalarExprRef>> {
    if let Some(conditions) = query_graph.get_parents(node_id).map(|parents| {
        parents
            .iter()
            .map(|parent| {
                if let QueryNode::Filter { conditions, .. } = query_graph.node(*parent) {
                    return conditions.clone();
                }
                Vec::new()
            })
            .fold(None, |acc: Option<HashSet<ScalarExprRef>>, predicates| {
                let set: HashSet<ScalarExprRef> = predicates.iter().cloned().collect();
                acc.map(|common| common.intersection(&set).cloned().collect())
                    .or(Some(set))
            })
            .unwrap_or_else(|| HashSet::new())
            .into_iter()
            .sorted()
            .collect::<Vec<_>>()
    }) {
        if !conditions.is_empty() {
            return Some(conditions);
        }
    }
    None
}

/// If all the parents of the given node are pruning nodes or filters leading to a pruning node,
/// computes the superset of columns from the given node required by them, and return a column
/// map for pruning the given node if not all columns are required.
///
/// Pruning nodes are those that do not forward all the columns from its input, namely Project
/// and Aggregate.
pub(crate) fn required_columns_from_parents(
    query_graph: &QueryGraph,
    node_id: NodeId,
) -> Option<HashSet<usize>> {
    let mut required_columns = HashSet::new();
    if let Some(Ok(())) = query_graph.get_parents(node_id).map(|parents| {
        // Check that every parent is a chain of 0 or more filters leading to a projection
        parents.iter().try_for_each(|parent| {
            let mut only_filter_and_project = true;
            query_graph.visit_subgraph_upwards_pre(
                &mut |query_graph, node_id| match query_graph.node(node_id) {
                    QueryNode::Aggregate {
                        group_key,
                        aggregates,
                        ..
                    } => {
                        required_columns.extend(group_key);
                        for aggregate in aggregates.iter() {
                            required_columns.extend(aggregate.operands.iter());
                        }
                        PreOrderVisitationResult::DoNotVisitInputs
                    }
                    QueryNode::Project { outputs, .. } => {
                        for proj_expr in outputs.iter() {
                            store_input_dependencies(proj_expr, &mut required_columns);
                        }
                        PreOrderVisitationResult::DoNotVisitInputs
                    }
                    QueryNode::Filter { conditions, .. } => {
                        for filter_expr in conditions.iter() {
                            store_input_dependencies(filter_expr, &mut required_columns);
                        }
                        PreOrderVisitationResult::VisitInputs
                    }
                    _ => {
                        only_filter_and_project = false;
                        PreOrderVisitationResult::Abort
                    }
                },
                *parent,
            );
            if only_filter_and_project {
                Ok(())
            } else {
                Err(())
            }
        })
    }) {
        let num_columns = num_columns(query_graph, node_id);
        if num_columns != required_columns.len() {
            return Some(required_columns);
        }
    }
    None
}

/// Return a column map for update expressions after pruning the input to only project
/// the given required columns.
pub(crate) fn required_columns_to_column_map(
    required_columns: &HashSet<usize>,
) -> HashMap<usize, usize> {
    required_columns
        .iter()
        .sorted()
        .enumerate()
        .map(|(i, j)| (*j, i))
        .collect::<HashMap<_, _>>()
}

/// Utility for most pruning rules. All the parents of the given node are expected to be
/// pruning nodes, or chains of 0 or more Filters leading to a pruning node, and the given
/// column map is required to contain all the columns required by any parent projection.
/// Clones all the filter nodes over `node_id` adapting their expressions to catch up
/// with the pruning of columns, until a pruning node is reached.
///
/// Returns a list with the replacement for the pruning nodes on top of `node_id` replacing
/// them with projections on top of `replacement_node_id`.
///
/// Pruning nodes are those that do not forward all the columns from its input, namely Project
/// and Aggregate.
pub(crate) fn apply_map_to_parents_and_replace_input(
    query_graph: &mut QueryGraph,
    node_id: NodeId,
    column_map: &HashMap<usize, usize>,
    replacement_node_id: NodeId,
) -> Vec<(NodeId, NodeId)> {
    struct Visitor {
        stack: Vec<NodeId>,
        paths: Vec<Vec<NodeId>>,
    }

    impl Visitor {
        fn new() -> Self {
            Self {
                stack: Vec::new(),
                paths: Vec::new(),
            }
        }
    }

    impl QueryGraphPrePostVisitor for Visitor {
        fn visit_pre(
            &mut self,
            query_graph: &QueryGraph,
            node_id: NodeId,
        ) -> PreOrderVisitationResult {
            self.stack.push(node_id);
            match query_graph.node(node_id) {
                QueryNode::Project { .. } | QueryNode::Aggregate { .. } => {
                    // The projection has been reached, stop here
                    self.paths.push(self.stack.clone());
                    PreOrderVisitationResult::DoNotVisitInputs
                }
                QueryNode::Filter { .. } => PreOrderVisitationResult::VisitInputs,
                _ => panic!("expected aggregate, project or filter node"),
            }
        }

        fn visit_post(&mut self, _: &QueryGraph, _: NodeId) {
            self.stack.pop();
        }
    }

    // Collect all the distinct paths leading projections from the node
    // being replaced
    let mut visitor = Visitor::new();
    if let Some(parents) = query_graph.get_parents(node_id) {
        for parent in parents.iter() {
            query_graph.visit_subgraph_upwards(&mut visitor, *parent);
        }
    }

    let mut replacements = HashMap::new();
    replacements.insert(node_id, replacement_node_id);

    // Clone all the filter nodes in the way of the projections, adapting
    // their expressions
    for path in visitor.paths.iter() {
        for current_node_id in path.iter() {
            if let None = replacements.get(current_node_id) {
                let new_node = match query_graph.node(*current_node_id) {
                    QueryNode::Aggregate {
                        group_key,
                        aggregates,
                        input,
                    } => {
                        let new_input = *replacements.get(input).unwrap();
                        let new_aggregates = aggregates
                            .iter()
                            .map(|k| {
                                AggregateExpr {
                                    op: k.op.clone(),
                                    operands: k
                                        .operands
                                        .iter()
                                        .map(|e| *column_map.get(e).unwrap())
                                        .collect_vec(),
                                }
                                .into()
                            })
                            .collect_vec();
                        let new_agg = query_graph.add_node(QueryNode::Aggregate {
                            group_key: group_key
                                .iter()
                                .map(|k| column_map.get(k).unwrap().to_owned())
                                .collect(),
                            aggregates: new_aggregates,
                            input: new_input,
                        });
                        new_agg
                    }
                    QueryNode::Project { outputs, input } => {
                        let new_input = *replacements.get(input).unwrap();
                        let new_proj = query_graph.project(
                            new_input,
                            rewrite_expr_vec(outputs, &mut |e| {
                                apply_column_map(e, column_map).unwrap()
                            }),
                        );
                        new_proj
                    }
                    QueryNode::Filter { conditions, input } => {
                        let new_input = *replacements.get(input).unwrap();
                        let new_filter = query_graph.filter(
                            new_input,
                            rewrite_expr_vec(conditions, &mut |e| {
                                apply_column_map(e, column_map).unwrap()
                            }),
                        );
                        new_filter
                    }
                    _ => panic!("expected aggregate, project or filter node"),
                };
                replacements.insert(*current_node_id, new_node);
            };
        }
    }

    // The last node of every path is a projection. We only need to replace
    // them, as the nodes leading to them are replaced as a side effect of
    // replacing them.
    visitor
        .paths
        .iter()
        .map(|path| path.last().unwrap())
        .map(|proj_id| (*proj_id, *replacements.get(proj_id).unwrap()))
        .collect::<Vec<_>>()
}

pub(crate) fn sort_projection(
    project: &Vec<ScalarExprRef>,
) -> Option<(Vec<usize>, Vec<ScalarExprRef>)> {
    let (reorder_map, sorted_project): (Vec<usize>, Vec<ScalarExprRef>) = project
        .iter()
        .cloned()
        .enumerate()
        .sorted_by_key(|(_, e)| e.clone())
        .unzip();
    if reorder_map.iter().enumerate().any(|(i, j)| i != *j) {
        Some((reorder_map, sorted_project))
    } else {
        None
    }
}
