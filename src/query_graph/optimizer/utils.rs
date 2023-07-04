//! Module containing utilities used by several optimization rules.

use std::collections::{HashMap, HashSet};

use itertools::Itertools;

use super::*;
use crate::{
    query_graph::{properties::num_columns, QueryNode},
    scalar_expr::{
        rewrite::{apply_column_map, rewrite_scalar_expr_vec},
        visitor::store_input_dependencies,
        ScalarExprRef,
    },
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
                    conditions.clone()
                } else {
                    Vec::new()
                }
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

/// If all the parents of the given node are projections, computes the superset of columns
/// from the given node required by them, and return a column map for pruning the given node
/// if not all columns are required.
pub(crate) fn required_columns_from_parent_projections(
    query_graph: &QueryGraph,
    node_id: NodeId,
) -> Option<HashMap<usize, usize>> {
    let mut required_columns = HashSet::new();
    if let Some(Ok(())) = query_graph.get_parents(node_id).map(|parents| {
        parents.iter().try_for_each(|parent| {
            if let QueryNode::Project { outputs, .. } = query_graph.node(*parent) {
                for proj_expr in outputs.iter() {
                    store_input_dependencies(proj_expr, &mut required_columns);
                }
                Ok(())
            } else {
                Err(())
            }
        })
    }) {
        let num_columns = num_columns(query_graph, node_id);
        if num_columns != required_columns.len() {
            return Some(required_columns_to_column_map(&required_columns));
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
/// Project nodes and the given column map is required to contain all the columns required
/// by any parent projection.
pub(crate) fn apply_map_to_parent_projections_and_replace_input(
    query_graph: &mut QueryGraph,
    node_id: NodeId,
    column_map: &HashMap<usize, usize>,
    replacement_node_id: NodeId,
) -> Vec<(usize, usize)> {
    let mut replacements = Vec::new();
    let parents = query_graph.get_parents(node_id).unwrap().clone();
    for parent in parents {
        if let QueryNode::Project { outputs, input } = query_graph.node(parent) {
            assert_eq!(*input, node_id);
            let new_proj = query_graph.project(
                replacement_node_id,
                rewrite_scalar_expr_vec(outputs, &mut |e| apply_column_map(e, column_map).unwrap()),
            );
            replacements.push((parent, new_proj));
        } else {
            panic!("expected projection node");
        }
    }
    replacements
}
