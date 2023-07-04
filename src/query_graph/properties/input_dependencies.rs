use std::collections::HashSet;

use crate::{
    query_graph::{NodeId, QueryGraph, QueryNode},
    scalar_expr::visitor::store_input_dependencies,
};

use super::num_columns;

pub fn input_dependencies(query_graph: &QueryGraph, node_id: NodeId) -> HashSet<usize> {
    let mut dependencies = HashSet::new();
    match query_graph.node(node_id) {
        QueryNode::Project { outputs: exprs, .. }
        | QueryNode::Filter {
            conditions: exprs, ..
        }
        | QueryNode::Join {
            conditions: exprs, ..
        } => exprs
            .iter()
            .for_each(|e| store_input_dependencies(e, &mut dependencies)),
        QueryNode::TableScan { .. } => {}
        QueryNode::Aggregate { group_key, .. } => dependencies.extend(group_key.iter()),
        QueryNode::Union { .. } => dependencies.extend(0..num_columns(query_graph, node_id)),
    }
    dependencies
}
