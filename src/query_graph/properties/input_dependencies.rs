use std::{any::TypeId, collections::HashSet, rc::Rc};

use crate::{
    query_graph::{NodeId, QueryGraph, QueryNode},
    scalar_expr::visitor::store_input_dependencies,
};

use super::num_columns;

struct InputDependenciesTag;

pub fn input_dependencies(query_graph: &QueryGraph, node_id: NodeId) -> Rc<HashSet<usize>> {
    let type_id = TypeId::of::<InputDependenciesTag>();
    if let Some(cached) = query_graph
        .property_cache
        .borrow_mut()
        .single_node_properties(node_id)
        .get(&type_id)
    {
        return cached.downcast_ref::<Rc<HashSet<usize>>>().unwrap().clone();
    }
    let mut dependencies = HashSet::new();
    match query_graph.node(node_id) {
        QueryNode::QueryRoot { input } => {
            if let Some(input) = input {
                dependencies.extend(0..num_columns(query_graph, *input));
            }
        }
        QueryNode::Project { outputs: exprs, .. }
        | QueryNode::Join {
            conditions: exprs, ..
        } => exprs
            .iter()
            .for_each(|e| store_input_dependencies(e, &mut dependencies)),
        QueryNode::TableScan { .. } => {}
        QueryNode::Aggregate {
            group_key,
            aggregates,
            ..
        } => {
            dependencies.extend(group_key.iter());
            for aggregate in aggregates.iter() {
                dependencies.extend(aggregate.operands.iter());
            }
        }
        QueryNode::Filter {
            conditions: exprs, ..
        } => exprs
            .iter()
            .for_each(|e| store_input_dependencies(e, &mut dependencies)),
        QueryNode::Union { .. } | QueryNode::SubqueryRoot { .. } | QueryNode::Apply { .. } => {
            dependencies.extend(0..num_columns(query_graph, node_id))
        }
    }
    let dependencies = Rc::new(dependencies);
    query_graph
        .property_cache
        .borrow_mut()
        .single_node_properties(node_id)
        .insert(type_id, Box::new(dependencies.clone()));
    dependencies
}
