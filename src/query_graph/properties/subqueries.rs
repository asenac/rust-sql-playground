use std::{any::TypeId, collections::BTreeSet, rc::Rc};

use crate::query_graph::{NodeId, QueryGraph};

struct SubqueryPropertyTag;

/// Returns a set with the subqueries the node contains, if any.
pub fn subqueries(query_graph: &QueryGraph, node_id: NodeId) -> Rc<BTreeSet<NodeId>> {
    let type_id = TypeId::of::<SubqueryPropertyTag>();
    if let Some(cached) = query_graph
        .property_cache
        .borrow_mut()
        .single_node_properties(node_id)
        .get(&type_id)
    {
        return cached
            .downcast_ref::<Rc<BTreeSet<NodeId>>>()
            .unwrap()
            .clone();
    }
    let subqueries = Rc::new(query_graph.node(node_id).collect_subqueries());
    query_graph
        .property_cache
        .borrow_mut()
        .single_node_properties(node_id)
        .insert(type_id, Box::new(subqueries.clone()));
    subqueries
}
