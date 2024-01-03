use std::{any::TypeId, collections::BTreeSet, rc::Rc};

use crate::{
    query_graph::{visitor::QueryGraphPrePostVisitor, NodeId, QueryGraph},
    visitor_utils::PreOrderVisitationResult,
};

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

/// Retrieve the subqueries within the given subgraph, but not the nested subqueries.
pub fn subgraph_subqueries(query_graph: &QueryGraph, node_id: NodeId) -> Rc<BTreeSet<NodeId>> {
    SubgraphSubqueries::subgraph_subqueries(query_graph, node_id)
}

struct SubgraphSubqueries {}

impl SubgraphSubqueries {
    fn subgraph_subqueries(query_graph: &QueryGraph, node_id: NodeId) -> Rc<BTreeSet<NodeId>> {
        let mut visitor = SubgraphSubqueries {};
        query_graph.visit_subgraph(&mut visitor, node_id);
        visitor.subgraph_subqueries_unchecked(query_graph, node_id)
    }

    fn subgraph_subqueries_unchecked(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<BTreeSet<NodeId>> {
        query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .get(&Self::metadata_type_id())
            .unwrap()
            .downcast_ref::<Rc<BTreeSet<NodeId>>>()
            .unwrap()
            .clone()
    }

    fn metadata_type_id() -> TypeId {
        TypeId::of::<Self>()
    }

    fn compute_property_for_node(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<BTreeSet<NodeId>> {
        let mut subqueries: BTreeSet<NodeId> = subqueries(query_graph, node_id).as_ref().clone();
        let query_node = query_graph.node(node_id);
        for input in 0..query_node.num_inputs() {
            let input_subqueries =
                self.subgraph_subqueries_unchecked(query_graph, query_node.get_input(input));
            subqueries.extend(input_subqueries.iter());
        }
        Rc::new(subqueries)
    }
}

impl QueryGraphPrePostVisitor for SubgraphSubqueries {
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
            let correlated_input_refs = self.compute_property_for_node(query_graph, node_id);
            query_graph
                .property_cache
                .borrow_mut()
                .node_bottom_up_properties(node_id)
                .insert(Self::metadata_type_id(), Box::new(correlated_input_refs));
        }
    }
}
