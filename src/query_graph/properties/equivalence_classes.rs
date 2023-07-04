use std::{any::TypeId, rc::Rc};

use crate::{
    query_graph::{NodeId, QueryGraph},
    scalar_expr::equivalence_class::{extract_equivalence_classes, EquivalenceClasses},
};

use super::pulled_up_predicates;

/// Property derived from the pulled up predicates.
pub fn equivalence_classes(query_graph: &QueryGraph, node_id: NodeId) -> Rc<EquivalenceClasses> {
    let type_id = TypeId::of::<Rc<EquivalenceClasses>>();
    if let Some(cached) = query_graph
        .property_cache
        .borrow_mut()
        .node_bottom_up_properties(node_id)
        .get(&type_id)
    {
        return cached
            .downcast_ref::<Rc<EquivalenceClasses>>()
            .unwrap()
            .clone();
    }
    // Do not use an else branch since we need to release the borrow above
    // in order to compute the pulled up predicates
    let predicates = pulled_up_predicates(query_graph, node_id);
    let classes = Rc::new(extract_equivalence_classes(&predicates));
    query_graph
        .property_cache
        .borrow_mut()
        .node_bottom_up_properties(node_id)
        .insert(type_id, Box::new(classes.clone()));
    classes
}
