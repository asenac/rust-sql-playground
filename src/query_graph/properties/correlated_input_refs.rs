use std::{
    any::TypeId,
    collections::{BTreeSet, HashMap},
    rc::Rc,
};

use itertools::Itertools;

use crate::{
    query_graph::{
        visitor::QueryGraphPrePostVisitor, CorrelationId, NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{visitor::visit_expr_pre, ScalarExpr},
    visitor_utils::PreOrderVisitationResult,
};

struct CorrelatedInputRefsTag;

/// Returns a set with the correlated input refs the node contains, if any.
pub fn node_correlated_input_refs(
    query_graph: &QueryGraph,
    node_id: NodeId,
) -> Rc<HashMap<CorrelationId, BTreeSet<usize>>> {
    let type_id = TypeId::of::<CorrelatedInputRefsTag>();
    if let Some(cached) = query_graph
        .property_cache
        .borrow_mut()
        .single_node_properties(node_id)
        .get(&type_id)
    {
        return cached
            .downcast_ref::<Rc<HashMap<CorrelationId, BTreeSet<usize>>>>()
            .unwrap()
            .clone();
    }
    let mut correlated_cols = HashMap::new();
    let query_node = query_graph.node(node_id);
    query_node.visit_scalar_expr(&mut |expr| {
        visit_expr_pre(expr, &mut |curr_expr| {
            match curr_expr.as_ref() {
                ScalarExpr::CorrelatedInputRef {
                    correlation_id,
                    index,
                    ..
                } => {
                    correlated_cols
                        .entry(*correlation_id)
                        .or_insert_with(|| BTreeSet::new())
                        .insert(*index);
                }
                ScalarExpr::ExistsSubquery { subquery }
                | ScalarExpr::ScalarSubquery { subquery }
                | ScalarExpr::ScalarSubqueryCmp { subquery, .. } => {
                    let subquery_correlated_input_refs =
                        subgraph_correlated_input_refs(query_graph, subquery.root);
                    merge_correlated_maps(
                        subquery_correlated_input_refs
                            .iter()
                            // Remove the references that correspond to parameters of the subquery
                            .filter(|(correlation_id, _)| {
                                subquery
                                    .correlation
                                    .as_ref()
                                    .map(|correlation| {
                                        correlation.correlation_id != **correlation_id
                                    })
                                    .unwrap_or(false)
                            }),
                        &mut correlated_cols,
                    );
                }
                _ => (),
            }
            PreOrderVisitationResult::VisitInputs
        });
    });

    // Store the property in the cache
    let correlated_cols = Rc::new(correlated_cols);
    query_graph
        .property_cache
        .borrow_mut()
        .single_node_properties(node_id)
        .insert(type_id, Box::new(correlated_cols.clone()));
    correlated_cols
}

/// Returns a set with the correlated input refs in the given subplan that escape
/// the context of the subplan.
pub fn subgraph_correlated_input_refs(
    query_graph: &QueryGraph,
    node_id: NodeId,
) -> Rc<HashMap<CorrelationId, BTreeSet<usize>>> {
    SubgraphCorrelatedInputRefs::subgraph_correlated_input_refs(query_graph, node_id)
}

pub fn subgraph_correlated_input_refs_annotator(
    query_graph: &QueryGraph,
    node_id: NodeId,
) -> Option<String> {
    let correlated_cols = subgraph_correlated_input_refs(query_graph, node_id);
    let correlated_cols = correlated_cols
        .iter()
        .sorted()
        .map(|(correlation_id, columns)| {
            columns
                .iter()
                .map(|column| format!("cor_{}.ref_{}", correlation_id.0, column))
        })
        .flatten()
        .join(", ");
    if correlated_cols.is_empty() {
        None
    } else {
        Some(format!("Correlated References: {}", correlated_cols))
    }
}

struct SubgraphCorrelatedInputRefs {}

impl SubgraphCorrelatedInputRefs {
    fn subgraph_correlated_input_refs(
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<HashMap<CorrelationId, BTreeSet<usize>>> {
        let mut visitor = SubgraphCorrelatedInputRefs {};
        query_graph.visit_subgraph(&mut visitor, node_id);
        visitor.subgraph_correlated_input_refs_unchecked(query_graph, node_id)
    }

    fn subgraph_correlated_input_refs_unchecked(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<HashMap<CorrelationId, BTreeSet<usize>>> {
        query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .get(&Self::metadata_type_id())
            .unwrap()
            .downcast_ref::<Rc<HashMap<CorrelationId, BTreeSet<usize>>>>()
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
    ) -> Rc<HashMap<CorrelationId, BTreeSet<usize>>> {
        // The correlated input refs in the node itself...
        let mut correlated_cols: HashMap<CorrelationId, BTreeSet<usize>> =
            node_correlated_input_refs(query_graph, node_id)
                .as_ref()
                .clone();
        // ... and the ones under its child subgraphs, ...
        let query_node = query_graph.node(node_id);
        for input in 0..query_node.num_inputs() {
            let input_correlated_cols = self
                .subgraph_correlated_input_refs_unchecked(query_graph, query_node.get_input(input));
            merge_correlated_maps(input_correlated_cols.iter(), &mut correlated_cols);
        }
        //... but remove ones in the correlation scope the node defines.
        if let QueryNode::Apply { correlation, .. } = &query_node {
            correlated_cols.remove(&correlation.correlation_id);
        }
        Rc::new(correlated_cols)
    }
}

impl QueryGraphPrePostVisitor for SubgraphCorrelatedInputRefs {
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

fn merge_correlated_maps<'a, I>(src: I, dst: &mut HashMap<CorrelationId, BTreeSet<usize>>)
where
    I: Iterator<Item = (&'a CorrelationId, &'a BTreeSet<usize>)>,
{
    for (correlation_id, columns) in src {
        dst.entry(*correlation_id)
            .or_insert_with(|| BTreeSet::new())
            .extend(columns.iter());
    }
}
