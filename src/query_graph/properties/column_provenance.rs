use std::{any::TypeId, rc::Rc};

use itertools::Itertools;

use crate::{
    query_graph::{visitor::QueryGraphPrePostVisitor, *},
    scalar_expr::{rewrite::lift_scalar_expr_2, ScalarExpr},
    visitor_utils::PreOrderVisitationResult,
};

use super::num_columns;

/// Information about how the node it is associated with relates to some source
/// node.
#[derive(Clone)]
pub struct ColumnProvenanceInfo {
    pub source_node: NodeId,
    /// The column expressions projected by the current node written in terms of the
    /// output of the source node.
    pub column_expressions: Rc<Vec<Option<ScalarExprRef>>>,
    /// Whether there is any filtering relation (Filter or Join) from the source node
    /// to the current node.
    pub filtered: bool,
    /// How to reach from the current node to the source node in inverse order
    pub inverse_path: Vec<usize>,
}

/// Returns a flattened version of the graph under the given node in pre-order.
///
/// This information can be used, for example, to detect whether two join operands
/// are joining information with the same provenance through a unique key, which
/// makes the join unnecessary.
pub fn column_provenance(
    query_graph: &QueryGraph,
    node_id: NodeId,
) -> Rc<Vec<Rc<ColumnProvenanceInfo>>> {
    ColumnProvenance::column_provenance(query_graph, node_id)
}

struct ColumnProvenance;

impl ColumnProvenance {
    fn column_provenance(
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<Vec<Rc<ColumnProvenanceInfo>>> {
        let mut visitor = ColumnProvenance {};
        query_graph.visit_subgraph(&mut visitor, node_id);
        visitor.column_provenance_unchecked(query_graph, node_id)
    }

    fn column_provenance_unchecked(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<Vec<Rc<ColumnProvenanceInfo>>> {
        query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .get(&Self::metadata_type_id())
            .unwrap()
            .downcast_ref::<Rc<Vec<Rc<ColumnProvenanceInfo>>>>()
            .unwrap()
            .clone()
    }

    fn metadata_type_id() -> TypeId {
        TypeId::of::<Self>()
    }

    fn compute_column_provenance_for_node(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<Vec<Rc<ColumnProvenanceInfo>>> {
        let mut prov: Vec<Rc<ColumnProvenanceInfo>> = Vec::new();

        // Default provenance
        let default_prov = ColumnProvenanceInfo {
            source_node: node_id,
            column_expressions: (0..num_columns(query_graph, node_id))
                .map(|i| Some(ScalarExpr::input_ref(i).into()))
                .collect_vec()
                .into(),
            filtered: false,
            inverse_path: Vec::new(),
        };
        prov.push(default_prov.into());

        match query_graph.node(node_id) {
            QueryNode::Filter {
                input,
                conditions: _,
                correlation_id: _,
            } => {
                let input_prov = self.column_provenance_unchecked(query_graph, *input);
                prov.extend(input_prov.iter().map(|prov_info| {
                    ColumnProvenanceInfo {
                        source_node: prov_info.source_node,
                        column_expressions: prov_info.column_expressions.clone(),
                        filtered: true,
                        inverse_path: prov_info
                            .inverse_path
                            .iter()
                            .cloned()
                            .chain(std::iter::once(0))
                            .collect_vec(),
                    }
                    .into()
                }));
            }
            QueryNode::Project {
                input,
                outputs,
                correlation_id: _,
            } => {
                let input_prov = self.column_provenance_unchecked(query_graph, *input);
                prov.extend(input_prov.iter().map(|prov_info| {
                    let lifting_map = prov_info
                        .column_expressions
                        .iter()
                        .enumerate()
                        .filter_map(|(i, expr)| {
                            if let Some(expr) = expr {
                                Some((expr.clone(), i))
                            } else {
                                None
                            }
                        })
                        .collect::<HashMap<_, _>>();
                    ColumnProvenanceInfo {
                        source_node: prov_info.source_node,
                        column_expressions: outputs
                            .iter()
                            .map(|expr| lift_scalar_expr_2(expr, &lifting_map))
                            .collect_vec()
                            .into(),
                        filtered: prov_info.filtered,
                        inverse_path: prov_info
                            .inverse_path
                            .iter()
                            .cloned()
                            .chain(std::iter::once(0))
                            .collect_vec(),
                    }
                    .into()
                }));
            }
            QueryNode::Aggregate {
                group_key,
                // For removing redundant joins we will need the aggregates to be part of
                // the column expressions, but we will need to include the key as well.
                // Perhaps by adding a wrapping expression in ExtendedScalarExpr
                aggregates,
                input,
            } => {
                let input_prov = self.column_provenance_unchecked(query_graph, *input);
                prov.extend(input_prov.iter().map(|prov_info| {
                    ColumnProvenanceInfo {
                        source_node: prov_info.source_node,
                        column_expressions: (0..group_key.len())
                            .map(|i| prov_info.column_expressions[i].clone())
                            .chain(aggregates.iter().map(|_| None))
                            .collect_vec()
                            .into(),
                        // The aggregate node doesn't filter, but reduces the input relation
                        filtered: prov_info.filtered,
                        inverse_path: prov_info
                            .inverse_path
                            .iter()
                            .cloned()
                            .chain(std::iter::once(0))
                            .collect_vec(),
                    }
                    .into()
                }));
            }
            QueryNode::Join {
                join_type,
                left,
                right,
                conditions: _,
            } => {
                let left_num_columns = if join_type.projects_columns_from_left() {
                    num_columns(query_graph, *left)
                } else {
                    0
                };
                let right_num_columns = if join_type.projects_columns_from_right() {
                    num_columns(query_graph, *right)
                } else {
                    0
                };
                if join_type.projects_columns_from_left() {
                    let input_prov = self.column_provenance_unchecked(query_graph, *left);
                    prov.extend(input_prov.iter().map(|prov_info| {
                        ColumnProvenanceInfo {
                            source_node: prov_info.source_node,
                            column_expressions: prov_info
                                .column_expressions
                                .iter()
                                .cloned()
                                .chain((0..right_num_columns).map(|_| None))
                                .collect_vec()
                                .into(),
                            filtered: false,
                            inverse_path: prov_info
                                .inverse_path
                                .iter()
                                .cloned()
                                .chain(std::iter::once(0))
                                .collect_vec(),
                        }
                        .into()
                    }));
                }
                if join_type.projects_columns_from_right() {
                    let input_prov = self.column_provenance_unchecked(query_graph, *right);
                    prov.extend(input_prov.iter().map(|prov_info| {
                        ColumnProvenanceInfo {
                            source_node: prov_info.source_node,
                            column_expressions: (0..left_num_columns)
                                .map(|_| None)
                                .chain(prov_info.column_expressions.iter().cloned())
                                .collect_vec()
                                .into(),
                            filtered: false,
                            inverse_path: prov_info
                                .inverse_path
                                .iter()
                                .cloned()
                                .chain(std::iter::once(1))
                                .collect_vec(),
                        }
                        .into()
                    }));
                }
            }
            _ => {}
        };

        prov.into()
    }
}

// The boilerplate once again
impl QueryGraphPrePostVisitor for ColumnProvenance {
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
            let column_provenance = self.compute_column_provenance_for_node(query_graph, node_id);
            query_graph
                .property_cache
                .borrow_mut()
                .node_bottom_up_properties(node_id)
                .insert(Self::metadata_type_id(), Box::new(column_provenance));
        }
    }
}
