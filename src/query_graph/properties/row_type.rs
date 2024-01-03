use std::{any::TypeId, rc::Rc};

use itertools::Itertools;

use crate::{
    data_type::DataType,
    query_graph::{visitor::QueryGraphPrePostVisitor, *},
    visitor_utils::PreOrderVisitationResult,
};

/// Returns the row type of the given node.
pub fn row_type(query_graph: &QueryGraph, node_id: NodeId) -> Rc<Vec<DataType>> {
    RowType::row_type(query_graph, node_id)
}

/// Given a join node returns the row type of the cross product of its operands.
/// This is the row type the expressions in the join refer to.
pub fn cross_product_row_type(query_graph: &QueryGraph, node_id: NodeId) -> Option<Vec<DataType>> {
    if let QueryNode::Join {
        join_type: _,
        conditions: _,
        left,
        right,
    } = query_graph.node(node_id)
    {
        Some(
            row_type(query_graph, *left)
                .iter()
                .chain(row_type(query_graph, *right).iter())
                .cloned()
                .collect_vec(),
        )
    } else {
        None
    }
}

/// Helper function to include row type information when explaining the plan.
pub fn row_type_annotator(query_graph: &QueryGraph, node_id: NodeId) -> Option<String> {
    let row_type = row_type(query_graph, node_id);
    Some(format!(
        "Row Type: {}",
        row_type
            .iter()
            .map(|data_type| format!("{}", data_type))
            .collect::<Vec<_>>()
            .join(", "),
    ))
}

struct RowType {}

impl RowType {
    fn row_type(query_graph: &QueryGraph, node_id: NodeId) -> Rc<Vec<DataType>> {
        let mut visitor = RowType {};
        query_graph.visit_subgraph(&mut visitor, node_id);
        visitor.row_type_unchecked(query_graph, node_id)
    }

    fn row_type_unchecked(&self, query_graph: &QueryGraph, node_id: NodeId) -> Rc<Vec<DataType>> {
        query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .get(&Self::metadata_type_id())
            .unwrap()
            .downcast_ref::<Rc<Vec<DataType>>>()
            .unwrap()
            .clone()
    }

    fn metadata_type_id() -> TypeId {
        TypeId::of::<Self>()
    }

    fn compute_row_type_for_node(
        &self,
        query_graph: &QueryGraph,
        node_id: NodeId,
    ) -> Rc<Vec<DataType>> {
        match query_graph.node(node_id) {
            QueryNode::Project { outputs, input } => {
                let input_row_type = self.row_type_unchecked(query_graph, *input);
                outputs
                    .iter()
                    .map(|e| e.data_type(query_graph, &input_row_type[..]))
                    .collect_vec()
                    .into()
            }
            QueryNode::Filter { input, .. } | QueryNode::SubqueryRoot { input } => {
                self.row_type_unchecked(query_graph, *input)
            }
            QueryNode::TableScan { row_type, .. } => row_type.clone(),
            QueryNode::Join {
                join_type,
                left,
                right,
                ..
            } => match join_type {
                JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter => self
                    .row_type_unchecked(query_graph, *left)
                    .iter()
                    .chain(self.row_type_unchecked(query_graph, *right).iter())
                    .cloned()
                    .collect_vec()
                    .into(),
                JoinType::Semi | JoinType::Anti => self.row_type_unchecked(query_graph, *left),
            },
            QueryNode::Aggregate {
                group_key,
                aggregates,
                input,
            } => {
                let input_row_type = self.row_type_unchecked(query_graph, *input);
                group_key
                    .iter()
                    .map(|e| input_row_type[*e].clone())
                    .chain(aggregates.iter().map(|agg| agg.data_type(&*input_row_type)))
                    .collect_vec()
                    .into()
            }
            QueryNode::Union { inputs } => {
                if inputs.is_empty() {
                    Default::default()
                } else {
                    self.row_type_unchecked(query_graph, inputs[0])
                }
            }
            QueryNode::Apply { left, right, .. } => self
                .row_type_unchecked(query_graph, *left)
                .iter()
                .chain(self.row_type_unchecked(query_graph, *right).iter())
                .cloned()
                .collect_vec()
                .into(),
        }
    }
}

impl QueryGraphPrePostVisitor for RowType {
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
            let row_type = self.compute_row_type_for_node(query_graph, node_id);
            query_graph
                .property_cache
                .borrow_mut()
                .node_bottom_up_properties(node_id)
                .insert(Self::metadata_type_id(), Box::new(row_type));
        }
    }
}
