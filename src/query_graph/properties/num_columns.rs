use std::any::TypeId;

use crate::{
    query_graph::{visitor::QueryGraphPrePostVisitor, *},
    visitor_utils::PreOrderVisitationResult,
};

/// Returns the number of columns the given node projects, caching the result in the
/// `QueryGraph` metadata.
// TODO(asenac) this will eventually be renamed as `row_type`, returning the data types
// of the columns projected by the given node.
pub fn num_columns(query_graph: &QueryGraph, node_id: NodeId) -> usize {
    NumColumns::num_columns(query_graph, node_id)
}

/// Helper function to include column information when explaining the plan.
pub fn num_columns_annotator(query_graph: &QueryGraph, node_id: NodeId) -> Option<String> {
    let num_columns = num_columns(query_graph, node_id);
    Some(format!("Num Columns: {}", num_columns,))
}

struct NumColumns {}

impl NumColumns {
    fn num_columns(query_graph: &QueryGraph, node_id: NodeId) -> usize {
        let mut visitor = NumColumns {};
        query_graph.visit_subgraph(&mut visitor, node_id);
        visitor.num_columns_unchecked(query_graph, node_id)
    }

    fn num_columns_unchecked(&self, query_graph: &QueryGraph, node_id: NodeId) -> usize {
        query_graph
            .property_cache
            .borrow_mut()
            .node_bottom_up_properties(node_id)
            .get(&Self::metadata_type_id())
            .unwrap()
            .downcast_ref::<usize>()
            .unwrap()
            .clone()
    }

    fn metadata_type_id() -> TypeId {
        TypeId::of::<Self>()
    }

    fn compute_num_columns_for_node(&self, query_graph: &QueryGraph, node_id: NodeId) -> usize {
        match query_graph.node(node_id) {
            QueryNode::QueryRoot { input } => {
                if let Some(input) = input {
                    self.num_columns_unchecked(query_graph, *input)
                } else {
                    0
                }
            }
            QueryNode::Project { outputs, .. } => outputs.len(),
            QueryNode::Filter { input, .. } | QueryNode::SubqueryRoot { input } => {
                self.num_columns_unchecked(query_graph, *input)
            }
            QueryNode::TableScan { row_type, .. } => row_type.len(),
            QueryNode::Join {
                join_type,
                left,
                right,
                ..
            } => {
                let left_columns = if join_type.projects_columns_from_left() {
                    self.num_columns_unchecked(query_graph, *left)
                } else {
                    0
                };
                let right_columns = if join_type.projects_columns_from_right() {
                    self.num_columns_unchecked(query_graph, *right)
                } else {
                    0
                };
                left_columns + right_columns
            }
            QueryNode::Aggregate {
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
            QueryNode::Union { inputs } => {
                if inputs.is_empty() {
                    0
                } else {
                    self.num_columns_unchecked(query_graph, inputs[0])
                }
            }
            QueryNode::Apply { left, right, .. } => {
                let left_columns = self.num_columns_unchecked(query_graph, *left);
                let right_columns = self.num_columns_unchecked(query_graph, *right);
                left_columns + right_columns
            }
        }
    }
}

impl QueryGraphPrePostVisitor for NumColumns {
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
            let num_columns = self.compute_num_columns_for_node(query_graph, node_id);
            query_graph
                .property_cache
                .borrow_mut()
                .node_bottom_up_properties(node_id)
                .insert(Self::metadata_type_id(), Box::new(num_columns));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        query_graph::QueryGraph,
        scalar_expr::{BinaryOp, ScalarExpr, ScalarExprRef},
    };

    use super::num_columns;

    #[test]
    fn test_num_columns() {
        let mut query_graph = QueryGraph::new();
        let table_scan_id = query_graph.table_scan(0, 10);
        let filter_1: ScalarExprRef = ScalarExpr::input_ref(0)
            .binary(BinaryOp::Eq, ScalarExpr::input_ref(1).into())
            .into();
        let filter_id = query_graph.filter(table_scan_id, vec![filter_1.clone()]);
        let project_id = query_graph.project(
            filter_id,
            (0..5).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );

        assert_eq!(num_columns(&query_graph, table_scan_id), 10);
        assert_eq!(num_columns(&query_graph, filter_id), 10);
        assert_eq!(num_columns(&query_graph, project_id), 5);
    }
}
