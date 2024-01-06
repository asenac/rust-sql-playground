use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::num_columns,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::ScalarExpr,
};

pub struct RemovePassthroughProjectRule {}

impl SingleReplacementRule for RemovePassthroughProjectRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if query_graph
            .get_parents(node_id)
            .map(|parents| parents.contains(&QueryGraph::ROOT_NODE_ID))
            .unwrap_or(false)
        {
            return None;
        }
        if let QueryNode::Project { outputs, input } = query_graph.node(node_id) {
            if query_graph.num_parents(node_id) > 0
                && outputs.len() == num_columns(query_graph, *input)
                && outputs
                    .iter()
                    .enumerate()
                    .all(|(id, expr)| match expr.as_ref() {
                        ScalarExpr::InputRef { index } => *index == id,
                        _ => false,
                    })
            {
                return Some(*input);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        query_graph::optimizer::SingleReplacementRule,
        query_graph::QueryGraph,
        scalar_expr::{BinaryOp, ScalarExpr, ScalarExprRef},
    };

    use super::RemovePassthroughProjectRule;

    #[test]
    fn test_remove_passthrough_project() {
        let mut query_graph = QueryGraph::new();
        let table_scan_id = query_graph.table_scan(0, 10);
        let filter_1: ScalarExprRef = ScalarExpr::input_ref(0)
            .binary(BinaryOp::Eq, ScalarExpr::input_ref(1).into())
            .into();
        let filter_id = query_graph.filter(table_scan_id, vec![filter_1.clone()]);
        let project_id_1 = query_graph.project(
            filter_id,
            (0..10).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );
        let project_id_2 = query_graph.project(
            filter_id,
            (0..5).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );
        let project_id_3 = query_graph.project(
            project_id_1,
            (0..10).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );
        let project_id_4 = query_graph.project(
            project_id_2,
            (0..10).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );

        let remove_passthrough_project = RemovePassthroughProjectRule {};
        assert!(remove_passthrough_project
            .apply(&mut query_graph, filter_id)
            .is_none());
        assert_eq!(
            remove_passthrough_project
                .apply(&mut query_graph, project_id_1)
                .unwrap(),
            filter_id
        );
        assert!(remove_passthrough_project
            .apply(&mut query_graph, project_id_2)
            .is_none());

        assert!(remove_passthrough_project
            .apply(&mut query_graph, project_id_3)
            .is_none());
        assert!(remove_passthrough_project
            .apply(&mut query_graph, project_id_4)
            .is_none());
    }
}
