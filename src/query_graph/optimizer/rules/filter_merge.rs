use crate::query_graph::{
    optimizer::{OptRuleType, SingleReplacementRule},
    NodeId, QueryGraph, QueryNode,
};

/// Optimization rule that fuses two chained Filter nodes, concatenating the filter expressions
/// they contain.
pub struct FilterMergeRule {}

impl SingleReplacementRule for FilterMergeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Filter { conditions, input } = query_graph.node(node_id) {
            if let QueryNode::Filter {
                conditions: child_conditions,
                input: child_input,
            } = query_graph.node(*input)
            {
                return Some(
                    query_graph.filter(
                        *child_input,
                        conditions
                            .clone()
                            .into_iter()
                            .chain(child_conditions.clone().into_iter())
                            .collect(),
                    ),
                );
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        query_graph::QueryGraph,
        query_graph::{optimizer::SingleReplacementRule, QueryNode},
        scalar_expr::{BinaryOp, ScalarExpr},
    };

    use super::FilterMergeRule;

    #[test]
    fn test_filter_merge() {
        let mut query_graph = QueryGraph::new();
        let table_scan_id = query_graph.table_scan(0, 10);
        let project_id = query_graph.project(
            table_scan_id,
            (0..10).map(|i| ScalarExpr::input_ref(i).to_ref()).collect(),
        );

        let filter_1 = ScalarExpr::input_ref(0)
            .binary(BinaryOp::Eq, ScalarExpr::input_ref(1).to_ref())
            .to_ref();
        let filter_id_1 = query_graph.filter(project_id, vec![filter_1.clone()]);
        let filter_2 = ScalarExpr::input_ref(2)
            .binary(BinaryOp::Gt, ScalarExpr::input_ref(3).to_ref())
            .to_ref();
        let filter_id_2 = query_graph.filter(filter_id_1, vec![filter_2.clone()]);
        query_graph.set_entry_node(filter_id_2);

        let filter_merge_rule = FilterMergeRule {};
        assert!(filter_merge_rule
            .apply(&mut query_graph, project_id)
            .is_none());

        assert!(filter_merge_rule
            .apply(&mut query_graph, filter_id_1)
            .is_none());

        let merged_filter_id = filter_merge_rule
            .apply(&mut query_graph, filter_id_2)
            .unwrap();
        if let QueryNode::Filter { input, conditions } = query_graph.node(merged_filter_id) {
            assert_eq!(*input, project_id);
            assert_eq!(*conditions, vec![filter_2, filter_1]);
        } else {
            panic!();
        }
    }
}
