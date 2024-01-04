use crate::{
    query_graph::{
        optimizer::{
            correlation_utils::update_correlated_references_in_subqueries, OptRuleType,
            SingleReplacementRule,
        },
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        rewrite::rewrite_expr_post,
        rewrite_utils::{apply_subquery_map, update_correlation_id},
    },
};

/// Optimization rule that fuses two chained Filter nodes, concatenating the filter expressions
/// they contain.
pub struct FilterMergeRule {}

impl SingleReplacementRule for FilterMergeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Filter {
            conditions,
            input,
            correlation_id,
        } = query_graph.node(node_id)
        {
            if let QueryNode::Filter {
                conditions: child_conditions,
                input: child_input,
                correlation_id: child_correlation_id,
            } = query_graph.node(*input)
            {
                let mut conditions = conditions.clone();
                let parent_node_conditions_len = conditions.len();

                let new_correlation_id = child_correlation_id.or(*correlation_id);
                conditions.extend(child_conditions.clone().into_iter());
                let new_input = *child_input;

                // If both filters may contain correlated subqueries, we need to make
                // them use a single correlation ID. The subqueries and conditions from
                // the outer filter node are rewritten to make them refer to the correlation
                // ID of the inner filter node.
                if correlation_id.is_some() && child_correlation_id.is_some() {
                    let correlation_id = correlation_id.unwrap();
                    let new_correlation_id = new_correlation_id.unwrap();
                    let subquery_map = update_correlated_references_in_subqueries(
                        query_graph,
                        node_id,
                        correlation_id,
                        |e| update_correlation_id(e, correlation_id, new_correlation_id),
                    );
                    conditions
                        .iter_mut()
                        .take(parent_node_conditions_len)
                        .for_each(|e| {
                            *e = rewrite_expr_post(&mut |e| apply_subquery_map(e, &subquery_map), e)
                        });
                }
                return Some(query_graph.possibly_correlated_filter(
                    new_input,
                    conditions,
                    new_correlation_id,
                ));
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
        scalar_expr::{BinaryOp, ScalarExpr, ScalarExprRef},
    };

    use super::FilterMergeRule;

    #[test]
    fn test_filter_merge() {
        let mut query_graph = QueryGraph::new();
        let table_scan_id = query_graph.table_scan(0, 10);
        let project_id = query_graph.project(
            table_scan_id,
            (0..10).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );

        let filter_1: ScalarExprRef = ScalarExpr::input_ref(0)
            .binary(BinaryOp::Eq, ScalarExpr::input_ref(1).into())
            .into();
        let filter_id_1 = query_graph.filter(project_id, vec![filter_1.clone()]);
        let filter_2: ScalarExprRef = ScalarExpr::input_ref(2)
            .binary(BinaryOp::Gt, ScalarExpr::input_ref(3).into())
            .into();
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
        if let QueryNode::Filter {
            input, conditions, ..
        } = query_graph.node(merged_filter_id)
        {
            assert_eq!(*input, project_id);
            assert_eq!(*conditions, vec![filter_2, filter_1]);
        } else {
            panic!();
        }
    }
}
