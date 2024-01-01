use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::unique_key,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::ScalarExpr,
};

pub struct AggregateRemoveRule {}

impl SingleReplacementRule for AggregateRemoveRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::Always
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Aggregate {
            group_key,
            aggregates,
            input,
        } = query_graph.node(node_id)
        {
            if !group_key.is_empty() {
                if let Some(input_unique_key) = unique_key(query_graph, *input) {
                    let group_key_expr = group_key
                        .iter()
                        .map(|col| ScalarExpr::input_ref(*col).into())
                        .collect::<Vec<_>>();
                    if input_unique_key.iter().all(|e| group_key_expr.contains(e)) {
                        let mut values = group_key_expr;
                        values.extend(
                            aggregates
                                .iter()
                                .map(|aggregate| aggregate.on_unique_tuple()),
                        );
                        return Some(query_graph.project(*input, values));
                    }
                }
            }
        }
        None
    }
}
