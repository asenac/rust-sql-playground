use std::collections::HashMap;

use crate::{
    query_graph::{
        optimizer::{utils::common_parent_filters, OptRuleType, SingleReplacementRule},
        properties::pulled_up_predicates,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::rewrite::{apply_column_map, to_column_map_for_expr_push_down},
};

pub struct FilterAggregateTransposeRule {}

impl SingleReplacementRule for FilterAggregateTransposeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Aggregate {
            group_key,
            aggregates,
            input: agg_input,
        } = query_graph.node(node_id)
        {
            if let Some(conditions) = common_parent_filters(query_graph, node_id) {
                let column_map = to_column_map_for_expr_push_down(group_key);
                let known_predicates = pulled_up_predicates(query_graph, *agg_input);
                let pushable_conditions = conditions
                    .iter()
                    .enumerate()
                    .filter_map(|(i, expr)| {
                        if let Some(condition) = apply_column_map(expr, &column_map) {
                            if !known_predicates.contains(&condition) {
                                return Some((i, condition));
                            }
                        }
                        None
                    })
                    .collect::<HashMap<_, _>>();

                if !pushable_conditions.is_empty() {
                    let new_group_key = group_key.clone();
                    let new_aggregates = aggregates.clone();
                    let new_filter = query_graph.filter(
                        *agg_input,
                        pushable_conditions
                            .iter()
                            .map(|(_, expr)| expr.clone())
                            .collect::<Vec<_>>(),
                    );
                    let new_aggregate = query_graph.add_node(QueryNode::Aggregate {
                        group_key: new_group_key,
                        aggregates: new_aggregates,
                        input: new_filter,
                    });

                    return Some(new_aggregate);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_filter_aggregate_transpose() {}
}
