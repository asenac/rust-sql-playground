use std::rc::Rc;

use crate::{
    query_graph::{
        optimizer::{
            utils::{
                apply_map_to_parent_projections_and_replace_input,
                required_columns_from_parent_projections, required_columns_to_column_map,
            },
            OptRuleType, Rule,
        },
        properties::num_columns,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::AggregateExpr,
};

/// Rule that given a shared aggregate where all its parents are pruning projections, computes
/// the superset of columns required by all its parents, and prunes the columns not used
/// by any of them, replacing the parents of the aggregate with projections over the pruned
/// aggregate.
/// Only aggregate expressions can be pruned.
pub struct AggregatePruningRule {}

impl Rule for AggregatePruningRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(
        &self,
        query_graph: &mut QueryGraph,
        node_id: NodeId,
    ) -> Option<Vec<(NodeId, NodeId)>> {
        if let QueryNode::Aggregate {
            group_key,
            aggregates,
            input,
        } = query_graph.node(node_id)
        {
            if let Some(mut required_columns) =
                required_columns_from_parent_projections(query_graph, node_id)
            {
                // All the columns from the grouping key are implicitly required
                required_columns.extend(0..group_key.len());
                let num_columns = num_columns(query_graph, node_id);
                if required_columns.len() == num_columns {
                    return None;
                }
                let new_group_key = group_key.clone();
                let new_aggregates: Vec<Rc<AggregateExpr>> = aggregates
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| {
                        let col_offset = group_key.len() + i;
                        required_columns.contains(&col_offset)
                    })
                    .map(|(_, e)| e.clone())
                    .collect();
                assert_ne!(new_aggregates.len(), aggregates.len());
                let new_input = *input;
                let new_aggregate = query_graph.add_node(QueryNode::Aggregate {
                    group_key: new_group_key,
                    aggregates: new_aggregates,
                    input: new_input,
                });

                // Rewrite the parent projections
                let column_map = required_columns_to_column_map(&required_columns);
                return Some(apply_map_to_parent_projections_and_replace_input(
                    query_graph,
                    node_id,
                    &column_map,
                    new_aggregate,
                ));
            }
        }
        None
    }
}
