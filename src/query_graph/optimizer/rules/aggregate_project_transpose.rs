use std::{
    collections::{BTreeSet, HashMap},
    rc::Rc,
};

use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{utils::sort_projection, OptRuleType, SingleReplacementRule},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{AggregateExpr, ScalarExpr, ToRef},
};

/// Given an aggregate node over a non-sorted projection, it creates a new aggregate node
/// over a sorted version of the projection, and adds a re-ordering projection on top of
/// the new aggregate node.
pub struct AggregateProjectTransposeRule {}

impl SingleReplacementRule for AggregateProjectTransposeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::BottomUp
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Aggregate {
            group_key,
            aggregates,
            input,
        } = query_graph.node(node_id)
        {
            if let QueryNode::Project {
                outputs,
                input: proj_input,
            } = query_graph.node(*input)
            {
                if let Some((reorder_map, sorted_proj)) = sort_projection(outputs) {
                    let column_map = reorder_map
                        .iter()
                        .enumerate()
                        .map(|(i, e)| (*e, i))
                        .collect::<HashMap<_, _>>();
                    let new_group_key = group_key
                        .iter()
                        .map(|k| *column_map.get(k).unwrap())
                        .collect::<BTreeSet<_>>();
                    let new_aggregates = aggregates
                        .iter()
                        .map(|k| {
                            Rc::new(AggregateExpr {
                                op: k.op.clone(),
                                operands: k
                                    .operands
                                    .iter()
                                    .map(|e| *column_map.get(e).unwrap())
                                    .collect_vec(),
                            })
                        })
                        .collect_vec();

                    // Reorder the grouping key elements in a projection over the new
                    // aggregate node
                    let group_key_len = group_key.len();
                    let aggregates_len = aggregates.len();
                    let reordering_proj = group_key
                        .iter()
                        .enumerate()
                        .map(|(i, k)| (i, *column_map.get(k).unwrap()))
                        .sorted_by_key(|(_, e)| *e)
                        .enumerate()
                        .sorted_by_key(|(_, (i, _))| *i)
                        .map(|(i, _)| i)
                        // ... and the aggregates
                        .chain(group_key_len..group_key_len + aggregates_len)
                        .map(|i| ScalarExpr::input_ref(i).to_ref())
                        .collect_vec();

                    let new_project = query_graph.project(*proj_input, sorted_proj);
                    let new_aggregate = query_graph.add_node(QueryNode::Aggregate {
                        group_key: new_group_key,
                        aggregates: new_aggregates,
                        input: new_project,
                    });
                    return Some(query_graph.project(new_aggregate, reordering_proj));
                }
            }
        }
        None
    }
}
