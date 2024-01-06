use std::collections::{BTreeSet, HashMap};

use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{OptRuleType, Rule},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::{
        rewrite::{dereference_extended_scalar_expr, dereference_scalar_expr},
        AggregateExpr, ExtendedScalarExpr, ExtendedScalarExprRef, ScalarExpr, ScalarExprRef,
        ToExtendedExpr, ToScalarExpr,
    },
};

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
struct AggregateKey {
    input: NodeId,
    group_key: Vec<ScalarExprRef>,
}

struct AggregateValue {
    agg_node_id: NodeId,
    aggregates: Vec<ExtendedScalarExprRef>,
}

/// Rule that folds aggregates over the same input with the same grouping key into a single
/// shared aggregate node.
pub struct CommonAggregateDiscoveryRule {}

impl Rule for CommonAggregateDiscoveryRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::RootOnly
    }

    fn apply(&self, query_graph: &mut QueryGraph, _: NodeId) -> Option<Vec<(NodeId, NodeId)>> {
        // Collect and classify  all the aggregate nodes in the query graph
        let mut classified_aggregates: HashMap<AggregateKey, Vec<AggregateValue>> = HashMap::new();
        for node_id in query_graph.nodes.keys().sorted() {
            if let QueryNode::Aggregate {
                group_key,
                aggregates,
                input,
            } = query_graph.node(*node_id)
            {
                let mut group_key = group_key
                    .iter()
                    .map(|i| ScalarExpr::InputRef { index: *i }.into())
                    .collect_vec();
                let mut aggregates = aggregates
                    .iter()
                    .map(|agg| agg.to_extended_expr())
                    .collect_vec();
                let mut normalized_input = *input;
                // Let's absorb projections so that we can still fold the following
                // two aggregations into a single one:
                //
                // Aggregate key[ref_0], Aggregates[max(ref_1)]
                //   Project ref_0, ref_1 + ref_2
                //     Shared node X
                //
                // Aggregate key[ref_0], Aggregates[max(ref_1)]
                //   Project ref_0, ref_1 + ref_3
                //     Shared node X
                //
                // The resulting aggregate will be:
                //
                // Aggregate key[ref_0], Aggregates[max(ref_1), max(ref_2]
                //   Project ref_0, ref_1 + ref_2, ref_1 + ref_3
                //     Shared node X
                while let QueryNode::Project { outputs, input } = query_graph.node(normalized_input)
                {
                    let extended_outputs =
                        outputs.iter().map(|e| e.to_extended_expr()).collect_vec();
                    for key in group_key.iter_mut() {
                        *key = dereference_scalar_expr(key, &outputs);
                    }
                    for agg in aggregates.iter_mut() {
                        *agg = dereference_extended_scalar_expr(agg, &extended_outputs);
                    }
                    normalized_input = *input;
                }

                classified_aggregates
                    .entry(AggregateKey {
                        input: normalized_input,
                        group_key,
                    })
                    .or_insert_with(|| Vec::new())
                    .push(AggregateValue {
                        agg_node_id: *node_id,
                        aggregates,
                    })
            }
        }
        let mut result: Option<Vec<(NodeId, NodeId)>> = None;
        let mut it = classified_aggregates.iter().filter(|(_, v)| v.len() > 1);
        while let Some((key, values)) = it.next() {
            let new_group_key = (0..key.group_key.len()).collect::<BTreeSet<_>>();
            let mut input_project = key.group_key.clone();
            let all_aggregates = values
                .iter()
                .map(|v| v.aggregates.iter())
                .flatten()
                .sorted()
                .dedup()
                .collect_vec();
            let new_aggregates = all_aggregates
                .iter()
                .map(|a| match a.as_ref() {
                    ExtendedScalarExpr::Aggregate { op, operands } => {
                        let operands = operands
                            .iter()
                            .map(|o| {
                                append_to_vector_if_not_present(
                                    &mut input_project,
                                    o.to_scalar_expr().unwrap(),
                                )
                            })
                            .collect_vec();
                        AggregateExpr {
                            op: op.clone(),
                            operands,
                        }
                        .into()
                    }
                    _ => panic!(),
                })
                .collect_vec();
            let input = query_graph.project(key.input, input_project);
            let new_aggregate = query_graph.add_node(QueryNode::Aggregate {
                group_key: new_group_key,
                aggregates: new_aggregates,
                input,
            });
            for value in values.iter() {
                let project = (0..key.group_key.len())
                    .chain(value.aggregates.iter().map(|a| {
                        key.group_key.len()
                            + all_aggregates
                                .iter()
                                .enumerate()
                                .find_map(|(i, o)| if *a == **o { Some(i) } else { None })
                                // the aggregate must be present in the list of aggregates
                                .unwrap()
                    }))
                    .map(|i| ScalarExpr::input_ref(i).into())
                    .collect_vec();
                let new_project = query_graph.project(new_aggregate, project);
                result
                    .get_or_insert_with(|| Vec::new())
                    .push((value.agg_node_id, new_project));
            }
        }
        result
    }
}

fn append_to_vector_if_not_present<E: Eq>(vec: &mut Vec<E>, e: E) -> usize {
    if let Some(index) = vec
        .iter()
        .enumerate()
        .find_map(|(i, o)| if e == *o { Some(i) } else { None })
    {
        index
    } else {
        vec.push(e);
        vec.len() - 1
    }
}
