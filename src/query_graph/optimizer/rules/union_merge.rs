use crate::query_graph::{
    optimizer::{OptRuleType, SingleReplacementRule},
    NodeId, QueryGraph, QueryNode,
};

pub struct UnionMergeRule {}

impl SingleReplacementRule for UnionMergeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Union { inputs } = query_graph.node(node_id) {
            let mut new_inputs = inputs.clone();
            let mut any = false;
            while let Some((idx, inputs)) =
                new_inputs.iter().enumerate().find_map(|(idx, input)| {
                    if let QueryNode::Union { inputs } = query_graph.node(*input) {
                        Some((idx, inputs.clone()))
                    } else {
                        None
                    }
                })
            {
                let mut flattened_union = (0..idx)
                    .map(|i| new_inputs[i].clone())
                    .collect::<Vec<usize>>();
                flattened_union.extend(inputs);
                flattened_union.extend((idx + 1..new_inputs.len()).map(|i| new_inputs[i].clone()));
                new_inputs = flattened_union;
                any = true;
            }
            if any {
                return Some(query_graph.add_node(QueryNode::Union { inputs: new_inputs }));
            }
        }
        None
    }
}
