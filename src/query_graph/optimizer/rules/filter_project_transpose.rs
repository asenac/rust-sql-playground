use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::rewrite::dereference_scalar_expr,
};

/// Given a Filter node on top of a Project node, it transposes them by creating a
/// new Filter node and a new Project node on top of it.
///
/// If the Project node is a shared node, ie. it has multiple parents, the original
/// Project node will still be referenced by the rest of its parents. In our model,
/// we are only interested in preserving shared Joins, Aggregates and any node
/// performing some expensive operation.
pub struct FilterProjectTransposeRule {}

impl SingleReplacementRule for FilterProjectTransposeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Filter { conditions, input } = query_graph.node(node_id) {
            if let QueryNode::Project {
                outputs,
                input: proj_input,
            } = query_graph.node(*input)
            {
                let new_conditions = conditions
                    .iter()
                    .map(|c| dereference_scalar_expr(c, outputs))
                    .collect::<Vec<_>>();
                let outputs = outputs.clone();
                let new_filter = query_graph.filter(*proj_input, new_conditions);
                return Some(query_graph.project(new_filter, outputs));
            }
        }
        None
    }
}
