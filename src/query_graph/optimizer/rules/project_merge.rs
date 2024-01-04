use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::rewrite::dereference_scalar_expr,
};

pub struct ProjectMergeRule {}

impl SingleReplacementRule for ProjectMergeRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::TopDown
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Project {
            input,
            outputs,
            correlation_id: None,
        } = query_graph.node(node_id)
        {
            if let QueryNode::Project {
                input: child_input,
                outputs: child_outputs,
                correlation_id: None,
            } = query_graph.node(*input)
            {
                return Some(
                    query_graph.project(
                        *child_input,
                        outputs
                            .clone()
                            .into_iter()
                            .map(|x| dereference_scalar_expr(&x, &child_outputs))
                            .collect(),
                    ),
                );
            }
        }
        None
    }
}
