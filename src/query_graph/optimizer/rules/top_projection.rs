use itertools::Itertools;

use crate::{
    query_graph::{
        optimizer::{OptRuleType, SingleReplacementRule},
        properties::num_columns,
        NodeId, QueryGraph, QueryNode,
    },
    scalar_expr::ScalarExpr,
};

/// Rule that ensures the root node of the query is a projection.
///
/// Adding a projection as the top level node helps with column pruning
/// as columns that are bound to other columns or constants can be pruned.
pub struct TopProjectionRule {}

impl SingleReplacementRule for TopProjectionRule {
    fn rule_type(&self) -> OptRuleType {
        OptRuleType::RootOnly
    }

    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId> {
        if let QueryNode::Project { .. } = query_graph.node(node_id) {
            None
        } else {
            let num_columns = num_columns(query_graph, node_id);
            Some(
                query_graph.project(
                    node_id,
                    (0..num_columns)
                        .map(|i| ScalarExpr::input_ref(i).into())
                        .collect_vec(),
                ),
            )
        }
    }
}
