use crate::{
    scalar_expr::ScalarExprRef,
    visitor_utils::{PostOrderVisitationResult, PreOrderVisitationResult},
};

use super::{visitor::QueryGraphPrePostVisitorMut, NodeId, QueryGraph, QueryNode};

/// Clones a subgraph, except for the pruned subgraphs, applying the given rewrite
/// to the expressions contained by the nodes.
pub fn deep_clone<P, R>(
    query_graph: &mut QueryGraph,
    subgraph: NodeId,
    prune: &P,
    rewrite: &mut R,
) -> NodeId
where
    P: Fn(&QueryGraph, NodeId) -> bool,
    R: FnMut(&ScalarExprRef) -> ScalarExprRef,
{
    let mut cloner = DeepCloner::new(prune, rewrite);
    query_graph.visit_subgraph_mut(&mut cloner, subgraph);
    cloner.stack.first().cloned().unwrap()
}

struct DeepCloner<'a, P, R>
where
    P: Fn(&QueryGraph, NodeId) -> bool,
    R: FnMut(&ScalarExprRef) -> ScalarExprRef,
{
    stack: Vec<NodeId>,
    prune: &'a P,
    rewrite: &'a mut R,
    skip_post: bool,
}

impl<'a, P, R> DeepCloner<'a, P, R>
where
    P: Fn(&QueryGraph, NodeId) -> bool,
    R: FnMut(&ScalarExprRef) -> ScalarExprRef,
{
    fn new(prune: &'a P, rewrite: &'a mut R) -> Self {
        Self {
            stack: Vec::new(),
            prune,
            rewrite,
            skip_post: false,
        }
    }

    fn clone_with_new_inputs(&mut self, query_graph: &mut QueryGraph, node_id: NodeId) -> NodeId {
        let mut cloned_node = query_graph.node(node_id).clone();
        let num_inputs = cloned_node.num_inputs();
        let inputs = &self.stack[self.stack.len() - num_inputs..];
        match &mut cloned_node {
            QueryNode::Project { outputs, input } => {
                outputs.iter_mut().for_each(|e| *e = (self.rewrite)(&e));
                *input = inputs[0];
            }
            QueryNode::Filter {
                conditions,
                input,
                correlation_id: _,
            } => {
                conditions.iter_mut().for_each(|e| *e = (self.rewrite)(e));
                *input = inputs[0];
            }
            QueryNode::TableScan {
                table_id: _,
                row_type: _,
            } => {}
            QueryNode::Join {
                join_type: _,
                conditions,
                left,
                right,
            } => {
                conditions.iter_mut().for_each(|e| *e = (self.rewrite)(e));
                *left = inputs[0];
                *right = inputs[1];
            }
            QueryNode::Aggregate {
                group_key: _,
                aggregates: _,
                input,
            } => *input = inputs[0],
            QueryNode::Union { inputs: inputs_ref } => *inputs_ref = inputs.to_vec(),
            QueryNode::SubqueryRoot { input } => *input = inputs[0],
            QueryNode::Apply {
                correlation_id: _,
                left,
                right,
                apply_type: _,
            } => {
                *left = inputs[0];
                *right = inputs[1];
            }
        }
        self.stack.truncate(self.stack.len() - num_inputs);
        query_graph.add_node(cloned_node)
    }
}

impl<'a, P, R> QueryGraphPrePostVisitorMut for DeepCloner<'a, P, R>
where
    P: Fn(&QueryGraph, NodeId) -> bool,
    R: FnMut(&ScalarExprRef) -> ScalarExprRef,
{
    fn visit_pre(
        &mut self,
        query_graph: &mut QueryGraph,
        node_id: &mut NodeId,
    ) -> PreOrderVisitationResult {
        if (self.prune)(query_graph, *node_id) {
            self.skip_post = true;
            self.stack.push(*node_id);
            PreOrderVisitationResult::DoNotVisitInputs
        } else {
            PreOrderVisitationResult::VisitInputs
        }
    }

    fn visit_post(
        &mut self,
        query_graph: &mut QueryGraph,
        node_id: &mut NodeId,
    ) -> PostOrderVisitationResult {
        if self.skip_post {
            self.skip_post = false;
        } else {
            let cloned_node = self.clone_with_new_inputs(query_graph, *node_id);
            self.stack.push(cloned_node);
        }
        PostOrderVisitationResult::Continue
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        query_graph::QueryGraph,
        scalar_expr::{BinaryOp, ScalarExpr, ScalarExprRef},
    };

    use super::deep_clone;

    /// Test that if no expression is rewritten the same node is returned.
    #[test]
    fn test_no_op() {
        let mut query_graph = QueryGraph::new();
        let table_scan_id = query_graph.table_scan(0, 10);
        let filter_1: ScalarExprRef = ScalarExpr::input_ref(0)
            .binary(BinaryOp::Eq, ScalarExpr::input_ref(1).into())
            .into();
        let filter_id = query_graph.filter(table_scan_id, vec![filter_1.clone()]);
        let project_id = query_graph.project(
            filter_id,
            (0..5).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );
        query_graph.set_entry_node(project_id);

        let cloned_project_id = deep_clone(&mut query_graph, project_id, &|_, _| false, &mut |e| {
            e.clone()
        });
        assert_eq!(cloned_project_id, project_id);
    }
}
