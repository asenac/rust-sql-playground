use crate::query_graph::*;
use crate::visitor_utils::*;

/// Trait for visiting a `QueryGraph`.
pub trait QueryGraphPrePostVisitor {
    fn visit_pre(&mut self, query_graph: &QueryGraph, node_id: NodeId) -> PreOrderVisitationResult;
    fn visit_post(&mut self, query_graph: &QueryGraph, node_id: NodeId);
}

/// Trait for visiting a `QueryGraph` for mutation purposes.
pub trait QueryGraphPrePostVisitorMut {
    fn visit_pre(
        &mut self,
        query_graph: &mut QueryGraph,
        node_id: &mut NodeId,
    ) -> PreOrderVisitationResult;

    fn visit_post(
        &mut self,
        query_graph: &mut QueryGraph,
        node_id: &mut NodeId,
    ) -> PostOrderVisitationResult;
}

/// Helper struct for visiting the query graph in pre-order using a closure.
struct QueryGraphPreVisitor<'a, F>
where
    F: FnMut(&QueryGraph, NodeId) -> PreOrderVisitationResult,
{
    visitor: &'a mut F,
}

impl<F> QueryGraphPrePostVisitor for QueryGraphPreVisitor<'_, F>
where
    F: FnMut(&QueryGraph, NodeId) -> PreOrderVisitationResult,
{
    fn visit_pre(&mut self, query_graph: &QueryGraph, node_id: NodeId) -> PreOrderVisitationResult {
        (self.visitor)(query_graph, node_id)
    }
    fn visit_post(&mut self, _: &QueryGraph, _: NodeId) {}
}

/// Visitation utilities.
impl QueryGraph {
    /// Visits the entire graph.
    pub fn visit<V>(&self, visitor: &mut V)
    where
        V: QueryGraphPrePostVisitor,
    {
        self.visit_subgraph(visitor, self.entry_node);
    }

    /// Visits the sub-graph under the given node.
    pub fn visit_subgraph<V>(&self, visitor: &mut V, node_id: NodeId)
    where
        V: QueryGraphPrePostVisitor,
    {
        let mut stack = vec![VisitationStep::new(node_id)];
        while let Some(step) = stack.last_mut() {
            if step.next_child.is_none() {
                match visitor.visit_pre(self, step.node) {
                    PreOrderVisitationResult::Abort => break,
                    PreOrderVisitationResult::VisitInputs => {}
                    PreOrderVisitationResult::DoNotVisitInputs => {
                        visitor.visit_post(self, step.node);
                        stack.pop();
                        continue;
                    }
                }
                step.next_child = Some(0);
            }

            let node = self.node(step.node);
            if step.next_child.unwrap() < node.num_inputs() {
                let input_idx = step.next_child.unwrap();
                step.next_child = Some(input_idx + 1);
                stack.push(VisitationStep::new(node.get_input(input_idx)));
                continue;
            }

            visitor.visit_post(self, step.node);
            stack.pop();
        }
    }

    /// Visits the entire query graph while modifying it.
    pub fn visit_mut<V>(&mut self, visitor: &mut V)
    where
        V: QueryGraphPrePostVisitorMut,
    {
        let mut stack = vec![VisitationStep::new(self.entry_node)];
        while let Some(step) = stack.last_mut() {
            if step.next_child.is_none() {
                match visitor.visit_pre(self, &mut step.node) {
                    PreOrderVisitationResult::Abort => break,
                    PreOrderVisitationResult::VisitInputs => {}
                    PreOrderVisitationResult::DoNotVisitInputs => {
                        let result = visitor.visit_post(self, &mut step.node);
                        stack.pop();
                        match result {
                            PostOrderVisitationResult::Abort => break,
                            PostOrderVisitationResult::Continue => continue,
                        }
                    }
                }
                step.next_child = Some(0);
            }

            let node = self.node(step.node);
            if step.next_child.unwrap() < node.num_inputs() {
                let input_idx = step.next_child.unwrap();
                step.next_child = Some(input_idx + 1);
                stack.push(VisitationStep::new(node.get_input(input_idx)));
                continue;
            }

            let result = visitor.visit_post(self, &mut step.node);
            stack.pop();
            match result {
                PostOrderVisitationResult::Abort => break,
                PostOrderVisitationResult::Continue => {}
            }
        }
    }

    /// Visits the entire query graph in pre-order.
    pub fn visit_pre<F>(&self, visitor: &mut F)
    where
        F: FnMut(&QueryGraph, NodeId) -> PreOrderVisitationResult,
    {
        self.visit_subgraph_pre(visitor, self.entry_node)
    }

    pub fn visit_subgraph_pre<F>(&self, visitor: &mut F, node_id: NodeId)
    where
        F: FnMut(&QueryGraph, NodeId) -> PreOrderVisitationResult,
    {
        let mut pre_post_visitor = QueryGraphPreVisitor { visitor };
        self.visit_subgraph(&mut pre_post_visitor, node_id);
    }

    pub fn collect_nodes_under(&self, node_id: NodeId) -> HashSet<NodeId> {
        let mut nodes = HashSet::new();
        self.visit_subgraph_pre(
            &mut |_, node_id| {
                if nodes.insert(node_id) {
                    PreOrderVisitationResult::VisitInputs
                } else {
                    PreOrderVisitationResult::DoNotVisitInputs
                }
            },
            node_id,
        );
        nodes
    }
}
