use std::collections::btree_set::Iter;

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

    /// Visits the sub-graph above the given node.
    /// If the same parent node is reached through multiple paths, it is visited
    /// multiple times.
    /// If the same parent node is the direct parent of a given node through more than
    /// one of its inputs, it is considered a single parent, and visited only once
    /// through that path. Note that `self.parents` is a set.
    pub fn visit_subgraph_upwards<V>(&self, visitor: &mut V, node_id: NodeId)
    where
        V: QueryGraphPrePostVisitor,
    {
        struct VisitationStep<'a> {
            node_id: NodeId,
            next_parent: Option<Iter<'a, NodeId>>,
        }
        let mut stack = vec![VisitationStep {
            node_id,
            next_parent: None,
        }];
        while let Some(step) = stack.last_mut() {
            if step.next_parent.is_none() {
                match visitor.visit_pre(self, step.node_id) {
                    PreOrderVisitationResult::Abort => break,
                    PreOrderVisitationResult::VisitInputs => {}
                    PreOrderVisitationResult::DoNotVisitInputs => {
                        visitor.visit_post(self, step.node_id);
                        stack.pop();
                        continue;
                    }
                }
                step.next_parent = self.get_parents(step.node_id).map(|parents| parents.iter());
            }

            if let Some(next_parent) = &mut step.next_parent {
                if let Some(next_parent) = next_parent.next() {
                    stack.push(VisitationStep {
                        node_id: *next_parent,
                        next_parent: None,
                    });
                    continue;
                }
            }

            visitor.visit_post(self, step.node_id);
            stack.pop();
        }
    }

    pub fn visit_subgraph_upwards_pre<F>(&self, visitor: &mut F, node_id: NodeId)
    where
        F: FnMut(&QueryGraph, NodeId) -> PreOrderVisitationResult,
    {
        let mut pre_post_visitor = QueryGraphPreVisitor { visitor };
        self.visit_subgraph_upwards(&mut pre_post_visitor, node_id);
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

    pub fn collect_nodes_above(&self, node_id: NodeId) -> HashSet<NodeId> {
        let mut nodes = HashSet::new();
        self.visit_subgraph_upwards_pre(
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{
        query_graph::{NodeId, QueryGraph, QueryNode},
        scalar_expr::{ScalarExpr, ToRef},
        visitor_utils::PreOrderVisitationResult,
    };

    fn collect_visited_nodes_upwards(query_graph: &QueryGraph, node_id: NodeId) -> Vec<NodeId> {
        let mut visited_nodes = Vec::new();
        query_graph.visit_subgraph_upwards_pre(
            &mut |_, node_id| {
                visited_nodes.push(node_id);
                PreOrderVisitationResult::VisitInputs
            },
            node_id,
        );
        visited_nodes
    }

    #[test]
    fn test_visit_subgraph_upwards() {
        let mut query_graph = QueryGraph::new();
        let table_scan = query_graph.table_scan(0, 5);

        let mut input = table_scan;
        let mut branch1 = Vec::new();
        for _ in 0..5 {
            input = query_graph.project(input, vec![ScalarExpr::false_literal().to_ref()]);
            branch1.push(input);
        }

        let mut branch2 = vec![];
        input = table_scan;
        for _ in 0..4 {
            input = query_graph.project(input, vec![ScalarExpr::true_literal().to_ref()]);
            branch2.push(input);
        }

        // The union is visited twice
        let union_ = query_graph.add_node(QueryNode::Union {
            inputs: vec![*branch1.last().unwrap(), *branch2.last().unwrap()],
        });
        let ordered = std::iter::once(&table_scan)
            .chain(branch1.iter())
            .chain(std::iter::once(&union_))
            .chain(branch2.iter())
            .chain(std::iter::once(&union_))
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(
            collect_visited_nodes_upwards(&query_graph, table_scan),
            ordered
        );

        let all_nodes = query_graph.nodes.keys().cloned().collect::<HashSet<_>>();
        assert_eq!(query_graph.collect_nodes_above(table_scan), all_nodes);
        assert_eq!(
            query_graph.collect_nodes_above(union_),
            HashSet::from([union_])
        );
        assert_eq!(
            query_graph.collect_nodes_above(*branch1.first().unwrap()),
            HashSet::from_iter(
                branch1
                    .iter()
                    .chain(std::iter::once(&union_))
                    .cloned()
                    .collect::<HashSet<_>>()
            ),
        );
    }
}
