//! JSON serializer for generating visual representations of the plans.
use std::collections::VecDeque;

use crate::{
    query_graph::{explain::explain_scalar_expr_vec, *},
    scalar_expr::ScalarExpr,
    visitor_utils::PreOrderVisitationResult,
};

use super::{
    properties::{default_annotators, subqueries},
    visitor::QueryGraphPrePostVisitor,
};

pub struct JsonSerializer<'a> {
    annotators: Vec<&'a dyn Fn(&QueryGraph, NodeId) -> Option<String>>,
    included_nodes: HashSet<NodeId>,
    graph: Graph,
    queue: VecDeque<NodeId>,
}

impl<'a> JsonSerializer<'a> {
    pub fn new(annotators: Vec<&'a dyn Fn(&QueryGraph, NodeId) -> Option<String>>) -> Self {
        Self {
            annotators,
            included_nodes: HashSet::new(),
            graph: Graph::new(),
            queue: VecDeque::new(),
        }
    }

    pub fn new_with_all_annotators() -> Self {
        Self::new(default_annotators())
    }

    /// Ensure the given subgraph is included in the output graph.
    pub fn add_subgraph(&mut self, query_graph: &QueryGraph, node_id: NodeId) {
        self.queue.push_back(node_id);
        while let Some(node_id) = self.queue.pop_front() {
            query_graph.visit_subgraph(self, node_id);
        }
    }

    pub fn add_node_replacement(
        &mut self,
        query_graph: &QueryGraph,
        original_node: NodeId,
        replacement_node: NodeId,
        label: String,
    ) {
        query_graph.visit_subgraph(self, original_node);
        query_graph.visit_subgraph(self, replacement_node);
        self.graph.edges.push(Edge {
            from: original_node.to_string(),
            to: replacement_node.to_string(),
            label,
        })
    }

    /// Finally, generate the JSON string.
    pub fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.graph)
    }
}

impl<'a> QueryGraphPrePostVisitor for JsonSerializer<'a> {
    fn visit_pre(&mut self, query_graph: &QueryGraph, node_id: NodeId) -> PreOrderVisitationResult {
        if !self.included_nodes.insert(node_id) {
            return PreOrderVisitationResult::DoNotVisitInputs;
        }
        let prefix = format!("[{}] ", node_id);
        let label = match query_graph.node(node_id) {
            QueryNode::Project { outputs, .. } => {
                format!("{}Project [{}]", prefix, explain_scalar_expr_vec(outputs))
            }
            QueryNode::Filter { conditions, .. } => {
                format!("{}Filter [{}]", prefix, explain_scalar_expr_vec(conditions),)
            }
            QueryNode::TableScan { table_id, .. } => {
                format!("{}TableScan id: {}", prefix, table_id)
            }
            QueryNode::Join {
                join_type,
                conditions,
                ..
            } => {
                format!(
                    "{}{} Join [{}]",
                    prefix,
                    join_type,
                    explain_scalar_expr_vec(conditions)
                )
            }
            QueryNode::Aggregate {
                group_key,
                aggregates,
                ..
            } => format!(
                "{}Aggregate key: [{}], aggregates: [{}]",
                prefix,
                group_key
                    .iter()
                    .map(|e| format!("{}", ScalarExpr::input_ref(*e)))
                    .collect::<Vec<_>>()
                    .join(", "),
                aggregates
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            QueryNode::Union { .. } => format!("{}Union", prefix),
            QueryNode::SubqueryRoot { .. } => format!("{}SubqueryRoot", prefix),
            QueryNode::Apply {
                correlation,
                apply_type,
                ..
            } => {
                format!(
                    "{}{} Apply correlation_id: {}, parameters: [{}]",
                    prefix,
                    apply_type,
                    correlation.correlation_id.0,
                    explain_scalar_expr_vec(&correlation.parameters),
                )
            }
        };
        let mut annotations = Vec::new();
        for annotator in self.annotators.iter() {
            if let Some(annotation) = (annotator)(query_graph, node_id) {
                annotations.push(annotation);
            }
        }
        self.graph.nodes.push(Node {
            id: node_id.to_string(),
            label: label,
            annotations,
        });
        let node = query_graph.node(node_id);
        for i in 0..node.num_inputs() {
            let to = node.get_input(i);
            self.graph.edges.push(Edge {
                from: node_id.to_string(),
                to: to.to_string(),
                label: format!("input {}", i),
            });
        }

        // Link the current node with the subqueries it references
        let subqueries = subqueries(query_graph, node_id);
        for subquery_root in subqueries.iter() {
            self.queue.push_back(*subquery_root);
            self.graph.edges.push(Edge {
                from: node_id.to_string(),
                to: subquery_root.to_string(),
                label: format!("subquery({})", subquery_root),
            });
        }
        return PreOrderVisitationResult::VisitInputs;
    }

    fn visit_post(&mut self, _: &QueryGraph, _: NodeId) {}
}

#[derive(Serialize, Deserialize)]
pub struct Node {
    id: String,
    label: String,
    annotations: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Edge {
    from: String,
    to: String,
    label: String,
}

#[derive(Serialize, Deserialize)]
pub struct Graph {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl Graph {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }
}
