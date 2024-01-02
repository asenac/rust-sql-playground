use crate::query_graph::visitor::*;
use crate::query_graph::*;
use crate::scalar_expr::ScalarExpr;
use crate::visitor_utils::PreOrderVisitationResult;

use super::properties::default_annotators;

/// Utility for explaining a query graph.
pub struct Explainer<'a> {
    pub(super) query_graph: &'a QueryGraph,
    pub(super) annotators: Vec<&'a dyn Fn(&QueryGraph, NodeId) -> Option<String>>,
    pub(super) leaves: HashSet<NodeId>,
    pub(super) entry_point: NodeId,
}

impl<'a> Explainer<'a> {
    pub fn new(query_graph: &'a QueryGraph) -> Self {
        Self {
            query_graph,
            leaves: HashSet::new(),
            annotators: Vec::new(),
            entry_point: query_graph.entry_node,
        }
    }

    pub fn with_all_annotators(self) -> Self {
        Self {
            query_graph: self.query_graph,
            leaves: self.leaves,
            annotators: default_annotators(),
            entry_point: self.entry_point,
        }
    }

    pub fn with_annotators(
        self,
        annotators: Vec<&'a dyn Fn(&QueryGraph, NodeId) -> Option<String>>,
    ) -> Self {
        Self {
            query_graph: self.query_graph,
            leaves: self.leaves,
            annotators,
            entry_point: self.entry_point,
        }
    }

    /// Treat the given nodes as leaves in the explain plan.
    pub fn with_leaves(self, leaves: HashSet<NodeId>) -> Self {
        Self {
            query_graph: self.query_graph,
            leaves,
            annotators: self.annotators,
            entry_point: self.entry_point,
        }
    }

    /// Override the entry point for the explain plan.
    pub fn with_entry_point(self, entry_point: NodeId) -> Self {
        Self {
            query_graph: self.query_graph,
            leaves: self.leaves,
            annotators: self.annotators,
            entry_point,
        }
    }

    /// Generate the explain plan.
    pub fn explain(&self) -> String {
        let mut explain = ExplainVisitor::new(self);
        self.query_graph
            .visit_subgraph(&mut explain, self.entry_point);
        let subquery_roots = self.query_graph.subquery_roots();
        for subquery_root in subquery_roots {
            explain.result += "\n";
            self.query_graph.visit_subgraph(&mut explain, subquery_root);
        }
        explain.result
    }
}

/// Explain functions.
impl QueryGraph {
    /// Returns a stringified version of the query graph.
    pub fn explain(&self) -> String {
        Explainer::new(&self).explain()
    }

    // Explains the query graph annotated with all available properties.
    pub fn fully_annotated_explain(&self) -> String {
        Explainer::new(self).with_all_annotators().explain()
    }
}

struct ExplainVisitor<'a> {
    indentation: usize,
    visited_nodes: HashSet<NodeId>,
    result: String,
    options: &'a Explainer<'a>,
}

impl<'a> ExplainVisitor<'a> {
    fn new(options: &'a Explainer) -> Self {
        Self {
            indentation: 0,
            visited_nodes: HashSet::new(),
            result: String::new(),
            options,
        }
    }
}

impl<'a> QueryGraphPrePostVisitor for ExplainVisitor<'a> {
    fn visit_pre(&mut self, query_graph: &QueryGraph, node_id: NodeId) -> PreOrderVisitationResult {
        let line_prefix = (0..2 * self.indentation).map(|_| ' ').collect::<String>();
        self.indentation += 1;
        if !self.visited_nodes.insert(node_id) {
            self.result += &format!("{}Recurring node {}\n", line_prefix, node_id);
            return PreOrderVisitationResult::DoNotVisitInputs;
        }
        let prefix = format!("{}[{}] ", line_prefix, node_id);
        let node = match query_graph.node(node_id) {
            QueryNode::Project { outputs, .. } => {
                format!("{}Project [{}]\n", prefix, explain_scalar_expr_vec(outputs),)
            }
            QueryNode::Filter { conditions, .. } => format!(
                "{}Filter [{}]\n",
                prefix,
                explain_scalar_expr_vec(conditions),
            ),
            QueryNode::TableScan { table_id, .. } => {
                format!("{}TableScan id: {}\n", prefix, table_id)
            }
            QueryNode::Join {
                join_type,
                conditions,
                ..
            } => {
                format!(
                    "{}{} Join [{}]\n",
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
                "{}Aggregate key: [{}], aggregates: [{}]\n",
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
            QueryNode::Union { .. } => format!("{}Union\n", prefix),
            QueryNode::SubqueryRoot { .. } => format!("{}SubqueryRoot\n", prefix),
        };
        self.result += &node;

        for annotator in self.options.annotators.iter() {
            if let Some(annotation) = (annotator)(query_graph, node_id) {
                self.result += format!("{}    - {}\n", line_prefix, annotation).as_str();
            }
        }

        if self.options.leaves.contains(&node_id) {
            PreOrderVisitationResult::DoNotVisitInputs
        } else {
            PreOrderVisitationResult::VisitInputs
        }
    }

    fn visit_post(&mut self, _: &QueryGraph, _: NodeId) {
        self.indentation -= 1;
    }
}

pub(crate) fn explain_scalar_expr_vec(vec: &Vec<ScalarExprRef>) -> String {
    vec.iter()
        .map(|e| format!("{}", e))
        .collect::<Vec<_>>()
        .join(", ")
}
