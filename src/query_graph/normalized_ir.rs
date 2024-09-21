use std::{
    collections::{BTreeSet, HashMap},
    rc::Rc,
};

use itertools::Itertools;

use super::{
    visitor::QueryGraphPrePostVisitor, ApplyType, JoinType, NodeId, QueryGraph, QueryNode,
};
use crate::{
    scalar_expr::{
        rewrite::{dereference_extended_scalar_expr, rewrite_expr_post},
        ExtendedScalarExpr, ExtendedScalarExprRef, ToExtendedExpr,
    },
    visitor_utils::PreOrderVisitationResult,
};

#[derive(Clone, PartialEq, Eq)]
pub enum NormalizedNode {
    Filter {
        conditions: Vec<ExtendedScalarExprRef>,
        input: NodeId,
    },
    TableScan {
        table_id: usize,
    },
    Aggregate {
        group_key: BTreeSet<ExtendedScalarExprRef>,
        input: NodeId,
    },
    Union {
        inputs: Vec<NodeId>,
    },
}

pub struct NormalizedQueryGraph {
    nodes: HashMap<NodeId, NormalizedNode>,
    next_node_id: usize,
}

impl NormalizedQueryGraph {
    pub fn new() -> NormalizedQueryGraph {
        Self {
            nodes: HashMap::new(),
            next_node_id: 0,
        }
    }

    pub fn add_node(&mut self, node: NormalizedNode) -> NodeId {
        // Avoid adding duplicated nodes
        if let Some(existing_node_id) = self.find_node(&node) {
            return existing_node_id;
        }
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        self.nodes.insert(node_id, node);
        node_id
    }

    /// Finds whether there is an existing node exactly like the given one.
    fn find_node(&self, node: &NormalizedNode) -> Option<NodeId> {
        self.nodes.iter().find_map(|(node_id, existing_node)| {
            if *node == *existing_node {
                Some(*node_id)
            } else {
                None
            }
        })
    }
}

struct NormalizedIrVisitor<'a> {
    normalized_query_graph: &'a mut NormalizedQueryGraph,
    node_cache: &'a mut HashMap<NodeId, (NodeId, Rc<Vec<ExtendedScalarExprRef>>)>,
}

impl<'a> NormalizedIrVisitor<'a> {
    fn normalize_single_input_expr(
        &mut self,
        input_expr: &ExtendedScalarExprRef,
        input_normalized_project: &Vec<ExtendedScalarExprRef>,
    ) -> ExtendedScalarExprRef {
        rewrite_expr_post(
            &mut |expr: &ExtendedScalarExprRef| {
                if let ExtendedScalarExpr::InputRef { index } = expr.as_ref() {
                    let expr = input_normalized_project[*index].clone();
                    let var = ExtendedScalarExpr::NormalizedVariable { input: 0, expr };
                    return Some(Rc::new(var));
                }
                // TODO(asenac): process subqueries
                None
            },
            input_expr,
        )
    }
}

impl<'a> QueryGraphPrePostVisitor for NormalizedIrVisitor<'a> {
    fn visit_pre(&mut self, _: &QueryGraph, node_id: NodeId) -> PreOrderVisitationResult {
        self.node_cache
            .get(&node_id)
            .map(|_| PreOrderVisitationResult::DoNotVisitInputs)
            .unwrap_or(PreOrderVisitationResult::VisitInputs)
    }
    fn visit_post(&mut self, query_graph: &QueryGraph, node_id: NodeId) {
        let entry = match query_graph.node(node_id) {
            QueryNode::QueryRoot { input } => input
                .map(|input| self.node_cache.get(&input).cloned())
                .unwrap_or(None),
            QueryNode::Project { outputs, input } => self.node_cache.get(input).cloned().map(
                |(normalized_node_id, input_normalized_project)| {
                    let normalized_project = outputs
                        .iter()
                        .map(|e| {
                            dereference_extended_scalar_expr(
                                &e.to_extended_expr(),
                                &input_normalized_project,
                            )
                        })
                        .collect_vec();
                    (normalized_node_id, Rc::new(normalized_project))
                },
            ),
            QueryNode::Filter { conditions, input } => self.node_cache.get(input).cloned().map(
                |(input_normalized_node_id, input_normalized_project)| {
                    let normalized_conditions = conditions
                        .iter()
                        .map(|e| {
                            self.normalize_single_input_expr(
                                &e.to_extended_expr(),
                                &input_normalized_project,
                            )
                        })
                        .collect_vec();
                    let normalized_node_id =
                        self.normalized_query_graph
                            .add_node(NormalizedNode::Filter {
                                conditions: normalized_conditions,
                                input: input_normalized_node_id,
                            });
                    let normalized_project = input_normalized_project
                        .iter()
                        .map(|e| {
                            Rc::new(ExtendedScalarExpr::NormalizedVariable {
                                input: 0,
                                expr: e.clone(),
                            })
                        })
                        .collect_vec();
                    (normalized_node_id, Rc::new(normalized_project))
                },
            ),

            QueryNode::TableScan { table_id, row_type } => {
                let project = row_type
                    .iter()
                    .enumerate()
                    .map(|(i, data_type)| {
                        Rc::new(ExtendedScalarExpr::BaseColumn {
                            index: i,
                            data_type: data_type.clone(),
                        })
                    })
                    .collect_vec();
                let normalized_node_id =
                    self.normalized_query_graph
                        .add_node(NormalizedNode::TableScan {
                            table_id: *table_id,
                        });
                Some((normalized_node_id, Rc::new(project)))
            }
            QueryNode::Join {
                join_type,
                conditions,
                left,
                right,
            } => todo!(),
            QueryNode::Aggregate {
                group_key,
                aggregates,
                input,
            } => self.node_cache.get(input).cloned().map(
                |(input_normalized_node_id, input_normalized_project)| {
                    let normalized_group_key = group_key
                        .iter()
                        .map(|key| {
                            Rc::new(ExtendedScalarExpr::NormalizedVariable {
                                input: 0,
                                expr: input_normalized_project[*key].clone(),
                            })
                        })
                        .collect();
                    let normalized_node_id =
                        self.normalized_query_graph
                            .add_node(NormalizedNode::Aggregate {
                                group_key: normalized_group_key,
                                input: input_normalized_node_id,
                            });
                    let normalized_project = group_key
                        .iter()
                        .map(|key| {
                            Rc::new(ExtendedScalarExpr::NormalizedVariable {
                                input: 0,
                                expr: input_normalized_project[*key].clone(),
                            })
                        })
                        .chain(aggregates.iter().map(|e| {
                            self.normalize_single_input_expr(
                                &e.to_extended_expr(),
                                &input_normalized_project,
                            )
                        }))
                        .collect_vec();
                    (normalized_node_id, Rc::new(normalized_project))
                },
            ),
            QueryNode::Union { inputs } => todo!(),
            QueryNode::SubqueryRoot { input } => self.node_cache.get(input).cloned(),
            QueryNode::Apply {
                correlation,
                left,
                right,
                apply_type,
            } => todo!(),
        };
        if let Some(entry) = entry {
            self.node_cache.insert(node_id, entry);
        }
    }
}

pub fn normalized_ir(
    query_graph: &QueryGraph,
    node_id: NodeId,
    normalized_query_graph: &mut NormalizedQueryGraph,
    node_cache: &mut HashMap<NodeId, (NodeId, Rc<Vec<ExtendedScalarExprRef>>)>,
) -> Option<(NodeId, Rc<Vec<ExtendedScalarExprRef>>)> {
    let mut visitor = NormalizedIrVisitor {
        normalized_query_graph,
        node_cache,
    };
    query_graph.visit_subgraph(&mut visitor, node_id);
    node_cache.get(&node_id).cloned()
}
