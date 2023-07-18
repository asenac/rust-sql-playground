use crate::scalar_expr::{AggregateExprRef, ScalarExprRef};
use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
};

use self::properties::PropertyCache;

pub mod explain;
pub mod json;
pub mod optimizer;
pub mod properties;
pub mod visitor;

pub type NodeId = usize;

#[derive(Clone, PartialEq, Eq, Copy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    /// Semi-join. Only the columns from the left relation are projected.
    Semi,
    /// Anti-join. Only the columns from the left relation are projected.
    Anti,
}

#[derive(Clone, PartialEq, Eq)]
pub enum QueryNode {
    Project {
        outputs: Vec<ScalarExprRef>,
        input: NodeId,
    },
    Filter {
        conditions: Vec<ScalarExprRef>,
        input: NodeId,
    },
    TableScan {
        table_id: usize,
        num_columns: usize,
    },
    Join {
        join_type: JoinType,
        conditions: Vec<ScalarExprRef>,
        left: NodeId,
        right: NodeId,
    },
    Aggregate {
        group_key: BTreeSet<usize>,
        aggregates: Vec<AggregateExprRef>,
        input: NodeId,
    },
    Union {
        inputs: Vec<NodeId>,
    },
}

pub struct QueryGraph {
    /// All the nodes in the query graph. May contain nodes not attached to the plan, ie.
    /// not reachable from the entry node.
    nodes: HashMap<NodeId, QueryNode>,
    /// The top-level root node of the query graph.
    pub entry_node: NodeId,
    /// The ID that will be given to the next node added to the query graph.
    next_node_id: usize,
    /// For each node, it contains a set with the nodes pointing to it through any of their
    /// inputs.
    parents: HashMap<NodeId, BTreeSet<NodeId>>,
    /// Keeps track of the number of node replacements the query graph has gone through.
    pub gen_number: usize,
    pub property_cache: RefCell<PropertyCache>,
}

impl QueryNode {
    /// Returns the number of inputs of this node.
    pub fn num_inputs(&self) -> usize {
        match self {
            Self::Project { .. } | Self::Filter { .. } | Self::Aggregate { .. } => 1,
            Self::TableScan { .. } => 0,
            Self::Join { .. } => 2,
            Self::Union { inputs } => inputs.len(),
        }
    }

    /// Returns the ID of the node at the given input of this node.
    pub fn get_input(&self, input_idx: usize) -> NodeId {
        assert!(input_idx < self.num_inputs());

        match self {
            Self::Project { input, .. }
            | Self::Filter { input, .. }
            | Self::Aggregate { input, .. } => *input,
            Self::TableScan { .. } => panic!(),
            Self::Join { left, right, .. } => {
                if input_idx == 0 {
                    *left
                } else {
                    *right
                }
            }
            Self::Union { inputs } => inputs[input_idx],
        }
    }

    /// Private method to set an input of this node. It is only meant to be called within
    /// `QueryGraph::replace_node`.
    fn set_input(&mut self, input_idx: usize, node_id: NodeId) {
        assert!(input_idx < self.num_inputs());

        match self {
            Self::Project { input, .. }
            | Self::Filter { input, .. }
            | Self::Aggregate { input, .. } => *input = node_id,
            Self::TableScan { .. } => panic!(),
            Self::Join { left, right, .. } => {
                if input_idx == 0 {
                    *left = node_id
                } else {
                    *right = node_id
                }
            }
            Self::Union { inputs } => inputs[input_idx] = node_id,
        }
    }
}

impl QueryGraph {
    pub fn new() -> QueryGraph {
        Self {
            nodes: HashMap::new(),
            entry_node: 0,
            next_node_id: 0,
            gen_number: 0,
            parents: HashMap::new(),
            property_cache: RefCell::new(PropertyCache::new()),
        }
    }

    pub fn set_entry_node(&mut self, entry_node: NodeId) {
        self.entry_node = entry_node;
    }

    /// Returns a reference to the node under the given ID. The provided ID must
    /// be a valid node ID. Otherwise, it panics.
    pub fn node(&self, node_id: NodeId) -> &QueryNode {
        self.nodes.get(&node_id).unwrap()
    }

    /// Adds a query node to the query graph. Registers the new node as a parent
    /// of its inputs. Returns the ID of the new node added to the query graph.
    pub fn add_node(&mut self, node: QueryNode) -> NodeId {
        // Avoid adding duplicated nodes
        if let Some(existing_node_id) = self.find_node(&node) {
            return existing_node_id;
        }
        let node_id = self.next_node_id;
        for i in 0..node.num_inputs() {
            self.parents
                .entry(node.get_input(i))
                .or_insert_with(|| BTreeSet::new())
                .insert(node_id);
        }
        self.next_node_id += 1;
        self.nodes.insert(node_id, node);
        node_id
    }

    /// Finds whether there is an existing node exactly like the given one.
    fn find_node(&self, node: &QueryNode) -> Option<NodeId> {
        self.nodes.iter().find_map(|(node_id, existing_node)| {
            if *node == *existing_node {
                Some(*node_id)
            } else {
                None
            }
        })
    }

    /// Returns the number of parents of the given node.
    pub fn num_parents(&self, node_id: NodeId) -> usize {
        self.parents.get(&node_id).map(|x| x.len()).unwrap_or(0)
    }

    /// Returns the parents of the given node.
    pub fn get_parents(&self, node_id: NodeId) -> Option<&BTreeSet<NodeId>> {
        self.parents.get(&node_id)
    }

    /// Replaces all the references to `node_id` to make them point to `new_node_id`.
    /// Invalidates the cached metadata for the nodes that are no longer part of the
    /// query graph.
    pub fn replace_node(&mut self, node_id: NodeId, new_node_id: NodeId) {
        self.invalidate_properties_upwards(node_id);

        // All the parents of the old node are now parents of the new one
        // unless the parent is the new node
        if let Some(mut parents) = self.parents.remove(&node_id) {
            for parent_id in parents.iter() {
                let parent_node = self.nodes.get_mut(&parent_id).unwrap();
                if *parent_id != new_node_id {
                    for input in 0..parent_node.num_inputs() {
                        if parent_node.get_input(input) == node_id {
                            parent_node.set_input(input, new_node_id);
                        }
                    }
                }
            }

            // Keep the new node as a parent of the old node
            if parents.remove(&new_node_id) {
                self.parents.insert(node_id, BTreeSet::from([new_node_id]));
            }

            if let Some(new_node_parents) = self.parents.get_mut(&new_node_id) {
                new_node_parents.extend(parents);
            } else {
                self.parents.insert(new_node_id, parents);
            }
        }
        // Replace the reference to the entry node as well
        if self.entry_node == node_id {
            self.entry_node = new_node_id;
        }

        self.remove_detached_nodes(node_id);
        self.gen_number += 1;
    }

    pub fn garbage_collect(&mut self) {
        let mut visited_nodes = HashSet::new();
        let mut stack = vec![self.entry_node];
        while !stack.is_empty() {
            let current = stack.pop().unwrap();
            if visited_nodes.insert(current) {
                let node = self.node(current);
                for input_idx in 0..node.num_inputs() {
                    stack.push(node.get_input(input_idx));
                }
            }
        }

        self.nodes = self
            .nodes
            .drain()
            .filter(|(x, _)| visited_nodes.contains(x))
            .collect();
    }
}

/// Useful node construction methods.
impl QueryGraph {
    pub fn table_scan(&mut self, table_id: usize, num_columns: usize) -> NodeId {
        self.add_node(QueryNode::TableScan {
            table_id,
            num_columns,
        })
    }

    pub fn filter(&mut self, input: NodeId, conditions: Vec<ScalarExprRef>) -> NodeId {
        if conditions.is_empty() {
            input
        } else {
            self.add_node(QueryNode::Filter { conditions, input })
        }
    }

    pub fn project(&mut self, input: NodeId, outputs: Vec<ScalarExprRef>) -> NodeId {
        self.add_node(QueryNode::Project { outputs, input })
    }

    pub fn inner_join(
        &mut self,
        left: NodeId,
        right: NodeId,
        conditions: Vec<ScalarExprRef>,
    ) -> NodeId {
        self.join(JoinType::Inner, left, right, conditions)
    }

    pub fn join(
        &mut self,
        join_type: JoinType,
        left: NodeId,
        right: NodeId,
        conditions: Vec<ScalarExprRef>,
    ) -> NodeId {
        self.add_node(QueryNode::Join {
            join_type,
            left,
            right,
            conditions,
        })
    }
}

impl Clone for QueryGraph {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            entry_node: self.entry_node,
            next_node_id: self.next_node_id,
            gen_number: self.gen_number,
            parents: self.parents.clone(),
            // Cached metadata is not cloned
            property_cache: RefCell::new(PropertyCache::new()),
        }
    }
}

impl QueryGraph {
    fn invalidate_properties_upwards(&mut self, node_id: NodeId) {
        let mut stack = vec![node_id];
        while let Some(current_id) = stack.pop() {
            let prev_size = stack.len();
            if let Some(parents) = self.parents.get(&current_id) {
                stack.extend(parents.iter());
            }

            for idx in prev_size..stack.len() {
                self.invalidate_node_properties(stack[idx]);
            }
        }
    }

    fn invalidate_node_properties(&mut self, node_id: NodeId) {
        self.property_cache
            .borrow_mut()
            .invalidate_node_properties(node_id)
    }

    /// Removes the nodes under the given one that are no longer
    /// attached to the plan.
    fn remove_detached_nodes(&mut self, node_id: NodeId) {
        let mut stack = vec![node_id];
        while let Some(current_id) = stack.pop() {
            if !self.parents.contains_key(&current_id) {
                self.invalidate_node_properties(current_id);

                let current_node = self.nodes.get(&current_id).unwrap();

                // A node may point more than once to the same input node
                let distinct_children = (0..current_node.num_inputs())
                    .map(|input| current_node.get_input(input))
                    .collect::<HashSet<_>>();

                // Unregister the current node as the parent of its inputs
                for child_id in distinct_children.iter() {
                    if let Some(parents) = self.parents.get_mut(child_id) {
                        parents.remove(&current_id);
                        if parents.is_empty() {
                            self.parents.remove(child_id);
                        }
                    }
                }
                self.nodes.remove(&current_id);
            }
        }
    }
}

impl JoinType {
    pub fn name(&self) -> &str {
        match self {
            JoinType::Inner => "Inner",
            JoinType::LeftOuter => "Left Outer",
            JoinType::RightOuter => "Right Outer",
            JoinType::FullOuter => "Full Outer",
            JoinType::Semi => "Semi",
            JoinType::Anti => "Anti",
        }
    }

    pub fn projects_columns_from_left(&self) -> bool {
        match self {
            JoinType::RightOuter
            | JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::FullOuter
            | JoinType::Semi
            | JoinType::Anti => true,
        }
    }

    pub fn projects_columns_from_right(&self) -> bool {
        match self {
            JoinType::RightOuter | JoinType::Inner | JoinType::LeftOuter | JoinType::FullOuter => {
                true
            }
            JoinType::Semi | JoinType::Anti => false,
        }
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use crate::scalar_expr::ScalarExpr;

    use super::*;

    #[test]
    fn test_add_node() {
        let mut query_graph = QueryGraph::new();
        let table_scan_id_1 = query_graph.table_scan(0, 10);
        let table_scan_id_2 = query_graph.table_scan(0, 10);
        assert_eq!(table_scan_id_1, table_scan_id_2);

        let project_id_1 = query_graph.project(
            table_scan_id_1,
            (0..10).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );
        let project_id_2 = query_graph.project(
            table_scan_id_2,
            (0..10).map(|i| ScalarExpr::input_ref(i).into()).collect(),
        );
        assert_eq!(project_id_1, project_id_2);

        let project_id_1 = query_graph.project(
            table_scan_id_1,
            vec![ScalarExpr::string_literal("value".to_owned()).into()],
        );
        let project_id_2 = query_graph.project(
            table_scan_id_2,
            vec![ScalarExpr::string_literal("value".to_owned()).into()],
        );
        assert_eq!(project_id_1, project_id_2);
    }
}
