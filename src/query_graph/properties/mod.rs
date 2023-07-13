//! This module contains the different properties that can be computed from the query graph.
//!
//! Most of these properties are computed bottom-up and contain a lot of boilerplate code that
//! could make use of some generics.
//!
use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use crate::query_graph::NodeId;

mod column_provenance;
mod equivalence_classes;
mod input_dependencies;
mod keys;
mod num_columns;
mod pulled_up_predicates;
mod row_type;

pub use column_provenance::column_provenance;
pub use column_provenance::ColumnProvenanceInfo;
pub use equivalence_classes::equivalence_classes;
pub use input_dependencies::input_dependencies;
pub use keys::empty_key;
pub use keys::keys;
pub use keys::keys_annotator;
pub use keys::unique_key;
pub use num_columns::num_columns;
pub use num_columns::num_columns_annotator;
pub use pulled_up_predicates::pulled_up_predicates;
pub use pulled_up_predicates::pulled_up_predicates_annotator;
pub use row_type::cross_product_row_type;
pub use row_type::row_type;
pub use row_type::row_type_annotator;

use super::QueryGraph;

/// Annotators used for explaining query plans.
pub fn default_annotators() -> Vec<&'static dyn Fn(&QueryGraph, NodeId) -> Option<String>> {
    vec![
        &num_columns_annotator,
        &row_type_annotator,
        &pulled_up_predicates_annotator,
        &keys_annotator,
    ]
}

/// Cache for compute properties
pub struct PropertyCache {
    /// Properties computed in a bottom-up manner.
    bottom_up_properties: HashMap<NodeId, HashMap<TypeId, Box<dyn Any>>>,
}

impl PropertyCache {
    pub fn new() -> Self {
        Self {
            bottom_up_properties: HashMap::new(),
        }
    }

    pub fn node_bottom_up_properties(
        &mut self,
        node_id: NodeId,
    ) -> &mut HashMap<TypeId, Box<dyn Any>> {
        self.bottom_up_properties
            .entry(node_id)
            .or_insert_with(|| HashMap::new())
    }

    pub fn invalidate_node_properties(&mut self, node_id: NodeId) {
        self.bottom_up_properties.remove(&node_id);
    }
}
