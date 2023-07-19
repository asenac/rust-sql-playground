use sqlparser::ast;

use crate::query_graph::QueryGraph;

pub trait CatalogObject {
    fn name(&self) -> String;
}

pub trait Table: CatalogObject {}

pub trait View: CatalogObject {}

pub trait Schema: CatalogObject {
    fn find_table(&self, name: &str) -> Option<Box<dyn Table>>;
}

pub trait Database: CatalogObject {
    fn find_schema(&self, name: &str) -> Option<Box<dyn Schema>>;
}

pub trait DatabaseCatalog {
    fn find_database(&self, name: &str) -> Option<Box<dyn Database>>;
}

pub struct Converter<'a> {
    catalog: &'a dyn DatabaseCatalog,
}

impl<'a> Converter<'a> {
    pub fn new(catalog: &'a dyn DatabaseCatalog) -> Self {
        Self { catalog }
    }

    pub fn process_query(&mut self, query: &ast::Query) -> Result<QueryGraph, String> {
        Err("here".to_owned())
    }
}
