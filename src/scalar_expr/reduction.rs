//! Utilities for reducing scalar expression

use itertools::Itertools;

use crate::{
    data_type::DataType,
    query_graph::{properties::num_columns, QueryGraph},
};

use super::{rewrite::rewrite_expr_pre_post, NaryOp, ScalarExpr, ScalarExprRef, Subquery};

/// Reduce the given expression recursively. Keeps trying until the expression cannot
/// be reduced any further.
pub fn reduce_expr_recursively(
    expr: &ScalarExprRef,
    query_graph: &QueryGraph,
    row_type: &[DataType],
) -> ScalarExprRef {
    rewrite_expr_pre_post(
        &mut |curr_expr: &ScalarExprRef| reduce_expr(curr_expr, query_graph, row_type),
        &expr,
    )
}

pub fn reduce_expr(
    expr: &ScalarExprRef,
    query_graph: &QueryGraph,
    row_type: &[DataType],
) -> Option<ScalarExprRef> {
    if let ScalarExpr::NaryOp {
        op: NaryOp::And,
        operands,
    } = expr.as_ref()
    {
        if operands.iter().any(|o| **o == ScalarExpr::false_literal()) {
            return Some(ScalarExpr::false_literal().into());
        }
        if operands.iter().any(|o| **o == ScalarExpr::true_literal()) {
            let new_operands = operands
                .iter()
                .filter(|o| *o.as_ref() == ScalarExpr::true_literal())
                .dedup()
                .cloned()
                .collect_vec();
            return Some(match new_operands.len() {
                0 => ScalarExpr::true_literal().into(),
                1 => new_operands[0].clone(),
                _ => ScalarExpr::nary(NaryOp::And, new_operands).into(),
            });
        }
    }
    if let ScalarExpr::NaryOp {
        op: NaryOp::Or,
        operands,
    } = expr.as_ref()
    {
        if operands.iter().any(|o| **o == ScalarExpr::true_literal()) {
            return Some(ScalarExpr::true_literal().into());
        }
        if operands.iter().any(|o| **o == ScalarExpr::false_literal()) {
            let new_operands = operands
                .iter()
                .filter(|o| *o.as_ref() == ScalarExpr::false_literal())
                .dedup()
                .cloned()
                .collect_vec();
            return Some(match new_operands.len() {
                0 => ScalarExpr::false_literal().into(),
                1 => new_operands[0].clone(),
                _ => ScalarExpr::nary(NaryOp::And, new_operands).into(),
            });
        }
    }
    if let ScalarExpr::BinaryOp { op, left, right } = expr.as_ref() {
        if op.propagates_null() && (left.is_null() || right.is_null()) {
            return Some(ScalarExpr::null_literal(expr.data_type(query_graph, row_type)).into());
        }
    }
    None
}

pub fn reduce_and_prune_exists_subplans_recursively(
    expr: &ScalarExprRef,
    query_graph: &mut QueryGraph,
    row_type: &[DataType],
) -> ScalarExprRef {
    rewrite_expr_pre_post(
        &mut |curr_expr: &ScalarExprRef| {
            prune_exists_subplan(curr_expr, query_graph)
                .or_else(|| reduce_expr(curr_expr, query_graph, row_type))
        },
        &expr,
    )
}

pub fn prune_exists_subplan(
    expr: &ScalarExprRef,
    query_graph: &mut QueryGraph,
) -> Option<ScalarExprRef> {
    if let ScalarExpr::ExistsSubquery { subquery } = expr.as_ref() {
        if num_columns(query_graph, subquery.root) > 0 {
            // Skip the root node
            let subquery_plan = query_graph.node(subquery.root).get_input(0);
            let correlation = subquery.correlation.clone();
            let project = query_graph.project(subquery_plan, vec![]);
            let new_subquery_root = query_graph.add_subquery(project);
            return Some(
                ScalarExpr::ExistsSubquery {
                    subquery: Subquery {
                        root: new_subquery_root,
                        correlation,
                    },
                }
                .into(),
            );
        }
    }

    None
}
