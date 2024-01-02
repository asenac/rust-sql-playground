//! Utilities for reducing scalar expression

use itertools::Itertools;

use crate::{data_type::DataType, query_graph::QueryGraph};

use super::{rewrite::rewrite_expr_pre_post, NaryOp, ScalarExpr, ScalarExprRef};

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
