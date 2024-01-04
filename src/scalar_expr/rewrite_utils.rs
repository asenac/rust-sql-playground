use std::collections::HashMap;

use crate::query_graph::{CorrelationId, NodeId};

use super::{ScalarExpr, ScalarExprRef};

/// If the given expression is a subquery expression, it updates the ID of the
/// subquery root node using the given map.
pub fn apply_subquery_map(
    expr: &ScalarExprRef,
    subquery_map: &HashMap<NodeId, NodeId>,
) -> Option<ScalarExprRef> {
    match expr.as_ref() {
        ScalarExpr::ScalarSubquery { subquery } => {
            subquery_map.get(&subquery).map(|new_subquery| {
                ScalarExpr::ScalarSubquery {
                    subquery: *new_subquery,
                }
                .into()
            })
        }
        ScalarExpr::ExistsSubquery { subquery } => {
            subquery_map.get(&subquery).map(|new_subquery| {
                ScalarExpr::ExistsSubquery {
                    subquery: *new_subquery,
                }
                .into()
            })
        }
        ScalarExpr::ScalarSubqueryCmp {
            op,
            scalar_operand,
            subquery,
        } => subquery_map.get(&subquery).map(|new_subquery| {
            ScalarExpr::ScalarSubqueryCmp {
                op: op.clone(),
                scalar_operand: scalar_operand.clone(),
                subquery: *new_subquery,
            }
            .into()
        }),
        _ => None,
    }
}

/// If the given expression is a correlated reference pointing to `old_correlation_id`,
/// it is rewritten to make it point to `new_correlation_id`.
pub fn update_correlated_reference(
    expr: &ScalarExprRef,
    correlation_id: CorrelationId,
    column_map: &HashMap<usize, usize>,
) -> Option<ScalarExprRef> {
    if let ScalarExpr::CorrelatedInputRef {
        correlation_id: expr_correlation_id,
        index,
        data_type,
    } = expr.as_ref()
    {
        if *expr_correlation_id == correlation_id {
            return Some(
                ScalarExpr::CorrelatedInputRef {
                    correlation_id,
                    index: *column_map.get(index).unwrap(),
                    data_type: data_type.clone(),
                }
                .into(),
            );
        }
    }
    None
}

/// If the given expression is a correlated reference pointing to `old_correlation_id`,
/// it is rewritten to make it point to `new_correlation_id`.
pub fn update_correlation_id(
    expr: &ScalarExprRef,
    old_correlation_id: CorrelationId,
    new_correlation_id: CorrelationId,
) -> Option<ScalarExprRef> {
    if let ScalarExpr::CorrelatedInputRef {
        correlation_id,
        index,
        data_type,
    } = expr.as_ref()
    {
        if *correlation_id == old_correlation_id {
            return Some(
                ScalarExpr::CorrelatedInputRef {
                    correlation_id: new_correlation_id,
                    index: *index,
                    data_type: data_type.clone(),
                }
                .into(),
            );
        }
    }
    None
}

pub fn apply_column_map_to_correlated_reference(
    expr: &ScalarExprRef,
    correlation_id: CorrelationId,
    column_map: &HashMap<usize, usize>,
) -> Option<ScalarExprRef> {
    if let ScalarExpr::CorrelatedInputRef {
        correlation_id: input_correlation_id,
        index,
        data_type,
    } = expr.as_ref()
    {
        if *input_correlation_id == correlation_id {
            return Some(
                ScalarExpr::CorrelatedInputRef {
                    correlation_id,
                    index: *column_map.get(index).unwrap(),
                    data_type: data_type.clone(),
                }
                .into(),
            );
        }
    }
    None
}

pub fn apply_column_map_to_input_ref(
    expr: &ScalarExprRef,
    column_map: &HashMap<usize, usize>,
) -> Option<ScalarExprRef> {
    if let ScalarExpr::InputRef { index } = expr.as_ref() {
        return Some(
            ScalarExpr::InputRef {
                index: *column_map.get(index).unwrap(),
            }
            .into(),
        );
    }
    None
}
