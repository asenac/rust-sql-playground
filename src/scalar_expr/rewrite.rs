//! Module containing expressions rewrites.
//!
//! All rewrites here tend to return the original expression if nothing changed.

use std::collections::{BTreeSet, HashMap};

use crate::scalar_expr::equivalence_class::*;
use crate::scalar_expr::visitor::*;
use crate::scalar_expr::*;
use crate::visitor_utils::PostOrderVisitationResult;
use crate::visitor_utils::PreOrderVisitationResult;

/// Clones the given expression with the given inputs unless they are the same
/// inputs it already has. In such case, it just returns the original expression.
fn clone_expr_if_needed(mut expr: ScalarExprRef, new_inputs: &[ScalarExprRef]) -> ScalarExprRef {
    let num_inputs = new_inputs.len();
    assert!(num_inputs == expr.num_inputs());

    if num_inputs > 0 {
        // Avoid cloning if not needed
        if !(0..num_inputs)
            .map(|x| expr.get_input(x))
            .zip(new_inputs.iter())
            .all(|(original, new)| {
                // Just compare pointers
                &*original as *const ScalarExpr == &**new as *const ScalarExpr
            })
        {
            expr = expr.clone_with_new_inputs(new_inputs).to_ref();
        }
    }
    expr
}

// Post-order rewrites

struct ScalarExprRewriterPost<'a, F>
where
    F: FnMut(&ScalarExprRef) -> Option<ScalarExprRef>,
{
    stack: Vec<ScalarExprRef>,
    rewrite: &'a mut F,
}

impl<'a, F> ScalarExprRewriterPost<'a, F>
where
    F: FnMut(&ScalarExprRef) -> Option<ScalarExprRef>,
{
    fn new(rewrite: &'a mut F) -> Self {
        Self {
            stack: Vec::new(),
            rewrite,
        }
    }
}

impl<F> ScalarExprPrePostVisitor for ScalarExprRewriterPost<'_, F>
where
    F: FnMut(&ScalarExprRef) -> Option<ScalarExprRef>,
{
    fn visit_pre(&mut self, _: &ScalarExprRef) -> PreOrderVisitationResult {
        PreOrderVisitationResult::VisitInputs
    }

    fn visit_post(&mut self, expr: &ScalarExprRef) -> PostOrderVisitationResult {
        let num_inputs = expr.num_inputs();
        let new_inputs = &self.stack[self.stack.len() - num_inputs..];
        let mut curr_expr = clone_expr_if_needed(expr.clone(), new_inputs);
        self.stack.truncate(self.stack.len() - num_inputs);
        if let Some(rewritten_expr) = (self.rewrite)(&curr_expr) {
            curr_expr = rewritten_expr;
        }
        self.stack.push(curr_expr);
        PostOrderVisitationResult::Continue
    }
}

/// Applies a post-order rewrite to the given expression.
pub fn rewrite_scalar_expr_post<F>(rewrite: &mut F, expr: &ScalarExprRef) -> ScalarExprRef
where
    F: FnMut(&ScalarExprRef) -> Option<ScalarExprRef>,
{
    let mut visitor = ScalarExprRewriterPost::new(rewrite);
    visit_scalar_expr(expr, &mut visitor);
    assert!(visitor.stack.len() == 1);
    visitor.stack.into_iter().next().unwrap()
}

/// Replaces the input refs in the given expression with the expression in the corresponding
/// position of the given vector of expressions.
pub fn dereference_scalar_expr(expr: &ScalarExprRef, map: &Vec<ScalarExprRef>) -> ScalarExprRef {
    rewrite_scalar_expr_post(
        &mut |expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = expr.as_ref() {
                return Some(map[*index].clone());
            }
            None
        },
        expr,
    )
}

pub fn shift_right_input_refs(expr: &ScalarExprRef, offset: usize) -> ScalarExprRef {
    rewrite_scalar_expr_post(
        &mut |expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = expr.as_ref() {
                return Some(ScalarExpr::input_ref(index + offset).to_ref());
            }
            None
        },
        expr,
    )
}

pub fn shift_left_input_refs(expr: &ScalarExprRef, offset: usize) -> ScalarExprRef {
    rewrite_scalar_expr_post(
        &mut |expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = expr.as_ref() {
                return Some(ScalarExpr::input_ref(index - offset).to_ref());
            }
            None
        },
        expr,
    )
}

/// Applies a rewrite to all expression within the given vector.
pub fn rewrite_scalar_expr_vec<F>(exprs: &Vec<ScalarExprRef>, rewrite: &mut F) -> Vec<ScalarExprRef>
where
    F: FnMut(&ScalarExprRef) -> ScalarExprRef,
{
    exprs.iter().map(|e| rewrite(e)).collect()
}

// Pre-order rewrites

struct ScalarExprRewriterPre<'a, F>
where
    F: FnMut(&ScalarExprRef) -> Result<Option<ScalarExprRef>, ()>,
{
    stack: Vec<ScalarExprRef>,
    rewrite: &'a mut F,
    skip_post: bool,
}

impl<'a, F> ScalarExprRewriterPre<'a, F>
where
    F: FnMut(&ScalarExprRef) -> Result<Option<ScalarExprRef>, ()>,
{
    fn new(rewrite: &'a mut F) -> Self {
        Self {
            stack: Vec::new(),
            rewrite,
            skip_post: false,
        }
    }
}

impl<F> ScalarExprPrePostVisitor for ScalarExprRewriterPre<'_, F>
where
    F: FnMut(&ScalarExprRef) -> Result<Option<ScalarExprRef>, ()>,
{
    fn visit_pre(&mut self, expr: &ScalarExprRef) -> PreOrderVisitationResult {
        match (self.rewrite)(expr) {
            Ok(Some(rewritten_expr)) => {
                self.stack.push(rewritten_expr);
                self.skip_post = true;
                PreOrderVisitationResult::DoNotVisitInputs
            }
            Ok(None) => PreOrderVisitationResult::VisitInputs,
            Err(_) => {
                self.stack.clear();
                PreOrderVisitationResult::Abort
            }
        }
    }

    fn visit_post(&mut self, expr: &ScalarExprRef) -> PostOrderVisitationResult {
        if self.skip_post {
            self.skip_post = false;
            return PostOrderVisitationResult::Continue;
        }
        let num_inputs = expr.num_inputs();
        let new_inputs = &self.stack[self.stack.len() - num_inputs..];
        let curr_expr = clone_expr_if_needed(expr.clone(), new_inputs);
        self.stack.truncate(self.stack.len() - num_inputs);
        self.stack.push(curr_expr);
        return PostOrderVisitationResult::Continue;
    }
}

/// Applies pre-order a rewrite to the given expression.
///
/// Returns None if the rewrite failed.
pub fn rewrite_scalar_expr_pre<F>(rewrite: &mut F, expr: &ScalarExprRef) -> Option<ScalarExprRef>
where
    F: FnMut(&ScalarExprRef) -> Result<Option<ScalarExprRef>, ()>,
{
    let mut visitor = ScalarExprRewriterPre::new(rewrite);
    visit_scalar_expr(expr, &mut visitor);
    visitor.stack.into_iter().next()
}

/// Tries to lift the given expression through the given projection.
/// Fails if an input ref expression not included in the projection is reached.
pub fn lift_scalar_expr(expr: &ScalarExprRef, proj: &Vec<ScalarExprRef>) -> Option<ScalarExprRef> {
    rewrite_scalar_expr_pre(
        &mut |expr: &ScalarExprRef| {
            if let Some(proj_col) = proj
                .iter()
                .enumerate()
                .find(|(_, proj_expr)| **proj_expr == *expr)
                .map(|(i, _)| i)
            {
                return Ok(Some(ScalarExpr::input_ref(proj_col).to_ref()));
            }
            if let ScalarExpr::InputRef { .. } = expr.as_ref() {
                Err(())
            } else {
                Ok(None)
            }
        },
        expr,
    )
}

pub fn lift_scalar_expr_2(
    expr: &ScalarExprRef,
    proj: &HashMap<ScalarExprRef, usize>,
) -> Option<ScalarExprRef> {
    rewrite_scalar_expr_pre(
        &mut |expr: &ScalarExprRef| {
            if let Some(proj_col) = proj
                .iter()
                .find(|(proj_expr, _)| **proj_expr == *expr)
                .map(|(_, i)| *i)
            {
                return Ok(Some(ScalarExpr::input_ref(proj_col).to_ref()));
            }
            if let ScalarExpr::InputRef { .. } = expr.as_ref() {
                Err(())
            } else {
                Ok(None)
            }
        },
        expr,
    )
}

pub fn to_column_map_for_expr_lifting(set: &BTreeSet<usize>) -> HashMap<usize, usize> {
    set.iter()
        .enumerate()
        .map(|(out_col, in_col)| (*in_col, out_col))
        .collect()
}

pub fn to_column_map_for_expr_push_down(set: &BTreeSet<usize>) -> HashMap<usize, usize> {
    set.iter()
        .enumerate()
        .map(|(out_col, in_col)| (out_col, *in_col))
        .collect()
}

pub fn apply_column_map(
    expr: &ScalarExprRef,
    column_map: &HashMap<usize, usize>,
) -> Option<ScalarExprRef> {
    rewrite_scalar_expr_pre(
        &mut |expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = expr.as_ref() {
                if let Some(mapped_index) = column_map.get(index) {
                    Ok(Some(ScalarExpr::input_ref(*mapped_index).to_ref()))
                } else {
                    Err(())
                }
            } else {
                Ok(None)
            }
        },
        expr,
    )
}

pub fn normalize_scalar_expr(expr: &ScalarExprRef, classes: &EquivalenceClasses) -> ScalarExprRef {
    rewrite_scalar_expr_pre(
        &mut |expr: &ScalarExprRef| {
            if let Some(class_id) = find_class(classes, expr) {
                let representative = classes[class_id].members.first().unwrap();
                if *representative != *expr {
                    return Ok(Some(representative.clone()));
                }
            }
            Ok(None)
        },
        expr,
    )
    .unwrap()
}

/// Applies the replacements in the given map in pre-order.
pub fn replace_sub_expressions_pre(
    expr: &ScalarExprRef,
    replacement_map: &HashMap<ScalarExprRef, ScalarExprRef>,
) -> ScalarExprRef {
    rewrite_scalar_expr_pre(
        &mut |expr: &ScalarExprRef| {
            if let Some(replacement) = replacement_map.get(expr) {
                return Ok(Some(replacement.clone()));
            }
            Ok(None)
        },
        expr,
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use crate::scalar_expr::{rewrite::lift_scalar_expr, *};

    #[test]
    fn test_lift_scalar_expr() {
        let proj = vec![
            ScalarExpr::input_ref(2).to_ref(),
            ScalarExpr::nary(
                NaryOp::Concat,
                vec![
                    ScalarExpr::input_ref(1).to_ref(),
                    ScalarExpr::input_ref(3).to_ref(),
                ],
            )
            .to_ref(),
        ];

        let tests = vec![
            (
                ScalarExpr::input_ref(2).to_ref(),
                Some(ScalarExpr::input_ref(0).to_ref()),
            ),
            (
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(2).to_ref(),
                        ScalarExpr::input_ref(3).to_ref(),
                    ],
                )
                .to_ref(),
                None,
            ),
            (
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(2).to_ref(),
                        ScalarExpr::input_ref(2).to_ref(),
                    ],
                )
                .to_ref(),
                Some(
                    ScalarExpr::nary(
                        NaryOp::Concat,
                        vec![
                            ScalarExpr::input_ref(0).to_ref(),
                            ScalarExpr::input_ref(0).to_ref(),
                        ],
                    )
                    .to_ref(),
                ),
            ),
        ];

        for (expr, expected) in tests {
            let lifted_expr = lift_scalar_expr(&expr, &proj);
            assert_eq!(lifted_expr, expected);
        }
    }
}
