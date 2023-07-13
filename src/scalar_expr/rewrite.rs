//! Module containing expressions rewrites.
//!
//! All rewrites here tend to return the original expression if nothing changed.

use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;

use crate::scalar_expr::equivalence_class::*;
use crate::scalar_expr::visitor::*;
use crate::scalar_expr::*;
use crate::visitor_utils::PostOrderVisitationResult;
use crate::visitor_utils::PreOrderVisitationResult;

pub trait RewritableExpr: Sized + VisitableExpr + Eq + Hash {
    /// Creates a clone of the given expression but whose inputs will be
    /// the given ones. Used for doing copy-on-write when rewriting expressions.
    fn clone_with_new_inputs(&self, inputs: &[Rc<Self>]) -> Rc<Self>;
}

// Post-order rewrites

struct ExprRewriterPost<'a, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Option<Rc<E>>,
{
    stack: Vec<Rc<E>>,
    rewrite: &'a mut F,
}

impl<'a, F, E> ExprRewriterPost<'a, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Option<Rc<E>>,
{
    fn new(rewrite: &'a mut F) -> Self {
        Self {
            stack: Vec::new(),
            rewrite,
        }
    }
}

impl<F, E> ExprPrePostVisitor<E> for ExprRewriterPost<'_, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Option<Rc<E>>,
{
    fn visit_pre(&mut self, _: &Rc<E>) -> PreOrderVisitationResult {
        PreOrderVisitationResult::VisitInputs
    }

    fn visit_post(&mut self, expr: &Rc<E>) -> PostOrderVisitationResult {
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
pub fn rewrite_expr_post<F, E>(rewrite: &mut F, expr: &Rc<E>) -> Rc<E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Option<Rc<E>>,
{
    let mut visitor = ExprRewriterPost::new(rewrite);
    visit_expr(expr, &mut visitor);
    assert!(visitor.stack.len() == 1);
    visitor.stack.into_iter().next().unwrap()
}

/// Replaces the input refs in the given expression with the expression in the corresponding
/// position of the given vector of expressions.
pub fn dereference_scalar_expr(expr: &ScalarExprRef, map: &Vec<ScalarExprRef>) -> ScalarExprRef {
    rewrite_expr_post(
        &mut |expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = expr.as_ref() {
                return Some(map[*index].clone());
            }
            None
        },
        expr,
    )
}

pub fn dereference_extended_scalar_expr(
    expr: &ExtendedScalarExprRef,
    map: &Vec<ExtendedScalarExprRef>,
) -> ExtendedScalarExprRef {
    rewrite_expr_post(
        &mut |expr: &ExtendedScalarExprRef| {
            if let ExtendedScalarExpr::InputRef { index } = expr.as_ref() {
                return Some(map[*index].clone());
            }
            None
        },
        expr,
    )
}

pub fn shift_right_input_refs(expr: &ScalarExprRef, offset: usize) -> ScalarExprRef {
    rewrite_expr_post(
        &mut |expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = expr.as_ref() {
                return Some(ScalarExpr::input_ref(index + offset).into());
            }
            None
        },
        expr,
    )
}

pub fn shift_left_input_refs(expr: &ScalarExprRef, offset: usize) -> ScalarExprRef {
    rewrite_expr_post(
        &mut |expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = expr.as_ref() {
                return Some(ScalarExpr::input_ref(index - offset).into());
            }
            None
        },
        expr,
    )
}

/// Applies a rewrite to all expression within the given vector.
pub fn rewrite_expr_vec<F, E>(exprs: &Vec<Rc<E>>, rewrite: &mut F) -> Vec<Rc<E>>
where
    E: VisitableExpr,
    F: FnMut(&Rc<E>) -> Rc<E>,
{
    exprs.iter().map(|e| rewrite(e)).collect()
}

// Pre-order rewrites

struct ExprRewriterPre<'a, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Result<Option<Rc<E>>, ()>,
{
    stack: Vec<Rc<E>>,
    rewrite: &'a mut F,
    skip_post: bool,
}

impl<'a, F, E> ExprRewriterPre<'a, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Result<Option<Rc<E>>, ()>,
{
    fn new(rewrite: &'a mut F) -> Self {
        Self {
            stack: Vec::new(),
            rewrite,
            skip_post: false,
        }
    }
}

impl<F, E> ExprPrePostVisitor<E> for ExprRewriterPre<'_, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Result<Option<Rc<E>>, ()>,
{
    fn visit_pre(&mut self, expr: &Rc<E>) -> PreOrderVisitationResult {
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

    fn visit_post(&mut self, expr: &Rc<E>) -> PostOrderVisitationResult {
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
pub fn rewrite_expr_pre<F, E>(rewrite: &mut F, expr: &Rc<E>) -> Option<Rc<E>>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Result<Option<Rc<E>>, ()>,
{
    let mut visitor = ExprRewriterPre::new(rewrite);
    visit_expr(expr, &mut visitor);
    visitor.stack.into_iter().next()
}

/// Tries to lift the given expression through the given projection.
/// Fails if an input ref expression not included in the projection is reached.
pub fn lift_scalar_expr(expr: &ScalarExprRef, proj: &Vec<ScalarExprRef>) -> Option<ScalarExprRef> {
    rewrite_expr_pre(
        &mut |expr: &ScalarExprRef| {
            if let Some(proj_col) = proj
                .iter()
                .enumerate()
                .find(|(_, proj_expr)| **proj_expr == *expr)
                .map(|(i, _)| i)
            {
                return Ok(Some(ScalarExpr::input_ref(proj_col).into()));
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
    rewrite_expr_pre(
        &mut |expr: &ScalarExprRef| {
            if let Some(proj_col) = proj
                .iter()
                .find(|(proj_expr, _)| **proj_expr == *expr)
                .map(|(_, i)| *i)
            {
                return Ok(Some(ScalarExpr::input_ref(proj_col).into()));
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
    rewrite_expr_pre(
        &mut |expr: &ScalarExprRef| {
            if let ScalarExpr::InputRef { index } = expr.as_ref() {
                if let Some(mapped_index) = column_map.get(index) {
                    Ok(Some(ScalarExpr::input_ref(*mapped_index).into()))
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
    rewrite_expr_pre(
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
pub fn replace_sub_expressions_pre<E: RewritableExpr>(
    expr: &Rc<E>,
    replacement_map: &HashMap<Rc<E>, Rc<E>>,
) -> Rc<E> {
    rewrite_expr_pre(
        &mut |expr: &Rc<E>| {
            if let Some(replacement) = replacement_map.get(expr) {
                return Ok(Some(replacement.clone()));
            }
            Ok(None)
        },
        expr,
    )
    .unwrap()
}

struct ExprRewriterPrePost<'a, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Option<Rc<E>>,
{
    stack: Vec<Rc<E>>,
    skip_post: bool,
    rewrite: &'a mut F,
}

impl<'a, F, E> ExprRewriterPrePost<'a, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Option<Rc<E>>,
{
    fn new(rewrite: &'a mut F) -> Self {
        Self {
            stack: Vec::new(),
            skip_post: false,
            rewrite,
        }
    }
}

impl<F, E> ExprPrePostVisitor<E> for ExprRewriterPrePost<'_, F, E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Option<Rc<E>>,
{
    fn visit_pre(&mut self, expr: &Rc<E>) -> PreOrderVisitationResult {
        match (self.rewrite)(expr) {
            Some(rewritten_expr) => {
                self.stack.push(rewritten_expr);
                self.skip_post = true;
                PreOrderVisitationResult::DoNotVisitInputs
            }
            None => PreOrderVisitationResult::VisitInputs,
        }
    }

    fn visit_post(&mut self, expr: &Rc<E>) -> PostOrderVisitationResult {
        if !self.skip_post {
            let num_inputs = expr.num_inputs();
            let new_inputs = &self.stack[self.stack.len() - num_inputs..];
            let mut curr_expr = clone_expr_if_needed(expr.clone(), new_inputs);
            self.stack.truncate(self.stack.len() - num_inputs);
            if let Some(rewritten_expr) = (self.rewrite)(&curr_expr) {
                curr_expr = rewritten_expr;
            }
            self.stack.push(curr_expr);
        } else {
            self.skip_post = false;
        }
        PostOrderVisitationResult::Continue
    }
}

/// Applies a rewrite during both the pre-order and the post-order part of the
/// expression traversal.
pub fn rewrite_expr_pre_post<F, E>(rewrite: &mut F, expr: &Rc<E>) -> Rc<E>
where
    E: RewritableExpr,
    F: FnMut(&Rc<E>) -> Option<Rc<E>>,
{
    let mut visitor = ExprRewriterPrePost::new(rewrite);
    visit_expr(expr, &mut visitor);
    assert!(visitor.stack.len() == 1);
    visitor.stack.into_iter().next().unwrap()
}

/// Clones the given expression with the given inputs unless they are the same
/// inputs it already has. In such case, it just returns the original expression.
fn clone_expr_if_needed<E: RewritableExpr>(mut expr: Rc<E>, new_inputs: &[Rc<E>]) -> Rc<E> {
    let num_inputs = new_inputs.len();
    assert!(num_inputs == expr.num_inputs());

    if num_inputs > 0 {
        // Avoid cloning if not needed
        if !(0..num_inputs)
            .map(|x| expr.get_input(x))
            .zip(new_inputs.iter())
            .all(|(original, new)| {
                // Just compare pointers
                &*original as *const E == &**new as *const E
            })
        {
            expr = expr.clone_with_new_inputs(new_inputs);
        }
    }
    expr
}

impl RewritableExpr for ScalarExpr {
    fn clone_with_new_inputs(&self, inputs: &[ScalarExprRef]) -> ScalarExprRef {
        assert!(inputs.len() == self.num_inputs());
        match self {
            ScalarExpr::BinaryOp { op, .. } => ScalarExpr::BinaryOp {
                op: op.clone(),
                left: inputs[0].clone(),
                right: inputs[1].clone(),
            },
            ScalarExpr::NaryOp { op, .. } => ScalarExpr::NaryOp {
                op: op.clone(),
                operands: inputs.to_vec(),
            },
            ScalarExpr::Literal { .. } | ScalarExpr::InputRef { .. } => panic!(),
        }
        .into()
    }
}

impl RewritableExpr for ExtendedScalarExpr {
    fn clone_with_new_inputs(&self, inputs: &[ExtendedScalarExprRef]) -> ExtendedScalarExprRef {
        assert!(inputs.len() == self.num_inputs());
        match self {
            ExtendedScalarExpr::BinaryOp { op, .. } => ExtendedScalarExpr::BinaryOp {
                op: op.clone(),
                left: inputs[0].clone(),
                right: inputs[1].clone(),
            },
            ExtendedScalarExpr::NaryOp { op, .. } => ExtendedScalarExpr::NaryOp {
                op: op.clone(),
                operands: inputs.to_vec(),
            },
            ExtendedScalarExpr::Literal { .. } | ExtendedScalarExpr::InputRef { .. } => panic!(),
            ExtendedScalarExpr::Aggregate { op, .. } => ExtendedScalarExpr::Aggregate {
                op: op.clone(),
                operands: inputs.to_vec(),
            },
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use crate::scalar_expr::{rewrite::lift_scalar_expr, *};

    #[test]
    fn test_lift_scalar_expr() {
        let proj = vec![
            ScalarExpr::input_ref(2).into(),
            ScalarExpr::nary(
                NaryOp::Concat,
                vec![
                    ScalarExpr::input_ref(1).into(),
                    ScalarExpr::input_ref(3).into(),
                ],
            )
            .into(),
        ];

        let tests = vec![
            (
                ScalarExpr::input_ref(2).into(),
                Some(ScalarExpr::input_ref(0).into()),
            ),
            (
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(2).into(),
                        ScalarExpr::input_ref(3).into(),
                    ],
                )
                .into(),
                None,
            ),
            (
                ScalarExpr::nary(
                    NaryOp::Concat,
                    vec![
                        ScalarExpr::input_ref(2).into(),
                        ScalarExpr::input_ref(2).into(),
                    ],
                )
                .into(),
                Some(
                    ScalarExpr::nary(
                        NaryOp::Concat,
                        vec![
                            ScalarExpr::input_ref(0).into(),
                            ScalarExpr::input_ref(0).into(),
                        ],
                    )
                    .into(),
                ),
            ),
        ];

        for (expr, expected) in tests {
            let lifted_expr = lift_scalar_expr(&expr, &proj);
            assert_eq!(lifted_expr, expected);
        }
    }
}
