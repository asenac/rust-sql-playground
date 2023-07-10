use std::collections::HashSet;

use crate::scalar_expr::*;
use crate::visitor_utils::*;

pub trait ScalarExprPrePostVisitor {
    fn visit_pre(&mut self, expr: &ScalarExprRef) -> PreOrderVisitationResult;
    fn visit_post(&mut self, expr: &ScalarExprRef) -> PostOrderVisitationResult;
}

pub fn visit_scalar_expr<V>(expr: &ScalarExprRef, visitor: &mut V)
where
    V: ScalarExprPrePostVisitor,
{
    let mut stack = vec![VisitationStep::new(expr.clone())];
    while let Some(step) = stack.last_mut() {
        if step.next_child.is_none() {
            match visitor.visit_pre(&step.node) {
                PreOrderVisitationResult::Abort => break,
                PreOrderVisitationResult::VisitInputs => {}
                PreOrderVisitationResult::DoNotVisitInputs => {
                    let result = visitor.visit_post(&step.node);
                    stack.pop();
                    match result {
                        PostOrderVisitationResult::Abort => break,
                        PostOrderVisitationResult::Continue => continue,
                    }
                }
            }
            step.next_child = Some(0);
        }

        if step.next_child.unwrap() < step.node.num_inputs() {
            let input_idx = step.next_child.unwrap();
            step.next_child = Some(input_idx + 1);
            let input_expr = step.node.get_input(input_idx);
            stack.push(VisitationStep::new(input_expr));
            continue;
        }

        let result = visitor.visit_post(&step.node);
        stack.pop();
        match result {
            PostOrderVisitationResult::Abort => break,
            PostOrderVisitationResult::Continue => {}
        }
    }
}
struct ScalarExprPreVisitor<'a, F>
where
    F: FnMut(&ScalarExprRef) -> PreOrderVisitationResult,
{
    visitor: &'a mut F,
}

impl<F> ScalarExprPrePostVisitor for ScalarExprPreVisitor<'_, F>
where
    F: FnMut(&ScalarExprRef) -> PreOrderVisitationResult,
{
    fn visit_pre(&mut self, expr: &ScalarExprRef) -> PreOrderVisitationResult {
        (self.visitor)(expr)
    }
    fn visit_post(&mut self, _: &ScalarExprRef) -> PostOrderVisitationResult {
        PostOrderVisitationResult::Continue
    }
}

/// Visits the sub-expressions in the given expression tree in pre-order.
pub fn visit_scalar_expr_pre<F>(expr: &ScalarExprRef, visitor: &mut F)
where
    F: FnMut(&ScalarExprRef) -> PreOrderVisitationResult,
{
    let mut pre_post_visitor = ScalarExprPreVisitor { visitor };
    visit_scalar_expr(expr, &mut pre_post_visitor);
}

struct ScalarExprPostVisitor<'a, F>
where
    F: FnMut(&ScalarExprRef) -> PostOrderVisitationResult,
{
    visitor: &'a mut F,
}

impl<F> ScalarExprPrePostVisitor for ScalarExprPostVisitor<'_, F>
where
    F: FnMut(&ScalarExprRef) -> PostOrderVisitationResult,
{
    fn visit_pre(&mut self, _: &ScalarExprRef) -> PreOrderVisitationResult {
        PreOrderVisitationResult::VisitInputs
    }
    fn visit_post(&mut self, expr: &ScalarExprRef) -> PostOrderVisitationResult {
        (self.visitor)(expr)
    }
}

/// Visits the sub-expressions in the given expression tree in post-order.
pub fn visit_scalar_expr_post<F>(expr: &ScalarExprRef, visitor: &mut F)
where
    F: FnMut(&ScalarExprRef) -> PostOrderVisitationResult,
{
    let mut pre_post_visitor = ScalarExprPostVisitor { visitor };
    visit_scalar_expr(expr, &mut pre_post_visitor);
}

/// Returns a set with the input columns referenced by the given expression.
pub fn collect_input_dependencies(expr: &ScalarExprRef) -> HashSet<usize> {
    let mut dependencies = HashSet::new();
    store_input_dependencies(expr, &mut dependencies);
    dependencies
}

pub fn store_input_dependencies(expr: &ScalarExprRef, dependencies: &mut HashSet<usize>) {
    visit_scalar_expr_pre(expr, &mut |curr_expr: &ScalarExprRef| {
        if let ScalarExpr::InputRef { index } = **curr_expr {
            dependencies.insert(index);
        }
        PreOrderVisitationResult::VisitInputs
    });
}
