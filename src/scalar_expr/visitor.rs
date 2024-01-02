use std::collections::HashSet;
use std::marker::PhantomData;

use crate::scalar_expr::*;
use crate::visitor_utils::*;

/// Trait that needs to be implemented for any expression type in order for
/// it to be visited with the utilities in this module.
pub trait VisitableExpr {
    fn num_inputs(&self) -> usize;

    fn get_input(&self, input_idx: usize) -> Rc<Self>;
}

pub trait ExprPrePostVisitor<E: VisitableExpr> {
    /// The expression is a mutable reference so we can override the current
    /// node in order to allow visiting the inputs of a different node. Useful
    /// when performing expression rewrites.
    fn visit_pre(&mut self, expr: &mut Rc<E>) -> PreOrderVisitationResult;

    fn visit_post(&mut self, expr: &Rc<E>) -> PostOrderVisitationResult;
}

pub fn visit_expr<E, V>(expr: &Rc<E>, visitor: &mut V)
where
    E: VisitableExpr,
    V: ExprPrePostVisitor<E>,
{
    let mut stack = vec![VisitationStep::new(expr.clone())];
    while let Some(step) = stack.last_mut() {
        if step.next_child.is_none() {
            match visitor.visit_pre(&mut step.node) {
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

struct ExprPreVisitor<'a, F, E>
where
    E: VisitableExpr,
    F: FnMut(&Rc<E>) -> PreOrderVisitationResult,
{
    visitor: &'a mut F,
    phantom: PhantomData<E>,
}

impl<F, E> ExprPrePostVisitor<E> for ExprPreVisitor<'_, F, E>
where
    E: VisitableExpr,
    F: FnMut(&Rc<E>) -> PreOrderVisitationResult,
{
    fn visit_pre(&mut self, expr: &mut Rc<E>) -> PreOrderVisitationResult {
        (self.visitor)(expr)
    }
    fn visit_post(&mut self, _: &Rc<E>) -> PostOrderVisitationResult {
        PostOrderVisitationResult::Continue
    }
}

/// Visits the sub-expressions in the given expression tree in pre-order.
pub fn visit_expr_pre<F, E>(expr: &Rc<E>, visitor: &mut F)
where
    E: VisitableExpr,
    F: FnMut(&Rc<E>) -> PreOrderVisitationResult,
{
    let mut pre_post_visitor = ExprPreVisitor {
        visitor,
        phantom: PhantomData,
    };
    visit_expr(expr, &mut pre_post_visitor);
}

struct ExprPostVisitor<'a, F, E>
where
    E: VisitableExpr,
    F: FnMut(&Rc<E>) -> PostOrderVisitationResult,
{
    visitor: &'a mut F,
    phantom: PhantomData<E>,
}

impl<F, E> ExprPrePostVisitor<E> for ExprPostVisitor<'_, F, E>
where
    E: VisitableExpr,
    F: FnMut(&Rc<E>) -> PostOrderVisitationResult,
{
    fn visit_pre(&mut self, _: &mut Rc<E>) -> PreOrderVisitationResult {
        PreOrderVisitationResult::VisitInputs
    }
    fn visit_post(&mut self, expr: &Rc<E>) -> PostOrderVisitationResult {
        (self.visitor)(expr)
    }
}

/// Visits the sub-expressions in the given expression tree in post-order.
pub fn visit_expr_post<F, E>(expr: &Rc<E>, visitor: &mut F)
where
    E: VisitableExpr,
    F: FnMut(&Rc<E>) -> PostOrderVisitationResult,
{
    let mut pre_post_visitor = ExprPostVisitor {
        visitor,
        phantom: PhantomData,
    };
    visit_expr(expr, &mut pre_post_visitor);
}

/// Returns a set with the input columns referenced by the given expression.
pub fn collect_input_dependencies(expr: &ScalarExprRef) -> HashSet<usize> {
    let mut dependencies = HashSet::new();
    store_input_dependencies(expr, &mut dependencies);
    dependencies
}

pub fn store_input_dependencies(expr: &ScalarExprRef, dependencies: &mut HashSet<usize>) {
    visit_expr_pre(expr, &mut |curr_expr: &ScalarExprRef| {
        if let ScalarExpr::InputRef { index } = **curr_expr {
            dependencies.insert(index);
        }
        PreOrderVisitationResult::VisitInputs
    });
}

impl VisitableExpr for ScalarExpr {
    fn num_inputs(&self) -> usize {
        match self {
            ScalarExpr::Literal { .. } => 0,
            ScalarExpr::InputRef { .. } => 0,
            ScalarExpr::BinaryOp { .. } => 2,
            ScalarExpr::NaryOp { operands, .. } => operands.len(),
            ScalarExpr::ExistsSubquery { .. } | ScalarExpr::ScalarSubquery { .. } => 0,
            ScalarExpr::ScalarSubqueryCmp { .. } => 1,
        }
    }

    fn get_input(&self, input_idx: usize) -> ScalarExprRef {
        assert!(input_idx < self.num_inputs());
        match self {
            ScalarExpr::BinaryOp { left, right, .. } => {
                if input_idx == 0 {
                    left.clone()
                } else {
                    right.clone()
                }
            }
            ScalarExpr::NaryOp { operands, .. } => operands[input_idx].clone(),
            ScalarExpr::Literal { .. }
            | ScalarExpr::InputRef { .. }
            | ScalarExpr::ExistsSubquery { .. }
            | ScalarExpr::ScalarSubquery { .. } => panic!(),
            ScalarExpr::ScalarSubqueryCmp { scalar_operand, .. } => scalar_operand.clone(),
        }
    }
}

impl VisitableExpr for ExtendedScalarExpr {
    fn num_inputs(&self) -> usize {
        match self {
            ExtendedScalarExpr::Literal { .. } => 0,
            ExtendedScalarExpr::InputRef { .. } => 0,
            ExtendedScalarExpr::BinaryOp { .. } => 2,
            ExtendedScalarExpr::Aggregate { operands, .. }
            | ExtendedScalarExpr::NaryOp { operands, .. } => operands.len(),
            ExtendedScalarExpr::ExistsSubquery { .. } => 0,
            ExtendedScalarExpr::ScalarSubquery { .. } => 0,
            ExtendedScalarExpr::ScalarSubqueryCmp { .. } => 1,
        }
    }

    fn get_input(&self, input_idx: usize) -> ExtendedScalarExprRef {
        assert!(input_idx < self.num_inputs());
        match self {
            ExtendedScalarExpr::BinaryOp { left, right, .. } => {
                if input_idx == 0 {
                    left.clone()
                } else {
                    right.clone()
                }
            }
            ExtendedScalarExpr::Aggregate { operands, .. }
            | ExtendedScalarExpr::NaryOp { operands, .. } => operands[input_idx].clone(),
            ExtendedScalarExpr::Literal { .. }
            | ExtendedScalarExpr::InputRef { .. }
            | ExtendedScalarExpr::ExistsSubquery { .. }
            | ExtendedScalarExpr::ScalarSubquery { .. } => panic!(),
            ExtendedScalarExpr::ScalarSubqueryCmp { scalar_operand, .. } => scalar_operand.clone(),
        }
    }
}
