//! This module contains the representation used for scalar expressions by the
//! query compiler and its query plan representation.
use core::fmt;
use std::rc::Rc;

use itertools::Itertools;

use crate::{
    data_type::DataType,
    value::{Literal, Value},
    visitor_utils::PostOrderVisitationResult,
};

use self::visitor::visit_expr_post;

pub mod equivalence_class;
pub mod reduction;
pub mod rewrite;
pub mod visitor;
pub use visitor::VisitableExpr;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum BinaryOp {
    Gt,
    Ge,
    /// SQL equality operator, that evaluates to NULL when any of its inputs is NULL.
    Eq,
    /// Non-null-rejecting equality, equivalent to SQL's IS NOT DISTINCT FROM
    RawEq,
    Lt,
    Le,
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum NaryOp {
    And,
    Or,
    Concat,
}

/// A _copy-on-write_ representation for the scalar expressions in the query plan.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum ScalarExpr {
    Literal(Literal),
    InputRef {
        index: usize,
    },
    BinaryOp {
        op: BinaryOp,
        left: Rc<ScalarExpr>,
        right: Rc<ScalarExpr>,
    },
    NaryOp {
        op: NaryOp,
        operands: Vec<Rc<ScalarExpr>>,
    },
}

pub type ScalarExprRef = Rc<ScalarExpr>;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum AggregateOp {
    Count,
    Min,
    Max,
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct AggregateExpr {
    pub op: AggregateOp,
    pub operands: Vec<usize>,
}

pub type AggregateExprRef = Rc<AggregateExpr>;

impl BinaryOp {
    pub fn function_name(&self) -> &str {
        match self {
            BinaryOp::RawEq => "raw_eq",
            BinaryOp::Eq => "eq",
            BinaryOp::Ge => "ge",
            BinaryOp::Gt => "gt",
            BinaryOp::Le => "le",
            BinaryOp::Lt => "lt",
        }
    }

    pub fn return_type(&self, _operand_types: &[DataType]) -> DataType {
        match self {
            BinaryOp::RawEq
            | BinaryOp::Eq
            | BinaryOp::Ge
            | BinaryOp::Gt
            | BinaryOp::Le
            | BinaryOp::Lt => DataType::Bool,
        }
    }

    /// Whether the result of the operation is null if any of their operands is null.
    pub fn propagates_null(&self) -> bool {
        match self {
            BinaryOp::RawEq => false,
            BinaryOp::Eq | BinaryOp::Ge | BinaryOp::Gt | BinaryOp::Le | BinaryOp::Lt => true,
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.function_name())
    }
}

impl NaryOp {
    pub fn function_name(&self) -> &str {
        match self {
            NaryOp::And => "and",
            NaryOp::Or => "or",
            NaryOp::Concat => "concat",
        }
    }

    pub fn return_type(&self, _operand_types: &[DataType]) -> DataType {
        match self {
            NaryOp::And | NaryOp::Or => DataType::Bool,
            NaryOp::Concat => DataType::String,
        }
    }
}

impl fmt::Display for NaryOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.function_name())
    }
}

/// Handy expression constructors.
impl ScalarExpr {
    pub fn string_literal(value: String) -> ScalarExpr {
        ScalarExpr::Literal(Literal {
            value: Value::String(value),
            data_type: DataType::String,
        })
    }

    pub fn true_literal() -> ScalarExpr {
        ScalarExpr::Literal(Literal {
            value: Value::Bool(true),
            data_type: DataType::Bool,
        })
    }

    pub fn false_literal() -> ScalarExpr {
        ScalarExpr::Literal(Literal {
            value: Value::Bool(false),
            data_type: DataType::Bool,
        })
    }

    pub fn null_literal(data_type: DataType) -> ScalarExpr {
        ScalarExpr::Literal(Literal {
            value: Value::Null,
            data_type,
        })
    }

    pub fn input_ref(index: usize) -> ScalarExpr {
        ScalarExpr::InputRef { index }
    }

    pub fn binary(self, op: BinaryOp, rhs: ScalarExprRef) -> ScalarExpr {
        ScalarExpr::BinaryOp {
            op,
            left: self.into(),
            right: rhs,
        }
    }

    pub fn nary(op: NaryOp, operands: Vec<ScalarExprRef>) -> ScalarExpr {
        ScalarExpr::NaryOp { op, operands }
    }
}

impl ScalarExpr {
    pub fn is_null(&self) -> bool {
        match self {
            Self::Literal(Literal {
                value: Value::Null,
                data_type: _,
            }) => true,
            _ => false,
        }
    }

    pub fn is_literal(&self) -> bool {
        match self {
            Self::Literal(_) => true,
            _ => false,
        }
    }

    pub fn data_type(&self, row_type: &[DataType]) -> DataType {
        let operand_types = (0..self.num_inputs())
            .map(|i| {
                let mut stack = Vec::new();
                visit_expr_post(&self.get_input(i), &mut |expr: &ScalarExprRef| {
                    let num_inputs = expr.num_inputs();
                    let typ = expr
                        .data_type_with_operand_types(row_type, &stack[stack.len() - num_inputs..]);
                    stack.truncate(stack.len() - num_inputs);
                    stack.push(typ);
                    PostOrderVisitationResult::Continue
                });
                stack.into_iter().next().unwrap()
            })
            .collect_vec();

        self.data_type_with_operand_types(row_type, &operand_types)
    }

    fn data_type_with_operand_types(
        &self,
        row_type: &[DataType],
        operand_types: &[DataType],
    ) -> DataType {
        match self {
            ScalarExpr::Literal(literal) => literal.data_type.clone(),
            ScalarExpr::InputRef { index } => row_type[*index].clone(),
            ScalarExpr::BinaryOp { op, .. } => op.return_type(operand_types),
            ScalarExpr::NaryOp { op, .. } => op.return_type(operand_types),
        }
    }
}

impl fmt::Display for ScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarExpr::Literal(literal) => write!(f, "{}", literal),
            ScalarExpr::InputRef { index } => write!(f, "ref_{}", index),
            ScalarExpr::BinaryOp { op, left, right } => write!(f, "{}({}, {})", op, left, right),
            ScalarExpr::NaryOp { op, operands } => {
                write!(f, "{}(", op)?;
                let mut sep = "";
                for operand in operands {
                    write!(f, "{}{}", sep, operand)?;
                    sep = ", ";
                }
                write!(f, ")")
            }
        }
    }
}

impl AggregateExpr {
    pub fn data_type(&self, row_type: &[DataType]) -> DataType {
        let operand_types = self
            .operands
            .iter()
            .map(|o| row_type[*o].clone())
            .collect_vec();
        self.op.return_type(&operand_types)
    }
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}(", self.op)?;
        let mut sep = "";
        for operand in self.operands.iter() {
            write!(f, "{}ref_{}", sep, operand)?;
            sep = ", ";
        }
        write!(f, ")")
    }
}

impl AggregateOp {
    pub fn return_type(&self, operand_types: &[DataType]) -> DataType {
        match self {
            AggregateOp::Count => DataType::BigInt,
            AggregateOp::Min | AggregateOp::Max => operand_types[0].clone(),
        }
    }

    pub fn function_name(&self) -> &str {
        match self {
            AggregateOp::Min => "min",
            AggregateOp::Max => "max",
            AggregateOp::Count => "count",
        }
    }
}

impl fmt::Display for AggregateOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.function_name())
    }
}

/// Representation for working with expressions that may contain aggregate expressions
/// and other expressions that are not allowed in the query graph.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum ExtendedScalarExpr {
    Literal(Literal),
    InputRef {
        index: usize,
    },
    BinaryOp {
        op: BinaryOp,
        left: Rc<ExtendedScalarExpr>,
        right: Rc<ExtendedScalarExpr>,
    },
    NaryOp {
        op: NaryOp,
        operands: Vec<Rc<ExtendedScalarExpr>>,
    },
    Aggregate {
        op: AggregateOp,
        operands: Vec<Rc<ExtendedScalarExpr>>,
    },
}

pub type ExtendedScalarExprRef = Rc<ExtendedScalarExpr>;

impl ExtendedScalarExpr {
    pub fn data_type(&self, row_type: &[DataType]) -> DataType {
        let operand_types = (0..self.num_inputs())
            .map(|i| {
                let mut stack = Vec::new();
                visit_expr_post(&self.get_input(i), &mut |expr: &ExtendedScalarExprRef| {
                    let num_inputs = expr.num_inputs();
                    let typ = expr
                        .data_type_with_operand_types(row_type, &stack[stack.len() - num_inputs..]);
                    stack.truncate(stack.len() - num_inputs);
                    stack.push(typ);
                    PostOrderVisitationResult::Continue
                });
                stack.into_iter().next().unwrap()
            })
            .collect_vec();

        self.data_type_with_operand_types(row_type, &operand_types)
    }

    fn data_type_with_operand_types(
        &self,
        row_type: &[DataType],
        operand_types: &[DataType],
    ) -> DataType {
        match self {
            ExtendedScalarExpr::Literal(literal) => literal.data_type.clone(),
            ExtendedScalarExpr::InputRef { index } => row_type[*index].clone(),
            ExtendedScalarExpr::BinaryOp { op, .. } => op.return_type(operand_types),
            ExtendedScalarExpr::NaryOp { op, .. } => op.return_type(operand_types),
            ExtendedScalarExpr::Aggregate { op, .. } => op.return_type(operand_types),
        }
    }
}

pub trait ToScalarExpr {
    fn to_scalar_expr(&self) -> Option<ScalarExprRef>;
}

impl ToScalarExpr for Rc<ExtendedScalarExpr> {
    fn to_scalar_expr(&self) -> Option<ScalarExprRef> {
        let mut stack: Vec<ScalarExprRef> = Vec::new();
        visit_expr_post(&self, &mut |expr: &ExtendedScalarExprRef| {
            let extended_expr = match expr.as_ref() {
                ExtendedScalarExpr::Literal(literal) => ScalarExpr::Literal(literal.clone()),
                ExtendedScalarExpr::InputRef { index } => ScalarExpr::InputRef { index: *index },
                ExtendedScalarExpr::BinaryOp {
                    op,
                    left: _,
                    right: _,
                } => {
                    let operands = &stack[stack.len() - 2..];
                    let expr = ScalarExpr::BinaryOp {
                        op: op.clone(),
                        left: operands[0].clone(),
                        right: operands[1].clone(),
                    };
                    stack.truncate(stack.len() - 2);
                    expr
                }
                ExtendedScalarExpr::NaryOp { op, operands } => {
                    let operands = &stack[stack.len() - operands.len()..];
                    let expr = ScalarExpr::NaryOp {
                        op: op.clone(),
                        operands: operands.iter().cloned().collect_vec(),
                    };
                    stack.truncate(stack.len() - operands.len());
                    expr
                }
                ExtendedScalarExpr::Aggregate { .. } => {
                    stack.clear();
                    return PostOrderVisitationResult::Abort;
                }
            };
            stack.push(extended_expr.into());
            PostOrderVisitationResult::Continue
        });
        stack.into_iter().next()
    }
}

pub trait ToExtendedExpr {
    fn to_extended_expr(&self) -> ExtendedScalarExprRef;
}

impl ToExtendedExpr for Rc<ScalarExpr> {
    fn to_extended_expr(&self) -> ExtendedScalarExprRef {
        let mut stack: Vec<ExtendedScalarExprRef> = Vec::new();
        visit_expr_post(self, &mut |expr: &ScalarExprRef| {
            let extended_expr = match expr.as_ref() {
                ScalarExpr::Literal(literal) => ExtendedScalarExpr::Literal(literal.clone()),
                ScalarExpr::InputRef { index } => ExtendedScalarExpr::InputRef { index: *index },
                ScalarExpr::BinaryOp {
                    op,
                    left: _,
                    right: _,
                } => {
                    let operands = &stack[stack.len() - 2..];
                    let expr = ExtendedScalarExpr::BinaryOp {
                        op: op.clone(),
                        left: operands[0].clone(),
                        right: operands[1].clone(),
                    };
                    stack.truncate(stack.len() - 2);
                    expr
                }
                ScalarExpr::NaryOp { op, operands } => {
                    let operands = &stack[stack.len() - operands.len()..];
                    let expr = ExtendedScalarExpr::NaryOp {
                        op: op.clone(),
                        operands: operands.iter().cloned().collect_vec(),
                    };
                    stack.truncate(stack.len() - operands.len());
                    expr
                }
            };
            stack.push(extended_expr.into());
            PostOrderVisitationResult::Continue
        });
        stack.into_iter().next().unwrap()
    }
}

impl ToExtendedExpr for Rc<AggregateExpr> {
    fn to_extended_expr(&self) -> ExtendedScalarExprRef {
        ExtendedScalarExpr::Aggregate {
            op: self.op.clone(),
            operands: self
                .operands
                .iter()
                .map(|i| ExtendedScalarExpr::InputRef { index: *i }.into())
                .collect_vec(),
        }
        .into()
    }
}
