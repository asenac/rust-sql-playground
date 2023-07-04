use core::fmt;
use std::rc::Rc;

use crate::{
    data_type::DataType,
    value::{Literal, Value},
};

pub mod equivalence_class;
pub mod rewrite;
pub mod visitor;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum BinaryOp {
    Gt,
    Ge,
    Eq,
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

    pub fn return_type(&self) -> DataType {
        match self {
            BinaryOp::RawEq
            | BinaryOp::Eq
            | BinaryOp::Ge
            | BinaryOp::Gt
            | BinaryOp::Le
            | BinaryOp::Lt => DataType::Bool,
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

    pub fn return_type(&self) -> DataType {
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
            left: self.to_ref(),
            right: rhs,
        }
    }

    pub fn nary(op: NaryOp, operands: Vec<ScalarExprRef>) -> ScalarExpr {
        ScalarExpr::NaryOp { op, operands }
    }

    pub fn to_ref(self) -> ScalarExprRef {
        Rc::new(self)
    }

    pub fn data_type(&self, row_type: &[DataType]) -> DataType {
        // TODO(asenac) compute types recursively
        match self {
            ScalarExpr::Literal(literal) => literal.data_type.clone(),
            ScalarExpr::InputRef { index } => row_type[*index].clone(),
            ScalarExpr::BinaryOp { op, .. } => op.return_type(),
            ScalarExpr::NaryOp { op, .. } => op.return_type(),
        }
    }

    pub fn num_inputs(&self) -> usize {
        match self {
            ScalarExpr::Literal { .. } => 0,
            ScalarExpr::InputRef { .. } => 0,
            ScalarExpr::BinaryOp { .. } => 2,
            ScalarExpr::NaryOp { operands, .. } => operands.len(),
        }
    }

    pub fn get_input(&self, input_idx: usize) -> ScalarExprRef {
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
            ScalarExpr::Literal { .. } | ScalarExpr::InputRef { .. } => panic!(),
        }
    }

    pub fn clone_with_new_inputs(&self, inputs: &[ScalarExprRef]) -> ScalarExpr {
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
