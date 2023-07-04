use core::fmt;

use crate::data_type::DataType;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Debug)]
pub enum Value {
    Bool(bool),
    Int(i32),
    BigInt(i64),
    String(String),
    List(Vec<Box<Value>>),
    Any(Box<Literal>),
    Null,
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Literal {
    pub value: Value,
    pub data_type: DataType,
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Literal::fmt(f, &self.value, &self.data_type)
    }
}

impl Literal {
    fn fmt(f: &mut fmt::Formatter, value: &Value, data_type: &DataType) -> fmt::Result {
        match (value, data_type) {
            (Value::Bool(value), DataType::Bool) => {
                if *value {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            (Value::Int(value), DataType::Int) => write!(f, "{}", value),
            (Value::BigInt(value), DataType::BigInt) => write!(f, "{}", value),
            // TODO(asenac) escape strings
            (Value::String(value), DataType::String) => write!(f, "'{}'", value),
            (Value::List(vec), DataType::Array(elem_type)) => {
                write!(f, "[")?;
                for (i, e) in vec.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    Self::fmt(f, &e, &elem_type)?;
                }
                write!(f, "]")
            }
            (Value::List(vec), DataType::Tuple(data_types)) => {
                write!(f, "(")?;
                for (i, (e, data_type)) in vec.iter().zip(data_types.iter()).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    Self::fmt(f, e, data_type)?;
                }
                write!(f, ")")
            }
            (Value::Null, _) => write!(f, "NULL"),
            (Value::Any(literal), DataType::Any) => write!(f, "{}", literal),
            (_, _) => panic!("unsupported value - data type pair"),
        }
    }

    pub fn is_null(&self) -> bool {
        if let Value::Null = self.value {
            true
        } else {
            false
        }
    }
}
