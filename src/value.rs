use core::fmt;

use itertools::Itertools;

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
    pub fn new(value: Value, data_type: DataType) -> Self {
        Self { value, data_type }
    }

    pub fn build_default(data_type: DataType) -> Self {
        Self {
            value: default_value_for_data_type(&data_type),
            data_type,
        }
    }

    pub fn build_null(data_type: DataType) -> Self {
        Self {
            value: Value::Null,
            data_type,
        }
    }

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

pub fn default_value_for_data_type(data_type: &DataType) -> Value {
    match data_type {
        DataType::Bool => Value::Bool(false),
        DataType::Int => Value::Int(0),
        DataType::BigInt => Value::BigInt(0),
        DataType::String => Value::String("".to_string()),
        DataType::Array(_) => Value::List(Vec::new()),
        DataType::Tuple(members) => Value::List(
            members
                .iter()
                .map(|nested_type| Box::new(default_value_for_data_type(nested_type)))
                .collect_vec(),
        ),
        DataType::Any => Value::Null,
        DataType::Unknown => panic!("cannot create value of unknown type"),
    }
}
