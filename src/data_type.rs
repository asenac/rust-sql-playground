use core::fmt;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Debug)]
pub enum DataType {
    Bool,
    Int,
    BigInt,
    String,
    Unknown,
    Any,
    Array(Box<DataType>),
    Tuple(Vec<DataType>),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "bool"),
            DataType::Int => write!(f, "int"),
            DataType::BigInt => write!(f, "bigint"),
            DataType::String => write!(f, "string"),
            DataType::Unknown => write!(f, "unknown"),
            DataType::Any => write!(f, "any"),
            DataType::Array(elem_type) => write!(f, "array({})", elem_type),
            DataType::Tuple(elem_types) => {
                write!(f, "tuple(")?;
                for (i, data_type) in elem_types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", data_type)?;
                }
                write!(f, ")")
            }
        }
    }
}
