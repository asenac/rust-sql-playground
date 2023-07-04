use core::{fmt, num};

#[derive(Clone)]
enum BinaryOp {
    Gt,
    Ge,
    Eq,
    Lt,
    Le,
}

#[derive(Clone)]
enum NaryOp {
    And,
    Or,
    Concat,
}

#[derive(Clone)]
enum ScalarExpr {
    Literal {
        value: String,
    },
    InputRef {
        index: usize,
    },
    BinaryOp {
        op: BinaryOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    NaryOp {
        op: NaryOp,
        operands: Vec<ScalarExpr>,
    },
}

impl BinaryOp {
    fn function_name(&self) -> &str {
        match self {
            BinaryOp::Eq => "eq",
            BinaryOp::Ge => "ge",
            BinaryOp::Gt => "gt",
            BinaryOp::Le => "le",
            BinaryOp::Lt => "lt",
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.function_name())
    }
}

impl NaryOp {
    fn function_name(&self) -> &str {
        match self {
            NaryOp::And => "and",
            NaryOp::Or => "or",
            NaryOp::Concat => "concat",
        }
    }
}

impl fmt::Display for NaryOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.function_name())
    }
}

impl ScalarExpr {
    fn literal(value: String) -> ScalarExpr {
        ScalarExpr::Literal { value }
    }

    fn input_ref(index: usize) -> ScalarExpr {
        ScalarExpr::InputRef { index }
    }

    fn binary(self, op: BinaryOp, rhs: Self) -> ScalarExpr {
        ScalarExpr::BinaryOp {
            op,
            left: Box::new(self),
            right: Box::new(rhs),
        }
    }

    fn nary(op: NaryOp, operands: Vec<ScalarExpr>) -> ScalarExpr {
        ScalarExpr::NaryOp { op, operands }
    }
}

impl fmt::Display for ScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarExpr::Literal { value } => write!(f, "'{}'", value),
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

#[derive(Clone)]
enum QueryNode {
    Project {
        outputs: Vec<ScalarExpr>,
        input: Box<QueryNode>,
    },
    Filter {
        conditions: Vec<ScalarExpr>,
        input: Box<QueryNode>,
    },
    TableScan {
        table_id: usize,
        num_columns: usize,
    },
    Join {
        conditions: Vec<ScalarExpr>,
        left: Box<QueryNode>,
        right: Box<QueryNode>,
    },
}

impl QueryNode {
    fn num_columns(&self) -> usize {
        match self {
            Self::Project { outputs, .. } => outputs.len(),
            Self::Filter { input, .. } => input.num_columns(),
            Self::TableScan { num_columns, .. } => *num_columns,
            Self::Join { left, right, .. } => left.num_columns() + right.num_columns(),
        }
    }

    fn table_scan(table_id: usize, num_columns: usize) -> QueryNode {
        QueryNode::TableScan {
            table_id,
            num_columns,
        }
    }

    fn filter(self, conditions: Vec<ScalarExpr>) -> QueryNode {
        QueryNode::Filter {
            conditions,
            input: Box::new(self),
        }
    }

    fn project(self, outputs: Vec<ScalarExpr>) -> QueryNode {
        QueryNode::Project {
            outputs,
            input: Box::new(self),
        }
    }

    fn join(self, other: Self, conditions: Vec<ScalarExpr>) -> QueryNode {
        QueryNode::Join {
            left: Box::new(self),
            right: Box::new(other),
            conditions,
        }
    }

    fn take_dangerous(&mut self) -> QueryNode {
        let empty = Self::table_scan(0, 0);
        std::mem::replace(self, empty)
    }
}

impl QueryNode {
    fn explain(&self) -> String {
        self.explain_recursive(0)
    }
    fn explain_recursive(&self, indentation: usize) -> String {
        let spaces = (0..2 * indentation).map(|_| ' ').collect::<String>();
        match self {
            Self::Project { outputs, input } => format!(
                "{}Project [{}]\n{}",
                spaces,
                outputs
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect::<Vec<_>>()
                    .join(", "),
                input.explain_recursive(indentation + 1)
            ),
            Self::Filter { conditions, input } => format!(
                "{}Filter [{}]\n{}",
                spaces,
                conditions
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect::<Vec<_>>()
                    .join(", "),
                input.explain_recursive(indentation + 1)
            ),
            Self::TableScan {
                table_id,
                num_columns,
            } => format!(
                "{}TableScan id: {}, num_columns: {}",
                spaces, table_id, num_columns
            ),
            Self::Join {
                left,
                right,
                conditions,
            } => format!(
                "{}Join [{}]\n{}\n{}",
                spaces,
                conditions
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect::<Vec<_>>()
                    .join(", "),
                left.explain_recursive(indentation + 1),
                right.explain_recursive(indentation + 1)
            ),
        }
    }
}

trait OptRule {
    fn apply(&self, plan: &mut QueryNode);
}

struct FilterMergeRule {}

impl OptRule for FilterMergeRule {
    fn apply(&self, plan: &mut QueryNode) {
        match plan {
            QueryNode::Project { input, .. } => self.apply(input),
            QueryNode::Filter { input, conditions } => {
                self.apply(input);

                if let QueryNode::Filter {
                    input: child_input,
                    conditions: child_conditions,
                } = &mut **input
                {
                    *plan = QueryNode::Filter {
                        input: Box::new(child_input.take_dangerous()),
                        conditions: conditions
                            .clone()
                            .into_iter()
                            .chain(child_conditions.clone().into_iter())
                            .collect(),
                    };
                }
            }
            QueryNode::TableScan { .. } => {}
            QueryNode::Join { left, right, .. } => {
                self.apply(left);
                self.apply(right);
            }
        }
    }
}

struct ProjectMergeRule {}

impl ScalarExpr {
    fn dereference(self, map: &Vec<ScalarExpr>) -> ScalarExpr {
        match self {
            Self::InputRef { index } => map[index].clone(),
            Self::BinaryOp { op, left, right } => Self::BinaryOp {
                op,
                left: Box::new(left.dereference(map)),
                right: Box::new(right.dereference(map)),
            },
            Self::NaryOp { op, operands } => Self::NaryOp {
                op,
                operands: operands.into_iter().map(|x| x.dereference(map)).collect(),
            },
            Self::Literal { .. } => self,
        }
    }
}

impl OptRule for ProjectMergeRule {
    fn apply(&self, plan: &mut QueryNode) {
        match plan {
            QueryNode::Project { input, outputs } => {
                self.apply(input);

                if let QueryNode::Project {
                    input: child_input,
                    outputs: child_outputs,
                } = &mut **input
                {
                    *plan = QueryNode::Project {
                        input: Box::new(child_input.take_dangerous()),
                        outputs: outputs
                            .clone()
                            .into_iter()
                            .map(|x| x.dereference(&child_outputs))
                            .collect(),
                    };
                }
            }
            QueryNode::Filter { input, .. } => self.apply(input),
            QueryNode::TableScan { .. } => {}
            QueryNode::Join { left, right, .. } => {
                self.apply(left);
                self.apply(right);
            }
        }
    }
}

// Interview questions:
// - Thoughts about the representation
//   - Too recursive
// - Look together at FilterMergeRule
// - Implement ProjectMergeRule
// - Implement predicate push down
// - Projection push down
// - Rule application framework
//   - Fix point?
// - CTEs -> DAGs
// - Add unique keys to TableScan and Reduce operator

fn main() {
    // select col0, col2 from (select col0, col9, col2 || col4 from (select * from table_1 where col0 = 'hello') where col5 = 'world')
    let table_scan_1 = QueryNode::table_scan(1, 10);
    let filter_1 =
        table_scan_1
            .filter(vec![ScalarExpr::input_ref(0)
                .binary(BinaryOp::Eq, ScalarExpr::literal("hello".to_string()))]);
    let filter_2 =
        filter_1
            .filter(vec![ScalarExpr::input_ref(5)
                .binary(BinaryOp::Eq, ScalarExpr::literal("world".to_string()))]);
    let project_1 = filter_2.project(vec![
        ScalarExpr::input_ref(0),
        ScalarExpr::input_ref(9),
        ScalarExpr::nary(
            NaryOp::Concat,
            vec![ScalarExpr::input_ref(2), ScalarExpr::input_ref(4)],
        ),
    ]);
    let project_2 = project_1.project(vec![ScalarExpr::input_ref(0), ScalarExpr::input_ref(2)]);

    let mut plan_1 = project_2.clone();
    println!("Before:\n\n{}", plan_1.explain());

    let filter_merge_rule = FilterMergeRule {};
    filter_merge_rule.apply(&mut plan_1);
    println!("\nAfter FilterMergeRule:\n\n{}", plan_1.explain());

    let project_merge_rule = ProjectMergeRule {};
    project_merge_rule.apply(&mut plan_1);
    println!("\nAfter ProjectMergeRule:\n\n{}", plan_1.explain());
}
