use crate::scalar_expr::*;
use std::collections::BTreeSet;

pub struct EquivalenceClass {
    pub null_rejecting: bool,
    pub members: BTreeSet<ScalarExprRef>,
}

pub type EquivalenceClasses = Vec<EquivalenceClass>;

impl EquivalenceClass {
    fn new(null_rejecting: bool, members: BTreeSet<ScalarExprRef>) -> Self {
        Self {
            null_rejecting,
            members,
        }
    }

    fn merge(&mut self, mut other: Self) {
        self.null_rejecting = self.null_rejecting || other.null_rejecting;
        self.members.append(&mut other.members);
    }
}

pub fn find_class(classes: &EquivalenceClasses, expr: &ScalarExprRef) -> Option<usize> {
    classes.iter().enumerate().find_map(|(class_id, class)| {
        if class.members.contains(expr) {
            Some(class_id)
        } else {
            None
        }
    })
}

pub fn extract_equivalence_classes(predicates: &Vec<ScalarExprRef>) -> EquivalenceClasses {
    let mut classes: EquivalenceClasses = Vec::new();
    for predicate in predicates.iter() {
        if let ScalarExpr::BinaryOp { op, left, right } = predicate.as_ref() {
            let null_rejecting = match op {
                BinaryOp::RawEq => false,
                BinaryOp::Eq => true,
                _ => continue,
            };
            let left_class = find_class(&classes, left);
            let right_class = find_class(&classes, right);
            match (left_class, right_class) {
                (None, None) => {
                    classes.push(EquivalenceClass::new(
                        null_rejecting,
                        BTreeSet::from([left.clone(), right.clone()]),
                    ));
                }
                (None, Some(class_id)) | (Some(class_id), None) => {
                    let new_class =
                        EquivalenceClass::new(true, BTreeSet::from([left.clone(), right.clone()]));
                    classes[class_id].merge(new_class);
                }
                (Some(class_left), Some(class_right)) => {
                    if class_left != class_right {
                        let min_class = std::cmp::min(class_left, class_right);
                        let max_class = std::cmp::max(class_left, class_right);
                        let removed_class = classes.remove(max_class);
                        classes[min_class].merge(removed_class);
                    }
                }
            }
        }
    }
    classes
}
