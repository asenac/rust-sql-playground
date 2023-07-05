use crate::scalar_expr::*;
use std::collections::BTreeSet;

/// An equivalence class is a group of expressions within a given context that
/// are known to always lead to the same values.
///
/// If `ref_0 = ref_1` is known to be true, then `ref_0` and `ref_1` belong to
/// the same equivalence class.
pub struct EquivalenceClass {
    /// Indicates that any of the equality predicates that lead to this class
    /// was using the null-rejecting equality operator, ie. the SQL equality
    /// operator (`BinaryOp::Eq`), and hence, none of the expression within the
    /// class will evaluate to NULL.
    pub null_rejecting: bool,
    /// The list of expressions belonging to the class.
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

    /// Merges two equivalence classes.
    fn merge(&mut self, mut other: Self) {
        self.null_rejecting = self.null_rejecting || other.null_rejecting;
        self.members.append(&mut other.members);
    }
}

/// Returns the index of the class within the given list of classes, if any,
/// the given expression belongs to.
pub fn find_class(classes: &EquivalenceClasses, expr: &ScalarExprRef) -> Option<usize> {
    classes.iter().enumerate().find_map(|(class_id, class)| {
        if class.members.contains(expr) {
            Some(class_id)
        } else {
            None
        }
    })
}

/// Extract the equivalence classes using the equality predicates among the given
/// list of predicates.
///
/// The same expression cannot belong to the two different classes. If `ref_0 = ref_1`
/// and `ref_1 = ref_2` are present in the given list of predicates, then `ref_0`, `ref_1`
/// and `ref_2` are part of the same equivalence class.
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
