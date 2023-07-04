pub enum PreOrderVisitationResult {
    VisitInputs,
    DoNotVisitInputs,
    Abort,
}

pub enum PostOrderVisitationResult {
    Continue,
    Abort,
}

pub struct VisitationStep<V> {
    pub node: V,
    pub next_child: Option<usize>,
}

impl<V> VisitationStep<V> {
    pub fn new(node: V) -> Self {
        Self {
            node,
            next_child: None,
        }
    }
}
