use crate::visitor_utils::{PostOrderVisitationResult, PreOrderVisitationResult};

use super::{visitor::QueryGraphPrePostVisitorMut, NodeId, QueryGraph};

pub mod rules;
pub(crate) mod utils;

pub enum OptRuleType {
    RootOnly,
    Always,
    TopDown,
    BottomUp,
}

/// Trait for rules that may replace the current node being visited by the optimizer.
pub trait SingleReplacementRule: Sync {
    fn rule_type(&self) -> OptRuleType;

    /// Given a node, this function is expected to return an equivalent node leading to the
    /// same results as the given one, or None if the optimization rule didn't apply to the
    /// given node.
    ///
    /// Also, the sub-plan under the returned node must not contain the given one.
    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId) -> Option<NodeId>;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>().split("::").last().unwrap()
    }
}

/// Trait for rules that may replace arbitrary nodes within the graph.
pub trait Rule: Sync {
    fn rule_type(&self) -> OptRuleType;

    /// Given a node, this function is expected to return an list of node replacements.
    fn apply(&self, query_graph: &mut QueryGraph, node_id: NodeId)
        -> Option<Vec<(NodeId, NodeId)>>;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>().split("::").last().unwrap()
    }
}

impl<T: SingleReplacementRule> Rule for T {
    fn rule_type(&self) -> OptRuleType {
        SingleReplacementRule::rule_type(self)
    }

    fn apply(
        &self,
        query_graph: &mut QueryGraph,
        node_id: NodeId,
    ) -> Option<Vec<(NodeId, NodeId)>> {
        self.apply(query_graph, node_id)
            .map(|replacement_node| vec![(node_id, replacement_node)])
    }
}

/// An optimizer is basically a rewrite pass, where all the rules work towards a shared
/// goal.
pub struct Optimizer {
    rules: Vec<Box<dyn Rule>>,
    root_only_rules: Vec<usize>,
    top_down_rules: Vec<usize>,
    bottom_up_rules: Vec<usize>,
}

pub type Replacement = (NodeId, NodeId);

pub trait OptimizerListener {
    /// Invoked for every node replacement performed by the optimizer.
    fn node_replacements(
        &mut self,
        rule: &dyn Rule,
        query_graph: &QueryGraph,
        replacements: &Vec<Replacement>,
    );
}

/// Structure for passing parameters to the optimizer.
pub struct OptimizerContext<'a> {
    listeners: Vec<&'a mut dyn OptimizerListener>,
}

/// Helper visitor to apply the optimization rules in an optimizer instance during a mutating
/// pre-post order visitation.
struct OptimizationVisitor<'a, 'b, 'c> {
    optimizer: &'a Optimizer,
    context: &'b mut OptimizerContext<'c>,
}

impl<'a> OptimizerContext<'a> {
    pub fn new() -> Self {
        Self {
            listeners: Vec::new(),
        }
    }

    pub fn append_listener(&mut self, listener: &'a mut dyn OptimizerListener) {
        self.listeners.push(listener)
    }
}

impl Optimizer {
    /// Builds an optimizer instance given a list of rules.
    pub fn new(rules: Vec<Box<dyn Rule>>) -> Self {
        let mut root_only_rules = Vec::new();
        let mut top_down_rules = Vec::new();
        let mut bottom_up_rules = Vec::new();
        for (id, rule) in rules.iter().enumerate() {
            match rule.rule_type() {
                OptRuleType::Always => {
                    top_down_rules.push(id);
                    bottom_up_rules.push(id);
                }
                OptRuleType::TopDown => top_down_rules.push(id),
                OptRuleType::BottomUp => bottom_up_rules.push(id),
                OptRuleType::RootOnly => root_only_rules.push(id),
            }
        }
        Self {
            rules,
            root_only_rules,
            top_down_rules,
            bottom_up_rules,
        }
    }

    /// Optimize the given query graph by applying the rules in this optimizer instance.
    pub fn optimize(&self, context: &mut OptimizerContext, query_graph: &mut QueryGraph) {
        // TODO(asenac) add mechanism to detect infinite loops due to bugs
        loop {
            let last_gen_number = query_graph.gen_number;

            self.apply_root_only_rules(context, query_graph);

            let mut visitor = OptimizationVisitor {
                optimizer: self,
                context,
            };
            query_graph.visit_mut(&mut visitor);

            if last_gen_number == query_graph.gen_number {
                // Fix-point was reached. A full plan traversal without modifications.
                break;
            }
        }
    }

    fn apply_root_only_rules(&self, context: &mut OptimizerContext, query_graph: &mut QueryGraph) {
        for rule in self
            .root_only_rules
            .iter()
            .map(|id| self.rules.get(*id).unwrap())
        {
            if let Some(replacements) = rule.apply(query_graph, query_graph.entry_node) {
                Self::notify_replacements(context, &**rule, query_graph, &replacements);
                for (original_node, replacement_node) in replacements {
                    query_graph.replace_node(original_node, replacement_node);
                }
            }
        }
    }

    fn notify_replacements(
        context: &mut OptimizerContext,
        rule: &dyn Rule,
        query_graph: &QueryGraph,
        replacements: &Vec<(NodeId, NodeId)>,
    ) {
        for listener in context.listeners.iter_mut() {
            listener.node_replacements(rule, query_graph, replacements);
        }
    }

    /// Apply a set of rules to the given node. It returns early if any of the rules
    /// replaces any node that is not the current one, as that invalidates the current
    /// traversal stack.
    ///
    /// If the given node is replaced, `node_id` is updated to point to the replacement
    /// node.
    ///
    /// Returns whether the current traversal can continue or must be aborted.
    fn apply_rule_list(
        &self,
        context: &mut OptimizerContext,
        query_graph: &mut QueryGraph,
        rules: &Vec<usize>,
        node_id: &mut NodeId,
    ) -> bool {
        let mut can_continue = true;
        for rule in rules.iter().map(|id| self.rules.get(*id).unwrap()) {
            if let Some(replacements) = rule.apply(query_graph, *node_id) {
                Optimizer::notify_replacements(context, &**rule, query_graph, &replacements);
                for (original_node, replacement_node) in replacements {
                    // Replace the node in the graph and apply the remaining rules to the
                    // returned one.
                    query_graph.replace_node(original_node, replacement_node);

                    if original_node == *node_id {
                        // Make the visitation logic aware of the replacement, so the inputs of
                        // the new node are visited during the pre-order part of the visitation.
                        *node_id = replacement_node;
                    } else {
                        // We must restart the traversal before applying any other rule.
                        can_continue = false;
                    }
                }
                if !can_continue {
                    break;
                }
            }
        }
        can_continue
    }
}

impl QueryGraphPrePostVisitorMut for OptimizationVisitor<'_, '_, '_> {
    fn visit_pre(
        &mut self,
        query_graph: &mut QueryGraph,
        node_id: &mut NodeId,
    ) -> PreOrderVisitationResult {
        if self.optimizer.apply_rule_list(
            self.context,
            query_graph,
            &self.optimizer.top_down_rules,
            node_id,
        ) {
            PreOrderVisitationResult::VisitInputs
        } else {
            PreOrderVisitationResult::Abort
        }
    }

    fn visit_post(
        &mut self,
        query_graph: &mut QueryGraph,
        node_id: &mut NodeId,
    ) -> PostOrderVisitationResult {
        if self.optimizer.apply_rule_list(
            self.context,
            query_graph,
            &self.optimizer.bottom_up_rules,
            node_id,
        ) {
            PostOrderVisitationResult::Continue
        } else {
            PostOrderVisitationResult::Abort
        }
    }
}

pub fn build_rule(rule_name: &str) -> Result<Box<dyn Rule>, ()> {
    use self::rules::*;
    match rule_name {
        "AggregateProjectTransposeRule" => Ok(Box::new(AggregateProjectTransposeRule {})),
        "AggregatePruningRule" => Ok(Box::new(AggregatePruningRule {})),
        "AggregateRemoveRule" => Ok(Box::new(AggregateRemoveRule {})),
        "AggregateSimplifierRule" => Ok(Box::new(AggregateSimplifierRule {})),
        "CommonAggregateDiscoveryRule" => Ok(Box::new(CommonAggregateDiscoveryRule {})),
        "EqualityPropagationRule" => Ok(Box::new(EqualityPropagationRule {})),
        "FilterAggregateTransposeRule" => Ok(Box::new(FilterAggregateTransposeRule {})),
        "FilterJoinTransposeRule" => Ok(Box::new(FilterJoinTransposeRule {})),
        "FilterMergeRule" => Ok(Box::new(FilterMergeRule {})),
        "FilterNormalizationRule" => Ok(Box::new(FilterNormalizationRule {})),
        "FilterProjectTransposeRule" => Ok(Box::new(FilterProjectTransposeRule {})),
        "JoinProjectTransposeRule" => Ok(Box::new(JoinProjectTransposeRule {})),
        "JoinPruningRule" => Ok(Box::new(JoinPruningRule {})),
        "ProjectMergeRule" => Ok(Box::new(ProjectMergeRule {})),
        "ProjectNormalizationRule" => Ok(Box::new(ProjectNormalizationRule {})),
        "PruneAggregateInputRule" => Ok(Box::new(PruneAggregateInputRule {})),
        "RemovePassthroughProjectRule" => Ok(Box::new(RemovePassthroughProjectRule {})),
        "UnionMergeRule" => Ok(Box::new(UnionMergeRule {})),
        "UnionPruningRule" => Ok(Box::new(UnionPruningRule {})),
        _ => Err(()),
    }
}

lazy_static! {
    pub static ref DEFAULT_OPTIMIZER: Optimizer = {
        use self::rules::*;
        let optimizer = Optimizer::new(vec![
            Box::new(AggregateProjectTransposeRule {}),
            Box::new(AggregatePruningRule {}),
            Box::new(AggregateRemoveRule {}),
            Box::new(AggregateSimplifierRule {}),
            Box::new(CommonAggregateDiscoveryRule {}),
            Box::new(EqualityPropagationRule {}),
            Box::new(FilterAggregateTransposeRule {}),
            Box::new(FilterJoinTransposeRule {}),
            Box::new(FilterMergeRule {}),
            Box::new(FilterNormalizationRule {}),
            Box::new(FilterProjectTransposeRule {}),
            Box::new(JoinProjectTransposeRule {}),
            Box::new(JoinPruningRule {}),
            Box::new(ProjectMergeRule {}),
            Box::new(ProjectNormalizationRule {}),
            Box::new(PruneAggregateInputRule {}),
            Box::new(RemovePassthroughProjectRule {}),
            Box::new(UnionMergeRule {}),
            Box::new(UnionPruningRule {}),
        ]);
        optimizer
    };
}
