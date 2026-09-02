// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic dependency graph algorithms shared by configuration and runtime wiring.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// A directed graph whose edges point from a consumer to its dependencies.
#[derive(Debug, Clone, Default)]
pub struct DependencyGraph<K> {
    dependencies: BTreeMap<K, BTreeSet<K>>,
}

impl<K> DependencyGraph<K>
where
    K: Clone + Ord,
{
    /// Creates an empty graph.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            dependencies: BTreeMap::new(),
        }
    }

    /// Adds a node without any dependencies.
    pub fn add_node(&mut self, node: K) {
        let _ = self.dependencies.entry(node).or_default();
    }

    /// Adds a dependency edge from `consumer` to `dependency`.
    pub fn add_dependency(&mut self, consumer: K, dependency: K) {
        let _ = self.dependencies.entry(dependency.clone()).or_default();
        let _ = self
            .dependencies
            .entry(consumer)
            .or_default()
            .insert(dependency);
    }

    /// Adds dependency edges from `consumer` to each dependency.
    pub fn add_dependencies<I>(&mut self, consumer: K, dependencies: I)
    where
        I: IntoIterator<Item = K>,
    {
        self.add_node(consumer.clone());
        for dependency in dependencies {
            self.add_dependency(consumer.clone(), dependency);
        }
    }

    /// Returns the exact members of all cyclic strongly connected components.
    ///
    /// Nodes merely downstream of a cycle are excluded.
    #[must_use]
    pub fn cycle_members(&self) -> Vec<K> {
        let dependents = self.dependents();
        let mut visited = BTreeSet::new();
        let mut finishing_order = Vec::with_capacity(self.dependencies.len());

        for start in self.dependencies.keys() {
            if visited.contains(start) {
                continue;
            }
            let mut stack = vec![(start.clone(), false)];
            while let Some((node, expanded)) = stack.pop() {
                if expanded {
                    finishing_order.push(node);
                    continue;
                }
                if !visited.insert(node.clone()) {
                    continue;
                }
                stack.push((node.clone(), true));
                if let Some(dependencies) = self.dependencies.get(&node) {
                    for dependency in dependencies.iter().rev() {
                        if !visited.contains(dependency) {
                            stack.push((dependency.clone(), false));
                        }
                    }
                }
            }
        }

        let mut assigned = BTreeSet::new();
        let mut cycle_members = BTreeSet::new();
        for start in finishing_order.into_iter().rev() {
            if !assigned.insert(start.clone()) {
                continue;
            }
            let mut component = Vec::new();
            let mut stack = vec![start];
            while let Some(node) = stack.pop() {
                component.push(node.clone());
                if let Some(node_dependents) = dependents.get(&node) {
                    for dependent in node_dependents.iter().rev() {
                        if assigned.insert(dependent.clone()) {
                            stack.push(dependent.clone());
                        }
                    }
                }
            }

            let is_self_cycle = component.len() == 1
                && self
                    .dependencies
                    .get(&component[0])
                    .is_some_and(|dependencies| dependencies.contains(&component[0]));
            if component.len() > 1 || is_self_cycle {
                cycle_members.extend(component);
            }
        }
        cycle_members.into_iter().collect()
    }

    /// Returns deterministic provider-first topological layers.
    ///
    /// # Errors
    ///
    /// Returns the exact cycle members when the graph is not acyclic.
    pub fn topological_layers(&self) -> Result<Vec<Vec<K>>, Vec<K>> {
        let cycle_members = self.cycle_members();
        if !cycle_members.is_empty() {
            return Err(cycle_members);
        }

        let dependents = self.dependents();
        let mut remaining_dependencies: BTreeMap<K, usize> = self
            .dependencies
            .iter()
            .map(|(node, dependencies)| (node.clone(), dependencies.len()))
            .collect();
        let mut ready: BTreeSet<K> = remaining_dependencies
            .iter()
            .filter_map(|(node, count)| (*count == 0).then_some(node.clone()))
            .collect();
        let mut layers = Vec::new();

        while !ready.is_empty() {
            let layer: Vec<K> = std::mem::take(&mut ready).into_iter().collect();
            for dependency in &layer {
                if let Some(node_dependents) = dependents.get(dependency) {
                    for dependent in node_dependents {
                        let remaining = remaining_dependencies
                            .get_mut(dependent)
                            .expect("every dependent is a graph node");
                        *remaining -= 1;
                        if *remaining == 0 {
                            let _ = ready.insert(dependent.clone());
                        }
                    }
                }
            }
            layers.push(layer);
        }
        Ok(layers)
    }

    /// Returns all graph nodes transitively required by `roots`, including the roots.
    #[must_use]
    pub fn reachable_dependencies<I>(&self, roots: I) -> BTreeSet<K>
    where
        I: IntoIterator<Item = K>,
    {
        let mut reachable = BTreeSet::new();
        let mut pending: VecDeque<K> = roots
            .into_iter()
            .filter(|root| self.dependencies.contains_key(root))
            .collect();
        while let Some(node) = pending.pop_front() {
            if !reachable.insert(node.clone()) {
                continue;
            }
            if let Some(dependencies) = self.dependencies.get(&node) {
                pending.extend(dependencies.iter().cloned());
            }
        }
        reachable
    }

    /// Returns deterministic consumer-first shutdown waves for `live` nodes.
    ///
    /// Dependencies are placed in a later wave than every live consumer,
    /// including when passive intermediate nodes create a transitive ordering.
    ///
    /// # Errors
    ///
    /// Returns the exact cycle members when the live subgraph is not acyclic.
    pub fn shutdown_waves(&self, live: &BTreeSet<K>) -> Result<Vec<Vec<K>>, Vec<K>> {
        let mut live_graph = Self::new();
        for node in live {
            live_graph.add_node(node.clone());
            if let Some(dependencies) = self.dependencies.get(node) {
                for dependency in dependencies {
                    if live.contains(dependency) {
                        live_graph.add_dependency(node.clone(), dependency.clone());
                    }
                }
            }
        }
        let cycle_members = live_graph.cycle_members();
        if !cycle_members.is_empty() {
            return Err(cycle_members);
        }

        let dependents = live_graph.dependents();
        let mut remaining_dependents: BTreeMap<K, usize> = live_graph
            .dependencies
            .keys()
            .map(|node| (node.clone(), dependents.get(node).map_or(0, BTreeSet::len)))
            .collect();
        let mut ready: BTreeSet<K> = remaining_dependents
            .iter()
            .filter_map(|(node, count)| (*count == 0).then_some(node.clone()))
            .collect();
        let mut waves = Vec::new();

        while !ready.is_empty() {
            let wave: Vec<K> = std::mem::take(&mut ready).into_iter().collect();
            for consumer in &wave {
                let Some(dependencies) = live_graph.dependencies.get(consumer) else {
                    continue;
                };
                for dependency in dependencies {
                    let remaining = remaining_dependents
                        .get_mut(dependency)
                        .expect("every live dependency has a dependent count");
                    *remaining -= 1;
                    if *remaining == 0 {
                        let _ = ready.insert(dependency.clone());
                    }
                }
            }
            waves.push(wave);
        }

        if waves.iter().map(Vec::len).sum::<usize>() != live.len() {
            unreachable!("acyclic live graph must produce a shutdown wave for every node");
        }
        Ok(waves)
    }

    fn dependents(&self) -> BTreeMap<K, BTreeSet<K>> {
        let mut dependents: BTreeMap<K, BTreeSet<K>> = self
            .dependencies
            .keys()
            .cloned()
            .map(|node| (node, BTreeSet::new()))
            .collect();
        for (consumer, dependencies) in &self.dependencies {
            for dependency in dependencies {
                let _ = dependents
                    .get_mut(dependency)
                    .expect("every dependency is a graph node")
                    .insert(consumer.clone());
            }
        }
        dependents
    }
}

#[cfg(test)]
mod tests {
    use super::DependencyGraph;
    use std::collections::BTreeSet;

    /// Scenario: Two roots feed a transitive consumer chain.
    /// Guarantees: Topological layers are provider-first and deterministically sorted.
    #[test]
    fn topological_layers_are_deterministic() {
        let mut graph = DependencyGraph::new();
        graph.add_node("independent");
        graph.add_dependency("middle", "provider");
        graph.add_dependency("consumer", "middle");

        assert_eq!(
            graph.topological_layers(),
            Ok(vec![
                vec!["independent", "provider"],
                vec!["middle"],
                vec!["consumer"],
            ])
        );
    }

    /// Scenario: A third node depends on one member of a two-node cycle.
    /// Guarantees: Cycle diagnostics include only the strongly connected cycle members.
    #[test]
    fn cycle_members_exclude_blocked_dependents() {
        let mut graph = DependencyGraph::new();
        graph.add_dependency("a", "b");
        graph.add_dependency("b", "a");
        graph.add_dependency("c", "a");

        assert_eq!(graph.cycle_members(), vec!["a", "b"]);
        assert_eq!(graph.topological_layers(), Err(vec!["a", "b"]));
    }

    /// Scenario: Only one consumer variant is rooted in a graph with two branches.
    /// Guarantees: Reachability retains exactly that root and its transitive dependencies.
    #[test]
    fn reachability_excludes_dead_branches() {
        let mut graph = DependencyGraph::new();
        graph.add_dependency("local-consumer", "local-provider");
        graph.add_dependency("shared-consumer", "shared-provider");

        assert_eq!(
            graph.reachable_dependencies(["local-consumer"]),
            BTreeSet::from(["local-consumer", "local-provider"])
        );
    }

    /// Scenario: A passive node connects two active nodes in a dependency chain.
    /// Guarantees: Consumer-first waves preserve transitive ordering through the intermediate.
    #[test]
    fn shutdown_waves_preserve_transitive_order() {
        let mut graph = DependencyGraph::new();
        graph.add_dependency("consumer", "passive");
        graph.add_dependency("passive", "provider");
        let live = BTreeSet::from(["consumer", "passive", "provider"]);

        assert_eq!(
            graph.shutdown_waves(&live),
            Ok(vec![vec!["consumer"], vec!["passive"], vec!["provider"]])
        );
    }

    /// Scenario: A dependency chain exists beside an unrelated live root.
    /// Guarantees: The unrelated root shuts down immediately with terminal consumers.
    #[test]
    fn shutdown_waves_do_not_delay_independent_roots() {
        let mut graph = DependencyGraph::new();
        graph.add_dependency("consumer", "provider");
        graph.add_node("independent");

        let live = BTreeSet::from(["consumer", "independent", "provider"]);
        assert_eq!(
            graph.shutdown_waves(&live),
            Ok(vec![vec!["consumer", "independent"], vec!["provider"]])
        );
    }
}
