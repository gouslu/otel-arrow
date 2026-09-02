// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Validation and deterministic ordering for extension dependencies.

use crate::error::Error;
use otel_arrow_dfe_config::ExtensionId;
use otel_arrow_dfe_config::dependency_graph::DependencyGraph;
use otel_arrow_dfe_config::pipeline::PipelineExtensions;
use std::collections::HashSet;

/// Returns extension IDs grouped into dependency layers.
///
/// Every extension in a layer depends only on extensions in earlier layers.
/// IDs within a layer are sorted to make construction and lifecycle ordering
/// deterministic across runs.
pub(crate) fn dependency_layers(
    extensions: &PipelineExtensions,
) -> Result<Vec<Vec<ExtensionId>>, Error> {
    let known: HashSet<ExtensionId> = extensions.keys().cloned().collect();
    let mut dependency_graph = DependencyGraph::new();
    for extension in &known {
        dependency_graph.add_node(extension.clone());
    }

    for (consumer, config) in extensions.iter() {
        for provider in config.capabilities.values() {
            if !known.contains(provider) {
                return Err(Error::ExtensionDependencyNotFound {
                    extension: consumer.clone(),
                    dependency: provider.clone(),
                });
            }
            dependency_graph.add_dependency(consumer.clone(), provider.clone());
        }
    }

    dependency_graph
        .topological_layers()
        .map_err(|extensions| Error::ExtensionDependencyCycle { extensions })
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_config::CapabilityId;
    use otel_arrow_dfe_config::extension::ExtensionUserConfig;

    fn extension(dependencies: &[(&str, &str)]) -> ExtensionUserConfig {
        let mut config = ExtensionUserConfig::with_type("urn:test:extension:dependency");
        config.capabilities = dependencies
            .iter()
            .map(|(capability, provider)| {
                (
                    CapabilityId::from((*capability).to_owned()),
                    ExtensionId::from((*provider).to_owned()),
                )
            })
            .collect();
        config
    }

    /// Scenario: Extensions form two independent roots and a transitive dependency chain.
    /// Guarantees: The graph is returned in deterministic dependency layers.
    #[test]
    fn creates_deterministic_dependency_layers() {
        let mut extensions = PipelineExtensions::default();
        extensions.insert("consumer".into(), extension(&[("cap_b", "middle")]));
        extensions.insert("independent".into(), extension(&[]));
        extensions.insert("middle".into(), extension(&[("cap_a", "provider")]));
        extensions.insert("provider".into(), extension(&[]));

        let layers = dependency_layers(&extensions).expect("dependency graph is valid");
        assert_eq!(
            layers,
            vec![
                vec![
                    ExtensionId::from("independent"),
                    ExtensionId::from("provider")
                ],
                vec![ExtensionId::from("middle")],
                vec![ExtensionId::from("consumer")],
            ]
        );
    }

    /// Scenario: An extension binds a capability to an undefined provider.
    /// Guarantees: Graph validation names both the consumer and missing dependency.
    #[test]
    fn rejects_missing_dependency() {
        let mut extensions = PipelineExtensions::default();
        extensions.insert("consumer".into(), extension(&[("cap", "missing")]));

        let error = dependency_layers(&extensions).expect_err("missing dependency must fail");
        assert!(matches!(
            error,
            Error::ExtensionDependencyNotFound {
                extension,
                dependency,
            } if extension == "consumer" && dependency == "missing"
        ));
    }

    /// Scenario: Two extensions depend on each other.
    /// Guarantees: Graph validation rejects the cycle and reports its members.
    #[test]
    fn rejects_dependency_cycle() {
        let mut extensions = PipelineExtensions::default();
        extensions.insert("a".into(), extension(&[("cap_b", "b")]));
        extensions.insert("b".into(), extension(&[("cap_a", "a")]));

        let error = dependency_layers(&extensions).expect_err("cycle must fail");
        assert!(matches!(
            error,
            Error::ExtensionDependencyCycle { extensions }
                if extensions == vec![ExtensionId::from("a"), ExtensionId::from("b")]
        ));
    }
}
