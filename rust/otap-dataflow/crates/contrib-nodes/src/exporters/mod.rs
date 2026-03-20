// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/// Geneva Exporter for Microsoft telemetry backend
#[cfg(feature = "geneva-exporter")]
pub mod geneva_exporter;

/// Azure Monitor Exporter for Azure Logs Ingestion API
#[cfg(feature = "azure-monitor-exporter")]
pub mod azure_monitor_exporter;

/// Minimal shared exporter demonstrating key-value store capability usage
#[cfg(feature = "shared-kv-exporter")]
pub mod shared_kv_exporter;
