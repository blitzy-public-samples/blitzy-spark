# Configure Quality Alerting Thresholds with Dropwizard Metrics Integration to Enable Proactive Data Quality Monitoring

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

As a **data platform administrator**, I want to configure quality alerting thresholds per rule and per DataFrame (via `spark.quality.alert.threshold.*` properties), with threshold violations triggering metric gauge updates exposed through all configured Dropwizard sinks (Prometheus, JMX, Graphite, StatsD, Console, CSV, Slf4j), so that I can receive proactive alerts through existing monitoring infrastructure (Grafana/Prometheus, Datadog, PagerDuty) when data quality drops below defined thresholds, reducing mean time to respond to quality incidents from 4 hours to under 15 minutes.

## Acceptance Criteria

### AC-1: Input Validation — Threshold Breach Registers Gauge with Breach Indicator

- **Given** a SparkConf with property `spark.quality.alert.threshold.orders.non_null_ratio=0.95`
- **When** quality validation completes for DataFrame "orders" and column "email" has non_null_ratio of 0.88
- **Then** a Dropwizard Gauge named `spark.quality.alert.orders.email.non_null_ratio` is registered in the MetricRegistry with value 0.88, and a companion Gauge named `spark.quality.alert.orders.email.threshold_breached` is registered with value 1 (indicating breach)

### AC-2: Expected Output — No Breach Scenario Reports Zero Breach Status

- **Given** threshold `spark.quality.alert.threshold.orders.non_null_ratio=0.90`
- **When** column "customer_id" has non_null_ratio of 0.98
- **Then** the Gauge `spark.quality.alert.orders.customer_id.non_null_ratio` reports value 0.98 and `spark.quality.alert.orders.customer_id.threshold_breached` reports value 0 (no breach)

### AC-3: Error Handling — Non-Numeric Threshold Falls Back to Default

- **Given** a threshold property `spark.quality.alert.threshold.orders.non_null_ratio=abc` (non-numeric value)
- **When** the configuration is loaded during MetricsSystem initialization
- **Then** the system logs a warning message identifying the invalid threshold value and property key, and uses the default threshold of 0.8 for that rule

### AC-4: Expected Output — Sink Propagation Delivers Gauges to All Configured Sinks

- **Given** the metrics.properties file configures a Prometheus sink and a JMX sink
- **When** a quality threshold breach is detected for DataFrame "orders"
- **Then** the breach Gauge value is visible in the Prometheus exposition endpoint (`/metrics/prometheus/`) and in JMX MBeans within 1 reporting cycle (default 10 seconds as configured in MetricsConfig)

### AC-5: Expected Output — Per-DataFrame Threshold Configuration Applied Independently

- **Given** threshold `spark.quality.alert.threshold.orders.non_null_ratio=0.95` and `spark.quality.alert.threshold.customers.non_null_ratio=0.90`
- **When** quality validation completes for both DataFrames
- **Then** each DataFrame's metrics use its own configured threshold for breach detection independently

### AC-6: Edge Case — Global Default Threshold Used When Per-DataFrame Config Absent

- **Given** no per-DataFrame threshold is configured but `spark.quality.alert.threshold.default.non_null_ratio=0.85` is set
- **When** quality validation completes for DataFrame "products"
- **Then** the default threshold of 0.85 is used for breach detection on all columns of "products"

### AC-7: Expected Output — Metric Registration Follows MetricsSystem Source Pattern

- **Given** a quality alerting threshold is configured
- **When** the `QualityMetricsSource` is registered with the MetricsSystem via `registerSource()`
- **Then** all quality-related Gauges appear under the `spark.quality.alert.*` namespace in the MetricRegistry, following the source registration pattern in `core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala`

## Sub-Tasks

- Implement `QualityMetricsSource` class extending `org.apache.spark.metrics.source.Source` trait, providing `sourceName = "quality"` and a `MetricRegistry` instance, following the pattern of `JvmSource`, `JVMCPUSource`, and `AppStatusSource` in `core/src/main/scala/org/apache/spark/metrics/source/`
- Implement quality threshold configuration parser that reads `spark.quality.alert.threshold.<dataframe>.<metric>` properties from SparkConf using the property prefix pattern, following the configuration loading approach in `core/src/main/scala/org/apache/spark/metrics/MetricsConfig.scala`
- Implement `registerQualityGauges(dataframeName: String, columnName: String, metricName: String, currentValue: Double, threshold: Double)` method that registers two Gauges in the MetricRegistry: one for the current metric value and one for threshold breach status (1 for breach, 0 for no breach)
- Register `QualityMetricsSource` with the `MetricsSystem` during quality subsystem initialization using `metricsSystem.registerSource(qualityMetricsSource)`, following the source registration lifecycle in `core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala`
- Implement threshold resolution logic: first check per-DataFrame threshold (`spark.quality.alert.threshold.<dataframe>.<metric>`), then fall back to global default (`spark.quality.alert.threshold.default.<metric>`), then fall back to system default (0.8)
- Ensure gauge updates are atomic and thread-safe using `AtomicDouble` or `AtomicLong` for gauge backing values, following the pattern of `AppStatusSource` counters in `core/src/main/scala/org/apache/spark/status/AppStatusSource.scala`
- Validate that metrics flow to all configured sinks (Prometheus, JMX, Graphite, StatsD, Console, CSV, Slf4j) via the existing sink infrastructure in `core/src/main/scala/org/apache/spark/metrics/sink/`
- Add configuration parameters: `spark.quality.alert.threshold.<dataframe>.<metric>`, `spark.quality.alert.threshold.default.<metric>`, and `spark.quality.alert.enabled` (boolean, default true)
- Write unit tests for threshold parsing, gauge registration, breach detection logic, default fallback, and invalid configuration handling
- Write integration test validating metric propagation to Prometheus and JMX sinks after a quality validation event

## Edge Cases

- **Edge Case 1 (Empty/Null Input):** No threshold configuration properties are set — system uses default threshold of 0.8 for all DataFrames and metrics; no error is raised.

- **Edge Case 2 (Boundary Values):** Quality metric value is exactly equal to the threshold (e.g., non_null_ratio=0.95 with threshold=0.95) — the `threshold_breached` Gauge reports 0 (no breach), as the condition is strictly less-than threshold.

- **Edge Case 3 (Invalid Input):** Threshold value set to a number outside [0.0, 1.0] (e.g., `spark.quality.alert.threshold.orders.non_null_ratio=1.5`) — the system logs a warning identifying the out-of-range value and clamps the threshold to the nearest valid boundary (1.0 in this case).

- **Edge Case 4 (Feature Disabled):** `spark.quality.alert.enabled=false` — the `QualityMetricsSource` is not registered with MetricsSystem, no quality Gauges appear in the MetricRegistry, and quality validation completes without metric gauge updates.

- **Edge Case 5 (Dynamic DataFrame Names):** DataFrame names containing dots or special characters (e.g., "my.database.table") — the metric name sanitization replaces dots with underscores to produce valid Dropwizard metric names (e.g., `spark.quality.alert.my_database_table.email.non_null_ratio`).

## Dependencies

- **STORY-002-02-01** (Compute Completeness Metrics) — provides the metric values (non_null_ratio, distinct count) compared against thresholds
- **FEATURE-002-01** (DataFrame Quality Rules Engine) — provides rule validation results triggering threshold checks
- **FEATURE-002-02** (Quality Metrics and Reporting) — parent feature
- **EPIC-002** (Declarative Data Quality Validation Framework) — parent epic
- **MetricsSystem** (`Source: core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala`) — lifecycle manager for Dropwizard metrics sources and sinks; provides `registerSource()`, `buildRegistryName()`, `start()`/`stop()` lifecycle, and sink integration (MetricsServlet, PrometheusServlet, JMX, Graphite, StatsD, Console, CSV, Slf4j sinks)
- **MetricsConfig** (`Source: core/src/main/scala/org/apache/spark/metrics/MetricsConfig.scala`) — configuration loader for metrics.properties and `spark.metrics.conf.*` SparkConf overrides; provides `subProperties()` for instance-scoped configuration parsing and `getInstance()` for per-instance Properties retrieval
- **Source trait** (`Source: core/src/main/scala/org/apache/spark/metrics/source/`) — `Source` trait with `sourceName` and `metricRegistry` fields; existing implementations include `JvmSource`, `JVMCPUSource`, `AccumulatorSource`, `StaticSources`
- **AppStatusSource** (`Source: core/src/main/scala/org/apache/spark/status/AppStatusSource.scala`) — pattern for defining Gauges and Counters backed by atomic values in a metrics Source
- **Sink implementations** (`Source: core/src/main/scala/org/apache/spark/metrics/sink/`) — `PrometheusServlet` (text exposition format), `MetricsServlet` (JSON via Jackson), `JmxSink`, `GraphiteSink`, `StatsdSink`, `ConsoleSink`, `CsvSink`, `Slf4jSink`

## Story Estimation Guidance

- **Story Points:** 8 (Fibonacci)
- **Rationale:** High complexity — requires implementing a custom metrics Source with dynamic Gauge registration, building a hierarchical threshold configuration parser with per-DataFrame and global-default fallback, integrating with the MetricsSystem lifecycle, validating propagation to multiple sink types, and handling metric name sanitization for special characters. The Dropwizard metrics patterns are established but dynamic Gauge registration and threshold resolution add design complexity. Estimated 5-7 days of development effort for one engineer.
- **Risk Factors:** Dynamic Gauge registration (Gauges added at runtime as DataFrames are validated) may encounter thread-safety issues with the MetricRegistry; metric name sanitization must produce valid identifiers across all sink types (Prometheus has specific naming restrictions); threshold configuration property namespace design must avoid collisions with existing `spark.*` properties.

## Definition of Done

- `QualityMetricsSource` is implemented and registered with MetricsSystem when `spark.quality.alert.enabled=true`
- Quality Gauges are registered dynamically as DataFrames are validated, with metric value and threshold breach status per column per DataFrame
- Threshold resolution follows per-DataFrame → global default → system default (0.8) fallback hierarchy
- Gauge values propagate to all configured Dropwizard sinks within one reporting cycle
- Invalid threshold values (non-numeric, out-of-range) are handled with warnings and fallback to defaults
- `spark.quality.alert.enabled=false` prevents QualityMetricsSource registration and gauge updates
- DataFrame names with special characters produce sanitized, valid metric names
- Unit tests cover threshold parsing, gauge registration, breach detection, default fallback, invalid config, and metric name sanitization
- Integration test validates metric visibility in Prometheus endpoint and JMX MBeans after quality validation
- Code review completed and merged to feature branch
- No forbidden terms used in any acceptance criteria or documentation
