# Alert on SLA Breaches via Metric Sinks to Enable Immediate Notification of Stream Performance Degradation

## User Story

**As a** DevOps engineer monitoring production Structured Streaming pipelines,
**I want to** receive SLA breach alerts as Dropwizard gauge metrics published through all configured metric sinks (Prometheus via PrometheusServlet at `/metrics/prometheus/`, JMX via JmxSink, Graphite via GraphiteSink, Slf4j via Slf4jSink, and CSV via CsvSink) whenever a streaming query's processing latency exceeds the configured `maxLatencyMs` or throughput falls below the configured `minThroughputRowsPerSec`, with each alert containing the query name, SLA type (latency or throughput), target value, observed value, breach count, and timestamp,
**So that** existing monitoring infrastructure (Prometheus/Grafana dashboards, JMX-based alerting, Graphite/StatsD collectors) can ingest SLA breach signals without custom integration work, reducing alert setup time from hours of custom scripting to under 5 minutes of metric sink configuration and enabling immediate automated notification when stream performance degrades.

### Secondary Persona

**As a** data platform administrator responsible for streaming pipeline reliability,
**I want to** see SLA breach metrics in the same dashboards and alerting channels used for all other Spark metrics,
**So that** SLA violation monitoring is consolidated with existing infrastructure health monitoring, eliminating the need to maintain a separate alerting pipeline and reducing mean-time-to-detection of streaming performance degradation from tens of minutes to under 60 seconds.

## Acceptance Criteria

### AC-1: Input Validation — Prometheus Sink Registration

- **Given** a Spark application with the Prometheus metric sink enabled via `*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet` in `metrics.properties`
- **When** a streaming query triggers an SLA breach
- **Then** the SLA breach gauge metric is registered in the MetricRegistry and is accessible via the PrometheusServlet at `/metrics/prometheus/` within the same polling cycle

### AC-2: Expected Output — Prometheus Format

- **Given** a streaming query named `ordersStream` that breaches its latency SLA (observed: 5500ms, target: 3000ms)
- **When** the Prometheus endpoint is scraped
- **Then** the response contains a metric named `driver.streaming.sla.ordersStream.latencyBreached` with a value of `1` (indicating active breach) and a companion metric `driver.streaming.sla.ordersStream.latencyBreachCount` with the cumulative count of breaches since query start

### AC-3: Expected Output — JMX

- **Given** a streaming query named `eventsStream` that breaches its throughput SLA (observed: 5000 rows/sec, target: 10000 rows/sec)
- **When** the JmxSink is configured and active
- **Then** a JMX MBean is exposed under the domain `metrics` with attributes `streaming.sla.eventsStream.throughputBreached` (value `1`) and `streaming.sla.eventsStream.throughputBreachCount` (cumulative count)

### AC-4: Expected Output — Breach Resolved

- **Given** a streaming query named `ordersStream` that previously had an active latency SLA breach
- **When** the query's processing latency drops below the configured `maxLatencyMs` for 1 completed micro-batch
- **Then** the `driver.streaming.sla.ordersStream.latencyBreached` gauge value is set to `0` (indicating resolved) while the breach count remains unchanged

### AC-5: Error Handling — No Sinks Configured

- **Given** a Spark application with no metric sinks configured in `metrics.properties` or via `spark.metrics.conf.*` properties
- **When** a streaming query triggers an SLA breach
- **Then** the SLA breach gauge is still registered in the internal MetricRegistry, a warning is logged indicating no sinks are available to export the breach metric, and no exception is thrown

### AC-6: Error Handling — Sink Failure

- **Given** a Graphite sink configured to send metrics to an unreachable Graphite host
- **When** an SLA breach metric is published
- **Then** the MetricsSystem logs the connection failure, continues to register the metric in the local MetricRegistry, and does not block or crash the streaming query execution

### AC-7: Edge Case — Multiple Concurrent Queries

- **Given** 3 streaming queries (`queryA`, `queryB`, `queryC`) each with their own SLA targets running in the same SparkSession
- **When** `queryA` and `queryC` breach their respective latency SLAs simultaneously
- **Then** separate gauge metrics are registered for each query (e.g., `driver.streaming.sla.queryA.latencyBreached` and `driver.streaming.sla.queryC.latencyBreached`) and `queryB` shows a breach value of `0`

### AC-8: Edge Case — Metric Name Sanitization

- **Given** a streaming query with a name containing special characters (e.g., `orders-stream.v2`)
- **When** an SLA breach gauge is registered
- **Then** the metric name replaces dots and special characters with underscores to ensure compatibility with all metric sinks (resulting in `driver.streaming.sla.orders_stream_v2.latencyBreached`)

## Sub-Tasks

- [ ] **ST-1**: Create `SlaAlertSource` class extending `org.apache.spark.metrics.source.Source` that registers SLA breach gauges in a `MetricRegistry` (following the pattern in `MetricsReporter.scala`)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — Reference pattern for Dropwizard gauge registration (inputRate-total, processingRate-total, latency gauges)
- [ ] **ST-2**: Implement gauge registration for each SLA type per query: `<queryName>.latencyBreached` (0 or 1), `<queryName>.latencyBreachCount` (cumulative), `<queryName>.throughputBreached` (0 or 1), `<queryName>.throughputBreachCount` (cumulative)
- [ ] **ST-3**: Register the `SlaAlertSource` with the MetricsSystem using `registerSource()` so that all configured sinks automatically export the breach gauges
  - `Source: core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala` — MetricsSystem registerSource(), MetricRegistry, sink management
- [ ] **ST-4**: Implement metric name sanitization — replace dots, hyphens, spaces, and other special characters with underscores for sink compatibility across Prometheus, JMX, Graphite, Slf4j, and CSV sinks
  - `Source: core/src/main/scala/org/apache/spark/metrics/sink/PrometheusServlet.scala` — normalizeKey() method for Prometheus format metrics endpoint
- [ ] **ST-5**: Integrate `SlaAlertSource` with the `SlaEvaluator` (from STORY-005-02-02) to update gauge values on each SLA evaluation cycle after every micro-batch completion
- [ ] **ST-6**: Implement breach state tracking — toggle breached gauge between `0` and `1`, increment breach count on each new violation transition from non-breached to breached state
- [ ] **ST-7**: Handle breach resolution — set breached gauge to `0` when SLA is met, preserve cumulative breach count without decrementing
- [ ] **ST-8**: Add logging via Slf4j for breach events (query name, SLA type, target value, observed value, timestamp) at WARN level, and resolution events at INFO level
  - `Source: core/src/main/scala/org/apache/spark/metrics/sink/Slf4jSink.scala` — Logging-based metrics export
- [ ] **ST-9**: Write unit tests validating gauge registration, value updates, metric name sanitization, and multi-query isolation with at least 90% code coverage for `SlaAlertSource`
- [ ] **ST-10**: Write integration tests verifying metric availability through Prometheus and JMX sinks using the PrometheusServlet `/metrics/prometheus/` endpoint and JMX MBean queries
  - `Source: core/src/main/scala/org/apache/spark/metrics/sink/JmxSink.scala` — JMX metrics export
  - `Source: core/src/main/scala/org/apache/spark/metrics/sink/GraphiteSink.scala` — Graphite metrics export

## Edge Cases

- **Empty/Null Input**: `SlaEvaluator` passes a `null` or empty query name to `SlaAlertSource` — the source must reject the registration with an `IllegalArgumentException` specifying that query name must be a non-empty string; no gauge is registered and the rejection is logged at ERROR level
- **Boundary Values**: Breach count reaches `Long.MAX_VALUE` (9,223,372,036,854,775,807) — the system must handle the overflow by resetting the counter to `0` and logging a warning that the counter has wrapped around for the specific query, including the query name and SLA type in the log message
- **Invalid Input**: A metric sink class specified in configuration does not exist or fails to instantiate — MetricsSystem must log the error at ERROR level and continue operating with remaining sinks without affecting SLA breach metric registration in the internal `MetricRegistry`
  - `Source: core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala` — registerSinks() handles instantiation failures with error logging
- **Rapid State Changes**: SLA breach and resolution alternate on every micro-batch — the gauge must reflect the exact state (`0` or `1`) at each evaluation without debouncing, and the breach count must increment on each new breach occurrence (each transition from `0` to `1` increments the count by exactly 1)
- **Query Restart**: A streaming query stops and restarts with the same name — breach gauges must be re-registered (or reused if already present in the `MetricRegistry`) and breach count must reset to `0` for the new query instance using `MetricsSystem.removeSource()` followed by `registerSource()` to ensure clean state

## Dependencies

- **STORY-005-02-02** (Configure Processing SLA Targets) — Provides the SLA target values (`maxLatencyMs`, `minThroughputRowsPerSec`) and `SlaEvaluator` that produces SLA violation records consumed by `SlaAlertSource` to update gauge metrics
- **MetricsSystem** (`core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala`) — Central metrics registration and sink management; `registerSource()` and `removeSource()` methods used to register and deregister the `SlaAlertSource`; `buildRegistryName()` constructs the full metric path including app ID and instance prefix
- **MetricsReporter** (`sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala`) — Reference pattern for registering Dropwizard gauges for streaming metrics; demonstrates `MetricRegistry.register()` with `Gauge[T]` for inputRate-total, processingRate-total, and latency metrics
- **PrometheusServlet** (`core/src/main/scala/org/apache/spark/metrics/sink/PrometheusServlet.scala`) — Prometheus format endpoint at `/metrics/prometheus/` that serializes gauges from the `MetricRegistry` using `normalizeKey()` for metric name formatting
- **JmxSink, GraphiteSink, Slf4jSink, CsvSink** (`core/src/main/scala/org/apache/spark/metrics/sink/`) — Metric export destinations that automatically consume gauges registered in the `MetricRegistry`; JmxSink uses `JmxReporter.forRegistry()`, GraphiteSink uses `GraphiteReporter.forRegistry()`
- **FEATURE-005-01** (Stream Health Monitoring) — Underlying health metrics infrastructure providing processing rate, watermark, and state store metrics that complement SLA breach alerts
- **STORY-005-02-01** (Detect Backpressure Conditions) — Backpressure detection metrics are closely related to SLA breach alerts; both use the same MetricsSystem registration pattern and may share dashboard views
- `Source: docs/monitoring.md` — Metrics sinks configuration reference, Prometheus endpoint documentation, REST API endpoints

## Story Estimation Guidance

- **Story Points**: 5 (Fibonacci scale)
- **Justification**: Follows an established pattern (`MetricsReporter.scala`) for gauge registration using `MetricRegistry.register()` with `Gauge[T]`, but requires multi-query metric management, metric name sanitization logic, breach state tracking (active/resolved state machine), and integration with the existing MetricsSystem across all sink types. Moderate complexity with clear reference implementations reduces uncertainty.
- **Complexity Factors**:
  - Multi-query gauge management — dynamic registration and deregistration of per-query gauges
  - Metric name sanitization across sink formats — Prometheus, JMX, and Graphite have different naming constraints
  - Breach state machine (active/resolved) — toggling between `0` and `1` with cumulative count tracking
  - Integration with multiple sink types — ensuring gauge values propagate through Prometheus, JMX, Graphite, Slf4j, and CSV sinks
  - Concurrent query safety — thread-safe gauge registration and update for multiple simultaneous streaming queries
- **Estimated Duration**: 3–5 days for a single developer familiar with the Spark metrics subsystem
- **Risk Factors**: Low — the MetricsReporter pattern is well-established and the MetricsSystem API is stable; primary risk is ensuring metric name compatibility across all sink formats

## Definition of Done

- [ ] `SlaAlertSource` class is implemented extending `org.apache.spark.metrics.source.Source` and is registered with the `MetricsSystem` via `registerSource()`
- [ ] SLA breach gauge metrics are registered in the Dropwizard `MetricRegistry` for each streaming query with configured SLA targets: `<queryName>.latencyBreached`, `<queryName>.latencyBreachCount`, `<queryName>.throughputBreached`, `<queryName>.throughputBreachCount`
- [ ] Breach metrics are accessible through all configured sinks: Prometheus (at `/metrics/prometheus/`), JMX (as MBeans under `metrics` domain), Graphite, Slf4j, and CSV
- [ ] Breach state (`0` for resolved, `1` for active) and cumulative breach count are tracked per query per SLA type
- [ ] Breach resolution sets the breached gauge to `0` while preserving the cumulative breach count
- [ ] Metric names are sanitized — dots, hyphens, spaces, and special characters are replaced with underscores for compatibility across all sink formats
- [ ] Multiple concurrent queries maintain isolated metrics without interference — each query's breach gauges are independent
- [ ] Unit tests validate gauge registration, gauge value updates (breach and resolution), metric name sanitization, breach count increment logic, and multi-query isolation scenarios
- [ ] Integration tests verify metric availability through at least Prometheus (`/metrics/prometheus/` endpoint scrape) and JMX (MBean attribute query) sinks
- [ ] WARN-level log messages are emitted for each breach event and INFO-level log messages for each resolution event, including query name, SLA type, target value, observed value, and timestamp
- [ ] Null or empty query names are rejected with `IllegalArgumentException` before gauge registration
- [ ] Code review is complete with no unresolved comments
