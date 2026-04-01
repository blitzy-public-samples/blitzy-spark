# Deliver Enhanced Real-Time Stream Observability for Production Streaming Workloads

## Epic Summary

Apache Spark Structured Streaming monitoring is limited to basic metrics exposed through `StreamingQueryProgress`; production teams operating streaming pipelines lack proactive health monitoring, backpressure detection, and SLA tracking capabilities, leading to delayed incident response and data delivery failures. This epic delivers an enhanced observability layer that extends the Spark Web UI and the Dropwizard Metrics 4.2.33 integration with real-time stream health dashboards, automated backpressure detection, configurable SLA target management, resource scaling recommendations, and exportable observability reports — replacing reactive troubleshooting with proactive, metrics-driven monitoring for both micro-batch and continuous processing modes. The scope covers stream health monitoring (processing rates, watermark progression, state store size, processing lag, checkpoint status) and backpressure/SLA management (backpressure condition detection, SLA target configuration, breach alerting, scaling recommendations, observability report generation) but does NOT include auto-scaling implementation or modifications to stream processing execution logic.

## Features Index

| Feature ID | Feature Name | Description | Link |
|---|---|---|---|
| FEATURE-005-01 | Stream Health Monitoring | Real-time monitoring of stream processing rates, watermark progression, state store size, processing lag detection, and checkpoint status — displayed in the Spark Web UI Structured Streaming tab and exposed through the metrics system | [FEATURE-005-01-stream-health-monitoring.md](./EPIC-005/FEATURE-005-01-stream-health-monitoring.md) |
| FEATURE-005-02 | Backpressure and SLA Management | Automated detection of backpressure conditions, configurable processing SLA targets with breach alerting, resource scaling recommendations based on observed stream performance, and exportable observability reports in JSON and CSV formats | [FEATURE-005-02-backpressure-and-sla-management.md](./EPIC-005/FEATURE-005-02-backpressure-and-sla-management.md) |

## Execution Strategy

### Parallel Epic Execution

This epic has **no cross-epic dependencies** and can be executed simultaneously with EPIC-001, EPIC-002, EPIC-003, and EPIC-004. Each epic operates on independent Spark subsystems with distinct module boundaries, enabling all five epics to run in parallel with separate development teams.

### Implementation Phases — Backend Before Frontend

All stories within this epic are organized into two sequential phases. **Phase 1 (Backend)** must be completed and fully tested before **Phase 2 (Frontend)** begins, ensuring that all metrics collection, detection algorithms, SLA configuration, alerting logic, and persistence layers are stable before UI visualization and report rendering work starts.

#### Phase 1 — Backend (Metrics Collection, Detection Algorithms, SLA Configuration, and Alerting)

Backend stories establish the stream metrics pipeline, backpressure detection engine, SLA target configuration, breach alerting, and scaling recommendation analysis. These must be implemented and pass all unit and integration tests before Phase 2 begins.

| Story ID | Story Name | Feature | Rationale |
|----------|-----------|---------|-----------|
| STORY-005-01-03 | Monitor State Store Size | FEATURE-005-01 | State store metrics collection — backend metrics pipeline |
| STORY-005-01-04 | Detect Processing Lag | FEATURE-005-01 | Lag detection algorithm — backend analysis engine |
| STORY-005-02-01 | Detect Backpressure Conditions | FEATURE-005-02 | Backpressure detection algorithm — backend analysis engine |
| STORY-005-02-02 | Configure Processing SLA Targets | FEATURE-005-02 | SLA configuration persistence — backend configuration layer |
| STORY-005-02-03 | Alert on SLA Breaches | FEATURE-005-02 | Breach alerting logic — backend Dropwizard Metrics integration |
| STORY-005-02-04 | Recommend Resource Scaling | FEATURE-005-02 | Scaling analysis algorithm — backend recommendation engine |

#### Phase 2 — Frontend (Visualization, Dashboards, and Reports)

Frontend stories consume data produced by Phase 1 backend services. These must not begin until all Phase 1 stories pass acceptance testing.

| Story ID | Story Name | Feature | Rationale |
|----------|-----------|---------|-----------|
| STORY-005-01-01 | Display Stream Processing Rates | FEATURE-005-01 | Web UI rate display — depends on metrics collection from Phase 1 |
| STORY-005-01-02 | Visualize Watermark Progression | FEATURE-005-01 | Web UI watermark visualization — depends on metrics pipeline from Phase 1 |
| STORY-005-01-05 | Show Checkpoint Status | FEATURE-005-01 | Web UI checkpoint health display — depends on checkpoint metrics from Phase 1 |
| STORY-005-02-05 | Generate Observability Reports | FEATURE-005-02 | JSON/CSV report generation — depends on all metrics, SLA, and scaling data from Phase 1 |

### Phase Gate Criteria

- **Phase 1 → Phase 2 Gate**: All 6 backend stories must have passing unit tests, passing integration tests, and code review approval before any Phase 2 story begins development
- **Sprint Planning**: Phase 1 stories should be prioritized in Sprints 1–2; Phase 2 stories should be planned for Sprints 3–4 after Phase 1 gate is passed

## Dependencies

- **F-003 (Real-Time Stream Processing / Structured Streaming)** — Provides the streaming query execution engine, micro-batch and continuous processing modes, checkpoint semantics, watermark propagation, and the `StreamingQuery` lifecycle that this epic instruments and monitors
- **Spark Web UI** (`docs/web-ui.md`) — Structured Streaming tab serves as the primary display surface for stream health dashboards, processing rate visualizations, watermark progression charts, and backpressure indicators
- **Metrics System — Dropwizard 4.2.33** (`core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala`) — Underlying `MetricRegistry`, `Source`, and `Sink` abstractions used to register streaming-specific gauges, counters, and histograms; supports export to Prometheus, Graphite, StatsD, JMX, CSV, and Console sinks
- **StreamingQuery API** (`sql/core/`) — `StreamingQueryProgress` provides per-batch input/processing/output rates, `StateStore` exposes state size and compaction metrics, offset tracking enables lag computation, and checkpoint metadata provides durability status
- **Kafka Integration** (`connector/kafka-0-10/`) — Kafka source-specific consumer group offset lag metrics, per-partition offset tracking, and rate limiter patterns used for Kafka-aware backpressure detection and lag reporting

## Definition of Done

- All features (FEATURE-005-01 and FEATURE-005-02) are complete, integrated, and verified against the Structured Streaming engine
- Stream health monitoring displays processing rates (input rows/sec, processed rows/sec), watermark progression (current watermark timestamp and advancement delta), state store size (number of keys, memory usage in bytes), processing lag (batch duration vs. trigger interval), and checkpoint status (last successful checkpoint time, checkpoint duration, checkpoint size) within the Spark Web UI Structured Streaming tab
- All stream health metrics are registered as named Dropwizard gauges and counters in the `MetricRegistry` and are accessible through all configured metric sinks (Prometheus, Graphite, StatsD, JMX, CSV, Console) and the REST API at `/api/v1/applications/[app-id]/streaming/statistics`
- Backpressure detection identifies and reports backpressure conditions in real-time when the ratio of batch processing duration to trigger interval exceeds a configurable threshold (default: 0.8), with per-source granularity for Kafka consumer lag
- SLA targets are configurable via Spark configuration properties (`spark.streaming.sla.*`) specifying maximum end-to-end latency, minimum throughput, and maximum processing lag per streaming query
- SLA breach events trigger alerts through the metrics and alerting system, emitting structured alert payloads containing query ID, breach type, observed value, threshold value, and timestamp
- Resource scaling recommendations are generated based on observed stream performance patterns, outputting specific suggestions for executor count, executor memory, and partition count adjustments when sustained SLA breaches or backpressure conditions are detected
- Observability reports consolidating health metrics, backpressure events, SLA compliance status, and scaling recommendations are exportable in both JSON and CSV formats via the REST API and programmatic API
- Integration tests validate monitoring accuracy and metric correctness across both micro-batch and continuous processing modes, covering at least: steady-state monitoring, backpressure simulation, SLA breach detection, watermark advancement tracking, state store growth monitoring, and checkpoint failure scenarios
- Documentation covers monitoring setup instructions, SLA configuration reference, alert configuration guide, scaling recommendation interpretation, and observability report generation with code examples in Scala, Python, and SQL
- Monitoring overhead is measured through dedicated performance benchmarks and must add less than 3% to stream processing latency (p99) when all observability features are enabled
- No regressions are introduced in existing Structured Streaming functionality, confirmed by passing the full streaming test suite
