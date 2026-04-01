# Deliver Enhanced Real-Time Stream Observability for Production Streaming Workloads

## Epic Summary

Apache Spark Structured Streaming monitoring is limited to basic metrics exposed through `StreamingQueryProgress`; production teams operating streaming pipelines lack proactive health monitoring, backpressure detection, and SLA tracking capabilities, leading to delayed incident response and data delivery failures. This epic delivers an enhanced observability layer that extends the Spark Web UI and the Dropwizard Metrics 4.2.33 integration with real-time stream health dashboards, automated backpressure detection, configurable SLA target management, resource scaling recommendations, and exportable observability reports — replacing reactive troubleshooting with proactive, metrics-driven monitoring for both micro-batch and continuous processing modes. The scope covers stream health monitoring (processing rates, watermark progression, state store size, processing lag, checkpoint status) and backpressure/SLA management (backpressure condition detection, SLA target configuration, breach alerting, scaling recommendations, observability report generation) but does NOT include auto-scaling implementation or modifications to stream processing execution logic.

## Features Index

| Feature ID | Feature Name | Description | Link |
|---|---|---|---|
| FEATURE-005-01 | Stream Health Monitoring | Real-time monitoring of stream processing rates, watermark progression, state store size, processing lag detection, and checkpoint status — displayed in the Spark Web UI Structured Streaming tab and exposed through the metrics system | [FEATURE-005-01-stream-health-monitoring.md](./EPIC-005/FEATURE-005-01-stream-health-monitoring.md) |
| FEATURE-005-02 | Backpressure and SLA Management | Automated detection of backpressure conditions, configurable processing SLA targets with breach alerting, resource scaling recommendations based on observed stream performance, and exportable observability reports in JSON and CSV formats | [FEATURE-005-02-backpressure-and-sla-management.md](./EPIC-005/FEATURE-005-02-backpressure-and-sla-management.md) |

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
