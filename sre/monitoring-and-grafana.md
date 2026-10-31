# Monitoring & Grafana — SRE Expert Checklist

A dashboard shows healthy pods while customers report timeouts. Another dashboard is empty, but nobody knows whether traffic stopped or the monitoring pipeline failed. An SRE needs to explain both the service and the system measuring it: where data originates, how it changes in transit, what a query actually measures, and how a notification reaches someone who can act.

This guide follows the supplied checklist and assumes basic Linux, HTTP, and Kubernetes knowledge. Examples use illustrative metrics, labels, and endpoints. Configuration fragments demonstrate mechanisms and need environment-specific authentication, resource limits, and validation. Grafana features differ by release, plugin, edition, and managed-service offering; the references point to official documentation checked while preparing this guide. No Grafana version is pinned by this repository.

## Contents

1. [Monitoring & Observability Fundamentals](#1-monitoring--observability-fundamentals)
2. [Grafana Architecture & Internals](#2-grafana-architecture--internals)
3. [Metrics & Data Sources](#3-metrics--data-sources)
4. [Prometheus & PromQL](#4-prometheus--promql)
5. [Dashboards & Visualization](#5-dashboards--visualization)
6. [Grafana Alerting](#6-grafana-alerting)
7. [Grafana Deployment & High Availability](#7-grafana-deployment--high-availability)
8. [Grafana as Code & GitOps](#8-grafana-as-code--gitops)
9. [Security & Access Control](#9-security--access-control)
10. [Performance & Scalability](#10-performance--scalability)
11. [Kubernetes & Cloud Monitoring](#11-kubernetes--cloud-monitoring)
12. [Reliability & Troubleshooting](#12-reliability--troubleshooting)
13. [Expert Interview Scenarios](#13-expert-interview-scenarios)

## 1. Monitoring & Observability Fundamentals

### Monitoring, observability, and measurement boundaries

**Monitoring** collects and evaluates signals to answer operational questions: is the service reachable, are errors increasing, is storage almost full? **Observability** is the ability to infer internal behavior from available outputs, including when investigating a failure that was not anticipated by an existing dashboard. Buying four telemetry backends does not guarantee observability; instrumentation must preserve enough context to test competing explanations.

**White-box monitoring** inspects internal state, such as queue depth, garbage collection, database locks, and application counters. **Black-box monitoring** tests externally visible behavior, such as completing an HTTPS request from another region. A successful health endpoint does not prove a checkout works; a failed external probe does not identify which internal dependency failed. Combine both perspectives.

| Signal | What it reveals | Main limitation |
| --- | --- | --- |
| Metrics | Aggregated rates, distributions, utilization, trends | Aggregation loses individual-event context; labels consume resources |
| Logs | Events, errors, state transitions, detailed context | Volume, indexing cost, sensitive fields, inconsistent structure |
| Traces | Request paths, dependency latency, retries, causality | Sampling and missing context can hide failures |
| Profiles | Where CPU time, allocations, or blocking occur in code | Collection overhead and interpretation depend on profiler and runtime |

Use consistent service identity, timestamps, and trace IDs to move between signals. A latency metric establishes impact, an exemplar selects a representative trace, logs explain an error, and a profile identifies a costly code path. A sampled trace is evidence about that request, not a count of all requests.

### Golden Signals, RED, and USE

The **Golden Signals** are latency, traffic, errors, and saturation. Split successful and failed request latency: fast errors can make aggregate latency look better during an outage. Saturation measures how close a resource is to its useful limit, often through queue length or waiting time rather than utilization alone.

**RED** measures Rate, Errors, and Duration for a request-oriented service. **USE** measures Utilization, Saturation, and Errors for each resource, such as CPU, disks, network interfaces, or connection pools. RED helps locate the affected service; USE helps explain which resource constrains it. For a worker queue, add arrival rate, completion rate, backlog age, and processing duration because HTTP request rate alone misses the actual work.

### SLIs, SLOs, SLAs, and budgets

An **SLI** is a measured reliability indicator with a precise numerator, denominator, measurement point, and time window. An availability SLI might be successful eligible requests divided by all eligible requests at the public edge. Define what counts as success: a business failure returned as HTTP 200 is not automatically a good event.

An **SLO** is the target for that indicator over a window. An **SLA** is a contractual agreement with defined consequences and may use different exclusions or measurement rules. The **error budget** is the permitted unreliability: for a request SLO, `(1 − SLO) × eligible requests`. For a time-based availability SLO of 99.95% over 30 days, the allowance is 21.6 minutes. A request-based budget is not automatically equivalent to that many minutes when traffic varies.

**Burn rate** is `observed bad-event fraction / allowed bad-event fraction`. A burn rate of 1 consumes budget at the sustainable average rate; 10 consumes it ten times as quickly under comparable traffic. Use budget policy to guide release risk and reliability investment. Section 6 works through multi-window alerting, following the approach in the [Google SRE workbook](https://sre.google/workbook/alerting-on-slos/).

### Collection models and monitoring scope

With **pull**, a collector discovers targets and requests measurements. It controls scrape cadence and observes scrape failures, but needs network reachability. With **push**, producers send telemetry to a receiver, which suits outbound-only networks and event pipelines but requires explicit detection of missing producers. Backpressure, buffering, retries, and timestamps matter in both models. Prometheus remote write pushes collected samples; it does not mean applications necessarily push their metrics.

An **agent-based** design runs a collector near workloads and can read local files, processes, or host metrics. An **agentless** design queries exposed APIs, network protocols, or managed-service endpoints. Agentless still requires a collector somewhere and often credentials with considerable access. Prometheus Pushgateway is mainly for appropriate short-lived batch-job patterns; retained series need lifecycle management and it is not a general replacement for scraping services.

Infrastructure monitoring covers hosts and resources; application monitoring covers request and business behavior; network monitoring covers loss, latency, bandwidth, DNS, and reachability. **Synthetic monitoring** exercises a known transaction on a schedule. Define vantage points and test data so a synthetic check measures the intended path without creating real purchases or destructive actions.

### Dimensions, cardinality, and operational response

A dimensional metric identifies a series by metric name and its complete label set. If all combinations occur, 100 services × 10 routes × 5 methods × 6 status classes × 20 instances produce 600,000 series before adding histogram buckets. Actual combinations and churn determine real cost. A new pod label creates a new series even if the measurement describes the same logical service.

Use bounded labels such as environment, region, cluster, service, route template, and status class. Avoid request IDs, raw URLs, emails, and arbitrary user input. Put per-request identity in logs, traces, or exemplars. Hashing a user ID does not reduce cardinality or necessarily make it safe to collect.

Monitoring provides evidence. **Alerting** decides when a condition deserves attention and routes it. **Incident management** assigns ownership, coordinates mitigation and communication, and records learning. An alert without a response owner or useful next action creates noise even if its query is mathematically correct.

## 2. Grafana Architecture & Internals

### Frontend, backend, and storage

Grafana's browser frontend renders dashboards, query editors, Explore, and administration pages. Its backend serves APIs, authenticates requests, checks authorization, manages configuration, executes supported queries, and evaluates Grafana-managed alert rules. Plugins provide panel visualizations, applications, and data source integrations.

Grafana normally queries telemetry stored elsewhere. Its configuration database stores dashboards, users, permissions, data source definitions, and other application state; it is not the Prometheus TSDB. Grafana Cloud bundles managed telemetry services, but that does not make a standalone Grafana server a metrics storage engine.

```text
Browser -> reverse proxy -> Grafana API / plugin backend -> telemetry backend
                                  |
                                  +-> configuration database
Grafana alert evaluator ----------> supported data source -> alert routing
```

For a typical panel, the frontend substitutes variables and sends a query request to Grafana. Grafana applies access checks and the data source/plugin executes the upstream request. The telemetry backend evaluates its query language and returns results; Grafana returns data frames for rendering and transformations. SQL connects through a backend component. Historical browser/direct modes are plugin-specific and must not be assumed available in current releases.

### Plugins and query execution

A frontend plugin supplies browser code such as a query editor. A backend plugin also runs a server-side executable, communicating with Grafana over gRPC. This allows protocols, secrets, and scheduled queries that cannot depend on a user's browser. Grafana-managed alerting requires a compatible backend data source with alerting support; a panel that renders successfully does not prove that its source supports alerts. See the [plugin backend architecture](https://grafana.com/developers/plugin-tools/key-concepts/backend-plugins/).

Browser transformations and dashboard variables are presentation features, not a shared computation engine for alert rules. When a dashboard calculation must drive an alert, express the logic in the backend query or a supported server-side expression and verify the resulting labels and values.

### Configuration database and resource model

SQLite is convenient for a single instance. PostgreSQL or MySQL provides a shared configuration database for an HA fleet. Database availability, connection limits, backups, and migrations become part of Grafana availability. An object-store bucket containing dashboards cannot replace this database.

Dashboards have stable UIDs and contain panels and queries. Folders organize resources and access. Organizations separate groups of resources and memberships within one installation; users can belong to multiple organizations. Teams simplify access assignment. Service accounts represent automation and receive scoped permissions rather than borrowing a human's login. None of these concepts automatically configures tenant isolation inside a telemetry backend.

### Editions, APIs, and real-time streaming

Grafana OSS provides core visualization, data source integrations, provisioning, and alerting. Enterprise adds commercial features such as selected enterprise plugins and finer access controls; Grafana Cloud operates a managed stack with plan-dependent features and limits. Authentication integrations, auditing, caching, and authorization capabilities need edition-specific checks before promising them in a design.

Provisioning files, HTTP APIs, and Terraform automate resources. Preserve stable UIDs across environments where practical and check the API supported by the deployed release; resource APIs and legacy endpoints evolve independently. Do not automate by editing Grafana's database tables directly.

**Grafana Live** streams data over WebSockets instead of repeatedly polling for every update. Proxies must permit upgrades, appropriate idle timeouts, and allowed origins. Connection capacity and reconnect storms require planning. Multi-instance stream distribution needs its own HA design; the Live Redis engine is separate from alerting HA coordination. See [Grafana Live setup](https://grafana.com/docs/grafana/latest/setup-grafana/set-up-grafana-live/).

## 3. Metrics & Data Sources

### Match the backend to the signal and operating model

| Backend | Typical role | Decision to explain in an interview |
| --- | --- | --- |
| Prometheus | Scraping, local TSDB, PromQL, rules | Simple local operation; each server has finite memory and disk |
| VictoriaMetrics | Metrics storage in single-server or cluster deployments | Compare topology, ingestion, MetricsQL compatibility, and operational limits |
| Mimir | Multi-tenant scalable Prometheus-compatible storage | Distributed ingestion/querying and object storage require more components |
| InfluxDB | Time-series workloads | Query languages, APIs, and storage architecture differ materially by major version |
| Graphite | Hierarchical metric paths and time-series aggregation | Naming and retention/aggregation schemas influence what can be queried later |
| Loki | Log storage with label-oriented indexing | Keep indexed labels bounded; inspect log content for detailed fields |
| Elasticsearch / OpenSearch | Search and analytics over indexed documents | Mapping, shard count, indexing, and retention determine cost and latency |
| Tempo / Jaeger | Distributed trace storage and search | Sampling, context propagation, retention, and storage topology matter |
| PostgreSQL / MySQL / ClickHouse | SQL exploration and analytics | Bound queries and use suitable schemas; plugin support varies by database |

ClickHouse is usually accessed through an installed data source plugin. SQL panels should apply time filters and sensible limits; a dashboard refreshing an unbounded analytical query can overload a production database. Metrics, logs, and traces should share service identity without forcing every backend to index every dimension.

### Exporters, discovery, and authentication

An exporter translates system state into a metrics endpoint. Prometheus discovers potential targets through static configuration, Kubernetes, cloud APIs, DNS, or file discovery. Target relabeling selects targets and establishes labels before scraping; metric relabeling filters or changes samples after collection. Remote-write relabeling acts on the samples sent to a remote destination. These stages have different effects and troubleshooting locations.

The collector must authenticate to the target, and Grafana must authenticate separately to the query backend. Use TLS with hostname and CA validation, narrowly scoped tokens, and private connectivity where appropriate. Server-side proxying protects upstream credentials from routine browser exposure, but makes Grafana an outbound network client that needs egress controls. Browser access, where a plugin supports it, adds CORS and client-network dependencies and is unsuitable for embedding shared secrets.

### Prometheus, Mimir, and Thanos

Prometheus combines discovery, scraping, local storage, querying, and rules in one server. An HA pair independently scrapes and stores similar data; it is not a shared-disk cluster. Shard collection when a server cannot handle the target/series volume, and retain local rules for failures that must be detected during a central outage.

Mimir separates scalable ingestion, storage, query, and rule functions and supports tenancy and object storage. Current architectures can include Kafka-backed ingest storage, so describe the deployed architecture rather than assuming every Mimir installation uses the same write path. Thanos commonly extends existing Prometheus servers through sidecars, object storage, and a query layer; its Receive component also supports remote-write-oriented architectures. Deduplication needs consistent replica labels. See the [Mimir architecture](https://grafana.com/docs/mimir/latest/references/architecture/) and [Thanos components](https://thanos.io/tip/components/).

The decision is about local autonomy, tenant boundaries, global queries, retention, failure domains, and operating cost. Object storage alone does not solve current ingestion failures, query capacity, or durable buffering. Check each product's actual retention and downsampling capabilities rather than treating all Prometheus-compatible backends as identical.

### Correlation and federation

An **exemplar** associates a metric observation with metadata such as a trace ID without making that ID a metric label. Grafana can link the observation to a trace if storage, instrumentation, sampling, and data source configuration support it. Missing exemplars do not prove tracing failed, and an exemplar can reference a trace already expired or not retained.

A mixed-source panel sends different queries to different data sources. Transformations can combine returned frames, but this is not an arbitrary distributed join executed inside Prometheus. Grafana correlations and data links navigate between signals using fields and labels. True federated queries require a backend capable of querying across the intended stores. Prometheus federation instead scrapes selected series from another Prometheus server; it is a distinct collection mechanism.

## 4. Prometheus & PromQL

### TSDB, WAL, chunks, and retention

Recent samples enter the TSDB head and are protected by a write-ahead log so a restart can replay recent writes. Samples are encoded into chunks, and persisted blocks contain chunks, an index, and metadata. Compaction merges blocks and retention eventually removes eligible old data. A restart can take substantial time when replaying a large head; killing it repeatedly may prevent recovery. See the [Prometheus storage documentation](https://prometheus.io/docs/prometheus/latest/storage/).

Storage demand includes the head, WAL, indexes, compacted blocks, and temporary compaction space. Retention limits are not an immediate disk-space guarantee. Use supported local storage and snapshots/backups suitable for the required recovery point; do not have multiple Prometheus servers write to the same TSDB directory.

### Collection time, evaluation time, and missing samples

The **scrape interval** determines how often Prometheus collects a target. The **evaluation interval** determines how often rules run; individual groups can override it. Scrape timeout bounds collection and must fit the configured interval. A dashboard's query step is a third setting: it controls result evaluation timestamps, not collection frequency.

An instant selector chooses a sufficiently recent sample before each evaluation time, subject to lookback and staleness. Staleness markers stop old series appearing current when a scrape or target disappears; removal timing differs by failure path. Range selectors collect samples in their window rather than filling gaps with zeros. A service producing no requests, a missing metric, and a failed scrape are three different states. See [PromQL query semantics](https://prometheus.io/docs/prometheus/latest/querying/basics/).

### Metric and expression types

| Type | Meaning | Correct use |
| --- | --- | --- |
| Counter | Cumulative count that can reset | `rate()` or `increase()` before aggregation |
| Gauge | Current value that can rise or fall | Direct value, aggregation, or suitable over-time functions |
| Classic histogram | Cumulative bucket counters plus count and sum | Aggregate compatible bucket rates, retaining `le`, then estimate quantiles |
| Native histogram | Structured histogram samples with bucket information | Use supported histogram-aware operations across a compatible pipeline |
| Summary | Client-computed quantiles and count/sum | Quantiles describe each instrumented instance/window and cannot simply be aggregated |

An **instant vector** has one sample per selected series at an evaluation time. A **range vector** contains a window of samples for each series. A **scalar** is one numeric value; PromQL also has a string type with limited practical use. Native histograms add structured samples rather than turning every sample into a float. A range query is repeated evaluation of an expression at successive steps, not another name for a range-vector selector.

### Rates and aggregation

```promql
# Request throughput, with counter resets handled per instance.
sum by (service) (
  rate(http_requests_total{env="prod"}[5m])
)

# Traffic-weighted server error fraction.
sum by (service) (rate(http_requests_total{env="prod",code=~"5.."}[5m]))
/
sum by (service) (rate(http_requests_total{env="prod"}[5m]))

# Average current queue depth across the selected instances.
avg by (service) (worker_queue_depth{env="prod"})

# Estimated requests over an hour; the result can be fractional.
sum by (service) (increase(http_requests_total{env="prod"}[1h]))
```

`rate()` estimates the average per-second counter increase over a range, adjusts for resets, and extrapolates to its boundaries. `irate()` uses the latest two samples, making it more sensitive to bursts and poor scrape timing. `increase()` expresses the corresponding estimated increase over the window. Use sufficiently sampled windows and inspect raw counters when results look wrong; extrapolated increases need not be integers. Apply rates before sums so resets remain visible. See the [PromQL function reference](https://prometheus.io/docs/prometheus/latest/querying/functions/).

The error expression can return no series if the error counter is never initialized. Zero traffic can produce `0/0`, which is not evidence of perfect availability. Initialize known bounded counters where appropriate, or use a carefully matched traffic-series baseline for missing error series. Do not use a global `or vector(0)` to declare all missing telemetry healthy. An average of instance error percentages also weights idle and busy instances equally; a ratio of summed counts measures the request-weighted result.

### Histograms and quantiles

```promql
# Classic histogram: keep le while combining buckets across instances.
histogram_quantile(
  0.99,
  sum by (service, le) (
    rate(http_request_duration_seconds_bucket{env="prod"}[5m])
  )
)

# Native histogram, when the entire collection/query path supports it.
histogram_quantile(
  0.99,
  sum by (service) (
    rate(http_request_duration_seconds{env="prod"}[5m])
  )
)
```

Classic histogram buckets are cumulative: the `le="0.3"` bucket includes observations at or below 300 ms. Choose boundaries near SLO thresholds. Quantiles interpolate within buckets and their accuracy depends on the distribution and bucket resolution. Never average per-instance p99 values to get a fleet p99. Summary quantiles cannot reconstruct an aggregate distribution; their count and sum can still support a combined mean. Native histograms improve representation and resolution choices but still have estimation error and require compatible clients, scraping, remote write, storage, and query support. See [histograms and summaries](https://prometheus.io/docs/practices/histograms/).

### Vector matching

Arithmetic between vectors matches label sets, which can silently remove unmatched series. `on(service, cluster)` selects the matching labels; `ignoring(instance)` excludes labels from the match. `group_left` permits many left-hand series per right-hand series, while `group_right` permits the reverse. The one side must actually be unique for the chosen keys; these modifiers do not make an ambiguous many-to-many arithmetic join valid.

```promql
sum by (cluster, service, code) (rate(http_requests_total[5m]))
/
on (cluster, service) group_left
sum by (cluster, service) (rate(http_requests_total[5m]))
```

This preserves each status code and divides it by its service total. Removing `cluster` from the keys could combine unrelated services or cause a matching error. Verify input uniqueness with `count by (...)` before attaching metadata. Logical operators such as `or`, `and`, and `unless` have set semantics; comparisons usually filter series unless `bool` is used. See the [operator reference](https://prometheus.io/docs/prometheus/latest/querying/operators/).

### Recording rules, downsampling, and remote storage

Recording rules persist expensive or frequently reused expressions under a new metric name. They improve dashboard response and standardize SLI definitions, but consume evaluation and storage capacity and do not retroactively generate history. Keep the dimensions required for later debugging and apply a consistent naming convention. Rules that depend on earlier results need an evaluation ordering and timing design appropriate to the rule engine.

Downsampling reduces temporal resolution or retains pre-aggregated data. A recording rule is not automatically a replacement for raw retention: both series remain until their storage policy removes them. Upstream downsampling must preserve counter and histogram semantics. A daily average cannot recover an hourly p99.

Remote write sends samples to another backend through a buffered queue and retry path. Monitor lag, throughput, failures, and backlog capacity; the buffer is not unlimited. Federation retrieves selected current series and is useful for hierarchical aggregates, but is not a complete raw-history backup. Long-term storage requires explicit durability, retention, deduplication, and recovery decisions.

## 5. Dashboards & Visualization

### Choose panels that answer a question

Time-series panels show change and correlation; stat panels show a clearly defined current or reduced value; gauges show position relative to a meaningful limit. Tables compare entities. Heatmaps show how a distribution evolves over time, while a histogram panel shows a distribution of values. Plotting raw cumulative histogram buckets as independent observations produces misleading distributions.

Set units, legends, thresholds, and reduction functions deliberately. A stat showing the maximum over 24 hours answers a different question from the latest sample. Use readable contrast and text labels so color is not the only indicator of failure. Show gaps as gaps where absence is meaningful, and annotate deployments and known incidents.

### Variables, repetition, and reuse

Query variables populate selections from a data source. Chained variables narrow choices, such as region → cluster → namespace → service. Reference multi-value selections using the correct query-language formatting; in PromQL this commonly means a regex matcher such as `cluster=~"$cluster"`. Bound variable queries to relevant metrics and scopes instead of retrieving every label value in a large tenant.

Repeating panels create a view for each selected entity, but selecting hundreds of pods multiplies query work and rendering cost. Prefer overview aggregates with drilldowns. Library panels reuse a common panel definition; changes affect its consumers, so review them as shared resources.

### Transformations and navigation

Transformations filter, join, reshape, and calculate returned frames. Join keys and timestamps must align, and a many-to-many join can greatly expand rows. Field overrides customize units, naming, thresholds, and display for selected fields without changing stored telemetry. Push expensive filtering and aggregation into the backend before returning large frames to the browser.

Dashboard links navigate between views; panel/data links pass context such as service, selected timestamp, or trace ID to a detailed view. Preserve time range and relevant variables, safely encode values, and avoid leaking credentials in URLs. A useful drilldown moves from fleet → service → instance → trace/log rather than opening an unrelated dashboard at the current time.

### Time, resolution, and query inspection

`$__interval` adapts a grouping interval to the displayed range and available resolution. It is not a safe universal range for `rate()`. For Prometheus, `$__rate_interval` is calculated as `max($__interval + scrape_interval, 4 × scrape_interval)`, using the query Min step when set or the data source scrape interval. Correct that setting if the actual scrape cadence differs. Dashboard variables do not exist in standalone alert evaluation. See [Prometheus template variables](https://grafana.com/docs/grafana/latest/datasources/prometheus/template-variables/).

Max data points, query step, panel minimum intervals, and backend limits affect cost and the shape of results. A week-long chart with a large step can miss a brief spike even though raw samples exist. Compare exact query, expanded variables, start/end timestamps, step, and instant/range mode when Grafana differs from another client.

The query inspector exposes request details, timings, returned data, and errors. Compare upstream query duration with transfer, transformation, and browser rendering time before tuning Grafana CPU. Grafana query caching is edition/data-source dependent; backend caches are a separate layer. Caching can reduce repeated work but must respect freshness and tenant boundaries.

### Dashboard lifecycle and design

Store dashboard JSON or the supported resource representation in Git, retaining stable UIDs and reviewing meaningful diffs. The dashboard schema evolves, so validate imports against the target version and strip only known volatile fields. A UI export may contain environment-specific data source references that need intentional mapping.

An infrastructure dashboard should lead with constrained resources and affected nodes. An application dashboard should lead with RED and dependency behavior. An SLO dashboard should show objective, actual SLI, remaining budget, and burn over useful windows. Separate fleet overviews from forensic views and state the measurement definition next to ambiguous indicators.

## 6. Grafana Alerting

### Ownership and the evaluation pipeline

**Grafana-managed rules** are stored and evaluated by Grafana, can use supported data sources, and can combine server-side expressions. **Data source-managed rules** are evaluated by the backend rule engine, such as Prometheus or a compatible ruler; Grafana's management support depends on the integration. Choose one owner for each rule so editing, deployment, and troubleshooting are unambiguous.

An evaluation queries the source, computes expressions, checks a condition, updates each alert instance, and hands firing/resolved changes to notification handling. One rule can produce an instance per label set. **Reduce** converts a time series to a number, **Math** combines numbers or series with label matching, and **Resample** aligns timestamps before operations. Reduction null/NaN handling and mismatched labels can change results; inspect intermediate outputs.

### States and timing

| State | Operational meaning |
| --- | --- |
| Normal | The configured alerting condition is not met |
| Pending | The condition is met, but its pending period has not elapsed |
| Alerting | The condition has persisted long enough to fire |
| Recovering | The breach ended, but the keep-firing period retains the firing state |
| No Data | Evaluation produced no usable data, subject to configured handling |
| Error | Evaluation failed or timed out, subject to configured handling |

An evaluation group defines cadence; a pending period requires persistence across evaluations, and a keep-firing period delays resolution to reduce flapping. These are independent of notification grouping delays. A long query, a missed schedule, and a long pending period all increase detection latency for different reasons. See the [Grafana alert lifecycle](https://grafana.com/docs/grafana/latest/alerting/fundamentals/alert-rule-evaluation/).

No Data and Error are Grafana-managed behaviors with configurable outcomes: dedicated states, Alerting, Normal, or Keep Last State. Current documentation applies the pending period to these state transitions; older versions can differ. Defaults can create separate `DatasourceNoData` and `DatasourceError` alerts, whose labels and routing differ from the original service alert. Keep Last State needs an independent stale-telemetry detector so a prolonged pipeline failure cannot preserve a misleading healthy state. See [No Data and Error behavior](https://grafana.com/docs/grafana/latest/alerting/fundamentals/alert-rule-evaluation/nodata-and-error-states/).

### Identity, routing, and notification timing

Labels identify and route alerts: `service`, `team`, `severity`, `environment`, and cluster are common. Annotations carry explanation, measured values, dashboard links, and runbooks. Do not put a continuously changing value in a label; it creates new alert identities, breaks deduplication, and can prevent a pending timer from completing.

Contact points define destinations such as Slack, email, or a paging system. Notification policies match labels in a routing tree and specify destinations, grouping, and timing. Know whether matching continues to siblings and which settings are inherited. Group by a useful incident boundary, such as team/service/alertname; grouping every pod separately produces storms, while grouping every service together hides distinct owners.

`group_wait` gives related first alerts time to join a group; `group_interval` controls updates to that group; `repeat_interval` controls reminders. Deduplication suppresses repeated equivalent notifications, while grouping bundles different alerts. Neither changes whether a rule is firing.

### Suppression, templates, and Alertmanager

A silence suppresses matching notifications for a bounded period. Mute timings express recurring suppression windows. Neither fixes the underlying condition or stops evaluation. **Inhibition** suppresses a dependent alert when another matching alert is active, such as suppressing many unreachable-instance alerts during a known cluster outage. Prometheus Alertmanager supports inhibition; verify support and management APIs for the chosen Grafana/Alertmanager version rather than assuming every UI exposes identical features.

Grafana alert annotations and notification templates use Go templating with different input contexts. An annotation describes an individual alert; a notification template can receive a group of firing and resolved alerts. Include impact, scope, start time, useful evidence, and a runbook. Test escaping and receiver formatting, and never include secrets in messages.

Alertmanager receives alerts, routes/groups them, applies silences and inhibition, deduplicates notifications, and retries deliveries. Prometheus HA replicas should deliver to all appropriate Alertmanager peers rather than depend on a single load-balanced recipient. A cluster exchanges notification and silence state; it does not replace identical routing configuration. Delivery is not an exactly-once guarantee, particularly during partitions or ambiguous receiver timeouts. See [Alertmanager HA](https://prometheus.io/docs/alerting/latest/high_availability/).

### Worked SLO: 99.95% over 30 days

Assume all eligible requests increment `slo_requests_total{service="checkout"}` and every bad eligible request also increments `slo_errors_total` with the same service identity. Initialize both counters, define timeout handling at the measurement point, and evaluate no-traffic separately. The allowed bad-event fraction is `0.0005`.

Create recording rules for the ratio of summed error rates to summed request rates over 5m, 30m, 1h, and 6h. For example, the 1h expression is:

```promql
sum by (service) (rate(slo_errors_total[1h]))
/
sum by (service) (rate(slo_requests_total[1h]))
```

For this hypothetical policy, page when either fast or sustained consumption is present:

```promql
(
  service:slo_error_ratio:rate1h > 14.4 * 0.0005
  and on (service)
  service:slo_error_ratio:rate5m > 14.4 * 0.0005
)
or
(
  service:slo_error_ratio:rate6h > 6 * 0.0005
  and on (service)
  service:slo_error_ratio:rate30m > 6 * 0.0005
)
```

These recording-rule names are illustrative and must be defined before using the expression. The thresholds are 0.72% and 0.30% bad requests. Over a 720-hour window, `14.4 × 1 / 720 = 2%` and `6 × 6 / 720 = 5%` of the budget, assuming comparable traffic. The long window establishes significance and the short window confirms the problem continues. Treat slower burn as a ticket using additional windows appropriate to the response policy; section 13 revisits this design.

Low traffic makes ratios noisy and absent traffic is undefined. Use synthetic transactions or a separate availability indicator for critical quiet services. Any minimum-traffic gate must have a documented alternative detector, or it can suppress a real outage. The [SRE multi-window approach](https://sre.google/workbook/alerting-on-slos/) explains the detection and reset trade-offs.

### Actionability and HA

Page for conditions requiring immediate action, usually user impact or imminent exhaustion. Use tickets for gradual capacity trends. A pending period rejects brief breaches; hysteresis uses different firing and recovery thresholds; keep-firing delays resolution. They solve different forms of flapping. Review false positives, repeated pages, and alerts nobody can act on.

By default, each Grafana HA replica evaluates all rules. Notification coordination uses Memberlist, or Redis where direct peer communication is unsuitable; a shared SQL database alone does not deduplicate notifications. Some current releases offer single-node evaluation mode, so verify its availability and failover behavior before relying on it. Network partitions can still cause duplicates: test peer loss, not just process failure. See [Grafana alerting HA](https://grafana.com/docs/grafana/latest/alerting/set-up/configure-high-availability/).

## 7. Grafana Deployment & High Availability

### Packaging, configuration, and persistence

Bare-metal and VM installs commonly use a service manager and persistent local configuration. Containers make images reproducible but require explicit persistence and secrets injection. Kubernetes adds scheduling, probes, and rollout controls; it does not make a single Grafana pod or its database highly available.

The Helm chart renders resources from values: image/plugin versions, `grafana.ini`, data sources, dashboards, ingress, persistence, and service settings. Inspect rendered manifests rather than assuming a chart value reached Grafana. A PVC is useful for a single-instance SQLite installation or local data, but sharing a SQLite file between replicas is not an HA design. Plugins can be baked into a pinned image to avoid startup dependence on downloads.

Grafana loads shipped defaults, then its selected custom configuration. `GF_<SECTION>_<KEY>` environment variables override corresponding file settings. Startup arguments and the package/chart can select paths and introduce further overrides; inspect the actual process arguments and startup logs for the effective result. Authentication settings saved through supported UI/API workflows can also take precedence over file settings. Do not edit `defaults.ini`. Environment expansion in provisioning has its own rules, especially for values containing `$`. See [Grafana configuration](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/).

### Active-active servers and sessions

Place two or more Grafana instances behind a load balancer and use shared PostgreSQL or MySQL. Align secrets/encryption settings, authentication, plugins, and provisioned resources across replicas. Make the database independently resilient, since all frontends depend on it. Standard login session information is shared through the database, so ordinary HA operation does not require sticky sessions. See [Grafana HA architecture](https://grafana.com/docs/grafana/latest/setup-grafana/set-up-for-high-availability/).

Configure the public `root_url`, scheme, subpath handling, forwarded headers, and secure cookies consistently. Trust forwarded identity only from the intended proxy. Allow WebSocket upgrades for Live and appropriate timeouts. Use readiness to withdraw a failing replica, and drain requests during termination; probe design should avoid restarting every replica during a transient dependency outage.

### Alerting coordination and failure domains

For Memberlist-based alerting HA, configure peer discovery and routable advertise addresses and permit the required TCP/UDP peer traffic, commonly port 9094. Kubernetes deployments often use a headless service for peers rather than a normal service VIP. Redis coordination instead requires a supported pub/sub setup, authentication, and TLS. Alerting Redis support and Grafana Live Redis support are separate and need separate compatibility checks.

Keep rule identities, labels, notification configuration, and receiver ownership consistent. Monitor peer membership and notification failures, and deliberately test a partition. Deduplicating notifications does not remove replicated query load. Sending the same rule through both Grafana and an independent Prometheus pipeline can also generate duplicates unrelated to cluster gossip.

### Backups, upgrades, and rollback

Back up the configuration database with a database-consistent method, provisioned files, required secrets/encryption keys, certificates, and plugin/image version records. SQLite backups need a consistent snapshot, commonly with Grafana stopped or an appropriate database backup method. Keep metrics/log/trace backend backups as a separate recovery plan. A dashboard JSON export alone cannot restore users, credentials, and alerting configuration.

Test restoration into an isolated environment with notification delivery disabled or redirected, then verify authentication, data source decryption, dashboards, and rules. Define RPO and RTO and include the SQL database in recovery exercises.

For upgrades, read migration and plugin compatibility notes, pin versions, validate a restored copy, back up, and roll with sufficient spare capacity. Zero downtime depends on database migration behavior and supported version coexistence; it is an objective to verify, not a guarantee of rolling pods. An older binary may not understand the migrated schema. A rollback may require restoring the prior database and configuration, with an explicit accounting of changes lost since the backup.

## 8. Grafana as Code & GitOps

### Data sources and dashboards

Keep desired resource definitions in Git with stable UIDs, reviewed changes, and secrets supplied through a secret manager. This conventional data source fragment uses server-side access; set TLS and authentication appropriate to the actual endpoint:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus-main
    type: prometheus
    access: proxy
    url: https://prometheus.example.internal
    isDefault: true
    editable: false
    jsonData:
      httpMethod: POST
      timeInterval: 30s
```

The value `timeInterval` describes the scrape cadence used for query calculations; it does not reconfigure Prometheus scraping. Dashboard providers then load JSON files from a configured path:

```yaml
apiVersion: 1
providers:
  - name: sre
    orgId: 1
    folder: SRE
    type: file
    disableDeletion: true
    allowUiUpdates: false
    options:
      path: /var/lib/grafana/dashboards
```

This example retains database dashboards when provisioning files disappear. Deliberate deletion therefore needs a separately reviewed action. Other policies can allow deletion, but file disappearance during a failed mount must not unexpectedly remove dashboards. Provisioned UI edits, if allowed, do not automatically update the Git files and can be overwritten by subsequent provisioning. See [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).

### Rules, policies, Terraform, and APIs

Alerting provisioning includes rules, contact points, policies, and mute timings, using schemas distinct from dashboard JSON. Export a minimal working resource from the target Grafana release, review its UIDs and data source references, and manage the full intended policy tree. A valid YAML parser cannot prove the alert query works or that a route reaches its receiver. Use supported provisioning APIs or files; do not assume an ordinary API export is directly importable as a provisioning file.

The Terraform Grafana provider manages supported resources and exposes drift through plans. Pin provider versions, import existing resources before adopting them, and protect state because sensitive values can be stored there. HTTP APIs are useful for focused automation with service account tokens; use bounded privileges, avoid logging tokens, and handle retries without creating duplicate resources. Each resource should have one reconciliation owner rather than competing Terraform, file, and UI writers.

### Kubernetes and environment strategy

A Helm sidecar can discover labeled ConfigMaps or Secrets and write their content into provisioning directories. Scope its watch permissions and namespaces. Argo CD or Flux reconciles Kubernetes manifests; the sidecar and Grafana then perform subsequent steps. A healthy GitOps application does not prove a dashboard imported successfully or an alert rule evaluated successfully—observe every stage.

Promote a shared dashboard definition with environment-specific data source UIDs or an explicit mapping. Scope resources by organization, folder, tenant, and service ownership, keeping actual tenant authorization in the backend. Validate JSON/YAML, render Helm, check UID uniqueness, review deletion effects, and test representative queries and routing before promotion.

## 9. Security & Access Control

### Authentication and authorization

OAuth2/OIDC delegates login to an identity provider; validate issuer, audience, redirects, scopes, and role mapping. LDAP integrates a directory, with group synchronization details depending on edition. SAML availability is edition-dependent. Auth proxy mode trusts an identity supplied by a reverse proxy, so direct access must be blocked and untrusted client identity headers stripped. Authentication does not prove the user is entitled to every data source. See [Grafana authentication options](https://grafana.com/docs/grafana/latest/setup-grafana/configure-access/configure-authentication/).

Basic organization roles and folder/dashboard permissions handle common access patterns. Fine-grained RBAC, data source permissions, and audit features vary by edition. Dashboard access alone does not constrain which data a user can query: by default, organization users can query organization data sources independently of a dashboard's saved queries. Where available, data source permissions limit query access, but per-tenant or per-row restrictions still belong in the backend or a trusted gateway. See [data source permissions](https://grafana.com/docs/grafana/latest/administration/data-source-management/).

Treat service account tokens as credentials: narrow their role/scope, set expiry where supported, rotate them, revoke unused accounts, and store them outside repositories. An API authenticated as a powerful service account can perform powerful actions regardless of the caller's intent. Prefer separate automation identities for distinct responsibilities.

### Secrets, transport, and plugins

Protect browser-to-Grafana and Grafana-to-backend traffic with TLS and valid trust chains. Restrict access to configuration, encryption keys, backups, and secret mounts. Encryption of stored data source secrets does not protect them from a compromised Grafana server that must decrypt and use them. Preserve encryption settings during restoration and rotate compromised credentials at the source.

Use HTTPS, secure/HttpOnly cookies, appropriate SameSite behavior for the authentication flow, and a correct public URL. Restrict proxy and server egress to intended destinations to limit SSRF impact. Keep internal metadata endpoints and privileged admin services unreachable from untrusted query paths.

Install trusted, signed plugins, pin their versions, and review compatibility and security advisories. Signing establishes provenance and integrity under the signing model, not absence of vulnerabilities. Backend plugins run executable code; frontend plugins run in users' browsers. Avoid broad unsigned-plugin exceptions and remove unused plugins.

### Query safety and governance

Dashboard variables are user input, not an authorization boundary. Escaping depends on SQL, regex, or other query syntax; never assume interpolation makes arbitrary queries safe. Give SQL data sources dedicated read-only accounts constrained to approved views/schemas, with timeouts and resource limits. Prevent data modification at the database layer, not merely through a hidden query editor.

Audit access changes, token creation, data source changes, rule edits, and administrative actions using supported audit facilities or complementary proxy/identity/database logs. Review memberships and service accounts regularly. Check tenant headers at a trusted gateway: a caller-supplied tenant ID must not grant access to another tenant's metrics.

## 10. Performance & Scalability

### Understand query multiplication

A hypothetical dashboard with 20 panels × 2 queries × 100 viewers refreshing every 10 seconds can create roughly 400 query requests per second before cache reuse, batching, and variable queries. Repeated panels and chained variable lookups add more work. Reducing refresh frequency or pre-aggregating a common expression may help more than adding Grafana replicas.

Bound time ranges, returned series, and resolution. Increase concurrency only after identifying spare capacity; otherwise it converts a manageable queue into memory pressure and upstream overload. Query timeouts should form a coherent chain across browser/proxy, Grafana, plugin, and backend, leaving room to cancel work rather than abandoning expensive orphaned queries.

### Backend and server bottlenecks

High cardinality expands active-series memory, indexes, ingestion work, query fan-out, and remote-write traffic. Churn adds series creation and compaction work even when concurrent series counts look stable. Start expensive PromQL with selective labels and aggregate at the right stage; a final `topk()` does not necessarily avoid scanning its expensive input.

Recording rules amortize repeated calculations but increase continuous evaluation load. Caching helps repeated identical work and needs tenant-aware keys and acceptable freshness. Data source connection pools protect SQL systems, but pool size must respect the shared database connection budget across all Grafana replicas.

Track Grafana HTTP latency/errors, CPU, memory, database latency/connections, plugin resource use, and query durations. For alerting, track evaluation duration, scheduling lag, failures, and rule-instance counts. Ensure evaluation duration fits cadence and reserve capacity for degraded backends. Default replicated Grafana evaluation multiplies backend query load with the number of replicas.

### Capacity, retention, and geography

Estimate samples per second from active series divided by scrape interval, then measure real peak ingestion, histogram payload size, churn, compression, and query patterns. As a hypothetical starting point, 2 million scraped series at 30 seconds imply about 66,700 samples/second before recording rules, duplicated collection, and other streams. Size head memory, WAL, compaction disk headroom, network, object storage requests, and failure recovery separately.

Retain fine resolution where it supports incident response and use suitable aggregate retention for older trends. Downsampling and retention differ across backends; Mimir, Thanos, and Prometheus do not all have identical native downsampling functions. Cost includes ingestion, storage, queries, cross-region transfer, and engineering effort, not just bytes at rest.

For multi-cluster/region deployments, preserve local collection and critical rules, standardize labels, and provide a global query path with explicit handling of partial responses. HA scraping needs backend-supported deduplication or distinct semantics to avoid double counting. Monitor the monitoring system with an independent vantage point, not only its own dashboard.

## 11. Kubernetes & Cloud Monitoring

### Distinguish state from resource consumption

**kube-state-metrics** exposes Kubernetes object state: desired replicas, phases, scheduling conditions, requests, and limits. **node-exporter** exposes host operating-system metrics. **cAdvisor**, commonly integrated through the kubelet, exposes container resource usage; kubelet metrics cover node-agent operations as well. These sources overlap in context but answer different questions. Permissions and available endpoints depend on the Kubernetes distribution and version.

Prometheus Operator reconciles Prometheus and related resources. A `ServiceMonitor` selects Services and their named endpoints; a `PodMonitor` selects pods. Prometheus must also select the monitor resources and their namespaces. Correct pod labels alone are insufficient if the Prometheus selector excludes the ServiceMonitor or its namespace. See the [Operator API reference](https://prometheus-operator.dev/docs/api-reference/api/).

### Workloads, control plane, and scaling

Monitor API server request errors/latency, admission failures, etcd latency/quorum/disk, scheduler backlog, and controller work queues where exposed. For workloads, compare desired/available replicas, readiness, restart causes, rollout duration, and service-level RED. Managed control planes may expose only a subset, requiring provider signals and external symptoms.

CPU usage can be low on average while CPU limits throttle bursts; inspect throttled periods/time alongside request latency and runnable pressure. OOMKilled indicates a container memory-limit failure in common cases; node-level OOM and eviction are different paths. Check termination reason, events, working set/RSS, limits, node pressure, and recent changes. A current gauge can miss the peak that already killed a process.

HPA uses resource, custom, or external metrics through the relevant APIs. CPU utilization targets relate to requests, so missing or unsuitable requests affect behavior. KEDA scales from event-source signals and an adapter path; inspect lag, source authentication, trigger health, and min/max constraints. Cluster autoscaler responds to unschedulable workloads and node-group constraints, not simply a high CPU chart. Verify that scaling signals remain available under the failure they are meant to fix.

### Network, storage, probes, and cloud services

Monitor DNS errors and latency, ingress/controller saturation, network drops/retransmits, CNI behavior, persistent-volume capacity/inodes, storage latency, and attach/mount failures. Correlate them with user-facing symptoms rather than paging separately for every transient pod event.

Blackbox exporter probes HTTP, TCP, DNS, and TLS from the exporter's location. Collect both probe success and duration/phase information, and monitor certificate expiry separately from present TLS success. Prometheus scrapes the exporter, which then probes the requested target; correct relabeling must preserve the target identity. Restrict selectable probe destinations so the exporter cannot become an internal network scanner.

Azure Monitor managed service for Prometheus stores Prometheus-compatible metrics, while Azure Managed Grafana is the managed visualization service. AWS Amazon Managed Service for Prometheus and Amazon Managed Grafana similarly separate collection/storage from visualization. Verify regional availability, supported APIs, identity, private networking, quotas, and pricing for the chosen offering rather than assuming parity with self-hosted software. See [Azure managed Prometheus](https://learn.microsoft.com/en-us/azure/azure-monitor/metrics/prometheus-metrics-overview) and [AWS managed Prometheus](https://docs.aws.amazon.com/prometheus/latest/userguide/what-is-Amazon-Managed-Service-Prometheus.html).

### Collectors and cross-cluster identity

OpenTelemetry Collector receives, processes, and exports telemetry using configured pipelines. Grafana Alloy is a collector distribution with OpenTelemetry and Prometheus integrations; it is not a telemetry database. Exporters expose or forward signals depending on context, so identify whether a named exporter is a scraped endpoint or an outbound pipeline component. Configure batching, memory limits, queues, retries, and persistent buffering where supported.

Standardize environment, cluster, region, namespace, and service identity across clusters while retaining bounded cardinality. For OpenTelemetry-to-Prometheus translation, verify name, unit, resource-attribute, and histogram handling. Avoid collecting the same endpoints through both an Operator-managed Prometheus and Alloy without an intentional duplication/deduplication plan.

## 12. Reliability & Troubleshooting

### Follow the data through each boundary

Start with the exact symptom, affected identity, and time range. Trace producer → discovery → scrape → local storage → remote ingestion → query → Grafana → alert evaluation → routing → receiver. Check the first boundary where expected evidence disappears. A successful later-stage health endpoint does not prove every earlier-stage input is present.

| Symptom | Evidence to inspect | Likely branches |
| --- | --- | --- |
| Missing metric | Exporter payload, discovered targets, relabeling, raw local query | Disabled collector, wrong process, filter, scrape/sample limits, conditional metric |
| Missing labels | Discovery metadata, relabeling stages, rule expressions, remote-write filters | Labels removed before scrape, aggregation removed dimensions, different remote path |
| Healthy target, absent expected series | Raw endpoint and sample counts, local vs remote query | `up=1` proves scrape success, not presence or correctness of every metric |
| Unexpected rates | Raw counter samples, scrape cadence, resets, instance identity | Too few samples, wrong type, reset hidden by aggregation, exporter bug |
| No Data alert | Exact rule query, labels, latest sample time, expected-series inventory | Empty query, stale identity, no requests, rolling pod replacement |
| Error alert | Evaluation error text, logs, data source health, query duration | Authentication, timeout, syntax, backend failure |
| Firing but no message | Receiver path, route labels, silence/mute state, delivery logs | Pending/group delay, wrong Alertmanager, failed webhook, blocked egress |
| Duplicate messages | Alert label sets, rule owners, HA membership, receiver configuration | Distinct identities, independent evaluators, partition, duplicate contact routes |
| Slow dashboard | Query inspector and backend timings, returned rows/series | Expensive query, fan-out, network transfer, browser transformation/rendering |

### Missing series, rolling pods, and counters

Query a raw metric with minimal selectors, then add labels and aggregations one at a time. `sum by (service)` intentionally removes pod labels; a subsequent pod filter cannot recover them. A series absent on one replica or tenant may be present on another. Distinguish local and remote storage and confirm that historical and current queries use the same labels.

For rolling deployments, alert on stable service/workload identity when user impact is service-wide. A pod-specific identity disappears naturally as pods are replaced. Grafana handles disappearance of some alert instances differently from an entire query returning No Data; check MissingSeries eviction behavior in the deployed version. Use desired-state inventory and `unless`-style comparisons when every expected entity must be accounted for. An `absent()` query alone cannot enumerate every missing label combination without an inventory.

Inspect counter resets with `resets()` and look at scrape timestamps and gaps. Rates need at least two suitable samples and robust windows. Stable target identity, correct counter types, and applying `rate()` before aggregation prevent several spike patterns. Do not smooth away unexplained spikes until instrumentation and query semantics are understood.

### Prometheus storage and remote write

For OOM, correlate active series/churn, head memory, query concurrency, and large-range queries with the termination time. Add headroom or reduce the offending workload; restarting without changing the cause repeats the outage. For slow WAL replay, inspect progress, disk latency, corruption messages, and startup resource limits before deciding it is hung.

If TSDB corruption is suspected, preserve the volume/snapshot and logs, identify the affected block/WAL region, and use version-appropriate documented recovery. Deleting the WAL or blocks can lose data; it is not a routine first troubleshooting step. Restore a known-good copy or isolate damaged data with explicit recovery trade-offs.

For remote-write backlog, compare produced samples/second with successfully sent samples/second, oldest unsent timestamp, retries, HTTP status codes, and queue/shard utilization. A 429 suggests rate limits or receiver saturation; authentication and rejected-label/sample errors need different fixes from transient server errors. Monitor dropped, failed, and retried samples separately. Buffer exhaustion or prolonged receiver loss can cause permanent gaps even after connectivity returns. See [remote-write tuning](https://prometheus.io/docs/practices/remote_write/).

### Grafana failures and blind spots

Database errors can affect login, dashboard load, configuration writes, and alerting. Check connection exhaustion, locks, migrations, disk space, and database failover. Authentication failures need identity-provider logs, redirect/public URL checks, clock synchronization, and trusted proxy headers. Plugin failures need plugin-specific logs, version compatibility, and upstream connectivity; a backend plugin can fail while Grafana itself still answers health checks.

Use Grafana `/metrics`, server logs, query inspector, and alert rule evaluation/state history together. State history requires the appropriate configuration/storage and may not retain the evidence you expect by default. Protect metrics and diagnostic endpoints according to deployment policy. A dashboard showing Grafana uptime does not prove its alert evaluator or notification delivery works.

Use an external dead-man/heartbeat check, independent synthetic requests, and periodic test notifications to detect end-to-end pipeline failure. Watch freshness and expected target coverage, not only error counters. A broken collector can emit no errors because it emits nothing. Test loss of DNS, identity, regional networking, storage, and the monitoring cluster itself, including how responders access runbooks during that outage.

## 13. Expert Interview Scenarios

### 1. Design an observability platform for 1,000+ Kubernetes nodes

Start with workload count, active series, churn, scrape cadence, telemetry volume, tenant boundaries, query concurrency, retention, and recovery goals. Node count alone cannot size the platform. Use per-cluster collection and local critical alerting, control cardinality at instrumentation/ingestion, and send metrics to a scalable backend such as Mimir or a Thanos-based architecture. Keep logs/traces/profiles in suitable stores with shared service identity. Explain HA ingestion deduplication, storage durability, limits per tenant, and measured capacity under a collector or zone failure. Finish with independent monitoring and a load test against realistic queries and churn.

### 2. Explain the lifecycle from metric generation to visualization

Walk through a request incrementing a counter in process memory, an exporter endpoint, discovery and target relabeling, a scrape and timestamp, metric relabeling, WAL/head/block storage, optional remote write, Grafana's authenticated query, PromQL evaluation, returned frames, transformations, and rendering. Identify where labels change and where delays accumulate. A missing panel can originate anywhere along that path; Grafana is only the last part.

### 3. Diagnose missing metrics despite healthy targets

Confirm the metric exists in the raw endpoint of the actual scraped pod, not a different replica. Check conditional initialization, disabled collectors, target/metric labels, sample limits, and metric relabeling. Query local Prometheus before the remote backend, then remove selectors and check freshness. `up=1` establishes successful scraping; it does not prove that a particular metric was exported, retained, forwarded, or selected correctly.

### 4. Explain unexpected `rate()` spikes or empty results

Inspect the raw counter over the same timestamps and count available samples. Empty output often follows too-short windows, rolling series identities, or missing scrapes. Spikes can arise from bad exporter resets/types, changed label identity, irregular sampling, or aggregation before reset correction. Compare `rate()` with `irate()`, check `resets()`, and confirm Grafana's actual expanded window. Do not interpret `rate(gauge)` as a meaningful counter rate merely because the query parses.

### 5. Design Grafana HA without routine duplicate notifications

Use active-active Grafana behind a load balancer with shared HA PostgreSQL/MySQL and consistent configuration. Configure alerting Memberlist peers and reachable advertise addresses, or a supported Redis coordination setup. Preserve stable alert labels and use one intended rule ownership path. Explain replicated evaluation load and optional version-dependent alternatives. Validate process/zone loss, rolling restarts, and network partitions; best-effort deduplication does not promise exactly-once delivery through every failure.

### 6. An alert fires in Prometheus but never reaches Slack

First confirm it is firing rather than pending and identify which Alertmanager receives it. Prometheus rules do not automatically use Grafana's contact points. Check Prometheus alert-delivery errors and Alertmanager's received alert, then walk label routing, silence/inhibition/mute rules, grouping delay, receiver selection, Slack credentials, channel permissions, response codes, and outbound network access. A contact-point test proves that test path at that moment; it does not prove the real alert matched that route.

### 7. Build SLO alerting for 99.95% availability

Define eligible and bad events at a user-relevant measurement point, plus a rolling 30-day window. The budget is 0.05% of eligible events. Use the section 6 fast and slow multi-window expressions, with explicit recording rules and traffic handling. Add quiet-service synthetics, telemetry freshness checks, severity/owner labels, and runbook links. Validate with historical incidents and synthetic failures; explain that a time-based equivalent of 21.6 minutes is not automatically the request budget.

### 8. Reduce millions of series without losing critical visibility

Find the largest metric families and label value counts, then inspect churn and which dimensions support actual incident questions. Replace raw URL paths with route templates, remove request/user IDs from metrics, reconsider bucket counts, and aggregate where instance detail is unnecessary. Move event-specific context into logs/traces. Estimate savings before rollout and compare critical SLI/rule results afterward. Do not blindly drop distinguishing labels from existing metrics: multiple streams can collapse into conflicting samples. Prefer producer changes or deliberate aggregation.

### 9. Optimize a dashboard taking 30 seconds to load

Measure slow panels and variable queries with query inspector, then compare query, transfer, transform, and rendering time. Reproduce the expensive query directly with identical range and step. Narrow selectors/ranges, reduce unnecessary repetitions, use appropriate resolution, precompute stable expensive expressions, and limit response cardinality. Inspect backend queues before raising concurrency and consider supported caching after query fixes. Report both load latency and backend work so an apparent frontend improvement does not merely move the bottleneck.

### 10. Design multi-region monitoring resilient to regional outages

Keep regional collectors, critical rules, and buffering independent of a central region. Provide external probes and an out-of-region notification path with independent identity/DNS assumptions. Replicate or separately ship telemetry according to RPO, cost, sovereignty, and supported backend replication semantics. Expose global query gaps and partial responses rather than showing a healthy global total with one region absent. Exercise a complete region loss, including object storage, dashboards, paging, and responder access.

### 11. Grafana differs from direct Prometheus queries

Compare the actual data source URL/tenant, expanded expression, time zone/display boundaries, start/end, step, instant/range mode, rate window, and query limits. Check transformations, unit conversion, stat reduction, and missing-value presentation. Different Prometheus replicas, remote-write lag, deduplication, cache freshness, and downsampled long-term data can also differ. Reproduce Grafana's inspector request directly; comparing similar-looking queries at different timestamps is insufficient evidence of a bug.

### 12. Persistent No Data alerts during rolling deployments

Inspect whether the entire rule is empty or only old pod instances disappeared. Check stable labels, ready replicas, discovery timing, scrape cadence, and whether each new counter has enough samples for the window. Aggregate service alerts before evaluating impact and retain separate rollout/readiness checks. Configure missing-series and No Data handling deliberately, with a bounded grace period backed by a freshness detector. Silencing every deployment or mapping all absence to Normal hides real failures.

### 13. Design GitOps dashboards, data sources, and rules

Use versioned definitions, stable UIDs, environment mappings, secret references, and one owner per resource. Choose file provisioning or Terraform/API workflows per supported resource; reconcile Kubernetes manifests with Argo CD/Flux and observe subsequent Grafana imports. Validate syntax, render output, import against the target release, run representative queries, and exercise notification routing. Define UI-edit and deletion policy, state protection, promotion, and rollback before adopting existing resources.

### 14. Monitor Kafka, PostgreSQL, Redis, Kubernetes, and applications

| System | Signals to connect to user impact |
| --- | --- |
| Kafka | Consumer lag/age and drain rate, partition leadership, under-replicated partitions, ISR changes, broker disk/latency, produce/fetch failures |
| PostgreSQL | Query latency, locks/deadlocks, active/waiting connections, replication lag, WAL/disk growth, checkpoints, vacuum health, transaction failures |
| Redis | Command latency, memory/fragmentation, evictions, rejected connections, blocked clients, replication health, persistence/failover status |
| Kubernetes | Desired/ready capacity, scheduling failures, restarts/OOM, node pressure, control-plane and network/storage health |
| Distributed applications | RED, dependency traces, queue age, business success, SLO burn, profiles for resource-heavy paths |

Choose exporters and native telemetry with least-privilege access and bounded labels. Define SLOs around business behavior, then use infrastructure signals to explain causes. Kafka lag alone is not delay without traffic/processing context; Redis memory utilization alone does not reveal whether eviction is acceptable; PostgreSQL replication lag needs a clear unit and read-freshness requirement.

### 15. Detect blind spots when monitoring itself fails

Have an independent receiver expect a periodic heartbeat from the alerting pipeline, and probe customer paths plus monitoring endpoints from another failure domain. Check expected coverage and sample freshness against inventory, remote-write age, rule scheduling, and delivery success. Periodically send an end-to-end test through the real notification route. An independent heartbeat that depends on the same cluster, DNS, identity, and region is not sufficiently independent. Demonstrate detection by stopping collectors, blocking ingestion, breaking routing, and losing the monitoring cluster in controlled exercises.
