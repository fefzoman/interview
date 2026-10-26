# Data Quality & Observability: Data Engineer Interview Guide

The pipeline is green, but finance says yesterday's revenue is wrong. That is where execution monitoring stops being enough. **Data quality** asks whether the data is fit for its intended use; **data observability** gathers the signals needed to detect a problem, understand its impact, and restore trust. A completed job can publish bad numbers, while a failed job can leave an older, valid result available but stale.

In an interview, connect each check to a business risk, a clearly defined measurement, an owner, and a response. A dashboard full of metrics is useful only if someone can act on it.

**Running example:** An orders pipeline ingests source transactions, builds an `orders` table, and publishes daily revenue. The analytical grain is one row per order; revenue includes only completed orders, grouped by order date and currency. Refunds require an explicitly agreed accounting rule. Examples are teaching snippets, not a complete deployed project. SQL uses broadly supported constructs; adapt date arithmetic and metadata queries to your engine.

## Contents

1. [Data Quality Dimensions](#1-data-quality-dimensions)
2. [Great Expectations, Soda, and dbt Tests](#2-great-expectations-soda-and-dbt-tests)
3. [Data Profiling, Statistical Validation, and Anomaly Detection](#3-data-profiling-statistical-validation-and-anomaly-detection)
4. [Freshness, Volume, Schema, and Distribution Monitoring](#4-freshness-volume-schema-and-distribution-monitoring)
5. [Data Lineage, Impact Analysis, and Root Cause Analysis](#5-data-lineage-impact-analysis-and-root-cause-analysis)
6. [Data Quality SLAs, SLOs, and SLIs](#6-data-quality-slas-slos-and-slis)
7. [Data Reconciliation, Quality Gates, and Quarantine](#7-data-reconciliation-quality-gates-and-quarantine)
8. [OpenLineage and DataHub](#8-openlineage-and-datahub)
9. [Interview Scenarios](#9-interview-scenarios)

## 1. Data Quality Dimensions

### Completeness, accuracy, consistency, and validity

Before adding a check, name the kind of failure you want to catch. A missing order, an incorrect amount, conflicting totals, and an invalid status are all quality problems, but they require different evidence. The dimensions below turn a vague concern about trust into a measurable question.

| Dimension | Question | Orders example | Typical evidence |
| --- | --- | --- | --- |
| Completeness | Is all required data present? | Every expected order arrived; required IDs are populated | Expected-key reconciliation, partition coverage, missing-value rate |
| Accuracy | Does the value represent reality correctly? | Recorded amount matches the authoritative transaction | Reconciliation against an authoritative system or reviewed reference data |
| Consistency | Do related representations agree under the same rules? | Revenue uses the same currency conversion and cancellation rules in finance and analytics | Cross-system comparisons and business invariants |
| Validity | Does data satisfy its allowed format, domain, and rules? | Status is allowed; amount has the expected type and range | Type, domain, range, format, and relationship checks |

A plausible positive amount can be valid and inaccurate: an order really cost 120, but the warehouse stores 12. Two tables can consistently contain the same wrong value. Accuracy needs evidence beyond a syntactically valid field.

**Completeness has multiple levels:**

- **Field:** Required `customer_id` is present on each eligible order.
- **Record:** All orders in an agreed source snapshot reached the destination.
- **Partition:** Every expected hour or business date has arrived.
- **Relationship:** Required parent or child records exist for the business process.

A table with no nulls can still be missing half its records. A row count alone cannot establish completeness when duplicates compensate for missing rows.

### Define the population before the percentage

```text
required-field completeness = populated required fields / eligible required fields
record completeness = distinct expected keys found / distinct expected source keys
validity rate = records satisfying a rule / records eligible for that rule
```

Specify whether empty strings, whitespace, placeholders such as `UNKNOWN`, and nulls count as missing. An optional delivery timestamp should not fail completeness for an order that has not shipped. For zero eligible records, report the rate as not applicable or unknown according to policy; do not silently turn it into 100%.

An exact expected population may be unavailable. Use a source manifest, control total, sequence range, or closed-window source snapshot when possible. A historical row-count estimate measures unusual volume, not proven completeness.

### Related dimensions and business rules

**Uniqueness** checks that a business key occurs at the intended grain. Order IDs can repeat legitimately in a line-item table; uniqueness should then apply to `(order_id, line_id)`. **Timeliness** checks whether usable data arrives by the time consumers need it.

Cross-field rules catch problems that individual-column checks miss: `shipped_at >= ordered_at`, a completed order has a settlement reference, and header totals agree with line totals under the defined rounding policy. Make exceptions explicit rather than applying a universal rule such as “all monetary values must be positive” to refunds.

**Interview check:** Explain how you would prove that a dataset is complete, why validity does not prove accuracy, and who defines the authoritative source when two systems disagree.

## 2. Great Expectations, Soda, and dbt Tests

### What each tool contributes

Once a rule is clear, you need a way to execute it, retain the result, and act on failure. Great Expectations, Soda, and dbt offer overlapping approaches to that work. Choose around the execution environment and ownership of the rule, rather than expecting a tool to supply the business definition.

| Tool | Main approach | Useful context | Operational consideration |
| --- | --- | --- | --- |
| Great Expectations (GX) | Express assertions as Expectations and run validations on defined batches | Python-integrated validation and reusable validation results | Configure data access, execution, result storage, and orchestration |
| Soda | Declare data quality checks in YAML and evaluate them against datasets | Readable checks and contracts shared across teams | Match configuration to the installed Soda generation and connector |
| dbt | Define SQL-based data tests alongside transformation models | Testing warehouse transformations within a dbt project | Schedule tests and freshness checks, retain artifacts, and handle failures |

These tools can overlap. Assign ownership so that one critical rule has one authoritative definition and response policy. Tests do not automatically provide complete lineage, statistically calibrated detection, or an incident process.

### Great Expectations

An **Expectation** is an assertion about data. An **Expectation Suite** groups assertions. A **Batch Definition** describes the data slice to validate; a **Validation Definition** associates that slice with a suite. A **Checkpoint** runs validation definitions and configured actions. **Validation Results** record outcomes, and **Data Docs** present expectations and results for people. [GX workflow](https://docs.greatexpectations.io/docs/core/introduction/gx_overview/), [GX glossary](https://docs.greatexpectations.io/docs/reference/learn/glossary/).

For the orders pipeline, validate each ingestion batch for required IDs, allowed statuses, and amount rules before publication. Keep the batch identifier and ruleset version with the results so a later investigation can reproduce the decision. Protect unexpected-value samples because they can contain customer data.

GX is a validation framework; the surrounding pipeline must interpret results and enforce publication decisions. Check installed GX versions before copying Python examples because older Validator-based tutorials and current workflows use different APIs.

### Soda

Soda supports declarative checks for dataset and column properties. The following illustrates its current **data contract language**, using a configured data source named `warehouse` and a dataset in `analytics.public`. It is distinct from classic SodaCL configurations that begin with `checks for orders:`; use syntax matching your installed release. [Soda contract reference](https://docs.soda.io/reference/contract-language-reference).

```yaml
dataset: warehouse/analytics/public/orders

checks:
  - row_count:
      threshold:
        must_be_greater_than: 0

columns:
  - name: order_id
    checks:
      - missing:
  - name: status
    checks:
      - invalid:
          valid_values: [pending, completed, cancelled]
```

This checks a nonempty table, populated IDs, and allowed nonmissing status values. Add a missing-value check to `status` if it is required. A production hourly feed needs a window-specific volume rule rather than only a whole-table nonempty check. Scheduling and blocking publication remain pipeline responsibilities.

### dbt generic and singular data tests

Use the [dbt testing guide](../pipelines/dbt.md#4-testing--data-quality) for test definitions, YAML and SQL examples, null handling, relationship checks, severity, and unit tests. Keep each rule in one place alongside the model that owns it.

For monitoring, schedule source freshness separately from model builds and tests. A fresh source does not prove that the final mart refreshed. Record the tested batch, ruleset version, and publication decision as part of the quality result.

### Design useful results

Record the dataset, partition or batch, check identifier, check version, observed value, threshold, status, execution time, and owner. Distinguish **passed**, **failed**, and **could not evaluate**. A query timeout or expired credential is not proof that the data passed.

## 3. Data Profiling, Statistical Validation, and Anomaly Detection

### Profiling: discover what is in the data

Before deciding which values are unusual, find out what the dataset actually contains. **Profiling** summarizes the data to support rule selection and investigation: inspect schema, row counts, null rates, distinct counts, duplicate keys, ranges, quantiles, string lengths, frequent categories, and relationships between columns. The orders query below provides a first look at several of those signals.

```sql
select
    count(*) as row_count,
    count(customer_id) as populated_customer_ids,
    count(distinct order_id) as distinct_order_ids,
    1.0 * sum(case when customer_id is null then 1 else 0 end)
        / nullif(count(*), 0) as missing_customer_rate,
    min(amount) as minimum_amount,
    max(amount) as maximum_amount,
    avg(amount) as average_amount
from orders
where order_date = DATE '2026-10-01';
```

The query returns a null missing rate for an empty partition; pair it with the row count to distinguish no data from no missing values. `count(distinct order_id)` excludes nulls, so this is not a standalone uniqueness test. The average also excludes null amounts.

Profile by meaningful segments: date, region, producer version, customer type, and payment method. A stable global average can hide a broken regional feed. Aggregate changes can also reflect a legitimate change in segment mix rather than a defect within each segment.

### Statistical validation: compare with an explicit expectation

| Method | Useful for | Important limitation |
| --- | --- | --- |
| Fixed business bounds | Impossible amounts, required domains, mandatory keys | Bounds require domain knowledge and maintenance |
| Mean and standard deviation | Fairly stable numerical metrics | Outliers, skew, and seasonality can invalidate simple thresholds |
| Median and median absolute deviation (MAD) | Robust baselines with occasional extreme values | Zero MAD needs a fallback; seasonality still matters |
| Quantile or prediction intervals | Skewed metrics and expected ranges | Historical coverage must match future conditions |
| Two-sample Kolmogorov–Smirnov test | Changes in a continuous numeric distribution | Ties, dependence, and large samples complicate interpretation |
| Chi-squared test | Categorical proportion changes | Needs suitable expected counts and aligned categories |
| Jensen–Shannon divergence | Changes in aligned categorical or binned distributions | Bin definitions and sparse categories affect the result |

A familiar score is `z = (observed - baseline_mean) / baseline_stddev`. A rule such as `abs(z) > 3` is a heuristic, not a universal probability guarantee. It is especially weak for low counts, skewed revenue, or autocorrelated time series. Define a fallback when baseline variance is zero.

A small p-value indicates evidence against the test's null hypothesis under its assumptions; it is not the probability that the pipeline is broken. Combine significance with an operational effect size, such as a material shift in median order value, and the number of affected records.

When testing thousands of columns, false positives accumulate. Use business prioritization, minimum sample sizes, multiple-testing control where appropriate, and incident-level grouping. Compare like-for-like cohorts rather than unrelated populations.

### Anomaly detection: identify unexpected behavior over time

1. Choose the metric and granularity: hourly accepted orders for each region.
2. Build a baseline from comparable historical windows: similar hour of week and business calendar.
3. Account for trend, holidays, promotions, and known upstream releases.
4. Evaluate a residual or prediction interval, with minimum absolute and relative changes.
5. Require persistence where appropriate and group related alerts.
6. Review incidents and false positives to recalibrate thresholds.

Keep hard constraints separate from learned expectations. A campaign can legitimately double volume, but a null primary key remains invalid. Conversely, a perfectly valid dataset with a 40% unexplained volume drop still deserves investigation.

Do not automatically teach an ongoing incident into the baseline. Exclude or label confirmed bad windows, version the detector, and handle new datasets with insufficient history explicitly. Planned backfills need separate treatment from normal arrivals.

### Sampling and approximate metrics

Sampling reduces profiling cost but can miss rare, high-impact defects. If independent uniform samples see a defect rate `p`, the probability of missing every defect in `n` draws is `(1 - p)^n`; clustered defects make this simple model unreliable.

Use full checks for critical keys, control totals, and financially material reconciliation when feasible. Approximate distinct counts and sketches are useful for trends, but their error bounds make them unsuitable as unquestioned proof of exact uniqueness or equality.

**Interview check:** Explain why “use a three-sigma threshold on every column” creates noise, and how you would detect a regional failure hidden by stable total volume.

## 4. Freshness, Volume, Schema, and Distribution Monitoring

### Freshness and end-to-end timeliness

A table updated one minute ago may contain orders that were already hours late when they arrived. To measure timeliness from the consumer's perspective, distinguish when the event happened, when the source committed it, when ingestion received it, and when validated output became readable:

```text
event time:        when the business event occurred
source commit:     when the source recorded the change
ingestion time:    when the platform received it
publication time:  when consumers could read validated output
```

**Ingestion lag** measures arrival delay relative to event or source commit time. **Consumer-visible lag** measures delay until validated data is published. **Freshness age** might be `now - latest qualifying timestamp`, but specify exactly which timestamp and population are used.

`max(event_time)` alone is weak: one recent record can mask a missing hour, and a future-dated event can make data appear fresh. Validate timestamps and combine freshness with partition coverage, volume, and source offsets or watermarks. A watermark is an estimate or guarantee under a particular system's rules, not proof that late data is impossible.

For a daily feed, monitor “the expected business-date partition is complete and validated by 07:00 in the agreed timezone.” A quiet source might legitimately emit no events; distinguish silence from a stalled connector through source heartbeats, offsets, or explicit empty manifests.

### Volume

Measure rows received, accepted, rejected, and published for each logical window. Compare against both historical expectations and source control totals when available. Track distinct business keys as well as physical row counts.

An overall increase can be caused by duplicate replay. A decrease can be caused by missing input, an intentional filter, or a changed business process. Monitor join expansion and filter rejection rates between stages to localize changes.

For streaming feeds, inspect per-partition lag and throughput: healthy aggregate throughput can hide one stalled partition. Separate normal incremental processing from backfills and replay traffic.

### Schema

Monitor column additions and removals, type changes, nullability, nested fields, and changes to required semantics. Compare against an explicit producer-consumer contract, not only yesterday's observed schema.

| Change | Potential effect | Response to consider |
| --- | --- | --- |
| Optional column added | Explicit projections may continue; positional readers or `select *` consumers can break | Check consumer compatibility before accepting |
| Required column removed or renamed | Queries fail or silently lose meaning | Block incompatible publication and coordinate migration |
| Integer widened to a larger integer | Some consumers accept it; strict serializers may reject it | Verify compatibility across the actual path |
| Numeric field becomes a string | Parsing failures, null casts, changed sorting | Reject or quarantine unless explicitly supported |
| Amount changes from dollars to cents | Schema can remain identical while values become wrong | Enforce semantic contracts and reconciliation |

A schema registry or model contract checks only what it expresses and enforces. It does not prove business accuracy or compatibility for every downstream consumer.

### Distribution

Monitor null rates, cardinality, quantiles, category proportions, and relationships such as amount per order or settlement rate. Mean alone misses tail changes and can remain stable when two segments move in opposite directions.

Use stable comparison bins and explicit handling for new categories, nulls, and empty windows. Compare region-specific metrics with region-specific baselines; seasonal mix changes can otherwise look like incidents.

### Operate the monitors

- Check that evaluations actually run: last successful evaluation time, runtime, query errors, and notification delivery.
- Route critical failures to a named owner with a runbook and affected downstream assets.
- Include the observed value, baseline, threshold, window, first occurrence, and investigation link.
- Group cascading failures around the earliest relevant upstream incident.
- Use partition pruning, cheap metadata where reliable, and targeted deep scans to control cost.

Measure detection delay and false-alert burden. Repeated unactionable alerts train people to ignore the system. A missing monitor result should remain visible as unknown, with escalation based on the dataset's criticality.

## 5. Data Lineage, Impact Analysis, and Root Cause Analysis

### Use lineage during an incident

When a revenue total changes unexpectedly, lineage helps identify the inputs and transformations that could have contributed. Use the dependency graph together with run IDs, partitions, timestamps, and dataset versions to locate the affected execution. The [governance guide](governance.md#1-data-catalogs-metadata-management-and-lineage) covers lineage levels, design versus runtime lineage, and the coverage gaps to keep in mind.

Keep data dependencies distinct from scheduling dependencies. A task can depend on another task for control flow without consuming its data. Conversely, a shared lookup table can affect a result without an obvious scheduler dependency.

### Impact analysis: look downstream

Before removing `orders.currency`, find downstream models, reports, exports, and other consumers that use it. Identify owners and business deadlines, then coordinate a migration or versioned contract. Table-level dependencies can overestimate affected consumers; column-level detail can narrow the scope if it is trustworthy.

During an incident, downstream traversal identifies what may be contaminated and what should be blocked, marked stale, or rebuilt. A dataset being reachable in the graph does not establish that every partition or historical result was affected.

### Root cause analysis: trace upstream with evidence

For a 40% drop in reported revenue:

1. Confirm the metric definition, affected time window, currency, and consumer-visible result.
2. Compare revenue, order counts, and amount distributions with the authoritative source for the same window.
3. Trace upstream lineage to find the earliest stage whose output diverges while its inputs remain correct.
4. Compare run IDs, input snapshots, code and configuration changes, rejection rates, and execution logs.
5. Check causes such as missing partitions, CDC lag, join drops, duplicate dimension keys, or an incorrect status filter.
6. Contain publication, repair the cause, replay affected data, and reconcile before restoring consumers.

Correlation is a starting point. A deployment near the incident time is a hypothesis until a changed rule, input, or execution explains the observed differences. Sometimes raw data is correct but a BI filter or cached result is wrong.

Preserve evidence before reruns overwrite staging data or logs. Record the incident timeline, impact, cause, recovery, and one targeted prevention improvement. Lineage accelerates investigation; it does not infer causality by itself.

**Interview check:** Describe how you would distinguish an upstream delivery problem from a transformation bug when both produce the same downstream volume alert.

## 6. Data Quality SLAs, SLOs, and SLIs

### Measurement, objective, and agreement

A promise such as 'orders arrive quickly' leaves both the measurement and the response to failure unclear. First define what users experience, then the target, then any agreement about consequences. SLIs, SLOs, and SLAs describe those separate parts.

| Term | Meaning | Example |
| --- | --- | --- |
| SLI: service-level indicator | A precisely defined measurement of user-relevant behavior | Fraction of eligible order changes visible in validated output within 15 minutes of source commit |
| SLO: service-level objective | A target for an SLI over a defined window | At least 99.5% of eligible changes meet that latency over a rolling 30 days |
| SLA: service-level agreement | An agreement with consumers specifying commitments and consequences | A contracted daily delivery deadline, incident communication, and agreed remedies |

An SLA can be internal or external; its exact obligations come from the agreement. An internal SLO can be stricter than an SLA to create room for recovery. “The pipeline usually finishes quickly” is neither a measurable indicator nor an objective.

### Define a defensible SLI

```text
latency SLI = eligible changes published within 15 minutes / eligible source changes
validity SLI = published eligible records meeting required rules / published eligible records
delivery SLI = deliveries meeting deadline and required checks / scheduled eligible deliveries
```

For the latency SLI, use a source-based denominator and count missing changes as failures once their 15-minute deadline passes. Measuring only changes that arrived hides lost records. Allow measurements to mature until the deadline and define how source manifests or offsets establish the population.

Specify the dataset and grain, source of truth, evaluation window, timezone, late-arrival policy, required checks, exclusions, and handling of unavailable measurements. Keep exclusions narrow and agreed in advance. Report observability outages rather than silently removing them from the denominator.

A validity SLI over published records describes what consumers received; it does not prove that expected records were delivered. Pair it with completeness and freshness indicators. Keeping the last valid snapshot can protect validity while freshness worsens.

### Error budgets

For a 99.5% objective, the tolerated bad fraction is 0.5%. With 2,000,000 eligible source changes, the budget is 10,000 changes that miss the latency objective. This event budget is different from a time-based availability budget or a budget of failed daily deliveries.

```text
allowed bad fraction = 1 - SLO target
burn rate = observed bad fraction / allowed bad fraction
```

If the bad fraction is 2% against a 0.5% allowance, the burn rate is `4×`. Use both short and long windows to detect rapid degradation and sustained problems, accounting for sparse traffic. A single delayed daily batch may have a large business impact despite a small incident duration.

### Choose objectives with consumers

Set objectives from decision deadlines and the cost of incorrect data. Financial close may require exact reconciliation before publication; an exploratory dashboard may tolerate a clearly labeled delay. Split objectives by critical dataset or consumer tier rather than hiding failures in a platform-wide average.

Assign a producer owner, a consumer contact, escalation, a recovery procedure, and a way to communicate status. Revisit objectives when source behavior or business needs change. An aspirational target without measurement or response ownership is not an operating agreement.

**Interview check:** Explain why “99.9% of tests pass” can be a poor SLI: adding many trivial checks can improve it while the one critical reconciliation still fails.

## 7. Data Reconciliation, Quality Gates, and Quarantine

### Reconciliation compares equivalent populations

A source and a warehouse can disagree simply because you compared different currencies or different points in time. **Reconciliation** checks agreement across systems or stages under an explicit mapping. Before counting or summing, align the grain, business filters, currency, timezone, cutoff, and snapshot or CDC position. Otherwise the comparison cannot tell you whether data is missing.

| Level | What to compare | What it misses |
| --- | --- | --- |
| Counts | Records per batch, partition, or business segment | Missing rows offset by duplicates or extra rows |
| Control totals | Revenue or quantity by date and currency | Offsetting value errors |
| Keys | Missing, extra, and duplicate business keys | Incorrect non-key values |
| Row values | Selected fields for aligned business keys | Uncompared fields and undefined equivalence rules |
| Hashes | Canonicalized row or partition digests | Hash collisions and canonicalization errors |

For changing sources, compare a consistent snapshot or an agreed CDC high-water mark. Comparing a live source at 10:00 with a target loaded through 09:55 reports expected lag as a discrepancy.

For CDC, distinguish **events processed** from **current rows**. Multiple updates and deletes can produce many events but one or zero final rows. Check offsets or sequence coverage and final state according to the source's semantics.

### Example: daily control totals

Assume `source_orders` and `orders` have already passed key, required-date, required-currency, and required-amount checks. They represent the same settled snapshot and use the same amount semantics. This query returns mismatched date-and-currency groups:

```sql
with source_totals as (
    select
        order_date,
        currency,
        count(*) as order_count,
        sum(amount) as revenue
    from source_orders
    where status = 'completed'
    group by order_date, currency
), target_totals as (
    select
        order_date,
        currency,
        count(*) as order_count,
        sum(amount) as revenue
    from orders
    where status = 'completed'
    group by order_date, currency
)
select
    coalesce(s.order_date, t.order_date) as order_date,
    coalesce(s.currency, t.currency) as currency,
    s.order_count as source_count,
    t.order_count as target_count,
    s.revenue as source_revenue,
    t.revenue as target_revenue
from source_totals s
full outer join target_totals t
    on s.order_date = t.order_date
    and s.currency = t.currency
where s.order_count is null
   or t.order_count is null
   or s.order_count <> t.order_count
   or s.revenue <> t.revenue;
```

The full outer join catches groups present on only one side. Use exact decimal or integer minor-unit arithmetic for money. If transformation rules legitimately introduce rounding, define the tolerance from those rules rather than choosing one to make the check pass. Matching totals still require key and row-level checks when material offsetting errors are possible.

For hash comparison, fix column order, type representation, timezone, null encoding, delimiters, and row ordering or order-independent aggregation. A simple sum of hashes can hide multiplicity problems; a digest is evidence with assumptions, not mathematical proof of equality.

### Quality gates control publication

A **quality gate** evaluates required results before allowing data to advance. A check that pages someone after publication is a detection control, but it did not prevent that release.

```text
ingest immutable raw input
    -> validate and transform into a candidate dataset
    -> evaluate required checks and reconciliation
    -> atomically publish the validated candidate
    -> monitor the consumer-visible result
```

Choose gate behavior explicitly:

- **Block:** Missing critical source partitions, duplicate primary keys, or failed financial reconciliation prevent publication.
- **Warn:** A noncritical distribution change creates an investigation while publication continues under an agreed policy.
- **Quarantine:** Isolate identifiable invalid records or affected partitions, and release the rest only if consumers accept partial coverage.

Do not quarantine rows for a table-level anomaly when no individual row is proven invalid. A volume drop may require holding the entire partition. A failed evaluation of a mandatory check should prevent publication unless an explicit, auditable override policy applies.

Validation and publication must refer to the same candidate version. Otherwise data can change after checks pass. Use transactions, immutable versions with a pointer switch, or an equivalent platform mechanism. Validate the whole declared scope; checking only new rows does not prove that an incremental merge preserved all historical invariants.

### Quarantine preserves recoverability

Retain the original payload or a secure reference, source identity and offset, batch ID, event and ingestion timestamps, violated rule, rule version, and replay state. Restrict access and retention because rejected records can contain the same sensitive data as accepted records.

Track counts and business value in quarantine. A feed can look perfectly valid after silently discarding every problematic customer. Include quarantined records in completeness and delivery accounting where the contract requires them.

Fix the producer, transformation, or rule, then replay with stable identities and idempotent writes. Record successful replay before marking records resolved. Keep failed records recoverable, avoid unbounded retry loops, and re-evaluate downstream reconciliation after repair.

**Interview check:** Explain how to prevent a rerun from double-counting revenue, and how consumers discover that a published partition excludes quarantined records.

## 8. OpenLineage and DataHub

### OpenLineage: a standard for emitting lineage metadata

To investigate a failed revenue publication, you need to connect the logical transformation, the particular execution, and its inputs and outputs. OpenLineage defines an event model around those identities: a **Job** is the logical process, a **Run** is one execution, and **Datasets** are its inputs and outputs. Jobs and datasets have names with namespaces; each run has its own identifier. [OpenLineage object model](https://openlineage.io/docs/spec/object-model/).

For example, an orders transformation emits events associating one run of `build_daily_revenue` with its input `orders` and output `daily_revenue`. Run lifecycle events such as start, completion, and failure describe execution state. A stable job identity lets repeated runs be associated with the same process, while distinct run identities distinguish attempts. [OpenLineage run cycle](https://openlineage.io/docs/spec/run-cycle/).

**Facets** attach structured metadata to jobs, runs, and datasets, including schemas, code references, and supported quality information. Integration support determines what is emitted; the existence of a facet in the specification does not mean every producer populates it. [OpenLineage facets](https://openlineage.io/docs/spec/facets/).

OpenLineage standardizes metadata exchange. It is not itself a warehouse, a quality gate, or a guarantee of correct business data. You need a backend to retain and query emitted metadata.

### DataHub: a metadata platform and catalog

DataHub organizes metadata about datasets and related assets so people can discover data and understand ownership and dependencies. Its lineage capabilities include upstream and downstream exploration, with column detail where available. Impact analysis helps identify consumers of a changed or failing asset. [DataHub lineage](https://docs.datahub.com/docs/features/feature-guides/lineage), [impact analysis](https://docs.datahub.com/docs/act-on-metadata/impact-analysis).

Metadata comes from configured integrations, APIs, or manual curation; a catalog cannot display dependencies it never received. Its usefulness depends on ingestion coverage and the freshness of ownership and lineage records.

### How they fit together

| Concern | OpenLineage | DataHub |
| --- | --- | --- |
| Primary role | Standard event model and instrumentation ecosystem | Metadata storage, discovery, and exploration platform |
| Main artifact | Events describing jobs, runs, datasets, and facets | Connected metadata entities and relationships |
| Use in an incident | Identify execution inputs, outputs, and context when emitted | Explore dependencies and find affected assets and owners |
| Key limitation | Collection quality depends on producer support | The graph depends on ingested metadata and identity matching |

DataHub can receive OpenLineage events through its documented integration. Align dataset identities across emitters and catalog ingestion so the same physical table does not appear as disconnected assets. Confirm supported event and facet mappings for your deployed versions. [DataHub OpenLineage integration](https://docs.datahub.com/docs/lineage/openlineage/).

For the running example, instrument transformation runs, send lineage metadata to the configured backend, and associate validation results with the same dataset and execution identifiers. During a revenue incident, the graph helps locate upstream candidates and downstream consumers; reconciliation and execution evidence establish what actually failed.

Monitor metadata delivery too. Missing events, inconsistent namespaces, and stale edges can produce a convincing but incomplete graph. Avoid embedding credentials, unrestricted row samples, or unnecessary personal data in metadata events.

## 9. Interview Scenarios

### “The pipeline is green, but the dashboard is wrong. What do you check?”

Begin with the number the consumer is questioning: its definition, filters, timezone, and freshness. Compare it with the authoritative source at the same cutoff, then follow lineage through the stages. Look for missing inputs, join fan-out, filter losses, duplicate replay, currency semantics, and stale BI caches. A green job tells you execution completed; the comparison tells you whether it produced the intended result.

### “Yesterday's order count dropped 35%. Would you block publication?”

Check source counts and manifests, seasonality, source lag, affected segments, and recent changes. If expected orders are missing and the dataset drives a critical decision, hold the affected candidate. If the source confirms a legitimate reduction, publish with evidence. An anomaly alone is not proof of invalid rows.

### “All rows pass validation. How could the data still be bad?”

Entire records or partitions may be missing; valid values may be inaccurate; records may be duplicated at the business grain; a join may multiply amounts; or the data may be stale. Add population reconciliation, semantic invariants, uniqueness, and end-to-end timeliness measurements.

### “A schema change broke ten reports. How do you recover?”

Use downstream impact analysis to identify owners and affected consumers. Stop the incompatible release, restore a compatible dataset version if available, and coordinate a corrected or versioned contract. Replay affected windows and validate results. Prevent recurrence with consumer compatibility checks and a publication gate.

### “How would you design quality checks for a new orders pipeline?”

Define grain, consumers, source truth, business rules, and delivery deadline first. Add required IDs, key uniqueness, allowed statuses, timestamp rules, and source reconciliation. Monitor freshness, volume, schema, and segmented distributions. Assign owners and choose which failures block, warn, or quarantine. Retain raw input and make replay idempotent.

### “Why are we receiving hundreds of quality alerts?”

Look for one upstream incident causing many downstream failures, unmodeled seasonality, weak sample sizes, and thresholds with no operational effect size. Group related failures, distinguish hard constraints from anomaly warnings, and route actionable incidents. Measure false alerts and detection delay before changing thresholds.

### “What is your first response to failed reconciliation?”

Verify the comparison uses the same snapshot, grain, cutoff, filters, and monetary rules. Identify missing and extra keys, duplicates, and value differences by partition. Preserve evidence, hold affected publication where required, repair the cause, and replay safely. Never adjust the expected total merely to clear the alert.

### “How would you explain the whole approach in one minute?”

“I define trustworthy data in terms of the consumer's decision and the dataset's grain. I validate critical rules and reconcile with the source before publication, then monitor freshness, volume, schema, and distributions. Every important signal has an owner and response policy. Lineage helps identify impact and trace upstream; reproducible checks and preserved raw data let us repair and replay safely. SLIs and SLOs measure whether consumers actually receive usable data on time.”
