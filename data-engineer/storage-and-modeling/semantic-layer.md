# Semantic Layer & Metrics: Data Engineer Interview Guide

Two dashboards can both report revenue and disagree because one includes refunds, uses a different date, or multiplies rows in a join. A semantic layer makes those decisions explicit by translating physical data into business concepts and reusable metric definitions. This guide follows a metric from its business meaning to the joins, aggregation rules, and consumer interfaces that keep its answer consistent.

**Scope:** Platform-neutral fundamentals with dbt Semantic Layer and MetricFlow examples. Product documentation was checked on October 25, 2026. The YAML uses the newer specification supported by dbt v1.12 and dbt v2; older projects use a different structure. Examples are teaching snippets, not a complete runnable dbt project. Hosted features depend on the plan, engine, adapter, and integration. [Specification compatibility](https://docs.getdbt.com/docs/build/latest-metrics-spec).

**Running example:** An online store has an order fact, order-line fact, payment fact, customer dimension, and calendar. The reporting order fact has one row per eligible paid order, including orders subsequently refunded, and excludes canceled/unpaid orders. Its `net_sales_amount` is merchandise sales after discounts and refunds, excluding tax and shipping, converted into one reporting currency. Refunds restate the original order date in this example; refund-date reporting would require a different model and metric.

## Contents

1. [Semantic Models, Entities, Measures & Dimensions](#1-semantic-models-entities-measures--dimensions)
2. [Metric Definitions & Business Logic Centralization](#2-metric-definitions--business-logic-centralization)
3. [dbt Semantic Layer & MetricFlow](#3-dbt-semantic-layer--metricflow)
4. [Aggregation, Time Dimensions & Granularity](#4-aggregation-time-dimensions--granularity)
5. [Conformed Dimensions & Join Paths](#5-conformed-dimensions--join-paths)
6. [Derived & Cumulative Metrics](#6-derived--cumulative-metrics)
7. [Consistency, Governance & Versioning](#7-consistency-governance--versioning)
8. [BI Integration, Query Optimization & Caching](#8-bi-integration-query-optimization--caching)
9. [Interview Practice](#9-interview-practice)

## 1. Semantic Models, Entities, Measures & Dimensions

### What the layer does

Cleaning an amount column does not yet define revenue. Once transformations produce dependable facts and dimensions, a semantic layer describes what those datasets mean and which queries are valid. Follow that separation through to the dashboard in the flow below.

```text
Operational sources
        |
        v
Transformations -> tested facts and dimensions
        |
        v
Semantic definitions -> metric query planning -> warehouse SQL
        |
        v
Dashboards, spreadsheets, notebooks, applications
```

The transformation layer cleans and reshapes data. The semantic layer describes business meaning and legal ways to query that data. It can generate SQL dynamically or serve precomputed results; it does not inherently require copying the entire warehouse.

A catalog describes what data exists. A semantic layer additionally provides executable rules for calculating and grouping business values. A BI tool can contain its own semantic model, but centralizing definitions outside one dashboard makes reuse across tools easier.

### Core concepts

| Concept | Meaning | Store example |
| --- | --- | --- |
| Physical model | A table, view, or transformation output | `fct_orders` |
| Semantic model | Metadata describing a dataset's entities, dimensions, and aggregation behavior | Orders at one row per order |
| Entity | A business object represented by an identifier; also connects models | Order, customer, product |
| Measure | An aggregatable quantity and its aggregation rule | Sum of `net_sales_amount` |
| Dimension | An attribute used to filter, group, or label results | Customer country, order channel, order date |
| Metric | A named business calculation with defined population and semantics | Net sales, average order value |
| Grain | What one row or one result represents | One order; one month and country |

**Measure versus metric:** A measure specifies an aggregation building block; a metric establishes its business meaning and use. A simple metric can expose one measure directly. The vocabulary varies by tool: newer dbt YAML expresses these building blocks as simple metrics rather than separate measures. [Measures and the newer specification](https://docs.getdbt.com/docs/build/measures).

### Entities and identity

`order_id` uniquely identifies rows in the order fact. `customer_id` repeats because a customer can place multiple orders. These roles define a many-orders-to-one-customer relationship.

An identifier's meaning includes its namespace. Two source systems can both have customer `123` without referring to the same customer. Resolve identity upstream or use an appropriate composite/surrogate key before declaring a shared entity.

In MetricFlow, entity types include `primary`, `unique`, `foreign`, and `natural`. A primary entity identifies a model's rows; a foreign entity can repeat. Natural entities can support historical relationships such as slowly changing dimensions. Entity declarations guide planning; uniqueness and temporal validity still need data tests. [Entities](https://docs.getdbt.com/docs/build/entities).

### Dimensions and grain

Categorical dimensions include country, channel, and customer segment. Time dimensions include order time, payment time, and shipment time. Numeric values can also be dimensions: a subscription tier numbered `1`, `2`, or `3` is a category, not necessarily something to sum.

A dimension is only valid for a metric if a meaningful relationship exists. Order-level sales cannot be grouped accurately by product merely by joining order lines: an order can contain several products. Use line-level sales or an explicitly defined allocation.

**Interview point:** Declare the fact grain before describing keys, measures, or joins. Correct-looking SQL over the wrong grain still produces the wrong metric.

## 2. Metric Definitions & Business Logic Centralization

### A metric is a contract

Ask two teams to implement `SUM(amount)` and they can still produce different revenue numbers. Which orders qualify? When do refunds count? Which currency and date should apply? A metric definition becomes a useful contract when it answers those questions explicitly:

| Contract field | Example |
| --- | --- |
| Stable name and description | `net_sales`: eligible order merchandise value after discounts/refunds |
| Business owner | Finance approves the meaning; analytics engineering maintains implementation |
| Input grain and population | One eligible paid order; canceled/unpaid orders excluded |
| Expression and aggregation | `SUM(net_sales_amount)` |
| Time basis | Original order date in the agreed business timezone |
| Units and currency | Reporting-currency amount; documented FX source and conversion date |
| Dimensions | Order channel; customer country with documented historical/current meaning |
| Null and empty-set behavior | Missing amount is a quality failure; confirmed no orders can yield zero |
| Freshness and completeness | Updated hourly; current day is provisional |
| Corrections | Refunds revise the original period; closed-period restatement policy documented |
| Compatibility | Definition version, effective date, affected consumers |

This example is an order-sales metric. Accounting revenue recognition might follow delivery or service periods and should have its own definition.

### Where business logic belongs

Put reusable cleaning, deduplication, currency conversion, and complex row-level business transformations in tested upstream models. Put metric aggregation, supported dimensions, metric filters, and combinations of metrics in the semantic definitions where the engine supports them.

For example, compute each order's reporting-currency amount once upstream. Define net sales centrally as its sum. Dashboards request that metric by month and country rather than maintaining their own refund rules and FX joins.

Some rules are global data contracts; others belong to one metric. Removing canceled orders from a shared source used for cancellation analysis would destroy necessary information. Our running example uses a dedicated eligible-order fact while retaining complete order history upstream.

### Centralization and its limits

Centralization gives consumers one reviewed implementation, shared documentation, dependency tracking, and a migration path when meaning changes. It does not force every dashboard to use that implementation or make incorrect source data correct.

Several metrics can be legitimate: gross bookings, net sales, cash collected, and recognized revenue answer different questions. Give them distinct names and owners. The goal is one agreed definition per named concept, not one number for every business purpose.

**Common failure:** Two dashboards both display “revenue,” but one filters by payment date and the other by order date. Compare their contracts before comparing their SQL.

## 3. dbt Semantic Layer & MetricFlow

### Responsibilities

A metric request crosses several responsibilities before returning a number. Transformations prepare the rows, definitions describe their meaning, MetricFlow plans the metric SQL, and the warehouse executes it. Separating these roles helps you investigate whether a wrong answer came from the input model, semantic rules, generated query, or consumer request.

| Component | Responsibility |
| --- | --- |
| dbt transformations | Build and test the physical datasets |
| Semantic definitions | Describe entities, dimensions, and metrics |
| MetricFlow | Interpret the semantic graph and construct metric SQL |
| dbt Semantic Layer service | Expose deployed definitions through supported query interfaces and integrations |
| Warehouse | Execute generated SQL and return results |
| Consumer | Request metrics, grouping, filters, and presentation |

MetricFlow uses relationships and aggregation metadata to plan queries. It can combine metric calculations and find valid dimensions across models. It is not a replacement for the warehouse or an upstream deduplication pipeline. [About MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow).

### Newer versus legacy YAML

| Concern | Legacy structure | Newer structure |
| --- | --- | --- |
| Semantic model | Top-level `semantic_models:` | `semantic_model:` inside a `models:` entry |
| Entities/dimensions | Semantic-model lists | Annotations under `columns:` |
| Aggregation building block | `measures:` | Model-level `type: simple` metrics |
| Metric parameters | Nested `type_params` | Direct metric keys |
| Derived inputs | `type_params.metrics` | `input_metrics` |

Use the specification supported by the project's runtime. Cross-model metric definitions go in a top-level `metrics:` block in the newer specification. Do not combine both structures into one unlabelled example. [Migration reference](https://docs.getdbt.com/docs/build/latest-metrics-spec).

### Minimal example using the newer specification

Assume `fct_orders` already implements the eligibility, currency, and refund rules described above. `order_date` is a business-calendar date, and `order_channel` is a physical column.

```yaml
models:
  - name: fct_orders
    semantic_model:
      enabled: true
      name: orders
    agg_time_dimension: order_date
    columns:
      - name: order_id
        entity:
          name: order
          type: primary
      - name: customer_id
        entity:
          name: customer
          type: foreign
      - name: order_date
        granularity: day
        dimension:
          type: time
      - name: order_channel
        dimension:
          type: categorical
    metrics:
      - name: net_sales
        label: Net sales
        type: simple
        agg: sum
        expr: net_sales_amount
      - name: eligible_orders
        label: Eligible orders
        type: simple
        agg: sum
        expr: 1
      - name: average_order_value
        label: Average order value
        type: ratio
        numerator: net_sales
        denominator: eligible_orders
```

The primary entity is an order, so a local dimension can be referenced as `order__order_channel`. A separate customer model with a primary customer entity can expose `customer__country` through a valid join. [Semantic models](https://docs.getdbt.com/docs/build/semantic-models).

Simple metrics declare the aggregation and expression. The ratio references metrics, allowing its numerator and denominator to be computed at the requested grouping. [Simple metrics](https://docs.getdbt.com/docs/build/simple), [ratio metrics](https://docs.getdbt.com/docs/build/ratio).

### Query lifecycle

For “monthly net sales by customer country,” the system resolves the metric and dimensions, identifies the source and join path, applies filters, plans aggregation, generates warehouse SQL, and returns the grouped result.

The normalized `metric_time` query dimension represents the chosen aggregation time for a metric. Combining metrics by this dimension aligns their output periods; it does not prove that order time and payment time have the same business meaning.

When debugging, inspect the generated SQL and its warehouse plan. Correct metadata should produce the same answer as a carefully written reference query at the same grain and filter scope.

## 4. Aggregation, Time Dimensions & Granularity

### Additive, semi-additive, and non-additive metrics

Daily sales can often be summed into monthly sales, but daily distinct-customer counts cannot be summed into monthly distinct customers. The same person may appear on several days. Before rolling a metric up, identify which dimensions permit addition and which require a different calculation.

| Type | Safe aggregation | Example | Common error |
| --- | --- | --- | --- |
| Additive | Sum across compatible dimensions and disjoint periods | Sales in one currency | Summing amounts in mixed currencies |
| Semi-additive | Aggregate across some dimensions but use another rule across time | Account balance at a snapshot | Summing daily closing balances into a monthly balance |
| Non-additive | Recompute from underlying data or sufficient intermediate state | Distinct customers, ratios, percentiles | Summing daily distinct users into monthly users |

Additivity depends on the dimension. A customer can appear in several channels, so summing channel-level distinct-customer counts can also overcount.

For a monthly balance, define whether the business needs the last available snapshot, a snapshot exactly at month end, or an average daily balance. These are different calculations. Missing snapshots require an explicit carry-forward or missing-data policy.

### Ratios and averages

Consider two days:

| Day | Sales | Orders | Average order value |
| --- | ---: | ---: | ---: |
| Monday | 100 | 1 | 100 |
| Tuesday | 900 | 90 | 10 |

The combined average order value is `1000 / 91`, about **10.99**. Averaging the two daily averages gives **55**, a different statistic.

```sql
SELECT
    order_channel,
    SUM(net_sales_amount) AS net_sales,
    COUNT(*) AS eligible_orders,
    1.0 * SUM(net_sales_amount) / NULLIF(COUNT(*), 0) AS average_order_value
FROM fct_orders
GROUP BY order_channel;
```

`COUNT(*)` works because the input has one row per eligible order. On an order-line fact it counts lines. Casting or multiplication avoids integer division where necessary; production money calculations should use appropriate fixed-precision decimal types.

For conversion rate, establish the counting unit: converted users divided by eligible users, converted sessions divided by eligible sessions, or orders divided by sessions. Event ordering and an attribution window may require matching events before aggregation. Merely dividing purchases this month by signups this month does not establish cohort conversion.

### Distinct counts and sufficient state

If Monday's users are `{A, B}` and Tuesday's are `{B, C}`, daily distinct counts sum to four, but the two-day distinct count is three.

To support correct rollups, retain raw identities, exact sets where practical, or mergeable approximate sketches with an accepted error bound. A daily scalar distinct count loses the overlap information. Similarly, retain sum and count for recomputing averages; daily medians alone cannot recover the overall median.

### Three different grains

1. **Storage grain:** one order, order line, event, or account snapshot.
2. **Time grain:** hour, day, week, month, or fiscal period.
3. **Result grain:** the complete grouping, such as month × country × channel.

You can usually roll daily data into calendar months if you retain sufficient aggregation state. You cannot recover hourly behavior from daily totals. A monthly aggregate cannot answer a partial-month request without finer-grained data.

Changing result grain can change a metric's interpretation. Average daily active users and monthly distinct active users are both useful, but they are not interchangeable.

### Time dimensions and boundaries

Choose which event anchors the metric: order creation, payment capture, fulfillment, or refund. Normalize timestamps to the required business timezone before deriving business dates. Preserve original event timestamps upstream.

Define week start, fiscal calendar, daylight-saving behavior, and whether a “day” means a calendar day or a fixed duration. Calendar days can contain 23 or 25 hours. A prior-year comparison also needs a leap-day and fiscal-period alignment policy.

Use half-open timestamp intervals: `event_ts >= start_ts AND event_ts < end_ts`. This prevents adjacent periods from double-counting their shared boundary and avoids guessing the last timestamp of a day.

Separate **event time** from ingestion/update time. Late orders or refunds can revise previously reported periods. An ingestion-time filter is useful for processing changes but usually does not define the business reporting period.

### Missing, zero, and incomplete data

A time spine supplies a complete sequence of dates. Join aggregate results to it when the output must contain dates with no events. MetricFlow uses configured time spines for cumulative calculations and time offsets. [Time spine configuration](https://docs.getdbt.com/docs/build/metricflow-time-spine).

Zero means the measurement is known and no qualifying activity occurred. Null can mean unknown, unavailable, or undefined. A failed pipeline should not silently become zero sales. A ratio with zero denominator is normally undefined; document whether consumers receive null or another explicitly agreed result.

## 5. Conformed Dimensions & Join Paths

### Conformed dimensions

Sales and returns can be compared by customer only if both facts agree on who the customer is and what the attributes mean. A **conformed dimension** supplies consistent identity, attributes, and business meaning across facts. Sharing customer, product, and calendar dimensions gives those analyses a common basis.

Conformance requires more than matching column names. Agree on customer identity, country definitions, unknown-member handling, fiscal periods, timezone, and historical interpretation. `country` might mean billing country in one source and shipping country in another.

A date dimension can play several roles: order date, ship date, and payment date. These are role-playing dimensions. Name the roles so consumers do not accidentally change the metric's time basis.

### Safe paths and fanout

```text
fct_orders                     dim_customers
one row per order   many -> one one row per customer
customer_id                    customer_id
net_sales_amount               country
```

This join preserves order grain if the customer key is genuinely unique. Declaring the relationship does not enforce uniqueness in the stored data.

By contrast, one order worth 100 with two payment rows becomes two rows after a raw order-to-payment join. Summing the order amount returns 200. Joining three lines and two payments for that order creates six rows and can inflate both facts.

Do not fix this with `SUM(DISTINCT amount)`: two different orders can legitimately have the same amount. Repair the relationship or aggregate the relevant facts before joining.

### Combining facts safely

Aggregate sales and payments independently to the agreed result grain, then join the aggregates on every grouping key. This is often called drill-across. For example:

```text
orders   -> aggregate by order_month and customer_country ----+
                                                            +-> align grouped results
payments -> aggregate by payment_month and customer_country --+
```

This compares sales booked in a month with payments captured in that month. It does not calculate payment completion for that month's order cohort. For a cohort question, first link and summarize payment events by order with the required time rules.

Use a full outer join when unmatched groups from either side must survive. Handle null grouping keys consistently; SQL null keys do not match through ordinary equality. Coerce missing values to zero only when absence has the intended business meaning.

MetricFlow plans joins using entity types and restricts fanout/chasm paths. Correct metadata and underlying cardinalities are essential. Its documented multi-hop limits also constrain which dimensions are reachable; a semantic graph does not permit arbitrary traversal. [MetricFlow join behavior](https://docs.getdbt.com/docs/build/join-logic).

### Historical and many-to-many dimensions

For a Type 2 customer dimension, several rows can share a natural customer ID. An equality join on that ID multiplies facts. Use a stored dimension-version key or a valid temporal join:

```sql
SELECT
    c.country,
    SUM(o.net_sales_amount) AS net_sales
FROM fct_orders AS o
JOIN dim_customers_history AS c
    ON o.customer_id = c.customer_id
   AND o.ordered_at >= c.valid_from
   AND (o.ordered_at < c.valid_to OR c.valid_to IS NULL)
GROUP BY c.country;
```

This requires non-overlapping customer validity intervals and a defined policy for unmatched facts. The inner join intentionally excludes unmatched rows; use a left join and an unknown bucket if totals must retain them. Reporting by current country instead requires a different lookup.

Many-to-many relationships, such as orders attributed to multiple campaigns, need a bridge and allocation policy or a clearly labelled non-additive result. If an order's sales are allocated across campaigns, its weights should sum to one. Some analyses intentionally give each campaign full credit, in which case campaign totals cannot be summed into overall sales.

**Interview point:** A semantic engine can reject unsafe declared joins. It cannot infer an allocation rule or repair false uniqueness claims.

## 6. Derived & Cumulative Metrics

### Derived metrics

Average order value sounds like a simple division, but the sales total and order count must describe compatible populations and periods. A **derived metric** applies an expression to other metrics, usually after each input has been aggregated to a compatible result grain. The formula is only as sound as that agreement.

| Metric | Formula | Required agreement |
| --- | --- | --- |
| Gross profit | Net sales − cost of goods sold | Same currency, scope, and cost attribution |
| Gross margin | Gross profit / net sales | Zero/negative-sales behavior |
| Average order value | Net sales / eligible orders | Same eligible order population |
| Year-over-year growth | (Current − prior year) / prior year | Comparable periods, calendar, and completeness |

Computing row-level margins and averaging them generally differs from dividing total profit by total sales. Apply arithmetic at the level the metric contract requires.

If `cost_of_goods_sold` is another simple metric on the same order model, append this entry to its existing `metrics:` list:

```yaml
- name: gross_profit
  label: Gross profit
  type: derived
  expr: net_sales - cost_of_goods_sold
  input_metrics:
    - name: net_sales
    - name: cost_of_goods_sold
```

MetricFlow supports input aliases, filters, and time offsets for derived metrics. Use offsets for a defined comparison period, and validate the calendar alignment and missing prior-period behavior. [Derived metric configuration](https://docs.getdbt.com/docs/build/derived).

### Cumulative, rolling, and period-to-date

| Calculation | Meaning | Boundary to define |
| --- | --- | --- |
| Running total | Accumulate from a fixed beginning through the current point | Start of history and restatements |
| Month-to-date | Accumulate from the start of the current month | Calendar and reset point |
| Rolling seven days | Recompute over a moving seven-day window | Whether the current partial day is included |
| Rolling active users | Distinct qualifying users within the moving window | Identity and qualifying event |

A rolling distinct-user metric must deduplicate across the whole window. Summing seven daily distinct-user values double-counts returning users.

For additive daily sales, a teaching SQL pattern is:

```sql
SELECT
    metric_date,
    country,
    daily_sales,
    SUM(daily_sales) OVER (
        PARTITION BY country
        ORDER BY metric_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_sales
FROM daily_sales_dense;
```

`daily_sales_dense` must have exactly one row for every date and reporting country, including zero-activity dates. Its dates must be consecutive. `ROWS` counts rows, so without a dense series this calculates seven available observations rather than seven calendar days.

Build the series across the whole required history first, calculate the window, and only then filter to the displayed dates. A chart beginning October 10 needs input from October 4 to calculate a complete seven-day value for October 10. A new country may require explicit zero rows before its first event if a full window is expected.

### Cumulative metrics in MetricFlow

Append either of these entries to the order model's `metrics:` list; configure a time spine separately:

```yaml
- name: net_sales_mtd
  label: Net sales month to date
  type: cumulative
  input_metric: net_sales
  grain_to_date: month

- name: net_sales_rolling_7_days
  label: Net sales rolling seven days
  type: cumulative
  input_metric: net_sales
  window: 7 days
```

`window` and `grain_to_date` are alternatives. When requesting a coarser output grain, `period_agg` controls selection/aggregation of cumulative values; choose it deliberately rather than summing overlapping cumulative outputs. MetricFlow documents `first`, `last`, and `average`, with `first` as the default. [Cumulative metric configuration](https://docs.getdbt.com/docs/build/cumulative).

A late correction to an additive daily metric can affect subsequent rolling windows that include that date, or the remainder of its month for MTD. An all-history running total can change every subsequent point. Backfills must refresh dependent outputs and caches accordingly.

## 7. Consistency, Governance & Versioning

### Consistency has several layers

Even a shared metric definition does not guarantee that two dashboards agree. They must also use the same source state, filters, time boundaries, permissions, and refresh state. Consistency therefore needs controls across the definition, data, deployment, and consumption path; a matching metric name is only the beginning.

| Layer | Useful control |
| --- | --- |
| Business meaning | Named owner, contract, reviewed definition changes |
| Data correctness | Grain/key tests, reconciliation, freshness and completeness checks |
| Query semantics | Valid grouping paths, reference queries, aggregation tests |
| Deployment | Known definition revision and compatible upstream models |
| Consumption | Certified metrics, documented filters, consistent BI behavior |
| Security | Tested row/column restrictions and effective execution identity |
| Operations | Alerting, audit logs, correction procedures, cache refresh |

Publishing a definition centrally without retiring competing dashboard formulas leaves multiple implementations in service. Track adoption and downstream dependencies.

### What to test

1. **Structural:** Order IDs are non-null/unique; dimension keys and relationship assumptions hold.
2. **Temporal:** SCD intervals do not overlap; required calendar dates exist.
3. **Business:** Eligible populations, refund attribution, FX conversion, and units follow the contract.
4. **Aggregation:** Ratios use compatible inputs; distinct counts and snapshots roll up correctly.
5. **Reference results:** A small known dataset covers repeated customers, refunds, zero denominators, missing dates, and multiple payments.
6. **Reconciliation:** Compare semantic results with trusted source totals and an independent SQL calculation.
7. **Access and freshness:** Users see only authorized rows, including through exports/caches; late corrections become visible within the promised interval.

For additive metrics with complete, disjoint dimension groups, grouping totals should reconcile to the overall total. Do not apply this invariant blindly to distinct counts or full-credit campaign attribution.

Schema validation proves the YAML has the expected structure. A query compiling successfully proves it can be planned. Neither proves the business definition or the data is correct.

### Versioning meaning, not just files

Git records definition changes. A consumer-facing metric version communicates that business meaning or compatibility changed. dbt model versioning and a metric's business version are separate concepts.

Changing a label is usually compatible. Changing refund treatment, time basis, eligible status, currency, or identity resolution can change historical results and must be evaluated as a semantic change even if the output schema stays identical.

A practical breaking-change process:

1. Describe the new meaning, owner approval, effective date, and historical policy.
2. Publish a separate stable name, such as `net_sales_v2`, when consumers need a migration period.
3. Run old and new definitions on the same data and explain differences by period and segment.
4. Update dashboards, exports, alerts, notebooks, and downstream calculations.
5. Rebuild affected aggregates/caches, then deprecate the old definition after migration.

Do not assume a YAML property automatically provides this lifecycle across all tools. Keep the naming and deprecation process explicit.

### Restatements and reproducibility

“What are sales for September now?” and “What did we report at September close?” require different data guarantees. Reproducing the second answer may require a retained data snapshot, historical dimension state, FX rates, metric revision, and the original query/security context.

A Git revision alone cannot reproduce a result if the source tables have changed. Decide whether reports show latest corrected history, frozen closed periods, or both with clear labels.

### Access controls

Document whether queries run as the end user or a shared service identity. Warehouse row-level policies that depend on user identity may behave differently through a shared account.

Enforce authorization at the service/data boundary and protect cached/exported copies. A dashboard filter is not an access control. Apply masking, minimum-group-size policies where required, and auditability according to the data's sensitivity; a metric aggregate can still disclose restricted information.

## 8. BI Integration, Query Optimization & Caching

### Integration patterns

A correct metric still has to reach its users at an acceptable speed and freshness. A live query, a warehouse export, and a BI extract place different responsibilities on the serving path. Compare not only query latency but also who refreshes the copy and enforces its permissions.

| Pattern | Benefits | Tradeoff |
| --- | --- | --- |
| Live metric query | Uses deployed definitions and current accessible data | Query latency, concurrency, and service availability |
| Materialized export | Standard table consumption and predictable serving | Scheduled refresh and another governed copy |
| BI extract/import | Fast local exploration | Separate refresh and security lifecycle |
| Custom API client | Metrics in notebooks or applications | Client authentication, pagination/polling, and error handling |

dbt documents integrations including Tableau and Power BI, plus JDBC, GraphQL, and a Python SDK for custom consumers. Saved-query exports provide a path for tools that read warehouse tables. Confirm each connector's supported capabilities and deployment requirements. [Available integrations](https://docs.getdbt.com/docs/platform-integrations/avail-sl-integrations).

Avoid redefining the same formula in each BI tool. Keep formatting, labels, and visual interaction in the presentation layer while requesting the governed calculation.

Check how the BI tool computes grand totals. For average order value, the total should recompute total sales / total orders, not sum or average the visible category ratios. Top-N displays and hidden filters can also make visible rows differ from an overall total.

### Optimize the actual query

Start with a representative slow request and inspect generated SQL, warehouse execution statistics, and service timing. Separate planning, queueing, execution, result transfer, and rendering latency.

| Observation | Likely action |
| --- | --- |
| Large scan for a small date range | Check predicate pushdown, partition pruning, and timestamp expressions |
| Large intermediate join | Check cardinality, deduplicate upstream, remove unnecessary joins |
| Expensive repeated aggregation | Consider an aggregate table or supported saved-query cache |
| Slow exact distinct counts | Reduce input legitimately or consider approved approximate sketches |
| Too many groups | Reduce unnecessary dimensions or paginate results |
| High concurrency and queueing | Tune workload capacity, scheduling, refreshes, and reuse |
| Fast warehouse query, slow dashboard | Check transfer size, BI calculations, and rendering |

Materialize repeated expensive transformations upstream when justified. Partition or cluster according to the actual filter/join workload. More semantic definitions do not automatically make the physical data faster to scan.

### Aggregate tables and query rewriting

An aggregate table can serve a query only if it preserves the required dimensions, time range, filters, and aggregation state. Monthly sales by country cannot answer sales by channel or an arbitrary ten-day range.

Daily sales plus order count can roll up into monthly average order value. Daily average order value alone cannot. Distinct users need compatible mergeable state or the underlying identities; daily scalar distinct counts are insufficient.

Automatic aggregate selection is a product capability to verify, not an assumed property of every semantic layer. Compare rewritten results with base-table results before relying on the optimization.

### Cache layers and correctness

| Cache | What it reuses | Correctness concern |
| --- | --- | --- |
| Warehouse result cache | A previous SQL result under engine rules | Source changes, execution context, cache eligibility |
| Semantic saved-query cache | Prepared metric results at a defined grouping | Definition/data revision, supported filters, permissions |
| BI/application cache | A retrieved result or extract | Refresh schedule, user scope, downstream calculations |

An application-owned cache should distinguish metric revision, dimensions, filters, time range, timezone/calendar, and effective authorization context wherever those affect results. The exact keying mechanism is implementation-specific.

A TTL bounds age; it does not prove that a cached result matches a new metric definition or permission change. Invalidate or rebuild affected results after source corrections, definition changes, access changes, and backfills. Track source freshness and cache age separately.

dbt's documented declarative caching uses saved queries with exports to create reusable warehouse cache tables. Its result caching relies on warehouse behavior. Declarative-cache availability and refresh behavior have product prerequisites. The current documentation states that underlying-table security context is not applied when serving cached data; cached copies need explicitly reviewed access controls. [dbt caching and access-control limitations](https://docs.getdbt.com/docs/use-dbt-semantic-layer/sl-cache).

Measure cold/warm latency, cache hit rate, bytes scanned, warehouse cost, error rate, and the age of the served data. A fast stale or unauthorized answer is a failed optimization.

## 9. Interview Practice

### A structure for design answers

For practice, take one disputed revenue number and design the agreement that would resolve it. Name the owner, eligible rows, time basis, and legal aggregations before discussing tools. Then follow the metric to a consumer and explain how you would verify the result.

1. Clarify the business question and name the metric owner.
2. State the source grain, identity, population, units, and time basis.
3. Define aggregation and valid dimensions, including historical meaning.
4. Describe join paths and prove cardinality assumptions.
5. Explain handling of nulls, late data, corrections, and versions.
6. Show how consumers query it and how permissions are enforced.
7. Validate against known results, then discuss latency and cost.

### Questions to rehearse

| Question | What a strong answer includes |
| --- | --- |
| What is the difference between a measure and a metric? | Aggregation building block versus a business calculation; terminology varies by tool |
| Why do two dashboards disagree on revenue? | Compare definitions, populations, dates, timezone, source state, permissions, and caches |
| Can monthly active users be calculated by summing DAU? | No; users overlap across days, so recompute distinct identities or merge valid state |
| Why did sales double after adding payments? | One-to-many join duplicated order amounts; summarize facts at compatible grain |
| How would you calculate monthly average order value? | Monthly eligible sales divided by monthly eligible orders; no averaging daily ratios |
| How would you report customer country historically? | Event-time dimension version or valid-time join, tested for overlapping intervals |
| How do you support month-to-date sales? | Chosen calendar/timezone, time spine, monthly reset, missing dates, and corrections |
| What is MetricFlow responsible for? | Planning metric SQL from semantic metadata and valid relationships |
| What changes between older and newer dbt metric YAML? | Separate measures/resources versus model annotations and simple metrics |
| How do you introduce a new revenue definition? | Owner review, parallel definitions, quantified differences, consumer migration, deprecation |
| How do you optimize a slow metric dashboard? | Trace the request, inspect generated SQL, fix scans/joins, then use valid aggregates/caching |
| How do you secure a metric cache? | Verify execution identity, cache isolation and grants, copied policies, invalidation, and access tests |
| How do you reproduce last quarter's reported number? | Retain the definition and data state, historical dimensions, FX rates, and query context |

### Scenarios to work through

**Sales and payment mismatch:** Orders total 100 in September; payment arrives in October. Explain why order-date sales and payment-date cash differ, then describe the model required to ask whether September's orders were eventually paid.

**Rolling-window gap:** Sales exist on October 1, 2, and 10. Explain why a seven-row window over those records is not a seven-day window. Build a complete calendar before calculating the window.

**Changed customer country:** A customer moves in June. Explain how current-country reporting reclassifies earlier orders, while event-time country reporting preserves the earlier classification.

**Refund after month close:** A September order is refunded in October. State whether the metric restates September or records an October adjustment, who approves the rule, and which outputs/caches require repair.

**Restricted cached result:** A manager's metric result is cached, then an analyst requests the same grouping. Explain how authorization context and cached-table policies prevent the analyst from receiving the manager's broader result.

### Common claims to challenge

- “A semantic layer fixes bad source data.” It still depends on correct grain, identity, and trusted inputs.
- “A primary entity declaration guarantees uniqueness.” Test the underlying data.
- “Centralization means every department needs the same revenue metric.” Different business questions need distinct, clearly named definitions.
- “All percentages can be averaged.” The required weighting and denominator determine the calculation.
- “An order can safely join to any dimension.” Reachability, cardinality, history, and allocation rules matter.
- “MTD is the sum of displayed cumulative rows.” Those rows overlap; select the required period state.
- “The metric name stayed the same, so the change is compatible.” Meaning can change without a schema change.
- “A successful query proves correctness.” Compilation and execution do not validate business semantics.
- “Caching preserves source-table permissions automatically.” Copies and execution identities require verification.
