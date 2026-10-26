# dbt: Data Engineer Interview Guide

An orders transformation may begin as one SQL file, then grow into a network of staging models, facts, tests, and reports. dbt makes that network explicit: it builds datasets from transformation code, manages their dependencies, and keeps tests and documentation alongside them. The useful questions remain concrete: what does one output row mean, how is it built, and what happens when a run is repeated?

**Example baseline:** dbt Core v1.10.5+ syntax, with a compatible adapter. This is a documentation baseline, not a claim about the latest release. Unit tests require v1.8+; YAML snapshots and microbatch require v1.9+, with adapter support. Current documentation also covers the Rust-based dbt v2 engine; engine generation, adapter version, and Platform features are separate compatibility questions. Examples use SQL models; date arithmetic marked **Snowflake** must be adapted for other warehouses. These are teaching snippets, not a complete runnable project. [dbt engines and versions](https://docs.getdbt.com/docs/introduction).

## Contents

1. [Architecture & Fundamentals](#1-architecture--fundamentals)
2. [Data Modeling & Materializations](#2-data-modeling--materializations)
3. [Incremental Processing](#3-incremental-processing)
4. [Testing & Data Quality](#4-testing--data-quality)
5. [Jinja, Macros & Packages](#5-jinja-macros--packages)
6. [Snapshots & Historization](#6-snapshots--historization)
7. [DAG Execution & State Management](#7-dag-execution--state-management)
8. [Performance & Optimization](#8-performance--optimization)
9. [Production & Advanced Topics](#9-production--advanced-topics)

## 1. Architecture & Fundamentals

### dbt Core vs. dbt Platform

Choosing dbt also means choosing who operates its development and execution environment. dbt Core provides the transformation framework and CLI; dbt Platform adds managed services around that work. Both normally rely on connected data-platform compute for processing, so distinguish the operating model from where the SQL runs.

| Aspect | dbt Core | dbt Platform |
| --- | --- | --- |
| Role | Open-source transformation framework and CLI | Managed development, deployment, and collaboration services around dbt |
| Running jobs | Your infrastructure and scheduler | Managed jobs, scheduling, and CI capabilities |
| Development | Your editor, CLI, Git workflows | Hosted development tools and integrations, including Studio IDE |
| Operations | You maintain runtime, secrets, artifacts, and monitoring | Managed capabilities vary by plan and environment |
| Data processing | Normally executes transformation SQL on the connected data platform | Also uses connected compute; Platform hosting does not imply storing your warehouse tables |

dbt Core can run production pipelines. Choosing Platform is an operating-model decision: managed services versus infrastructure your team maintains. Do not describe Core and Platform as two SQL dialects. The hosted Studio IDE supports editing, version control, compilation, and execution against your database. [Studio IDE](https://docs.getdbt.com/docs/platform/studio-ide/develop-in-studio), [dbt product overview](https://www.getdbt.com/product/dbt).

### Projects, models, resources, and DAG

A **project** groups transformation code and configuration. A SQL **model** usually contains a `select` statement; its materialization determines how dbt persists the result. **Resources** also include sources, seeds, snapshots, tests, macros, and exposures.

```text
analytics/
  dbt_project.yml
  models/
    staging/
      sources.yml
      stg_orders.sql
    intermediate/
      int_order_payments.sql
    marts/
      fct_orders.sql
      schema.yml
  macros/
  tests/
  seeds/
  snapshots/
  selectors.yml
```

The **DAG**, or directed acyclic graph, expresses resource dependencies. A model can run after its required upstream work succeeds. A cycle such as `orders -> customers -> orders` prevents a valid build order. Folder names organize code; dependencies come from references, not folder position.

### Compilation, execution, ref(), and source()

```yaml
# models/staging/sources.yml
version: 2

sources:
  - name: commerce
    database: raw
    schema: commerce
    tables:
      - name: orders
```

```sql
-- models/staging/stg_orders.sql
select
    id as order_id,
    customer_id,
    amount_cents / 100.0 as amount,
    status,
    updated_at
from {{ source('commerce', 'orders') }}
```

```sql
-- models/marts/fct_orders.sql
select order_id, customer_id, amount, status, updated_at
from {{ ref('stg_orders') }}
```

`source()` resolves a declared external relation and records source lineage; it does not ingest that table. `ref()` resolves a model, seed, or snapshot and records an internal dependency. It handles the physical relation name for the selected environment, including schema and alias configuration. Hardcoded table names bypass this dependency information. [Sources](https://docs.getdbt.com/docs/build/sources), [ref reference](https://docs.getdbt.com/reference/dbt-jinja-functions/ref).

Think of execution in three steps:

1. **Parse:** read project resources, configuration, and dependencies into a manifest.
2. **Compile:** render Jinja and resolve references into SQL.
3. **Execute:** materialization logic issues warehouse DDL/DML and records results.

`dbt compile` does not materialize models, but it can access the warehouse for introspection and `run_query()` calls. In the v1 Jinja lifecycle, `execute` is false during parsing and true during node compilation/execution; it does not mean “this command is building tables.” [execute variable](https://docs.getdbt.com/reference/dbt-jinja-functions/execute).

### Adapters, connections, and environments

An **adapter** supplies platform-specific connection handling, relation operations, SQL generation, and materializations. Examples include `dbt-snowflake`, `dbt-bigquery`, and `dbt-postgres`. Portable Jinja does not make every handwritten SQL expression portable.

For local Core execution, `dbt_project.yml` names a profile, and `profiles.yml` defines connection outputs such as `dev` and `prod`. `--target` chooses an output. Connection fields differ by adapter; hosted Platform environments manage these settings through their own configuration. Keep credentials outside committed project files, using an appropriate secret mechanism. [profiles.yml](https://docs.getdbt.com/docs/local/profiles.yml).

Development should have separate writable schemas and limited permissions. Changing `target.name` alone provides no isolation if both targets resolve to the same relations. Verify database, schema, role, and any custom schema-generation macro.

### SQL, Jinja, and YAML configuration

| Language | Purpose | Example |
| --- | --- | --- |
| SQL | Row transformations, joins, aggregations | `select customer_id, sum(amount) ...` |
| Jinja | Generate SQL and invoke dbt context functions | `{{ ref('stg_orders') }}` |
| YAML | Project settings, resource properties, tests, documentation | `materialized: table` |

```yaml
# dbt_project.yml
name: interview_analytics
version: '1.0.0'
config-version: 2
profile: interview_analytics

models:
  interview_analytics:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

The model-file `config()` block can override inherited settings. Use folder defaults for shared behavior and local overrides for exceptions. A YAML properties file named `schema.yml` can describe many resources; the filename itself does not create a warehouse schema.

**Interview answer:** “dbt parses dependencies, compiles transformation code, and executes it through an adapter. The warehouse normally processes the rows; dbt manages how the transformations are built, tested, and documented.”

## 2. Data Modeling & Materializations

### View, table, incremental, and ephemeral

A model's SQL describes its result, but its **materialization** determines how consumers get that result and when you pay to compute it. A small staging query, a frequently read report, and a large changing fact table can justify different choices. Consider build cost and consumer cost together.

| Materialization | What dbt builds | Good fit | Main cost or limitation |
| --- | --- | --- | --- |
| `view` | A stored query definition | Lightweight staging with current upstream data | Consumers repeatedly pay for the query; nested views can be expensive |
| `table` | A persisted result rebuilt on runs | Reused datasets with affordable rebuilds | Recompute and storage; contents stay as of the last build |
| `incremental` | A table maintained with selected changes | Large datasets with a reliable change-processing rule | Correctness depends on filtering, keys, strategy, and repair procedures |
| `ephemeral` | A CTE incorporated into downstream SQL | Small internal transformations | Repeated computation, larger compiled SQL, no standalone relation |

Ephemeral is code reuse, not a cache. A view is not a persisted query result. dbt also supports adapter-dependent materialized views; those are distinct from ordinary views and from dbt's `table` materialization. [Materializations](https://docs.getdbt.com/docs/build/materializations).

For example, if five expensive marts repeat the same large ephemeral join, persisting that shared result may save more than removing one intermediate write. If a staging model only renames columns, a view may be sufficient. Choose using measured build and consumer costs.

### Custom materializations

A custom materialization defines a new persistence lifecycle using dbt's materialization blocks and adapter operations. It can implement behavior absent from built-ins, but becomes responsible for existing relations, full refresh, hooks, transactions, cleanup, and relation-cache consistency. Returning the built relations is part of that integration.

A macro that generates a `case` expression is not a custom materialization. Prefer built-in configurations when they already implement the required behavior; a custom lifecycle needs adapter-specific validation and upgrade maintenance. [Creating materializations](https://docs.getdbt.com/guides/create-new-materializations).

### Staging, intermediate, and marts

```text
source orders ----> stg_orders ------+
                                    +--> int_order_payments --> fct_orders
source payments --> stg_payments ----+

source customers -> stg_customers ----------------------------> dim_customers
```

| Layer | Responsibility | Typical grain |
| --- | --- | --- |
| Staging | Rename, cast, standardize, apply source-specific cleanup | Usually the source record grain |
| Intermediate | Joins, pivots, deduplication, reusable business steps | Explicit grain required by the next transformation |
| Marts | Publish business entities, facts, dimensions, aggregates | Consumer-facing business grain |

These are conventions, not enforced dbt resource types. Staging usually stays close to one source; intermediate models explain meaningful transformations; marts expose a stable business interface. Avoid copying source cleanup into every mart. [Project layering](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

### Dimensional modeling: facts, dimensions, and grain

Start with **grain**: exactly what does one row represent?

- `fct_order_items`: one row per order line; measures include quantity and line revenue.
- `fct_orders`: one row per order; order-level measures include shipping cost.
- `dim_customers`: one row per customer, or per historical customer version for SCD Type 2.

A **fact** records an event or state with measurements and dimension keys. A **dimension** provides descriptive attributes. A surrogate key identifies a dimensional row independently from a source business key; historical versions need distinct row identifiers.

**Fanout example:** order 10 has three lines and shipping cost 9. Joining its order record to every line and summing shipping produces 27. Aggregate line measures to order grain first, or allocate shipping using an explicit rule. A uniqueness test on `order_id` catches accidental grain changes in an order-level mart.

Measures also have aggregation rules: revenue often sums across dates, an account balance usually does not, and percentages usually need recomputing from their numerator and denominator. A model name beginning with `fct_` proves none of these properties.

### Dependencies and DAG optimization

Use `ref()` for internal dependencies and keep them acyclic. Split a model when the step has a clear grain, reuse value, or operational purpose. Combining trivial layers may simplify the graph; persisting expensive shared work may reduce repeated scans.

Wall-clock time depends on the **critical path**, the longest chain of required work, as well as concurrency and warehouse contention. Ten independent two-minute models can overlap; ten two-minute models in a chain need roughly twenty minutes before overhead. Extra threads cannot remove that chain.

**Interview pitfall:** Choosing incremental for every large-looking table before defining its change semantics. An incorrect fast table is harder to operate than a correct affordable rebuild.

## 3. Incremental Processing

### Strategies: what changes in the target?

Selecting only changed input is half of an incremental design. The other half is deciding what those rows do to the target: append, replace matching keys, or rebuild complete partitions or time batches. Each strategy below needs a different completeness and replay rule.

| Strategy | Basic behavior | Suitable use | Correctness requirement |
| --- | --- | --- | --- |
| `append` | Insert all selected rows | Immutable events with controlled ingestion | Replays must not append duplicate logical records |
| `merge` | Match keys, update matched rows, insert unmatched rows | Mutable records and current-state tables | Deterministic source row per key and correct matching scope |
| `delete+insert` | Remove matching target keys, then insert selected rows | Replacing complete records for selected keys | Each selected key needs its complete replacement data |
| `insert_overwrite` | Replace selected partitions or adapter-defined overwrite scope | Rebuilding affected date partitions | Supply all rows belonging to each replacement scope |
| `microbatch` | Process configured event-time batches | Large time-series datasets | Appropriate event-time boundaries and complete batch recomputation |

Names do not guarantee identical implementation across adapters. Check the adapter/version support matrix, generated SQL, and transaction behavior. Ordinary key-based merge generally implements current state, not automatic SCD Type 2 or automatic hard-delete propagation. [Incremental strategies](https://docs.getdbt.com/docs/build/incremental-strategy).

### is_incremental() and unique_key

`is_incremental()` is true when the model is configured as incremental, its target already exists as a table, and full refresh is not in effect. Both branches must produce valid SQL. `{{ this }}` refers to the model's target relation. [Configuring incremental models](https://docs.getdbt.com/docs/build/incremental-models).

`unique_key` tells a supported strategy how rows match. It does not create a uniqueness assertion, deduplicate the input, or necessarily create a physical constraint. Test it for uniqueness and nulls. For a composite key, supported adapters accept a column list such as `['order_id', 'line_id']`; avoid ambiguous string concatenation.

The following **Snowflake** example maintains one current row per order. Assume `stg_order_changes` retains complete row images, non-null `updated_at`, and an `ingestion_sequence` that breaks ties within each order. This example processes inserts and updates; CDC deletes require a separate rule.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='fail'
) }}

with candidates as (
    select order_id, customer_id, amount, updated_at, ingestion_sequence
    from {{ ref('stg_order_changes') }}
    {% if is_incremental() %}
    where updated_at >= (
        select dateadd(
            day, -3,
            coalesce(max(updated_at), '1900-01-01'::timestamp)
        )
        from {{ this }}
    )
    {% endif %}
),
ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc, ingestion_sequence desc
        ) as row_num
    from candidates
)
select order_id, customer_id, amount, updated_at, ingestion_sequence
from ranked
where row_num = 1
```

Reason through the cases before calling this production-ready:

| Case | Expected behavior or required action |
| --- | --- |
| First run | No target-dependent filter; build from available history |
| Existing empty target | `coalesce` prevents a null watermark from excluding all rows |
| Retry with unchanged input | Re-select and merge the same deterministic winners |
| Two changes with the same timestamp | Sequence determines the winning row |
| Update arriving two days late | Overlap can include it |
| Update older than the overlap | It can be missed; repair or a different change cursor is required |
| Future-dated source timestamp | Can advance the watermark incorrectly; validate timestamp quality |

If older event versions can arrive after newer ones, selecting only a narrow arrival batch and blindly merging can regress target state. Use a reliable change ordering and compare with existing target versions where necessary. An ingestion cursor can discover arrivals more reliably than business event time, but still requires deterministic conflict resolution.

### incremental_predicates and partition pruning

There are two different scan problems:

1. The model's `where` clause limits source rows and transformation work.
2. `incremental_predicates` can limit which existing target rows participate in matching.

For example, on Snowflake a target predicate could be:

```yaml
incremental_predicates:
  - "DBT_INTERNAL_DEST.order_date >= dateadd(day, -7, current_date)"
```

**Correctness trap:** suppose an incoming correction has key 42, whose existing row is thirty days old. The predicate hides that target row from the merge. The correction can be treated as unmatched and inserted as a duplicate. A three-day source arrival lookback does not prove that all matching target rows are seven days old or newer.

Use target pruning only with a defensible invariant about match locations. Separately, align source filters with partition columns and inspect the query plan; a selective SQL condition does not always yield physical partition pruning.

### on_schema_change and schema evolution

| Setting | Intent |
| --- | --- |
| `ignore` | Default behavior; do not automatically synchronize the target schema |
| `fail` | Stop when a supported schema comparison detects a mismatch |
| `append_new_columns` | Add newly selected columns without removing old ones |
| `sync_all_columns` | Synchronize additions/removals and supported type changes |

These settings do not backfill historical values for a new column. Dropped or renamed columns can still break SQL; type changes may be unsupported or expensive; nested-field handling is adapter-specific. A rename may appear as a removal plus an addition. Plan consumer changes and historical recomputation explicitly. [Schema-change behavior](https://docs.getdbt.com/docs/build/incremental-models?name=Fusion&version=2.0).

### Full refresh, late data, and backfills

`dbt run --full-refresh --select fct_orders` requests a rebuild of the selected incremental model, subject to its `full_refresh` configuration. It does not restore source records that upstream retention has already deleted. Rebuilding a parent also does not automatically repair unselected materialized descendants.

Choose a late-data policy from observed lateness and correction requirements:

- **Overlap window:** replay recent changes using merge/deduplication.
- **Affected keys or partitions:** identify and recompute the complete affected output scope.
- **Backfill:** replay an explicit historical range with controlled downstream repair.
- **Full rebuild:** use when complete source history exists and cost is acceptable.

A changed order can alter an older customer's aggregate or a historical daily total. Filtering input events by recent arrival alone is insufficient if the output must recompute all contributing rows for an affected aggregate. Discover changed groups first, then rebuild each group's complete result.

### Microbatch

Microbatch uses `event_time`, `begin`, and `batch_size` to define bounded batches. `lookback` reprocesses previous batches; upstream models/sources need appropriate `event_time` configuration for automatic input filtering. Independent batches can be retried, and supported execution can parallelize them. It remains scheduled batch processing. [Microbatch models](https://docs.getdbt.com/docs/build/incremental-microbatch).

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_at',
    begin='2025-01-01',
    batch_size='day',
    lookback=3
) }}

select event_id, customer_id, event_at, amount
from {{ ref('stg_events') }}
```

```yaml
models:
  - name: stg_events
    config:
      event_time: event_at
```

```bash
dbt run --select fct_events_microbatch \
  --event-time-start '2025-02-01' \
  --event-time-end '2025-02-08'
```

For this example, name the model file `fct_events_microbatch.sql`. The explicit range describes a backfill; the end boundary is exclusive. A correction outside the lookback still requires explicit reprocessing. Data without an event-time annotation may be read repeatedly rather than automatically filtered.

### Idempotency, deduplication, and CDC

**Idempotency** means replaying the same logical input leaves the same logical output. It needs a stable key, deterministic transformations, correct replacement scope, and a strategy that handles replay. `current_timestamp` in business output, random identifiers, or accumulating deltas can make repeated execution change results.

For CDC, establish whether the source contains full images or partial patches, how ordering works, and how deletes are encoded. Deduplicate to the final change per key using a reliable sequence. Partial patches need reconstruction before replacing complete rows. A tombstone must either remain as a deleted state or trigger explicit deletion through supported logic; simply filtering tombstones out leaves old target rows behind.

**Interview answer:** “Incremental is a correctness contract: which changes can arrive, how I detect them, what output they affect, and how I replay or repair them. The strategy only implements part of that contract.”

## 4. Testing & Data Quality

### Generic and singular data tests

A model can build successfully and still contain duplicate keys or invalid relationships. dbt data tests express queries that identify those violations: normally, zero violating records means success, subject to configured thresholds. A **generic test** is reusable, parameterized SQL in a `{% test ... %}` block and attached through YAML; a **singular test** is a standalone query in the test directory.

| Built-in test | Checks | Important boundary |
| --- | --- | --- |
| `unique` | Non-null values do not repeat | Add `not_null` separately |
| `not_null` | Required values are populated | Does not enforce uniqueness |
| `relationships` | Non-null child keys exist in the referenced parent | Does not prove parent uniqueness or join cardinality |
| `accepted_values` | Non-null values belong to an allowed set | Add `not_null` if nulls are prohibited |

Tests assess actual data; defining YAML does not install equivalent database constraints. [Data tests](https://docs.getdbt.com/docs/build/data-tests).

```yaml
version: 2

models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests:
          - not_null
          - unique
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_customers')
                field: customer_id
      - name: status
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['placed', 'paid', 'cancelled']
```

This uses the v1.10.5+ `arguments` form. The `relationships` example assumes `dim_customers` has one row per `customer_id`, rather than multiple SCD versions.

```sql
-- tests/order_amount_must_be_nonnegative.sql
select order_id, amount
from {{ ref('fct_orders') }}
where amount < 0
```

### Custom tests, macros, and severity

```sql
-- tests/generic/nonnegative.sql
{% test nonnegative(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
```

Attach it as `data_tests: [nonnegative]` on a column. Its null behavior is deliberate: combine it with `not_null` when needed. This is useful once the rule is reused; a single model-specific rule can remain a singular test.

`severity: error` evaluates `error_if` first, then `warn_if`; `severity: warn` only evaluates the warning condition. Defaults use nonzero failures. Warning promotion flags can make warnings fail a job. Define policy deliberately rather than downgrading a broken key constraint to keep the pipeline green. [Test severity](https://docs.getdbt.com/reference/resource-configs/severity).

Useful production assertions include source-to-mart reconciliation at the same grain, SCD interval non-overlap, and an explicit tolerance for monetary rounding. Persisted failing rows can help diagnosis, but may contain sensitive data and need appropriate access and retention.

### Unit tests

Unit tests compare a SQL model's output against expected rows using small fixture inputs. They test logic before full materialization, rather than validating production data. In the baseline they still use the warehouse; fixture-driven does not mean connection-free. Support and restrictions depend on the engine and adapter. [Unit tests](https://docs.getdbt.com/docs/build/unit-tests).

```sql
-- models/marts/order_flags.sql
select order_id, amount > 0 as is_positive
from {{ ref('stg_orders') }}
```

```yaml
unit_tests:
  - name: classify_positive_zero_and_negative_amounts
    model: order_flags
    given:
      - input: ref('stg_orders')
        rows:
          - {order_id: 1, amount: 10}
          - {order_id: 2, amount: 0}
          - {order_id: 3, amount: -5}
    expect:
      rows:
        - {order_id: 1, is_positive: true}
        - {order_id: 2, is_positive: false}
        - {order_id: 3, is_positive: false}
```

Put the YAML under `models/`. For incremental logic, test both `is_incremental()` branches using supported overrides and fixture the existing `this` relation when required. These tests validate selected query output; warehouse integration tests must still verify actual merge/overwrite behavior across runs.

### Source freshness

Freshness checks whether the source is recent enough according to its configured timestamp or supported metadata. It does not prove completeness: one recent row can hide a missing historical partition.

```yaml
sources:
  - name: commerce
    schema: commerce
    config:
      loaded_at_field: _loaded_at
      freshness:
        warn_after: {count: 2, period: hour}
        error_after: {count: 4, period: hour}
    tables:
      - name: orders
```

This is a separate illustrative source declaration; merge its settings into the earlier source rather than declaring the same source twice. `_loaded_at` must exist and represent the intended loading time. In the baseline, run `dbt source freshness` explicitly; `dbt build` does not perform it. Current v2 also provides `dbt freshness` for sources and models. [Freshness configuration](https://docs.getdbt.com/reference/resource-configs/freshness).

### Contracts and schema enforcement

A contract enforces a model's declared output names and types for supported materializations/adapters. Column `data_type` declarations without `contract.enforced` are not equivalent enforcement.

```yaml
models:
  - name: order_flags
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: integer
      - name: is_positive
        data_type: boolean
```

Types must match actual warehouse output; the example assumes an integer source key. Contracts cover structure; data tests cover values. Physical constraint enforcement varies by warehouse. In the baseline, ephemeral models do not support contracts, and contracted incremental models require `on_schema_change: fail` or `append_new_columns`. [Model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts).

### CI testing and validation

Use unit fixtures for edge cases, data tests for built results, and integration runs for materialization behavior. For a merge model, build an isolated target, replay the same input, introduce an update and duplicate, then verify key uniqueness and expected final rows. Check a schema mismatch and a late correction too.

In `dbt build`, unit tests can gate model construction, while error-level data test failures can block downstream resources. This is not a transaction over the whole DAG: upstream relations may already have changed. A failed job can leave partially updated datasets. [build behavior](https://docs.getdbt.com/reference/commands/build).

**Interview pitfall:** “All tests passed, so the data is correct.” Tests only demonstrate the asserted properties on the selected data; they can miss incorrect grain, omitted records, or an untested business definition.

## 5. Jinja, Macros & Packages

### Templating, variables, and environment variables

If a model repeats the same expression for several statuses, Jinja can generate that SQL before the warehouse executes it. `{{ ... }}` renders an expression, `{% ... %}` controls generation, and `{# ... #}` marks a template comment. A Jinja loop therefore repeats SQL text at generation time; it does not run a distributed loop over warehouse rows.

```sql
{% set statuses = ['paid', 'cancelled'] %}

select
    customer_id,
    {% for status in statuses %}
    sum(case when status = '{{ status }}' then amount else 0 end)
        as {{ status }}_amount{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_orders') }}
group by customer_id
```

The example uses a trusted fixed list. Do not insert untrusted strings into SQL identifiers or literals without validation and appropriate quoting.

`var('name', default)` reads project/CLI variables; `env_var('NAME')` reads process environment variables. For example, `dbt run --vars '{lookback_days: 3}'` supplies a variable; a macro must still validate a variable if it changes SQL structure. Environment variables are strings unless converted; secret values belong in supported credential configuration and must not be rendered into model SQL or logs.

### Macros, dispatch, and adapter-specific SQL

A macro is a reusable Jinja function. It can render SQL, return a value, or perform a supported dbt operation. Keep repeated business logic understandable; generating a thousand columns makes compilation compact but the query expensive.

```sql
-- macros/cents_to_currency.sql
{% macro cents_to_currency(column_name) %}
({{ column_name }} / 100.0)
{% endmacro %}
```

Use it as `{{ cents_to_currency('amount_cents') }}` for a trusted column expression. Money still needs an explicit precision and rounding policy in the resulting model.

`adapter.dispatch()` resolves an adapter-specific implementation, using macro prefixes such as `snowflake__` and `default__` plus configured namespace search order. A package can expose one interface with different SQL underneath; dispatch does not translate arbitrary SQL. [Dispatch](https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch).

### run_query() and statement blocks

In Core v1, `run_query()` returns an Agate table for a result-producing query and can return `none` for statements without a result. It does not automatically start a transaction. Guard result-dependent Jinja during parsing and keep compile-time queries bounded. [run_query](https://docs.getdbt.com/reference/dbt-jinja-functions/run_query).

```sql
-- macros/log_order_count.sql
{% macro log_order_count() %}
    {% if execute %}
        {% set query %}
            select count(*) as row_count from {{ ref('stg_orders') }}
        {% endset %}
        {% set result = run_query(query) %}
        {% if result is not none %}
            {{ log('Order rows: ' ~ result.columns[0].values()[0], info=true) }}
        {% endif %}
    {% endif %}
{% endmacro %}
```

Invoke this deliberately with `dbt run-operation log_order_count`; do not call it from every model. Its full count can be expensive. `execute` protects parsing, not compilation from warehouse access.

For more control, `statement(name, fetch_result=True, auto_begin=True)` exposes execution settings; `load_result(name)` retrieves response metadata and fetched data. Dependencies hidden inside execution-only branches or statement blocks may require an explicit SQL dependency comment such as `-- depends_on: {{ ref('stg_orders') }}`. [Statement blocks](https://docs.getdbt.com/reference/dbt-jinja-functions/statement-blocks).

### Hooks

| Hook | Scope | Typical purpose |
| --- | --- | --- |
| `pre-hook` | Before a resource is materialized | Required session or preparation SQL |
| `post-hook` | After a resource is materialized | Adapter-specific maintenance or auditing |
| `on-run-start` | Start of supported command invocation | Shared setup |
| `on-run-end` | End of supported command invocation | Summarize execution results |

Hooks are SQL or macros producing SQL. Invocation hooks also apply to commands such as compile, not only production model runs. `on-run-end` has additional context including results. It is not a guaranteed callback after process termination. [Invocation hooks](https://docs.getdbt.com/reference/project-configs/on-run-start-on-run-end).

Hook transaction behavior depends on the adapter. A failing post-hook can leave a built relation in place; a retried audit insert can duplicate records. Make side effects replay-safe and use declarative configurations, such as supported grants configuration, where they cover the need.

### Packages and reusable transformations

Packages are dbt projects containing reusable macros, models, tests, and other resources. Install declared dependencies with `dbt deps`. Pin compatible versions and commit the dependency lockfile so development, CI, and deployment resolve consistently. Review package code and enabled resources as part of the project's execution surface. [Packages](https://docs.getdbt.com/docs/build/packages).

`dbt_utils`, for example, supplies reusable transformation and testing helpers. A package is useful for established functionality, but a two-line expression does not automatically justify a dependency. Macro reuse is not equivalent to sharing a governed model across independently deployed projects.

**Interview answer:** “Jinja runs in dbt to generate or inspect SQL; the warehouse runs the resulting SQL. I use macros for repeated rules and dispatch where the warehouse dialect genuinely differs.”

## 6. Snapshots & Historization

### SCD Type 2 and change-detection strategies

If a source keeps only the customer's current region, today's row cannot by itself explain last year's report. **Snapshots** preserve observed versions of mutable records as SCD Type 2 history. A stable business `unique_key` connects those versions; the change-detection strategy determines when dbt records a new one.

| Strategy | Detects change using | Best when | Failure mode |
| --- | --- | --- | --- |
| `timestamp` | A newer `updated_at` | Every relevant source update advances a reliable timestamp | Changes with unchanged, null, or regressed timestamps can be missed |
| `check` | Differences in configured `check_cols` | No trustworthy update timestamp | Unlisted columns are not tracked; volatile columns create noise |

Prefer timestamp when the source provides a reliable update time. Both approaches observe source state at snapshot execution; neither reconstructs changes that occur and disappear between runs. [Snapshots](https://docs.getdbt.com/docs/build/snapshots).

```yaml
# snapshots/customers_snapshot.yml
version: 2

snapshots:
  - name: customers_snapshot
    relation: source('commerce', 'customers')
    config:
      schema: snapshots
      unique_key: customer_id
      strategy: timestamp
      updated_at: updated_at
      hard_deletes: invalidate
```

Declare `commerce.customers` as a source before using this example. This is v1.9+ YAML snapshot syntax and assumes an adapter supporting `hard_deletes`. For the check alternative, use `strategy: check` and `check_cols: ['name', 'segment']` instead of relying on `updated_at` for detection. Older projects may use SQL `{% snapshot %}` blocks.

### Validity intervals and historical joins

| customer_id | segment | dbt_valid_from | dbt_valid_to |
| --- | --- | --- | --- |
| 7 | standard | 2025-01-01 | 2025-03-01 |
| 7 | premium | 2025-03-01 | NULL |

Use half-open validity intervals: `[dbt_valid_from, dbt_valid_to)`. A fact at the boundary belongs to the new version. The current version normally has null `dbt_valid_to`; `dbt_valid_to_current` can configure a sentinel instead. `dbt_scd_id` identifies a snapshot version, while `dbt_updated_at` records snapshot change-detection metadata.

```sql
select o.order_id, c.segment
from {{ ref('fct_orders') }} as o
left join {{ ref('customers_snapshot') }} as c
  on o.customer_id = c.customer_id
 and o.ordered_at >= c.dbt_valid_from
 and (o.ordered_at < c.dbt_valid_to or c.dbt_valid_to is null)
```

This assumes compatible timestamp semantics, the default null current marker, and no overlapping intervals. The earlier minimal `fct_orders` example must expose `ordered_at` before using this join. Joining only on `customer_id` multiplies facts across versions.

Timestamp strategy uses source update times for ordinary version boundaries; check strategy typically uses snapshot observation time when no update timestamp is supplied. Observation history is not necessarily exact business-effective history. A source timestamp alone also cannot recover unobserved intermediate states.

### Hard deletes and historical tracking

| `hard_deletes` value | Deleted source record behavior |
| --- | --- |
| `ignore` | Keep the previously current version open |
| `invalidate` | Close the current version when deletion is observed |
| `new_record` | Add an explicit deleted version with `dbt_is_deleted` metadata |

These newer settings require supported adapters and do not automatically migrate an existing snapshot table. Older `invalidate_hard_deletes` configuration should not be combined with `hard_deletes`. [Hard-delete configuration](https://docs.getdbt.com/reference/resource-configs/hard-deletes).

Deletion detection requires a complete view of the intended source population. Snapshotting only today's source partition can falsely interpret older missing rows as deletions. A temporary empty source after an ingestion failure can likewise invalidate history if treated as authoritative.

Protect snapshot tables as historical state. Starting a snapshot today does not recover yesterday's overwritten values, and dropping/recreating it loses observations unless they exist elsewhere. Snapshot execution is not full-refreshable in the same way as an incremental model.

### Snapshots vs. incremental models

| Question | Snapshot | Incremental model |
| --- | --- | --- |
| Primary goal | Capture observed historical versions | Efficiently maintain a transformed dataset |
| Typical row identity | Business key plus historical version | Model's configured grain/key |
| History automatic? | Yes, according to the snapshot strategy | No; implement it explicitly if required |
| Rebuild from raw history? | Only if equivalent observations/history exist | Possible when source history and logic are sufficient |

A common design snapshots mutable customer source records and derives an SCD dimension downstream, while incrementally processing order facts. If a complete ordered CDC log already exists, it can be a more precise basis for history than periodic observations.

**Interview pitfall:** “A snapshot captures every update.” It captures states visible at execution, subject to its detection strategy and source reliability.

## 7. DAG Execution & State Management

### Commands

The command you run defines which work dbt attempts. Building models, testing existing relations, compiling SQL, and updating snapshots are distinct operations. Know that boundary before interpreting a successful exit as proof that the entire dataset or project was validated.

| Command | Purpose | Boundary to remember |
| --- | --- | --- |
| `dbt run` | Materialize selected models | Does not also run all data tests |
| `dbt build` | Build selected models, seeds, snapshots, and tests in DAG order | Not an atomic deployment of the entire project |
| `dbt test` | Execute selected data/unit tests | Data tests need their referenced relations |
| `dbt compile` | Render selected resources into SQL | Can access the warehouse; does not build model relations |
| `dbt seed` | Load version-controlled CSV reference data | Intended for small controlled datasets, not general ingestion |
| `dbt snapshot` | Update snapshot history | Preserves state rather than recreating all past observations |

Also know `dbt debug` for connection/configuration diagnosis, `dbt deps` for dependencies, and `dbt ls` for inspecting selections. The table uses the stated v1 baseline; later versions add resource types and capabilities. [Command reference](https://docs.getdbt.com/reference/commands/build).

### Node selection: graph operators, tags, selectors

| Selection | Meaning |
| --- | --- |
| `fct_orders` | The named node, subject to command resource filtering |
| `+fct_orders` | Node and its ancestors |
| `fct_orders+` | Node and its descendants |
| `+fct_orders+` | Ancestors, node, and descendants |
| `1+fct_orders` | Node and one upstream generation |
| `tag:finance` | Resources carrying the finance tag |
| `tag:finance,config.materialized:incremental` | Intersection of both conditions |
| `tag:finance tag:daily` | Union of the two selections |

Graph selection does not override the command's purpose: `dbt run` will not load a selected seed. Quote selectors so the shell does not interpret them. [Graph operators](https://docs.getdbt.com/reference/node-selection/graph-operators).

For a reused selection, define `selectors.yml`:

```yaml
selectors:
  - name: finance
    definition:
      method: tag
      value: finance
```

Run `dbt ls --selector finance` to inspect it before `dbt build --selector finance`. Tests can be indirectly selected through their parents; multi-parent tests and indirect-selection modes affect the actual set. Inspect the selected tests rather than assuming a model selector isolates one table.

### State-based selection and artifacts

**State selection** compares the current project to prior artifacts. `state:modified` detects changes to resource definitions and relevant dependencies, not new rows in a warehouse table. A macro change may mark many dependent nodes modified. Variable/environment-value changes have comparison caveats. [State comparison caveats](https://docs.getdbt.com/reference/node-selection/state-comparison-caveats).

| Artifact | What it describes | Useful for |
| --- | --- | --- |
| `manifest.json` | Parsed project, dependencies, resource metadata | State comparison, deferral, lineage |
| `run_results.json` | Results and timings for executed resources | Failures, performance analysis, status monitoring |
| `catalog.json` | Warehouse relation/column metadata from v1 docs generation | Generated documentation |
| `sources.json` | Source freshness observations in the baseline | Source freshness monitoring |

Artifacts are versioned schemas, normally written under `target/`. A manifest describes the project; it does not prove every relation was successfully built. Publish a trusted successful production artifact set with its commit and invocation metadata. [Artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts).

### Slim CI and defer

**Slim CI** builds changed resources and relevant descendants while reusing unchanged upstream production relations through deferral:

```bash
dbt build --target ci \
  --select 'state:modified+' \
  --state artifacts/production \
  --defer \
  --favor-state \
  --target-path target/ci
```

Assume `artifacts/production/manifest.json` is a compatible manifest from a successful production deployment. The CI target must resolve to an isolated writable namespace.

State answers **what changed**; defer answers **where an unselected reference resolves**. By default, an eligible unselected node defers when its relation does not exist in the current target; `--favor-state` prefers the state relation even if a current-target relation exists. Ephemeral models are not deferred. [Deferral](https://docs.getdbt.com/reference/node-selection/defer).

Keep the state input separate from the output `target-path`, so a command does not overwrite its comparison baseline. Selected models still build in CI. Deferral needs read access to referenced production relations; tests can compare mixed environments.

**Critical CI gap:** if a selected incremental model does not exist in the CI schema, its first build follows the full-build path. Slim CI alone does not test incremental updates. Prepare a representative existing target, through an isolated build or supported clone, then exercise a second run. Also retain broader validation for changes that state comparison cannot safely capture.

### Threads, concurrency, and failure propagation

`threads` limits concurrent eligible work scheduled by dbt; it is not the number of warehouse workers or query-plan tasks. Available DAG nodes, warehouse slots, locks, and query duration determine achieved concurrency.

A failed model normally blocks descendants; independent branches can continue unless fail-fast behavior stops the invocation. Error-level tests in `build` can also gate descendants; multi-parent tests have more specific blocking rules. Warnings normally do not block downstream work unless promoted. Capture the final exit status and resource results rather than reading only the last successful query.

Rerunning only the failed node may leave skipped descendants stale. Select the required downstream scope, verify already-applied side effects, and avoid overlapping writers to the same target.

**Interview answer:** “A state manifest lets me compare project definitions, and defer lets CI reuse unchanged upstream relations. Neither mechanism proves an incremental replay works or that source data is unchanged.”

## 8. Performance & Optimization

### Incremental vs. full refresh

An incremental model can process very little new data and still spend most of its time scanning the target for a merge. Count the whole cost: source reads, transformations, target matching and writes, and consumer queries. Incremental processing helps when the affected scope is much smaller than the table and maintenance stays efficient. Large merge scans, repeated small-file work, or difficult repair procedures can erase that advantage.

A full rebuild can be simpler and competitive for small dimensions or frequently rewritten datasets. Compare total compute, elapsed time, freshness, and operating burden. Maintain a rebuild/backfill procedure even when the normal run is incremental.

### Warehouse-specific SQL optimization

| Platform | Investigate | Common mistake |
| --- | --- | --- |
| Snowflake | Query profile, pruning, clustering, spilling, warehouse size/concurrency | Assuming a selective predicate guarantees micro-partition pruning |
| BigQuery | Bytes scanned, partition filters, clustering, shuffle | Reading all partitions to update a small date range |
| Databricks/Spark | Plan, shuffle/skew, file layout, partition/cluster choices, maintenance | Repeatedly scanning many small files or overpartitioning |
| Postgres/Redshift | Plans, statistics, appropriate indexes or sort/distribution design | Reusing a physical-design recipe from a different engine |

These are investigation directions, not universal tuning commands. Confirm supported adapter configuration and use the warehouse's plan and measurements before changing physical design. Materialization choices change where repeated computation is paid. [Materialization guidance](https://docs.getdbt.com/docs/build/materializations).

### Early filtering and partition pruning

Filter before expensive joins/windows when semantics allow it. Project needed columns. Use predicates compatible with physical partitioning and verify the resulting scan scope. Applying a function to a partition column can impede pruning on some systems; a direct range condition is often easier to optimize.

Moving a filter can change meaning: ranking all versions and then filtering is not equivalent to filtering versions before ranking. Similarly, applying a `where` condition to the right side of a left join can remove unmatched rows. Optimize after identifying the intended output, not by mechanically moving every condition upward.

### DAG parallelism and bottlenecks

An interview investigation can follow this sequence:

1. Compare wall-clock time with per-node timing in `run_results.json`.
2. Find the critical dependency chain and repeated expensive subqueries.
3. Separate warehouse queue time, execution time, and dbt parsing/compilation overhead.
4. Inspect the slow query's actual plan: scans, joins, skew, spill, and writes.
5. Change one cause and compare equivalent data/results and resource usage.

If twelve models are waiting behind one twenty-minute model, raising threads from eight to sixteen may not help. If independent queries spend time queued, more threads can worsen contention. If compilation is slow because macros query metadata repeatedly, warehouse resizing may not fix the main issue.

### Query cost and consumer cost

Measure the workload, not just build duration. A table may cost more to build than a view but save money when hundreds of dashboards reuse it. An ephemeral model may avoid storage but repeat its expensive logic in multiple consumers.

Track processed bytes or platform-equivalent compute, rows affected, query history identifiers, build frequency, and downstream latency. Include tests, freshness queries, hooks, and documentation introspection in total cost. Avoid truncating CI data in a way that removes the edge cases the change is meant to validate.

**Interview answer:** “I identify whether time is spent waiting in the DAG, queued in the warehouse, scanning data, matching target rows, or executing expensive operators. Then I optimize that cause and verify output equivalence.”

## 9. Production & Advanced Topics

### CI/CD, environments, and deployment

A tested SQL change is ready to release only when the deployed environment, selected models, and affected consumers are accounted for. A practical release flow carries the same reviewed revision from reproducible execution through validation and publication:

1. Pin the engine, adapter, and package dependencies; run from a reproducible environment.
2. Parse/compile and run unit tests plus selected integration/data checks in an isolated CI target.
3. Review breaking contracts, changed grains, migration needs, and replay behavior.
4. Deploy the approved commit using production credentials and an explicit command selection.
5. Run source freshness and required builds/tests; collect exit status, artifacts, and query metadata.
6. Publish the successful production state for later CI and alert on failures/freshness breaches.

Use separate write permissions and credentials per environment. Limit production source access appropriately, and prevent CI from resolving its output schemas to production. Treat logs, compiled SQL, failure samples, and artifacts as potentially sensitive operational data.

Deployment can partially succeed. Rolling back Git restores code but does not restore overwritten data, snapshot history, or an incompatible schema change. For risky changes, plan the data migration and recovery path alongside the code release. A two-phase versioned model migration can preserve old consumers while new ones move.

### Airflow integration and scheduling

Airflow manages dependencies across ingestion, dbt, and consumers. dbt manages transformations inside its own graph. Typical coordination is:

```text
ingest + validate source availability
              |
       check source freshness
              |
       dbt build selected DAG
              |
       publish success / refresh consumers
```

Run dbt through a controlled command/container task or trigger and monitor a managed dbt job. Choose the integration according to who owns scheduling and runtime. A single dbt invocation is often sufficient; one Airflow task per model adds coordination and artifact complexity and needs a concrete benefit.

Wait for completion, propagate nonzero exit status, retain artifacts, and make retry behavior safe. Do not mark a managed job successful merely because its trigger request was accepted. Avoid independent schedules writing the same relations concurrently, and pass historical intervals explicitly for backfills instead of using only wall-clock “today.”

### Documentation, lineage, and exposures

Document grain, business definitions, ownership, freshness expectations, and important limitations near the model. In the v1 baseline, `dbt docs generate` creates documentation artifacts and `dbt docs serve` previews the site. Source/model references provide lineage; hardcoded SQL references can leave gaps. Current v2 and Platform documentation hosting have different artifact and publishing workflows. [Documentation](https://docs.getdbt.com/docs/build/documentation).

An **exposure** describes a downstream dashboard, application, or analysis and its dependencies. It supports impact analysis and selection; it does not run the dashboard.

```yaml
exposures:
  - name: revenue_dashboard
    type: dashboard
    owner:
      name: Analytics
      email: analytics@example.com
    depends_on:
      - ref('fct_orders')
```

Ask which consumers depend on a model before changing its grain or dropping a column. Exposure metadata makes that conversation easier, but only reflects declared or integrated consumers. [Exposures](https://docs.getdbt.com/docs/build/exposures).

### Semantic Layer and metrics

Use [Semantic Layer and MetricFlow](../storage-and-modeling/semantic-layer.md#3-dbt-semantic-layer--metricflow) for semantic model configuration and query planning, and [metric definitions](../storage-and-modeling/semantic-layer.md#2-metric-definitions--business-logic-centralization) for business rules and aggregation behavior.

The dbt models supplying these definitions still need tested grain, keys, and historical validity. Keep semantic configuration aligned with the installed product/version.

### Model versioning and contracts

Model versions allow an old interface and a breaking new interface to coexist. They are separate from Git commits and from a package's release version.

```yaml
models:
  - name: dim_customers
    latest_version: 2
    versions:
      - v: 1
      - v: 2
```

With the default naming convention, implement `dim_customers_v1.sql` and `dim_customers_v2.sql`. Consumers can pin `{{ ref('dim_customers', version=1) }}`; an unversioned reference resolves to the configured latest version. Use contracts, tests, documentation, and an explicit migration/deprecation process together. Versioning alone does not enforce equivalent business meaning or migrate downstream consumers. [Model versions](https://docs.getdbt.com/docs/mesh/govern/model-versions).

### Debugging compilation and execution failures

| Symptom | First checks | Likely area |
| --- | --- | --- |
| Missing `ref` or source | Resource name, package, enabled state, YAML declaration | Project/dependency definition |
| Jinja undefined variable | Macro arguments, `var`, environment, parse-time guards | Template context |
| SQL syntax/type error | Rendered SQL, adapter dialect, actual column types | Compilation output or warehouse SQL |
| Permission or missing relation | Current role/database/schema, defer target, build order | Environment/access or missing upstream build |
| Merge duplicate-match error | Source duplicates, deterministic winner, target key quality | Incremental grain/order |
| Successful run, missing updates | Watermark, lateness, target predicates, source retention | Incremental completeness |
| Slow run | Node timings, query history, queueing, plans, macro queries | DAG, warehouse, or compilation |

A focused baseline workflow is:

```bash
dbt debug --target dev
dbt ls --select '+fct_orders'
dbt compile --target dev --select fct_orders
dbt run --target dev --select fct_orders --debug
```

Run these deliberately in an isolated development environment; `run` changes its target. Inspect rendered SQL under `target/compiled`, executable materialization SQL under `target/run` when generated, and `logs/dbt.log`. Match the failure to warehouse query history where possible. A compile success does not prove permissions, data quality, or materialization success.

Do not immediately “fix” a merge failure with full refresh. Identify the duplicate grain or faulty matching rule first, then repair affected data and add the assertion that would have exposed it.

### Interview scenarios to practice

| Question | A strong answer should cover |
| --- | --- |
| An incremental table has duplicate keys after retries. Why? | Append/replay behavior, source deduplication, null keys, target pruning, concurrent writers |
| Revenue corrections arrive thirty days late. What changes? | Arrival detection, affected output grain, complete recomputation scope, bounded backfill, reconciliation |
| CI passed but production incremental execution failed. Why? | Missing existing target in CI, untested incremental branch, representative replay/integration checks |
| Customer history is missing intermediate changes. Can snapshots recover it? | Observation frequency, change strategy, whether ordered CDC or other historical evidence exists |
| The project has more threads but no speedup. What next? | Critical path, warehouse queueing, repeated work, actual query profiles |
| A model's column must be renamed without breaking dashboards. How? | Consumer inventory, new version/contract, coexistence, migration and retirement |
| A job failed halfway through. Is the old dataset still intact? | Per-resource changes, adapter transactions, partial deployment, downstream staleness, recovery plan |

For each answer, name the invariant, give a concrete failure example, describe the smallest correction, and explain the validation that would prove it works.
