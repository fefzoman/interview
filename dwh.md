# Data Warehousing (DWH): Data Engineer Interview Guide

A data warehouse integrates data from operational systems into a consistent, historical foundation for analytics. In interviews, connect each design choice to the business question, data grain, correctness guarantees, latency, and operating cost.

**Scope:** Platform-neutral fundamentals, with selected Snowflake, BigQuery, Redshift, and dbt examples. Product documentation was checked on October 21, 2026; capabilities depend on deployment, edition, adapter, and version. SQL examples illustrate individual patterns rather than a complete deployable pipeline. Bind parameters, timestamp syntax, transactions, and `MERGE` behavior vary by engine.

**Running example:** An online store has customers, products, orders, order lines, payments, and shipments. A sales fact has one row per order line; customer attributes can change over time. This example appears throughout the guide.

## Contents

1. [Architecture & Fundamentals](#1-architecture--fundamentals)
2. [Data Modeling](#2-data-modeling)
3. [Advanced Modeling](#3-advanced-modeling)
4. [Data Integration & Transformation](#4-data-integration--transformation)
5. [Query Processing & Optimization](#5-query-processing--optimization)
6. [Distributed DWH & Performance](#6-distributed-dwh--performance)
7. [Data Quality & Governance](#7-data-quality--governance)
8. [Modern DWH & Analytics](#8-modern-dwh--analytics)
9. [Production & Advanced Topics](#9-production--advanced-topics)
10. [Interview Practice](#10-interview-practice)

## 1. Architecture & Fundamentals

### OLTP vs. OLAP

**Online transaction processing (OLTP)** serves operational workflows, such as placing an order. **Online analytical processing (OLAP)** answers analytical questions, such as monthly revenue by product category.

| Aspect | OLTP | OLAP |
| --- | --- | --- |
| Typical operation | Read or modify a few records | Scan, join, and aggregate many records |
| Priority | Low latency, transaction integrity, concurrent writes | Efficient analysis across large datasets |
| Common modeling choice | Normalized entities | Dimensional or other analytical models |
| Common physical layout | Row-oriented storage and indexes | Columnar storage and distributed scans |
| History | Primarily current operational state | Integrated historical state and events |
| Example | Update one order's shipping address | Compare return rates across three years |

These are workload patterns, not absolute product categories. An OLAP system can support ACID transactions, and an OLTP database can run analytical SQL. Heavy reporting on a production OLTP system can compete with customer transactions for CPU, memory, and I/O. Replication or extraction isolates analytics, with an explicit freshness tradeoff.

### DWH vs. Data Lake vs. Lakehouse

| Architecture | Core idea | Strength | Responsibility or limitation |
| --- | --- | --- | --- |
| Data warehouse | Managed analytical tables with integrated business meaning | SQL analytics, predictable serving, governed reporting | Curating models and controlling compute and storage costs |
| Data lake | Store varied data in files, often in object storage | Flexible ingestion, replay, exploration, ML inputs | Cataloging, quality, access controls, and table consistency need deliberate design |
| Lakehouse | Add transactional table metadata and analytical management over lake storage | Share governed tables across analytical engines | Engine compatibility, catalog coordination, file maintenance, and operational complexity |

**Schema-on-write** validates a representation before publishing it. **Schema-on-read** interprets data when it is consumed. Real platforms use both: a lake needs contracts, and a warehouse can hold semi-structured data.

A directory of Parquet files is not automatically a lakehouse. A transactional table format, catalog, compatible readers and writers, and governance provide the additional guarantees. A warehouse can query lakehouse tables or coexist with a lake; the categories increasingly overlap.

### Enterprise DWH, Data Marts, and Data Vault

- **Enterprise DWH (EDW):** integrates enterprise subjects using consistent definitions, identifiers, and history. It describes scope and integration, not one required schema shape.
- **Data mart:** an analytical dataset for a subject or team, such as finance or marketing. A dependent mart draws from integrated enterprise data; an independent mart integrates its own sources and can create conflicting metrics.
- **Data Vault:** a historical integration modeling approach separating business identities, relationships, and descriptive history. It commonly feeds dimensional marts rather than serving BI users directly.

These can coexist: an EDW can contain a Data Vault integration layer and publish dimensional sales and finance marts. Data Vault is not a competing storage engine.

### ETL vs. ELT, staging, ODS, and DWH layers

**ETL** extracts data, transforms it in an intermediate engine, then loads the target. **ELT** loads data into an analytical platform before transforming it there. The difference is where transformation occurs, not whether data quality matters.

| Approach | Useful when | Main tradeoff |
| --- | --- | --- |
| ETL | Data must be sanitized before entering the target; specialized processing is needed | Separate transformation infrastructure and potentially less replayable detail |
| ELT | The target has scalable compute and raw inputs should be retained | Raw data access, target resource contention, and transformation costs need control |

A common flow is:

```text
Operational sources -> ingestion / CDC -> restricted raw landing
                                            |
                                   staging / standardization
                                            |
                               integrated historical core
                                            |
                                  marts / semantic layer -> BI
```

| Layer | Purpose | Example |
| --- | --- | --- |
| Landing/raw | Preserve received payloads and ingestion metadata for replay | Source order JSON plus source position and arrival time |
| Staging | Type, rename, validate, and standardize source data | Convert amounts to decimal; parse timestamps |
| ODS: operational data store | Integrate relatively current operational data for operational reporting | Current customer and order status across applications |
| Integrated core | Reconcile identifiers, preserve history, apply shared integration rules | Historical customers and orders across countries |
| Presentation/marts | Publish business-friendly facts, dimensions, and aggregates | Daily sales by customer segment |

An ODS is optional; it is usually more current-state oriented than a historical warehouse. Layer names are conventions, and not every logical layer needs a physical table. In dbt, “staging” often means source-aligned transformation models, not a deployment environment or a raw-file landing area.

### Inmon vs. Kimball architectures

| Aspect | Inmon's classic approach | Kimball's approach |
| --- | --- | --- |
| Integration center | Enterprise model, commonly normalized to third normal form (3NF) | Dimensional bus with conformed dimensions |
| Delivery path | Integrated EDW, then downstream marts | Deliver business-process dimensional models incrementally |
| Enterprise consistency | Shared enterprise integration model | Shared dimensions, facts, and business definitions |
| Main challenge | Upfront integration effort and downstream serving work | Maintaining conformance as teams and marts grow |

Kimball does not mean unrelated departmental silos. Inmon does not prohibit dimensional marts. In practice, separate historical integration and dimensional serving layers can combine ideas from both. Start the architecture discussion with requirements rather than declaring one approach universally better.

### MPP, shared-nothing, and shared-storage

**Massively parallel processing (MPP)** divides a query among workers. Workers scan subsets of data, perform local operations, exchange intermediate results, and combine results.

- **Shared-nothing:** workers have independent CPU, memory, and local storage ownership. Good placement enables parallel local work; joins on incompatible keys require network exchange.
- **Shared-storage:** workers access common persistent storage. Compute can scale independently, while local caches and distributed query exchanges still affect performance.
- **Hybrid designs:** shared durable storage with independent MPP compute clusters and local caches.

MPP describes parallel execution; shared-nothing and shared-storage describe resource organization. They are not mutually exclusive labels at every layer. Snowflake explicitly describes a hybrid architecture combining shared storage with MPP compute. [Snowflake architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts).

**Interview question: Why build a DWH if the production database already supports SQL?** Explain workload isolation, cross-source integration, historical consistency, analytical modeling, and governance. SQL support alone does not provide those properties.

## 2. Data Modeling

### Star vs. Snowflake schemas

A **star schema** places a fact table at the center, joined to descriptive dimensions. A **snowflake schema** normalizes parts of those dimensions into additional tables.

```text
Star:       fact_sales -> dim_product(category_name, department_name)

Snowflake:  fact_sales -> dim_product -> dim_category -> dim_department
```

Star schemas simplify query paths and business navigation. Snowflaking can centralize shared hierarchies and reduce repeated attributes, but adds joins and can complicate historical consistency. Columnar compression often reduces the storage penalty of repeated dimension labels; benchmark query and maintenance costs rather than deciding from storage size alone.

The snowflake schema modeling pattern is unrelated to the Snowflake product name.

### Fact tables, dimension tables, and grain

- **Fact:** a measurement or recorded occurrence at a declared grain. A fact table contains dimension references and usually measures.
- **Dimension:** descriptive context used to filter, group, and label facts, such as customer, product, or date.
- **Grain:** the exact business meaning of one row. Declare it before choosing measures or keys.

For the running example:

```text
fact_sales: one row per (source_system, order_id, order_line_id)
  source_system, order_id, order_line_id, customer_sk, product_sk, order_date_key,
  quantity, gross_amount, discount_amount, net_amount, currency_code
```

Do not place the full order shipping charge on every line: summing it duplicates the charge. Keep it at order grain or allocate it using a documented rule. A unique row ID does not substitute for a business grain declaration.

| Measure class | What can be summed? | Example |
| --- | --- | --- |
| Additive | Across relevant dimensions, within compatible units | Sales quantity; revenue in one reporting currency |
| Semi-additive | Across some dimensions, but not others | Account balances across accounts, not across dates |
| Non-additive | Requires recomputing from components | Margin percentage; distinct customers |

For margin, compute `SUM(profit) / SUM(revenue)` with zero handling; averaging row percentages generally changes the weighting. Currency conversion needs a defined rate type and effective date. These distinctions are part of dimensional measure design. [Kimball: additive, semi-additive, and non-additive facts](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/additive-semi-additive-non-additive-fact/).

### Transactional, periodic snapshot, and accumulating snapshot facts

| Fact type | Grain | Write pattern | Typical question |
| --- | --- | --- | --- |
| Transactional | One business event or transaction line | Usually append, with a correction strategy | What sold yesterday? |
| Periodic snapshot | One entity per fixed reporting interval | Publish an interval's state | What was each account's end-of-day balance? |
| Accumulating snapshot | One workflow instance | Update milestone dates as it progresses | How long from order creation to shipment? |

A transaction fact is sparse: no event means no row. A periodic snapshot is designed around regular coverage, including inactive entities where the business requires them. An accumulating snapshot typically has several date roles, such as created, paid, shipped, and delivered. Updating its current row alone does not retain every intermediate workflow state; keep events or additional history if that is required. [Kimball: transaction facts](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/transaction-fact-table/), [periodic snapshots](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/periodic-snapshot-fact-table/), [accumulating snapshots](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/accumulating-snapshot-fact-table/).

### Slowly Changing Dimensions: Types 0–6

An SCD policy specifies how an attribute responds to change. Different attributes in the same dimension can use different policies.

| Type | Mechanism | Example and implication |
| --- | --- | --- |
| 0 | Keep the original value | Retain original acquisition channel |
| 1 | Overwrite the attribute | Correct a misspelled name; previous value is unavailable here |
| 2 | Insert a new versioned row | Preserve the customer's region at each point in time |
| 3 | Add an attribute for a prior or alternative value | Keep `current_region` and `previous_region`; limited history |
| 4 | Move selected attributes into a mini-dimension | Keep changing customer profiles separately; facts reference the applicable profile |
| 5 | Type 4 plus an overwritten current-profile reference in the base dimension | Support the profile at the event and the customer's current profile |
| 6 | Type 2 history plus Type 1 current-value attributes | Keep historical region and overwrite current region across an entity's versions |

Types 4–6 follow Kimball's terminology. Some materials call a separate current/history-table pattern “Type 4”; clarify the mechanism rather than relying on the number. Type 6 is also commonly described as combining Types 1, 2, and 3. Maintaining its current-value attributes requires updating historical rows for that entity, not just its latest row. [Kimball: SCD Types 0, 4, 5, and 6](https://www.kimballgroup.com/2013/02/design-tip-152-slowly-changing-dimension-types-0-4-5-6-7/).

A Type 2 dimension might look like this; timestamps are UTC:

| customer_sk | customer_id | region | valid_from | valid_to |
| --- | --- | --- | --- | --- |
| 101 | C17 | West | 2025-01-01 00:00 | 2025-04-01 00:00 |
| 205 | C17 | East | 2025-04-01 00:00 | NULL |

Use half-open intervals `[valid_from, valid_to)`: the end belongs to the next version. Here `NULL` means the open end. For an event at exactly April 1, the East version applies.

```sql
SELECT f.order_line_id, d.customer_sk, f.net_amount
FROM staged_sales AS f
JOIN dim_customer AS d
  ON f.customer_id = d.customer_id
 AND f.order_ts >= d.valid_from
 AND (f.order_ts < d.valid_to OR d.valid_to IS NULL);
```

This example assumes one source namespace and resolved dimension coverage. In a real load, add the source namespace when keys can collide, and route unmatched facts to an explicit inferred/unknown-member policy rather than silently dropping them through an inner join. Validate that exactly one version matches each fact. [Kimball: Type 2 dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/).

For a normal forward-moving Type 2 change, expire the current version and insert its successor atomically. Detect actual attribute changes using null-safe comparison. Historical corrections can require splitting intervals and rebuilding affected fact references; a simple current-row update does not handle that case.

### Surrogate vs. natural keys

| Key | Meaning | Main concern |
| --- | --- | --- |
| Natural/business key | Identifier meaningful to a source or business, such as customer number | Can be reused, changed, or collide across sources |
| Surrogate key | Warehouse-controlled identifier for a modeled row | Needs consistent generation and mapping |

A Type 2 surrogate key identifies a version; the customer's durable identity connects versions. Keep source business keys for traceability. An integer surrogate is compact, while deterministic hashes can support parallel loading but require canonical inputs and collision handling. Neither key type guarantees source uniqueness or correct entity matching.

### Conformed and role-playing dimensions

**Conformed dimensions** have compatible keys, attributes, and business definitions across fact tables. Sales and returns can share a product dimension, allowing consistent product-level comparison.

Do not directly join sales lines to return lines and then sum both: multiple rows on each side create fanout. Aggregate each fact independently to a compatible grain, then join the aggregates using conformed attributes. A dimension with the same name but different calendar rules is not conformed.

A **role-playing dimension** is one logical dimension used for several roles. `dim_date` can join as order date, shipment date, and delivery date. Use aliases or role-specific views so BI users can select the intended relationship.

### Degenerate dimensions, junk dimensions, and bridge tables

- **Degenerate dimension:** an identifier retained in the fact without its own dimension table when it has no additional descriptive attributes. Example: order number on an order-line fact.
- **Junk dimension:** combines several low-cardinality flags or indicators into a dimension, such as gift status, sales channel, and priority. Avoid blindly materializing every possible combination if only a subset occurs.
- **Bridge table:** represents a multivalued relationship, such as one sale attributed to multiple campaigns. Its grain and validity determine how facts can be counted or allocated.

**Interview question: What causes double counting?** Common causes are mixed grains, nonunique dimension joins, overlapping Type 2 intervals, many-to-many joins without allocation, and direct joins between detailed fact tables.

## 3. Advanced Modeling

### Data Vault 2.0: Hubs, Links, and Satellites

Data Vault 2.0 includes modeling, architecture, and delivery practices. Its central modeling separation is:

| Object | Stores | Store example |
| --- | --- | --- |
| Hub | Distinct business identities with load metadata | `hub_customer(customer_hk, customer_id, load_ts, record_source)` |
| Link | Relationships between identities at a declared grain | `link_order_customer(order_customer_hk, order_hk, customer_hk, load_ts, record_source)` |
| Satellite | Descriptive attributes and observed history for a parent hub or link | `sat_customer_profile(customer_hk, load_ts, hashdiff, region, segment, record_source)` |

Hubs are identity stores, not full descriptive dimensions. Links represent relationships; their descriptive context and history can live in satellites. Split satellites when source, change rate, or security needs differ. The illustrated satellite assumes one observation per parent and load timestamp; multi-active or repeated same-time records need a richer grain. [Scalefree: Data Vault structures and technical tests](https://www.scalefree.com/blog/data-warehouse/technical-tests-of-a-data-vault-powered-edw/).

Hash keys and hash differences are common implementations. Define canonical serialization, null treatment, source namespaces where needed, and hash algorithms consistently. Hashing a key does not solve business identity reconciliation, and load timestamps alone do not establish business-effective order.

### Raw Vault vs. Business Vault

- **Raw Vault:** preserves auditable source observations using technical integration rules, source lineage, and history. “Raw” does not mean “no processing”: parsing, key derivation, and structural validation still occur.
- **Business Vault:** applies reusable business interpretation, such as entity matching, derived status, or calculations. Point-in-time (PIT) and bridge structures can make historical access more efficient.
- **Information marts:** present business-friendly dimensional or other serving models to consumers.

Preserve source evidence so changed business rules can be reapplied. Version important rules and retain inputs needed to reproduce outputs. A vault is often an integration foundation rather than the direct reporting interface. [Dan Linstedt: serving data from a Data Vault](https://datavaultalliance.com/engineering/getting-data-out-of-a-data-vault-is-a-lot-easier-than-some-people-think/).

**Tradeoff:** Vault structures support traceability and incremental integration but introduce more tables, joins, metadata, and loading work. Use them when source change, auditability, and integration complexity justify that cost.

### Normalization vs. denormalization

Normalization separates attributes according to dependencies to reduce redundancy and update anomalies. In 3NF, non-key attributes should not depend transitively on a candidate key through other non-key attributes. Denormalization deliberately repeats or prejoins data to simplify reads.

An analytical architecture may normalize its integration core and denormalize its serving layer. Denormalization transfers work from query time to load time and increases the work needed to propagate corrections. Neither approach determines whether the data has valid keys, history, or business definitions.

### One Big Table (OBT) vs. dimensional models

An OBT prejoins descriptive attributes onto a fact-like dataset. Keep an explicit grain: “one big table” is a serving shape, not permission to mix orders, lines, payments, and shipments arbitrarily.

| Choice | Good fit | Main risk |
| --- | --- | --- |
| OBT | Stable, focused workload; easy self-service; repeated expensive joins | Duplicated attributes, costly updates, metric drift, and join fanout during construction |
| Dimensional model | Reusable dimensions and several analytical processes | Consumers need correct relationships and grain awareness |

A sales OBT with event-time region preserves one historical interpretation. A current-region OBT needs historical rows refreshed when a customer moves. Flattening cannot eliminate the need to choose temporal semantics. Both shapes can coexist, with a dimensional core feeding selected wide serving tables.

### Late-arriving facts and dimensions

**Late fact:** an event arrives after its business period or after newer dimension versions have loaded. Resolve its foreign keys using the dimension version effective at the event time, then update the affected reporting periods.

**Late dimension:** a fact arrives before its dimension attributes. Depending on the contract:

1. Create an **inferred member** with the known business key and a stable surrogate key, then fill its missing attributes when they arrive.
2. Use a designated **unknown member**, record unresolved keys, and repair references later.
3. Hold facts in a bounded reconciliation queue when publishing incomplete data is unacceptable.

Keep “unknown,” “not applicable,” and “invalid” distinguishable when they mean different things to the business. Filling an inferred record is different from receiving evidence of a genuine historical change. Retroactive dimension changes can require new intervals and fact rekeying. [Kimball: late-arriving facts](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-fact/), [late-arriving dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/late-arriving-dimension/).

### Many-to-many relationships and factless facts

For campaign attribution:

```text
fact_sales:          order_line_id, net_amount
bridge_attribution:  order_line_id, campaign_sk, allocation_weight
```

If a $100 sale has weights 0.6 and 0.4, allocated campaign revenue is $60 and $40. Validate uniqueness at the bridge grain, valid campaign references, and a total weight of 1 per allocated sale. Giving each campaign the full $100 is a different metric—“influenced revenue”—whose campaign totals are not additive.

A **factless fact table** records an occurrence or coverage relationship without numeric measures: one row per student attending a class, or one row per product eligible for a promotion on a date. Counts become measures. To find non-events, compare expected coverage with observed events; missing event rows alone do not define the population that should have occurred. [Kimball: bridge tables](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/multivalued-dimension-bridge-table/), [factless facts](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/factless-fact-table/).

### Historization and temporal modeling

Separate these clocks:

| Clock | Meaning | Example |
| --- | --- | --- |
| Event/business time | When an event happened | Order placed on March 3 |
| Valid/effective time | When a business assertion applies | Customer region effective from March 1 |
| System/recorded time | When this system recorded that assertion | Region correction loaded on March 10 |

**Bitemporal modeling** tracks valid time and system time. It can answer both “What do we now believe was true on March 3?” and “What did our system believe on March 3, before the correction arrived?” An ordinary Type 2 table with only one validity interval does not necessarily answer both.

Use consistent timezones, precise boundary rules, and nonoverlapping intervals per identity and temporal axis. Source updates, source deletions, inferred defaults, and business-effective changes need explicit history semantics. Storage-level time travel is a different mechanism: it retains earlier table states rather than automatically modeling business validity.

**Interview question: How would you handle a customer's region corrected retroactively?** Establish effective and recorded times, preserve the original assertion if auditability requires it, split the affected interval, identify facts in that interval, and rebuild dependent outputs under a controlled restatement.

## 4. Data Integration & Transformation

### ETL/ELT pipelines and incremental loading

A production pipeline has more than a transformation query: extraction boundaries, durable landing, validation, identity resolution, target publication, progress tracking, and recovery.

An incremental load processes a bounded change set instead of rebuilding all history. A useful batch protocol is:

1. Read the last **committed** source position and choose a stable upper bound.
2. Land the batch durably with a batch ID and source metadata.
3. Validate, deduplicate, and order changes according to the target's semantics.
4. Apply changes and reconcile affected outputs.
5. Publish and advance progress only when recovery is safe.

If target changes and the checkpoint share a transaction boundary, commit them together. Otherwise, design replay to be idempotent: after a crash, repeating the batch must not double count or regress state. A checkpoint written before durable target publication can lose data.

### CDC, watermarking, and high-water marks

**Change data capture (CDC)** captures inserts, updates, and deletes, often from a database transaction log. Log positions and transaction metadata support ordering and recovery. Snapshot bootstrap must coordinate with log capture so changes during the snapshot are neither lost nor misapplied.

**High-water mark** commonly means a saved extraction position, such as a commit sequence or `updated_at` boundary. In stream processing, a **watermark** often estimates event-time progress and controls how long state remains open for late data. These meanings are related to progress but are not interchangeable.

| Mechanism | Advantage | Important limitation |
| --- | --- | --- |
| Log-based CDC | Captures mutations, including deletes, with source positions | Requires permissions, bootstrap coordination, and sufficient log retention |
| Timestamp polling | Simple when a reliable change column exists | Clock precision, late commits, unchanged timestamps, and hard deletes can cause gaps |
| Increasing ID | Efficient for strictly appended data | Usually misses updates and deletes; allocation order may differ from commit order |
| Overlapping lookback | Reprocesses a recent window to tolerate bounded lateness | Requires deduplication and cannot recover arbitrarily late changes |

For a reliable timestamp source, use a bounded window with parameters in the source timezone:

```sql
SELECT order_id, status, updated_at
FROM source_orders
WHERE updated_at >= :window_start
  AND updated_at < :window_end;
```

Advance the committed boundary to `window_end` only after success. An overlap moves the next `window_start` backward. Ties need a defined paging strategy, often `(updated_at, primary_key)` within a stable snapshot. A tuple cursor helps paging ties but does not solve a transaction that commits later with an older timestamp. Use CDC or periodic reconciliation when that behavior is possible.

### Full refresh vs. incremental vs. MERGE

| Approach | Meaning | Good fit | Risk |
| --- | --- | --- | --- |
| Full refresh | Recompute the complete intended target | Small datasets, major logic changes, recoverable rebuilds | Expensive; historical inputs may no longer exist |
| Incremental | Process changed inputs or affected output ranges | Large datasets with reliable change tracking | Missed updates, deletes, or dependency changes |
| `MERGE` | SQL operation applying conditional changes by a match | Upserting a prepared change set | Duplicate source matches and incorrect ordering can corrupt results or cause errors |

`MERGE` is an implementation tool, not a third extraction strategy. An incremental load can append, merge, delete-and-insert, or replace partitions. A full refresh can build a replacement table and publish it atomically rather than truncating the live table first.

The following current-state example assumes `stage_order_changes` already contains exactly one latest change per `order_id`. `source_version` is strictly ordered per key; deletes are retained as tombstones so an older replay cannot resurrect a deleted order:

```sql
MERGE INTO current_orders AS t
USING stage_order_changes AS s
   ON t.order_id = s.order_id
WHEN MATCHED AND s.source_version > t.source_version THEN
  UPDATE SET status = s.status,
             is_deleted = s.is_deleted,
             source_version = s.source_version
WHEN NOT MATCHED THEN
  INSERT (order_id, status, is_deleted, source_version)
  VALUES (s.order_id, s.status, s.is_deleted, s.source_version);
```

Require non-null keys and versions, a stable namespace, and equal-version payload consistency. Consumers exclude tombstones through a governed view. Tombstones must remain available for the possible replay horizon, or equivalent version state must live elsewhere. Concurrent writers also need serialization or an appropriate isolation/retry strategy. This current-state reduction deliberately does not preserve every intermediate change; an event-history target needs each distinct change retained.

### Deduplication, idempotency, and backfills

**Deduplication** chooses one record per declared identity. Define whether identity means business entity, business event, or source change. Two purchases by the same customer are not duplicates simply because their attributes match.

```sql
WITH ranked AS (
    SELECT order_id, status, is_deleted, source_version, ingestion_id,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY source_version DESC, ingestion_id DESC
           ) AS rn
    FROM landed_order_changes
)
SELECT order_id, status, is_deleted, source_version
FROM ranked
WHERE rn = 1;
```

This prepares a current-state batch under the same assumptions as the `MERGE`. `ingestion_id` is only a deterministic tie breaker for equivalent replays; conflicting payloads at one source version should fail validation. Do not use “latest per entity” when building a historical change log or a Type 2 history, because it discards intervening versions.

**Idempotency:** applying the same logical batch twice has the same intended effect as applying it once. Stable event IDs, version-aware upserts, and atomic partition replacement are common mechanisms. Appending the same aggregate twice or applying `balance = balance + delta` twice is not idempotent without separate deduplication.

**Backfill:** recompute a historical range after missing data or changed logic. Bound it by business time and dependency scope; preserve inputs and code versions; isolate resources; validate a replacement; then publish. Coordinate overlapping live runs so an old backfill cannot overwrite newer results. Replacing one day's raw data can require rebuilding a longer range for rolling windows or cumulative metrics.

### Schema evolution and data contracts

Technical compatibility and semantic compatibility are different. Adding a nullable field is often structurally compatible, but can still break a consumer using positional columns or `SELECT *`. Changing an amount from dollars to cents can preserve the SQL type while breaking every metric.

A useful **data contract** defines:

- Schema, nullability, grain, keys, and event identity.
- Field meaning, units, timezone, and allowed values.
- Ordering, delete behavior, history, and late-data policy.
- Delivery expectations, ownership, versioning, and deprecation rules.

Validate at ingestion and publication boundaries. Quarantine malformed or incompatible records with enough metadata for repair, protect sensitive payloads, and make rejected volumes visible. Automatic schema evolution should not silently accept a changed business meaning.

### Data reconciliation and validation

Reconciliation proves that source and target agree under the same scope and accounting rules. Match extraction positions, effective filters, timezone, grain, and delete handling before comparing totals.

| Check | Detects | Limitation |
| --- | --- | --- |
| Row and distinct-key counts | Missing records, duplicates | Equal counts can hide substitutions |
| Totals by date/source/currency | Missing value or incorrect allocation | Offsetting errors can cancel |
| Key-level anti-joins | Records missing on either side | Requires comparable identity and scope |
| Attribute comparison or hashes | Changed values | Requires canonicalization; hashes have collision risk |
| Referential and temporal checks | Orphans, overlapping history | Does not prove the source was complete |

Track input, accepted, rejected, duplicate, inserted, updated, and deleted counts per batch. Define tolerances only where business semantics justify them. A successful SQL statement proves execution, not reconciliation.

**Interview question: Why can `MAX(updated_at)` miss data?** A transaction may commit after extraction with an older timestamp, timestamps may tie or be imprecise, some updates may not change the field, and hard-deleted records disappear. Explain the source guarantees before selecting the cursor.

## 5. Query Processing & Optimization

### Columnar storage, compression, and encoding

Columnar storage groups values from the same column, so a query reading four columns can avoid decoding the other columns. Similar values also compress well.

| Technique | Idea | Useful pattern |
| --- | --- | --- |
| Dictionary encoding | Replace repeated values with small codes | Region or status strings |
| Run-length encoding | Store a value and its repetition count | Long sorted runs of equal values |
| Delta encoding | Store differences between nearby values | Ordered timestamps or IDs |
| Bit packing | Use only the bits required for a value range | Small integers |
| General compression | Compress encoded blocks | Many column types, with CPU/I/O tradeoffs |

Compression and encoding are related but distinct. Engines may execute on encoded or vectorized batches, reducing CPU work. Columnar storage is suited to analytical scans but does not make all point lookups, joins, or updates cheap. Updates can rewrite larger storage units depending on the engine.

### Partitioning, clustering, and sorting

- **Partitioning:** divides data into physical or logical subsets, often by date. It can support pruning, retention, and bounded replacement.
- **Clustering:** improves physical locality for selected keys. Its exact meaning differs across platforms.
- **Sorting:** orders rows within storage units, helping range filters, compression, or merge operations.

Partition on fields that align with common filters and maintenance boundaries, such as event date. Avoid excessive tiny partitions and files. Cluster or sort for selective recurring filters, while considering write and maintenance costs. Leading-key significance and supported key counts are platform-specific.

Storage partitioning, hash distribution across workers, and a streaming topic's partitions solve different problems. A date-partitioned table may still shuffle for a join on customer ID.

### Predicate pushdown and partition pruning

**Predicate pushdown** moves supported filtering toward the data source or scan. **Partition pruning** skips partitions proven irrelevant from partition metadata. **Column pruning** avoids unneeded columns. Block or file statistics can enable additional skipping within selected partitions.

```sql
SELECT product_sk, SUM(net_amount) AS revenue
FROM fact_sales
WHERE order_date >= DATE '2025-03-01'
  AND order_date < DATE '2025-04-01'
GROUP BY product_sk;
```

If `order_date` is the partition key, this form exposes a useful range. Wrapping a key in a function or using incompatible casts may prevent pruning, although some optimizers rewrite supported expressions. Verify bytes and partitions scanned instead of assuming a filter was effective. Filters on ingestion date and event date can select different rows when events arrive late.

### Join strategies and query execution plans

| Join strategy | Mechanism | Typical fit or risk |
| --- | --- | --- |
| Hash join | Build a hash table from one input and probe with the other | Equality joins; oversized build side can spill |
| Sort-merge join | Sort or reuse sorted inputs, then merge matching keys | Large equality joins; sorting can be expensive |
| Nested-loop join | Probe or compare one side for rows of the other | Small inputs or effective indexed lookup; otherwise expensive |

Broadcast and repartition describe **data movement**, not the same classification as hash or sort-merge join algorithms. A hash join can follow a broadcast or a repartition.

Read a plan from scans through exchanges, joins, and aggregates. Check estimated versus actual rows, filter selectivity, broadcast size, skew, spills, and repeated subqueries. `EXPLAIN` generally shows estimates; a profile or execution analysis supplies actual metrics. Commands that execute the query to collect metrics can still perform writes, so inspect their semantics before using them on DML.

Before tuning a join, prove key uniqueness and expected cardinality. A faster many-to-many join can still return incorrect totals.

### Materialized views and result caching

A **materialized view** persists query-derived data and has a refresh policy. It trades storage and refresh work for faster reads. Eligibility, incremental refresh, transparent rewrite, and staleness behavior vary by engine.

A **result cache** reuses a prior query result when the engine's eligibility and invalidation rules allow it. A **data cache** avoids rereading underlying blocks. A plan cache reuses planning work. They solve different costs.

Benchmark representative cold and warm behavior separately. A cached result can hide an inefficient scan. For any persisted aggregate, define refresh dependencies and maximum permitted staleness; fast stale answers can violate the reporting contract.

### Statistics, cardinality estimation, and CBO

A **cost-based optimizer (CBO)** compares candidate plans using estimated cardinality and resource cost. Statistics can include row counts, distinct counts, null fractions, histograms, and value ranges.

Bad estimates can select the wrong build side, broadcast too much data, or join large intermediates too early. Stale statistics, correlated columns, skew, and complex expressions are common causes. Confirm a material estimate/actual mismatch before refreshing statistics, changing physical design, or adding hints. Statistics collection is automatic in some systems and explicit in others.

**Interview question: How do you investigate a slow query?** Establish queue time versus execution time, inspect the profile, verify scan volume and joins, compare estimates with actuals, locate skew or spills, then change one justified factor and measure both runtime and cost.

## 6. Distributed DWH & Performance

### MPP execution, distribution, and shuffling

A distributed query typically performs local scans and partial aggregations, exchanges rows, then completes joins or final aggregations. Hash distribution places equal distribution keys together; even or round-robin distribution spreads rows without join-key locality; replication copies selected data to multiple workers.

**Shuffle** redistributes intermediate data across the network. It consumes serialization, network, memory, and sometimes disk resources. Reducing rows and columns before an exchange can save more than increasing compute size. Physical colocation can avoid some movement, but only when the engine recognizes compatible distributions.

### Broadcast vs. repartitioned joins

| Strategy | Data movement | Good fit | Main failure mode |
| --- | --- | --- | --- |
| Broadcast | Copy the small side to participating workers | A filtered dimension small enough for each worker | Compressed storage size hides a large in-memory hash table |
| Repartitioned | Redistribute inputs by join key | Two large inputs without suitable colocation | Network cost and skew on popular keys |
| Colocated | Join already compatible local partitions | Physical layout matches the join | Layout may favor one workload and hurt another |

As a rough model, broadcasting a dimension of size `D` to `W` workers transfers data on the order of `D × W`, while repartitioning can move much of the relevant inputs. Topology, existing distribution, filtering, and execution details change the actual volume. Decide from measured post-filter size and memory headroom, not row count alone.

### Data skew and hot partitions

**Skew** means uneven work distribution. One popular customer or a large null-key bucket can leave one worker processing far more data than others. A **hot partition** can also be a write bottleneck when many writers target the same physical partition.

Look for long-tail tasks, uneven bytes, spills concentrated on a worker, and maximum-to-median task duration. Match the remedy to the cause:

- Preaggregate when the metric allows it.
- Broadcast a genuinely small side.
- Separate known heavy hitters into a dedicated path.
- Use adaptive skew handling if available.
- Salt keys only when necessary and preserve join semantics; the other side must be expanded or coordinated correctly.

Adding workers does not fix a single indivisible hot key. Changing a storage partition key does not automatically fix join-key skew.

### Workload management, concurrency, and queuing

End-to-end latency includes queueing, compilation, execution, and result delivery. A query that runs quickly alone can miss its target under concurrency.

Use workload classes or pools for ingestion, interactive BI, scheduled reporting, and backfills. Control concurrency, priorities, timeouts, and resource limits. Too many concurrent queries can increase spills and make every query slower; a queue can protect throughput and tail latency. Measure p95 latency and throughput under a representative workload, not just the fastest isolated execution.

### Compute/storage separation and autoscaling

Separate persistent storage from compute so each can grow independently and multiple compute groups can serve shared data. This improves flexibility but still leaves shared metadata, network, ingestion, and storage limits.

- **Scale up:** increase resources for an execution group; can help CPU or memory constraints.
- **Scale out:** add workers or compute groups; can help parallelism or concurrency, depending on the product.
- **Autoscaling:** adjusts capacity based on policy and demand; startup delay, quotas, caches, and spend caps matter.

Do not assume adding independent clusters makes one query faster. In Snowflake, multi-cluster warehouses primarily address concurrency, while resizing a warehouse changes resources available to individual queries. [Snowflake: multi-cluster warehouses](https://docs.snowflake.com/en/user-guide/warehouses-multicluster).

### Cost-performance optimization

Optimize a unit of useful work: cost per successful pipeline run, per dashboard refresh, or per query meeting its latency target.

1. Remove unnecessary scans, joins, columns, and duplicate computation.
2. Repair physical layout and incremental processing where the workload supports them.
3. Right-size compute and concurrency from profiles.
4. Stop idle resources and schedule flexible workloads appropriately.
5. Include maintenance, storage retention, data transfer, retries, and operational effort.

Under compute-time pricing, doubling compute but halving runtime can leave compute cost roughly unchanged. Under bytes-scanned pricing, pruning can directly reduce query charges. Billing granularity and platform rules affect both examples; avoid universal price comparisons.

**Interview question: The query is fast in development and slow in production. Why?** Check scale, concurrency, queue time, skew, statistics, cache state, data distribution, and production security policies. Query text alone is insufficient evidence.

## 7. Data Quality & Governance

### Data quality dimensions and testing

| Dimension | Question | Example test |
| --- | --- | --- |
| Accuracy | Does it reflect the business reality? | Compare settled payments with the payment provider |
| Completeness | Is expected data present? | All expected source partitions and mandatory fields arrived |
| Uniqueness | Are identities duplicated? | One row per order-line business key |
| Validity | Does it obey defined rules? | Status belongs to the approved domain |
| Consistency | Do related representations agree? | Order totals match allocated lines under the same rules |
| Timeliness | Is it available when needed? | Source progress meets the freshness objective |

Test at multiple boundaries: contract checks at ingestion, invariants after transformation, reconciliation before publication, and freshness or volume monitoring during operation. Unit tests cover transformation logic on controlled inputs; data tests inspect actual datasets; integration tests cover the pipeline path.

```sql
SELECT source_system, order_id, order_line_id, COUNT(*) AS row_count
FROM fact_sales
GROUP BY source_system, order_id, order_line_id
HAVING COUNT(*) > 1;
```

An empty result passes this uniqueness check; it does not prove completeness or non-null keys. Add separate checks for required keys, missing partitions, referential integrity, Type 2 overlaps, and reconciliation. Thresholds should reflect expected behavior: a zero-row source can be legitimate on a holiday or evidence of a failed feed.

### Data lineage and metadata management

**Lineage** connects sources, transformations, models, and downstream consumers. Table-level lineage supports impact analysis; column-level lineage helps trace sensitive or derived attributes. Dynamic SQL, external scripts, and manual extracts can leave gaps.

**Metadata** includes technical schemas, operational run information, and business meaning. Keep source positions, batch IDs, code versions, ownership, classification, and transformation dependencies. Lineage helps find affected dashboards after a bad load; reproducibility also requires retained inputs and versioned rules.

### Data catalog, ownership, and contracts

A catalog makes assets discoverable and records definitions, ownership, classifications, and reliability expectations. Naming a table “certified” is useful only if a defined validation and ownership process supports it.

An owner is accountable for meaning, access decisions, quality expectations, and lifecycle. A steward may handle definitions and curation; an operating team may handle execution. Contracts make producer-consumer obligations explicit. Agree on who responds to an incident and who approves breaking semantic changes.

### PII protection, RBAC, and row/column security

- **Minimize and classify:** collect only needed sensitive fields and identify where they flow.
- **RBAC:** grant permissions through job-function roles using least privilege; separate administration, loading, and consumption duties where needed.
- **Row security:** restrict visible records, such as tenant or regional data.
- **Column security:** restrict or mask sensitive attributes such as email or salary.
- **Encryption and tokenization:** protect storage and transport; tokenize identifiers when direct identity is unnecessary.

Enforce controls at a trusted query or storage boundary. A dashboard filter is not a security boundary. An email hash is usually pseudonymous, not anonymous, and unsalted hashes can be guessed. Encryption at rest does not stop a user with authorized query access from reading plaintext.

Check service accounts, raw storage, exports, caches, derived tables, and logs, because each can expose data outside the intended view. Preserve required access controls when publishing materialized data. Test negative access cases with representative roles rather than validating only successful reads.

### Auditing, compliance, and retention

Audit access, privilege changes, exports, pipeline changes, and important data mutations. Protect logs against unauthorized modification and avoid putting sensitive payloads in them.

Retention defines how long each data class and its copies remain available. Coordinate warehouse tables, raw landing, snapshots, backups, caches, and downstream extracts. A business delete event, a privacy erasure request, and expiration under a retention policy have different meanings. Legal requirements and legal holds need review by the responsible policy or legal team; no single retention period fits every dataset.

**Interview question: How do you protect tenant data?** Establish a trusted tenant identity, enforce row and object access, include tenant identity in joins and keys where required, restrict exports and raw access, and test for cross-tenant leakage. Resource isolation is a separate requirement.

## 8. Modern DWH & Analytics

### Snowflake, BigQuery, and Redshift architectures

| Platform | Architecture and execution | Practical design focus |
| --- | --- | --- |
| Snowflake | Shared persistent storage; independent MPP virtual warehouses; cloud services for coordination and metadata | Warehouse sizing and concurrency; micro-partition pruning; workload separation |
| BigQuery | Serverless distributed analytics with separate storage and compute; slots represent compute capacity | Partitioning, clustering, bytes scanned, slot capacity, and reservations |
| Redshift | MPP execution with leader/compute-node concepts for provisioned clusters; RA3 managed storage separates storage growth from compute; Serverless offers managed capacity | Distribution, sort design, workload management, capacity, and movement |

Snowflake's three layers are storage, compute, and cloud services. [Snowflake architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts).

BigQuery separates storage and compute and offers managed columnar storage. Serverless does not mean that capacity, contention, and query design no longer matter. [BigQuery overview](https://docs.cloud.google.com/bigquery/docs/introduction).

For Redshift, distinguish provisioned-cluster concepts from the Serverless namespace/workgroup model; older local-storage-only descriptions do not explain RA3 managed storage. [Redshift system architecture](https://docs.aws.amazon.com/redshift/latest/dg/c_high_level_system_architecture.html), [provisioned clusters and managed storage](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-clusters.html), [Redshift Serverless](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-whatis.html).

These products support overlapping workloads. Compare the actual data sizes, query mix, update patterns, concurrency, access controls, operational skills, and measured cost. Do not assume SQL syntax, constraint enforcement, or transaction scope transfers unchanged between them.

### dbt: models, materializations, tests, and snapshots

dbt manages analytical transformation code and its dependency graph. It generally operates after data has been loaded; it is not a replacement for source extraction or log-based CDC.

A SQL model describes a dataset. `ref()` resolves another model and records a dependency; `source()` references declared source data. A materialization determines how the result is represented:

| Materialization | Behavior | Tradeoff |
| --- | --- | --- |
| View | Stores a query definition | Fresh query-time evaluation, repeated compute |
| Table | Rebuilds a persisted result on a run | Fast reads, rebuild work and between-run staleness |
| Incremental | Updates a persisted table from a selected change set | Efficient updates, author owns change selection and recovery correctness |
| Ephemeral | Compiles into consuming queries, commonly as CTEs | No separately queryable relation; repeated logic can enlarge queries |
| Materialized view | Uses supported warehouse-managed materialized views | Refresh and feature support depend on the adapter and engine |

An incremental `unique_key` config tells an applicable strategy how to match rows; it does not itself prove uniqueness or retrieve missing late records. Strategies such as merge and partition overwrite depend on the adapter. [dbt materializations](https://docs.getdbt.com/docs/build/materializations), [incremental models](https://docs.getdbt.com/docs/build/incremental-models).

**Data tests** check properties of stored data. Common generic tests include `not_null`, `unique`, `accepted_values`, and `relationships`; singular tests are SQL queries returning failing rows. Tests do not automatically quarantine bad rows or create enforced database constraints. Unit tests exercise model logic on controlled inputs. [dbt data tests](https://docs.getdbt.com/docs/build/data-tests), [dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests).

**Snapshots** record observed changes in mutable data using Type 2-style validity metadata. Timestamp or check strategies detect changes between runs. If a source changes A → B → C between snapshots, only A and C may be observed; snapshots do not reconstruct the missing B. Delete handling and configuration depend on the dbt version and strategy. [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots).

### Semantic layers

A semantic layer centralizes business entities, relationships, dimensions, and metric definitions so tools calculate the same concept consistently. For revenue, define the qualifying event, returns treatment, currency basis, time dimension, and aggregation behavior.

A metric definition alone is insufficient if joins introduce fanout. Relationships need cardinality and entity keys; metrics need valid grains and time semantics. A semantic layer can generate queries over governed models, but it depends on the quality and security of those models. dbt's Semantic Layer uses semantic models and metric definitions to serve consistent metrics. [dbt Semantic Layer](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl).

### Medallion architecture: Bronze, Silver, and Gold

| Layer | Typical responsibility | Store example |
| --- | --- | --- |
| Bronze | Retain source observations with ingestion metadata | Received order events, including duplicates |
| Silver | Validate, deduplicate, standardize, and integrate | Typed orders and consistent customer identities |
| Gold | Publish business-ready models and aggregates | Sales fact, customer dimension, daily revenue |

Medallion names describe refinement stages rather than a required technology. A Bronze layer can still apply minimum structural and security checks. Gold can contain dimensional tables or wide aggregates; it need not be one giant table. Define actual contracts and publication gates because the color alone does not guarantee quality. [Databricks: medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion).

### Data Mesh vs. centralized DWH

**Data Mesh** is an organizational and architectural approach emphasizing domain ownership, data as a product, a self-service platform, and federated computational governance. A centralized DWH concentrates integration or operating responsibility in a central team.

Mesh is not a storage format and does not require one database per domain. Domain-owned data products can use a shared warehouse. Shared identity, contracts, discoverability, security, and cross-domain metric definitions still need coordination. A central team can provide the platform while domains own meaning and quality. [Zhamak Dehghani: Data Mesh principles](https://martinfowler.com/articles/data-mesh-principles.html).

Mesh can reduce a central team's domain bottleneck, but introduces coordination and product ownership demands. Choose it based on organizational needs and capability, rather than assuming decentralization improves every pipeline.

### Lakehouse integration and open table formats

Parquet is a file format. Iceberg, Delta Lake, and Hudi add table metadata and transactional management over data files, with different capabilities and engine support.

| Format | Interview-level distinction | Operational concern |
| --- | --- | --- |
| Apache Iceberg | Snapshot-based table metadata, field IDs, schema evolution, and partition evolution | Catalog and commit coordination; reader/writer compatibility |
| Delta Lake | Transaction log describing table versions and changes | Protocol and feature support across engines; file and log maintenance |
| Apache Hudi | Record-level mutation and incremental processing with table services | Indexing, compaction, clustering, and query-type choices |

Iceberg can evolve supported schemas and partition specifications without rewriting all existing data files; old and new partition layouts can coexist. This does not remove the need for consumer compatibility. [Iceberg evolution](https://iceberg.apache.org/docs/latest/evolution/).

Delta Lake supplies transaction management and table versioning over lake data. [Delta Lake documentation](https://docs.delta.io/).

Hudi emphasizes upserts, deletes, incremental queries, and services such as compaction. [Hudi overview](https://hudi.apache.org/docs/overview/).

A warehouse may query these tables externally, manage them natively, or ingest them into internal tables. Check read versus write support, supported table features, credential boundaries, and whether query engines share the same catalog. A transaction on one table format does not automatically make a multi-table transformation atomic. Plan compaction, snapshot expiration, orphan cleanup, and retention with active readers and recovery requirements in mind.

**Interview question: Does adopting a lakehouse eliminate modeling?** No. Transactional storage manages table state; the team must still define grain, identifiers, relationships, metric semantics, history, access, and quality.

## 9. Production & Advanced Topics

### Historical corrections and restatements

A **correction** changes an erroneous historical assertion. A **restatement** republishes a derived result using corrected data or rules. A business reversal, such as a refund, can be a new event rather than an overwrite; accounting and reporting requirements determine the representation.

A controlled restatement process:

1. Identify affected keys, effective dates, reporting periods, and dependent models.
2. Preserve original inputs and rule versions where required.
3. Recompute into isolated tables or partitions.
4. Reconcile corrected results, including allocations and downstream aggregates.
5. Publish through an atomic switch or a coordinated versioned release.
6. Record the reason, version, and consumer impact.

State whether reports show “as originally reported” or “latest corrected” history. If both are required, keep publication versions or sufficient temporal history. A correction to March revenue may also affect quarter totals, rolling metrics, and cumulative balances.

### Late-arriving and out-of-order data

Late means arrival after an expected boundary; out-of-order means arrival order differs from source or business order. A change can be out-of-order without being very late.

For current-state targets, compare source versions per entity and prevent older changes from replacing newer ones. For event-history targets, preserve distinct events and order them by the required semantics. For business-effective dimensions, source commit order does not necessarily equal effective-time order.

A bounded lateness policy might keep recent windows open, route older events to a repair queue, and periodically reconcile closed periods. Measure lateness distribution before choosing the bound. Records outside a lookback window need an explicit policy; silently discarding them is a correctness decision, not just a performance optimization.

### Exactly-once effects and transactional consistency

**Exactly-once effects** means each logical event affects the intended target once, despite retries. It is an end-to-end property with a defined boundary, not a universal guarantee from an orchestrator or `MERGE` statement.

Common building blocks are durable source positions, stable event identity, deduplication, transactional publication, and checkpoints tied to publication. With an external checkpoint, a useful crash sequence is:

```text
1. Read batch B.
2. Commit B to the target.
3. Crash before checkpointing.
4. Read B again after recovery.
5. Reapplying B produces the same target state.
6. Commit the checkpoint.
```

This works only if replay is truly idempotent. Target writes plus an external email, payment, or API call are not automatically one transaction. Use an outbox and consumer-side idempotency where such side effects are required.

**Transactional consistency** also concerns readers: a dashboard must not combine a refreshed fact with an incompatible old dimension. Use a supported multi-table transaction, consistent release identifiers, or versioned views. Distinguish statement atomicity, transaction isolation, source snapshot consistency, and cross-system consistency; one does not imply all the others.

### Zero-downtime schema migrations

Use an **expand–migrate–contract** sequence:

1. Add the new nullable field, table, or versioned interface.
2. Deploy writers that populate compatible old and new representations as needed.
3. Backfill historical values and validate meaning, counts, and permissions.
4. Move consumers to the new interface and monitor usage.
5. Remove the old representation after the agreed compatibility period.

For a rename, an alias or compatibility view can protect consumers during transition. For unit changes, publish an explicit new field such as `amount_cents` rather than silently changing `amount`. Atomic table swaps and view replacement have engine-specific behavior; check grants, dependencies, locks, and stream or CDC consumers before relying on them.

### SLA/SLO, freshness, and observability

| Term | Meaning | Example |
| --- | --- | --- |
| SLI | Measured indicator | Proportion of expected hourly partitions published on time |
| SLO | Internal reliability target | 99% of expected hourly partitions available within 30 minutes |
| SLA | Agreement with defined obligations and consequences | Consumer service agreement for a daily publication deadline |

Distinguish schedule success, ingestion lag, transformation lag, publication freshness, and completeness. `MAX(event_time)` can look fresh because of one future-dated record while most expected data is missing. A source heartbeat or commit position plus coverage checks often gives a stronger signal.

Track source lag, input/output volumes, rejected rows, duplicates, schema changes, reconciliation deltas, query failures, queue time, and cost. Alert on consumer impact and sustained breaches, with an owner and recovery procedure. A green scheduler is not evidence that business data arrived.

### Disaster recovery, time travel, and rollback

- **RPO (recovery point objective):** maximum tolerable data loss measured as time.
- **RTO (recovery time objective):** maximum tolerable time to restore the required service.
- **Time travel:** query or restore retained historical table states within platform limits.
- **Rollback:** restore a prior acceptable state or publish a compensating correction.
- **Disaster recovery:** restore service after a larger failure, including the data, code, permissions, catalog, orchestration, secrets references, and dependencies.

Time travel can help recover a bad write; it is not a substitute for an independently recoverable backup or a regional recovery plan. Availability depends on retention and surviving metadata and files. Storage snapshots do not reconstruct upstream logs that have expired.

Test restores and failovers against the RPO/RTO, including source replay and access controls. After restoring an older target, reset or reconcile checkpoints: leaving a checkpoint ahead of restored data can permanently skip records. Rewinding it without idempotency can create duplicates. Confirm platform-specific recovery behavior before an incident. [Snowflake: Time Travel](https://docs.snowflake.com/en/user-guide/data-time-travel), [BigQuery: time travel](https://docs.cloud.google.com/bigquery/docs/time-travel), [Redshift: snapshots](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-snapshots.html).

### Multi-tenant workload isolation

Separate **data isolation** from **resource isolation**. Row security may prevent tenant A from reading tenant B's data while A's expensive query still delays B.

| Deployment shape | Advantage | Tradeoff |
| --- | --- | --- |
| Shared tables with tenant keys and enforced policies | Efficient shared operation | Policy, join, and export mistakes can expose data; workloads contend |
| Separate schemas or datasets | Clearer object ownership and lifecycle | More objects and grants; compute may still be shared |
| Separate compute pools | More predictable resource use | More capacity planning and potentially idle cost |
| Separate accounts/projects or stronger boundaries | Greater administrative isolation | Higher provisioning, governance, and cross-tenant reporting effort |

Enforce trusted tenant identity, scoped keys, and least privilege. Route workloads to appropriate pools, apply quotas and timeouts, and measure per-tenant utilization and tail latency. Choose isolation based on contractual requirements and measured contention. [BigQuery: multi-tenant workload practices](https://docs.cloud.google.com/bigquery/docs/best-practices-for-multi-tenant-workloads-on-bigquery).

**Interview question: A pipeline failed after writing half its outputs. What do you do?** Establish the committed boundary, prevent publication of an inconsistent release, identify durable source inputs, replay or rebuild idempotently, reconcile, publish, and advance progress. Avoid rerunning blind while assuming partial writes disappeared.

## 10. Interview Practice

### A structure for architecture answers

1. **Clarify requirements:** business questions, sources, scale, latency, history, sensitivity, and budget.
2. **Declare grain and identity:** what each record means and how changes are ordered.
3. **Choose the flow:** extraction, landing, integration, serving, and publication.
4. **Prove correctness:** duplicates, deletes, temporal lookup, retries, late data, and reconciliation.
5. **Explain performance:** scans, pruning, distribution, joins, concurrency, and cost.
6. **Explain operations:** ownership, quality gates, access, monitoring, backfills, and recovery.
7. **State a tradeoff:** what the design makes easier and what it makes more expensive.

### Questions to rehearse

| Question | What a strong answer includes |
| --- | --- |
| Design a sales warehouse | Order-line grain, measures, currencies, customer/product/date dimensions, returns, and history |
| Why are totals too high after a join? | Key cardinality, mixed grain, bridge allocation, and overlapping SCD versions |
| Which SCD type should region use? | Whether reporting needs event-time region, current region, or both |
| How do you capture deletes incrementally? | CDC tombstones or an explicit deletion/reconciliation mechanism |
| How do retries avoid duplicate revenue? | Stable identities, idempotent writes, versioning, and checkpoint boundaries |
| How do you replay six months of data? | Retained inputs, dependency range, isolation, reconciliation, and coordinated publication |
| Why did adding workers not help? | Serial bottlenecks, skew, colocation, network, queueing, and inappropriate scale-out |
| When would you choose Data Vault? | Integration and audit requirements weighed against modeling and serving complexity |
| Does `unique_key` guarantee unique dbt rows? | Matching configuration versus explicit tests and source deduplication |
| How do you recover a bad deployment? | Compatibility, versioned outputs, restore boundaries, checkpoint reconciliation, and downstream repair |

### Common claims to challenge

- “OLAP systems cannot provide transactions.” Workload type and transaction guarantees are separate properties.
- “A surrogate key fixes duplicates.” Identity quality and deduplication still require rules.
- “An SCD2 table always preserves what we knew at the time.” That requires recorded-time semantics as well as business validity.
- “Incremental means `updated_at > MAX(updated_at)`.” That assumes source guarantees that often do not hold.
- “`MERGE` makes a pipeline exactly once.” Extraction, identity, ordering, publication, and recovery must agree.
- “A table format makes the whole lake transactional.” Transaction scope and engine compatibility must be checked.
- “The job succeeded, so the data is correct.” Execution success does not prove freshness, completeness, or reconciliation.
- “Serverless removes capacity planning.” Concurrency, quotas, reservation choices, and cost remain relevant.
