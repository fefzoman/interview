# Data Engineer Interview Preparation

Interview guides grouped by subject. Start with foundations, then follow the topic tree below. Choose cloud and platform guides to match your target role; study AI when relevant to the interview.

## Topic tree

- **foundations/**
  - [Core engineering concepts](foundations/core.md)
  - [Python](foundations/python.md)
  - [SQL](foundations/sql.md)
  - [Database engine internals](foundations/database.md)
- **ingestion/**
  - [Data ingestion patterns](ingestion/ingestion.md)
  - [Replication and CDC](ingestion/replication.md)
  - [Kafka](ingestion/kafka.md)
- **processing/**
  - [Spark](processing/spark.md)
  - [Flink and stream processing](processing/flink.md)
- **storage-and-modeling/**
  - [Data warehousing and modeling](storage-and-modeling/dwh.md)
  - [Iceberg](storage-and-modeling/iceberg.md)
  - [ClickHouse](storage-and-modeling/clickhouse.md)
  - [DuckDB](storage-and-modeling/duckdb.md)
  - [MongoDB](storage-and-modeling/mongodb.md)
  - [Semantic layer and metrics](storage-and-modeling/semantic-layer.md)
- **pipelines/**
  - [Airflow orchestration](pipelines/airflow.md)
  - [dbt transformations](pipelines/dbt.md)
- **quality-and-governance/**
  - [Data contracts and schemas](quality-and-governance/data-contracts.md)
  - [Data quality and observability](quality-and-governance/data-quality.md)
  - [Data platform testing](quality-and-governance/testing.md)
  - [Governance and security](quality-and-governance/governance.md)
- **cloud-and-platforms/**
  - [AWS](cloud-and-platforms/aws.md)
  - [GCP](cloud-and-platforms/gcp.md)
  - [Databricks](cloud-and-platforms/databricks.md)
  - [Snowflake](cloud-and-platforms/snowflake.md)
- **system-design/**
  - [Data platform system design](system-design/data-platform-system-design.md)
  - [Data / Enterprise Architect](system-design/data-enterprise-architect.md)
  - [Cost engineering and FinOps](system-design/finops.md)
- **ai/**
  - [AI data engineering](ai/ai.md)

## Where to review each concept

Read shared concepts in the main guide below, then use product guides for implementation and failure scenarios. A related topic appearing in a product guide applies the concept to that system; it does not need a second full introduction.

| Concept | Main guide | Continue with |
| --- | --- | --- |
| Distributed systems, cross-system transactions, storage, performance, and reliability | [Core engineering](foundations/core.md) | Spark, Flink, Kafka, and system design |
| Storage engines, indexes, WAL, ACID, MVCC, isolation, query planning, and execution | [Database internals](foundations/database.md) | SQL and database-specific guides |
| Python language, runtime, concurrency, and Python testing | [Python](foundations/python.md) | Pipeline implementation examples |
| SQL semantics, analytical queries, query rewrites, and transaction statements | [SQL](foundations/sql.md) | dbt and warehouse-specific SQL |
| API/file extraction, cursors, retries, ingestion identity, and replay | [Ingestion](ingestion/ingestion.md) | Replication for database CDC; Kafka for broker behavior |
| Database CDC, snapshot handoff, source positions, replication lag, and cutover | [Replication](ingestion/replication.md) | Source database and target platform guides |
| Warehouse architecture, grain, facts, dimensions, and historical modeling | [Data warehousing](storage-and-modeling/dwh.md) | dbt, Iceberg, and platform guides |
| Metric definitions, aggregation, semantic joins, and MetricFlow | [Semantic layer](storage-and-modeling/semantic-layer.md) | Warehouse models that supply the metrics |
| Schema formats, registries, compatibility, evolution, and contract tests | [Data contracts](quality-and-governance/data-contracts.md) | Governance for approvals; testing for pipeline validation |
| Quality dimensions, monitoring, reconciliation, publication gates, and lineage tools | [Data quality](quality-and-governance/data-quality.md) | dbt for dbt test syntax; testing for fixtures and failure injection |
| Test levels, fixtures, replay verification, checksums, load tests, and failure injection | [Platform testing](quality-and-governance/testing.md) | Python and dbt for tool-specific test implementation |
| Catalogs, ownership, lineage concepts, access, privacy, retention, and audits | [Governance](quality-and-governance/governance.md) | Cloud and platform guides for enforcement |
| Cost models, sizing economics, budgets, and cost attribution | [FinOps](system-design/finops.md) | Platform-specific resource and query costs |
| Requirements, capacity estimates, component choices, and end-to-end failure handling | [System design](system-design/data-platform-system-design.md) | The worked design after recalling the component guides |
| Enterprise frameworks, MDM, domain ownership, architecture evaluation, and technology strategy | [Data / Enterprise Architect](system-design/data-enterprise-architect.md) | Governance, data contracts, semantic modeling, system design, and FinOps |

## Recall flow before an interview

Follow one dataset from source to consumer. At each step, recall the concepts without opening the guide, explain a trade-off and a failure scenario, then review only what you missed.

1. **Foundations:** solve a SQL query and a short Python task; recall joins, window functions, indexes, transactions, and distributed systems basics.
2. **Ingestion:** explain batch, streaming, and CDC; trace offsets, retries, deduplication, schema changes, and replay.
3. **Processing:** walk through a Spark or Flink job; recall partitioning, shuffles, skew, state, watermarks, and recovery.
4. **Storage and modeling:** choose a warehouse or lakehouse layout; explain grain, facts and dimensions, partitioning, table formats, and query performance.
5. **Pipelines:** connect ingestion, transformation, and serving; explain dependencies, incremental runs, idempotency, backfills, and orchestration.
6. **Quality and governance:** explain how you validate results, detect failures, enforce contracts, trace lineage, and protect sensitive data.
7. **Cloud and platforms:** map the flow to the stack in the job description; recall service choices, operational limits, and cost drivers. Review AI topics if the role requires them.
8. **System design:** sketch the complete flow from memory; state volume, freshness, reliability, and cost requirements, then defend your choices.
9. **Final recall:** revisit the gaps you noted and rehearse project examples covering a design decision, a production incident, and a measurable improvement.
