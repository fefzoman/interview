# Data Contracts & Schema Management: Data Engineer Interview Guide

A data contract defines what a producer promises to deliver and what consumers can rely on. A schema describes the shape of that data; the wider contract also defines meaning, ownership, quality, delivery expectations, and how changes are introduced.

For interviews, connect every compatibility rule to a reader, a writer, and a deployment order. Use an order-events pipeline as a running example: an order service publishes events, and billing, analytics, and fraud detection consume them independently.

Examples are illustrative. Schema Registry details below refer to Confluent where named; other registries and serializer versions can behave differently.

## Contents

1. [Producer-Consumer Contracts & Schema Ownership](#1-producer-consumer-contracts--schema-ownership)
2. [Avro, Protobuf & JSON Schema](#2-avro-protobuf--json-schema)
3. [Schema Registry, Schema IDs & Serialization](#3-schema-registry-schema-ids--serialization)
4. [Backward, Forward & Full Compatibility](#4-backward-forward--full-compatibility)
5. [Schema Evolution, Breaking Changes & Versioning](#5-schema-evolution-breaking-changes--versioning)
6. [Structural vs. Semantic Contracts](#6-structural-vs-semantic-contracts)
7. [Contract Validation & CI/CD Enforcement](#7-contract-validation--cicd-enforcement)
8. [Contract Testing & Consumer-Driven Contracts](#8-contract-testing--consumer-driven-contracts)
9. [Interview Scenarios & Quick Reference](#9-interview-scenarios--quick-reference)

## 1. Producer-Consumer Contracts & Schema Ownership

### What belongs in a data contract?

| Contract element | Example for order events | Failure it helps prevent |
| --- | --- | --- |
| Identity and scope | `OrderPlaced`, one event per accepted order | Confusing an order snapshot with a line item or status update |
| Structure | `order_id` is a string; `amount_minor` is an integer | Parsing errors and incompatible field types |
| Meaning | Amount is the total charged, including tax, in the currency's minor unit | Incorrect revenue despite valid types |
| Keys and delivery | `event_id` identifies an event; delivery can contain duplicates | Double counting after retries |
| Time | `occurred_at` is business event time in UTC; ingestion time is separate | Incorrect windows and freshness calculations |
| Quality and service levels | Required IDs; documented freshness and completeness targets | Silent missing or late data |
| Ownership and access | Orders team owns production; billing consumes; access is restricted | Unowned failures and unintended disclosure |
| Change process | Compatibility policy, notice period, deprecation owner | Uncoordinated deployments |

A useful contract is version controlled, discoverable, and checked where feasible. A document with no owner or enforcement can explain expectations but cannot prevent violations.

Contracts apply to topics, APIs, files, warehouse tables, and published datasets. For a batch table, define grain, keys, partition meaning, completion signals, freshness, and how corrections replace or update previous results.

### Producer and consumer responsibilities

The **producer** owns the published interface: emitting valid data, preserving its meaning, announcing changes, and responding to violations. Internal database changes should not automatically become public interface changes. A stable event model or published view can isolate consumers from source implementation details.

The **consumer** owns its processing assumptions: handling documented nulls, defaults, duplicates, unknown values, and delivery behavior. It declares which fields and guarantees it needs rather than silently depending on everything the producer happens to emit.

A pipeline that reads one dataset and publishes another is both a consumer and a producer. Its transformation must preserve the upstream guarantees it relies on and establish the downstream guarantees it advertises.

### Who owns the schema?

| Role | Responsibility |
| --- | --- |
| Domain or producer team | Own schema meaning, implementation, release, and deprecation |
| Consumer teams | Publish requirements, test compatibility, and migrate usage |
| Platform team | Operate registry, access controls, tooling, and shared enforcement |
| Governance or security owners | Define classification, retention, and access requirements |

Ownership needs an accountable team and an escalation path. Consumers influence the contract; that does not require every consumer to approve every additive change. Define when review is required, particularly for breaking or semantic changes.

**Interview question:** Who should own a Kafka event schema? The producing domain usually owns the public interface and its meaning. Platform tooling enforces shared rules, while consumers make their dependencies visible and verify changes against them.

## 2. Avro, Protobuf & JSON Schema

### Comparing the formats

| Aspect | Avro | Protobuf | JSON Schema |
| --- | --- | --- | --- |
| Schema representation | JSON schema document | `.proto` definitions | JSON document describing valid JSON instances |
| Typical payload | Compact binary records | Compact binary messages with numeric field tags | JSON text; JSON Schema itself is not a serialization encoding |
| Application integration | Generic records or generated types | Commonly generated types | JSON objects plus a validator |
| Evolution focus | Writer-reader schema resolution | Stable field numbers and wire types | Changes to the set of accepted documents |
| Common use | Data pipelines and event interchange | Service messages and typed APIs | JSON events, APIs, and configuration validation |

Choose based on existing clients, ecosystem support, debugging needs, and evolution requirements. Payload size and speed depend on actual data, encoding, compression, and implementation; a format name alone does not determine performance.

### Avro: writer schema, reader schema, and defaults

An example Avro v2 record adds a nullable field with a default to a v1 record that contained only `order_id`:

```json
{
  "type": "record",
  "name": "OrderPlaced",
  "namespace": "interview.orders",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "coupon_code", "type": ["null", "string"], "default": null}
  ]
}
```

When a v2 reader reads v1 data, it supplies `null` for the missing `coupon_code`. A v1 reader ignores that extra field in v2 data. Nullability alone does not provide a missing-field default. Defaults support reading older records; they do not make a field optional in the Avro encoding. Libraries may populate defaults during object construction, so distinguish library convenience from the format rule. [Avro specification](https://avro.apache.org/docs/1.12.0/specification/).

### Protobuf: field numbers are part of the contract

```proto
syntax = "proto3";
package interview.orders;

message OrderPlaced {
  string order_id = 1;
  int64 amount_minor = 2;
  string currency = 3;
  optional string coupon_code = 4;
  reserved 5;
  reserved "legacy_customer_code";
}
```

Binary Protobuf identifies fields by number. Preserve those numbers and reserve deleted ones; reserving names also protects future schema edits. `optional` provides explicit presence, allowing code to distinguish an absent coupon from a present empty string. An ordinary proto3 scalar with implicit presence returns its type default when absent.

Adding a field is generally binary wire safe, but application assumptions can still break. For example, adding an enum value can invalidate an exhaustive switch. Binary unknown-field behavior and ProtoJSON behavior differ; do not transfer binary compatibility guarantees to JSON. [Protobuf language guide](https://protobuf.dev/programming-guides/proto3/), [ProtoJSON compatibility](https://protobuf.dev/programming-guides/json/).

### JSON Schema: presence, null, and additional properties

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "order_id": {"type": "string", "minLength": 1},
    "amount_minor": {"type": "integer", "minimum": 0},
    "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
    "coupon_code": {"type": ["string", "null"]}
  },
  "required": ["order_id", "amount_minor", "currency"],
  "additionalProperties": false
}
```

`properties` constrains a property when present; `required` controls presence. Here `coupon_code` can be absent or explicitly null. `additionalProperties: false` rejects undeclared fields, so an old validator rejects a new payload containing an added field. An open schema permits extra properties by default, but later constraining a previously unconstrained property can also reject old data. [JSON Schema object reference](https://json-schema.org/understanding-json-schema/reference/object).

`default` is an annotation, not a standard instruction to insert missing values. A currency pattern checks spelling shape, not whether the currency exists or whether its monetary units are correct. Pin the schema dialect and validator configuration; do not assume every implementation enforces `format` identically. [JSON Schema annotations](https://json-schema.org/understanding-json-schema/reference/annotations), [format validation](https://json-schema.org/understanding-json-schema/reference/type#format).

**Interview trap:** “Adding an optional field is always safe.” It depends on the format, content model, reader behavior, registry policy, and consumer code.

## 3. Schema Registry, Schema IDs & Serialization

### What the registry stores

A registry manages schemas and compatibility policy. In Confluent terminology, a **subject** is a named schema lineage, a **version** is a position within that subject, and a **schema ID** identifies a registered schema within the registry's scope. Reusing a schema can reuse its ID across subjects; subject version numbers are not global IDs.

With the common topic naming strategy, `orders-value` and `orders-key` are separate subjects. Record-based strategies change which schemas share a compatibility boundary. A subject is therefore not necessarily a topic, and selecting a naming strategy is part of interface design.

### Serialization and deserialization path

```text
Producer object + selected schema
    -> serializer looks up or registers schema, according to configuration
    -> serializer encodes payload and schema identification
    -> Kafka stores bytes
    -> consumer deserializer resolves schema and decodes payload
    -> consumer applies business rules
```

The classic Confluent integer-ID payload prefix is:

```text
Avro / JSON Schema: [magic byte: 0][schema ID: 4 bytes][payload]
Protobuf:          [magic byte: 0][schema ID: 4 bytes][message indexes][payload]
```

The ID uses big-endian byte order; Protobuf message indexes select a message within its schema. This is a specific framing convention, not a universal property of these formats. Avro container files embed a writer schema rather than using this registry prefix. [Confluent serialization formats](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html), [Avro container files](https://avro.apache.org/docs/1.12.0/specification/#object-container-files).

The consumer must resolve the schema associated with the message, rather than blindly use the latest subject schema. Avro resolution can then map the writer's representation into the reader's expected schema.

### Operational consequences

- **Registry availability:** serializers and deserializers commonly cache schemas. An outage may spare cached paths while breaking a cold start or an unseen schema lookup; test the actual client behavior.
- **Replay:** retain schema definitions and referenced schemas for as long as retained data may need them. Deleting schema metadata can make historical messages unusable.
- **Environment migration:** IDs in development and production need not match. Copying raw messages with embedded IDs requires preserving or translating the schema mapping.
- **Registration control:** a production application can use preapproved schemas instead of automatically registering arbitrary new versions. Restrict registration and compatibility-policy changes to authorized identities.

A registry compatibility check compares schema definitions. It does not prove that every actual record is valid or that a producer used the expected serializer. Broker-side features, where available and enabled, have their own scope; an ID check is not proof of business correctness.

**Interview question:** Is a schema ID the same as an event version? No. An ID locates schema metadata. A subject version tracks registry history. A business event version describes the public interface, including meaning, and may change independently.

## 4. Backward, Forward & Full Compatibility

### Start with reader and writer direction

Let `W1` and `W2` write old and new data; let `R1` and `R2` be old and new readers.

| Mode | Required direction | Typical rollout consequence |
| --- | --- | --- |
| Backward | `R2` reads `W1` | Upgrade consumers before producers |
| Forward | `R1` reads `W2` | Upgrade producers before consumers |
| Full | Both directions | Supports mixed schema versions in either order |
| None | No compatibility guarantee enforced | Requires an explicit migration strategy |

Backward looks back at previously written data. Forward lets existing readers tolerate future writes. Full requires both. These directions concern schema readability; safe application behavior still needs testing. [Confluent compatibility definitions](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html).

Backward alone does not guarantee that an old consumer can process new events during a producer-first deployment. Forward alone does not guarantee that a new consumer can replay old data. Full does not guarantee that every historical version works unless the required history is included in the checks.

### Avro change examples

Assume unchanged record identity and other fields, with normal Avro reader-writer resolution:

| Change from v1 to v2 | Backward? | Forward? | Reason |
| --- | --- | --- | --- |
| Add a field with a valid default | Yes | Yes | New reader supplies default; old reader ignores extra field |
| Add a field without a default | No | Yes | New reader cannot supply a value for old records |
| Remove a field that had no default in v1 | Yes | No | Old reader cannot fill its now-missing field |
| Remove a field that had a valid default in v1 | Yes | Yes | Old reader fills the missing field |
| Change `int` to `long` | Yes | No | Reader can promote `int` to `long`, not the reverse |

These are Avro examples, not a shared rule table for every format. A nullable union without a default still fails the missing-field case. [Avro schema resolution](https://avro.apache.org/docs/1.12.0/specification/#schema-resolution).

### Nontransitive vs. transitive checks

For Confluent compatibility modes, nontransitive checks compare a candidate with the latest registered version. Transitive modes compare it with all earlier versions in the subject.

An illustrative backward-compatibility chain:

1. **v1:** `customer_id` is a required string without a default.
2. **v2:** the field is removed. A v2 reader ignores that field in v1 data.
3. **v3:** the field returns as an integer with a default of `0`. A v3 reader can read v2 data using the default.
4. A v3 reader cannot read v1's string as an integer. Both adjacent backward checks can pass while the historical replay fails.

`BACKWARD_TRANSITIVE` detects that historical conflict. Replay and rollback requirements should determine the versions tested; latest-version compatibility alone is insufficient for a long-lived event log.

### Compatibility is more than decoding

Distinguish **wire compatibility**, **source/API compatibility**, and **business compatibility**. A message can decode while generated client code stops compiling, a warehouse cast fails, or a dashboard calculates a different business metric.

**Interview question:** Which mode fits a consumer that must replay years of events? Explain the need for backward compatibility across the retained history, plus historical fixtures and consumer tests. The mode alone does not preserve old schemas, transformations, or business meaning.

## 5. Schema Evolution, Breaking Changes & Versioning

### What can break a consumer?

| Change | Potential failure |
| --- | --- |
| Rename or remove a used field | Lookup fails, defaults hide missing data, or generated API changes |
| Change type, precision, or scale | Decode failure, overflow, rounding, or invalid warehouse mapping |
| Add an enum value | Unknown branch, rejected row, or incorrect fallback |
| Change nullability or required fields | Existing records fail validation or violate consumer assumptions |
| Change currency units or timestamp meaning | Valid values produce incorrect calculations |
| Change key, grain, or event identity | Deduplication, joins, partitioning, and aggregation change |
| Stop emitting deletes or change correction behavior | Downstream state becomes stale |
| Add sensitive fields | Wider exposure through existing subscribers or logs |

Assess the actual interface: serialized keys, headers, envelopes, payload, topic or table identity, and delivery guarantees. Validating only the value schema misses key changes that affect ordering, joins, and compaction.

### Expand, migrate, contract

Suppose `customer_id` must be renamed to `buyer_id`:

1. **Expand:** add the new field in a compatible way; retain the old field and define consistency between them.
2. **Publish both:** populate both fields from the same authoritative value. Test that they agree.
3. **Migrate:** consumers adopt the new field, including a defined fallback for historical records where it is absent.
4. **Observe:** verify that active consumers migrated and that rollback and replay still work.
5. **Contract:** retire the old field only under the compatibility and deprecation policy; introduce a new major interface if necessary.

Removing the field may remain forbidden by historical compatibility even after live consumers migrate. Do not disable the registry policy just to force registration; decide whether to keep the field deprecated or create a separate contract lineage.

### Versioning policy

Semantic versioning can communicate intent: a major change breaks the public contract, a minor change extends it compatibly, and a patch corrects documentation or behavior without breaking guarantees. Define the policy locally; version labels do not prove compatibility.

Use a new topic, event type, endpoint, or published table interface when two incompatible meanings must coexist. A new subject alone does not protect an old consumer if incompatible records still arrive on the same subscription.

Every major migration needs a consumer inventory, transition period, data migration or replay plan, reconciliation, and retirement criteria. Preserve `event_id` or define an explicit mapping when dual publishing; otherwise consumers can count the same business event twice.

### Rollback and historical data

Rolling a producer back does not remove the records its newer version already emitted. Verify that the rollback reader and writer combinations still work with data already in the log or table.

A changed schema does not rewrite stored data or repair historical values. Backfills need their own transformation version, validation, identity rules, and publication strategy. If old data lacks information needed by the new contract, do not fabricate a plausible default and present it as a known fact.

## 6. Structural vs. Semantic Contracts

### Shape and meaning are different guarantees

| Structural rule | Semantic rule |
| --- | --- |
| `amount_minor` is an integer | Amount uses the declared currency's minor unit and includes tax |
| `occurred_at` is a timestamp | It represents order acceptance, not connector ingestion |
| `order_id` is a nonempty string | It uniquely identifies an order within a declared scope |
| `status` is an allowed string | Status transitions follow the business lifecycle |
| `customer_id` is nullable | Null means guest checkout, rather than an upstream lookup failure |

Structural vs. semantic describes the guarantee, not simply the tool implementing it. A schema may enforce a domain constraint such as a numeric minimum. Uniqueness across records, freshness, and reconciliation usually need state or additional data.

### A schema-compatible business incident

The producer keeps `amount` as an integer but changes its unit from dollars to cents. Old consumers still deserialize it and report revenue at 100 times the intended value. A registry schema comparison may see no change at all.

A safer design uses an explicit unit-bearing field, a currency field, documented meaning, and tests against known business examples. A deliberate unit change requires a migration even if its wire representation is identical.

Semantic checks for the order pipeline might include:

- Revenue aggregates reconcile to accepted orders after an agreed lateness window.
- Repeated `event_id` values have consistent contents; duplicates do not increase revenue.
- Currency belongs to the supported business set, and conversions use the agreed rate date.
- A cancellation references an existing order or follows a documented late-arrival process.
- Missing customer identity follows the agreed guest-checkout rule.

Choose rules for the event's meaning. Nonnegative amounts may suit an order-placement event but reject valid refund or adjustment events. An absolute “event time cannot be in the future” rule may mishandle documented clock skew.

### Service-level and privacy guarantees

Define freshness, completeness, and availability with measurable boundaries. For example, an agreed target could be “99.9% of accepted orders are available in the analytical table within five minutes over a calendar month.” Specify source of truth, measurement clocks, exclusions, and the allowed correction window.

A contract can classify sensitive fields and define retention and permitted uses, but access controls and deletion workflows must enforce those rules. Inspect invalid-event logging and quarantine paths too; they can contain the same sensitive payload as the main stream.

**Interview question:** Can JSON Schema enforce a data contract? It can enforce expressible per-document rules when validation runs. Cross-record meaning, delivery guarantees, freshness, ownership, and change coordination need additional mechanisms.

## 7. Contract Validation & CI/CD Enforcement

### Validate at different boundaries

| Boundary | Check | Example response to failure |
| --- | --- | --- |
| Pull request | Schema syntax, compatibility, ownership, consumer expectations | Block merge with a specific incompatibility report |
| Producer serialization | Actual record matches selected schema and required rules | Reject emission; surface a handled application error |
| Ingestion | Decode, schema identity, classification, basic invariants | Reject or quarantine under a documented policy |
| Transformation | Mapping, keys, null behavior, units | Fail the affected stage or isolate invalid records |
| Publication | Dataset completeness and aggregate reconciliation | Keep the last valid publication; alert the owner |
| Runtime monitoring | Freshness, volume, null rates, duplicates, distribution drift | Alert, pause publication, or trigger an incident response |

Schema compatibility checks are **schema against schema**. Record validation is **data against schema**. Semantic validation is **data against business rules and context**. All three answer different questions.

### A practical CI/CD flow

1. Change the contract and schema together in version control, including meaning, owner, examples, and deprecation notes.
2. Parse the schema with the supported implementation; compile Protobuf definitions or validate the JSON Schema dialect as appropriate.
3. Check compatibility against the required deployed and historical versions in the correct subject and policy.
4. Run positive and negative payload fixtures, real serialization round trips, consumer tests, and semantic checks.
5. Review affected consumers and classify semantic changes that automated schema checks cannot detect.
6. Register the approved schema through controlled deployment. Registration must still enforce policy because registry state may have changed since CI ran.
7. Deploy in the required order, monitor contract violations, and verify retirement criteria before removing an old interface.

Bind deployment to the tested schema artifact. A passing test for one schema does not approve an application that can register a different schema at runtime. Restrict policy edits so a failing application cannot silently turn compatibility checks off.

### Handling invalid records without losing data

Choose reject, quarantine, or stop based on the contract and the consequence of missing or incorrect data. Quarantine should retain an event identifier, schema identity, source position, validation reason, and enough protected payload to investigate and replay.

For a stream, advancing progress before durably preserving an invalid record can lose it. Persist the failure and its processing position consistently using the system's transaction or idempotency mechanism. If quarantine publication fails, do not acknowledge the original as successfully handled.

For a batch, stage data, validate it, and publish through the destination's safe replacement or transaction mechanism. Do not expose a partially validated partition by overwriting good data before checks finish.

Document whether quarantined records count against completeness, who owns remediation, and how repaired data is replayed without duplicate effects. A pipeline that sends every record to quarantine should not appear healthy.

## 8. Contract Testing & Consumer-Driven Contracts

### Complementary test layers

| Test | What it establishes | What it cannot establish alone |
| --- | --- | --- |
| Schema compatibility | Definitions satisfy the configured evolution rules | Actual producer output is valid |
| Payload validation | A specific instance satisfies schema constraints | Every possible production instance will pass |
| Serialization round trip | Real serializer/deserializer versions agree | Correct downstream business effects |
| Consumer contract test | Consumer handles messages satisfying its expectations | Producer actually emits those messages |
| Provider verification | Producer meets published consumer expectations in tested states | Network, broker, ordering, and operational behavior |
| Integration or replay test | Selected versions work through the real pipeline path | Untested volume, failures, and business cases |

### Producer-driven and consumer-driven contracts

With a **producer-driven contract**, the producer publishes its interface and consumers implement against it. With **consumer-driven contracts**, consumers publish executable examples or matchers describing the parts they depend on, and the provider verifies those expectations.

For messaging, a consumer test passes a representative message into its real handler and asserts the resulting behavior. Provider verification checks that the producer's actual message-building logic can produce messages satisfying the consumer's contract. Pact supports this style of message contract testing; it does not require the producer and consumer to run together. [How Pact works](https://docs.pact.io/getting_started/how_pact_works).

For example, billing declares that `amount_minor` is an integer in minor units, `currency` is present, and the event identity supports deduplication. Its test verifies a known order produces the expected accounting effect. The producer verifies the billing expectation alongside fraud and analytics expectations before release.

Use flexible matchers for irrelevant values and exact assertions for business-significant ones. Hard-coding an incidental UUID can make tests brittle; allowing any number where a known unit conversion matters can conceal a real bug.

### A useful compatibility test matrix

| Writer | Reader | Purpose |
| --- | --- | --- |
| Old | New | Backward compatibility and historical replay |
| New | Old | Forward compatibility and mixed deployments |
| New | New | Normal behavior after migration |
| Historical versions | New | Retained-data compatibility and old defaults |
| New | Rollback candidate | Rollback after new records already exist |

Use fixtures for missing fields, explicit nulls, empty values, unknown enum values, boundaries, invalid payloads, and historic schema versions. Test the actual parser and handler: a fake JSON dictionary does not verify a binary Avro or Protobuf path.

Keep deployed consumer versions visible. Passing a contract from an unreleased consumer does not prove compatibility with the version currently in production. Stale contracts also need a retirement process so abandoned consumers do not indefinitely block evolution.

### What contract tests do not replace

Contract tests can run quickly and isolate interface failures. They do not replace integration tests for broker configuration, authentication, partitioning, delivery, ordering, retries, and sink behavior. They also do not prove production freshness or correctness across an entire dataset.

**Interview question:** Is a shared schema enough for consumer-driven testing? No. Two applications can share a schema while disagreeing on null handling, units, enum behavior, or mandatory business fields. Test the consumer's behavior and verify the producer against those expectations.

## 9. Interview Scenarios & Quick Reference

### Scenario: add an optional field

“We want to add `coupon_code` without coordinating every release.”

Identify the format, field presence rules, existing readers, content model, and policy. For Avro, a nullable field with a valid null default supports old records and old readers under the assumptions above. For closed JSON Schema, old validators reject payloads containing the new field. Run the required version matrix before selecting rollout order.

### Scenario: registry accepts the change, but revenue is wrong

“The schema check passed, yet daily revenue doubled.”

Trace event grain, units, event identity, duplicate delivery, key changes, and transformation logic. Reconcile a known order from source through its serialized event to the aggregate. Schema acceptance proves only the configured structural check; it cannot establish the intended accounting result.

### Scenario: new consumers fail on replay

“Live traffic works, but replaying last year's topic fails.”

Check whether historical schema IDs and references are resolvable, whether the latest reader was tested against all retained versions, and whether transformations still handle earlier meanings and missing fields. Latest-version checks, a warm schema cache, or a recent happy-path fixture can hide historical failures.

### Scenario: remove a field with unknown consumers

“Nobody seems to use `legacy_customer_code`. Can we remove it?”

Use declared subscriptions, consumer inventories, code search, lineage, contract tests, and owner confirmation to establish usage. Absence of recent traffic does not prove a batch or disaster-recovery consumer is retired. Deprecate first; remove only when the migration and compatibility policy permit it.

### Short answers to remember

- **Schema vs. contract:** schema defines structure; contract also defines meaning, delivery, ownership, quality, and evolution.
- **Backward:** new readers read old data; consumers usually deploy first.
- **Forward:** old readers read new data; producers usually deploy first.
- **Full:** both directions, within the versions covered by the checks.
- **Transitive:** checks all previous subject versions rather than only the latest.
- **Schema ID vs. version:** metadata identity vs. position within a subject; neither automatically expresses a business version.
- **Nullable vs. optional vs. default:** accepting null, allowing absence, and supplying a missing value are distinct concepts.
- **Breaking change:** any change that violates a supported consumer guarantee, including semantic changes with identical types.
- **Registry vs. runtime validation:** registering compatible schemas does not validate all actual records or business rules.
- **Consumer-driven contract:** consumer expectations become executable checks verified against the producer.

**Interview exercise:** Design an order-events contract for billing, fraud detection, and analytics. Explain ownership, schema format, subject strategy, key and event identity, compatibility policy, replay horizon, deployment order, semantic tests, invalid-record handling, and a breaking-change migration. Name the evidence that would allow you to retire the old version.
