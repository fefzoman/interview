# YAML — SRE Expert Checklist

A deployment can fail even when its YAML parses successfully. A parser may turn a string into a boolean, Helm may replace a list, or Kubernetes may reject a field that a linter accepts. Preparing for an SRE interview means explaining where each transformation happens and proving what the receiving system actually sees.

This guide follows configuration from source text through parsing, rendering, validation, admission, and reconciliation. It assumes familiarity with basic infrastructure tooling. Examples are standalone fragments unless described as complete resources. Parser behavior depends on the library version, resolver, loader, and destination type; record those details when reproducing a problem. Helm merging examples use Helm 3 behavior; check the deployed major version before applying them elsewhere.

## Contents

1. [Fundamentals & Syntax](#1-fundamentals--syntax)
2. [Advanced YAML](#2-advanced-yaml)
3. [Parsing & Serialization](#3-parsing--serialization)
4. [Validation & Tooling](#4-validation--tooling)
5. [YAML in SRE Infrastructure](#5-yaml-in-sre-infrastructure)
6. [Security](#6-security)
7. [Production Troubleshooting](#7-production-troubleshooting)
8. [Expert Interview Scenarios](#8-expert-interview-scenarios)

## 1. Fundamentals & Syntax

### YAML 1.1 vs 1.2, JSON compatibility, and schemas

YAML is a serialization language. A configuration product adds its own allowed fields, defaults, interpolation, and execution behavior. Understanding YAML does not establish whether a Kubernetes Deployment or a CI workflow is meaningful.

YAML 1.2 changed implicit typing to align better with JSON. Its **Core schema** recognizes common scalar types, while its **JSON schema** uses narrower JSON-like resolution. The **Failsafe schema** resolves untagged scalars as strings and provides mappings and sequences. These YAML schemas determine node tags; they are different from JSON Schema documents that validate application fields. See the [YAML specification changes](https://yaml.org/spec/1.2.2/ext/changes/).

| Plain scalar | Typical YAML 1.1 resolution | YAML 1.2 Core resolution |
| --- | --- | --- |
| `yes`, `on`, `off` | Boolean | String |
| `true`, `false` | Boolean | Boolean |
| `0123` | Octal integer, decimal 83 | Decimal integer 123 |
| `0o123` | Not the standard 1.1 octal notation | Octal integer, decimal 83 |
| `null`, `~`, an empty value | Null | Null |
| `2026-10-31` | Often a timestamp/date | String under Core |

YAML 1.2 aims to accept JSON documents, but full YAML contains features JSON cannot represent directly. Practical parsers can still have compatibility gaps. A `%YAML 1.2` directive declares intent; it cannot make a parser implement unsupported rules. For portable infrastructure, use explicit `true`/`false` and quote identifiers, dates, and leading-zero strings. The [YAML 1.1 type definitions](https://yaml.org/type/index.html) explain the older resolution rules.

### Scalars, mappings, sequences, and nesting

A **scalar** is a single value. A **mapping** associates unique keys with values. A **sequence** is an ordered collection. Values can themselves contain mappings and sequences:

```yaml
service:
  name: checkout
  replicas: 3
  enabled: true
  ports:
    - name: http
      port: 8080
    - name: metrics
      port: 9090
  annotations: {}
  owners: []
  maintenance: null
```

Here, `service` contains a mapping, `ports` contains a sequence of mappings, and `name` contains a string scalar. Empty mappings `{}`, empty sequences `[]`, null, and a missing field carry different information. A consumer may default a missing field but reject null, or treat an empty list as an explicit request to remove entries.

### Block vs flow style and indentation

Block style expresses nesting through indentation. Flow style uses braces and brackets; these two fragments describe the same ordinary data:

```yaml
service:
  ports: [8080, 9090]
  labels: {team: sre, tier: backend}
```

```yaml
service:
  ports:
    - 8080
    - 9090
  labels:
    team: sre
    tier: backend
```

Use spaces for indentation; tabs cannot serve as YAML indentation. Two spaces is a convention, not a language requirement. Siblings must align and nested content must follow the grammar's indentation rules. A sequence beneath a mapping can legally use an indentless style, but a consistent repository convention reduces mistakes.

A colon followed by separation introduces a mapping value, and a separated `#` starts a comment. Quote strings containing those combinations: `message: "status: ready # verified"`. Flow collections add punctuation constraints, so avoid relying on borderline plain-scalar syntax. The [YAML 1.2.2 specification](https://yaml.org/spec/1.2.2/) defines the grammar and data model.

### Strings, integers, floats, booleans, and null

```yaml
name: checkout
replicas: 3
ratio: 0.75
enabled: false
optional: null
empty_string: ""
build_id: "0123"
```

Types matter at the destination. Kubernetes environment variable values and ConfigMap `data` values are strings, while `spec.replicas` is an integer. Quoting every field is therefore as incorrect as quoting none of them.

```yaml
env:
  - name: FEATURE_ENABLED
    value: "true"
```

A scalar's textual appearance does not reveal its final runtime type. Validate the parsed value against the receiving field's contract. This matters especially when configuration crosses YAML, JSON, a language runtime, and an API.

### Quoting, escaping, and explicit tags

Single quotes preserve backslashes literally; write an embedded single quote twice. Double quotes interpret YAML escapes such as `\n`, `\t`, `\"`, and `\\`. An unsupported escape such as `\d` inside double quotes is an error.

```yaml
single: 'it''s ready'
regex: '^\d+$'
message: "first line\nsecond line"
path: "C:\\logs\\app"
literal_null: "null"
```

Quoting prevents ordinary implicit scalar resolution, but an explicit tag requests a type:

```yaml
identifier: !!str 0123
replicas: !!int "3"
enabled: !!bool "true"
```

Tags identify node types; they are not comments. Not every downstream format or loader accepts them, and serialization can discard their presentation once a native value has been constructed. Prefer ordinary quoted strings for interoperability, and use tags when the complete processing chain supports them.

### Multiline strings: `|`, `>`, and chomping

**Literal style** (`|`) preserves content line breaks. **Folded style** (`>`) generally turns breaks between ordinary adjacent lines into spaces. Folding preserves paragraph separation and treats more-indented lines specially; it is not equivalent to replacing every newline with a space.

For the following two content lines, each terminated by a line break, the values are:

| Header | Result shown with escapes | Trailing behavior |
| --- | --- | --- |
| `|` | `"alpha\nbeta\n"` | Clip: retain one final newline |
| `|-` | `"alpha\nbeta"` | Strip final newline and trailing empty lines |
| `|+` | `"alpha\nbeta\n"` | Keep final newline and any additional trailing empty lines |
| `>` | `"alpha beta\n"` | Fold and clip |
| `>-` | `"alpha beta"` | Fold and strip |

`|+` differs from `|` when trailing empty lines exist. An explicit indentation indicator such as `|2` helps when leading content spaces would otherwise make indentation detection ambiguous.

Use literal style for scripts whose command boundaries depend on newlines:

```yaml
script: |-
  set -eu
  printf '%s\n' 'starting'
  printf '%s\n' 'finished'
```

YAML quoting does not apply inside block scalar content: the shell receives those quotes and backslashes. Indentation removes the common content prefix; unintended extra spaces can break a heredoc terminator or change embedded Python. Inspect the resulting string, not just the source alignment.

### Multiple documents: `---` and `...`

`---` starts a document, and `...` explicitly ends one. A stream can contain multiple documents; each has its own anchors and may construct a separate native object. Consumers differ in whether they accept a stream or require exactly one document.

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: checkout-settings
data:
  mode: "safe"
...
---
apiVersion: v1
kind: Service
metadata:
  name: checkout
spec:
  ports:
    - port: 80
      targetPort: 8080
```

A single-document loader should reject additional documents rather than silently selecting one. For Python streams, use a multi-document API and actually consume its iterator so errors in later documents are detected.

## 2. Advanced YAML

### Anchors, aliases, and the representation graph

An anchor names a node with `&`; an alias refers to that node with `*`:

```yaml
labels: &shared_labels
  team: sre
  service: checkout
primary:
  labels: *shared_labels
secondary:
  labels: *shared_labels
```

An alias is a reference, not textual substitution or a parameterized template. The YAML representation graph can contain shared nodes and cycles, whereas JSON represents a tree. Depending on construction, modifying one aliased mapping in memory can affect another reference to the same object. Do not assume the loader made independent copies.

Aliases refer to anchors already encountered in the same document. They cannot import another file or cross `---`. Anchor names and comments belong to serialization/presentation rather than the ordinary application data model. A dump/load operation can change or lose them.

### Merge keys, precedence, and shallow merging

The `<<` merge key is a YAML 1.1 draft feature supported by many parsers as an extension; it is not part of YAML 1.2 Core. Confirm support in the actual consumer.

```yaml
defaults: &defaults
  retries: 3
  resources:
    cpu: "100m"
    memory: "128Mi"
production:
  <<: *defaults
  retries: 5
  resources:
    memory: "256Mi"
```

With merge support, `production.retries` is 5 and `production.resources` contains only `memory`. The explicit `resources` mapping replaces the inherited value as a whole: this is a shallow merge. It does not recursively add `cpu`.

Explicit keys in the receiving mapping win over merged keys, regardless of placement. In `<<: [*first, *second]`, overlapping keys from the first mapping win. That ordering differs from many override-file systems. Avoid repeating `<<` keys; use a merge sequence. These rules come from the [merge key draft](https://yaml.org/type/merge.html), and do not define Helm or Kustomize merging.

### Duplicate keys and complex mapping keys

Mappings require unique keys. Nevertheless, some loaders accept duplicates and keep the first or last value, while others reject them. Treat acceptance as a compatibility hazard:

```yaml
replicas: 2
replicas: 10
```

Once a loader has discarded one entry, subsequent JSON Schema validation cannot recover the evidence. Reject duplicates before constructing an ordinary dictionary. Type resolution can also create collisions: keys such as `true` and `on` may both resolve to a boolean under 1.1-style rules.

YAML also permits complex keys:

```yaml
? [region, zone]
: location
unset:
explicit_null: null
empty: ""
```

A sequence key cannot be represented directly as a JSON object member name and may be unhashable in a host language. The last three values are null, null, and an empty string. An omitted key is different again. Keep infrastructure mapping keys as strings unless the consuming format explicitly allows something else.

### Custom tags and schema resolution

A tag such as `!SecretRef` or `!Duration` requires application-defined interpretation. A safe loader can reject an unknown tag; another loader may preserve it as a node or invoke a registered constructor. A tag alone does not fetch a secret or validate a duration.

Resolution chooses a tag for an untagged scalar; construction turns the tagged node into a native value. Custom resolvers can therefore change types without visible tags in the input. Register only narrow, audited constructors, validate their inputs, and avoid constructors that perform arbitrary imports or side effects.

## 3. Parsing & Serialization

### From bytes to application objects

It helps to identify which layer failed:

```text
bytes → tokens → parsing events → representation graph → native objects
        scanning    parsing          composition         construction
```

Scanning recognizes indicators and scalar text; parsing establishes document and collection structure. Composition resolves aliases into nodes. Construction applies tags and produces language objects. An indentation error occurs before an application's field validation; an unknown custom tag may fail during construction.

Serialization reverses the direction: represent native values as nodes, serialize the graph, and emit text. Ordinary native dictionaries usually contain no original comments or quote style, so an emitter cannot reconstruct them accurately.

### Parser compatibility: PyYAML, ruamel.yaml, go-yaml, SnakeYAML

| Library/family | Behavior to remember | Production implication |
| --- | --- | --- |
| PyYAML | Common loaders use 1.1-style implicit resolution, including `on` and `yes`; safe loading supports standard tags | `safe_load` prevents ordinary arbitrary Python object construction, but does not mean YAML 1.2 typing or duplicate-key rejection |
| ruamel.yaml | Defaults toward YAML 1.2; offers round-trip structures and comment/style preservation | Choose loader mode deliberately and test pure Python versus accelerated paths |
| go-yaml v3 | Supports much of 1.2 with selected 1.1 compatibility; `yes`/`on` can decode as booleans into typed boolean destinations | Generic-map decoding and typed-struct decoding can differ; identify the exact module/version |
| SnakeYAML | Classic library implements YAML 1.1; SnakeYAML Engine targets YAML 1.2 | Library family, constructor, and loader options all affect compatibility and security |

These are specific distinctions, not guarantees for every release or wrapper. See [PyYAML loader documentation](https://pyyaml.org/wiki/PyYAMLDocumentation), [ruamel.yaml compatibility notes](https://yaml.dev/doc/ruamel.yaml/pyyaml/), [go-yaml v3 compatibility](https://github.com/yaml/go-yaml/tree/v3), and [SnakeYAML's library description](https://github.com/snakeyaml/snakeyaml).

Create small fixtures containing booleans, numeric-looking strings, timestamps, duplicates, aliases, and multiple documents. Run them through the exact consumer path: a standalone parser may behave differently from a wrapper that converts YAML to JSON before decoding an API object.

### Safe loading and round-trip editing

For a Python application that expects ordinary YAML data:

```python
import yaml

with open("config.yaml", encoding="utf-8") as stream:
    documents = list(yaml.safe_load_all(stream))
```

This example consumes all documents, but it does not enforce input size, duplicate rejection, an application schema, or a document-count policy. Add those at the application's boundary. Never select `UnsafeLoader` for untrusted configuration. Do not assume `FullLoader` is interchangeable with a minimal safe-data loader.

When a tool must edit human-maintained files while retaining comments and style, use a round-trip representation rather than a plain dictionary:

```python
from pathlib import Path
from ruamel.yaml import YAML

path = Path("values.yaml")
yaml = YAML()
yaml.preserve_quotes = True
with path.open(encoding="utf-8") as stream:
    data = yaml.load(stream)
data["replicas"] = 3
with path.open("w", encoding="utf-8") as stream:
    yaml.dump(data, stream)
```

This illustrates serialization, not a production-safe file replacement routine. Automation should write a temporary file, validate it, and atomically replace the original while preserving required metadata. Round-trip support preserves much more presentation than ordinary loading, but is not a promise of byte-for-byte preservation. See [ruamel.yaml round-trip details](https://yaml.dev/doc/ruamel.yaml/detail/).

### YAML-to-JSON limits, coercion, precision, and timestamps

Conversion can lose comments, formatting, tags, alias identity, and key type information. Cycles cannot become ordinary JSON trees. Non-string keys need rejection or an explicit conversion policy; blindly stringifying them can create collisions. Multi-document streams need a chosen envelope, such as a JSON array or one JSON document per line.

Integers can be exact in one runtime and rounded in another. For example, JavaScript's ordinary `Number` cannot exactly represent every integer above `2^53 - 1`. Quote identifiers that must preserve every digit. Binary floating-point also cannot represent every decimal fraction exactly; monetary or precision-sensitive values need a contract such as decimal strings plus application validation.

A 1.1-style loader may construct a date object from `2026-10-31`; a 1.2 Core resolver treats it as a string. A later JSON serializer may reject the date object or convert it using a framework-specific format. Quote date-like strings when the application expects strings, and test the full conversion path. For concrete conversion behavior, see [yq's JSON conversion documentation](https://mikefarah.gitbook.io/yq/usage/convert).

## 4. Validation & Tooling

### Structural vs semantic validation

Validation should answer progressively stronger questions:

| Layer | What it establishes | What it does not establish |
| --- | --- | --- |
| Syntax and lint | Text parses; selected style and ambiguity rules pass | Correct application fields or runtime behavior |
| Schema | Required fields, types, enums, and selected constraints match | All relationships, permissions, or reachable dependencies |
| Native application validation | The product accepts its own configuration | Successful operation under production conditions |
| Policy/admission | Organizational and cluster restrictions accept the request | Sufficient capacity or healthy dependencies |
| Runtime verification | Deployment becomes healthy and serves correctly | Future reliability under every failure mode |

JSON Schema can constrain application data after parsing, including required properties and rejection of unknown fields. OpenAPI describes APIs and their schemas; supported schema dialects differ by OpenAPI version and consumer. Kubernetes CRDs use structural OpenAPI schemas with Kubernetes-specific extensions and may add CEL validation. Do not assume an arbitrary JSON Schema validator exactly reproduces API-server behavior.

### Syntax checks with yamllint and yq

The following `yq` commands use **Mike Farah's yq v4**, not the unrelated Python `yq` wrapper:

```bash
yamllint -d '{extends: default, rules: {key-duplicates: enable, truthy: enable}}' config.yaml
yq eval '.' config.yaml > /dev/null
yq -o=json '.' config.yaml
```

Lint source files before a loader can discard duplicate keys. Enable rules for truthy ambiguity, octal values, and indentation where appropriate. Helm templates contain Go template syntax and cannot generally be linted as ordinary YAML until rendered. `yq` is useful for parsing and inspecting data, but does not by itself validate the application's schema. See [yamllint rules](https://yamllint.readthedocs.io/en/stable/rules.html).

### Kubernetes and Helm validation

```bash
helm lint ./chart -f values-prod.yaml
helm template checkout ./chart -f values-prod.yaml > rendered.yaml
kubeconform -strict -summary -kubernetes-version 1.34.0 rendered.yaml
kubectl --context staging apply --dry-run=client --validate=strict -f rendered.yaml
kubectl --context staging apply --dry-run=server --validate=strict -f rendered.yaml
```

The Kubernetes version above is an example target, not a claim about the current release. Pin it to the supported target cluster and supply schemas for CRDs. Do not silently skip unknown kinds in a release gate. Kubeconform validates against schemas, but cannot reproduce every server-side semantic rule or admission policy. See [kubeconform](https://github.com/yannh/kubeconform).

Client dry run avoids submitting a write but can still need API discovery or schema access. Server dry run uses the target API server's request handling, defaulting, validation, and compatible admission chain without persisting the requested object. It needs credentials and connectivity; it does not schedule pods or prove controllers will succeed. Webhooks must declare compatible dry-run behavior. See [Kubernetes API dry-run semantics](https://kubernetes.io/docs/reference/using-api/api-concepts/#dry-run).

`helm lint` checks a chart and its conventions; `helm template` exposes generated manifests. Neither alone proves acceptance by the target cluster. Supply the intended release name, namespace, Kubernetes version, API capabilities, dependencies, and values when rendering. Templates using `lookup` or cluster-dependent capabilities need special attention because a local render may differ from an actual operation.

### CI/CD validation, policy, and normalization

For thousands of files, classify inputs by consumer and render boundary rather than treating every `.yaml` file identically. A scalable pipeline should:

1. Detect affected charts, overlays, and consumers, including reverse dependencies when shared inputs change.
2. Lint plain source YAML and reject duplicates; scan source for credentials.
3. Render every supported affected environment with pinned tools, dependencies, and inputs.
4. Validate rendered documents against the correct schemas, including CRDs; fail on missing schemas.
5. Run native product checks and policy tests for privileges, images, resource requirements, and ownership.
6. Run server dry run in an appropriately isolated, representative environment with controlled credentials.
7. Retain redacted render artifacts and meaningful diffs; deploy through the normal reconciler and verify health.

Cache schemas and chart dependencies by immutable version or digest, parallelize independent validation, and run broader checks periodically to catch dependency-analysis gaps. Do not give untrusted pull requests production credentials: rendering itself may invoke plugins, subprocesses, or external dependencies.

For a single JSON-compatible document, a normalized comparison can reduce formatting noise:

```bash
yq -o=json '.' before.yaml | jq -S . > before.json
yq -o=json '.' after.yaml | jq -S . > after.json
diff -u before.json after.json
```

Sort mapping keys, but do not sort lists indiscriminately: order may be meaningful. Preserve types, resolve aliases consistently, and choose an explicit policy for null versus absent fields. Normalization after permissive parsing cannot reveal duplicate entries already lost.

## 5. YAML in SRE Infrastructure

### Kubernetes resources, CRDs, and API versions

A resource's `apiVersion` and `kind` choose an API contract. `metadata` identifies the object; `spec` usually declares desired state, while controllers generally maintain `status`. The YAML parser does not know whether an API version exists, a selector matches pod labels, or an update changes an immutable field.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: checkout
spec:
  replicas: 3
  selector:
    matchLabels:
      app: checkout
  template:
    metadata:
      labels:
        app: checkout
    spec:
      containers:
        - name: checkout
          image: registry.example.com/checkout:1.2.3
          ports:
            - containerPort: 8080
```

This illustrates structure; production requirements such as resource limits and security settings depend on policy. CRDs register additional kinds, served versions, validation, and sometimes conversion webhooks. Validate custom resources against the installed CRD version, and ensure the API is established before submitting dependent objects. Valid YAML with an unavailable `apiVersion` is still an invalid API request.

### Helm values, templates, overrides, and precedence

Helm reads values, combines them, and executes templates to produce manifests. A values file is data; `{{ ... }}` in a value is not automatically evaluated. A chart must explicitly use a function such as `tpl` to interpret a value as template content.

For ordinary Helm 3 inputs, increasing specificity is chart defaults, parent-chart overrides for a subchart, user values files, then command-line values. With repeated `-f` flags, later files take precedence. Maps generally merge recursively; scalar values and lists replace previous values. Upgrade options such as `--reuse-values` change the effective input set, so reproduce the actual command. See [Helm 3 values](https://helm.sh/docs/v3/chart_template_guide/values_files/) and [upgrade options](https://helm.sh/docs/v3/helm/helm_upgrade/).

```yaml
# Base values
resources:
  requests:
    cpu: "100m"
    memory: "128Mi"
ports: [8080, 9090]
```

```yaml
# Later override file
resources:
  requests:
    memory: "256Mi"
ports: [8443]
```

The ordinary combined values retain CPU `100m`, update memory to `256Mi`, and contain only port `8443`. This differs from YAML's shallow `<<` merge. A nested map disappearing suggests a type change, null removal, a template transformation, or another input. In Helm 3 coalescing, null can remove a default key. Lists are not automatically merged by fields such as `name`.

Use `--set-string buildId=0123` for string identifiers and a chart `values.schema.json` to validate inputs. Check the rendered field type too: a template can turn a correctly typed input into incorrect YAML. Template functions such as `default` also treat values such as `false`, zero, and empty collections as empty, which can surprise chart authors. See the [Helm template function reference](https://helm.sh/docs/v3/chart_template_guide/function_list/#default).

### Kustomize bases, overlays, strategic merge, and JSON patches

Kustomize starts with resource bases and applies overlays and transformations without embedding Go templates. Use `kubectl kustomize overlays/prod` to inspect final output before validation.

Strategic merge uses schema information to decide how fields and lists merge; for recognized Kubernetes lists it can merge entries by keys such as a container's `name`. It does not imply arbitrary lists or CRDs receive identical treatment. JSON Patch uses explicit operations such as `add`, `replace`, and `remove`; array index paths are sensitive to ordering, and `~` and `/` require JSON Pointer escaping. Patch order and target selection affect results. See [Kustomize patches](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/).

### Argo CD and Flux GitOps

GitOps controllers fetch source, render desired resources, compare them with the cluster, and reconcile changes. Investigate the exact source revision and render inputs before attributing a difference to YAML.

Argo CD Applications specify sources and destinations; Flux uses resources such as GitRepository, Kustomization, and HelmRelease to separate source, build, and reconciliation concerns. A Flux `Kustomization` custom resource is different from a Kustomize `kustomization.yaml`. Health checks, dependencies, pruning, and reconciliation intervals influence behavior beyond successful parsing. See [Flux Kustomizations](https://fluxcd.io/flux/components/kustomize/kustomizations/).

Defaults, mutating webhooks, controller-owned fields, list ordering, and nondeterministic templates can produce differences after submission. Ignore only well-understood fields owned by another actor; broad ignore rules can conceal real drift. See [Argo CD diff customization](https://argo-cd.readthedocs.io/en/stable/user-guide/diffing/).

### Ansible playbooks, inventories, and variables

A playbook is commonly a sequence of plays containing tasks; YAML inventory defines hosts, groups, children, and variables. Ansible then adds its variable resolution, Jinja evaluation, module argument validation, and execution behavior. Quote expressions when they begin a YAML value, for example `destination: "{{ deploy_path }}"`.

Role defaults are easy to override; inventory, play, task, and role variables participate in more specific precedence rules. Extra variables supplied with `-e` have the highest variable precedence, but command-line options are not all extra variables. Avoid memorizing a shortened ordering as a universal merge rule. Use `ansible-inventory --host HOST` to inspect inventory variables, then investigate play/task overrides separately. `ansible-playbook --syntax-check playbook.yaml` does not execute tasks, and check mode support varies by module. See [Ansible precedence rules](https://docs.ansible.com/projects/ansible/latest/reference_appendices/general_precedence.html).

### GitHub Actions and GitLab CI

GitHub Actions interprets workflow fields such as `on`, `jobs`, `needs`, and expression contexts. A generic YAML 1.1 editor can rewrite the key `on` to a boolean and damage a workflow even though GitHub accepts the original syntax. Quote `'on'` when the file must pass through such an editor, or use a compatible parser. Expression evaluation and runner shell execution are additional stages. See [GitHub workflow syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax).

GitLab CI adds `include`, `extends`, `rules`, and its own merge semantics. `extends` combines mappings using GitLab's rules, while array values such as scripts are not universally appended. YAML anchors remain document-local; they are not cross-file inheritance. Use GitLab CI Lint and inspect the merged configuration to diagnose included templates and rule evaluation. See the [GitLab CI reference](https://docs.gitlab.com/ci/yaml/).

### Docker Compose and environment interpolation

Compose interprets `${VAR}`, defaults such as `${VAR:-fallback}`, and required variables such as `${VAR:?set VAR}`. `$$` escapes interpolation so a dollar sign can reach the container command. Host-shell variables, `.env`, explicit environment files, and container `environment` settings play different roles; an interpolation source does not automatically become a container variable.

```yaml
services:
  checkout:
    image: "registry.example.com/checkout:${IMAGE_TAG:?set IMAGE_TAG}"
    environment:
      LOG_LEVEL: "${LOG_LEVEL:-info}"
    command: ["sh", "-c", 'printf "%s\n" "$$LOG_LEVEL"']
```

Use `docker compose config` to inspect the resolved model and `docker compose config --environment` to inspect interpolation inputs. Output can expose secrets. The dollar escape in this example belongs to Compose, not YAML. See [Compose interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/).

### Prometheus and Alertmanager

Prometheus configuration describes scrape jobs, discovery, relabeling, and rule files. Alertmanager configuration describes routing, grouping, inhibition, and receivers. A valid YAML document can still contain invalid PromQL, an unavailable receiver, or a route that sends alerts to the wrong team.

```bash
promtool check config prometheus.yml
promtool check rules alerts.yml
promtool test rules rule-tests.yml
amtool check-config alertmanager.yml
```

Use matching product versions for checks. Test representative firing and recovery cases, and verify routing behavior with the product's routing tools. After a reload, check readiness, logs, and reload-success signals; an accepted reload request alone is insufficient evidence. See [promtool](https://prometheus.io/docs/prometheus/latest/command-line/promtool/), [amtool usage](https://github.com/prometheus/alertmanager#amtool), and the [Alertmanager management API](https://prometheus.io/docs/alerting/latest/management_api/).

## 6. Security

### Unsafe deserialization and arbitrary object construction

The dangerous boundary appears when a loader lets input choose constructors for runtime objects. Some language-specific tags can instantiate classes or invoke behavior. The risk depends on the loader, allowed tags, library version, and constructors; YAML syntax itself does not execute code.

Use safe-data loaders, reject unneeded tags, validate structure, and maintain patched dependencies. In Java, review the selected constructor and loader options rather than assuming every SnakeYAML setup behaves alike. Audit custom constructors too: a function registered with a safe loader can reintroduce side effects. Prefer plain strings, numbers, booleans, lists, and string-keyed mappings at untrusted boundaries.

### Alias expansion and resource exhaustion

A small document can describe a large repeated structure through aliases. Some parsers preserve shared references efficiently, but later traversals, copying, or JSON conversion can expand work dramatically. Deep nesting, huge scalars, and very large collections create additional denial-of-service paths.

Set input-byte, nesting-depth, alias, and execution-resource limits where supported. Bound downstream traversal and output as well as parsing. Reject cycles when the application expects a tree, and reject aliases entirely if they are unnecessary. Safe object construction does not provide resource limits by itself.

### Secrets in YAML and Git

Base64 encoding in Kubernetes Secret `data` provides no confidentiality. Secrets can leak through commits, historical revisions, rendered manifests, diff artifacts, debug output, process arguments, and support bundles. Restrict decryption and cluster access, protect artifacts, and use secret scanning. If a credential is committed, rotate or revoke it promptly; deleting the latest file does not revoke copies.

| Approach | What Git contains | Main operational concern |
| --- | --- | --- |
| SOPS | Encrypted values plus encryption metadata | Key access, decryption identity, rotation, and plaintext handling during rendering |
| External Secrets Operator | References and synchronization configuration | Secret-store IAM, controller permissions, refresh behavior, and provider availability |
| Sealed Secrets | Ciphertext decryptable by the controller | Controller private-key backup and recovery; sealing scope and key lifecycle |

These approaches protect different parts of the lifecycle. They do not remove the need to protect materialized Kubernetes Secrets or application access. See [SOPS](https://github.com/getsops/sops), [External Secrets Operator](https://external-secrets.io/latest/), and [Sealed Secrets](https://github.com/bitnami/sealed-secrets).

### Schema enforcement, admission, and injection

Reject unknown fields and unsafe structures at ingestion, and enforce deployment restrictions at admission so alternative clients cannot bypass CI. Policies can require approved image registries, prohibit privileged containers, and constrain host access. Schema validation and admission operate after parsing and do not make an unsafe loader safe.

Templating and shell execution introduce separate trust boundaries. Interpolating an untrusted GitHub expression directly into a `run` script can create shell syntax before the shell starts. Pass it through an environment variable and quote the expansion:

```yaml
steps:
  - name: Show pull request title
    env:
      PR_TITLE: ${{ github.event.pull_request.title }}
    run: printf '%s\n' "$PR_TITLE"
```

Keep data out of `eval`, avoid shell invocation when argument arrays suffice, and do not pass untrusted strings to template evaluation such as Helm `tpl`. YAML quoting alone cannot secure a later interpreter. See [GitHub script injection guidance](https://docs.github.com/en/actions/concepts/security/script-injections).

## 7. Production Troubleshooting

### Start by identifying the failing boundary

Retain the source revision, exact command, tool versions, effective values, render output, target API version, and relevant error. Redact credentials before sharing. A useful investigation follows the same configuration through each transformation rather than switching parsers until one accepts it.

| Symptom | Likely cause | Focused check |
| --- | --- | --- |
| Error on a visually aligned line | Tab, nonbreaking space, or wrong parent indentation | Inspect bytes and preceding lines |
| `on` becomes `true`; an ID loses zeros | Implicit type resolution | Inspect types using the actual loader |
| A value disappears without an error | Duplicate key or a later override | Lint original inputs; inspect precedence |
| Script runs as one unexpected command | Folded scalar or changed indentation | Print the parsed string with escapes |
| YAML lint passes; API rejects resource | Schema, served version, semantic validation, or admission | Strict server dry run against the target |
| Local Helm output differs from GitOps | Different inputs, chart version, capabilities, or post-rendering | Reproduce controller rendering |
| Synchronization repeats | Mutation, defaults, unstable rendering, or competing ownership | Compare desired, admitted, and live fields |

### Invisible characters, tabs, encoding, and line endings

Use UTF-8 as the repository convention. YAML processors can support other Unicode encodings, but tooling paths are not uniformly compatible. CRLF line endings alone are not invalid YAML; mixed endings or embedded carriage returns can still cause problems in surrounding tools and scripts.

```bash
sed -n '1,100l' config.yaml
od -An -tx1 -N 256 config.yaml
```

A nonbreaking space can look like an ordinary indent while failing to act as one. A byte-order mark or zero-width character in an unexpected position can alter a key. Inspect the original bytes before normalizing them, and check the line before the reported error: that is often where the structure became invalid.

### Unexpected types, duplicates, and multiline corruption

For a trusted local file, print the parsed type and representation of the suspect field. Python's `repr(value)` reveals `\n`, spaces, and carriage returns; this is especially useful for scripts and certificates. Use the consumer's parser for the final diagnosis even if another tool helps inspect the file.

Reject duplicate keys in the original document. Comparing already-constructed dictionaries will miss the discarded entry. For scripts, distinguish YAML folding, template whitespace trimming, shell continuation rules, and heredoc indentation. A change from `|` to `>` can alter command boundaries without a syntax error in YAML.

### Rendered output, values precedence, and parser incompatibility

For a Helm release in a known namespace:

```bash
helm get values checkout --namespace production --all
helm get manifest checkout --namespace production
helm template checkout ./chart --namespace production -f values-prod.yaml
kubectl --context production --namespace production get deployment checkout -o yaml
```

These commands inspect different stages. Computed release values and stored manifests describe a release revision; a fresh render depends on local inputs, and live objects include later API mutations and controller changes. Select the matching release revision when investigating a rollback or failed upgrade. Treat all outputs as potentially sensitive.

If a CI parser accepts a file that the application rejects, compare library versions, schemas, duplicate handling, tag support, document count, and destination types. Reduce the disagreement to one field. Quoting a string can fix a typing issue; it cannot fix an unsupported application field.

### GitOps drift and normalization

Separate formatting differences from typed data differences, then account for defaults and field ownership. `"3"` and `3` are different types even if both print as 3. Mapping order is usually not semantic; list order can be.

Inspect fields such as generated metadata, `status`, and `managedFields` with a defined comparison policy. Do not remove meaningful desired fields to get a clean diff. If an HPA owns replica count, establish that ownership explicitly. If a webhook injects fields, determine whether the reconciler should preserve them. Fix nondeterministic templates at their source, and keep any ignore rule narrow and documented.

## 8. Expert Interview Scenarios

### 1. Why can `on: true` behave differently across parsers?

**Answer:** Both the key and the value undergo resolution. Under common YAML 1.1 rules, `on` resolves to boolean true, so the result can be a mapping like `{True: True}`. Under YAML 1.2 Core, the key is the string `on` and the value is boolean true. A workflow editor using a 1.1-style loader may therefore corrupt a GitHub Actions trigger key during serialization.

Prove it with the exact loader, print key types, and inspect serialized output. Quote `'on'` when necessary and add a regression fixture to configuration editing tools. Also test typed destinations: a parser can retain some 1.1 conversions only when decoding into a boolean field.

### 2. Why does Kubernetes reject a manifest that is valid YAML?

**Answer:** Parsing establishes a data structure; Kubernetes validates an API request. Consider `spec.replicas: "3"`: the scalar is a valid YAML string, but the Deployment field requires an integer. Other causes include unknown fields, missing required fields, unserved API versions, uninstalled CRDs, immutable updates, selector mismatches, RBAC, or admission rejection.

Read the API error and field path, inspect rendered types, and reproduce with strict server dry run against the correct context and namespace. Use schema validation for fast feedback, but keep server checks for API semantics. A manifest accepted by the API can still fail later because of image pulls, scheduling, or application startup.

### 3. Why do Helm overrides replace nested structures or lists?

**Answer:** Identify the merge mechanism first. YAML `<<` is shallow, ordinary Helm 3 values maps merge recursively, and Helm lists replace earlier lists. An override of `[8443]` replaces `[8080, 9090]`; Helm does not infer how to merge list elements.

If a nested map disappears, inspect whether a later input changed its type or set it to null. Then check upgrade reuse options and template functions that build a different structure. Compare effective values with rendered output. If the chart needs named overrides, a map keyed by stable names can be easier to manage than a list, provided the template and API ordering requirements allow it.

### 4. How do you debug a shell command corrupted by YAML folding?

This fragment can parse successfully but execute incorrectly:

```yaml
run: >-
  echo starting
  echo finished
```

**Answer:** It constructs `echo starting echo finished`, a single shell command. Inspect the scalar after parsing or rendering, then run a shell syntax check and a safe reproduction. Use literal `|` or `|-` when command boundaries depend on newlines. Check blank lines and more-indented content before assuming every folded line becomes a space.

For a backslash-continued command, YAML folding can replace the newline after `\` with a space, changing what the shell escapes. Fix the YAML representation and the shell command together; do not add random quoting until one execution appears to work.

### 5. How would you validate thousands of infrastructure YAML files?

**Answer:** Build a dependency-aware pipeline by consumer: workflows, charts, overlays, and native product configuration need different checks. Reject duplicates early, render affected targets with pinned inputs, validate all generated documents against versioned schemas, and run product and policy checks. Cache immutable dependencies and parallelize independent work.

Use isolated server dry runs where cluster behavior matters, retain redacted artifacts, and run periodic full validation to verify impact analysis. Define failure behavior for missing schemas and unavailable validators. Keep production credentials outside untrusted PR execution. After deployment, verify controller convergence and service health because static validation cannot prove runtime success.

### 6. What causes YAML deserialization vulnerabilities, and how do you mitigate them?

**Answer:** Object construction can give data the authority to select runtime classes or behavior. An attacker may exploit permitted constructors and available classes; this is separate from alias-based resource exhaustion. The exact risk depends on the loader configuration and version.

Use a safe-data loader, restrict tags and custom constructors, validate the result, patch dependencies, and limit bytes, depth, aliases, CPU, and memory. Test malicious fixtures in isolation. Do not claim that `safe_load` alone prevents denial of service, duplicate ambiguity, or later shell/template injection.

### 7. Why do Helm-rendered, Git-stored, and live Kubernetes configurations differ?

**Answer:** Compare three stages with matching provenance. Git may contain chart templates and input values rather than the final desired resource. Rendering depends on the chart artifact, dependencies, values order, release name, namespace, capabilities, and post-renderers. The API then adds defaults and admission mutations, and controllers update live state afterward.

Reproduce the GitOps controller's render from its exact source revision. Compare that with the relevant Helm release manifest when Helm actually manages the release, then compare desired objects with live objects using schema-aware normalization and field ownership. Argo CD can use Helm solely for rendering, so a Helm release record may not exist.

Classify each difference: intended default, another controller's field, serialization noise, nondeterministic output, or unauthorized drift. Fix the source or ownership contract, reconcile through the normal workflow, and verify health. Use narrow diff exclusions only for differences whose cause and owner are understood.
