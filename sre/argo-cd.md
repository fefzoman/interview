# Argo CD — SRE Expert Checklist

A deployment can finish successfully while the Application stays `OutOfSync`, an HPA fights the reconciler, or a removed cluster label triggers deletion of production Applications. An SRE needs to explain the controllers and ownership rules behind those outcomes, then recover without turning a configuration incident into data loss.

This guide follows the supplied checklist. It assumes familiarity with Kubernetes, Git, Helm, and basic SRE practice. Examples use illustrative repositories and namespaces; YAML fragments show only the relevant fields unless labeled as complete resources. Argo CD delivers declarative GitOps continuous delivery for Kubernetes. It does not build images, replace Kubernetes workload controllers, or back up application data.

Version baseline: API details were checked against the official Argo CD documentation, including the v3.5.1 documentation available through Context7. This is a documentation baseline, not a claim about the newest supported release. Match server, CLI, CRDs, Kubernetes compatibility, and feature settings to your installation. Links using `/stable/` can change independently of this guide.

## Contents

1. [Fundamentals & GitOps Architecture](#1-fundamentals--gitops-architecture)
2. [Argo CD Components & Internals](#2-argo-cd-components--internals)
3. [Applications & Configuration](#3-applications--configuration)
4. [Synchronization & Reconciliation](#4-synchronization--reconciliation)
5. [Helm & Kustomize Integration](#5-helm--kustomize-integration)
6. [ApplicationSet & Multi-Cluster GitOps](#6-applicationset--multi-cluster-gitops)
7. [Security & Access Control](#7-security--access-control)
8. [High Availability & Reliability](#8-high-availability--reliability)
9. [Performance & Scalability](#9-performance--scalability)
10. [Observability & Troubleshooting](#10-observability--troubleshooting)
11. [Deployment Strategies & Production Operations](#11-deployment-strategies--production-operations)
12. [Expert Interview Scenarios](#12-expert-interview-scenarios)

## 1. Fundamentals & GitOps Architecture

### Declarative configuration, versioning, and reconciliation

Declarative configuration describes the intended result: a Deployment should use an image digest, expose a port, and mount a particular ConfigMap. Version control records how that intention changes. A reconciler repeatedly compares the rendered desired objects with observed cluster objects and takes permitted actions to converge them.

The complete delivery input includes more than YAML in one repository. Chart dependencies, external values, renderer versions, Kubernetes capabilities, and referenced artifacts can change the output. Reproducibility requires controlling those inputs. Git history explains intent; Kubernetes events, operation results, and workload telemetry explain what actually happened.

### Continuous Delivery, Continuous Deployment, and GitOps

| Practice | Main property | Production implication |
| --- | --- | --- |
| Continuous Delivery | Validated changes remain ready to deploy | An approval or schedule can gate production |
| Continuous Deployment | Every qualifying change deploys automatically | Automated policy makes the deployment decision |
| GitOps | Versioned declarative intent is pulled and continuously reconciled | Can implement either approval-gated or automatic delivery |

A pull request can be the approval gate, followed by automatic Argo CD synchronization after merge. GitOps does not require every development commit to reach production.

### Pull versus push and eventual consistency

A push pipeline connects to a destination and executes deployment commands. A pull reconciler observes an approved source and drives convergence using its own destination credentials. Argo CD is pull-based with respect to desired configuration, even when a central controller sends API requests to remote clusters. A Git webhook accelerates discovery; it does not carry an imperative deployment transaction.

Desired state is rendered configuration at resolved source revisions. Live state is the controller's observed Kubernetes state, usually read through caches. Both observations can lag. A Git commit, API write, scheduler decision, readiness transition, and cache update occur at different times, so consistency is eventual. It is conditional on valid inputs, access, healthy dependencies, and a policy that allows progress.

### Argo CD, Flux, and traditional CI/CD

| Approach | Typical design | Operational distinction |
| --- | --- | --- |
| Argo CD | Application-oriented comparison, UI/API, AppProjects, ApplicationSets | Strong application inventory and centralized operations; credentials and control-plane scope need isolation |
| Flux CD | Composable Kubernetes controllers for sources and reconciliation | GitOps APIs compose through CRDs; operational workflows often center on Kubernetes tooling |
| Traditional pipeline | Ordered jobs build, test, and push changes | Excellent build orchestration; ongoing drift correction must be supplied separately |

These approaches can coexist. CI builds and verifies an artifact, then proposes a digest update in Git. Argo CD deploys approved configuration. Flux provides another GitOps implementation rather than a fundamentally different definition of desired state. See the official [Flux architecture](https://fluxcd.io/flux/components/).

### Git as source of truth, drift, and self-healing

Drift means observed objects differ from rendered intent. A manual edit, webhook mutation, HPA update, or another Application can cause it. Detection reports the difference; self-healing authorizes correction of live drift under automated sync. Neither feature decides whether the desired configuration is safe.

Git is authoritative for the fields and resources assigned to GitOps. Runtime status, dynamic replica counts, generated credentials, and database contents need other owners. Define that boundary explicitly rather than trying to force every live field to equal Git.

### Argo projects and Kubernetes-native management

Argo CD reconciles desired application resources. Argo Workflows runs Kubernetes-native workflow graphs and Jobs, such as batch processing or build tasks. Argo Rollouts controls progressive workload delivery, traffic shifting, and analysis. They can integrate, but installing Argo CD does not install a canary engine or workflow runner.

`Application`, `AppProject`, and `ApplicationSet` are custom resources in the Kubernetes API. Controllers watch these objects, record status, and act on their specifications. Kubernetes RBAC protects direct CRD access; Argo CD RBAC protects actions through its API. Both access paths matter.

## 2. Argo CD Components & Internals

### Component responsibilities

| Component | Responsibility | Typical failure symptom |
| --- | --- | --- |
| API Server | REST/gRPC interfaces, UI, authentication, authorization, operation requests | UI/CLI unavailable or access denied |
| Application Controller | Compare desired/live state, assess health, execute sync and prune | Stale status, delayed drift correction, stalled operations |
| Repository Server | Fetch/cache source and generate manifests through rendering tools | Manifest generation errors or slow comparisons |
| Redis | Cache generated manifests and application-related data | Cache errors, slower operations, expensive reconstruction |
| ApplicationSet Controller | Generate and reconcile Application specifications | Missing, unexpectedly changed, or deleted Applications |
| Dex | Optional identity broker for upstream identity providers | SSO login failure |
| Notifications Controller | Evaluate triggers and render/send notification templates | Missing or duplicate delivery notifications |

The API server requests operations; the Application Controller performs reconciliation. Repository Server renders manifests rather than deploying them. Redis is a cache rather than the durable application registry. These distinctions identify which component to inspect first. See the [architecture overview](https://argo-cd.readthedocs.io/en/stable/operator-manual/architecture/).

Notifications evaluate triggers against Application state, select message templates, and deliver to configured subscriptions. A trigger can describe a successful sync or degraded health; a template supplies the message, and a subscription supplies recipients. Deduplicate repeated state observations so reconciliation does not become a notification storm.

### Informers, watchers, caches, and work queues

A Kubernetes list establishes an initial resource view; watches deliver subsequent events. Informer-backed caches allow comparisons without reading every object from the API on every loop. Watch disconnects, relists, permissions, and large object sets affect freshness and memory use.

Events enqueue reconciliation keys. Workers process those keys and may retry transient failures with backoff. Duplicate events need not cause duplicate completed deployments, but they can still create load. A watch event says that something changed; the worker must read current state before deciding what to do.

### Reconciliation lifecycle

```text
CI publishes an immutable artifact and updates configuration in Git
  → webhook or periodic source check requests refresh
  → controller requests desired manifests from Repository Server
  → source revisions resolve; Helm/Kustomize/plugin produces objects
  → controller compares desired objects with cached Kubernetes state
  → manual request or automated policy starts a sync operation
  → controller applies hooks/resources and permitted deletions
  → Kubernetes controllers converge workloads
  → watches update observed state, health, and notifications
```

Generation, comparison, synchronization, and workload readiness are separate stages. An Application can render successfully but lack permission to create a Deployment, or synchronize successfully while its Pods cannot pull the image.

### Refresh, hard refresh, polling, and webhooks

A normal refresh requests an updated comparison using applicable caches. A hard refresh also invalidates cached target manifests, forcing regeneration. It can expose stale rendering results or mutable upstream dependencies, but it costs more and does not repair invalid configuration.

Polling provides periodic discovery when webhooks are missed. Webhooks reduce detection latency; authenticate their configured secrets, validate incoming events, and rate-limit the endpoint. Retain periodic reconciliation unless a deliberate alternative provides equivalent recovery. The [FAQ](https://argo-cd.readthedocs.io/en/stable/faq/) explains refresh behavior and source checks; the [webhook guide](https://argo-cd.readthedocs.io/en/stable/operator-manual/webhook/) covers provider integration.

## 3. Applications & Configuration

### Application fields and a minimal example

This complete illustrative Application requires an existing `payments` AppProject, registered destination, and repository access:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: payments-prod
  namespace: argocd
spec:
  project: payments
  source:
    repoURL: https://github.com/example/platform-config.git
    targetRevision: main
    path: apps/payments/overlays/prod
  destination:
    server: https://kubernetes.default.svc
    namespace: payments
  syncPolicy:
    automated:
      enabled: true
      selfHeal: true
      prune: false
      allowEmpty: false
    syncOptions:
      - CreateNamespace=true
```

`source` selects input and renderer configuration; `destination` identifies the target cluster and namespace; `project` selects authorization boundaries; `syncPolicy` governs automation and apply options. `CreateNamespace=true` requires permission to create the namespace. An Application's destination namespace supplies a default for eligible namespaced resources; it is not an authorization sandbox for all rendered manifests.

### Sources and target revisions

A directory supplies ordinary YAML/JSON; Helm renders templates; Kustomize builds bases and overlays; Jsonnet evaluates configuration; a Config Management Plugin supplies custom generation. Pick one generation mechanism per source and keep its inputs reviewable.

For Git, `targetRevision` can select a branch, tag, or commit SHA. Branches move; tags can be moved unless repository policy prevents it; a full commit SHA identifies a fixed Git object. For a Helm repository source, it selects a chart version or supported version constraint. A chart stored in Git uses the Git revision and `path`, rather than `chart` to select a repository chart.

A multi-source Application uses `sources`, which takes precedence over singular `source`. Each source renders separately; results form one resource set. If two sources emit the same group/kind/namespace/name, the later source wins and produces a warning. Keep unrelated services in separate Applications to retain independent ownership and failure boundaries. See [multiple sources](https://argo-cd.readthedocs.io/en/stable/user-guide/multiple_sources/).

### Health versus sync status

Sync status answers whether tracked live configuration matches desired configuration: commonly `Synced`, `OutOfSync`, or `Unknown`. Health answers whether assessed resources are operational: `Healthy`, `Progressing`, `Degraded`, `Suspended`, `Missing`, or `Unknown`, depending on the resource and assessment.

A `Synced` Deployment can be `Degraded` because no Pods become ready. An `OutOfSync` Deployment can still serve traffic correctly. Custom resource health may need a reviewed health customization; a green application badge does not establish business correctness or cover every descendant resource automatically. See [resource health](https://argo-cd.readthedocs.io/en/stable/operator-manual/health/).

### AppProjects and application namespaces

AppProjects constrain allowed source repositories, destinations, and resource kinds, and can contain roles and sync windows. Restrict cluster-scoped resources explicitly and inspect namespaced resource permissions. A broad default project is a poor tenant boundary.

The Application CR's namespace and its workload destination namespace are different concepts. Applications commonly live in `argocd`; supporting Applications in other namespaces requires explicit configuration and the project's `sourceNamespaces` permission. A tenant that can write directly to Application CRs or projects can bypass UI restrictions unless Kubernetes access is also constrained. See [projects](https://argo-cd.readthedocs.io/en/stable/user-guide/projects/) and [Applications in any namespace](https://argo-cd.readthedocs.io/en/stable/operator-manual/app-any-namespace/).

### Resource tracking and ownership

Label tracking commonly uses `app.kubernetes.io/instance`; it can collide with other tooling and has label-length limits. Annotation tracking uses `argocd.argoproj.io/tracking-id` to include application and resource identity. `annotation+label` retains an informational label while the annotation determines tracking. Defaults changed across releases; inspect the configured method before migrating.

Use distinct installation IDs when multiple Argo CD instances manage resources in the same cluster. Tracking metadata identifies management relationships; it does not replace Kubernetes authorization or ensure that two Applications cannot target the same object. Enable `FailOnSharedResource=true` where overlapping management should fail. See [resource tracking](https://argo-cd.readthedocs.io/en/stable/user-guide/resource_tracking/).

### App-of-Apps, inheritance, and promotion

An App-of-Apps root manages child Application objects. It is useful for bootstrap, but permission to edit the root repository can effectively authorize deployment across every project the child definitions can select. Treat that repository as administrative infrastructure.

Applications have no general object-oriented inheritance system. Reuse configuration through Helm values, Kustomize overlays/components, or ApplicationSet templates. Promote an immutable artifact identifier by changing environment configuration in Git. Keep environment-specific policy visible, and avoid UI parameter overrides that silently become another configuration layer.

## 4. Synchronization & Reconciliation

### Manual sync and automated policy

Manual sync requires an explicit operation; refreshes can still detect drift. Automated sync starts eligible operations when the Application is `OutOfSync`. `selfHeal` permits correction after live changes, `prune` permits deletion of tracked resources absent from desired output, and `allowEmpty` removes the automated-prune safeguard against an empty desired set. `allowEmpty` does not enable pruning by itself.

In versions supporting `automated.enabled`, `false` disables automation even if other automated flags are present. For ApplicationSet-managed Applications, change the generating template or supported override mechanism; a direct edit to the child can be restored by its owner.

An automated sync is keyed by resolved revision and application parameters. A prior successful combination is normally not synced again unless self-healing applies. A failed combination does not imply endless fresh automatic attempts; configured operation retries are a separate mechanism. Multi-source changes can also trigger synchronization that affects resources from an unchanged source. See the versioned [automated sync semantics](https://github.com/argoproj/argo-cd/blob/v3.5.1/docs/user-guide/auto_sync.md).

### Phases, waves, and hook cleanup

| Phase | Purpose |
| --- | --- |
| `PreSync` | Checks or preparation before ordinary resources |
| `Sync` | Ordinary resource application and hooks in this phase |
| `PostSync` | Validation after successful sync and required health |
| `SyncFail` | Failure handling; does not undo earlier writes |
| `PostDelete` | Cleanup after Application resources are deleted; available from v2.10 |

Ordering uses phase, then numeric wave, then kind and name. Lower waves run first; negative waves are allowed. Pruning reverses wave ordering. A wave blocked by unhealthy resources can prevent later waves. A `PreSync` hook cannot depend on an ordinary `Sync` resource merely because that resource has a lower wave.

`HookSucceeded` and `HookFailed` control cleanup by outcome; `BeforeHookCreation` removes a previous named hook before its next execution. Hooks do not run in selective resource sync. Waves order resources inside an operation; they do not automatically establish readiness dependencies between independent child Applications. See [sync phases and waves](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-waves/).

### Apply mechanisms

| Mechanism | Behavior | Consequence |
| --- | --- | --- |
| Default apply | Client-side apply using previous applied configuration | Large last-applied annotations can fail |
| `ServerSideApply=true` | API server manages apply fields | Examine field ownership; Argo CD uses force-conflicts in the documented implementation |
| `Replace=true` | Replace existing object or create it | Potentially disruptive; takes precedence over SSA |
| `Force=true,Replace=true` | Delete and recreate selected resource | Changes identity and can interrupt service |

Treat force and replace as resource-specific decisions. They are not fixes for readiness, invalid values, or missing permission. The [sync options reference](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/) documents these options and their destructive implications.

### Pruning, propagation, and CRDs

`PruneLast=true` defers pruning to a final implicit wave after earlier work succeeds and becomes healthy. `PrunePropagationPolicy` selects foreground, background, or orphan deletion behavior. Foreground waits for blocking dependents; background lets garbage collection remove dependents asynchronously; orphan retains dependents. Finalizers can delay completion under either cascading approach.

`SkipDryRunOnMissingResource=true` skips dry-run when a resource kind is not yet discoverable; it does not install that kind or bypass validation during the actual write. Argo CD handles the common case of a CRD included in the same sync, but discovery establishment, the operator, admission webhooks, and readiness can still require ordering or separate bootstrap stages.

Use `Prune=false` or `Prune=confirm` for selected critical objects. `Delete=false` and `Delete=confirm` concern Application deletion and address a different path. Namespace and CRD deletion can remove many dependent objects; a PVC retention annotation alone cannot protect against deleting its namespace. Test the full deletion chain and storage policy. See [sync options](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/).

### Diffing, normalization, and runtime ownership

Diffing removes irrelevant representation differences and can use configured comparison strategies. Server-side diff predicts API behavior using server-side dry-run; server-side apply controls writes. These are separate features. Inspect the installed release's [diff strategies](https://argo-cd.readthedocs.io/en/stable/user-guide/diff-strategies/) before depending on admission behavior or caching.

`ignoreDifferences` can target JSON pointers, JQ expressions, or fields associated with managed-field managers. First fix invalid fields, unstable templates, and mistaken ownership. A broad ignore rule can conceal an actual security or deployment change. See [diff customization](https://argo-cd.readthedocs.io/en/stable/user-guide/diffing/).

For an HPA-managed Deployment, prefer omitting static replicas from the desired template where practical. This Application fragment scopes an exception to one Deployment:

```yaml
spec:
  ignoreDifferences:
    - group: apps
      kind: Deployment
      name: payments
      namespace: payments
      jsonPointers:
        - /spec/replicas
  syncPolicy:
    syncOptions:
      - RespectIgnoreDifferences=true
```

Ignoring a field normally changes comparison, not what sync writes. `RespectIgnoreDifferences=true` also accounts for ignored fields during sync for existing resources. Initial creation still uses the desired object, so inspect chart defaults and field-ownership migration when introducing the rule. This avoids recurring correction of a value intentionally owned by the autoscaler.

### Selective sync, retries, and partial failures

Resource-level sync targets selected objects and skips hooks; it can bypass migration or validation assumptions. `ApplyOutOfSyncOnly=true` reduces writes to unchanged resources, but still requires comparison. Use the normal complete operation when coordinated hooks are part of correctness.

A sync is not an atomic transaction. Several objects may update before one fails, and successful writes remain. Retries must be safe against that partial state, especially for migration Jobs and external side effects. This fragment configures bounded operation retries:

```yaml
spec:
  syncPolicy:
    retry:
      limit: 5
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 3m
```

Retries help transient API failures; they cannot repair forbidden access or malformed manifests. Decide whether retries should stay on the original revision or refresh to a newer one using supported retry settings. Distinguish retry exhaustion from revision deduplication when explaining why automation stopped.

## 5. Helm & Kustomize Integration

### Rendering and Helm value precedence

Argo CD uses Helm to render a chart, then manages the resulting resources itself. It does not normally create a Helm release with Helm's upgrade history. `helm list` and `helm rollback` therefore do not describe this delivery lifecycle.

The documented precedence, highest first, is `parameters` → `valuesObject` → inline `values` → `valueFiles` → chart defaults. Later value files override earlier ones. Inspect the Application and rendered manifests rather than relying on a UI list of parameters. Supported Helm hooks map to Argo CD hooks; unsupported hooks and install-versus-upgrade assumptions need review. Defining Argo CD hooks changes Helm-hook handling, so use one explicit lifecycle model. See [Helm integration](https://argo-cd.readthedocs.io/en/stable/user-guide/helm/).

### Dependencies, OCI, and external values

Pin chart versions and dependency versions, retain dependency lockfiles where applicable, and configure registry credentials explicitly. OCI-backed Helm chart sources require the repository configuration and URL format supported by the installed release; Helm-source examples commonly omit `oci://` from `repoURL`. Registry authentication, repository trust, and version immutability are separate concerns.

This multi-source fragment takes a chart and its values from separate locations:

```yaml
spec:
  sources:
    - repoURL: https://charts.example.com
      chart: payments
      targetRevision: 1.4.2
      helm:
        valueFiles:
          - $values/environments/prod/payments.yaml
    - repoURL: https://github.com/example/platform-config.git
      targetRevision: main
      ref: values
```

`$values` refers to the root of the source with `ref: values`; it must begin the values-file path. Omitting `path` on that source avoids separately rendering its files as manifests. Fix both chart and values revisions when an exact reproducible release is required. See [external value files](https://argo-cd.readthedocs.io/en/stable/user-guide/multiple_sources/).

### Kustomize composition and generated names

A base defines reusable resources; an overlay selects a base and applies environment changes. Patches alter selected fields, while components bundle optional reusable configuration. Keep overlay changes small enough that a reviewer can identify the final image, destination, privileges, and storage effects.

ConfigMap and Secret generators normally append a content hash to names and update recognized references. A content change can therefore update a Pod template and trigger rollout. Disabling suffix hashes changes that behavior and requires another mechanism to restart consumers when necessary. Ensure custom resource references are handled correctly before pruning old generated objects. See the Kubernetes [Kustomize guide](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/).

### Deterministic output and generation failures

The same input should render the same manifests. Avoid random chart functions, current timestamps, moving remote bases, mutable image tags, and uncontrolled network lookups. Pin renderer and plugin versions, dependencies, and relevant capabilities. Never resolve non-determinism by broadly ignoring generated Secret data.

For generation errors, reproduce with the same revision, renderer version, values order, and credentials. Check syntax, dependencies, missing files, tool timeouts, memory, and temporary disk space. A cached error can survive an input-independent repair until cache expiry or hard refresh. Hard refresh is a diagnostic step after identifying the dependency; repeatedly clearing all caches can amplify an outage.

## 6. ApplicationSet & Multi-Cluster GitOps

### Generators, templates, and composition

ApplicationSet reconciles a set of Application definitions. Its generators produce parameters; its template turns each parameter set into an Application. It does not replace each generated Application's normal reconciliation.

| Generator | Input and use |
| --- | --- |
| List | Explicit elements, such as approved environment/cluster pairs |
| Git | Repository directories or structured configuration files |
| Cluster | Registered cluster Secrets and label selectors |
| Matrix | Combinations of two generator outputs, such as services × clusters |
| Merge | Merge parameter sets by configured keys, adding targeted overrides |
| Pull Request | Open proposals from a supported provider, commonly preview environments |

Go templates support expressive substitution; enable `missingkey=error` to catch missing parameters instead of silently generating an unintended definition. Composition has constraints, especially around nested generators and merge keys with Go templates; check the installed version before designing a complex matrix. Preview the generated Applications and deletion set before rollout. See [generators](https://argo-cd.readthedocs.io/en/stable/operator-manual/applicationset/Generators/) and [Go templates](https://argo-cd.readthedocs.io/en/stable/operator-manual/applicationset/GoTemplate/).

Cluster discovery is selection over registrations and labels, not automatic trust in every cluster on the network. Onboarding should establish ownership, credentials, environment labels, allowed projects, and bootstrap readiness. A changed selector can be a production deployment or deletion event. PR generators additionally need strict limits on untrusted branches, destinations, and secret access.

### Ownership and deletion policy

Generated Applications normally have an owner reference to their ApplicationSet. Removing a generator result can delete its Application; an Application deletion finalizer can then delete workloads. Kubernetes garbage collection after deleting the ApplicationSet is another deletion path.

`applicationsSync: create-update` prevents controller-driven removal of missing generated Applications when the controller permits that per-set policy. It does not by itself prevent owner-reference garbage collection. `preserveResourcesOnDeletion: true` avoids adding the workload-deletion finalizer to generated Applications; inspect existing finalizers before relying on a changed setting. Preserve Applications separately through a tested orphaning/deletion procedure when required. These settings do not stop an ordinary child sync from pruning resources.

Keep three questions separate: may the generator delete an Application, may garbage collection delete it, and may deleting that Application cascade to workloads? See [resource modification policy](https://argo-cd.readthedocs.io/en/stable/operator-manual/applicationset/Controlling-Resource-Modification/).

### App-of-Apps versus ApplicationSet

App-of-Apps declares child Applications as rendered resources and suits an explicit bootstrap inventory. ApplicationSet generates many similar definitions from discovered inputs and suits repeated service/environment/cluster patterns. Both can coexist, with an administrative root installing controlled ApplicationSets.

Neither pattern makes child workload readiness an automatic global dependency graph. Separate bootstrap stages or explicitly assessed health gates are needed when later Applications depend on an operator, admission webhook, or storage driver being operational.

### Centralized versus per-cluster control planes

A central Argo CD installation provides shared inventory and policy with fewer installations, but concentrates remote credentials, connectivity requirements, and failure impact. Per-cluster installations limit credential reach and often retain local reconciliation during regional disconnection, at the cost of fleet upgrades and policy distribution.

For a large organization, partition control planes by production trust boundary, region, or business ownership. Register remote clusters with restricted ServiceAccounts and verified API endpoints. Do not assume the convenience defaults of a registration command are least privilege. Cross-namespace projects inside one installation provide policy boundaries, while separate installations or clusters provide stronger fault and credential boundaries.

Promotion from dev to staging to production should update explicit immutable versions after each environment's checks. Avoid selecting production by a label that an untrusted team can edit, or moving one shared branch in a way that upgrades every environment at once.

## 7. Security & Access Control

### Three authorization layers

Argo CD API RBAC controls who can view, synchronize, update, delete, or execute supported actions through the API. AppProjects constrain what an Application may deploy and where. Destination Kubernetes RBAC constrains what the controller's credentials can actually do. Direct Kubernetes access to Argo CD CRDs is a fourth entry path into those decisions.

Argo CD's Casbin-style policy lines bind subjects to resources, actions, objects, and effects. Groups map external identity claims to roles. This illustrative policy grants a team visibility and sync within one project:

```csv
p, role:payments-operator, applications, get, payments/*, allow
p, role:payments-operator, applications, sync, payments/*, allow
g, sre-payments, role:payments-operator
```

The object format changes when Applications in arbitrary namespaces are enabled; verify matching semantics and default-role grants. A permissive default role can undermine carefully scoped group rules. Test both allowed and denied operations after SSO changes. See [RBAC configuration](https://argo-cd.readthedocs.io/en/stable/operator-manual/rbac/).

### SSO, Dex, OIDC, and LDAP

Argo CD can integrate directly with an OIDC provider or use Dex as an identity broker. LDAP integration commonly occurs through a supported Dex connector or an upstream identity system. Verify issuer, audience/client settings, redirect URLs, trusted roots, and group claims; authentication success does not imply correct authorization.

Keep a controlled recovery path for identity-provider outages and audit local accounts and automation tokens. Avoid retaining a broadly used initial administrator identity. Changes to group naming can unexpectedly remove access or broaden it if matching rules are loose.

### Repository and cluster credentials

Use scoped SSH keys, HTTPS tokens, or GitHub App credentials, with read-only repository access where possible. Credential templates reuse authentication for matching repository URL prefixes; review matching behavior and avoid unexpectedly broad prefixes. Rotation must cover stored Secrets, upstream issuance, expiry, and validation that refresh still works.

Cluster registrations store access configuration and credentials or supported credential-acquisition settings. Restrict their Secrets and the controller ServiceAccount. A tenant permitted to deploy Pods may inherit powerful workload identities, secret access, host mounts, or network reach even without permission to create a ClusterRoleBinding. Enforce workload policy and destination permissions as well as Argo CD project rules. See [declarative setup](https://argo-cd.readthedocs.io/en/stable/operator-manual/declarative-setup/) and the [security overview](https://argo-cd.readthedocs.io/en/stable/operator-manual/security/).

### Secrets and trust boundaries

| Approach | Where plaintext becomes available | Recovery requirement |
| --- | --- | --- |
| SOPS with an appropriate integration | Decryption tool/controller or rendering environment | Recover keys or KMS authorization; Argo CD does not decrypt SOPS files by default |
| External Secrets | Destination controller reads a secret backend | Restore backend access and controller identity |
| Sealed Secrets | Destination controller decrypts sealed resources | Preserve and protect sealing private keys |
| Vault integration | Plugin, external-secret controller, or workload identity path | Restore authentication, policies, and required secret versions |

Rendering plaintext Secrets on Repository Server can expose them through caches, API output, or diagnostics. Prefer an architecture where destination controllers or workloads retrieve secrets when that fits the requirements, and restrict all readers of rendered manifests. Encrypted Git content alone does not define who can decrypt it.

### TLS, signatures, and supply-chain controls

Verify HTTPS certificate chains and SSH host keys. Add a custom CA or approved host key explicitly instead of disabling verification to resolve an outage. Secure destination API connections and internal service traffic according to deployment capabilities and threat boundaries.

GnuPG verification can require trusted signatures for Git-sourced content through project policy, subject to documented source and feature limitations. Argo CD 3.5 introduces Source Integrity Verification through `AppProject.spec.sourceIntegrity`; the older `signatureKeys` configuration is deprecated, and the two declarations cannot be combined. Check limitations for templated ApplicationSet projects before relying on verification. See [source integrity](https://argo-cd.readthedocs.io/en/stable/user-guide/source-integrity/) and [Git GnuPG verification](https://argo-cd.readthedocs.io/en/stable/user-guide/source-integrity-git-gpg/).

Git commit verification does not verify container image signatures or automatically validate every Helm dependency. Verify Argo CD artifacts, chart provenance where supported, and workload images through the appropriate build, registry, and admission controls. See [Argo CD artifact verification](https://argo-cd.readthedocs.io/en/stable/operator-manual/signed-release-assets/).

### Plugins and privilege escalation

A Config Management Plugin executes code during rendering. Use the supported sidecar model, controlled images, non-root execution, isolated temporary directories, minimal mounts, and restricted network access. A sidecar is a process boundary, not a complete sandbox against hostile repository input. Do not share a temporary directory with Repository Server or pass Git credentials to a plugin without a demonstrated need. See [Config Management Plugins](https://argo-cd.readthedocs.io/en/stable/operator-manual/config-management-plugins/).

Protect repositories that define Applications, AppProjects, ApplicationSets, plugins, and Argo CD itself. A templated project field derived from untrusted input can turn repository write access into elevated deployment authority. Restrict remote Kustomize bases and chart dependencies too: a trusted top-level repository can consume untrusted code or manifests.

Expose UI/API only through intended ingress and authentication paths. Protect webhook secrets and rate limits, restrict Redis to authorized control-plane clients, and prevent tenant workloads from reaching internal services unnecessarily. Keep patches current and preserve audit records without logging credentials or full Secret manifests.

## 8. High Availability & Reliability

### What HA actually replicates

Replicate API Server and Repository Server behind stable Services. Use a supported Redis HA topology when cache availability warrants it; Sentinel coordinates Redis master discovery/failover, not lossless durable storage. Spread replicas across failure domains and verify that quorum and networking survive the failures being claimed.

Application Controller scale commonly uses cluster sharding. In static sharding, replica count and configured shard count must agree; merely adding replicas does not guarantee balanced work. One large destination cluster can dominate one shard. Different distribution modes have different failover behavior.

Leader election is component-specific. Replicated ApplicationSet or Notifications controllers must use their supported leadership configuration; do not assume every replica actively processes every object. Application Controller sharding is not equivalent to a single active leader with instant standby takeover. Check the selected distribution mode and recovery behavior. See [high availability](https://argo-cd.readthedocs.io/en/stable/operator-manual/high_availability/).

### Durable state, caches, and runtime availability

Argo CD CRDs, ConfigMaps, and Secrets are persisted through the management cluster's Kubernetes API, normally backed by etcd. Redis and repository working copies are reconstructible caches. Losing Redis causes errors and reconstruction load; it should not erase Application definitions or delete running workloads.

Losing a controller interrupts comparison and operations for its assigned work. Destination Deployments, StatefulSets, Services, and Pods continue under their Kubernetes controllers. Application runtime availability can still be affected by separate dependencies such as an unavailable registry, broken cluster control plane, or a half-completed deployment.

During Git or Repository Server failure, fresh desired-state generation may fail while cached information remains available. Do not promise that stale data permits every operation. Recover connectivity, credentials, or rendering capacity, then watch queue drain and cache warm-up rather than triggering fleet-wide hard refreshes.

### Disruption budgets and resource placement

Use PodDisruptionBudgets consistent with replica counts, anti-affinity or topology spread, resource requests, readiness probes, and termination grace periods. PDBs limit voluntary disruptions; they do not stop node or zone failure. A strict PDB on too few replicas can block maintenance without providing meaningful redundancy.

Capacity after a failure matters as much as replica count. Remaining Repository Servers need enough memory and temporary disk to absorb rendering load; remaining controllers need API and cache capacity to recover observations.

### Backups, upgrades, and reconstruction

Back up Applications, AppProjects, ApplicationSets, relevant ConfigMaps and Secrets, repository credentials, cluster registrations, signing trust, and encryption keys according to the recovery design. Exported configuration is sensitive. Test that exports contain the intended namespace and objects; an empty-looking successful command is not proof of a usable backup. The official [disaster recovery procedure](https://argo-cd.readthedocs.io/en/stable/operator-manual/disaster_recovery/) describes `argocd admin export` and import with matching tooling.

Git reconstructs declarative objects, not persistent volume contents, databases, external DNS, cloud identities, or lost decryption keys. Preserve separate workload backups and infrastructure state, and measure restore time through drills.

Before upgrades, review every intervening release note and upgrade guide, compatible Kubernetes versions, CRD schemas, RBAC changes, bundled renderers, and plugins. Test a representative fleet including CRDs and hooks. Rolling replicas does not make schema incompatibilities reversible; retain a recovery plan compatible with the upgraded stored resources.

## 9. Performance & Scalability

### Measure latency by stage

The useful delivery budget is **source detection → queue wait → generation/comparison → sync execution → workload readiness**. A slow health transition is not necessarily controller queueing, and a fast reconcile duration does not include all time waiting to start.

`timeout.reconciliation` and its jitter control periodic source checks; the versioned documentation describes a default of 120 seconds plus up to 60 seconds of jitter. Shorter periods increase Git and rendering load. Webhooks improve detection latency but can create bursts; retain bounded periodic recovery. Do not infer a universal reconciliation deadline from the configured polling interval.

### Queues, processors, and sharding

Status processors handle application comparison/status work; operation processors handle synchronization operations. The documented default flags are `--status-processors=20` and `--operation-processors=10`; installed packaging can override them. Increasing workers helps only while downstream capacity remains available.

Look for hot shards, large resource caches, API throttling, and CPU saturation before changing counts. Cluster-based sharding does not automatically split one enormous Application or destination cluster. Partition control planes and application ownership when the workload shape exceeds the useful scaling unit.

### Repository and cache capacity

Repository Server concurrency, including `--parallelismlimit`, bounds simultaneous generation work. Each Helm/Kustomize/plugin process consumes memory and may write substantial temporary data. Scale replicas and tune limits together with per-render cost; unlimited parallelism can turn a latency problem into OOM restarts.

Git caches reduce fetch and revision-resolution work; manifest caches reduce repeated generation. Use explicit references and stable inputs. Redis memory pressure, eviction, or connection failures reduce cache effectiveness and increase downstream load. Measure process memory, cache size, connection behavior, render concurrency, and filesystem space rather than assuming Redis failures are isolated.

### Monorepos and unnecessary refreshes

A commit in a monorepo can cause many Applications to refresh even when their effective input did not change. `argocd.argoproj.io/manifest-generate-paths` identifies relevant paths for supported optimizations. Include shared bases, libraries, and values; an incomplete declaration can conceal a meaningful input change.

Keep renderer contexts small, avoid vendoring irrelevant large trees into every generation operation, and test path filtering with representative commits. Separate update-trigger filtering from diff-ignore policy: suppressing high-frequency irrelevant status updates can save controller work, while hiding desired-state differences changes what operators can see. See [reconcile optimization](https://argo-cd.readthedocs.io/en/stable/operator-manual/reconcile/) and [monorepo scaling](https://argo-cd.readthedocs.io/en/stable/operator-manual/high_availability/#monorepo-scaling-considerations).

### Kubernetes API, memory, and fleet sizing

Client QPS/burst settings and API Priority and Fairness affect list/watch and write capacity. Increase client limits only after checking destination API latency, throttling, watch cardinality, and server capacity. More concurrency can harm unrelated controllers sharing the API.

Large Applications create large resource trees, status payloads, diff work, and controller memory requirements. Split by ownership and lifecycle where that creates meaningful operational boundaries. For thousands of Applications across hundreds of clusters, load-test realistic resource counts and churn, then size each shard, Repository Server pool, Redis tier, and management cluster. There is no reliable capacity rule based only on Application count.

## 10. Observability & Troubleshooting

### Signals and alerting

| Layer | Signals to collect | Operational question |
| --- | --- | --- |
| Applications | `argocd_app_info`, `argocd_app_sync_total`, `argocd_app_reconcile` | Which desired changes fail or take too long? |
| Controller | Workqueue depth/wait, processing rates, errors, memory, Kubernetes request metrics | Is work arriving faster than it completes? |
| Repository Server | Git request and manifest-generation latency/errors, pending work, cache behavior, disk | Is source access or rendering the bottleneck? |
| Redis | Memory, evictions, connections, failures, latency from Redis monitoring | Are caches available and effective? |
| API Server | Request rate/latency/errors, authentication and authorization failures | Can operators access the control plane reliably? |
| Workloads | Rollout events, readiness, service error rate and latency | Did deployment deliver healthy service? |

Metric names and available cache counters vary by release. Inspect actual `/metrics` output before writing queries; derive cache effectiveness from available hit/miss instrumentation and logs rather than assuming a universal hit-rate metric. Prometheus Operator ServiceMonitors require the Operator CRDs, matching labels, and endpoints; a ServiceMonitor alone does not install monitoring.

Grafana should correlate desired revision, sync result, reconciliation latency, controller saturation, and destination health. Alert on sustained degradation, failed syncs, stale observation, or delivery latency outside an agreed objective. Give normal rollouts time before paging on `Progressing` or `OutOfSync`, and cap labels to avoid cardinality growth. See [Argo CD metrics](https://argo-cd.readthedocs.io/en/stable/operator-manual/metrics/).

### CLI investigation

These commands inspect an illustrative Application; they do not deploy changes:

```bash
argocd app get payments-prod --refresh
argocd app diff payments-prod
argocd app manifests payments-prod
argocd app resources payments-prod
argocd app history payments-prod
argocd app logs payments-prod --kind Deployment --name payments
argocd app wait payments-prod --sync --health --timeout 180
kubectl -n argocd get application payments-prod -o yaml
kubectl -n payments get pods,events
kubectl -n payments describe deployment payments
```

Manifest output can contain Secrets; handle it as sensitive data. `argocd app diff` commonly uses a nonzero exit status to signal differences, so interpret its output before treating it as a tool failure. Check the [CLI reference](https://argo-cd.readthedocs.io/en/stable/user-guide/commands/argocd_app/) for flags supported by the installed CLI.

The following commands change deployment state and require an operational decision after reviewing the diff:

```bash
argocd app sync payments-prod
argocd app rollback payments-prod 12
```

The rollback identifier is an entry from Application history, not a Git commit. Argo CD documents that rollback cannot be performed with automated sync enabled. For GitOps recovery, prefer a reviewed Git change restoring the desired immutable version. A direct rollback does not update Git.

### Classify the failure before acting

| Observation | First evidence and likely directions |
| --- | --- |
| `OutOfSync` | Exact diff; pending prune, dropped fields, ownership conflict, mutation, unstable output |
| Sync `Unknown` | Application conditions; unavailable source, render error, inaccessible destination |
| `Missing` | Object existence, previous apply errors, namespace, resource identity |
| `Progressing` | Updated/ready replicas, scheduling, image pulls, PVCs, readiness probes, pending hook |
| `Degraded` | Resource status and events; rollout deadline, failed Job, controller-specific health |
| Health `Unknown` | Unsupported/custom kind, missing health assessment, unavailable observations |

Start with Application conditions and operation results, then inspect the specific resource and responsible component. Do not equate a comparison error with workload downtime.

### Authentication, admission, and rendering failures

For Git access, distinguish invalid/expired token, missing repository permission, SSH host-key mismatch, DNS/connectivity failure, and TLS trust failure. Verify the endpoint and trust configuration before rotating unrelated credentials. For rendering, reproduce exact tools and inputs, then inspect Repository Server logs and resource pressure.

For Kubernetes errors, distinguish `Forbidden` RBAC from admission rejection, API timeout, and missing resource discovery. An admission webhook timeout may come from unavailable endpoints or networking, whereas a policy denial is an intentional rejection. Broadening RBAC cannot repair a webhook's failed policy check.

### Stuck deletion and repeated ownership conflicts

When deletion stalls, inspect the Application's deletion timestamp/finalizers, operation status, destination reachability, and child resource finalizers. `resources-finalizer.argocd.argoproj.io` enables cascading resource cleanup; its `/background` variant selects background behavior. Recover the controller's ability to delete, or fix the blocked child according to its owning controller's procedure.

Removing a finalizer abandons that cleanup obligation and can leave resources running. Use it only with explicit understanding of orphaned resources, their credentials, storage, and future ownership. Never recommend blanket finalizer removal as the first response.

For shared resources, identify all Applications and all Argo CD installations targeting the same group/kind/namespace/name, then inspect tracking metadata and managed fields. Select one owner and migrate without allowing either old owner to prune the retained resource. A repeated diff can be two controllers taking turns overwriting the object.

## 11. Deployment Strategies & Production Operations

### Workload strategies and progressive delivery

A standard Kubernetes Deployment implements rolling updates. Blue-green retains separate versions and switches traffic; canary gradually exposes a candidate to users or requests. Argo CD applies the objects expressing these strategies; it does not infer traffic policy from a pair of Deployments.

Argo Rollouts adds a Rollout controller and related resources. An `AnalysisTemplate` defines measurements and success/failure rules; an `AnalysisRun` executes them during delivery. Failed analysis can abort progression and return traffic toward the stable version according to strategy. That does not necessarily revert the desired manifest in Git. Avoid a reconciliation policy that keeps promoting the same known-bad candidate. See [Rollouts analysis](https://argo-rollouts.readthedocs.io/en/stable/features/analysis/).

### Promotion, immutable artifacts, and approvals

Build once, verify the artifact, and promote its immutable digest across environments. Environment-specific configuration changes independently. Rebuilding the same source for production can change dependencies, timestamps, or compiler inputs and creates a different artifact from the one tested.

Use pull-request review, protected branches, required validation, and appropriately scoped repository permissions as approval controls. Pin chart versions, Git inputs, and image digests together in release metadata. An approved PR is meaningful only if the merged result and its dependencies are the inputs actually deployed.

### Sync windows and freezes

AppProject sync windows define allow/deny schedules for matching Applications, clusters, or namespaces. Verify timezone and manual-sync exceptions. A freeze must specify whether emergency manual operations are allowed and who can change the window or configuration.

Windows regulate sync eligibility; they do not stop direct Kubernetes writes or replace repository authorization. Check version-specific behavior for operations already running when a window changes. See [sync windows](https://argo-cd.readthedocs.io/en/stable/user-guide/sync_windows/).

### Rollback and stateful operations

A Git revert restores desired configuration through the normal audit and reconciliation path. Temporarily disabling automation and invoking history rollback can support emergency recovery, but it requires a subsequent Git correction before automation resumes. Schema compatibility, secrets, and retained artifacts determine whether either approach is safe.

Database migrations should be idempotent, observable, and compatible with both old and new application versions during rollout. Prefer expand/migrate/contract changes. A successful `PreSync` migration is not undone by a later failed Deployment. Include backup and restore procedures for changes that cannot be reversed safely.

### Cluster add-ons and self-management

CNI, CSI, DNS, ingress, certificate controllers, and policy engines have dependencies outside manifest ordering. Bring up API connectivity and foundational networking before installing components that need them. Ensure an admission webhook does not block installation or recovery of its own controller.

Self-managing Argo CD creates a bootstrap dependency: GitOps cannot repair itself if its network, credentials, CRDs, or controller are unavailable. Keep a versioned minimal bootstrap and documented independent access path. Restrict review for changes to the root application and Argo CD configuration, and test recovery from a broken self-upgrade.

## 12. Expert Interview Scenarios

A strong answer states the failure boundary, names the evidence that distinguishes plausible causes, proposes a narrow recovery, and explains how to prevent recurrence. The scenarios below build on the mechanisms above.

### 1. Explain the lifecycle from Git commit to deployment

CI publishes an immutable image and merges its reference into approved configuration. A webhook or polling discovers the source change; Repository Server resolves revisions and renders objects. Application Controller compares them with observed cluster state, checks policy, and runs an eligible sync in phase/wave order. Kubernetes workload controllers perform the rollout; watches update health and notifications. Finish by separating desired-state convergence from readiness and user-facing success.

### 2. Synchronization succeeds but the Application remains OutOfSync

Read the exact remaining diff and resolved revision first. Look for resources pending prune, invalid fields dropped by the API, mutation/defaulting, nondeterministic templates, shared ownership, or a newer commit arriving during the operation. Reproduce rendering and compare with live fields. Fix the input or ownership rule; use a narrowly scoped ignore only for intentional external ownership. Another blind sync may repeat the same outcome.

### 3. Argo CD repeatedly overwrites HPA replicas

Confirm that desired output includes `spec.replicas` and inspect which manager updates it. Remove the static field where supported, or scope `ignoreDifferences` to the relevant Deployment and enable `RespectIgnoreDifferences=true` for sync behavior. Check initial-creation defaults and field ownership. Verify that HPA scaling persists through both refresh and an unrelated full sync.

### 4. Repository Server OOMs during Helm rendering

Use previous-container logs, termination reason, memory peaks, and concurrent requests to distinguish one huge render from excessive parallelism. Reproduce the chart and dependency tree with the same values and tool version. Reduce concurrent generation, right-size requests/limits, and simplify oversized output or inputs; scale replicas if throughput requires it. Check temporary storage too. Increasing the timeout does not fix memory exhaustion.

### 5. Design for more than 100 Kubernetes clusters

Start with trust boundaries, connectivity, expected resources/churn, and delivery objectives. Partition control planes where credential blast radius or regional failure requires it; use restricted cluster identities, project boundaries, and controlled ApplicationSet selectors. Shard controller work, replicate rendering/API capacity, and monitor hot clusters rather than balancing counts alone. Include backup, fleet upgrades, capacity after failure, and a bootstrap path independent of the fleet control plane.

### 6. Thousands of Applications experience reconciliation delays

Separate detection latency, queue waiting, rendering, API writes, and readiness using timestamps and metrics. Check queue growth, worker saturation, hot shards, Git latency, cache errors, and API throttling. Reduce unnecessary refreshes and fix the bottleneck before adding workers. Validate that queue drain improves without increasing destination API latency or OOM restarts.

### 7. Recover after Redis or Application Controller loss

Confirm that Kubernetes-stored Argo CD objects and destination workloads remain intact. Restore the failed component and its credentials/configuration; validate shard assignments and Redis endpoint/authentication. Expect cache reconstruction and relisting load. Follow a small representative set of Applications through refresh and sync before expanding intervention. Do not reconstruct existing workloads by deleting and recreating every Application.

### 8. Order CRDs, operators, and dependent resources

Install the CRD early, then the controller and required webhook/certificates, then custom resources once the API and reconciler are usable. Within an Application, use explicit phases/waves and appropriate health gates. Explain why a PreSync Job cannot depend on a CRD scheduled for the later Sync phase. Missing-kind dry-run handling solves discovery checks, not operator readiness. Across Applications, use an explicit bootstrap process or tested child health gating.

### 9. Diagnose Progressing or Degraded Applications

Locate the resource contributing the status. Inspect desired/current/ready replicas, generation observation, Pod events, probes, image pulls, quota, scheduling, PVCs, and hook Jobs. For custom resources, inspect the owning operator and health script. Repair the blocked dependency; do not rewrite health to `Healthy` merely to unblock later waves. Confirm service behavior after the resource recovers.

### 10. Prevent accidental pruning of critical resources

Keep one owner, review deletion diffs, limit automatic pruning appropriately, and use confirmation or retention options on selected resources. Inspect Application finalizers, ApplicationSet policies, namespace deletion, CRD deletion, and storage reclaim behavior as separate paths. Test a bad-path or empty-generator event in a safe environment. Recoverable backups remain necessary even when deletion safeguards are configured.

### 11. Design strict multi-tenant GitOps

Map tenants to approved repositories, projects, destinations, and resource permissions. Restrict direct CRD writes, cluster credentials, workload identities, privileged Pods, and access to the Argo CD namespace. Keep project selection and bootstrap configuration under trusted ownership. Use separate installations or clusters when a shared controller credential or renderer would violate the required boundary. Demonstrate negative authorization tests as well as successful deployment.

### 12. Helm renders unexpected values

Read all configured parameters, `valuesObject`, inline values, ordered value files, and chart defaults. Verify chart revision, source references, file paths, YAML types, and parameter string handling. Reproduce with the same precedence and Helm version, then inspect `argocd app manifests`. Check whether an ApplicationSet template or higher-level root restored an unexpected override. Cache invalidation follows input verification rather than replacing it.

### 13. Manual rollback is reversed by reconciliation

Identify what was rolled back: Kubernetes workload state, Argo CD history, or Git intent. A direct Kubernetes undo changes live state while Git still requests the new version, so self-healing can restore it. Argo CD history rollback requires disabling automated sync; once automation returns, unresolved Git intent still matters. Restore the approved version in Git and check database compatibility before declaring recovery complete.

### 14. ApplicationSet unexpectedly deletes Applications

Compare generator output before and after the event: cluster labels, Git paths/files, PR closure, selectors, and naming parameters. Distinguish a successful smaller result set from a generator evaluation error; errors do not universally mean an empty desired set. Inspect policy settings, controller override permissions, owner references, and finalizers. Restore intended inputs and add tested deletion safeguards; do not assume `create-update` covers parent deletion.

### 15. Design production promotion with immutable versions

Build and sign once, store a digest and provenance, and deploy that artifact to development. Advance the same identifier to staging and production through reviewed configuration changes and environment checks. Pin chart/configuration inputs and control sync-window exceptions. Record the deployed revision and artifact digest together so an incident can identify exactly what changed and select a compatible recovery target.

### 16. Restore after complete cluster loss

Restore infrastructure, network/API access, identities, and essential storage first. Install compatible Argo CD CRDs/control plane and restore configuration or a minimal trusted bootstrap. Recover repository access, cluster registrations, encryption keys, and external-secret access. Restore application data from its own backups before allowing dependent workloads to serve traffic. Reconcile progressively, verify PVC binding and service objectives, and avoid pruning while recovered ownership is still uncertain.

### 17. Debug duplicate ownership across Applications

Find every desired definition of the resource, including other Argo CD installations. Inspect resource identity, tracking annotations/labels, installation IDs, warnings, and managed fields. Choose a single owner and plan a transfer that prevents the old owner from deleting the object. Add shared-resource failure policy and repository checks where appropriate. Do not confuse multi-source last-source-wins inside one Application with independent Applications fighting over an object.

### 18. Optimize a busy monorepo

Measure which commits refresh which Applications and where time is spent. Narrow generation paths while including shared dependencies, reduce expensive render contexts, pin inputs, and distribute webhook/polling work with supported controls. Tune rendering concurrency, cache capacity, and controller workers against measured bottlenecks. Validate with both unrelated commits and shared-base changes so optimization reduces load without missing deployments.
