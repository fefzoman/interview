# Ansible — SRE Expert Checklist

A configuration rollout succeeds on most servers, fails on a few, and disconnects the controller from one availability zone. Knowing Ansible syntax is only the beginning of handling that incident. An SRE must explain which operations actually completed, whether retrying is safe, and how to restore service without widening the outage.

This guide follows the interview checklist and assumes basic Linux, SSH, YAML, and infrastructure experience. Examples target Linux unless stated otherwise. YAML snippets are fragments unless introduced as complete files; application paths, service names, and endpoints are illustrative. Confirm behavior against the installed `ansible-core`, collection, Python, and operating-system versions. In particular, the [ansible-core 2.19 porting guide](https://docs.ansible.com/projects/ansible-core/2.19/porting_guides/porting_guide_core_2.19.html) documents significant templating and conditional changes; an engine upgrade requires compatibility testing.

## Contents

1. [Fundamentals & Architecture](#1-fundamentals--architecture)
2. [Inventory & Configuration](#2-inventory--configuration)
3. [Playbooks & Task Execution](#3-playbooks--task-execution)
4. [Roles, Collections & Reusability](#4-roles-collections--reusability)
5. [Execution Internals & Performance](#5-execution-internals--performance)
6. [Security & Secrets](#6-security--secrets)
7. [Infrastructure & SRE Automation](#7-infrastructure--sre-automation)
8. [Reliability & Failure Handling](#8-reliability--failure-handling)
9. [Testing, CI/CD & Observability](#9-testing-cicd--observability)
10. [Expert Interview Scenarios](#10-expert-interview-scenarios)

## 1. Fundamentals & Architecture

### Agentless automation and push versus pull

Ansible normally runs on a **control node**, connects to **managed nodes**, and executes work without a permanently installed Ansible agent. Agentless does not mean dependency-free: typical Linux modules require a supported remote Python interpreter, SSH access, and suitable permissions. Windows commonly uses PowerShell through WinRM or PSRP; network devices can use persistent CLI or API connections. Cloud modules often execute on the controller against an API rather than inside a virtual machine.

In the usual **push** model, a controller initiates a run and selects its targets. This fits deployments, coordinated maintenance, and incident response. In a **pull** model, nodes periodically retrieve and apply configuration. `ansible-pull` can fetch a repository and execute locally, but scheduling, trusted revisions, credentials, locking, and reporting still need design. A completed push run does not continuously reconcile subsequent drift.

### Core components

The **inventory** defines hosts, groups, connection details, and associated variables. A **playbook** contains plays that map an ordered set of tasks to selected hosts. A **module** implements an operation such as installing a package or managing a file. **Plugins** extend behavior around those operations: connections, inventory discovery, lookups, filters, strategies, callbacks, and more.

Roles organize reusable automation; collections package related roles, modules, and plugins under a namespace. These are content boundaries, not extra execution agents. A handler is a task scheduled by a change notification, usually to reload or restart a service after configuration changes.

### `ansible-core`, community Ansible, and compatibility

`ansible-core` provides the execution engine, command-line tools, and `ansible.builtin` content. The community `ansible` package combines a compatible core release with a curated collection set. Ansible Automation Platform adds supported orchestration and operational services; it is not another name for the community package.

Collection releases have independent versions and may declare `requires_ansible`; modules can also require SDKs and supported remote runtimes. Pin the engine, collections, controller libraries, and image together. Check controller Python and managed-node Python support separately; neither compatibility nor security maintenance follows merely from a successful installation. Consult the [release and maintenance policy](https://docs.ansible.com/projects/ansible/latest/reference_appendices/release_and_maintenance.html).

### What happens during a task

For a typical Python module over SSH, execution follows this path:

1. The controller loads configuration and inventory, resolves the play's host set, and prepares tasks.
2. For each host, it resolves variables, evaluates conditions, and templates arguments. Lookups and filters generally run on the controller.
3. The strategy schedules a worker; the action plugin prepares the operation and invokes connection and become plugins as needed.
4. The controller establishes or reuses SSH, discovers the interpreter when necessary, and prepares the module and its required module utilities. Ordinarily it transfers a payload to a remote temporary location; pipelining can avoid that file transfer for eligible modules.
5. The remote interpreter executes the module, which inspects state and may modify it. Results return as structured data, usually JSON.
6. The controller processes results, applies failure/change expressions, registers values, queues handler notifications, emits callback events, and cleans up temporary resources where applicable.

This is a common path, not a universal one: `debug` can complete on the controller, `raw` does not use the normal Python module wrapper, and API modules may run locally. See [module architecture](https://docs.ansible.com/projects/ansible/latest/dev_guide/developing_program_flow_modules.html).

### Idempotency and desired state

An idempotent operation reaches a desired state and makes no additional change when repeated against that state. `package: state=present` expresses a state; `shell: echo value >> file` repeats an action. Module selection helps, but input stability and external systems determine whether the whole playbook converges. Timestamps in templates, moving package targets, or unconditional restarts can change every run.

Ansible mixes declarative module arguments with imperative task ordering. It does not calculate and atomically commit a complete infrastructure transaction. `changed_when: false` changes reporting and handler notifications; it cannot make a destructive command safe to repeat.

### Comparing configuration tools

| Tool | Typical operating model | Interview distinction |
| --- | --- | --- |
| Ansible | Controller initiates ordered tasks over connections or APIs | Strong fit for orchestration and configuration; periodic reconciliation must be arranged |
| Terraform | Providers manage resource lifecycle using configuration and state | Strong fit for provisioning and dependency planning; state and resource ownership matter |
| Puppet | Commonly agents periodically apply compiled catalogs | Ongoing policy convergence is central; orchestration is a separate concern |
| Chef | Commonly clients execute recipes and converge resources | Configuration logic is code; client runs and server services shape operations |
| Salt | Commonly master/minion execution and state convergence | Event-driven operation and fast remote execution; agentless modes also exist |

These are typical deployments, not absolute product limits. Choose by lifecycle, orchestration, connectivity, operating model, and existing investment rather than by whether a tool can technically create a VM.

## 2. Inventory & Configuration

### Static inventory, groups, and targeting

An INI inventory can express hosts and nested groups:

```ini
[web]
web01 ansible_host=192.0.2.11
web02 ansible_host=192.0.2.12

[db]
db01 ansible_host=192.0.2.21

[production:children]
web
db
```

The equivalent YAML structure uses `hosts`, `children`, and `vars`:

```yaml
all:
  children:
    production:
      children:
        web:
          hosts:
            web01:
              ansible_host: 192.0.2.11
            web02:
              ansible_host: 192.0.2.12
        db:
          hosts:
            db01:
              ansible_host: 192.0.2.21
```

A child host belongs to its parent groups. Hosts may belong to multiple groups, but group membership does not create isolated copies of a host. An inventory alias such as `web01` is the identity used for targeting; `ansible_host` is its connection address. Duplicate aliases for one machine can cause unintended repeated work. YAML also avoids some INI inline-value typing surprises. See [inventory construction](https://docs.ansible.com/projects/ansible/latest/inventory_guide/intro_inventory.html).

Patterns select that host set: `web:db` is a union, `web:&production` an intersection, and `production:!db` an exclusion. Quote patterns at the shell. A play's `hosts` and CLI `--limit` intersect; `--limit` does not add hosts absent from the inventory. Preview targets before a rollout:

```bash
ansible-playbook -i inventories/production/hosts.yml site.yml --list-hosts --limit 'web:&production'
```

### Variables, facts, and scope

Keep environment configuration under `inventories/production/group_vars/` and host exceptions under `host_vars/`. Role defaults provide overridable behavior; ordinary inventory variables describe the deployment. Avoid defining the same setting in many places just because precedence permits it.

**Facts** are discovered properties such as OS details and interfaces, normally gathered at play start. Access them explicitly, for example `ansible_facts['distribution']`. Cached facts can be stale. `hostvars` exposes host variables, but another host's gathered facts exist only if gathered or loaded from cache. Magic variables describe Ansible's execution: `inventory_hostname`, `groups`, `group_names`, `ansible_play_batch`, `ansible_play_hosts`, and `ansible_play_hosts_all`. The batch is the current serial group; the all-host list represents the play's original targets.

Play variables apply to that play. Inventory variables, registered results, and `set_fact` values are associated with hosts during a run. Registered results do not automatically persist across runs. `set_fact: cacheable: true` creates both a high-precedence host variable and a lower-precedence fact eligible for caching; a later run uses the cached fact's precedence. Clearing facts does not clear the separate current-run host variable.

### Precedence: separate settings from variables

For behavior settings, the broad order from low to high is configuration, ordinary CLI options, playbook keywords, variables, then applicable direct assignments. Thus `-u deploy` does not necessarily beat inventory `ansible_user`; `-e ansible_user=incident` supplies an extra variable and can override it. Direct module arguments belong to that module's option system, rather than being a universal way to override unrelated variables. See [general precedence rules](https://docs.ansible.com/projects/ansible/latest/reference_appendices/general_precedence.html).

For common variable sources, a useful low-to-high summary is:

| Source | Consequence |
| --- | --- |
| Role defaults | Intended for consumer overrides |
| Inventory group variables, then host variables | More specific inventory information generally wins |
| Facts and cached facts | Can override ordinary inventory values |
| Play vars, vars prompts, vars files | Override those earlier sources |
| Role vars | Strong defaults; poor place for normal user configuration |
| Block vars, then task vars | Scope-specific overrides |
| `include_vars` | Runtime variable loading |
| Registered variables and `set_fact` | High-precedence host-associated results |
| Role/include-role parameters, then include parameters | Explicit invocation inputs |
| Extra variables (`-e`) | Highest variable precedence |

Inventory and playbook-adjacent `group_vars`/`host_vars` have distinct positions in the full order. Parent groups precede children; sibling groups normally merge alphabetically, with `ansible_group_priority` available in inventory to adjust ordering. Multiple inventory sources introduce load order too. Dictionaries are normally replaced, not recursively merged; use an explicit `combine` policy when merging is required. This summary is deliberately shorter than the [full variable precedence reference](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_variables.html#understanding-variable-precedence).

### Configuration discovery and environment isolation

Ansible uses the first configuration file it finds: `ANSIBLE_CONFIG`, current-directory `ansible.cfg`, `~/.ansible.cfg`, then `/etc/ansible/ansible.cfg`. It does not merge all four. Environment settings override corresponding file settings, while ordinary CLI options override configuration. Current-directory discovery has protections for world-writable directories. Diagnose the actual environment with `ansible --version` and `ansible-config dump --only-changed`.

Multiple `-i` sources are combined; combining staging and production is not isolation. Use separate inventories, credentials, cloud identities, caches, and job templates. Treat `--limit` as a targeting control within that arrangement rather than an authorization boundary. Playbook-directory variables can unintentionally affect multiple environments, so keep their use explicit.

### Dynamic inventory, constructed groups, and caching

Dynamic inventory plugins query sources such as AWS, Azure, VMware, or a CMDB. Examples include `amazon.aws.aws_ec2` and `azure.azcollection.azure_rm`; VMware plugin choice depends on the installed collection and migration status. Credentials and controller-side SDK requirements belong to the plugin's execution context. Custom plugins should provide stable identifiers, validate source data, and fail clearly if discovery is incomplete.

The `ansible.builtin.constructed` plugin derives variables and groups from existing inventory values using expressions, `groups`, or `keyed_groups`. Load source inventory before constructed inventory; constructed groups cannot depend on facts that have not been made available. Inventory caching reduces discovery API calls; fact caching avoids repeated host inspection. Their contents, freshness requirements, and invalidation policies differ even if they share backend infrastructure. See [inventory plugins and caching](https://docs.ansible.com/projects/ansible/latest/plugins/inventory/constructed.html).

Inspect dynamic inventory with `ansible-inventory --graph` and a specific `--host`. Restrict tags, subscriptions/accounts, regions, and expected host counts. If an inventory query unexpectedly returns zero or ten times the usual targets, fail the deployment preflight rather than silently treating discovery as correct.

## 3. Playbooks & Task Execution

### Plays, conditions, loops, and results

A play selects hosts and defines execution context; its tasks call modules in an order shaped by the strategy. Roles provide reusable task sets, while collections distribute those roles and modules. This fragment installs packages only on a known OS family:

```yaml
- name: Install diagnostic tools
  ansible.builtin.package:
    name: "{{ item }}"
    state: present
  loop:
    - curl
    - jq
  when: ansible_facts['os_family'] == 'Debian'
  register: diagnostic_packages
```

For modules accepting a package list, pass the list directly to reduce calls. A registered loop result contains a `results` list; it is not a single module response. Use `loop_control.loop_var` for nested reusable task files to avoid overwriting `item`. Skipped tasks can still produce registered results, so test `is skipped`, `is failed`, or expected keys before reading fields.

`set_fact` computes host-associated values for subsequent tasks. It is useful for derived data but should not become an improvised shared state database. Expressions within one `set_fact` task should not rely on another value being assigned earlier in that same mapping.

### Jinja2, filters, tests, and lookups

Templates render configuration and module arguments. Filters transform values (`default`, `combine`, `to_json`); tests evaluate properties (`defined`, `match`, `version`); lookups retrieve external data. `lookup('ansible.builtin.env', 'NAME')` reads the controller's environment, not the target's. Prefer `query()` when a lookup should return a list.

Quote a YAML value beginning with a template, such as `port: "{{ service_port }}"`. In `when`, `until`, `failed_when`, and `changed_when`, write expressions without `{{ }}`. Convert string inputs explicitly with filters such as `int` or `bool` where appropriate, and validate required inputs with assertions. Newer templating behavior enforces boolean conditions more strictly; do not rely on nonempty strings being treated as boolean policy. Never build executable template expressions from untrusted inventory or API text.

### Static imports and dynamic includes

| Operation | When content is expanded | Practical consequence |
| --- | --- | --- |
| `import_tasks` / `import_role` | Static preprocessing | Child tasks appear in task listings; imported child tasks inherit applicable tags and conditions |
| `include_tasks` / `include_role` | Runtime when reached | Supports runtime selection and loops; child content may be absent from pre-run listings |

An import's condition is evaluated for each imported task, so an early task that changes a referenced value can change later execution. An include's condition determines whether content is loaded at that point. Tags on an include normally select the include itself; use explicit inner tags or `apply` when child tasks must inherit them. Avoid using host-dependent runtime facts to select a static import path. See [reusing playbooks](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_reuse.html).

### Blocks and explicit result semantics

`block` groups tasks under shared directives. `rescue` handles a task failure, and `always` runs cleanup after the block/rescue path when execution can reach it. These constructs do not catch every failure: invalid task definitions and unreachable hosts do not trigger ordinary rescue handling. Successful rescue allows execution to continue, so explicitly fail afterward if a failed rollout must remain failed. See [block error handling](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_blocks.html).

`failed_when` defines an application's failure contract; `changed_when` defines its change contract. Lists of expressions are implicitly ANDed; write a single expression using `or` when either condition should fail. A read-only command often needs `changed_when: false`, while a mutating command needs meaningful state detection. Prefer `command` with `argv` over shell interpretation; use `shell` only for required shell behavior and handle untrusted arguments carefully.

### Handlers and restart timing

A changed task can `notify` a handler by name or a `listen` topic. Repeated notifications are deduplicated per host until handlers run. Handlers execute in definition/insertion order, not notification order. Normal flush points follow `pre_tasks`, the roles/tasks section, and `post_tasks`; `meta: flush_handlers` runs pending handlers earlier, which is useful before a health check. A handler can run again if notified after an earlier flush.

By default, a later task failure can prevent a notified handler from running on that host. `force_handlers` changes that behavior but cannot make an unreachable host execute work. Choose it according to whether restarting after a partial configuration is safe. Handlers do not provide a cluster-wide restart lock. See [handler semantics](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_handlers.html).

### Tags, delegation, `run_once`, and become

Tags select tasks for operational entry points such as configuration or verification. They can skip prerequisites, so test supported tag combinations. `always` and `never` are special tags with explicit selection/skip behavior; neither is a security boundary.

`delegate_to` changes where a task executes while `inventory_hostname` still identifies the original host. Connection and interpreter settings generally resolve in the delegated host's context. Results/facts remain associated with the original host unless the relevant operation uses `delegate_facts: true`. Multiple hosts can concurrently delegate to the same machine, causing races; serialize shared writes or use a separate orchestration play. See [delegation behavior](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_delegation.html).

Under the normal linear strategy, `run_once` runs for one host in the current batch and distributes its result to that batch. With `serial`, it can execute again in each batch. A dedicated localhost play is clearer for a true once-per-run operation. Do not rely on `run_once` under `free` for global coordination.

`become: true` enables privilege escalation; `become_user` selects the destination account but does not itself enable escalation. Scope privilege to the operations requiring it and remember that delegated local tasks can inherit escalation settings too.

### Async execution and bounded retries

`async` sets a remote job's allowed runtime. With positive `poll`, Ansible polls completion before continuing that host's task sequence. With `poll: 0`, it launches work and continues; later collect its result with `async_status`:

```yaml
- name: Start application maintenance
  ansible.builtin.command:
    argv:
      - /usr/local/bin/app-maintenance
      - --reconcile
  async: 900
  poll: 0
  register: maintenance_job

- name: Wait for maintenance
  ansible.builtin.async_status:
    jid: "{{ maintenance_job.ansible_job_id }}"
  register: maintenance_result
  until: maintenance_result is finished
  retries: 90
  delay: 10
```

This assumes an application command safe to reconcile. `async` is not a durable job queue; retain job IDs and account for controller interruption and status-file cleanup. Do not launch dependent package operations concurrently because they compete for locks. Cleanup of a status file is not cancellation. See [asynchronous actions](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_async.html).

Use `until`, `retries`, and `delay` to bound retries of a transient condition. Poll a read-only readiness endpoint rather than repeatedly performing a migration. Retry safety depends on the underlying operation, including whether it completed before the connection failed.

### Check and diff mode

`--check` asks supporting modules to predict changes; it does not prove the complete deployment will succeed. Unsupported tasks may skip, registered values may differ, and future state may be unavailable. A task with `check_mode: false` can make real changes even during a check run. `--diff` exposes supported before/after content and can reveal secrets; use `diff: false` for sensitive files. Neither mode replaces testing on representative hosts. See [check and diff mode](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_checkmode.html).

## 4. Roles, Collections & Reusability

### Role structure and interfaces

```text
roles/web/
├── defaults/main.yml
├── vars/main.yml
├── tasks/main.yml
├── handlers/main.yml
├── templates/web.conf.j2
├── files/
└── meta/
    ├── main.yml
    └── argument_specs.yml
```

Put consumer-tunable inputs in `defaults/main.yml`; reserve `vars/main.yml` for values that intentionally have stronger precedence. Tasks implement convergence, handlers perform notified changes, templates render configuration, and files hold static content. `meta/main.yml` declares role metadata and dependencies. Keep dependencies understandable: hidden package installations or restarts inside dependency chains complicate debugging.

Static `import_role` supports pre-expanded execution; dynamic `include_role` supports runtime decisions and loops. Variable exposure depends on the action's `public` behavior and applicable core configuration/version; explicitly define the intended interface rather than depending on variables leaking from roles. `meta/argument_specs.yml` validates entry-point arguments such as types, required fields, and choices before role work. Assertions still handle semantic constraints such as mutually compatible settings. See [roles and argument validation](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_reuse_roles.html).

Design a role around one operational responsibility, namespace its variables, document supported platforms, and make restart and check-mode behavior explicit. Avoid roles that silently provision cloud resources, modify database schemas, and restart clusters as one inseparable operation.

### Collections, Galaxy, and dependency management

A fully qualified collection name such as `ansible.builtin.template` or `kubernetes.core.k8s` identifies `namespace.collection.plugin`. It avoids ambiguous short-name resolution and makes ownership visible. A collection can include roles, modules, plugins, shared utilities, and documentation.

Ansible Galaxy distributes community content; private Galaxy-compatible repositories or automation hubs can distribute approved content. Version-pin roles and collections in requirements manifests, including Git roles by immutable commit when appropriate. Validate transitive dependencies too: pinning one top-level collection does not freeze Python SDKs or every dependency resolver result. Keep internal content names and configured servers explicit to avoid accidentally resolving an internal dependency from a public source.

### Custom modules and plugins

| Extension | Appropriate responsibility |
| --- | --- |
| Module | Inspect/manage a resource, validate arguments, report `changed`, support check mode when possible |
| Action plugin | Coordinate controller-side preparation and module execution |
| Filter plugin | Transform data; preferably deterministic and free of side effects |
| Lookup plugin | Retrieve controller-accessible data, files, or secrets |

Reuse an existing module before writing a custom one. A custom module should return structured results, avoid leaking secrets, distinguish transport/API failures from absent resources, and calculate changes from observed state. Controller plugins execute with the automation identity's authority, making them part of the trusted codebase.

### Execution Environments and `ansible-builder`

An Execution Environment (EE) is a container image containing the controller runtime and automation dependencies. Builder's version 3 definition can specify the base image, core/runner packages, Galaxy requirements, Python packages, and system packages. Builder can discover collection-declared controller dependencies; it does not install required libraries on managed nodes. See the [Builder 3 definition](https://docs.ansible.com/projects/builder/en/latest/definition/).

For reproducibility, pin the base image by digest, pin core, runner, collections, and Python dependencies, and use controlled package repositories. Build once, test the image, then promote the same digest through environments. Record both the automation Git revision and image digest for each job. An image alone is insufficient if runtime dependencies or remote package versions still float. Inject credentials at execution time rather than baking them into image layers.

## 5. Execution Internals & Performance

### Strategies and concurrency controls

| Control | Meaning | Operational consequence |
| --- | --- | --- |
| `linear` strategy | Hosts in the current batch advance task by task | Slow hosts create task barriers |
| `free` strategy | Each host advances independently | Fast hosts proceed; cross-host task ordering is lost |
| `host_pinned` strategy | Active hosts retain a worker slot through their play work | Limits hosts in progress; consult installed plugin behavior |
| `forks` | Controller worker concurrency | More workers consume controller and downstream capacity |
| `serial` | Hosts admitted into a play batch | Limits rollout scope and availability impact |
| `throttle` | Concurrency limit for selected work | Can restrict a bottleneck but cannot exceed forks/batch capacity |

`serial: [1, 5, '10%']` expresses a canary followed by larger batches. `throttle: 1` serializes that task within the job, not across independent jobs. Two controllers can still restart the same cluster unless a scheduler or external lock prevents it. See [strategies and batching](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_strategies.html) and the [host_pinned plugin](https://docs.ansible.com/projects/ansible/latest/collections/ansible/builtin/host_pinned_strategy.html).

### Reducing connection and fact overhead

SSH multiplexing reuses an authenticated connection; `ControlPersist` keeps the master connection alive for reuse. Pipelining reduces eligible module transfer operations, subject to module behavior and become/sudo configuration. Test it against the actual estate rather than disabling security checks to make it work. Persistent network connections have their own timeouts and session limits. See the [SSH connection options](https://docs.ansible.com/projects/ansible/latest/collections/ansible/builtin/ssh_connection.html).

Fact gathering can be a large fraction of a simple play. Disable automatic gathering when facts are unnecessary, or use `gather_subset` to request needed categories. Use a fact cache only with a freshness policy and access controls; a stale interface or hostname can produce incorrect configuration. Avoid gathering large facts from every node merely to read one host's OS family. See [facts and caching](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_vars_facts.html).

### Scaling to thousands of hosts

At scale, measure controller CPU and resident memory, SSH handshakes, DNS latency, file descriptors, bastion capacity, target-side load, package mirrors, and API limits. A rough lower bound is total host-task work divided by effective concurrency, but barriers and shared bottlenecks often dominate. Increasing forks cannot fix a saturated package repository or expensive controller-side Jinja rendering.

Partition jobs by independent failure domain or ownership, retain small deployment batches, and prevent overlapping writers to the same resources. Separate discovery and reporting from mutation when practical. Keep inventory API calls cached, avoid quadratic `hostvars` loops, batch module arguments, and minimize unnecessary controller delegation. Automation Platform execution capacity can distribute jobs; a single playbook does not automatically spread itself across every execution node.

Mitogen is a third-party execution optimization with its own support and compatibility constraints. Evaluate it against the exact core/Python/module set, including become, async, and failure paths. Benchmark built-in SSH reuse and pipelining first. Treat claims of universal speedup as hypotheses; the [Mitogen Ansible documentation](https://mitogen.networkgenomics.com/ansible_detailed.html) describes its integration and limitations.

### Callbacks and profiling

Callbacks format output or consume task events for logging and metrics. Large results, excessive verbosity, and synchronous log delivery can themselves become bottlenecks. Use a compatible profiling callback, such as `ansible.posix.profile_tasks` when available, to identify slow tasks, then distinguish transport time from actual remote work. Preserve event context while redacting sensitive values. See the [profile_tasks callback](https://docs.ansible.com/projects/ansible/latest/collections/ansible/posix/profile_tasks_callback.html).

## 6. Security & Secrets

### Ansible Vault and external secret stores

Ansible Vault encrypts files or individual variable values at rest. Vault IDs label password sources so environments can use different keys; they do not grant RBAC or make possession of the password harmless. Enable matching restrictions when relying on labels for separation. Use `ansible-vault rekey` for supported file rekeying; encrypted variable values need an appropriate decrypt/re-encrypt workflow. Rotating a Vault password does not rotate a database credential embedded inside the encrypted content. See [Ansible Vault](https://docs.ansible.com/projects/ansible/latest/vault_guide/vault.html).

External stores such as HashiCorp Vault or cloud secret managers allow short-lived, scoped retrieval with audit trails. Authenticate the automation identity through a workload identity or carefully controlled bootstrap credential, then retrieve only the required secret. Account for secret-store outages, token expiry during long jobs, and how applications receive rotated values. A lookup normally retrieves the secret on the controller, so it still becomes plaintext in process memory and potentially in rendered files.

### SSH, bastions, and privilege escalation

Use individual or workload-specific SSH identities, verified host keys or an SSH CA, and short-lived credentials when supported. A bastion/ProxyJump supplies connectivity but also becomes a capacity and trust dependency. Avoid broadly forwarding an SSH agent to untrusted hosts. Protect private keys, control sockets, and temporary credentials on the controller.

Use a dedicated automation account and scoped escalation. Ansible often executes temporary module code through an interpreter, so a narrow sudo allowlist of commands like `/usr/bin/systemctl` may not match normal module execution. Conversely, allowing unrestricted root Python execution grants broad root capability. Design privileges around the actual module execution path, hosts, and operations; do not mistake a cosmetic sudo command list for confinement. See [privilege escalation](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_privilege_escalation.html).

### `no_log` and plaintext exposure

Apply `no_log: true` to secret-bearing tasks and `diff: false` to sensitive file tasks. Avoid later debugging their registered results, persisting secrets with cached `set_fact`, or logging expanded command lines. `no_log` suppresses normal Ansible task output; it does not encrypt memory, protect destination files, scrub external service logs, or reliably hide values explicitly printed elsewhere. Debug modes require particular care. See [logging and no_log](https://docs.ansible.com/projects/ansible/latest/reference_appendices/logging.html).

Keep secrets out of CLI extra variables when process listings or shell history could expose them. Protect editor swap/backup files, temporary artifacts, inventories containing credentials, and job output retention. Use restrictive destination permissions such as quoted `'0600'`, correct ownership, and application-supported secret delivery.

### Supply chain, RBAC, and credential isolation

Review roles, plugins, collections, SDKs, and base images as executable code. Pin approved artifacts, scan images, verify signatures where supported, and control registry/repository access. A signed dependency still needs review for suitability and privilege. Never expose production credentials to untrusted pull-request automation.

AWX and Automation Platform can authorize job templates, inventories, projects, and credentials separately. Limit who can launch a job, edit its source, change inventory/limits, supply extra variables, select an EE, or attach a credential. An encrypted credential store does not stop a malicious authorized playbook from exfiltrating injected secrets. Restrict code authority and execution egress as well as credential visibility. See [AWX role-based access control](https://docs.ansible.com/projects/awx/en/24.6.1/userguide/rbac.html); consult the deployed release's documentation for its permission model.

## 7. Infrastructure & SRE Automation

### OS configuration and service lifecycle

Use package modules for software, `user`/`group` for accounts, `file` for ownership and permissions, and `systemd_service` for systemd units. Generic `package` and `service` modules help portability, but operating-system package names and service semantics still differ. Provision a minimal image or cloud-init bootstrap first when remote Python and connectivity are absent; `raw` can help bootstrap, but then normal validation and idempotency must be supplied deliberately.

For configuration deployment, render deterministically, validate before replacement, and notify a handler only on change. This fragment assumes an installed NGINX binary and a complete configuration template:

```yaml
- name: Install validated NGINX configuration
  ansible.builtin.template:
    src: nginx.conf.j2
    dest: /etc/nginx/nginx.conf
    owner: root
    group: root
    mode: '0644'
    validate: '/usr/sbin/nginx -t -c %s'
  notify: Reload nginx
```

`validate` substitutes a temporary filename before installing it. Include paths and application behavior must work with that temporary candidate; validation does not prove dependency or traffic health. Use atomic writes where the module/filesystem supports them. For SSH or firewall changes, preserve a tested administrative path and arrange recovery before applying a rule that could sever control access. See [template validation](https://docs.ansible.com/projects/ansible/latest/collections/ansible/builtin/template_module.html).

### Rolling deployments and health gates

A safe rollout follows a state machine: check spare capacity, drain one failure domain, wait for connections to finish, deploy an immutable release, validate and activate it, check local readiness and external behavior, then restore traffic. A process being active is weaker evidence than successful requests through the load balancer. Monitor error rate, latency, saturation, and dependencies before admitting another batch.

Use `serial` to bound simultaneous disruption and delegated load-balancer operations for drain/enable. Record whether this job actually drained the host before attempting compensation. Do not unconditionally enable traffic in `always`: failed or unreachable nodes should remain isolated. Section 8 shows a failure-handling fragment for this design.

### Databases and coordinated cluster upgrades

Bootstrap requires an explicit initial leader or seed selection, durable membership information, and protection against accidentally creating a second cluster. Upgrades need product-specific version compatibility, backups, restore tests, replication/lag checks, and a documented order for followers and leaders. `run_once` is not a distributed lock or a guarantee that a migration executed exactly once.

Calculate quorum from the voting membership and inspect current health before every disruptive step. Serializing one host at a time is insufficient if another voter is already unavailable. Rejoins may require resynchronization; wait for that state before moving to the next member. Database schema and data-format changes can make rollback impossible even if the old package is available.

### Kubernetes, Helm, cloud resources, and ownership

Collections such as `kubernetes.core` can manage Kubernetes resources or Helm releases; cloud collections manage infrastructure APIs. Their Python libraries, authentication, API versions, and target context must be explicit. Avoid repeatedly overwriting fields owned by operators, autoscalers, GitOps controllers, or another infrastructure tool.

Assign a single owner to each resource and important field. A common boundary is Terraform for networks, instances, and IAM; cloud-init for initial boot; Ansible for OS configuration and coordinated operations; Kubernetes controllers for ongoing application reconciliation. Ansible can provision resources too, but replacing Terraform entails consciously replacing its state, planning, import, and lifecycle workflow. Never have both tools independently manage the same security group or instance lifecycle.

### Immutable infrastructure and day-2 operations

Immutable infrastructure rebuilds and replaces an image rather than repeatedly mutating hosts. Ansible can create those images and orchestrate replacement. In-place configuration remains useful for long-lived systems and urgent operations, but record the change so subsequent rebuilds do not remove it.

Day-2 work includes patching with reboot detection and `reboot`/`wait_for_connection`, certificate rotation with trust overlap and expiry checks, backup verification, and disaster-recovery orchestration. A successful recovery playbook must restore data, dependencies, access, and traffic within RTO/RPO goals; merely recreating servers is insufficient.

## 8. Reliability & Failure Handling

### Partial failures and unreachable hosts

A failed task normally removes that host from later ordinary tasks while other hosts continue. An unreachable result means Ansible could not perform execution through its connection path; it is distinct from a module reporting failure. Neither result proves that no remote side effect occurred. Inspect actual state before retrying an operation whose response was lost.

`ignore_errors` continues after ordinary task failure; it does not cover undefined variables, syntax problems, or unreachable hosts. `ignore_unreachable` handles the connection case. `meta: clear_host_errors` can reactivate hosts for subsequent work, but it repairs neither connectivity nor state. `any_errors_fatal` stops across hosts after the failing task finishes for the current batch. `max_fail_percentage` triggers only when its threshold is exceeded, and with `serial` it applies to the batch: two failures out of four exceed 49%, but do not exceed 50%. See [error handling](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_error_handling.html).

### Compensation, rollback, and keeping failed nodes isolated

The following **task fragment** assumes the host is already drained, a previous release is recorded, and each referenced task file implements an idempotent action. It deliberately leaves an unverified host out of traffic:

```yaml
- name: Deploy the drained host
  block:
    - name: Activate candidate release
      ansible.builtin.include_tasks: activate-release.yml

    - name: Require application readiness
      ansible.builtin.uri:
        url: http://127.0.0.1:8080/ready
        status_code: 200
      register: readiness
      until: readiness.status | default(0) == 200
      retries: 12
      delay: 5

    - name: Return the verified host to traffic
      ansible.builtin.include_tasks: enable-traffic.yml

  rescue:
    - name: Restore the previous release while drained
      ansible.builtin.include_tasks: restore-release.yml

    - name: Stop rollout for investigation
      ansible.builtin.fail:
        msg: Release failed; host remains drained pending recovery verification.
```

Use this inside a play with explicit batch and abort settings, such as `serial: 1` and `any_errors_fatal: true`. The included files must own restart/handler timing, validate the restored release, and make load-balancer state changes safe to repeat. A production workflow also needs an external recovery path for unreachable hosts because rescue cannot handle disconnection. See [block failure semantics](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_blocks.html).

### Retry safety, quorum, and consistency

Use idempotency keys for APIs that support them, inspect resource state before creation, and check migration/version markers before repeating destructive operations. A local `creates` guard is useful for some commands but cannot prove a distributed database operation completed correctly. Avoid blindly retrying disk formatting, database initialization, membership changes, or irreversible schema operations.

For quorum-sensitive maintenance, use a cluster-level lock across automation jobs, inspect voters and failure domains, take one permitted member out, and wait for full recovery. A task-level throttle does not enforce the entire drain/restart/rejoin sequence. Split a leader transition into explicit, verified steps instead of hoping task barriers create a cluster protocol.

Ansible provides neither a distributed transaction nor automatic rollback. A handler can restart half the cluster before a later failure stops work. Compensation may itself fail; atomic writes protect one file replacement, not a fleet. Design each intermediate state to be serviceable or recoverable.

### Checkpoints and resumable automation

Record the target release, observed version, drained status, backup identifiers, job revision, and verification outcome in a durable system appropriate to the operation. On resume, rediscover actual state and compare it with checkpoints; controller-local facts may be gone or stale. Release activation can often be reversed; destructive data transformations require backups, forward repair, or a separately designed migration plan.

`--start-at-task` is a convenience, not a durable checkpoint. It can bypass prerequisite facts, variables, authentication, and safety checks, and it cannot reliably enter dynamically included tasks. Prefer a named, tested recovery entry point or an idempotent rerun with explicit targets and current-state preflight.

## 9. Testing, CI/CD & Observability

### Validation layers

| Layer | What it establishes | What it leaves unresolved |
| --- | --- | --- |
| `yamllint` | YAML style and configured structural rules | Module behavior and operational safety |
| `ansible-lint` | Known automation-quality problems | Correctness of the desired state |
| `ansible-playbook --syntax-check` | Basic playbook loading and syntax | Runtime includes, connectivity, and real outcomes |
| Unit tests | Custom module/plugin logic, mocked edge cases | Real API and operating-system behavior |
| Integration tests | Behavior on representative systems | Untested platform combinations and production scale |
| Idempotency run | Second convergence avoids unexpected changes | Whether the first run achieved a useful, safe state |
| Failure/recovery tests | Response to selected failure paths | Every possible partial completion |

Molecule orchestrates scenarios for creating test instances, converging roles, verifying outcomes, and cleanup; exact drivers and sequences depend on its installed version. Containers are fast but often do not accurately model systemd, kernel, SSH, firewall, reboot, and storage behavior. Use VMs or appropriate dedicated environments for those cases. Testinfra can assert resulting services, permissions, and configuration independently of the task implementation. See [Molecule's role-testing workflow](https://docs.ansible.com/projects/molecule/getting-started-roles/).

### A practical CI/CD flow

Lint and syntax-check first, then test roles against representative supported platforms using pinned content. Converge again to detect unintended changes, verify service behavior, and inject failures around restart, health checking, and rollback. Build the execution image and promote its tested digest with the reviewed playbook revision.

Before a production job, inspect resolved inventory and targets, use `--check --diff` where safe and meaningful, then run a canary and progressively larger batches. Example commands assume the repository contains the named playbook and inventory:

```bash
yamllint .
ansible-lint site.yml
ansible-playbook -i inventories/staging/hosts.yml site.yml --syntax-check
ansible-inventory -i inventories/staging/hosts.yml --graph
ansible-playbook -i inventories/staging/hosts.yml site.yml --check --diff --limit web01
```

Diff mode should be used only after sensitive tasks suppress their diffs. `--limit` reduces scope; `--start-at-task` requires the prerequisite analysis described above. GitOps can trigger Ansible jobs from reviewed changes or scheduled reconciliation, but a push-only playbook does not become a continuous controller merely because its source is in Git.

### AWX, Automation Platform, and auditability

Job templates bind approved projects, inventories, credentials, EEs, and launch settings. Workflow templates link jobs through success/failure paths and can provide gates and recovery actions. Schedules support routine patching or drift checks; inventory synchronization and project updates also need monitored failure behavior. Workflow branches are orchestration, not automatic transactional rollback. See [AWX workflows](https://docs.ansible.com/projects/awx/en/24.6.1/userguide/workflows.html), with UI and permission details checked against the deployed release.

Retain actor, source revision, image digest, inventory snapshot/target set, execution identity, timestamps, and sanitized task events. Record actual versions and health outcomes because a green job only means its declared conditions passed. Limit log access and retention according to their contents.

### Debugging and monitoring

Start with `ansible --version`, `ansible-config dump --only-changed`, a bounded inventory inspection, and a single representative host. `-vvv` helps expose connection, become, and module execution details, but review secret exposure before sharing output. Use `debug` for selected nonsecret values, `assert` for assumptions, and `ansible-doc` for the installed module's options and check-mode support.

Track queue time separately from execution time, per-task latency distributions, host failure/unreachable rates, unexpected change counts, inventory discovery failures, and rollout health. Compare against estate size so growth is not mistaken for regression. Scheduled read-only checks or check-mode jobs can detect some drift, but unsupported modules and mutable external services leave gaps. Define who investigates drift instead of automatically overwriting every difference.

## 10. Expert Interview Scenarios

### 1. Explain the complete internal lifecycle of an Ansible task

Begin with configuration discovery, inventory, host selection, and variable resolution. Explain task expansion, condition evaluation, controller templating, strategy scheduling, action plugins, connection reuse, become, module packaging/transfer or pipelining, interpreter execution, structured results, failure/change overrides, registration, handlers, callbacks, and cleanup. Then name exceptions: controller-only actions, API tasks, network connections, and `raw`. A strong answer distinguishes where code executes from which host receives the result.

### 2. Design configuration management for 10,000+ servers

Define convergence frequency, target duration, network boundaries, available capacity, and disruption budgets first. Use authoritative inventory, controlled caching, pinned EEs, regional execution placement, independent partitions, and small mutation batches. Prevent overlapping ownership. Benchmark representative tasks and measure controller CPU/memory, SSH/bastion saturation, mirrors, and API quotas. Increase concurrency only while useful throughput rises and health remains stable. Preserve per-host outcomes and a recovery path for disconnected segments.

### 3. Why does an apparently idempotent playbook always report changes?

Find the first task that changes on the second run and inspect a sanitized diff. Check template timestamps or unstable ordering, `state: latest`, unconditional `restarted`, shell commands, mode/owner normalization, API defaults, generated identifiers, and competing controllers. Confirm whether the underlying state changes or only reporting is wrong. Replace imperative work with a suitable module or explicit state comparison. Use `changed_when` only when it accurately describes the operation; hiding changes can suppress required handlers.

### 4. Resolve a variable defined in several places

Ask for exact source locations and task context. Suppose role defaults set `app_port: 8080`, inventory sets `8081`, play vars set `8082`, task vars set `8083`, and extra vars set `8084`: the extra variable wins. A role var would beat ordinary play vars but lose to those task vars and extra vars. Ordinary `-u` is a different precedence category from `-e ansible_user=...`. For a dispute, inspect inventory separately, then the effective value at the affected task without printing secrets. Explain cached-fact and sibling-group exceptions instead of relying on “last file wins.”

### 5. Design a zero-downtime deployment with automatic failure handling

State the prerequisites: redundant healthy capacity, traffic draining, compatible application/schema versions, and reliable readiness/health signals. Canary one host, validate configuration, activate a versioned release, verify locally and through traffic, then re-enable it. Gate the next batch on user-facing health. On failure, stop admission, retain isolation, and restore the old release only when data/schema compatibility permits it. Demonstrate readiness failure, rollback failure, and controller disconnection. Zero downtime is a property to prove with capacity and health, not a consequence of `serial: 1`.

### 6. Recover a cluster upgrade that updated only half the nodes

Freeze further mutation and collect actual versions, membership, quorum, leader, replication state, and upgrade checkpoints. Check the product's supported mixed-version window and whether any data-format migration occurred. If the state is supported and healthy, a controlled forward completion may be safer than downgrade. Otherwise follow the product's rollback/restore procedure. Keep healthy members serving, take a fresh backup when safe, and resume only after each recovered member is verified. Do not rerun initialization or downgrade all nodes blindly.

### 7. Prevent simultaneous restarts from breaking quorum

Count current voting members and healthy voters, not just inventory hosts. With three voters and one already down, restarting another loses majority. Serialize the whole maintenance sequence with a cluster-scoped lock, include external maintenance in the budget, verify leader handoff when needed, and wait for voting health after rejoin. `serial`, handler timing, and `throttle` help within a job; they do not coordinate independent jobs or replace the cluster's own protocol.

### 8. A playbook works on 100 hosts but stalls on 5,000

Locate whether delay is in job scheduling, inventory, fact gathering, connection setup, become, module runtime, result processing, or a task barrier. Compare task timings and inspect slow hosts without running extremely verbose output fleet-wide. Test hypotheses such as DNS failure, bastion session exhaustion, file descriptor limits, expensive Jinja/hostvars loops, package mirror saturation, throttled APIs, or log backpressure. Change one factor and compare throughput and error rate. Switching to `free` can reduce barriers but may violate required cross-host ordering.

### 9. Distribute secrets without storing plaintext credentials

Clarify where plaintext is unavoidable: an application or controller must usually use the secret in memory. Avoid persisted plaintext credentials in Git and images. Use workload identity to retrieve short-lived, least-privilege values from an external store, suppress logs/diffs, avoid fact caching, and deliver through an application-supported protected mechanism. Record retrieval/access metadata without the value. Include rotation, overlap, revocation, audit, and secret-store failure behavior. Ansible Vault is an at-rest alternative when external retrieval is unavailable, but its unlock material still needs protection.

### 10. Replace or complement Terraform, cloud-init, or operators?

Trace ownership by lifecycle and field. Terraform commonly owns provisioning; cloud-init handles first boot; Ansible configures long-lived nodes and coordinates operations; Kubernetes operators continuously reconcile application-specific state. Complement them when those boundaries are clear. Replace a component only after accounting for its state/history, drift detection, lifecycle behavior, and recovery mechanisms. Avoid running Ansible on operator-owned fields or letting Terraform and Ansible independently update one cloud resource.

### 11. Create a reproducible CI/CD execution environment

Pin the source revision, base digest, engine, runner, collections, SDKs, and system packages; constrain build repositories and retain artifacts. Build with the supported Builder schema, test the resulting image against representative targets, scan/sign it as required, and promote one immutable digest. Record injected runtime inputs and inventory alongside job metadata. Keep secrets out of layers. Reproducibility includes remote package/resource targets: a pinned controller still produces different outcomes if it installs moving `latest` releases.

### 12. Recover after automation changes production networking

Stop competing jobs and preserve logs, last-known-good configurations, and the target list. Use an independent path such as cloud console, serial console, or an established out-of-band management network. Restore routing/firewall/DNS configuration on a canary through that path, verify control and application traffic from outside, then recover remaining hosts in bounded groups. Handle unreachable nodes explicitly; ordinary rescue cannot fix the transport it needs to execute through.

For future runs, arrange a platform-native timed rollback or out-of-band recovery before mutation, validate candidate configuration, preserve a tested management path, and cancel rollback only after independent reachability and service checks. Schedule the recovery mechanism so it survives SSH and controller failure. A backup file is useful evidence, but it cannot restore itself when the only connection is gone.
