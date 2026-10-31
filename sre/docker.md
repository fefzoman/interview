# Docker — SRE Expert Checklist

A container can be running while its application is unavailable, run out of memory while the host still has free RAM, or disappear while its data survives. Explaining these situations requires following the boundaries between the application, runtime, kernel, storage, and orchestration system. This guide prepares SRE candidates to explain those boundaries and investigate production failures with evidence.

The examples assume Linux containers and basic Linux administration knowledge. Docker Desktop normally runs Linux containers inside a Linux VM, so host paths, networking, and kernel inspection differ from a native Linux installation. Commands containing `CONTAINER`, `IMAGE`, `PID`, or other uppercase placeholders require replacement. Diagnostic commands may expose configuration or credentials; redact output before sharing it. Examples that build or launch applications require the indicated application files and approved image references.

Docker Engine, Compose, Buildx, BuildKit, containerd, and the kernel have separate versions. Check `docker version`, `docker info`, `docker compose version`, and `docker buildx version` before applying version-sensitive advice. The explanations below distinguish classic Docker storage from the containerd image store and standalone containers from orchestrated services.

## Contents

1. [Fundamentals & Architecture](#1-fundamentals--architecture)
2. [Linux Container Internals](#2-linux-container-internals)
3. [Images & Dockerfile](#3-images--dockerfile)
4. [BuildKit & Image Building](#4-buildkit--image-building)
5. [Docker Networking](#5-docker-networking)
6. [Storage & Persistence](#6-storage--persistence)
7. [Resource Management & Performance](#7-resource-management--performance)
8. [Security & Hardening](#8-security--hardening)
9. [Docker Compose & Orchestration](#9-docker-compose--orchestration)
10. [Registry & Image Distribution](#10-registry--image-distribution)
11. [Reliability & Production Operations](#11-reliability--production-operations)
12. [Observability & Troubleshooting](#12-observability--troubleshooting)
13. [Expert Interview Scenarios](#13-expert-interview-scenarios)

## 1. Fundamentals & Architecture

### Containers, virtual machines, and OS-level virtualization

A Linux container is a group of ordinary Linux processes with a constrained view of the system and controlled access to resources. Processes share the host kernel, even when their filesystem contains a different Linux distribution. A VM virtualizes a machine and runs its own guest kernel. Containers usually start faster and use less memory than full VMs because they do not boot a separate OS, but sharing a kernel creates a common failure and security boundary.

An image packages userspace dependencies; it does not make arbitrary kernels, CPU architectures, devices, or system calls compatible. An ARM executable still needs ARM hardware or emulation. Strong isolation between mutually untrusted tenants may require VMs, sandboxed runtimes, or separate hosts in addition to container controls.

### Docker Engine, containerd, runc, and OCI

The Docker CLI sends requests to the Docker Engine REST API over a local socket or configured remote connection. The daemon, `dockerd`, manages Docker objects and policy: containers, networks, volumes, image operations, and integrations. A Docker context selects the endpoint; the CLI can control a remote machine without making that obvious from the command itself.

The usual Linux execution path is:

```text
docker CLI / Compose
        | Engine REST API
     dockerd
        | container management
     containerd
        | runtime-v2 shim
       runc
        | Linux system calls
 application process + namespaces + cgroups + mounts
```

containerd manages container execution and, depending on the configured image store, content and snapshots. A shim supervises the container after the short-lived OCI runtime has created and started it; `runc` is not a permanent daemon for each container. This separation allows process supervision to outlive some management-process restarts, but Docker's configured shutdown behavior still matters. Engine 29.7.0 introduced experimental [embedded containerd](https://docs.docker.com/engine/daemon/embedded-containerd/), which can run inside `dockerd`; component responsibilities do not imply a fixed process layout.

OCI standardizes interoperability at several boundaries. The [Image Specification](https://specs.opencontainers.org/image-spec/) defines image manifests, configuration, filesystem changesets, and indexes. The [Runtime Specification](https://specs.opencontainers.org/runtime-spec/) defines the runtime bundle, execution configuration, and lifecycle for creating a container process. Distribution is a separate specification for exchanging content. An image manifest is not an OCI runtime `config.json`; a higher-level runtime translates image defaults and requested settings into execution configuration.

### Container lifecycle

| Operation | Meaning | SRE consequence |
| --- | --- | --- |
| `docker create` | Creates container metadata and filesystem state without starting the application | A created container has no running application PID |
| `docker start` | Starts an existing container with its saved configuration | Does not rebuild it or apply a new image tag |
| `docker run` | Creates a new container and starts it, pulling according to pull policy | Repeated invocations create distinct containers unless cleanup removes them |
| `docker stop` | Sends the configured stop signal, then force-kills after the timeout | The application needs time and signal handling to flush state |
| `docker kill` | Sends a signal, `SIGKILL` by default | The default gives the application no cleanup opportunity |
| `docker rm` | Removes container metadata and its writable layer | Persistent mounts have separate lifecycles; removal options can remove anonymous volumes |

An exited container normally retains its writable layer and logs until removed. `--rm` removes a container automatically on exit; use it deliberately because postmortem evidence and anonymous volume data can disappear. A restart reuses the same container configuration; recreation makes a new container from a desired configuration.

### Docker, containerd, CRI-O, and Podman

| Tool | Primary role | Important distinction |
| --- | --- | --- |
| Docker Engine | Application container platform with API, image, network, and volume management | Includes developer and standalone host workflows |
| containerd | Runtime service used by higher-level platforms | Kubernetes can use its CRI plugin directly |
| CRI-O | Runtime implementation focused on Kubernetes CRI | Uses OCI runtimes without implementing the Docker Engine API |
| Podman | Container and pod management with daemonless local operation | Supports rootless use and a service API, but Docker compatibility is not universal |

CRI is Kubernetes' interface to runtime services; OCI is a lower-level set of image/runtime standards. Sharing OCI images does not mean these tools have interchangeable APIs, networking implementations, or configuration files.

### Rootful and rootless architecture

Rootful Docker normally has a privileged daemon. A container using `USER 10001` still runs under that daemon's management. With user namespace remapping, container IDs map to subordinate host IDs, while `dockerd` remains privileged. With rootless Docker, both daemon and containers run in a user namespace under an unprivileged host account. Rootless networking and privileged-port handling use additional mechanisms, and effective resource limits depend on cgroup delegation. Confirm features on the deployed setup rather than assuming rootful behavior. See Docker's [rootless architecture](https://docs.docker.com/engine/security/rootless/).

## 2. Linux Container Internals

### Namespaces: which view is isolated?

Namespaces change what processes can see or name. They do not reserve CPU or RAM, and not every supported namespace is automatically isolated by Docker.

| Namespace | Isolates | Operational implication |
| --- | --- | --- |
| PID | Process IDs and process visibility | The same process has a container PID and a host PID |
| NET | Interfaces, routes, sockets, firewall state | `localhost` normally refers to this namespace |
| MNT | Mount table | A mount can be visible in one namespace and absent in another |
| UTS | Hostname and NIS domain name | Container hostname changes need not affect the host |
| IPC | System V IPC and POSIX message queues | Shared-memory applications may require explicit IPC configuration |
| USER | UID/GID mappings and capability scope | UID 0 inside need not map to host UID 0 |
| CGROUP | View of cgroup paths | Hides ancestry; actual limits come from cgroup controllers |
| TIME | Offsets for selected clocks, including monotonic and boot-time clocks | Does not provide an independently adjustable wall clock; Docker does not generally expose it as an ordinary run option |

The kernel's [namespace manual](https://man7.org/linux/man-pages/man7/namespaces.7.html) documents the distinctions. Options such as `--network host` or `--pid host` deliberately share a namespace and weaken that part of isolation.

### cgroups v1 and v2

cgroups organize processes for resource accounting and control. v1 can place controllers in separate hierarchies; v2 uses a unified hierarchy with more consistent delegation and controller interactions. A child cannot escape a tighter ancestor limit. A container configured for 2 GiB can therefore be constrained by an enclosing systemd slice before reaching its own limit.

On v2, `cpu.max` controls CPU bandwidth, `memory.max` bounds memory, `memory.high` applies reclaim pressure, `io.max` limits supported block I/O, and `pids.max` bounds tasks. `memory.current`, `memory.events`, `cpu.stat`, `io.stat`, and pressure stall information help explain what happened. v1 has different file names and semantics; identify the hierarchy through `/proc/PID/cgroup` and the mounted cgroup filesystems before inspecting counters. The [kernel cgroup v2 documentation](https://docs.kernel.org/admin-guide/cgroup-v2.html) is the reference for controller semantics.

### Capabilities, seccomp, and mandatory access control

Linux capabilities divide privileged operations into narrower permissions. Dropping `CAP_NET_RAW`, for example, restricts raw-socket operations, while `CAP_SYS_ADMIN` covers many powerful operations and should be treated as especially dangerous. Capabilities are scoped by user namespaces and interact with other kernel checks.

Seccomp filters system calls. AppArmor constrains operations through profiles that commonly use paths; SELinux enforces label-based policy. These controls complement Unix permissions and namespaces. An `EPERM` error might come from any of them, so inspect audit messages and the effective profile rather than fixing it with `--privileged` or disabling enforcement globally.

### OverlayFS, filesystem roots, and mount propagation

OverlayFS merges lower directories with a writable upper directory. Reads use the visible version; changing a lower-layer file can trigger copy-up before modifying it. Deleting a lower file records a whiteout that hides it rather than deleting the original image data. Large copy-up operations can therefore introduce latency and extra disk consumption. See the [OverlayFS design](https://docs.kernel.org/filesystems/overlayfs.html).

A mount namespace gives the process a separate mount table. A runtime typically constructs a root filesystem, mounts required pseudo-filesystems, and switches the process root using mechanisms such as `pivot_root`. `chroot` changes pathname resolution but is insufficient as a security boundary by itself. Mount propagation controls whether mount and unmount events travel between related mount trees: private mounts isolate those events, shared mounts propagate them, and slave mounts receive without propagating back. File writes and mount-event propagation are different things.

### PID 1, signals, and process exhaustion

The container's initial process is PID 1 inside its PID namespace. It must handle termination and reap adopted children, or delegate these duties to an init process such as the one Docker supplies with `--init`. Linux gives namespace PID 1 special signal behavior; relying on default termination behavior can fail when no handler is installed.

A shell wrapper that starts an application without `exec` can retain PID 1 and fail to forward signals. Zombies consume process-table entries until reaped. Thread creation also consumes task slots, so a PID limit can break an application with relatively few visible processes. Check `pids.current`, `pids.events`, thread counts, and application errors such as failure to fork.

### Isolation boundaries and failure mechanisms

An escape can result from a kernel or runtime vulnerability, but configuration can also give a process the access it needs without exploiting a bug: a Docker socket, writable host root mount, sensitive device, or excessive capabilities. Namespace isolation does not repair such grants. Patch the host kernel and runtime, reduce exposed interfaces, and use stronger isolation when the trust model requires it.

Resource failures need separate hypotheses. A cgroup OOM can kill a task despite free host memory; a global OOM occurs under host pressure. CPU quota exhaustion throttles runnable tasks. Memory pressure can cause reclaim and long stalls before any kill. PID exhaustion prevents new tasks, while file-descriptor exhaustion prevents new opens or sockets. A single symptom such as timeouts cannot distinguish these mechanisms.

## 3. Images & Dockerfile

### Layers, manifests, configuration, and content addressing

An image manifest references a configuration blob and ordered filesystem layer blobs by digest. The configuration contains settings such as command, environment, user, and rootfs information. A layer is a filesystem changeset, not a running container or full independent filesystem. Registry blobs are often compressed; their digests differ from the configuration's uncompressed layer DiffIDs. A local image ID commonly identifies the configuration, so it is not necessarily the digest used in `repository@sha256:...`.

An OCI index, or Docker manifest list, references platform-specific manifests. Pulling a multi-platform tag selects a compatible manifest, then retrieves its configuration and missing layers. Pinning the index digest can preserve multi-platform selection; pinning one platform's manifest fixes that platform's artifact. Content addressing detects changed bytes, but it does not establish who produced them or whether they are safe.

### Dockerfile instructions and runtime defaults

| Instruction | Purpose | Common interview trap |
| --- | --- | --- |
| `FROM` | Selects a base and begins a stage | A mutable tag can resolve to different content tomorrow |
| `RUN` | Executes a build step | It does not run each time a container starts |
| `COPY` | Copies build-context or stage files | Source paths are bounded by the selected context |
| `ADD` | Supports copying plus features such as local archive extraction and remote sources | Prefer `COPY` when those extra semantics are unnecessary |
| `ENTRYPOINT` | Defines the primary executable | Runtime command arguments commonly become its arguments |
| `CMD` | Supplies a default command or default entrypoint arguments | Arguments after the image can replace it |
| `ARG` | Declares build-time parameters | Scope matters; it is unsuitable for secrets |
| `ENV` | Persists environment defaults into later build steps and runtime configuration | Overrides can change runtime behavior |
| `EXPOSE` | Documents an intended container port | Does not publish a host port |
| `WORKDIR` | Sets the working directory for subsequent instructions and execution | Prefer explicit absolute paths |
| `USER` | Sets subsequent build/runtime user context | Does not automatically fix existing file ownership |
| `HEALTHCHECK` | Defines a command whose result updates container health | Standalone Docker does not restart a container solely because it becomes unhealthy |

Exec form, such as `ENTRYPOINT ["/app/server"]`, executes directly without an implicit shell. Shell form normally invokes `/bin/sh -c` on Linux, which enables expansion and pipelines but adds signal and quoting considerations. JSON exec form does not expand `$VARIABLE` by itself. If startup requires a shell script, end it with `exec /app/server "$@"` after setup. See the [Dockerfile reference](https://docs.docker.com/reference/dockerfile/) for exact instruction and override rules.

### Caching, context, and layer size

Place stable dependency inputs before frequently changed source. For example, copying a lockfile and installing dependencies before copying application code prevents ordinary source changes from invalidating the expensive dependency step. Cache decisions depend on instruction inputs; a cached package-install step does not automatically refresh because a remote repository changed.

`.dockerignore` reduces transfer size and excludes accidental inputs such as `.git`, credentials, local virtual environments, and build output. It controls the build context, not access to every external mount or a universal secret policy. Removing a secret in a later layer does not erase it from an earlier layer. Similarly, deleting large files in a later `RUN` does not shrink earlier layer blobs; create and clean temporary files within the same step or keep them outside the final stage.

### Multi-stage builds, distroless, and scratch

Multi-stage builds copy selected artifacts into a runtime stage, leaving compilers, package caches, and source behind. Distroless images contain selected runtime libraries and support files with fewer interactive tools. `scratch` is an empty base: the application must supply everything it needs, including an appropriate executable, CA certificates, and any required user or timezone files. Dynamic linkage, DNS behavior, and libc differences still matter.

This example assumes a Go module with a server under `./cmd/server` and a build that works without cgo. `GO_BASE` must be an approved Go image reference pinned to a real digest. `BUILDPLATFORM`, `TARGETOS`, and `TARGETARCH` are supplied by BuildKit:

```dockerfile
# syntax=docker/dockerfile:1
ARG GO_BASE
FROM --platform=$BUILDPLATFORM ${GO_BASE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -o /out/server ./cmd/server

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/server /server
USER 65532:65532
ENTRYPOINT ["/server"]
```

Verify that the chosen build base contains the certificate bundle and that this application tolerates an empty runtime filesystem. The image has no shell, and a health probe must not assume `curl` exists. This pattern improves size and attack surface, but the copied executable and certificates still need updates.

### Pinning and reproducibility

A tag is a mutable name; a digest identifies particular bytes. Pin base images, dependencies, build tooling, and Dockerfile frontends when reproducibility requires it. Use automated updates and tests to replace pinned content as fixes arrive. Pinning without an update process preserves vulnerabilities as reliably as it preserves functionality.

Reproducible output also depends on package indexes, timestamps, compiler behavior, source ordering, locale, network inputs, and architecture. Record resolved inputs and compare output digests across controlled rebuilds. Build once and promote the verified digest between environments instead of rebuilding separately for each environment.

## 4. BuildKit & Image Building

### Legacy builder, execution graph, and Buildx

The legacy builder follows a more sequential image-building model. BuildKit converts build definitions into a content-addressed dependency graph, executes independent work concurrently, and can omit stages that do not contribute to the requested output. Steps within a dependency chain remain ordered. See the [BuildKit architecture](https://docs.docker.com/build/buildkit/).

Buildx is a CLI interface for builder management and BuildKit features. The `docker` driver uses the daemon's integrated builder; `docker-container` runs a separate BuildKit container; `kubernetes` uses Kubernetes pods; `remote` connects to an existing BuildKit daemon. These choices affect isolation, cache persistence, platform support, and how outputs reach registries or local storage. With a separate builder, do not assume a successful build automatically appears in `docker images`; choose an appropriate `--load`, `--push`, or exporter.

### Build mounts and credential handling

| Mount or facility | Use | Constraint |
| --- | --- | --- |
| Cache mount | Reuse package downloads or compiler output | Must tolerate misses and stale content; some package tools require locked sharing |
| Bind mount | Expose build inputs temporarily to one step | Mounted content is not automatically an output layer; write intended outputs elsewhere |
| Secret mount | Supply a credential file or variable to a step | Avoid printing or copying it into resulting files |
| SSH mount | Forward an SSH agent/socket for authenticated fetches | Validate host keys and restrict which builds can use the agent |

For example, `RUN --mount=type=secret,id=repo_token,required=true ...` makes a supplied token available only to that instruction. Pass it with `docker buildx build --secret id=repo_token,src=TOKEN_FILE ...`. Build arguments and ordinary environment variables are inappropriate substitutes because they can leak through metadata, history, attestations, logs, or intermediate outputs. A malicious build step can still exfiltrate a mounted secret; temporary availability is not a trust boundary against the build itself.

### Cache exporters and distributed builds

Internal cache helps persistent builders. External cache helps ephemeral CI workers: inline cache travels in image metadata, registry cache is a separate registry artifact, and local cache exports to a directory. Other backends depend on builder support. Registry `mode=max` can retain intermediate-stage results; inline cache has narrower export behavior. Driver and image-store capabilities affect supported exporters, as documented in [cache backends](https://docs.docker.com/build/cache/backends/).

Use separate write scopes for trusted release builds and untrusted pull requests. Treat remote caches and cache mounts as build inputs: verify their origin, protect registry write permissions, and ensure tests run even when artifacts come from cache. Content hashes prevent undetected blob changes; they do not prove that the producer used approved source or tooling. Mutable shared cache references also create overwrite races between pipelines.

### Multi-platform builds and cross-compilation

QEMU emulation lets a builder execute foreign-architecture binaries, often with substantial overhead for compilation and compression. Native builders provide representative execution and performance at the cost of operating more workers. Cross-compilation produces target binaries on the build machine, but native libraries, cgo, and architecture-specific tests complicate it. Test each target on suitable hardware or a deliberately chosen emulation environment. Docker documents these [multi-platform strategies](https://docs.docker.com/build/building/multi-platform/).

The following is a CI template. Replace registry coordinates and `GO_BASE` with approved values; it assumes a capable authenticated builder and the Dockerfile above:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg GO_BASE="$GO_BASE" \
  --cache-from type=registry,ref=registry.example.com/team/app:buildcache \
  --cache-to type=registry,ref=registry.example.com/team/app:buildcache,mode=max \
  --provenance=mode=max --sbom=true \
  --tag registry.example.com/team/app:release --push .
```

An index ties the resulting platform manifests together. Verify that the registry and downstream tooling preserve the index and attestations. Provenance records build origin and inputs; an SBOM inventories components. Neither proves the artifact is vulnerability-free or that an untrusted builder told the truth. Sign the resulting digest under an approved identity and enforce verification before deployment. Detailed provenance may expose build arguments, so keep secrets out of them.

For distributed CI, place workers near source and registries, persist useful cache, bound concurrency by CPU/memory/I/O, and measure queue time separately from build time. Isolate untrusted jobs from release credentials and privileged builders. A faster build service that shares host authority across unrelated tenants creates a larger incident scope.

## 5. Docker Networking

### Drivers and packet paths

| Driver | Typical use | Tradeoff |
| --- | --- | --- |
| `bridge` | Communication between containers on one host | External access commonly uses routing/NAT and published ports |
| `host` | Share host networking on Linux | Shares ports and network visibility; publishing flags are unnecessary |
| `none` | No normal external network connectivity | Application must work without a configured network path |
| `overlay` | Cross-host networking, commonly with Swarm | Encapsulation, control-plane connectivity, and MTU matter |
| `macvlan` | Give containers distinct MAC addresses on an external L2 network | Requires network support; host-to-child communication has special restrictions |
| `ipvlan` | Share a parent MAC with separate container addressing, using L2 or L3 modes | Routing and host reachability depend on the selected mode |

See Docker's [network driver overview](https://docs.docker.com/engine/network/drivers/). On a typical rootful bridge network, a veth pair links the container network namespace to a host bridge. Packets cross that bridge toward another container or through host routing toward an external destination. Rootless networking and Desktop introduce additional forwarding layers.

Container-to-container traffic on the same user-defined bridge uses container ports directly. Publishing is for reaching a container through a host address. A container can contact a service on a reachable host address, but its `127.0.0.1` points to itself. On Linux, an explicit `host-gateway` mapping can provide a host name when supported; Docker Desktop commonly supplies `host.docker.internal`. Neither fixes a host service listening only on an unreachable address.

### Publishing, NAT, and firewalls

`-p 127.0.0.1:8080:80` publishes container port 80 through host loopback port 8080. Omitting the host address normally publishes on all host interfaces, so exposure can be broader than intended. `EXPOSE 80` alone does not publish anything. An application listening only on container loopback is generally unreachable through the container's network interface.

For common bridge NAT configurations, DNAT rewrites published inbound destinations and SNAT/masquerading rewrites outbound source addresses. Connection tracking maintains flow state. Routed modes and IPv6 configurations can have different paths, so derive the path from actual routes and rules.

Docker manages firewall rules. With the iptables backend, `DOCKER-USER` is a conventional place for user forwarding policy; by that point, published traffic has generally already undergone DNAT. The nftables backend uses different structures and does not provide the same chain contract. Host tools such as ufw may not filter published traffic as expected because forwarding and NAT occur on a different path. Inspect the active backend instead of flushing rules or disabling Docker's rule management. See [Docker packet filtering](https://docs.docker.com/engine/network/packet-filtering-firewalls/).

### DNS, overlays, and CNI

User-defined networks provide embedded DNS, typically exposed inside the container at `127.0.0.11`. Names and network aliases are scoped to attached networks. The default legacy bridge does not provide the same automatic name-based discovery. Multiple addresses can exist for a service, and cached DNS answers or long-lived connections can continue to use an old endpoint after recreation.

Docker overlay networks use VXLAN encapsulation for cross-host data paths. Check node connectivity, configured data-path ports, firewall rules, and encapsulation overhead; the commonly used defaults include TCP 2377 for Swarm management, TCP/UDP 7946 for discovery, and UDP 4789 for VXLAN. Restrict these to trusted networks. Encryption must be configured where required; an overlay is not inherently a confidential network.

Kubernetes uses CNI plugins to configure pod networking; the kubelet talks to runtimes through CRI. CNI implementations may use overlays, routing, eBPF, or other mechanisms. Docker bridge settings and Docker network aliases do not configure Kubernetes pod networking or its Service discovery.

### IPv4/IPv6, MTU, and troubleshooting

Dual-stack operation requires coherent addresses, routes, DNS answers, firewall policy, and application listeners. An AAAA answer with a broken IPv6 route can produce delays even when IPv4 works. VXLAN, VPNs, and tunnels reduce effective MTU; small pings can pass while large responses stall when path-MTU discovery is blocked.

Troubleshoot from the affected namespace: resolve the name, test the IP and port, check the listener, inspect the route, then trace packets through each hop. Compare UDP and TCP DNS, inspect retransmissions and conntrack pressure, and test with realistic packet sizes. A port conflict occurs in the namespace where the bind happens: published host ports conflict on the host, while separate network namespaces can each bind their own port 80.

## 6. Storage & Persistence

### Writable layers, overlay2, and snapshotters

The container writable layer belongs to that container. It survives stop/start but is lost on removal. Databases and other durable application data should use an explicitly managed persistent mount so recreation does not destroy them.

`overlay2` is Docker's classic storage driver using the kernel OverlayFS filesystem. With the containerd image store, containerd uses snapshotters, often the overlayfs snapshotter, instead of Docker's classic graph-driver management. Engine 29.0 and later use the containerd image store by default on fresh installations; upgrades can retain classic storage. Identify the actual backend with `docker info`. Switching stores can hide existing objects without deleting them and needs a migration plan. See the [containerd image store](https://docs.docker.com/engine/storage/containerd/).

Do not infer storage location solely from `/var/lib/docker`. With a containerd image store, content and snapshots can be under `/var/lib/containerd`, and Docker's `data-root` does not relocate every containerd path. Inspect [daemon configuration](https://docs.docker.com/engine/daemon/) and mounts. Compressed content plus unpacked snapshots also means registry size, reported image size, and physical disk consumption can differ.

### Volumes, bind mounts, and tmpfs

| Mount | Lifecycle and ownership | Best fit |
| --- | --- | --- |
| Named volume | Managed by Docker or a volume driver; independent of one container | Durable application state with explicit ownership and backup policy |
| Bind mount | An existing path on the daemon host | Configuration or host-integrated data with deliberate path coupling |
| tmpfs | Memory-backed mount removed when unmounted/container stops | Temporary data that need not persist; contents may reach swap depending on host configuration |

A bind mount is resolved on the daemon host, not the CLI machine. Prefer `--mount` for explicit semantics; a missing bind source normally errors rather than silently creating a directory as `-v` can. Mounting over a path hides the original contents. A new empty volume can be initialized from existing image contents unless copy-up is disabled. These behaviors can explain both unexpected files and apparently missing data. See [volume semantics](https://docs.docker.com/engine/storage/volumes/).

Numeric UID/GID ownership matters across the boundary. An application running as UID 10001 needs appropriate permissions on its mounted data; names in `/etc/passwd` do not remap ownership. Rootless and user namespace mappings change the corresponding host IDs. Fix ownership deliberately, including SELinux labeling where required, instead of granting world-write access.

Read-only mounts restrict writes through that mount, but do not make the data immutable to the host or another writable mount. Bind propagation is normally private; workloads that need nested host mounts require deliberate propagation settings and compatible host mounts. Verify recursive read-only behavior on the actual kernel. Docker-managed volumes use private propagation rather than exposing the same propagation controls as bind mounts.

### Backups, migrations, and abrupt termination

A volume is persistence, not a backup or replication system. Define recovery point and recovery time objectives, use database-native backups or coordinated snapshots, and store copies outside the host's failure domain. A file archive taken while a database writes may be inconsistent. Preserve numeric ownership, permissions, labels, database version requirements, and encryption keys during migration; restore to a separate target and test before cutover.

`SIGKILL`, host crashes, and power loss can interrupt writes. Journaling protects certain filesystem structures but does not guarantee application transaction consistency. Recovery depends on application logs, write ordering, `fsync`, storage durability, and tested crash recovery. Avoid running multiple independent database instances against the same data directory unless the database explicitly supports it.

### Disk usage, inodes, and filesystem performance

Disk consumption includes shared image layers, writable layers, volumes, build cache, logs, and host files. Shared layers mean summing image sizes overstates some usage; open deleted files can make `df` report more usage than `du`. Inode exhaustion prevents file creation even with free bytes. Use `docker system df -v`, filesystem byte/inode usage, and mount-aware directory inspection together.

Write-heavy workloads can suffer from OverlayFS copy-up and metadata overhead. Volumes bypass the container union layer, but the backing filesystem or remote driver still determines durability and performance. Monitor latency and queue depth alongside throughput. Never repair disk pressure by deleting files directly inside Docker or containerd storage directories; their metadata relationships are runtime-managed.

## 7. Resource Management & Performance

### CPU quotas, weights, and placement

`--cpus=1.5` limits aggregate CPU bandwidth to roughly one and a half cores' worth of execution over scheduler periods. It does not pin the application to particular cores. CPU shares on v1 and weights on v2 influence proportional allocation under contention; they are not hard limits or guaranteed reservations. `--cpuset-cpus` restricts eligible CPUs, while `--cpuset-mems` can constrain eligible NUMA memory nodes where supported.

A multithreaded service can consume its quota early in a period and then stall. Average host CPU can look low while request latency rises because the container is throttled. Correlate `cpu.stat` throttling deltas with latency, runnable threads, quota configuration, and ancestor limits. CPU affinity can reduce migration but also reduce scheduling flexibility; on NUMA systems, mismatched CPU and memory placement can increase remote-memory access costs.

### Memory, swap, and OOM

A memory limit constrains charged memory, which includes more than the application heap. Native allocations, stacks, page cache, tmpfs, and kernel-accounted allocations can matter. A reservation is a softer control, not dedicated physical RAM. On cgroup v2, distinguish the hard ceiling from reclaim/protection settings and inspect the actual files generated by the runtime.

Docker's `--memory-swap` specifies the combined memory-plus-swap allowance when used with `--memory`; setting both to the same value disables container swap allowance. Availability depends on host and controller support. Container tools such as `free` may show host-level swap information rather than the effective allowance. OOM behavior also depends on whether a single task or the entire workload is killed; losing a worker may leave PID 1 alive but the service degraded. See [Docker resource constraints](https://docs.docker.com/engine/containers/resource_constraints/).

### I/O, PIDs, and file descriptors

Block I/O weights and bandwidth/IOPS limits operate through supported controllers and devices. They are not universal limits on all filesystem activity: buffered writes, device-mapper layers, network filesystems, and storage drivers complicate attribution and enforcement. Verify limits under the actual storage stack and compare cgroup `io.stat` with device metrics.

`--pids-limit` bounds tasks; `--ulimit nofile=SOFT:HARD` sets process file-descriptor limits. Application connection pools, open files, threads, and host-wide limits can each become the bottleneck. A workload may also fail to create processes because of `RLIMIT_NPROC`, whose user-level accounting is different from a cgroup PID limit. Match the observed failure to the relevant limit before raising it.

### Startup, density, and capacity planning

Break startup into scheduling, authentication, manifest lookup, download, verification, unpacking, mount setup, process initialization, and readiness. Smaller images, shared cached layers, nearby registries, and pre-pulling approved digests reduce some components; dependency initialization can still dominate. Increasing concurrent pulls may saturate network or disk and make tail latency worse.

For density planning, budget host kernel/daemon memory, cache, logging, and peak workload demand. CPU overcommit can be reasonable for bursty services; memory overcommit creates sharper failure risks. Distinguish throttling from CPU contention, memory pressure from heap growth, and storage contention from application lock contention. Aggregate averages hide noisy neighbors and simultaneous peaks.

Benchmark representative traffic, data sizes, concurrency, and failure conditions. Report throughput, p95/p99 latency, errors, resource use, and throttling against an SLO. Measure on the intended architecture and NUMA topology, keep headroom for failures and upgrades, and repeat with a cold image/cache when startup matters.

## 8. Security & Hardening

### Least privilege and root isolation

Use a non-root application UID, drop unnecessary capabilities, enable `no-new-privileges`, and keep seccomp and mandatory access controls active. A read-only root filesystem limits filesystem modification, while explicitly writable volumes/tmpfs provide required state and temporary space. Test the application's actual write and syscall requirements.

This launch template assumes an application listening on port 8080, compatible with an immutable root filesystem and UID 10001. Replace `APPROVED_IMAGE_AT_DIGEST` with a verified image reference:

```bash
docker run -d --name api \
  --user 10001:10001 \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --cap-drop ALL --security-opt no-new-privileges=true \
  --memory 512m --cpus 1 --pids-limit 128 \
  --publish 127.0.0.1:8080:8080 \
  APPROVED_IMAGE_AT_DIGEST
```

`--privileged` broadly relaxes device and security restrictions; it should trigger an architectural review, not serve as a routine permissions fix. Host namespaces, powerful capabilities, and host mounts also enlarge access independently. Rootless Docker reduces daemon privilege; `userns-remap` reduces mapped container privilege while retaining a rootful daemon. Neither eliminates the shared-kernel boundary or replaces patching and profile enforcement.

### Docker socket, API, and remote access

Control of a rootful Docker daemon generally implies host-root authority: a client can request a container with sensitive host mounts or other powerful settings. Membership in the `docker` group is therefore privileged access. Mounting `/var/run/docker.sock` into a CI job or monitoring agent delegates that authority. A `:ro` socket mount does not make API requests read-only.

Protect remote Engine endpoints with authenticated transport, narrowly scoped access, and authorization where required. SSH contexts can reuse SSH controls; remote TCP access requires correctly verified mutual TLS and certificate lifecycle management. Do not expose an unauthenticated daemon on a network. Rootless daemon access still exposes that user's containers, files, credentials, and resources. See Docker's [daemon attack surface](https://docs.docker.com/engine/security/).

### Scanning, signatures, and registry trust

Trivy, Grype, and Docker Scout identify known vulnerable packages using inventories and vulnerability data. Results depend on package detection, database freshness, platform, and applicability. Prioritize exploitable critical paths and available fixes, document exceptions, and rescan deployed digests as new findings emerge. A clean scan is a point-in-time result, not proof of safety.

Use Cosign/Sigstore or an approved signing system to bind an image digest to a signer identity and associated attestations. Verification policy must check the expected identity and issuer or approved key, the actual digest, and required evidence; see [Cosign verification](https://docs.sigstore.dev/cosign/verifying/verify/). Merely finding a signature is insufficient. A private registry restricts distribution; it does not automatically make every stored image trusted.

### Secrets, SBOMs, provenance, and SLSA

Runtime environment variables can leak through inspection, process diagnostics, logs, or crash dumps. Prefer narrowly mounted secret files or a secret service with workload identity, then control access and rotation. Compose file-backed secrets improve delivery ergonomics but are not automatically an encrypted secret store. BuildKit secrets prevent ordinary layer persistence only when build steps use them correctly.

An SBOM answers what components an artifact contains. Provenance answers how and from which inputs it was produced. The [SLSA specification](https://slsa.dev/spec/v1.2/) supplies requirements for evaluating build and supply-chain assurance; generating an attestation flag alone does not establish a particular level. Isolate builders, constrain credentials, record inputs, verify artifacts at deployment, and maintain a rebuild/update path.

When a container escape is suspected, isolate the host, preserve evidence, revoke exposed credentials, and treat other workloads on the host as potentially affected. Rebuild from trusted infrastructure rather than assuming removal of one container repairs a compromised host.

## 9. Docker Compose & Orchestration

### Specification and basic operations

The Compose Specification models services, networks, volumes, configs, and secrets. A service defines desired container configuration; networks describe connectivity; volumes describe persistent mounts. Configs and secrets deliver files with different intent, but capabilities depend on the backend. Modern Compose uses `docker compose`; the old top-level `version` field is not a reliable feature switch.

| Command | Use | Operational distinction |
| --- | --- | --- |
| `docker compose up -d` | Create/start services and reconcile configuration changes | May recreate containers; state must live outside writable layers |
| `docker compose down` | Stop/remove project containers and ordinary project networks | `--volumes` can remove project volume data; external volumes have separate ownership |
| `docker compose run --rm SERVICE COMMAND` | Start a one-off container | Does not publish the service's ports by default |
| `docker compose exec SERVICE COMMAND` | Execute in an existing service container | Depends on that container being running |
| `docker compose config` | Render and validate the resolved configuration | Output can contain sensitive interpolated values |

### Interpolation, .env, and environment precedence

Compose interpolation happens while resolving the Compose model. Shell variables and interpolation files such as `.env` or `--env-file` provide inputs, with shell values generally taking precedence. A `.env` entry is not automatically injected into every container; the service must use it through `environment`, `env_file`, or another interpolated field. `$$` escapes a dollar sign so it can reach the container for later expansion.

For container environment conflicts, `docker compose run -e` has highest precedence. Values passed through interpolated `environment`/`env_file` come next, then literal `environment`, service `env_file`, and image `ENV` defaults. Also check whether a value is unset, empty, or inherited. See the [full precedence rules](https://docs.docker.com/compose/how-tos/environment-variables/envvars-precedence/). Use `${NAME:?message}` for required interpolation inputs and inspect the rendered model before deployment.

### Dependencies, health, and recovery

Short-form `depends_on` orders startup without establishing application readiness. Long form can wait for `service_healthy` or successful completion of a prerequisite job. A health check must verify the property the dependent service needs and use tools actually present in the image. Applications still need bounded retries and reconnection because dependencies can fail after startup.

`depends_on: ... restart: true` concerns dependency changes from explicit Compose operations; it is different from a service-level restart policy and does not create general dependency supervision. See [Compose startup ordering](https://docs.docker.com/compose/how-tos/startup-order/). Standalone Docker health state alone does not restart unhealthy containers or remove them from an external load balancer.

Service restart policies control process-exit recovery. They do not repair bad credentials, corrupt data, or incompatible migrations. Give shutdown enough time through `stop_grace_period`, ensure signals reach the application, and persist data on explicit mounts.

### Profiles, overrides, scaling, and discovery

Profiles select optional service groups, such as diagnostics or development tooling. Multiple `-f` files merge a base model with overrides, but merge rules differ by field; volumes and ports can have special behavior, and many relative paths resolve from the base file. Always review `docker compose -f BASE -f OVERRIDE config` rather than assuming the result.

`docker compose up --scale worker=3` runs multiple service containers on a host. A fixed `container_name` prevents ordinary scaling, and identical fixed host-port mappings conflict. Services discover peers by network-scoped names; use a suitable proxy/load balancer and reconnect behavior rather than assuming DNS is full traffic management. Bind mounts and local volumes do not become replicated storage when a service scales.

### Compose, Swarm, and Kubernetes

Compose primarily manages a project through a Docker endpoint; it is not a multi-node scheduler by itself. Swarm has managers, workers, desired-state services, tasks, placement, rolling updates, and overlay networks. Managers rely on quorum, while each task is one scheduled instance; standalone container behavior and Swarm service behavior differ. `docker stack deploy` does not support every modern Compose feature or build workflow.

Kubernetes supplies a broader reconciliation model with pods, controllers, Services, probes, storage integrations, and policy mechanisms. It also requires more platform operations. Choose based on availability, scaling, tenancy, and operational requirements, and distinguish declarative configuration from the controller that keeps enforcing it.

## 10. Registry & Image Distribution

### API, blobs, manifests, and authentication

A registry stores content-addressed blobs and manifests under repositories, with tags naming manifests or indexes. A typical pull checks authentication, resolves the requested reference, selects the platform manifest when needed, downloads missing configuration/layers, verifies digests, and unpacks content locally. A push uploads missing blobs before publishing manifests and tags; blob reuse reduces transfer.

The Registry HTTP API V2 uses endpoints such as `/v2/`, `/v2/NAME/manifests/REFERENCE`, and `/v2/NAME/blobs/DIGEST`. Authentication often begins with a challenge and a token request scoped to repository actions such as pull or push. Authorization is separate from identity; a valid login may still lack repository access. See the [Distribution API reference](https://distribution.github.io/distribution/spec/api/).

Private registries need trusted TLS, least-privilege credentials, scoped automation accounts, audit logs, and credential rotation. Use a credential helper and `docker login --password-stdin` with a secure input source rather than placing passwords on a command line. The daemon or builder must reach and trust the registry; successful access from a developer's shell does not establish either condition.

### Mirrors, caching, and platform resolution

Pull-through caches retain upstream content to reduce bandwidth and dependency on upstream availability. They can still require upstream access for tag resolution or cache misses; authentication and upstream rate limits remain relevant. Registry mirrors have implementation and upstream scope constraints, so do not assume one Docker daemon mirror setting transparently proxies every private registry.

When a pull reports no matching manifest, inspect the index and requested OS/architecture/variant. A tag may point only to amd64, an intended multi-platform push may have replaced the index with one manifest, or a registry/tool may not preserve required media types. Compare the resolved digest across environments rather than comparing tag strings alone.

### Retention, garbage collection, and disaster recovery

Deleting a tag or manifest does not necessarily reclaim its blobs immediately. Garbage collection removes content no longer referenced according to the registry's implementation. Coordinate GC with writes and supported online/offline procedures; races can remove content an in-flight push needs. Retention must preserve rollback digests, signatures, SBOMs, provenance, and any referrer relationships used by policy.

Enforce tag immutability for releases, but also protect digest-referenced artifacts from retention deletion. A deployment pinned by digest cannot pull an artifact that has been removed. Replication should include metadata, artifacts, and required authentication dependencies, not just some layer objects. Back up registry storage, configuration, keys, and identity dependencies, then test restoration and an actual image pull in an isolated recovery environment.

For availability, distribute registry frontends and durable storage across suitable failure domains, monitor token services as well as blob storage, and plan regional failover. Node caches improve resilience for already-present images but do not replace a registry recovery plan for replacement hosts.

### Pull-failure diagnosis

Separate DNS, TCP connectivity, TLS validation, token acquisition, repository authorization, reference resolution, download, digest verification, and unpacking. HTTP 401 can be a normal authentication challenge; repeated 401 after token exchange suggests a credential/token problem. A 403 points toward authorization, 429 toward throttling, and an unpack failure may be local disk/inode pressure rather than a registry fault. Check daemon/build-worker proxy settings and `NO_PROXY`, CA trust, time synchronization, and the exact failing component. Do not fix TLS failures by permanently disabling verification.

## 11. Reliability & Production Operations

### Restart policies, health, and shutdown

| Policy | Exit behavior | Important distinction |
| --- | --- | --- |
| `no` | No automatic restart | Default for standalone containers |
| `on-failure[:N]` | Restart on nonzero exit, optionally bounded | Does not restart simply because the daemon restarts |
| `always` | Restart after exits | A manual stop suppresses restart until manual start or daemon restart |
| `unless-stopped` | Restart unless explicitly stopped | A stopped container remains stopped across daemon restart |

Use the [restart policy documentation](https://docs.docker.com/engine/containers/start-containers-automatically/) for edge cases. Distinguish process liveness, container health, application readiness, and external reachability. A restart loop can increase dependency load or repeatedly damage state; monitor exit reasons and restart counts.

`docker stop` sends the image/configured stop signal, normally `SIGTERM`, and sends `SIGKILL` if the process does not exit within the stop timeout. A graceful application stops accepting new work, drains in-flight requests, flushes durable state, and exits. Align load-balancer draining, application deadlines, and runtime timeout. Longer timeouts do not fix a shell swallowing signals or a deadlocked shutdown handler.

### Daemon restarts, live restore, and upgrades

Without live restore, Docker normally stops containers when the daemon shuts down. Live restore can preserve Linux containers through certain daemon outages, but API management is unavailable during the outage, logging buffers can fill, and reconnection depends on compatible daemon state/configuration. Upgrade support is limited; it is not a guarantee for arbitrary major-version jumps or host reboots. See [live restore limitations](https://docs.docker.com/engine/daemon/live-restore/).

On conventional Linux installations, daemon configuration is often in `/etc/docker/daemon.json`; rootless installations use a user configuration and user systemd service. Inspect systemd unit/drop-in flags for conflicting settings. Validate a proposed configuration with the installed daemon's validation option before rollout. Reload only supported settings; other changes need a restart and a maintenance plan.

Test Engine, containerd, runtime, storage, firewall, API-client, and kernel compatibility together. Canary a host, preserve rollback inputs and backups, and check workloads after restart. Changing storage backends or restoring an old daemon binary is not automatically a safe data rollback.

### Garbage collection and logging

Set explicit image/build-cache retention and disk alerts. Before pruning, enumerate what is unused, preserve rollback images and evidence, and verify volume ownership. An unused volume can hold the only copy of a stopped application's data. Avoid blanket `docker system prune --all --volumes` in incident runbooks.

Applications commonly write to stdout/stderr; Docker's logging driver controls collection and local retention. The default `json-file` driver has no rotation unless configured. The `local` driver rotates by default; `journald` and remote drivers have different host and downstream dependencies. `docker logs` availability depends on the driver and caching setup. Blocking log delivery can stall applications; non-blocking buffers can drop logs under pressure.

For a host using `json-file`, a daemon configuration fragment can bound per-container log storage:

```json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
```

Merge with existing configuration and plan the required rollout; daemon defaults generally apply to newly created containers. Size retention using expected volume, incident needs, and collection outages. Never externally edit or rotate Docker-managed log files as a normal retention mechanism. See [logging driver configuration](https://docs.docker.com/engine/logging/configure/).

### Host failure, deployment strategies, and state

A standalone Docker daemon has no automatic host-failure scheduler or transparent active-active replacement. Restart policy cannot restart a process on another machine after its host disappears. Live restore cannot survive power loss. Production recovery needs replacement capacity, desired-state configuration, traffic routing, image availability, and a storage recovery strategy.

Blue-green deployment runs old and new versions concurrently and switches traffic after readiness checks; rolling deployment replaces subsets while maintaining capacity. Both need graceful draining, backwards-compatible database migrations, externalized state, and a rollback plan. Compose recreation on one host does not itself guarantee zero downtime. Local volumes remain local when a service moves unless an external storage system provides the required semantics.

Define RPO/RTO for databases, registry, secrets, and configuration. Restore tests should include application validation and client traffic. Hundreds of services generally require scheduling and reconciliation across hosts, failure-domain placement, admission policy, centralized observability, capacity management, and ownership boundaries, whether implemented with Swarm, Kubernetes, or another platform.

### Docker-built images in Kubernetes

Kubernetes removed its built-in dockershim in v1.24. Nodes commonly use containerd or CRI-O through CRI; Docker Engine requires an external adapter such as `cri-dockerd` when chosen as the runtime. Docker-built OCI-compatible images remain usable. The kubelet controls pod/container lifecycle, restart behavior, probes, logs, and resources through its runtime integration; Docker daemon settings do not automatically apply. See the [Kubernetes runtime documentation](https://kubernetes.io/docs/setup/production-environment/container-runtimes/).

Use `kubectl` and appropriate CRI tools such as `crictl` for node investigations. A Docker daemon on the same machine can manage a different object store and runtime namespace, so an empty `docker ps` does not prove that no Kubernetes containers are running.

## 12. Observability & Troubleshooting

### Establish identity, timing, and scope

Begin with the affected service, container ID, image digest, host, Docker context, and incident window. Determine whether the failure affects one task, one host, one image version, one network, or the whole platform. Capture state before restarting or pruning because those actions can destroy evidence.

```bash
docker context show
docker version
docker info
docker ps -a --no-trunc
docker inspect CONTAINER
docker logs --since 15m --tail 200 CONTAINER
docker events --since 30m --until 0s --filter container=CONTAINER
docker stats --no-stream CONTAINER
docker top CONTAINER
docker system df -v
docker history --no-trunc IMAGE
```

`inspect` reveals actual configuration, mounts, health state, and exit details; it is stronger evidence than a stale Compose file. `logs` depends on driver behavior. `events` offers lifecycle context but is not a durable audit store. `stats` is a sample with presentation-specific accounting, and `top` exposes processes without proving readiness. `history` helps explain build instructions and size but does not reconstruct the original source or establish trust.

`docker exec CONTAINER COMMAND` starts another process inside a running container. It can fail because the container is stopped, its PID budget is exhausted, or the executable is absent. Distroless/scratch images need an approved diagnostic approach instead of assuming a shell. Diagnostic containers that share namespaces or mounts require explicit privileges and can alter the target environment.

### Daemon, containerd, and kernel evidence

On a systemd Linux host, inspect bounded journal windows:

```bash
journalctl -u docker.service --since '-30 min' --no-pager -n 200
journalctl -u containerd.service --since '-30 min' --no-pager -n 200
journalctl -k --since '-30 min' --no-pager -n 200
```

The service names and logs depend on installation: a Docker-managed or embedded containerd might not have its own systemd unit. Rootless Docker commonly uses `journalctl --user -u docker.service`. Kernel logs can reveal OOM kills, filesystem errors, denied operations, and network problems absent from application logs.

### Namespace and cgroup inspection

Get the host PID from `docker inspect --format '{{.State.Pid}}' CONTAINER`, confirm it is nonzero and still belongs to the intended live container, then inspect from the daemon host:

```bash
lsns -p PID
cat /proc/PID/cgroup
cat /proc/PID/status
sudo nsenter --target PID --net ip address
sudo nsenter --target PID --net ip route
sudo nsenter --target PID --net ss -lntup
```

Joining only the network namespace still uses the caller's filesystem and resolver configuration. For DNS debugging, examine the target's `/etc/resolv.conf` or execute a resolver inside the container; a host resolver invoked through `nsenter --net` can otherwise test the wrong DNS configuration. Derive the correct cgroup directory and inspect controller counters without writing to runtime-managed files.

### Network and filesystem evidence

Use `ss` for listeners and connection state, `ip` for addresses/routes, bounded `tcpdump` captures for packet paths, and `conntrack` counters for connection-tracking pressure. Inspect `iptables-save` or `nft list ruleset` according to the active backend. Filter packet captures by endpoint/port and protect captures because payloads can contain sensitive data. Compare both sides of a veth/bridge path before blaming the application.

Use `df -h` and `df -i` for filesystem capacity, `findmnt` for backing mounts and options, and bounded `du -x` scans of the affected filesystem for attribution. `lsof +L1` can reveal deleted files still held open. `mount` or `/proc/PID/mountinfo` helps explain namespace-specific mounts. A full filesystem can also stop log delivery, image extraction, daemon metadata writes, and database checkpoints at once.

### Metrics and exit interpretation

cAdvisor can expose container resource metrics for Prometheus, while Grafana visualizes them with application and host signals. Deployment privileges, runtime support, cgroup version, and metric availability must be checked. Monitor CPU usage and throttling, memory working set/RSS/cache and limit, OOM events, PID usage, I/O latency, network errors, restart counts, health, and filesystem bytes/inodes. Include node pressure metrics and SLO symptoms; a memory working-set chart alone does not explain every charged byte.

| Evidence | Possible interpretation | Required follow-up |
| --- | --- | --- |
| Exit 137 | Conventionally `128 + SIGKILL` | Could be OOM, timeout escalation, manual kill, or an explicit application exit; corroborate |
| Exit 143 | Conventionally `128 + SIGTERM` | Check shutdown timing and whether the application handled the signal |
| `State.OOMKilled=true` | Runtime recorded an OOM-related container exit | Compare cgroup events and kernel logs; inspect surviving workers too |
| Exit 126/127 | Docker run commonly reports command not invocable/not found | Check permissions, executable, interpreter, architecture, and dynamic loader |
| Exit 125 from `docker run` | Docker invocation/daemon setup failure | Distinguish CLI failure from the application's exit status |
| Healthy process, failing requests | Readiness, dependency, routing, or capacity problem | Test the client-visible path and application-specific signals |

A startup failure needs inspection of image platform, entrypoint, mounts, user permissions, LSM denials, and missing interpreters/libraries. An `ENOENT` for an existing executable can mean its interpreter or dynamic loader is missing. A crash loop needs the first failure and timeline, not just the latest restart's logs.

For daemon startup failures, inspect configuration conflicts, socket ownership, backend support, disk/inode space, and service logs. Suspected corrupt layers require preserving evidence, checking filesystem/hardware health, and using supported runtime recovery or image re-pull procedures. Back up persistent application data before rebuilding runtime storage; deleting the storage root is not an ordinary repair step.

## 13. Expert Interview Scenarios

Use a consistent answer structure: explain the mechanism, state competing hypotheses, collect evidence that distinguishes them, choose a bounded mitigation, and verify recovery. Include the data-loss and availability consequences of the action.

### 1. Explain the complete lifecycle from docker run to a Linux process

The CLI resolves its context and sends Engine API requests. Docker resolves the image according to pull policy, authenticates to the registry when needed, selects a platform manifest, verifies content, and prepares storage. It creates container metadata, mounts, networking, logging, and resource/security configuration. containerd and its shim invoke an OCI runtime, which establishes the configured namespaces, cgroup membership, root filesystem, user, capabilities, and syscall/security controls before executing the initial process. Docker records state and attaches I/O as requested. Distinguish create from start and explain where an error can leave a created but stopped container.

### 2. Explain how namespaces, cgroups, and OverlayFS provide isolation

Namespaces separate views, cgroups account for and constrain resource use, and OverlayFS assembles an image with per-container filesystem changes. None alone provides complete security: OverlayFS does not limit syscalls, cgroups do not hide processes, and namespaces do not cap memory. Add capabilities, seccomp, LSM policy, and careful host-resource access. State the shared-kernel limitation and the impact of explicitly shared namespaces.

### 3. Diagnose OOM despite available host memory

Inspect the container's configured limit and its actual cgroup plus ancestors. Correlate memory events, peak/current usage where available, kernel logs, exit timing, and application allocation behavior. Account for native allocations, page cache, tmpfs, and thread stacks. Host free memory does not override a cgroup ceiling. Reduce concurrency/cache or repair a leak, or raise the limit with capacity evidence; verify the workload under peak traffic and confirm the host retains headroom.

### 4. Explain why SIGTERM does not stop a container gracefully

Identify PID 1 and the process tree, then inspect entrypoint form, shell wrappers, installed handlers, configured stop signal, and stop timeout. Reproduce in a disposable environment while tracing shutdown logs. Replace an unnecessary shell with exec form or `exec` in the wrapper, add proper signal handling/reaping, and implement bounded draining. Verify that in-flight work completes and the process exits before the deadline without `SIGKILL`.

### 5. Troubleshoot a Docker host at 100% disk usage

Identify the full filesystem and whether bytes, inodes, or quotas are exhausted. Attribute usage to logs, cache, images, writable layers, volumes, and deleted-open files using runtime and filesystem evidence. Stop uncontrolled growth and consider adding space before removing anything. Reclaim only confirmed disposable objects through supported commands; preserve volumes, rollback artifacts, and incident evidence. Validate application writes, daemon operations, and alerts afterward, then add bounded retention and capacity headroom.

### 6. Diagnose intermittent DNS and connectivity failures

Compare affected hosts, networks, containers, names, and time windows. Separate lookup failures from connection failures by testing the same destination by name and IP. Check embedded DNS/upstream behavior, stale records, UDP/TCP DNS, conntrack saturation, packet loss, retransmissions, MTU, and firewall changes. Capture both successful and failing paths. Verify from the original client namespace after mitigation; a successful host lookup does not validate container DNS.

### 7. Explain asymmetric container and host service access

Draw the source namespace, listener address, destination address, routes, and firewall path for each direction. A host-loopback listener is not ordinarily reachable through the container gateway; a container-loopback listener is not reachable through a published port. Service names may resolve only inside Docker networks, and macvlan host-child restrictions can block otherwise plausible paths. On Desktop, account for the VM. Correct the listener, route, name, or publication after identifying the intended exposure.

### 8. Optimize a 5 GB image

Measure which layers and files dominate and compare compressed pull size with unpacked size. Remove build contexts, compilers, caches, and artifacts from the final stage using multi-stage builds. Reorder dependency installation for cache reuse, use `.dockerignore`, and clean transient files in the step that creates them. Choose a smaller compatible runtime base and validate certificates, users, libraries, diagnostics, and performance. Measure cold pulls and rebuilds as well as image size; deleting files in a later layer is insufficient.

### 9. Design secure, reproducible multi-architecture CI

Use isolated BuildKit workers, pinned source/toolchain/base inputs, dependency lockfiles, scoped secret/SSH mounts, and trusted cache namespaces. Select native builds or cross-compilation deliberately and test each supported platform. Publish an index, SBOM, and provenance; sign and verify the resulting digest and required identities. Separate untrusted jobs from release credentials. Promote the same verified artifact and retain its attestations and rollback references.

### 10. Explain why a local build succeeds but CI fails

Compare the exact source revision, context path, `.dockerignore`, Dockerfile/frontend, builder versions/driver, target stage, architecture, and output mode. Then compare credentials, secret mounts, proxy/CA/DNS, file permissions and case sensitivity, available disk/memory, network access, and dependency resolution. Local cache may hide undeclared dependencies or stale packages. Reproduce with an equivalent clean builder and `--progress=plain`, redacting secrets; `--no-cache` alone does not refresh every external input or disable all cache mounts.

### 11. Diagnose throttling with low average CPU

Inspect quota/period and ancestor controls, then compare `cpu.stat` deltas with request latency and runnable threads. A burst can consume the budget across several cores quickly and wait for replenishment while host averages stay low. Check cpusets and workload concurrency as well. Tune quota or parallelism using peak-demand measurements and SLO tests; do not conclude that adding host CPUs alone removes a per-container quota.

### 12. Recover persistent data after recreation

Inspect the new container's mount sources and compare project names, volume names, bind paths, UID/GID, and application data paths with the old deployment. A different volume or a mount hiding existing data can resemble loss. Stop writers before recovery and preserve the original data. Reattach the correct persistent store or restore a consistent backup to a separate volume, then validate application records. Data stored only in a removed writable layer may have no supported recovery path; explain that limitation and fix the persistence design.

### 13. Explain the risks of mounting docker.sock

The socket grants access to the daemon's API, usually enabling host-root-equivalent actions on rootful Docker. A compromised job can create privileged containers, mount host files, read other workloads' secrets, or stop services. Making the socket mount read-only does not restrict API verbs. Prefer a service with narrowly scoped operations, isolated builders, or dedicated hosts according to the task's trust model; rootless access reduces but does not eliminate the exposed authority.

### 14. Design infrastructure for hundreds of production services

Start with SLOs, tenant trust, scale, deployment frequency, and state requirements. Provide multi-host scheduling/reconciliation, failure-domain placement, readiness-aware routing, bounded resources, autoscaling/capacity plans, trusted image distribution, and controlled deployments. Add central logs/metrics/traces, secret identity, security policy, upgrade canaries, and tested data recovery. Choose an orchestrator that the team can operate; account for quorum/control-plane failures and stateful fencing. A collection of restart policies on individually managed hosts does not supply these properties automatically.

### 15. Explain Docker versus the Kubernetes runtime

Docker includes an application-oriented API, build/distribution workflows, and standalone network/volume management. Kubernetes asks a CRI runtime such as containerd or CRI-O to manage pod sandboxes and containers; that runtime uses OCI runtimes to launch processes, and pod networking is configured through CNI. OCI-compatible images built with Docker still run there. Explain dockershim's removal, the optional external adapter for Docker Engine, and why investigations should use the actual Kubernetes/CRI control path rather than assuming Docker manages those containers.
