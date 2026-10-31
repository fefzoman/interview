# AWS — SRE Expert Checklist

An application can run in three Availability Zones and still go down because all replicas depend on one NAT gateway, one exhausted database connection pool, or one deployment role. Preparing for an expert AWS interview means tracing those dependencies and explaining what happens when they fail.

This guide follows the supplied checklist and assumes basic Linux, networking, and Kubernetes knowledge. Each section connects service behavior to operational decisions; the final section applies those ideas to interview scenarios. Treat a topic as understood when you can explain its mechanism, failure modes, evidence, and recovery trade-offs without relying on a service name alone.

Documentation reviewed: **2026-10-31**. Features, quotas, supported versions, regional availability, and prices change. Check the linked documentation for the account, Region, engine, and deployment model under discussion. Numerical architecture examples are hypothetical; commands are read-only examples and have not been run against an AWS account.

## Contents

1. [AWS Fundamentals & Architecture](#1-aws-fundamentals--architecture)
2. [IAM, Identity Center & Authentication](#2-iam-identity-center--authentication)
3. [AWS Networking & VPC](#3-aws-networking--vpc)
4. [Load Balancing & Traffic Management](#4-load-balancing--traffic-management)
5. [Compute — EC2 & Auto Scaling](#5-compute--ec2--auto-scaling)
6. [Containers — ECS, EKS & Fargate](#6-containers--ecs-eks--fargate)
7. [Storage — S3, EBS, EFS & FSx](#7-storage--s3-ebs-efs--fsx)
8. [Databases & Caching](#8-databases--caching)
9. [Messaging & Event-Driven Services](#9-messaging--event-driven-services)
10. [Serverless & Application Services](#10-serverless--application-services)
11. [Monitoring & Observability](#11-monitoring--observability)
12. [Security & Governance](#12-security--governance)
13. [Infrastructure as Code & Automation](#13-infrastructure-as-code--automation)
14. [Reliability & Disaster Recovery](#14-reliability--disaster-recovery)
15. [Cost Optimization & Capacity Planning](#15-cost-optimization--capacity-planning)
16. [Troubleshooting & Production Operations](#16-troubleshooting--production-operations)
17. [Expert Interview Scenarios](#17-expert-interview-scenarios)

## 1. AWS Fundamentals & Architecture

### Infrastructure and failure domains

A **Region** is a geographic deployment boundary containing multiple **Availability Zones (AZs)**. An AZ consists of one or more data centers with infrastructure isolated from other AZs. A **Local Zone** places selected services near a metropolitan area and remains associated with a parent Region. **Edge locations** bring services such as CloudFront caching and Route 53 DNS closer to users; they are not general-purpose AZs. [AWS global infrastructure](https://docs.aws.amazon.com/whitepapers/latest/aws-overview/global-infrastructure.html), [AWS infrastructure](https://aws.amazon.com/about-aws/global-infrastructure/)

Distinguish resource scope from dependency scope:

| Scope | Examples | Operational consequence |
| --- | --- | --- |
| Global | IAM, Route 53 public DNS, CloudFront distributions | A configuration mistake can affect workloads in several Regions |
| Regional | VPCs, Lambda functions, EKS clusters, S3 buckets | Deploy and validate regional dependencies separately |
| Zonal | Subnets, EC2 instances, EBS volumes | Placement and recovery must account for AZ loss |

A regional service may offer several availability configurations. An RDS database being regional does not establish that its particular deployment is Multi-AZ. AZ names such as `us-east-1a` can map differently across accounts; use AZ IDs to coordinate physical placement across accounts.

### Responsibility, SLAs, and architecture quality

AWS secures the infrastructure of the cloud. Customers secure their identities, data, application code, and configuration, with the boundary changing by service. On EC2, the customer patches the guest OS; with managed databases, AWS performs more infrastructure maintenance, while customers still own schema choices, access, and application recovery. [Shared Responsibility Model](https://aws.amazon.com/compliance/shared-responsibility-model/)

A service-level agreement (SLA) defines a service-specific commitment, measurement method, exclusions, and possible credits. It does not guarantee that your application will meet its service-level objective (SLO). Check deployment requirements in the actual SLA; do not quote one availability percentage for all configurations. Durability measures preservation of data, while availability measures whether it can be accessed. [AWS SLAs](https://aws.amazon.com/legal/service-level-agreements/)

The **Well-Architected Framework** has six pillars:

| Pillar | Evidence in an SRE design |
| --- | --- |
| Operational excellence | Versioned changes, runbooks, owners, learning from incidents |
| Security | Least privilege, isolation, audit, protected credentials |
| Reliability | Failure containment, recovery tests, bounded retries |
| Performance efficiency | Measured bottlenecks and appropriate resource types |
| Cost optimization | Cost per useful request and removal of waste |
| Sustainability | Efficient utilization and avoidance of unnecessary processing |

Explain a trade-off between pillars: spare capacity costs money but may be necessary to survive an AZ failure. [Six pillars](https://docs.aws.amazon.com/wellarchitected/latest/framework/the-pillars-of-the-framework.html)

### APIs, identifiers, quotas, and consistency

The console, CLI, SDKs, and infrastructure tools call AWS APIs. Select the correct endpoint, Region, partition, and credentials; most authenticated API calls use Signature Version 4. Public, private, regional, dual-stack, and FIPS endpoints have different purposes. An endpoint choice affects connectivity and compliance, while IAM still determines authorization.

A **quota** limits an account or resource dimension, such as regional vCPUs or API request rate. Some are adjustable and others are fixed. A successful quota increase does not reserve physical capacity. Inventory quotas before failover and load tests, including downstream services that autoscaling will consume.

An ARN generally follows `arn:partition:service:region:account-id:resource`, but services vary: S3 bucket ARNs omit Region and account, and resource separators differ. Names have service-specific uniqueness and length rules. Tags such as `Owner`, `Environment`, `Application`, `CostCenter`, and `DataClassification` support inventory, billing, and access policy. Protect changes to tags used for authorization, and keep secrets out of names and tags.

AWS does not have one universal consistency model. IAM changes can take time to propagate; an SDK waiter can observe resource readiness after an asynchronous create request. S3 object reads have stronger semantics than that. Retry only retryable failures, apply a deadline and capped exponential backoff with jitter, and use idempotency tokens where supported. A timeout after a create or write can mean the operation succeeded but its response was lost.

### Organizations and landing zones

**AWS Organizations** groups accounts into **Organizational Units (OUs)** for governance and consolidated billing. Accounts provide strong administrative and quota boundaries; OUs group accounts by policy needs. An OU does not supply network isolation. **Control Tower** orchestrates a governed multi-account **landing zone**, account provisioning, and controls built on other AWS services. [Control Tower](https://docs.aws.amazon.com/controltower/latest/userguide/what-is-control-tower.html)

A practical design separates production, development, security tooling, log archives, and shared services. Keep application workloads out of the management account. Automate account creation, identity, baseline logging, network allocation, budgets, and ownership. Separate accounts limit the impact of compromised credentials and accidental changes, but centralized identity, DNS, networking, and deployment systems remain dependencies to examine.

## 2. IAM, Identity Center & Authentication

### Identities and policy types

An **IAM user** is an identity that can have long-lived credentials. A **group** assigns permissions to users; it cannot be assumed like a role. A **role** supplies permissions through temporary sessions and has a trust policy defining who may assume it. A **service principal**, such as `ec2.amazonaws.com`, identifies an AWS service in a policy.

**Identity-based policies** attach to users, groups, or roles. **Resource-based policies** attach to supported resources and specify principals, as with an S3 bucket policy. A role trust policy is a resource-based policy controlling role assumption; it does not grant access to the application's data.

| Policy packaging | Ownership and operational trade-off |
| --- | --- |
| AWS-managed | AWS maintains it; convenient, but its permissions can change |
| Customer-managed | Reusable, versioned policy controlled by your team |
| Inline | Embedded in one identity; useful for tightly coupled permissions, harder to reuse |

Policy statements match actions, resources, principals where applicable, and conditions. `Allow` on the wrong ARN, missing dependent actions, or an unmatched condition does not authorize a request. [IAM policies](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html)

### Authorization evaluation and its exceptions

For an ordinary IAM request, start with implicit deny, build the authenticated request context, and evaluate all applicable controls. Any applicable explicit deny wins. Without one, a valid grant must survive the relevant permission ceilings. Root has special permissions, but member-account root activity is still subject to applicable organization controls; it is not a universal escape route.

| Control | What it constrains |
| --- | --- |
| Permissions boundary | Maximum permissions available through identity policies for a user or role |
| Session policy | Further restriction on a temporary session |
| Service control policy (SCP) | Maximum permissions of covered principals in member accounts |
| Resource control policy (RCP) | Maximum access to supported resources in covered member accounts, including access by external principals |

These controls do not independently grant application access. SCPs do not constrain identities in the management account or service-linked roles. RCP coverage depends on supported services and resource types. [SCP behavior](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps.html), [RCP behavior](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_rcps.html)

Do not describe every request as a simple intersection of all policy types. Within one account, an applicable resource policy can grant access independently of an identity policy. A grant directly to a role-session ARN is not limited by implicit denies in identity policies, boundaries, or session policies; a grant to a role ARN has different boundary/session behavior. Applicable explicit denies and organization restrictions still matter. KMS key policies and some services add service-specific requirements. [Evaluation overview](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html), [Detailed evaluation](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic_policy-eval-denyallow.html)

For cross-account resource access, generally verify authorization on both the caller and resource sides. For cross-account role assumption, the source needs permission to call `sts:AssumeRole` and the destination trust policy must trust it. Subsequent calls use the assumed role's session and target-account permissions. [Cross-account roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_common-scenarios_aws-accounts.html)

### Conditions, ABAC, and privilege escalation

Conditions can restrict access by MFA, organization, source network, TLS use, principal tags, resource tags, or request tags. **Attribute-based access control (ABAC)** uses these attributes to scale access rules, such as allowing a team to manage resources tagged with its team identifier. Restrict who can assign those tags and which services/actions support the condition keys; otherwise users may retag themselves into access.

`iam:PassRole` allows a caller to give a role to an AWS service. It does not directly assume the role, but the combination of passing a powerful role and changing executable code can create privilege escalation. Limit role ARNs and `iam:PassedToService`, and examine deployment, policy-editing, and trust-editing permissions together. PassRole is a permission check rather than a standalone API event; inspect the service API operation that accepted the role. [PassRole](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_passrole.html)

### STS, federation, and credential delivery

STS returns an access key ID, secret access key, session token, and expiration. SDK credential provider chains find and refresh credentials from configured sources, including SSO, web identity, container endpoints, and instance metadata. Stale environment credentials can override the intended workload identity. Requested session duration is constrained by the API and role; role chaining through `AssumeRole` is limited to one hour. [Role chaining](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html)

An **external ID** helps a third-party role distinguish customers and prevent the confused deputy problem. It is not a password or a replacement for a restricted trusted principal. For supported service-to-service integrations, constrain `aws:SourceArn` and `aws:SourceAccount` as appropriate to prevent another customer's resource from using the service's authority against yours.

**IAM Identity Center** manages workforce access through permission sets and account assignments. A permission set becomes an IAM role configuration in assigned accounts. Connect an external identity provider with SAML for sign-in and SCIM for provisioning users and groups; authentication and provisioning are separate flows. Design group ownership, deprovisioning, MFA, session duration, audit, and emergency access. Removing an assignment should not be assumed to invalidate every existing credential immediately. [Identity Center](https://docs.aws.amazon.com/singlesignon/latest/userguide/what-is.html)

**OIDC federation** exchanges a workload token for temporary AWS credentials. Restrict issuer, audience, and subject so an unrelated CI repository or service account cannot assume the role. **IAM Roles Anywhere** lets external workloads use trusted X.509 certificates to obtain temporary credentials; the certificate authority, certificate rotation, and private key protection become part of the trust boundary. [OIDC federation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html), [Roles Anywhere](https://docs.aws.amazon.com/rolesanywhere/latest/userguide/introduction.html)

| Workload | Credential mechanism and distinction |
| --- | --- |
| EC2 | Instance profile contains a role; temporary credentials are delivered through IMDS |
| ECS | Task role grants application permissions; task execution role serves agent actions such as supported image/log operations |
| EKS IRSA | Projected service-account OIDC token is exchanged with STS; trust scopes the cluster issuer and service account |
| EKS Pod Identity | EKS association maps cluster/namespace/service account to a role; an agent and EKS Auth supply credentials |

Pod Identity avoids configuring a cluster OIDC provider for that mechanism; both approaches need supported SDKs and network paths. Check compute compatibility, especially Fargate. Restrict node metadata access: workload roles do not make a shared node a strong isolation boundary. [Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html)

### Debugging authorization

First prove the actual caller with `aws sts get-caller-identity`. Then identify the exact action, resource, account, Region, and conditions in the failed request. Inspect identity policy, resource/trust policy, boundary, session policy, SCP/RCP, endpoint policy, and any KMS dependency. CloudTrail can identify the caller and denied operation when that event category is logged; it is not a complete execution trace of every policy evaluation.

Use **IAM Access Analyzer** to validate policies and find unintended access. Use the **policy simulator** to test a hypothesis, while checking its support and limitations; a simulation is not proof that every real request context or cross-account control has been modeled.

## 3. AWS Networking & VPC

### Subnets, routes, and filtering

A VPC is regional; each subnet occupies one AZ. Allocate non-overlapping CIDRs with room for nodes, Pods, endpoints, load balancers, and failover expansion. A public subnet has a route to an Internet Gateway (IGW); an IPv4 instance also needs a public address and permitted traffic to use that path. A private subnet can have outbound internet access through NAT. An isolated subnet has no general internet route. [VPC concepts](https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html)

Route lookup chooses the most specific matching destination. An IGW supplies internet connectivity; a public NAT gateway translates outbound IPv4 traffic for private sources and does not accept arbitrary inbound internet connections. The conventional zonal NAT design uses one gateway per AZ and routes local workloads to it, avoiding a shared zonal dependency. An **egress-only IGW** supports outbound IPv6 sessions without unsolicited inbound initiation. IPv6 still requires deliberate firewall policy.

| Property | Security Group | Network ACL |
| --- | --- | --- |
| Attachment | Network interface/resource | Subnet |
| State | Stateful | Stateless |
| Rules | Allow rules | Ordered allow and deny rules |
| Return traffic | Allowed for tracked connections | Must be permitted explicitly, including ephemeral ports |

A security group reference authorizes traffic associated with another group; it does not create a route. Routes, filters, listeners, DNS, and the return path must all work. Avoid treating the word “private” as evidence that a workload cannot exfiltrate data.

### DNS and endpoints

The VPC DNS resolver answers public DNS, VPC names, and associated Route 53 **private hosted zones**. Private zone visibility follows VPC association and DNS configuration. With **split-horizon DNS**, the same name returns different answers internally and externally. A matching private namespace can shadow a public name; a missing record may produce NXDOMAIN instead of public fallback.

Resolver **inbound endpoints** let external networks query through AWS resolvers; **outbound endpoints and forwarding rules** send selected queries to other DNS servers. Hybrid resolution requires routes, UDP/TCP 53 access, endpoint redundancy, and rules without forwarding loops. Cross-account zone associations and shared rules must be managed explicitly. [Route 53 Resolver](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/resolver.html)

**Gateway endpoints** for S3 and DynamoDB use route table entries. **Interface endpoints** use private ENIs and security groups, typically with private DNS, to reach supported services through PrivateLink. Endpoint policies add a permission boundary for that path; they do not replace IAM or resource policies. Include endpoints for bootstrap dependencies such as ECR, S3 image layers, STS, logs, and secrets when eliminating internet egress. [PrivateLink](https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html)

### Connecting networks

| Option | Appropriate use | Constraint to explain |
| --- | --- | --- |
| VPC Peering | Small numbers of directly connected VPCs | Non-transitive; overlapping CIDRs and route management complicate growth |
| Transit Gateway (TGW) | Regional hub connecting many VPCs and hybrid links | Attachment route tables, costs, and inspection symmetry need design |
| Cloud WAN | Policy-driven global network across Regions and sites | Central policy and segmentation add their own operational responsibilities |
| Site-to-Site VPN | Encrypted IPsec connectivity over internet or supported private paths | Tunnel redundancy, BGP, throughput, and MTU matter |
| Direct Connect | Dedicated connectivity with predictable network characteristics | A single circuit/location is a failure domain; encryption is not automatic at the application level |

A TGW attachment is associated with a route table for its incoming route lookup; propagation inserts routes into selected tables. Association and propagation are different. Trace both directions through VPC and TGW tables. Stateful appliances can drop asymmetric flows, so inspection designs may need appliance mode and carefully controlled AZ paths. Cross-Region TGW peering and cross-account sharing require explicit route and ownership decisions. [Transit Gateway](https://docs.aws.amazon.com/vpc/latest/tgw/what-is-transit-gateway.html)

### Address capacity and troubleshooting

IPv4 exhaustion can stop Pods, load balancers, or endpoint creation before CPU runs out. Track available subnet addresses, ENI/IP limits, overlapping ranges, and reserved capacity. Dual-stack requires supported service endpoints, DNS answers, routes, filters, and working application behavior for both families; IPv6 adoption is not just changing a subnet setting.

**VPC Flow Logs** reveal supported flow metadata and ACCEPT/REJECT decisions, not payloads or every possible traffic category. **Reachability Analyzer** evaluates supported configuration paths rather than actively sending application traffic. **Network Firewall** provides managed filtering/inspection, but requires traffic steering, symmetric routes, logs, and capacity planning. For a failed connection, walk DNS → source address → route → filters → intermediary → listener → return path.

## 4. Load Balancing & Traffic Management

### Choose the protocol and distribution unit

| Service | Main behavior | Typical use |
| --- | --- | --- |
| ALB | Layer 7 HTTP/HTTPS request routing, host/path/header rules | Web services, APIs, container ingress |
| NLB | Layer 4 flow distribution and supported transport/TLS listeners | Transport services, stable entry addresses, PrivateLink integrations |
| Gateway Load Balancer | Distributes encapsulated traffic to virtual appliances | Transparent firewall and inspection fleets |

Connection balancing cannot redistribute requests inside a long-lived connection as freely as a request-aware proxy. HTTP/2, WebSockets, affinity, and streaming can explain uneven target load even when the number of accepted connections looks balanced. [Elastic Load Balancing](https://docs.aws.amazon.com/elasticloadbalancing/latest/userguide/what-is-load-balancing.html)

Listeners accept traffic and select actions; target groups define destinations, health checks, and attributes. Registration does not mean readiness. Verify target type, enabled AZs, port, protocol, health path, success matcher, and security rules. A health check should establish that a target can accept useful work without turning a shared dependency failure into unnecessary fleet churn.

### TLS and connection lifecycle

ALB HTTPS and NLB TLS listeners can terminate TLS using certificates managed through ACM. **SNI** allows certificate selection for different server names. Validate hostname coverage, chain, expiry, listener policy, Region, and client compatibility; CloudFront viewer certificates have specific regional requirements.

ALB supports mutual TLS verification and a passthrough mode that forwards client certificate information in headers; ALB still terminates TLS in that passthrough mode. NLB TLS listeners do not implement mTLS; TCP passthrough lets the backend perform it. Treat forwarded identity headers as trusted only through the controlled proxy path. [ALB mTLS](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/mutual-authentication.html), [NLB listeners](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-listeners.html)

**Deregistration delay** gives in-flight work time to finish when removing a target. Coordinate it with application shutdown, Kubernetes termination grace, readiness withdrawal, and maximum request duration. Idle timeouts govern inactive connections, not every application's total request deadline. Backend keep-alive settings and resets during rollout can produce errors even when average latency is healthy.

Cross-zone load balancing changes which AZs a load balancer node can send traffic to; defaults and override support differ by type and target group. Ensure the remaining zones have enough healthy capacity after zonal loss. Test unhealthy-all-target behavior: some load balancers fail open, so “unhealthy” does not always mean “receives no requests.” [ELB behavior](https://docs.aws.amazon.com/elasticloadbalancing/latest/userguide/how-elastic-load-balancing-works.html)

### Global traffic, protection, and errors

Route 53 **weighted** routing supports controlled traffic shares, **latency** routing selects based on AWS latency measurements, **failover** routing selects a secondary based on health, and **geolocation** routing uses user geography. DNS caches, TTLs, resolvers, and existing connections prevent instantaneous or exact per-request distribution.

**CloudFront** is an edge CDN and reverse proxy with caching and origin behavior. **Global Accelerator** supplies static anycast entry addresses and routes supported traffic over AWS's network to healthy regional endpoints; it does not cache application objects. Anycast announces the same address from multiple network locations. These tools solve different latency and failover problems.

**WAF** filters application requests and can enforce rate-based rules on supported services. **Shield** provides DDoS protection, with additional capabilities under Shield Advanced. Combine edge protection, origin restrictions, bounded request cost, and backend admission control; an accepted expensive request can still overwhelm an application.

For ALB errors, distinguish the load balancer's status from the target's status in access logs. **502** often indicates resets, malformed responses, or backend TLS/connectivity issues; **503** can indicate no usable registered targets; **504** indicates a connection or response timeout. Correlate target health reasons, processing timings, connection counts, target saturation, and downstream latency before changing timeouts. [ALB troubleshooting](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-troubleshooting.html)

## 5. Compute — EC2 & Auto Scaling

### Instance architecture and boot

Choose instance families by the limiting resource: general-purpose, compute, memory, storage, accelerated compute, or network needs. Compare CPU architecture, sustained versus burst performance, memory bandwidth, EBS bandwidth, packet rate, and supported features. A smaller burstable instance may look cheap until sustained load exhausts its CPU credits. Graviton uses Arm, so validate binaries and dependencies.

The **Nitro System** offloads virtualization, networking, and storage functions to dedicated components and uses a lightweight hypervisor for virtualized instances. That improves isolation and efficiency, while instance-specific network and storage ceilings still apply. Bare-metal instances and older virtualization generations have different operational characteristics.

An **AMI** defines the starting image, architecture, and block device configuration. **User data** supplies boot instructions, commonly executed by **cloud-init** on Linux. Many cloud-init actions run once per instance; rebooting does not necessarily repeat bootstrap. Check cloud-init status and logs when a running instance never becomes ready. Bake stable dependencies into a golden image and keep boot-time work small and repeatable.

Require **IMDSv2** where compatible. It uses a session token for metadata requests, reducing some SSRF attack paths, but applications still need protection from SSRF and excessive metadata access. Instance profiles deliver temporary role credentials through IMDS; do not place permanent keys or secrets in user data. [EC2 metadata](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)

### Purchasing, placement, and capacity

| Option | What it buys | Reliability implication |
| --- | --- | --- |
| On-Demand | Usage without a long-term compute commitment | Availability still depends on capacity and quotas |
| Reserved Instances | Billing discount for a commitment; some zonal reservations also reserve capacity | Regional discount alone does not reserve an instance |
| Savings Plans | Discount for committed eligible hourly spend | Does not reserve capacity |
| Spot | Discounted spare capacity that can be interrupted | Requires replaceable work, diversification, and interruption handling |
| On-Demand Capacity Reservation | Capacity for matching instances in a selected AZ | Reserved unused capacity can still be charged |

A **cluster placement group** favors low-latency placement, a **spread placement group** separates a small number of critical instances, and a **partition placement group** separates groups of instances for distributed systems. Placement can constrain launches. **Dedicated Hosts** expose a dedicated physical host, often for licensing or placement control; they do not automatically provide application redundancy.

Spot interruption notices and rebalance recommendations help trigger draining or checkpointing, but recovery must tolerate sudden loss too. Keep essential baseline capacity available, diversify eligible instance types/AZs, and bound recovery by queue retention and useful checkpoints.

### Scaling and safe replacement

An **Auto Scaling Group (ASG)** maintains desired capacity between configured minimum and maximum sizes. A **launch template** versions the image, instance configuration, networking, and role. A **mixed instances policy** combines types and purchasing options; ensure every eligible type can run the workload and meets its memory, architecture, and network requirements.

| Scaling policy | Mechanism | Appropriate signal |
| --- | --- | --- |
| Target tracking | Adjusts capacity toward a target metric | CPU or request load that changes predictably with capacity |
| Step scaling | Applies configured adjustments at alarm thresholds | Explicitly sized responses to different overload levels |
| Scheduled | Changes capacity at known times | Business opening hours or planned events |
| Predictive | Forecasts recurring demand | Stable historical patterns, supplemented by reactive scaling |

Account for instance warmup, health-check grace periods, and application readiness. CPU is a poor scaling signal for workers blocked on an external API; backlog per worker or estimated queue delay may work better. Scaling cannot solve a downstream database limit. [EC2 Auto Scaling](https://docs.aws.amazon.com/autoscaling/ec2/userguide/scale-your-group.html)

**Lifecycle hooks** pause launch or termination for controlled bootstrap/drain work, with timeouts and completion handling. ASG health checks replace failed instances; explicitly configure the relevant load balancer health integration when needed. **Instance refresh** rolls a template change through a fleet. Define healthy-capacity limits, checkpoints, alarms, and a rollback image before beginning a replacement.

### Networking, disks, and host operations

An **ENI** owns addresses and security group associations. Enhanced networking, commonly through ENA, improves network performance, but bandwidth, packets per second, connection tracking, and ENI limits can become separate bottlenecks. EBS-optimized networking has its own instance ceiling; provisioning faster volumes does not bypass that ceiling.

**Instance store** is host-local ephemeral storage suitable for rebuildable caches and scratch work. It is lost on events such as stop/start or host loss. **EBS** persists independently of a running guest, but termination behavior depends on `DeleteOnTermination`. Stopped instances can still incur EBS and other resource charges. [EC2 lifecycle](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html)

Separate **system status checks** for underlying infrastructure, **instance status checks** for guest reachability, and **attached EBS status checks** where supported. Eligible automatic recovery addresses infrastructure failures and is not application failover. Capture console output, guest logs, disk fullness, memory pressure, and recent changes before repeatedly rebooting. [Automatic recovery](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-recover.html)

**Systems Manager Session Manager** provides controlled administration without opening inbound SSH; **Run Command** executes managed commands; **Patch Manager** coordinates patch baselines and maintenance. They need a working agent, permissions, and service connectivity. Record administrative activity using supported logging; encrypted SSH/port-forwarding sessions do not expose their payloads to Session Manager session logging. [Systems Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/what-is-systems-manager.html)

## 6. Containers — ECS, EKS & Fargate

### Separate orchestration from compute

**ECS** supplies AWS-native container orchestration. **EKS** supplies a managed Kubernetes control plane and the Kubernetes ecosystem. **Fargate** is a managed compute option for supported ECS tasks and EKS Pods, rather than a competing orchestrator. Choose by required platform behavior, operational skills, isolation, scheduling controls, and total cost.

In ECS, a **cluster** groups resources, a **task definition** versions container configuration, a **task** is a running instance of that definition, and a **service** maintains tasks and deploys revisions. **Capacity providers** connect scheduling with capacity strategies, such as an ASG or Fargate/Fargate Spot. `awsvpc` networking gives tasks ENIs and security groups; service discovery can publish destinations through Cloud Map. Keep application task permissions separate from execution permissions. [ECS architecture](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/Welcome.html)

EKS manages control plane availability, but application reliability depends on worker placement, scheduling, DNS, networking, storage, and correct workload configuration. **Managed node groups** automate parts of EC2 node lifecycle; self-managed nodes give more control and maintenance responsibility. Neither removes the need to plan disruptive upgrades.

### Node provisioning and Pod networking

| Mechanism | Capacity decision | Operational boundary |
| --- | --- | --- |
| Cluster Autoscaler | Adjusts existing node groups for unschedulable Pods | Requires groups whose templates match workload constraints |
| Karpenter | Provisions suitable nodes from workload and NodePool constraints | Customer operates its installation/configuration in a conventional EKS setup |
| EKS Auto Mode | AWS operates integrated node provisioning and supported infrastructure capabilities | Managed components, immutable nodes, compatibility constraints, and additional charges |

Auto Mode extends management into compute, networking, load balancing, and storage integration. It does not remove application ownership, resource requests, disruption policies, quotas, or testing. Evaluate the supported workload and customization boundaries before enabling it. [EKS Auto Mode](https://docs.aws.amazon.com/eks/latest/userguide/automode.html)

The **AWS VPC CNI** ordinarily assigns VPC addresses to Pods using node ENIs. A node can have free CPU while reaching its Pod/IP ceiling. With **prefix delegation**, ENIs receive address prefixes rather than only individual secondary IPs; this improves allocation efficiency and density but needs compatible software, instance support, and suitable contiguous free ranges. It does not create subnet space. Warm allocation settings can reserve substantial address capacity. [VPC CNI prefixes](https://docs.aws.amazon.com/eks/latest/userguide/cni-increase-ip-addresses.html)

Follow traffic through Pod address selection, node routing, source NAT, security groups, and any NAT gateway. SNAT behavior depends on address family and CNI settings; never assume an external service sees the Pod IP. **Security groups for Pods** add workload-level VPC filtering on supported configurations, with interactions involving branch ENIs, CNI enforcement modes, and network policy. Check these before changing enforcement during an incident.

### Identity, access, ingress, and storage

IRSA and Pod Identity deliver workload AWS permissions as described in section 2. Kubernetes **RBAC** controls Kubernetes API actions. EKS **access entries** associate IAM principals with supported cluster access mechanisms; IAM authentication establishes who is calling, while RBAC or EKS access policies determine allowed Kubernetes operations. A role allowed to describe the EKS cluster is not automatically allowed to list its Secrets.

The **AWS Load Balancer Controller** reconciles supported Ingress and Service resources into ALBs/NLBs. An ALB instance target commonly reaches a node's NodePort before the Pod; an IP target reaches a routable Pod IP. Check actual target mode, controller events, subnet discovery, role permissions, health checks, and security group rules. Conventional controller installations and Auto Mode integrations have different ownership and supported settings.

The **EBS CSI driver** provisions zonal block volumes. Use topology-aware scheduling, often `WaitForFirstConsumer`, to avoid placing a volume and Pod in incompatible AZs. Multi-Attach is not a general solution for shared filesystems. The **EFS CSI driver** supplies shared file storage through NFS and access points. A bound PersistentVolumeClaim does not prove the node can attach or mount it.

### Add-ons, upgrades, images, and cost

**CoreDNS** resolves cluster service names; **kube-proxy** implements Service forwarding in conventional setups; the **CNI** supplies Pod networking. Monitor them as dependencies. Coordinate add-on and node versions with the control plane's supported compatibility matrix; use provider-specific lifecycle controls for components managed by Auto Mode.

For upgrades, check deprecated APIs, admission webhooks, controllers, CSI/CNI compatibility, and version skew. Upgrade a nonproduction environment, then roll nodes with spare capacity. Draining respects supported eviction behavior and disruption budgets, but a PDB cannot prevent involuntary node failure. Stateful workloads need recovery-aware shutdown and placement, not just a larger PDB.

**ECR** stores images and provides temporary authentication, repository policies, replication, and scanning options. Pin production deployments to digests, check replication completion before failover, and distinguish vulnerability scanning from image signature verification. Pull failures may be caused by missing S3/ECR endpoints, expired credentials, repository policy, or architecture mismatch.

Compare Fargate's resource combinations, storage, networking, privileged/host-access restrictions, and platform-specific features with EC2 flexibility. EKS Fargate has specific restrictions on DaemonSets, storage, and workload identities. [EKS Fargate constraints](https://docs.aws.amazon.com/eks/latest/userguide/fargate.html) Include node idle capacity, control plane fees, managed features, NAT, IP addresses, load balancers, logs, and persistent storage in a cost comparison.

## 7. Storage — S3, EBS, EFS & FSx

### Choose the access model first

| Model | AWS examples | Application contract |
| --- | --- | --- |
| Object | S3 | API operations on objects; no general POSIX filesystem semantics |
| Block | EBS | Raw block device; filesystem and database consistency belong above it |
| File | EFS, FSx | Shared file protocols and filesystem semantics, varying by service |

An S3 bucket contains objects identified by keys; a **prefix** is part of a key, not a physical directory. General-purpose S3 object operations provide strong read-after-write consistency for PUT, overwrite, DELETE, and LIST. This does not make multiple objects one transaction, nor make replication or event delivery synchronous. Check the specific semantics of directory buckets and specialized storage classes. [S3 consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)

### S3 lifecycle, protection, and access

S3 Standard serves frequently accessed data. Standard-IA and One Zone-IA exchange lower storage cost for retrieval and other constraints, with One Zone carrying a different failure boundary. Glacier Instant Retrieval supports immediate access; Flexible Retrieval and Deep Archive require restore workflows. **Intelligent-Tiering** moves eligible objects between access tiers, with monitoring charges and optional archive tiers. **Lifecycle policies** automate transitions and expiry; calculate retrieval charges, minimum duration, object size, and restore time before selecting a class. [S3 storage classes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html)

**Versioning** preserves prior object versions after overwrites and ordinary deletes. A delete marker can hide an object without deleting its historical versions, but authorized callers can still permanently delete versions. **Object Lock** adds retention and legal holds: governance mode has controlled bypass, while compliance mode enforces stronger retention restrictions. Design retention before production use and limit changes to lifecycle, retention, and replication policies. [Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html), [Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)

Replication copies eligible objects asynchronously to configured destinations, including cross-Region or cross-account targets. Existing objects may need separate replication work, and deletion/version behavior depends on settings. Encrypt at rest using suitable S3/KMS choices and use TLS in transit. Cross-account restore and replication also require working KMS permissions; owning the object copy is insufficient if its key becomes unavailable. [S3 replication](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html)

**Multipart uploads** parallelize large transfers and permit part retries; incomplete uploads consume storage until completed or aborted. A **presigned URL** delegates the signer's permitted operation for a bounded lifetime and acts like a bearer capability. Its useful lifetime can end earlier when its signing credentials expire. **Bucket policies** and **access points** express access boundaries; access points do not bypass the bucket's underlying controls. [Presigned URL behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)

For performance, parallelize bounded work, reuse connections, and monitor latency, errors, client bandwidth, and KMS request pressure. AWS documents request-rate scaling per partitioned prefix; gradual scaling can briefly produce `503 Slow Down`. Retry with backoff and avoid synchronized request surges. [S3 performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)

S3 event notifications can target supported consumers and integrate with EventBridge. Delivery can be duplicated or reordered; use durable idempotency and object/version identity when processing. Avoid a loop where a function writes to the same prefix that triggers it. [S3 notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)

### EBS volumes and snapshots

**gp3** separates capacity from provisioned IOPS/throughput within supported limits. **io2** targets demanding latency, durability, and I/O requirements. IOPS measures operation count; throughput measures bytes per second. Small random I/O and large sequential I/O stress different limits. Inspect both volume limits and the attached instance's EBS limits.

EBS volumes attach within their AZ. An EBS snapshot is an incremental managed backup, but restoring it creates a usable volume without manually replaying a snapshot chain. Snapshot blocks may initialize lazily on first access; account for this in recovery tests or use supported initialization/fast restore options. Quiesce or coordinate application writes for application-consistent backups.

Encryption uses KMS and applies to supported volume/snapshot operations. **Multi-Attach** is limited to supported Provisioned IOPS configurations and instances in one AZ; applications need coordinated writes and suitable filesystems/fencing. It does not turn an ordinary filesystem into safe shared storage. For AZ recovery, restore or replicate data into the recovery AZ and schedule compute accordingly. [EBS](https://docs.aws.amazon.com/ebs/latest/userguide/what-is-ebs.html)

### EFS and FSx

**EFS Regional** stores data across AZs; **EFS One Zone** has a single-AZ failure boundary. Clients use NFS and mount targets, normally placed for AZ-local access. **Access points** define an application entry directory and POSIX identity constraints; IAM authorization, network security, and filesystem permissions still matter. [EFS architecture](https://docs.aws.amazon.com/efs/latest/ug/whatisefs.html)

General Purpose and Max I/O are performance modes, with compatibility differences; General Purpose offers lower per-operation latency. Elastic, Provisioned, and Bursting are throughput modes. Metadata-heavy small-file work can behave very differently from large sequential transfers. Compare permitted throughput, client behavior, mount options, and storage class latency before assuming file count or total capacity predicts performance. [EFS performance](https://docs.aws.amazon.com/efs/latest/ug/performance.html)

| FSx variant | Typical workload |
| --- | --- |
| Windows File Server | Windows SMB shares and Active Directory integration |
| Lustre | Parallel HPC/analytics filesystems, often connected to S3 datasets |
| NetApp ONTAP | ONTAP features, multiprotocol storage, enterprise migration |
| OpenZFS | NFS workloads benefiting from ZFS capabilities |

Each FSx variant has its own deployment, backup, throughput, and failover options; do not apply one variant's Multi-AZ behavior to all of them. [FSx family](https://aws.amazon.com/fsx/)

Persistence, availability, and backup answer different questions. A persistent volume can remain intact while its AZ is unreachable; replication can copy an accidental deletion; a backup can exist but be unusable because the recovery account lacks its key. Test the complete restore path and its elapsed time.

## 8. Databases & Caching

### RDS availability and read scaling

RDS manages infrastructure operations such as backups and maintenance, while the team owns workload behavior, parameter choices, connection management, and recovery validation. Automated backups support point-in-time recovery within configured retention; manual snapshots follow a separate lifecycle. Restores generally produce a new database endpoint, requiring a controlled application cutover.

| Deployment | Replication and serving behavior | Primary purpose |
| --- | --- | --- |
| RDS Multi-AZ DB instance | Synchronous standby in another AZ; standby does not serve reads | Failover availability |
| RDS Multi-AZ DB cluster | Writer plus two readable instances in three AZs; semisynchronous behavior for supported engines | Availability with read capacity |
| RDS read replica | Usually asynchronous, engine-dependent replication; may be promoted | Read scaling or a recovery option with possible lag |

Say which engine and deployment you mean. “Multi-AZ standbys never serve reads” is incorrect for Multi-AZ DB clusters. Read replicas are not equivalent to the automatic failover of a Multi-AZ instance deployment. [RDS Multi-AZ](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.html), [Multi-AZ cluster replication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html), [Read replicas](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ReadRepl.html)

Maintenance and failover can close connections and interrupt transactions. Configure DNS refresh, connection timeouts, pooling, bounded reconnects, and transaction-safe retries. An uncertain commit must be reconciled with business identifiers rather than blindly duplicated.

### Aurora and connection pooling

Aurora separates compute from a distributed cluster volume, with six storage copies across three AZs. Storage redundancy alone does not provide an immediately ready replacement writer: place suitable Aurora replicas in other AZs and define promotion priorities. A cluster writer endpoint follows the writer; a reader endpoint distributes new connections among eligible readers and does not balance each query inside a connection.

After failover, clients must resolve the endpoint, reconnect, and handle interrupted or uncertain transactions. Monitor replica lag and test consistency-sensitive reads. **RDS Proxy** pools connections and can reduce connection storms and failover disruption, but session pinning, transaction behavior, supported features, and pool limits affect the benefit. It cannot create unlimited database throughput. [Aurora high availability](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Concepts.AuroraHighAvailability.html)

### DynamoDB

DynamoDB distributes items by partition key, optionally with a sort key within the item collection. Design keys around access patterns and key distribution. A **global secondary index (GSI)** supports a different key/access path and adds storage, write, and capacity considerations. Tables and local secondary indexes support strong reads; GSI and stream reads are eventually consistent. A strong read of one item is not a transaction over unrelated items. [DynamoDB read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)

**Provisioned capacity** sets throughput with optional autoscaling; **on-demand** adapts request capacity within service behavior and limits. Neither removes hot-key or maximum-throughput constraints. **Adaptive capacity** helps uneven workloads but cannot make one item support unlimited traffic. Inspect throttling reasons, consumed capacity, item size, GSI pressure, and key distribution; use sharding or caching only with a clear read/aggregation strategy.

**Global Tables** replicate across Regions. Multi-Region eventual consistency (MREC) asynchronously replicates and resolves conflicts using last-writer-wins behavior. Multi-Region strong consistency (MRSC) offers different cross-Region guarantees and topology/feature restrictions; check current supported Regions and APIs. Do not claim all Global Tables are eventually consistent. **Streams** expose item changes for downstream processing; **PITR** supports recovery from logical mistakes that replication would propagate. [Global Tables modes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html)

### Caching, durability, and performance evidence

ElastiCache supports engines including Redis OSS and Valkey, with node-based and serverless options. In node-based replication groups, replicas and automatic failover can improve availability; cluster mode distributes hash slots across shards and requires a compatible client. Replica reads may be stale, hot keys remain possible, and asynchronous replication can lose recent cache writes on failover. [ElastiCache](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/WhatIs.html)

**MemoryDB** combines in-memory access with a durable Multi-AZ transactional log for database use cases. Choose it when the in-memory store must be a durable system of record, rather than assuming cache replication has the same contract. Backups, client compatibility, recovery, and cost still require evaluation. [MemoryDB](https://docs.aws.amazon.com/memorydb/latest/devguide/what-is-memorydb.html)

Monitor database load and wait events alongside CPU, I/O latency, locks, connections, cache hit rate, eviction, replication lag, and query plans. Validate backups by restoring and checking application-level invariants. For cache failures, use bounded fallbacks, jittered TTLs, and request coalescing to avoid sending a stampede to the database.

## 9. Messaging & Event-Driven Services

### Queues and fan-out

**SQS Standard** offers at-least-once delivery and best-effort ordering. **SQS FIFO** preserves ordering within a message group and suppresses duplicate sends within its deduplication contract. FIFO does not make a consumer's external side effects exactly once: a worker can commit a database write and crash before deleting the message. Build idempotency around a durable business operation identifier. [SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)

The **visibility timeout** hides a received message while a consumer works; it is not a transaction lock. Set it to cover processing and deletion, extend it for supported long-running work, and expect redelivery after failure. Excessively short visibility causes concurrent duplicates; excessive visibility delays recovery. **Long polling** reduces empty receives. A **dead-letter queue (DLQ)** isolates repeatedly failing messages after the configured receive count; alert on arrivals, diagnose the cause, and redrive at a safe rate after repair. Moving a FIFO message to a DLQ can affect business ordering.

**SNS** publishes to subscribers and supports fan-out and subscription filtering. An SNS-to-SQS design gives each consumer its own durable queue, scaling, retention, and failure handling. Check both topic/queue policies and KMS permissions, and distinguish failure to deliver a notification from failure to process it after queue delivery.

### Routing, streams, and Kafka

**EventBridge** routes events from AWS services, applications, and partners. In the rules-and-targets bus model, rules match event patterns and route to targets with supported transformations. Configure delivery retries and DLQs, and use archives/replay where supported. **EventBridge Scheduler** manages one-time or recurring schedules with its own time-zone, retry, and target configuration. Verify the bus type's ordering and retention contract rather than assuming every EventBridge product has identical semantics. [EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-what-is.html)

**Kinesis Data Streams** stores records in shards selected through partition keys. Ordering is scoped to the applicable shard/key and producer sequencing behavior, not the entire stream. Retention enables replay independently of consumer progress. Provisioned mode exposes shard capacity management; on-demand manages capacity within its scaling behavior. Track write/read throttling, consumer lag, hot keys, and shard changes. More shards do not distribute a single unchanged hot partition key. [Kinesis concepts](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html)

**MSK** operates Kafka infrastructure, while the team still designs topics, partition count, replication, producer acknowledgments, consumer groups, retention, and replay. Check broker/storage saturation, under-replicated partitions, consumer lag, and rebalances. Private connectivity, advertised broker addresses, security groups, TLS, and supported IAM/SASL authentication must all work. IAM control-plane access is distinct from Kafka data-plane authorization. [MSK](https://docs.aws.amazon.com/msk/latest/developerguide/what-is-msk.html)

### Lambda consumers and reliable processing

An **event source mapping** polls supported queues/streams and invokes Lambda with batches. Its error and checkpoint behavior depends on the source. With SQS, messages remain hidden during processing and are deleted on success; without partial batch responses, a failed batch can repeat successful records. Configure `ReportBatchItemFailures` and return the required failure identifiers when using that feature. FIFO handlers must preserve group order when reporting failures. [Lambda with SQS](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html)

For stream sources, a poison record can block progress in its shard. Use supported retry limits, maximum record age, batch splitting, and failure destinations intentionally. Queue DLQs, event source mapping failure handling, and Lambda asynchronous invocation destinations are different mechanisms.

**Step Functions** makes orchestration state, retries, timeouts, and error transitions explicit. A compensating action reverses a business effect where possible, such as releasing reserved inventory; it is not a distributed database rollback. Compensations must also tolerate retries and partial failure.

A reliable asynchronous design follows a measurable contract:

1. Accept work only after durable persistence; use an outbox or equivalent pattern when a database update and event publication must stay consistent.
2. Validate schema and operation identity before expensive processing.
3. Apply idempotency at the side-effect boundary, ideally atomically with the side effect.
4. Bound consumer concurrency to downstream capacity and extend leases/visibility deliberately.
5. Separate transient errors from poison messages, and retain enough context for controlled replay.
6. Alert on oldest message age and completion rate, not queue depth alone.

If arrivals exceed completed useful work, the backlog grows. Increasing consumers helps only until the next bottleneck; after that it can amplify errors, retries, and cost.

## 10. Serverless & Application Services

### Lambda execution and concurrency

Lambda initializes an execution environment, runs the handler, and may reuse the environment. **Cold starts** include runtime and application initialization. Reuse SDK clients and connections where safe, but do not depend on process reuse or local temporary files for durable state. Memory selection also affects available compute and cost per execution.

Approximate concurrency as `arrival rate × average duration`. For a hypothetical 400 requests/s lasting 0.25 seconds, steady concurrency is about 100 before accounting for variability and headroom. **Reserved concurrency** reserves part of the regional pool for a function and caps its concurrency; it does not preinitialize environments. **Provisioned concurrency** prepares environments for a configured version/alias and incurs additional cost; requests beyond it can still cold-start or throttle depending on limits. [Lambda concurrency](https://docs.aws.amazon.com/lambda/latest/dg/lambda-concurrency.html)

VPC-enabled functions use managed networking to access VPC resources. Placement in a public subnet does not give the function a public IP or internet path; use the appropriate NAT or endpoints. The execution role controls AWS access, while security groups and routes control networking. The standard function timeout limit is 15 minutes; supported asynchronous/event-source invocations on Lambda Managed Instances can allow up to 90 minutes. Check the compute and invocation model, and budget against upstream gateway/client deadlines too. [Lambda timeout](https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html)

### Invocation and API contracts

With a **synchronous invocation**, the caller receives the result and owns application-level retry decisions; a client or intermediary may retry. With **asynchronous invocation**, Lambda accepts the event into an internal queue and applies its configured retry/age behavior, with supported destinations for outcomes. **Poll-based event source mappings** have source-specific semantics. State who owns retries at every hop so multiple layers do not multiply load.

| API Gateway choice | Useful distinction |
| --- | --- |
| REST API | Richer API management, including usage plans/API keys, request validation, caching, and supported private API/WAF features |
| HTTP API | Lower-overhead HTTP API product with features such as JWT authorizers and selected integrations |

Both need deliberate authentication, authorization, throttling, payload handling, logging, and integration timeout design. IAM, Lambda authorizers, Cognito/JWT options, and private integrations differ by product. API keys are not a substitute for user authentication; throttling is not a precise financial cap. [REST versus HTTP APIs](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-vs-rest.html)

### Workflows, secrets, discovery, and configuration

**Step Functions Standard** supports durable long-running workflows up to one year and an exactly-once workflow execution model, subject to configured retries. **Express** supports executions up to five minutes; asynchronous Express is at least once, while synchronous Express is at most once. Workflow guarantees do not make remote side effects magically atomic: handle ambiguous task results and explicit retries safely. Standard uses state-transition pricing, while Express uses execution/resource-duration pricing. [Workflow types](https://docs.aws.amazon.com/step-functions/latest/dg/choosing-workflow-type.html)

**Secrets Manager** emphasizes secret lifecycle, retrieval, and rotation. **SSM Parameter Store** provides hierarchical configuration and SecureString values, with tier-specific limits and features. Choose based on rotation needs, integrations, access patterns, throughput, and cost. Cache secret retrieval within bounded freshness, and test how clients reconnect after rotation; replacing a stored password is only one step of credential rotation. [Secrets Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html)

**Cloud Map** registers service instances for supported DNS/API discovery. **AppConfig** distributes validated application configuration and feature flags with controlled rollout and monitoring. Configuration can trigger an outage as easily as a binary release; validate it, roll it out gradually, and define fallback behavior when retrieval fails.

Event-driven serverless applications fit bounded units of work and variable traffic. Containers suit workloads needing long-running processes, specialized runtime control, or predictable sustained utilization. Compare latency, concurrency, retries, state, connection behavior, deployment complexity, and cost per completed operation. A useful hybrid keeps durable events between independently scalable compute components.

## 11. Monitoring & Observability

### Metrics, logs, alarms, and collection

**CloudWatch Metrics** stores numerical time series identified by namespace, name, and dimensions. **Logs** stores event records; **Alarms** evaluate conditions and trigger supported actions; **Dashboards** present shared views. Start with user-visible availability, latency, and correctness, then add saturation and dependency signals that explain them. [CloudWatch](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html)

Standard-resolution custom metrics have one-minute resolution; high-resolution custom metrics support one-second resolution, with supported alarm periods and additional cost considerations. Resolution is distinct from publication frequency and aggregation period. Average latency hides tails; a count or rate interpreted with the wrong statistic can mislead. Dimensions create separate time series, so request IDs and unbounded user IDs can produce costly cardinality. [Custom metrics](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/publishingMetrics.html)

**Logs Insights** queries records within selected groups and time windows. Subscription filters stream supported logs to destinations for central processing; delivery, destination permissions, retry behavior, and downstream backpressure require monitoring. Set retention deliberately, redact sensitive values before ingestion, and avoid scanning months of logs for a five-minute incident. [Logs Insights](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/AnalyzingLogData.html)

Example Logs Insights QL for application logs that provide these fields:

```text
fields @timestamp, service, status, duration_ms
| filter service = "checkout" and status >= 500
| stats count(*) as failures, pct(duration_ms, 95) as p95_ms by bin(5m)
```

The **CloudWatch Agent** collects configured host logs and additional system metrics, including memory and disk signals absent from basic EC2 monitoring. **Container Insights** adds container/platform telemetry. **Application Signals** supplies supported application performance and service-level views. Instrumentation still needs permissions, connectivity, cardinality limits, and a check that exported telemetry actually arrives.

### Audit, configuration, and tracing

**CloudTrail management events** record control-plane actions such as modifying a policy. **Data events** record supported resource operations such as S3 object access and require explicit configuration for coverage. Event history is not a complete permanent account audit archive. Organization trails centralize covered activity across accounts; protect destination policies, retention, and KMS access. [CloudTrail event categories](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/logging-management-events-with-cloudtrail.html)

**AWS Config** records supported resource configuration and history, relationships, and rule evaluations. Config answers “what changed and was it compliant?” while CloudTrail helps answer “who made the API call?” Recording scope and delivery must be enabled; absence of a finding does not establish that every resource is secure. [AWS Config](https://docs.aws.amazon.com/config/latest/developerguide/WhatIsConfig.html)

**X-Ray** provides AWS distributed tracing capabilities; **OpenTelemetry** provides vendor-neutral instrumentation and collection. Propagate trace context through HTTP and asynchronous messages, sample intentionally, and correlate traces with structured logs. Prefer maintained OTel instrumentation for new work; AWS X-Ray SDKs and the daemon entered maintenance mode on February 25, 2026. A missing span can indicate sampling or export failure rather than missing execution. [X-Ray to OTel migration](https://docs.aws.amazon.com/xray/latest/devguide/xray-sdk-migration.html)

**Amazon Managed Service for Prometheus (AMP)** provides managed Prometheus-compatible ingestion/query capabilities. **Amazon Managed Grafana** supplies managed visualization with configured data sources and access. Scraping, remote write, labels, permissions, query limits, and data source health remain operational concerns.

### Network evidence, centralization, and alert quality

VPC Flow Logs help inspect network metadata. ELB access logs show supported traffic and timing details. S3 access logs offer additional request evidence with delivery limitations; select CloudTrail data events where the audit requirement needs that mechanism. Confirm that the requested record type was enabled before interpreting missing logs.

Use EventBridge operational events to trigger alerts or bounded automation, such as collecting diagnostics after a failure. Automations need idempotency, restricted roles, stop conditions, and observable outcomes. Centralize logs in a protected account and use supported cross-account observability links for metrics/logs/traces; cross-account viewing and copying data are distinct designs.

Page on actionable SLO threats, using multiwindow burn-rate alerts where appropriate. Route capacity forecasts and low-urgency hygiene issues to nonpaging channels. Track alert usefulness, missing-data behavior, and telemetry pipeline health. Control cost with retention, sampling, bounded dimensions, appropriate metric resolution, and scoped queries, while preserving incident and audit requirements.

## 12. Security & Governance

### Encryption and credential lifecycle

**KMS key policies** establish authorization for a key; IAM permissions work through the key's policy/delegation model. **Grants** provide scoped delegated use, often for AWS service integrations. Cross-account use generally requires both key-side and caller-side permission. A disabled key, restrictive policy, or scheduled deletion can make otherwise healthy storage inaccessible.

With **envelope encryption**, a data key encrypts the payload and a KMS key protects the data key. Store the encrypted data key with the ciphertext; protect plaintext data keys in memory and use supported encryption context constraints where appropriate. Automatic key rotation does not mean rewriting every old object or rotating an application password. [KMS concepts](https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html)

Use Secrets Manager for managed secret lifecycle where appropriate, prefer temporary role credentials for AWS APIs, and rotate/revoke remaining long-lived credentials through a tested workflow. Encrypt data at rest and use TLS in transit; one does not replace the other. Manage certificate issuance, renewal, trust stores, expiry alerts, and deployment across every termination point.

### Detection and governance services

| Service/control | Main role | Follow-up responsibility |
| --- | --- | --- |
| GuardDuty | Detects suspicious activity from supported telemetry and protection plans | Triage findings and contain compromised resources |
| Security Hub | Centralizes supported security posture/findings workflows | Assign owners, prioritize, and verify remediation |
| Inspector | Finds supported workload vulnerabilities and exposure | Patch/rebuild and verify deployment of repaired artifacts |
| Detective | Supports investigation of related security activity | Build and test an incident hypothesis |
| Access Analyzer | Identifies supported unintended access and policy issues | Reduce permissions without breaking required integrations |

Enabling a service is not remediation. Delegate administration, cover required accounts/Regions, route findings, and measure response. [GuardDuty](https://docs.aws.amazon.com/guardduty/latest/ug/what-is-guardduty.html)

SCPs constrain covered member-account principals; RCPs constrain supported resources; neither grants access. Control Tower preventive, detective, and proactive controls use different enforcement mechanisms. Config conformance packs package rules and supported remediation for consistent assessment. Roll out organization policies through test OUs and examine exceptions, including emergency access and essential service operations.

### Exfiltration, workload supply chains, and incident response

**S3 Block Public Access** limits supported public access paths at account/bucket/access-point levels. It does not stop every broad authenticated cross-account grant. Combine restricted bucket policies, organization/resource conditions, endpoint policies, KMS permissions, and controlled egress. Test service-to-service exceptions before enforcing source-network restrictions that may block legitimate AWS integrations.

WAF protects supported application entry points, Shield mitigates DDoS threats, and Network Firewall inspects steered network traffic. Their scope differs; none makes application authorization optional. Maintain root account security with strong MFA, protected recovery channels, no unnecessary root access keys, and the supported centralized root management model where adopted. Emergency access should be restricted, monitored, tested, and usable when the primary IdP fails.

For a compromised credential, record the identity and scope, contain access, revoke or expire relevant sessions using supported mechanisms, rotate remaining secrets, and preserve evidence. Disabling one access key does not invalidate every already-issued STS session. [Restricting existing temporary credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_control-access_disable-perms.html) Quarantine hosts with a reviewed access path, capture relevant volatile/disk evidence as appropriate, and avoid destroying the only forensic copy during cleanup.

Supply-chain security includes trusted build inputs, dependency control, ECR scanning, image digest pinning, signing, and admission/deployment verification. A signature establishes provenance under a trust policy; a vulnerability scan reports known findings. Neither proves the code is harmless. Keep build and deployment roles separate and prevent an untrusted CI change from obtaining a production signing/deployment identity.

## 13. Infrastructure as Code & Automation

### Ownership, state, and tool selection

Infrastructure as code (IaC) makes desired configuration reviewable and repeatable. It does not establish that a deployment is safe: a valid plan can replace a database, remove connectivity, or grant excessive permissions. Trace resource ownership and state before applying changes.

| Tool | Operating model | Trade-off |
| --- | --- | --- |
| Terraform AWS provider | Terraform computes changes using configuration, provider APIs, and state | Broad ecosystem and explicit plans; team protects state and manages provider/version behavior |
| CloudFormation | AWS manages stack state and resource operations | Native integration and stack lifecycle; updates can enter failed/rollback states |
| CDK | Constructs in programming languages synthesize infrastructure templates | Reuse and abstractions; generated CloudFormation must still be understood |

Terraform **state** maps configuration addresses to remote resources and may contain secrets even when outputs are marked sensitive. Protect remote state with access control, encryption, versioning, and a supported locking mechanism. **Import** adopts an existing resource into management; it does not prove the resulting configuration matches the live resource. **Drift** is divergence between desired, tracked, and actual configuration.

Pin tested CLI/provider/module versions and review dependency lock changes. Lifecycle settings such as `prevent_destroy`, `create_before_destroy`, and `ignore_changes` affect planning, but do not replace independent backup or access controls. Replacement ordering can fail because of name uniqueness, quotas, or resource dependencies.

For the S3 backend, current Terraform supports S3 lockfiles using `use_lockfile`; DynamoDB-based locking is deprecated in current documentation. Use the mechanism supported by the repository's pinned Terraform version. Force-unlock only after proving the prior writer is no longer active; removing an active lock can corrupt coordination. [Terraform S3 backend](https://developer.hashicorp.com/terraform/language/backend/s3)

### CloudFormation lifecycle and fleet deployment

A **stack** owns a collection of resources. A **change set** previews additions, changes, deletions, and replacements, but successful preview does not guarantee runtime success. Check deletion/retention policies for persistent resources. Rollback attempts to restore stack-managed infrastructure; it does not undo every external side effect or database migration. [CloudFormation change sets](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-changesets.html)

When rollback fails, inspect stack events and the original failing resource before continuing recovery. Skipping resources during supported rollback recovery can leave drift requiring reconciliation. **StackSets** deploy supported stacks across accounts and Regions, with concurrency and failure-tolerance controls. Start with a small deployment wave so one bad baseline cannot damage hundreds of accounts simultaneously.

CDK synthesis is a build step: inspect generated templates, IAM changes, assets, and replacements. Avoid nondeterministic environment lookups or unreviewed construct changes. Choose one authoritative owner for each resource; Terraform, CloudFormation, and a Kubernetes controller simultaneously managing the same object can fight one another.

### API automation, GitOps, and delivery identities

CLI/SDK scripts need explicit account/Region selection, pagination, bounded concurrency, deadlines, retry classification, and useful error reporting. A partial list is dangerous input to cleanup automation. SDK paginator objects or CLI automatic pagination help, but know the difference between service page size, client item limits, and continuation tokens. [AWS CLI pagination](https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-pagination.html)

With **GitOps**, Argo CD reconciles Kubernetes resources from declared sources; Helm packages parameterized manifests. Protect repository access and controller permissions, test chart upgrades, and define how emergency changes are reconciled afterward. AWS infrastructure provisioning and Kubernetes application reconciliation have different state and failure boundaries.

GitHub Actions, GitLab CI, or CodePipeline should build immutable artifacts, validate infrastructure/application changes, deploy progressively, and report health. Use **OIDC-based CI authentication** where supported, binding role trust to exact project/repository and approved branch/environment claims. A cross-account deployment role limits target permissions and keeps temporary credentials separate from build identities. [OIDC workloads](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html)

Golden AMIs and immutable node replacement improve repeatability. Automate image builds, patch validation, rollout, and retirement of vulnerable images. After a failed deployment, establish actual resource state, protect data, restore service with a known artifact where safe, then reconcile IaC. Repeated blind applies or manual state edits can deepen the failure.

## 14. Reliability & Disaster Recovery

### Define the recovery contract

The Well-Architected Reliability Pillar emphasizes sound foundations, controlled change, and recovery from failures. Begin with a user-facing SLO and named failure scenarios. **Multi-AZ** addresses zonal failures; **multi-Region** addresses a broader geographic and service boundary while adding replication, routing, and operational complexity.

**RTO** is the target maximum recovery time; **RPO** is the target maximum tolerable data loss expressed in time. In a hypothetical system with RTO 30 minutes and RPO 5 minutes, demonstrate that detection, decision, promotion/restore, capacity readiness, routing, and validation fit within 30 minutes, and that recoverable data is never more than 5 minutes behind. Observed replication lag is evidence, not an unconditional promise.

| Strategy | Recovery environment | Main compromise |
| --- | --- | --- |
| Backup and restore | Restore data and build required infrastructure | Low steady cost, more recovery work |
| Pilot light | Essential state/services maintained; additional capacity started during recovery | More readiness work than warm standby |
| Warm standby | Functional reduced-capacity environment | Must scale and validate under recovery demand |
| Active-passive | One serving environment, another prepared for takeover | Readiness varies from pilot light to full capacity |
| Active-active | Multiple environments serve traffic | Conflict handling, consistency, and failover capacity remain difficult |

These are strategies, not fixed RTO/RPO guarantees. Prefer precreated failover resources and data-plane mechanisms when possible, since an incident may impair the control-plane operations needed to build capacity. [AWS DR strategies](https://docs.aws.amazon.com/whitepapers/latest/disaster-recovery-workloads-on-aws/disaster-recovery-options-in-the-cloud.html)

### Contain failures and bound work

**Cells** divide customers/work into independently recoverable units; **partitions** separate resources or workloads; **bulkheads** cap resource sharing so one component cannot exhaust the whole system. Isolation must include databases, queues, deploy pipelines, quotas, and control dependencies where required. A global configuration rollout can defeat otherwise independent cells.

Timeouts bound waiting, **circuit breakers** stop repeated calls to an unhealthy dependency, **load shedding** rejects excess work, and **graceful degradation** preserves essential functionality. Use retries only when useful work can plausibly succeed. Apply capped exponential backoff with jitter, a shared deadline, and a retry budget; retries at several layers can multiply exponentially.

For a three-AZ service carrying load evenly, losing one AZ leaves roughly two-thirds of its original capacity before replacements. If the other zones cannot carry total demand at safe utilization, the design needs headroom, prewarming, or an explicit degraded mode. Autoscaling speed is constrained by boot, images, networking, quotas, and service capacity.

### Routing, backups, and hidden dependencies

Route 53 failover changes DNS responses subject to caching and health configuration. Global Accelerator changes routing for supported endpoints through its network entry points, avoiding some DNS migration delays, but does not move database state or preserve every connection. Define dependency-aware health: a live web process with an unusable database may not be a viable recovery destination.

**AWS Backup** centralizes supported backup plans and recovery points, with service-specific cross-account/cross-Region features. Verify resource support, copy completion, retention, vault controls, KMS ownership, and recovery permissions. Backup frequency alone does not determine RPO if jobs fail or logs are missing. [AWS Backup](https://docs.aws.amazon.com/aws-backup/latest/devguide/whatisbackup.html)

Map regional dependencies beyond compute: identity/token issuance, DNS, secrets, KMS, ECR images, certificates, build artifacts, observability, quotas, and operator access. Keep recovery material accessible outside the failed boundary. Replication does not protect against every logical corruption or malicious deletion; retain independent protected recovery points.

### Prove recovery

Use **Fault Injection Service** and controlled game days to test explicit hypotheses, such as whether losing a node causes less than a chosen error-budget impact. Define steady-state measurements, a bounded blast radius, stop conditions, ownership, and rollback before injection. A successful node-kill experiment is not proof of regional disaster recovery.

A runbook must identify decision authority, prerequisites, fencing of old writers, data recovery/promotion, capacity activation, traffic changes, validation, and failback. Test it with application integrity checks and realistic traffic. Record actual RTO/RPO, unexpected dependencies, and corrective actions; repeat after meaningful architectural changes.

## 15. Cost Optimization & Capacity Planning

### Understand the bill and useful output

AWS costs combine resource duration, allocated capacity, requests, storage, provisioned performance, and data transfer, with service/Region-specific rules. Track cost per successful request, tenant, job, or stored useful TB alongside reliability. Lower spend caused by failed work is not an efficiency improvement.

**Cost Explorer** analyzes spend and usage. **Budgets** monitors configured thresholds and can trigger supported actions; ordinary budget alerts do not create an immediate hard spending cap. **Cost Anomaly Detection** identifies unusual spend patterns but has detection/data latency. Use allocation tags and cost categories for ownership, then **showback** to report consumption or **chargeback** to allocate actual costs. Account for shared platform costs. [AWS Cost Management](https://docs.aws.amazon.com/cost-management/latest/userguide/what-is-costmanagement.html)

### Optimize resources before commitments

Measure a representative demand period, including peaks and failover. Rightsize EC2 using memory, CPU, network, disk, and latency evidence; averages alone can hide resource cliffs. Validate Graviton compatibility and benchmark the real workload. Improve autoscaling signals and reduce idle capacity while retaining the headroom required by the availability design.

Apply Savings Plans or Reserved Instances to a stable usage baseline after removing waste. Evaluate term, utilization, coverage, flexibility, and the risk of committing to an architecture about to change. Spot fits interruption-tolerant capacity with recovery and useful checkpointing; a discount does not compensate for lost deadlines or duplicate work.

For EKS, include the control plane, support tier, nodes, managed features, idle DaemonSet/system capacity, persistent volumes, load balancers, public IPv4, NAT, and cross-AZ traffic. Pod requests influence scheduling and node purchases; inflated requests can waste capacity, while underallocation can create throttling, eviction, and outages.

### Storage, telemetry, and network economics

S3 lifecycle can lower storage spend while raising request, transition, retrieval, and restore costs. Track noncurrent versions and incomplete multipart uploads. Archive only when retrieval time and frequency fit the workload. EBS sizing includes provisioned IOPS/throughput and retained snapshots; shrinking spend may require migration rather than simply lowering a volume's capacity setting.

CloudWatch charges can grow through log ingestion, indefinite retention, high-resolution alarms, custom metric cardinality, traces, and repeated queries. Start with noisy log sources and unused time series; preserve audit obligations and the signals needed to diagnose rare failures.

Compare **NAT gateways and VPC endpoints** by total cost: gateway hours, processed bytes, interface-endpoint hours per AZ, service-specific processing, and remaining internet egress. S3/DynamoDB gateway endpoints have a different pricing model from interface endpoints. Endpoints are not automatically cheaper for every low-volume service. Account for topology and resilience as well as price.

Cross-AZ and cross-Region transfer rules vary by service and direction. Draw the actual path, including load balancer targets, replication, centralized egress, and inspection, then apply current regional pricing. Do not assume all same-Region traffic is free or that one generic per-GB rate covers every hop.

### Capacity is an operational dependency

Track Service Quotas, subnet addresses, ENIs, database connections, broker capacity, load balancer scale, and available instance types. Quotas authorize resource quantity; Capacity Reservations address matching EC2 capacity. Neither solves application bottlenecks. Forecast expected demand and the extra demand after failover, and request increases before the incident.

For a cost-reduction target, propose measured changes in waves, verify each against latency/error SLOs, and retain a rollback path. Separate one-time cleanup savings from recurring savings, and normalize against traffic so a quiet month does not look like a successful optimization.

## 16. Troubleshooting & Production Operations

### A repeatable investigation sequence

Establish affected users, start time, failing operations, accounts/Regions/AZs, recent changes, and the current SLO impact. Stabilize the system with a bounded mitigation, then follow evidence from the client through dependencies. Preserve timestamps and request IDs so application, network, and AWS records can be correlated.

The CLI can prove identity, query configuration, and inspect health. Specify a profile and Region where relevant. `--debug` reveals request construction, retries, and API responses, but can expose sensitive headers or payloads; redact before sharing. Classify API errors into authentication/authorization, validation, throttling, capacity, and service failure instead of retrying every error.

Read-only examples with illustrative identifiers:

```bash
aws sts get-caller-identity --profile sre
aws ec2 describe-instance-status --include-all-instances --region us-east-1 --profile sre
aws ec2 describe-subnets --region us-east-1 --profile sre \
  --query 'Subnets[].{Subnet:SubnetId,AZ:AvailabilityZone,FreeIPv4:AvailableIpAddressCount}'
aws elbv2 describe-target-health --region us-east-1 --profile sre \
  --target-group-arn arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/example/0123456789abcdef
aws sqs get-queue-attributes --region us-east-1 --profile sre \
  --queue-url https://sqs.us-east-1.amazonaws.com/123456789012/example \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible ApproximateNumberOfMessagesDelayed
```

Keep pagination enabled for inventory unless deliberately testing a page. A `--query` expression filters returned data; it does not necessarily reduce the server-side scan or API calls. Do not perform destructive cleanup from an unverified partial response.

### Symptom → evidence → next hypothesis

| Symptom | Evidence to collect | What it can distinguish |
| --- | --- | --- |
| AccessDenied despite an Allow | Caller ARN, exact action/resource, request context, applicable policies, CloudTrail event | Wrong session, condition mismatch, explicit deny, boundary/session restriction, SCP/RCP, endpoint or KMS policy |
| AssumeRole fails | Source permissions, destination trust, external ID, MFA, OIDC audience/subject, session duration | Trust failure versus permission ceiling or malformed federation context |
| EC2 launch fails | Exact API/ASG activity error, quota usage, subnet addresses, AMI/type architecture, KMS access | Insufficient capacity versus quota, networking, or encrypted-volume permission failure |
| EC2 running but unavailable | System/instance/EBS status, serial/console output, guest logs, memory/disk, routes | Host failure versus guest exhaustion or network path failure |
| TCP connection times out | Source/destination, route tables, SGs, both NACL directions, flow logs, listener | Missing route/filter/listener versus unresponsive application; no single timeout proves the cause |
| DNS lookup fails | Resolver used, answer/rcode, private zone associations, forwarding rules, endpoint health | NXDOMAIN/shadowing versus forwarding, network, or cache problem |
| ALB unhealthy or 502/503/504 | Target reason codes, access/connection logs, ELB versus target status, backend latency | Health matcher, reset, no target, backend TLS, connect timeout, or slow response |
| EKS Pod Pending | Scheduler events, requests, taints, affinity, node pool limits, PVC topology | No suitable compute versus policy/topology constraints |
| EKS Pod stuck creating its sandbox | CNI logs/events, ENI limits, free subnet IPs, contiguous prefixes, EC2 API errors | IP exhaustion, fragmentation, CNI IAM/throttling, or node configuration |
| EBS attach fails | Pod/node AZ, volume AZ, attachment state, CSI logs/role, KMS key | Zonal mismatch, stale attachment, permission, or node volume limit |
| EFS mount times out | DNS result, mount targets, NFS TCP 2049 path, SG/NACL, client mount logs | Missing AZ target/DNS/network versus IAM/POSIX/access-point rejection |
| S3 403/404 | Exact key/version, bucket Region, caller policies, KMS, endpoint policy, request ID | Access denial versus missing object; without ListBucket permission a missing key may produce 403 |
| RDS failover or connection storm | RDS events, writer/endpoint DNS, pool state, connections, waits, lag | Promotion/reconnect issue versus slow queries, locks, saturation, or stale reads |
| SQS backlog or DLQ growth | Oldest age, arrivals/completions, receive counts, visible/in-flight counts, consumer errors | Insufficient throughput versus poison messages, visibility churn, or blocked FIFO group |
| Missing CloudWatch metric/log | Account/Region, namespace/dimensions, timestamps, agent/exporter, IAM/network, delivery errors | No publication versus wrong query identity, delivery failure, or ingestion delay |

Use the row to form a testable hypothesis, not to jump from a symptom to a single cause. For example, adding nodes does not solve EKS sandbox failures when every candidate subnet is out of addresses.

### AWS incidents and operational control

Consult **AWS Health** for account-specific events and the public service status for broader context, while comparing independent application telemetry. A green status page does not disprove a localized fault. Check quota utilization and recent control-plane errors before attributing every failure to AWS.

During a service degradation, stop unnecessary changes, cap retries, shed optional traffic, protect durable work, and use tested failover criteria. Record mitigation decisions and update stakeholders through the incident process. After recovery, reconcile temporary changes, validate data integrity, and turn the timeline into concrete reliability improvements.

## 17. Expert Interview Scenarios

These are worked design approaches, not AWS service guarantees. Begin each answer with requirements and assumptions, draw the dependency path, explain failure behavior, then name the measurements and tests that would establish success.

### 1. Design a highly available application across three AZs

Clarify traffic, latency SLO, state, and acceptable degradation. Put an ALB in subnets across three AZs, distribute stateless ECS tasks or EKS Pods across those AZs, and use the appropriate Multi-AZ database deployment. Keep data subnets isolated from general internet routing and application permissions scoped to workload roles.

Provide local egress or required endpoints without one shared zonal dependency. Spread replicas with topology rules and ensure the remaining two AZs can sustain demand after one fails. Test AZ isolation, dependency health, reconnect behavior, rollout under reduced capacity, and user-visible error-budget impact. Three copies alone do not establish usable failover capacity.

### 2. Design two-Region DR with defined RTO/RPO

For a hypothetical RTO of 30 minutes and RPO of 5 minutes, propose warm standby only if measured scale-up and data recovery fit those targets. Precreate networking, identity, keys, artifacts, secrets, certificates, and quotas. Replicate application data with a service-supported mechanism, and retain independent protected backups.

Specify how to detect failure, fence the old writer, assess recoverable data, promote the recovery database, activate capacity, change traffic, and validate business operations. Budget each step and include decision time. Rehearse with realistic data and document failback; a DNS change is only one part of recovery.

### 3. Explain complete IAM authorization evaluation

Start with the authenticated principal and session, action, resource, and request context. Look for applicable explicit denies. Then explain the appropriate identity/resource grants and permission ceilings: boundary, session policy, SCP, and RCP. Include service-specific controls such as KMS and VPC endpoint policies where relevant.

Distinguish same-account resource grants, cross-account evaluation, and role assumption. Explain why a resource policy granting directly to a session can behave differently from one naming its role, and why organization controls do not grant access. Finish with a concrete request so the evaluation can be tested rather than reduced to a misleading universal intersection formula.

### 4. Diagnose AccessDenied despite an explicit Allow

Prove the actual caller and compare the failed request with the allowed action/resource. A deployment may have used an unexpected profile or old environment credentials. Check conditions, resource policy denies, session/boundary limits, organization policies, endpoint policy, and dependent KMS permissions.

For example, `s3:GetObject` can be allowed while decrypting an SSE-KMS object fails because key authorization is missing. Use the error details and logged request context to isolate the failing control. Add only the required permission or correct the request; attaching AdministratorAccess may leave explicit denies in place and makes the security problem worse.

### 5. Design organization-wide SSO

Use an organization Identity Center deployment connected to the workforce IdP. Map IdP groups to reviewed permission sets and assign them to accounts according to job function and environment. SAML handles sign-in, while SCIM keeps users/groups provisioned.

Separate daily administration, production deployment, read-only investigation, and emergency access. Define group approval, MFA, session duration, removal of access, and audit. Test permission-set updates and deprovisioning, including existing session behavior. Keep a monitored emergency route that does not depend entirely on the unavailable primary IdP.

### 6. Implement secure cross-account access without static credentials

Have the workload use its native temporary identity, then assume a narrowly scoped role in the target account. Restrict target trust to the intended principal and conditions; grant the source only the required `sts:AssumeRole` access. For a third-party deputy, include a customer-specific external ID.

Alternatively, use direct resource-policy access where the service supports the desired contract. Account for both sides of authorization and cross-account KMS access. Verify automatic refresh, session expiration, audit identity, and a test where an unrelated principal is denied. For CI, start with constrained OIDC federation rather than stored AWS keys.

### 7. Explain Internet → ALB → EKS Pod → RDS

Assume an internet-facing ALB with HTTPS and IP targets:

```text
Client → DNS answer → ALB public endpoint:443
                       │ TLS termination and listener rule
                       ↓
                 target group → Pod private IP:application-port
                                      │ database DNS + TLS
                                      ↓
                              RDS private endpoint:DB-port
```

The ALB accepts one connection and opens another to the target. Trace subnet routes, ALB egress, target ingress, health checks, NACL return ports, CNI routing, and any Pod security groups. The application resolves RDS and opens a separate connection permitted by the database security group. No NAT gateway is required for this ordinary private VPC path. In instance target mode, include NodePort and node-to-Pod forwarding. IAM permissions and database authentication remain separate from packet delivery.

### 8. Diagnose EKS startup failures from VPC IP exhaustion

Distinguish scheduler `Pending` from a scheduled Pod stuck in `ContainerCreating` with sandbox/CNI errors. Inspect events, CNI logs, free subnet addresses, node ENI/IP limits, warm-pool settings, and EC2 API permissions/throttling. With prefix delegation, total free address count can hide lack of a sufficiently contiguous range.

Mitigate by moving capacity to suitable subnets, reducing unnecessary reservations, or safely replacing nodes with corrected settings. Longer term, plan larger/nonoverlapping address space, supported prefix delegation, custom networking, or IPv6. Confirm that subsequent scale-out works; adding nodes to an exhausted subnet can consume the remaining addresses without fixing Pods.

### 9. Scale unpredictable traffic using Spot and On-Demand

Keep an On-Demand baseline for essential availability, with diversified Spot capacity for replaceable work. Use a mixed-instance ASG or appropriate container capacity pools. Validate equivalent resource requirements and spread across AZs and instance pools.

Scale on a useful load signal, such as queue delay or request load per target, and budget for bootstrap and warmup. Handle interruptions with draining, checkpointing, and idempotent reprocessing. Cap concurrency against database limits and define an overload mode. Test simultaneous Spot loss and demand growth; spare quota does not guarantee replacement capacity.

### 10. Investigate intermittent ALB 504s

Separate ALB-generated 504s from backend-generated responses. Correlate access-log timings with target/AZ, application traces, database waits, connection pools, and rollout times. Check target connection establishment, backend TLS, response/idle timing, and NACL return paths.

If one AZ is affected, inspect its routes and dependencies; if only slow endpoints fail, investigate their work and downstream waits. A saturated database pool can make healthy web targets stop responding. Reduce expensive work or bound concurrency while fixing the bottleneck. Increase a timeout only when the legitimate request contract and available resources support the longer wait.

### 11. Recover production databases after AZ or regional loss

For AZ loss, identify the actual Multi-AZ deployment and use its supported failover mechanism. Confirm the new writer, reconnect clients, validate commits, and watch for connection storms. Avoid promoting an unrelated lagging replica simply because its endpoint responds first.

For Region loss, follow the DR runbook: determine replica currency or restore point, fence prior writers, promote/restore, reconcile endpoint/secrets/key access, and verify business data. State the possible data loss before opening writes. Keep evidence of ambiguous operations for reconciliation, and plan failback from the newly authoritative dataset.

### 12. Explain RDS Multi-AZ versus read replicas

Use three cases. A Multi-AZ DB instance has a synchronous, nonreadable standby for automatic failover. A supported Multi-AZ DB cluster has a writer and two readable instances across three AZs with semisynchronous behavior. A conventional read replica uses engine-specific, usually asynchronous replication and primarily provides read scaling or a promotion option.

Discuss read-after-write behavior, lag, promotion, lost connections, endpoint changes, and the possibility of data loss for asynchronous recovery. Do not promise one universal failover time. The engine and deployment type determine the mechanism; application retry and reconnect behavior determine the user impact.

### 13. Protect durable S3 data from accidental deletion

Choose a storage class with the required AZ durability and retrieval behavior. Enable versioning, restrict permanent version deletion and lifecycle changes, and use Object Lock retention/legal holds where the business requires them. Separate ordinary writers from retention administrators.

Add a separately controlled copy or backup, with deliberate replication/delete semantics and independently usable encryption keys. Alert on destructive policy changes and failed replication. Demonstrate restoration of overwritten objects and objects hidden by delete markers, and test recovery if the source account is compromised. Durability of the storage medium does not prevent authorized logical deletion.

### 14. Troubleshoot a Lambda/SQS pipeline with millions of messages

Measure oldest age, ingress, successful completions, visible/in-flight messages, Lambda duration/errors/throttles, and DLQ arrivals. Check event source mapping state, reserved and mapping concurrency, batch settings, visibility timeout, FIFO group distribution, and downstream capacity.

If all batch records repeat after one failure, implement configured partial batch responses and durable idempotency. Isolate poison messages and repair their cause. Increase consumers only to a tested downstream-safe level. Estimate drain time as `backlog / (completion rate − arrival rate)` when completion exceeds arrivals; otherwise recovery requires reducing arrivals or raising useful capacity first.

### 15. Diagnose NAT gateway port exhaustion

Correlate outbound failures with NAT `ErrorPortAllocation`, active connection metrics, destination concentration, and connection churn. Inspect client reuse, source ephemeral ports, conntrack, and idle resets as competing explanations. Translation capacity is finite for each source IPv4 address and destination combination, so concentrated connections can fail before aggregate bandwidth is exhausted. A healthy NAT resource state does not establish available translation capacity. [NAT troubleshooting](https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-troubleshooting.html)

Reuse bounded connection pools and reduce retry storms. Where supported, add NAT address capacity or distribute traffic across gateways; use VPC endpoints to remove relevant AWS API traffic from NAT. Route conventional zonal gateways locally and validate source-address allowlists before changing egress addresses. Test at peak destination concentration, not only aggregate throughput.

### 16. Centralize multi-account monitoring, logging, and security

Create protected log archive and security administration accounts, with organization trails and explicitly selected data events. Enable required Config/security service coverage, delegate supported administration, and use supported cross-account observability for investigation.

Preserve source account/Region/application identity in telemetry. Protect ingestion and KMS policies, retention, and archive deletion. Alert on logging gaps and failed exports as well as application failures. Centralization must not make the only usable dashboard or emergency access path depend on the failing workload account. Test a member-account incident from detection through evidence retrieval and containment.

### 17. Migrate on-premises workloads with minimal downtime

Inventory dependencies, protocols, data size/change rate, identity, latency requirements, and allowable downtime. Establish redundant connectivity, address/DNS planning, a target landing zone, and observable application behavior before cutover. Choose rehost/replatform/refactor per workload rather than forcing one approach.

Seed data, run supported continuous replication or CDC, validate schema and checksums/business invariants, and rehearse cutover. During the final transition, control writes, measure lag, fence the old writer, apply remaining changes, switch endpoints, and verify transactions. Define a rollback point and data reconciliation strategy; routing back after new writes is more complicated than restoring an old DNS value.

### 18. Design a secure EKS platform for isolated teams

Clarify whether teams are trusted coworkers or mutually untrusted tenants. Namespaces and RBAC help organize cooperative tenancy, but stronger boundaries may require separate accounts/clusters and dedicated nodes. Shared kernels, cluster-scoped controllers, DNS, and API capacity create shared risk.

Apply restricted Pod security, resource quotas, network policy, workload IAM roles, controlled secrets, admission policies, and supply-chain verification. Prevent access to node credentials and dangerous host/privileged features. Separate platform and workload administration and monitor audit activity. Test attempted cross-team access, noisy-neighbor exhaustion, controller compromise, and an upgrade that affects shared components.

### 19. Reduce AWS cost by 30% without compromising reliability

Treat 30% as a target to validate against the bill and SLOs. Attribute spending, measure cost per useful unit, and identify idle resources, oversized requests/instances, excessive logs, storage versions, transfer paths, and unused commitments.

Prioritize reversible waste removal, then benchmark rightsizing/Graviton, improve scheduling/autoscaling, and optimize storage/egress. Apply commitments to the stable remaining baseline and Spot only to suitable work. Preserve failure headroom, backups, security, and observability. Roll out in waves and compare equal-demand periods. If the measured safe opportunities total less than 30%, report the remaining trade-offs instead of asserting savings that require weakening the reliability objective.

### 20. Handle AWS degradation with cascading application failures

Declare an incident from user impact, identify affected operations and failure domains, and consult AWS Health alongside independent telemetry. Stop retry multiplication and unneeded deployments. Apply timeouts, circuit breaking, concurrency caps, and load shedding; preserve essential transactions and buffer suitable work durably.

Use a pretested failover only when the destination is healthy, has capacity, and can meet the data-loss constraint. Moving traffic to a second Region with the same failing dependency can spread the outage. After stabilization, reconcile uncertain writes and queued work, remove temporary controls safely, and document why the dependency failure cascaded beyond its intended boundary.

### 21. Build a landing zone for hundreds of accounts

Organize OUs by governance requirements, with separate management, security, log archive, infrastructure, and workload accounts. Automate account vending through Control Tower/Account Factory or a deliberately owned alternative. Baseline identity, controls, logging, network allocation, budgets, tags, and lifecycle ownership.

Use versioned policies and deployment waves, delegated administrators, central inventory, and drift detection. Manage exceptions with owners and expiry, and test account closure/offboarding as well as creation. Avoid an unsegmented shared network or a globally privileged deployment role that lets one compromised workload affect the fleet. Validate a newly created account before declaring it production-ready.

### 22. Compare EC2, ECS, EKS, Fargate, and Lambda

| Choice | Favor it when | Responsibility/trade-off to discuss |
| --- | --- | --- |
| EC2 | Need host control, specialized configuration, or predictable sustained resource use | OS lifecycle, placement, capacity, and application scheduling |
| ECS | Want AWS-native container orchestration | Task/service design and compute choice, without Kubernetes API operations |
| EKS | Need Kubernetes APIs, ecosystem, and platform controls | Kubernetes compatibility and add-on/workload operations; management scope depends on mode |
| Fargate | Supported containers benefit from managed compute | Resource/feature constraints, startup behavior, networking, and per-task/Pod economics |
| Lambda | Work fits its invocation, duration, and integration contracts | Cold starts, concurrency, retries, runtime limits, and downstream load |

Compare them for the same workload and SLO. ECS/EKS select orchestration; EC2/Fargate select container compute options; Lambda offers a function execution model. Include team operational capacity, portability requirements, security boundaries, and total cost, then justify the simplest option that meets the stated requirements.
