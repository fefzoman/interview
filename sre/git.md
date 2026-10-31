# Git — SRE Expert Checklist

A failed deployment may require undoing a configuration change, finding a lost commit, or proving which source produced a running image. Git helps with all three, but only if you distinguish stored objects, references, local files, and the systems that consume them. An SRE should be able to explain those boundaries and recover without destroying evidence or another engineer's work.

This guide assumes basic command-line experience and follows the supplied checklist. Commands use `main`, `origin`, and example paths; adapt them to the repository. Uppercase names such as `BAD_COMMIT` represent values you must replace. Recovery examples assume committed work unless stated otherwise. Before destructive operations, preserve the repository and uncommitted files; a backup branch preserves commits, not unstaged changes or untracked files. Check `git --version` and the installed command's help when tooling differs.

## Contents

1. [Fundamentals & Architecture](#1-fundamentals--architecture)
2. [Core Git Operations](#2-core-git-operations)
3. [Branching & History Management](#3-branching--history-management)
4. [Git Internals & Performance](#4-git-internals--performance)
5. [Git Networking & Authentication](#5-git-networking--authentication)
6. [Git Security & Supply Chain](#6-git-security--supply-chain)
7. [Git in CI/CD & GitOps](#7-git-in-cicd--gitops)
8. [Reliability, Recovery & Troubleshooting](#8-reliability-recovery--troubleshooting)
9. [Expert Interview Scenarios](#9-expert-interview-scenarios)

## 1. Fundamentals & Architecture

### Distributed repositories, working tree, staging area, and `.git`

Git is a distributed version control system: a normal full clone contains local history and can create commits, inspect history, and merge without contacting a central server. The hosting service supplies collaboration and policy, but commits do not require it. Shallow and partial clones deliberately contain less information, and a clone does not automatically include hosting metadata, every server ref, or external Git LFS content.

The **working tree** contains checked-out files. The **staging area**, implemented by the **index**, describes the proposed next snapshot. The repository stores objects, references, configuration, and operational metadata, usually beneath `.git`. In a linked worktree or submodule, `.git` can be a file pointing elsewhere. A bare repository has repository data without an ordinary working tree and is common on servers.

```text
working tree --git add--> index --git commit--> commit object
     ^                                           |
     +----------- checkout / restore ------------+
```

Editing a file after staging it does not change the staged version. This is why reviewing both `git diff` and `git diff --staged` matters before a production change.

### Blobs, trees, commits, and annotated tags

Git represents a snapshot with objects linked by object IDs:

| Object | Contains | Operational consequence |
| --- | --- | --- |
| Blob | File content, without its filename | Identical content can share one blob |
| Tree | Entry names, modes, and IDs for blobs or subtrees; submodules use commit IDs | Names and directory structure belong here |
| Commit | Root tree ID, parent IDs, author, committer, message, and optional headers/signature | A commit identifies both a snapshot and its history |
| Annotated tag | Target ID, object type, tag name, tagger, message, and optional signature | Release metadata is itself an object |

A lightweight tag is a reference directly to an object; it has no separate tag object. A normal branch is a movable reference to a commit. Git stores snapshots conceptually, even when storage compression represents an object as a delta against another object.

### Content addressing, SHA-1, SHA-256, and object IDs

An object ID hashes an object header and its content: conceptually `hash(type + " " + length + NUL + content)`. Changing any hashed byte changes the ID. Changing a tree or a parent ID therefore changes the commit ID even if the human-readable patch looks identical.

SHA-1 IDs have 40 hexadecimal characters; SHA-256 IDs have 64. Abbreviations must be unambiguous within the repository and can become ambiguous as it grows. Scripts should avoid assuming all IDs have exactly 40 characters. An object hash detects content changes; it does not establish who approved that content.

Git supports creating SHA-256 repositories with `git init --object-format=sha256`. Do not assume every hosting service, client library, or existing SHA-1 repository can interoperate with them. Verify the actual toolchain before choosing an object format. See [Git object-format options](https://git-scm.com/docs/git-init) and the [hash transition design](https://git-scm.com/docs/hash-function-transition).

### Commit DAG and parent relationships

Commits form a **directed acyclic graph (DAG)**: a commit points backward to its parent commits. A root commit has no parent, an ordinary commit has one, and a merge commit has two or more. Branch names label tips; commits do not contain a branch membership field.

```text
A---B---C-------M   main
     \         /
      D-------E     feature
```

`M^1` is the first parent, normally the branch checked out when the merge was created; `M^2` is the second parent. `HEAD~2` follows the first-parent chain twice. Parent order matters when reverting a merge or reading release history with `git log --first-parent`.

### References, HEAD, detached HEAD, and upstreams

Local branches live under `refs/heads/`, tags under `refs/tags/`, and remote-tracking refs commonly under `refs/remotes/origin/`. `HEAD` usually contains a symbolic reference such as `ref: refs/heads/main`. A **detached HEAD** points directly to a commit: new commits are valid, but create a branch before leaving if you want an easy way to retain them.

`origin/main` records the last fetched state of a remote branch; it is not a live connection to the server. A local `main` can move independently. Its upstream configuration tells Git which remote/ref to use for tracking comparisons and default integration behavior; push behavior also depends on push configuration.

```bash
git branch -vv
git branch --set-upstream-to=origin/main main
git rev-parse --symbolic-full-name '@{upstream}'
git symbolic-ref HEAD
git switch -c rescue-detached
```

The last command names the current commit, including work made while detached. `origin/HEAD` can also be a symbolic ref to a remote-tracking branch; it does not make your local `HEAD` follow the server's default branch automatically.

### Reflog, reachability, and garbage collection

An object is reachable when Git can follow references and object links to it. Branches and tags keep history reachable, and reflog entries can also keep old tips available during their retention period. A **reflog** records local reference movements, including resets and rebases. It is local operational history, not a shared audit log, and is not transferred by clone or fetch.

Deleting a branch removes a name, not necessarily its commits. Garbage collection can eventually remove unreachable objects after the relevant retention and expiry conditions are met. Recovery is therefore time-sensitive. Reflogs can be disabled or expired; server and bare-repository behavior may differ. Never depend on reflog retention as a backup policy.

Loose objects are individually compressed files. Packfiles group objects and can delta-compress related content. Neither format changes the logical object ID. The index is separate mutable state used to construct the next tree, not another name for the object database.

## 2. Core Git Operations

### Create, inspect, stage, and commit

`init` creates repository metadata; `clone` copies a repository and normally configures a remote and checks out an initial branch. `status` compares HEAD, index, and working tree. `add` stages selected content, and `commit` records the index as a snapshot linked to the current history.

```bash
git init -b main
git clone https://git.example.com/platform/service.git
git status --short
git add -p
git diff
git diff --staged
git commit -m "fix: correct readiness probe"
git log --graph --decorate --oneline --all -20
git show HEAD
```

These are independent examples, not one setup script. `git diff` shows unstaged changes; `git diff --staged` shows the proposed commit. `git show COMMIT` displays commit metadata and changes, while `git show COMMIT:path/to/file` reads a file from that snapshot. `git commit -a` stages tracked modifications and deletions but does not include new untracked files.

### Fetch, pull, push, merge, and rebase

`fetch` receives objects and updates refs according to fetch mappings without integrating them into the current branch. `pull` fetches and then integrates, with behavior controlled by options and configuration. Make the decision explicit: `git pull --ff-only` rejects divergence, while `git pull --rebase` replays local work. `push` sends objects and requests remote ref updates; the server can reject those updates through permissions, hooks, or history policy.

```bash
git fetch origin
git log --left-right --oneline HEAD...origin/main
git merge --ff-only origin/main
git push -u origin HEAD
```

A merge combines histories; a rebase recreates selected commits on another base. The appropriate choice depends on whether the commits are private and whether preserving the original topology matters.

### Switch, checkout, branch, and tag

`switch` handles branch navigation; `restore` handles file restoration. Older `checkout` supports both roles, which makes its intent less obvious in runbooks. `branch` creates, lists, renames, or deletes branch refs without normally switching the working tree. `tag` manages release names; tags do not automatically travel with every push.

```bash
git switch -c fix/probe
git switch main
git switch --detach v1.2.3
git branch -d fix/probe
git tag -a v1.2.4 -m "Release 1.2.4"
git push origin refs/tags/v1.2.4
```

`branch -d` checks whether Git considers the branch merged; `-D` overrides that safeguard. Treat published release tags as immutable even when a server technically permits moving them.

### Reset vs restore vs revert

For a commit-level reset, Git moves the current branch, or detached HEAD, to the target and optionally updates other state:

| Operation | HEAD / branch | Index | Working tree | Typical use |
| --- | --- | --- | --- | --- |
| `reset --soft TARGET` | Moves | Preserved | Preserved | Rebuild recent local commits |
| `reset --mixed TARGET` | Moves | Replaced from target | Preserved | Uncommit and unstage local work |
| `reset --hard TARGET` | Moves | Replaced | Tracked files replaced | Discard local tracked state after preservation |
| `restore --staged PATH` | Unchanged | Restored from HEAD by default | Preserved | Unstage a path |
| `restore PATH` | Unchanged | Preserved | Restored from index | Discard unstaged changes to a path |
| `revert COMMIT` | New commit on current history | Updated | Updated | Undo a published change with an audit trail |

`reset PATH` is a path-level operation and does not move HEAD. `restore --source=COMMIT --staged --worktree -- PATH` can replace both staged and local content, so specify the destination deliberately. `reset --hard` can also remove untracked paths that obstruct writing tracked files. It does not safely preserve arbitrary local work.

Use revert for shared deployment history. A revert applies an inverse change to the current state and can conflict; it is not a time machine for databases or external infrastructure.

### Cherry-pick, stash, reflog, and bisect

`cherry-pick COMMIT` applies a selected commit's change onto the current branch and normally creates a new commit. It is useful for backports, but repeating it across branches can complicate later merges. `stash` stores a temporary working state locally; use descriptive messages and remember that ignored files require separate treatment.

```bash
git stash push -u -m "before incident investigation"
git stash list
git stash apply 'stash@{0}'
git reflog --date=iso -20
git bisect start
git bisect bad BAD_COMMIT
git bisect good GOOD_COMMIT
git bisect run ./test-regression.sh
git bisect reset
```

The stash commands assume a stash was created. `-u` includes untracked files, not ignored ones; `apply` retains the stash, while successful `pop` removes it. Conflicts require normal resolution. A stash remains local and is unsuitable as the only copy of valuable work.

Bisect narrows the regression to a commit using a reproducible test. For `bisect run`, exit 0 means good, ordinary nonzero failure codes mean bad, and 125 means skip; special or out-of-range codes can abort the run. Flaky tests and incompatible historical environments can produce misleading results.

### Interactive rebase, amend, squash, and fixup

`git commit --amend` replaces the current commit with a new one. `git rebase -i BASE` lets you reorder, edit, drop, squash, or fix up commits after the selected base. `squash` combines changes and edits the message; `fixup` normally discards the fixup commit's message.

```bash
git commit --fixup=TARGET_COMMIT
git rebase -i --autosquash BASE_COMMIT
```

Review the resulting diff and run relevant checks. Rewriting a commit usually rewrites its descendants because their parent IDs change. Coordinate any rewrite of published history, preserve the original tip, and re-sign rewritten commits if policy requires signatures. Ordinary rebase does not preserve merge topology; `--rebase-merges` recreates it with additional review needs. See the [rebase manual](https://git-scm.com/docs/git-rebase).

### Repository configuration files

`.gitignore` controls which untracked paths Git normally offers to add; it does not untrack existing files or prevent forced additions. To stop tracking a file while keeping the local copy, use `git rm --cached -- PATH`, add an ignore rule, and commit the change. This does not remove earlier copies from history.

`.gitattributes` applies path-specific behavior such as line-ending normalization, diff/merge drivers, and LFS filters. A rule such as `*.sh text eol=lf` can prevent cross-platform script failures. Changing normalization can cause a large diff; review intentional renormalization separately.

`.gitmodules` records submodule paths and URLs, while the superproject tree records each selected submodule commit. A clone may require `git submodule update --init --recursive`. Review URLs and exact commits before fetching dependencies, especially in untrusted changes.

### Worktrees, sparse checkout, partial clone, and shallow clone

| Mechanism | Reduces or separates | Tradeoff |
| --- | --- | --- |
| Worktree | Multiple checkouts sharing an object database | Separate index/HEAD per checkout, shared repository resources |
| Sparse checkout | Materialized working-tree paths | History remains unless combined with another mechanism |
| Partial clone | Initially transferred object classes | Missing objects may require later network access |
| Shallow clone | Available commit ancestry | Merge bases, tags, bisect, and changelogs may be incomplete |

```bash
git worktree add ../incident-fix -b incident/fix origin/main
git clone --filter=blob:none --sparse https://git.example.com/platform/mono.git
git sparse-checkout set services/payments libs/common
git clone --depth=20 https://git.example.com/platform/service.git
```

Run the sparse-checkout command inside the partial clone; the clone examples are alternatives. Partial clone requires server support and may fetch omitted blobs during checkout or inspection. A shallow clone can be deepened with `git fetch --deepen=100` or made complete with `git fetch --unshallow` when the source has full history. See the [clone options](https://git-scm.com/docs/git-clone).

## 3. Branching & History Management

### Fast-forward, three-way merge, and conflicts

A fast-forward moves a branch to a descendant without creating a merge commit. If both sides have advanced, a three-way merge compares each tip against a merge base and combines the changes. `git merge-base main feature` identifies a best common ancestor; complex histories can have more than one merge base.

The `ort` strategy handles ordinary two-head merges in modern Git. Strategy selection (`-s`) differs from strategy options (`-X`). In particular, `-s ours` records a merge while keeping our entire tree; `-X ours` favors our side for conflicting hunks while retaining other incoming changes. Neither is a general-purpose substitute for reviewing conflicts. Octopus merges can join multiple heads when the merge is straightforward.

```bash
git status
git diff --name-only --diff-filter=U
git ls-files -u
git add -- resolved-file.yaml
git merge --continue
```

During a conflict, the index can hold stage 1 (base), stage 2 (ours), and stage 3 (theirs); some entries may be absent in add/delete conflicts. Resolve the intended behavior, not just conflict markers. During rebase, “ours” refers to the rebased-on history and “theirs” to the commit being replayed, which surprises people applying merge terminology mechanically.

### Rebase mechanics and branching models

Rebase selects commits, checks out a new base, and replays their changes as new commits before moving the branch tip. The new parent alone is enough to change the ID; committer metadata and conflict resolutions can also change. A clean-looking linear history does not prove that the replay preserved behavior.

| Model | How work reaches production | SRE consideration |
| --- | --- | --- |
| GitFlow | Feature/develop/release/hotfix branches | Supports parallel release lines but creates integration and backport work |
| Trunk-based development | Small changes integrate frequently into trunk | Requires fast checks, feature flags where needed, and reversible changes |
| Release branches | Selected fixes land on maintained version branches | Track backports and test each supported line |

Choose the model around release cadence and maintenance obligations. A branch name alone is neither an approval record nor evidence that a commit was deployed.

### Signatures and protected branches

Signed commits and annotated tags bind object content to a signing key. Branch protection can require reviews, status checks, signed commits, or restricted updates depending on the host. These are separate controls: a valid signature does not establish that tests passed, and a required check is useful only if its workflow and execution context are protected.

### Force push and explicit leases

`--force` can replace a remote tip even when that discards others' history. `--force-with-lease` adds an expectation about the old remote value. Its default expectation commonly comes from a remote-tracking ref, which a background fetch can update without your review.

For an authorized rewrite of a private feature branch, capture the remote tip before rewriting and review it. After rewriting, push with that exact expectation:

```bash
git fetch origin
expected=$(git rev-parse refs/remotes/origin/feature)
git branch backup/feature-before-rewrite feature
# Perform and verify the intended rewrite of feature before pushing.
git push --force-with-lease="refs/heads/feature:$expected" origin feature:feature
```

The lease rejects a changed server tip; it does not decide whether your rewrite is correct or approved. Use ordinary pushes on protected shared history. The [push manual](https://git-scm.com/docs/git-push) explains the background-fetch hazard.

### Deleted branches, incorrect rebases, and divergent histories

After a mistaken reset or rebase, identify the old tip in the reflog, inspect it, and create a rescue branch before moving anything else. A deleted branch's own reflog may be gone, so inspect HEAD and other surviving reflogs. `ORIG_HEAD` may help after some operations but can be overwritten by later commands.

A non-fast-forward push means the server cannot move the destination tip forward to your proposed tip under the normal rule. Fetch and compare both histories. Merge shared histories or rebase your unpublished commits, resolve conflicts, rerun checks, and push normally. If someone pushes again, repeat against the new state; do not turn an integration race into a force push.

### Reverting a merge and the meaning of `-m`

`git revert -m 1 MERGE_COMMIT` reverses the merge's change relative to parent 1 while preserving the graph. `-m` selects the **mainline parent number**, not a message or a branch name. Inspect parent order first:

```bash
git show --no-patch --pretty=raw MERGE_COMMIT
git diff MERGE_COMMIT^1 MERGE_COMMIT
git revert -m 1 MERGE_COMMIT
```

The original merge remains an ancestor. Merging the same branch again will not automatically restore changes from already-merged commits. Reintroducing them may require reverting the revert or creating new corrected changes. See the [merge-revert warning](https://git-scm.com/docs/git-revert).

## 4. Git Internals & Performance

### Loose objects, packfiles, and delta chains

In the traditional files layout, a loose object's path uses the first two hexadecimal ID characters as a directory and the remainder as a filename. A packfile contains a header, object entries, and a checksum trailer; entries can store a complete compressed object or a delta against a base. Pack indexes map IDs to offsets, avoiding a sequential scan.

Delta bases need not be the previous version of the same path. Long chains cost CPU and reads during reconstruction, so repacking balances disk use against access cost. A thin transfer pack can reference bases the receiver already has; the receiver resolves those dependencies when storing it. The [pack format documentation](https://git-scm.com/docs/gitformat-pack) defines the on-disk details.

### Index structure and reference resolution

The index tracks path, mode, object ID, stage, and filesystem metadata used to detect changes efficiently. Extensions can cache tree information, filesystem monitor state, or untracked-directory information. It can contain conflict stages rather than one final entry per path. `git ls-files --stage` exposes the staged IDs; copying the working tree alone does not preserve this state. See the [index format](https://git-scm.com/docs/gitformat-index).

Reference lookup resolves names such as branches, tags, and revision expressions into objects. Under the files ref backend, refs may be loose files or entries in `packed-refs`; symbolic refs point to other names. Other ref backends exist, so scripts should use `rev-parse`, `for-each-ref`, `symbolic-ref`, and `update-ref` instead of assuming every branch is a file.

### Commit-graph, bitmaps, and multi-pack indexes

A commit-graph caches commit relationships and generation information to speed ancestry queries. Reachability bitmaps encode reachable object sets for faster enumeration during supported fetch and maintenance operations. A multi-pack index locates objects across multiple packs without opening every pack index. These accelerate access; they do not replace the underlying history or act as backups.

Measure first: slow `status` often points to filesystem scanning; slow `log` may involve graph traversal; slow clone can involve network, server pack generation, large blobs, or checkout filters. `git count-objects -vH` gives a useful initial storage summary.

### Maintenance, pruning, and repacking

`git gc` combines maintenance tasks such as repacking and expiring eligible data. `git repack` reorganizes packs; pruning removes eligible unreachable objects. Prefer supported scheduled maintenance for active repositories rather than repeatedly running expensive aggressive repacks.

Do not run immediate pruning during recovery or alongside object-writing processes. Reflog expiry reduces the available recovery window, and `git gc --prune=now` increases concurrent-write risk. Preserve a copy and understand retention before maintenance. The [garbage collection manual](https://git-scm.com/docs/git-gc) explains these constraints.

### Large repositories, LFS, submodules, and subtrees

Sparse checkout reduces checked-out files; partial clone reduces initial object transfer. Git LFS stores small pointer files in Git and large content in separate LFS storage. It requires client filters, authentication, server availability, and backup of those external objects. Adding LFS now does not automatically remove old large blobs from history.

A submodule keeps a separate repository and pins a commit through a gitlink; nested authentication, fetches, and coordinated changes remain operational concerns. A subtree copies another project's content into a directory, allowing ordinary checkout of that content but making synchronization an explicit integration process. Neither mechanism removes dependency ownership or supply-chain review needs.

On case-insensitive filesystems, names such as `Config.yaml` and `config.yaml` can collide. Use an intermediate rename when necessary and verify behavior on the deployment filesystem. Git tracks an executable bit for regular files, not arbitrary Unix permissions, ownership, or ACLs. `core.fileMode` controls detection of executable-bit changes; `git update-index --chmod=+x script.sh` can record an intentional mode change.

### Plumbing commands worth explaining

```bash
git cat-file -t HEAD
git cat-file -p HEAD
git hash-object path/to/file
git ls-tree -r HEAD
git rev-list --count HEAD
git for-each-ref --format='%(refname) %(objectname)' refs/heads/
git update-ref refs/heads/rescue KNOWN_COMMIT EXPECTED_OLD_COMMIT
git fsck --full
```

`cat-file` inspects stored objects; `hash-object` calculates an object ID and writes an object only with `-w`; `ls-tree` reads tree entries; `rev-list` walks commits. `update-ref` can compare the expected old value before changing a ref, avoiding a blind overwrite. `fsck` checks object integrity and connectivity; it reports problems but does not restore missing content.

## 5. Git Networking & Authentication

### SSH and HTTPS transport

SSH usually authenticates through a key offered by an agent or selected identity file. HTTPS commonly uses a personal access token (PAT), OAuth-backed helper, or short-lived platform credential. Both can provide encrypted transport; choose based on identity management, network constraints, and credential lifecycle.

An SSH deploy key is normally associated with repository access and may be read-only or writable depending on the host. An SSH agent can keep unlocked keys available without putting private keys into every process, but agent forwarding extends access to the agent and needs deliberate boundaries. Host key verification authenticates the SSH server: verify fingerprints through a trusted channel before accepting a new key. `ssh-keyscan` alone does not prove server identity.

Credential helpers retrieve and store HTTPS credentials. OS-backed secure storage is preferable to plaintext files; Git's `store` helper writes credentials unencrypted. PAT scopes, expiration, repository grants, and organization SSO requirements affect access independently. Avoid tokens in URLs, command history, or logs. See [Git credential handling](https://git-scm.com/docs/gitcredentials).

### Protocol v2 and fetch negotiation

During fetch, client and server exchange capabilities and information about wanted and already-held commits. The server can then send the needed reachable objects rather than the complete repository. Protocol v2 separates commands such as `ls-refs` and `fetch`, allowing more selective reference discovery. It is not an authentication mechanism and does not eliminate expensive pack generation.

Shallow fetch records ancestry boundaries; partial fetch can omit selected object classes. Diagnose incomplete history separately from an authorization problem. Protocol details and supported capabilities are described in [Git protocol v2](https://git-scm.com/docs/gitprotocol-v2).

### Remote URLs and refspec mappings

```bash
git remote -v
git remote get-url --push origin
git config --get-all remote.origin.fetch
git fetch origin refs/heads/release:refs/remotes/origin/release
git push origin HEAD:refs/heads/fix/probe
```

A refspec maps source to destination. A typical fetch mapping is `+refs/heads/*:refs/remotes/origin/*`; the leading `+` permits updating those local tracking refs even after a remote rewrite. It is not permission to force-update the server. An empty push source, such as `:refs/heads/old`, requests deletion and deserves the same care as other destructive ref changes.

### Authentication, authorization, and signing

Authentication establishes an identity; authorization decides whether that identity may read, write, or update a protected ref. A working SSH handshake can coexist with a rejected push. “Repository not found” may deliberately hide a private repository from an unauthorized identity. Check the selected account, remote URL, token scopes, repository grants, and branch rules before changing credentials indiscriminately.

GPG and SSH signing are independent of transport authentication. Configure a trusted signing identity, then use `git commit -S` or `git tag -s`. Verify with `git verify-commit COMMIT` or `git verify-tag TAG`. SSH verification needs a trusted allowed-signers configuration; GPG verification needs an appropriate trusted key policy. A cryptographically valid signature from an unknown or unauthorized key is insufficient approval.

### Focused diagnostics

```bash
GIT_SSH_COMMAND='ssh -v -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519' git ls-remote origin
GIT_TRACE=1 git ls-remote origin
GIT_CURL_VERBOSE=1 git ls-remote origin
```

The first command helps identify which SSH key is selected; the others reveal subprocess and HTTP/TLS behavior. Traces can contain sensitive URLs, headers, and infrastructure details despite built-in redaction. Capture them in restricted logs and redact before sharing. Fix the trusted CA chain, hostname, proxy, clock, or credential configuration rather than disabling TLS or SSH host verification.

## 6. Git Security & Supply Chain

### Secrets: prevent, rotate, then clean history

Keep secrets in a secret manager or an appropriately encrypted delivery mechanism. Ignore rules, local secret scanning, and push protection reduce accidental exposure, but each can have gaps. Enforce suitable server-side checks and limit what CI credentials can read or modify.

When production credentials reach a repository, revoke or rotate them first and investigate their use. Removing the file in a new commit leaves the previous blob accessible. `git filter-repo` and BFG Repo-Cleaner can rewrite affected history, but rewrites change IDs and require coordination across branches, tags, clones, forks, and caches. Preserve incident evidence under restricted access; do not create extra broadly accessible copies of the secret.

After an authorized cleanup, coordinate remote updates and fresh clones or careful cleanup of existing clones. Otherwise an old clone can reintroduce the secret. Hosting caches and pull-request references may require provider-specific removal; rewrite tools cannot revoke an already copied credential. See the [GitHub sensitive-data removal procedure](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository).

### Hooks, branch rules, and repository permissions

`pre-commit` can check staged content, `commit-msg` can validate a message, and `pre-push` can check proposed updates. Client hooks are local and bypassable; they are not normally installed by cloning a repository. Server hooks such as `pre-receive` and `update` can reject changes when the hosting platform supports them.

Use branch protection or rulesets for required reviews, checks, and restricted updates. CODEOWNERS routes ownership review; enforcement requires the corresponding review rule. Review ownership rules, CI definitions, and policy files themselves. Apply least privilege to human accounts, service identities, deploy tokens, and automation, with separate read and write credentials where appropriate.

### Untrusted repositories and `safe.directory`

Repository content, build scripts, configured filters, and installed hooks can execute code in the right context. Keep Git patched, inspect untrusted changes before executing them, and isolate untrusted builds from secrets. A repository-owned setup script that installs hooks is still code requiring review.

`safe.directory` controls which differently owned repositories Git will treat as trusted for relevant repository configuration and operations. If ownership is unexpected, investigate it. When shared ownership is intentional, configure the exact trusted path from an appropriate protected configuration scope. A wildcard trust exception weakens the ownership boundary; it is not a repair for malicious content. See [`safe.directory` configuration](https://git-scm.com/docs/git-config#Documentation/git-config.txt-safedirectory).

### Pinning, signed releases, and provenance

Pin CI actions and Git dependencies to reviewed full commit IDs where supported, and pin deployed images by digest. Version tags can move unless policy prevents it. A pin fixes identity; it does not prove safety, and transitive downloads or build tools can still introduce mutable inputs.

Signed release tags associate a release object with an accepted signing identity. Build provenance connects an artifact digest to source, builder identity, and build process. Verify those claims against trusted issuers and admission policy. Keep dependency lockfiles, controlled builders, artifact signing, and audit records aligned so the path from reviewed change to running workload is inspectable.

## 7. Git in CI/CD & GitOps

### Desired state, reconciliation, drift, and rollback

In GitOps, reviewed Git content defines desired configuration. Argo CD or Flux retrieves a revision, renders manifests, compares them with live state, and reconciles differences according to policy. Git does not contain all runtime truth: database state, generated credentials, controller defaults, and observed health live elsewhere.

Drift is a difference between desired and observed state. Automatic correction depends on controller configuration, ownership, and ignored fields. Argo CD's automated synchronization, pruning, and self-healing are separate controls. A direct cluster edit may be overwritten when reconciliation enforces Git state. Prefer a reviewed corrective commit; for emergency manual intervention, explicitly coordinate suspension and later resume. See [Argo CD sync policy](https://argo-cd.readthedocs.io/en/stable/user-guide/auto_sync/) and [Flux Kustomization controls](https://fluxcd.io/flux/components/kustomize/kustomizations/).

A Git revert provides a new desired revision that can restore prior manifests. It cannot reverse irreversible schema migrations or recover deleted application data. Verify rendered configuration, reconciliation status, workload health, and user-facing service indicators after rollback.

### Reproducible deployment identity

A commit ID identifies Git source; an image digest identifies the built image. Record both, along with the deployment configuration revision. A mutable branch or tag can resolve differently between pipeline steps, and rebuilding the same source can produce a different artifact if dependencies or builders change. Resolve inputs once, build once, and promote the same verified digest across environments.

### CI checkout and event behavior

GitHub Actions checkout fetches a single commit by default; `fetch-depth: 0` requests all history for branches and tags. Pull-request jobs can check out a synthetic merge commit rather than the contributor's head, depending on the event and configuration. Choose the ref deliberately and never run untrusted pull-request code with privileged credentials. See the [checkout action documentation](https://github.com/actions/checkout).

GitLab Runner can reuse a checkout or clone anew according to its Git strategy; shallow history is controlled through `GIT_DEPTH`, with `0` disabling the depth limit. Jobs commonly run with detached HEAD. Do not assume a writable local branch or the presence of every release tag. See [GitLab runner Git settings](https://docs.gitlab.com/ci/runners/configure_runners/).

Triggers are platform-specific. Define intended branch, tag, pull-request, and path behavior explicitly, including what required checks do when workflows are filtered out. A documentation-only path filter can be useful, but shared build logic, lockfiles, generated inputs, and cross-service dependencies must still trigger affected builds.

### Concurrent pipelines, version bumps, and releases

Suppose pipeline A builds an older commit slowly, while pipeline B builds a newer commit quickly. B deploys first; A later writes its older image into the environment configuration. Both pipelines may have succeeded even though production regressed. A normal non-fast-forward rejection protects Git history but does not stop a bot from fetching, retrying, and committing stale intent on the new tip.

Serialize promotion per environment, and recheck eligibility against the current desired revision before writing. Use expected-old-value checks for Git updates and discard superseded work according to an explicit release policy. Cancellation alone is insufficient if an old job already started an external deployment. Record the source revision and artifact digest at every promotion.

Version-bump bots should use restricted identities, avoid self-triggering loops, and handle concurrent updates through fresh validation. Create a release tag on the commit actually built and verified. Publish immutable artifacts, and use atomic ref updates where supported when a release requires multiple Git refs to move together; this is not a transaction across registries and clusters.

### Monorepos and promotion boundaries

Selective builds start with an explicit comparison base, such as the pull request's merge base or the previous deployed revision. Include transitive dependents and shared tooling. Use filename-safe change enumeration such as `git diff --name-only -z BASE HEAD` when automating. If the base is missing or dependency impact is uncertain, fail clearly or use a defined broader-build fallback instead of silently skipping work.

| Promotion layout | Strength | Cost or failure mode |
| --- | --- | --- |
| Branch per environment | Separate ref history and protection | Merges and cherry-picks can introduce unrelated changes |
| Directory per environment | Environment updates reviewable together | Repository access is broad; write rules need enforcement |
| Repository per environment | Separate permissions and ownership | More automation and coordination across repositories |

Promote explicit artifact digests or narrowly scoped configuration changes. Choose the boundary around access control and ownership rather than assuming the layout itself provides isolation.

## 8. Reliability, Recovery & Troubleshooting

### Lost commits and repository corruption

Stop operations that could expire or overwrite evidence and preserve the repository, including local metadata and files. Then search surviving local reflogs:

```bash
git reflog --all --date=iso -50
git show CANDIDATE_COMMIT
git branch rescue/lost-work CANDIDATE_COMMIT
git log --oneline rescue/lost-work -10
```

If reflogs do not help, `git fsck --full --no-reflogs --unreachable` can identify objects still present outside ordinary ref reachability. Inspect candidate commits with `show` or `cat-file`, then name the correct tip. A dangling blob may contain staged file content without its original filename. Unstaged content never stored in Git cannot be recovered through reflog.

For suspected corruption, run `git fsck --full` on a preserved copy and distinguish missing/corrupt objects from normal dangling objects. Obtain affected objects from a verified independent clone or backup. A fresh clone often restores server-backed history, but preserve local-only commits and staged content first. Investigate disk, filesystem, memory, or storage failures before trusting the repaired repository.

### Server outages, mirrors, and backups

Local full clones allow offline work during an outage, but server availability still affects collaboration, CI, dependency fetches, and GitOps source retrieval. Controllers may retain previously fetched data; verify actual behavior rather than assuming every new reconciliation can proceed without Git.

```bash
git clone --mirror https://git.example.com/platform/service.git service.git
git -C service.git fsck --full
git -C service.git bundle create ../service.bundle --all
git bundle verify service.bundle
```

A mirror copies the refs exposed by the source, but ongoing mirroring can propagate deletions and malicious rewrites. Keep independent versioned, access-controlled backups. Bundles preserve included Git history, not LFS payloads, platform settings, issues, CI secrets, or unreachable local work. Back up those separately and protect credentials according to the organization's secret-handling policy.

Define recovery point and recovery time objectives, verify restoration into an isolated service, and test authentication, branch rules, webhooks, LFS, submodules, and a representative CI/GitOps fetch. An up-to-date mirror is useful redundancy; a successful restore drill is evidence of recovery capability.

### Non-fast-forward and interrupted operations

```bash
git fetch origin
git log --left-right --graph --oneline HEAD...origin/main
git status
```

Check whether you are pushing to the intended ref and whether remote history was rewritten. For normal divergence, integrate after review and push normally. For a protected-branch rejection, use the required review path; force will not fix an authorization rule.

Use `git status` to identify the in-progress operation. Resolve files, stage them, and run the matching `git merge --continue`, `git rebase --continue`, or `git cherry-pick --continue`. To abandon, use the matching `--abort`. Preserve valuable conflict-resolution edits first; aborting may discard them, and a merge started with dirty files may not reconstruct every pre-merge change. `--skip` drops a replayed change, so use it only after understanding why that commit is no longer needed.

### SSH, TLS, proxies, credentials, and slow repositories

For SSH failures, inspect the remote hostname, offered key, agent contents, host fingerprint, and account mapping. For HTTPS failures, distinguish DNS/connectivity, proxy authentication, certificate trust, token validity, SSO authorization, and repository permissions. Check system time and CA configuration. Repeated retries cannot repair a wrong identity or a revoked credential.

For performance, measure the slow phase before changing storage. Inspect `git count-objects -vH`, bounded command timing, and restricted `GIT_TRACE2_PERF` logs. Check large tracked binaries, untracked build directories, antivirus/network-filesystem overhead, LFS downloads, server CPU, and pack fragmentation. Apply sparse checkout, partial clones, ignore rules, or maintenance to the measured bottleneck.

### `.git/index.lock` and concurrent operations

Git uses lock files while atomically updating mutable state. An existing index lock can mean an active Git command, an editor waiting for a commit message, or a crashed process. Locate the actual worktree-specific path with `git rev-parse --git-path index.lock`; inspect local processes and CI activity before removing anything.

Remove only a confirmed stale lock after the writer has stopped. Deleting an active lock permits concurrent writers and can lose staged updates. Separate concurrent jobs into independent checkouts or worktrees, and remember that worktrees still share some repository state. Recheck status and the staged diff afterward.

### GitOps conflicts and configuration drift

Identify the controller, resource owner, selected revision, rendered manifests, and current error. Separate source-fetch failure from render failure, API validation failure, field-ownership conflict, and application health failure. Git convergence alone does not prove service recovery.

If two reconcilers manage the same field, choose a single owner or explicitly partition ownership. If automation commits stale desired state, fix promotion ordering. If emergency changes must persist temporarily, coordinate reconciliation controls and record the eventual Git change before resuming. Excluding a field from drift detection is a policy decision, not a universal fix for conflicting writers.

## 9. Expert Interview Scenarios

### 1. What happens internally during commit, fetch, merge, and push?

**Commit:** staged paths reference blob objects, usually written when content was added. Git writes or reuses trees for the index snapshot, creates a commit with tree/parent/metadata, and updates the current branch or detached HEAD with applicable reflog updates. Hooks and signing can affect or reject the process. Working-tree edits absent from the index are excluded.

**Fetch:** the client discovers relevant refs, negotiates common history and capabilities, receives missing objects, verifies/stores them, and updates configured local refs plus fetch metadata. It does not merge into the current branch.

**Merge:** Git resolves tips and merge bases. It may fast-forward; otherwise it combines trees, records unresolved paths in the index if necessary, and creates a commit with multiple parents after resolution. A textual success still requires behavioral validation.

**Push:** the client proposes ref updates and sends required objects. The server checks object connectivity, authorization, hooks, and ref-update rules, then accepts or rejects updates and reports results. Without a supported atomic request, multiple ref updates can have mixed outcomes.

### 2. Recover after an accidental `git reset --hard`

Preserve the current repository and stop cleanup. Read the HEAD reflog, inspect the tip before the reset, and create `rescue/before-reset` at that ID. Compare it with the current branch and recover through a branch switch, cherry-pick, or a carefully chosen local reset after preserving remaining work. Reflog can recover committed work still present; it does not guarantee restoration of overwritten unstaged edits.

### 3. Why does rebasing change commit hashes?

A commit hashes its parent IDs as well as its tree, metadata, and message. Replaying onto another base changes the parent, producing a new object ID. Descendants must refer to the new IDs and therefore change too. The patch can remain equivalent while the commit identity and signatures differ; verify the result rather than equating similar messages with identical commits.

### 4. Undo a production deployment commit safely

Identify the configuration revision and artifact actually deployed, then create a revert on a fresh branch from current shared history. For a merge, inspect parent order before selecting `-m`. Review the inverse diff, validate manifests and rollback compatibility, merge through the normal emergency policy, and observe reconciliation plus service health. Keep the audit trail and address schema or data changes through their own recovery plan.

### 5. Design a secure, auditable GitOps workflow

Require reviewed configuration changes and protected policy files. Build reviewed source in an isolated, minimally privileged builder; publish an immutable image digest with verifiable provenance. Promote that digest through reviewed environment changes. Give the reconciler read access to Git and narrowly scoped cluster permissions, keep plaintext secrets outside Git, and record source/configuration revisions, approvals, and the deployed digest. Test both rollback and recovery of the Git hosting service.

### 6. Production credentials were committed

Revoke or rotate the credential immediately, contain exposed automation, and investigate access logs. Preserve restricted incident evidence, identify affected refs and copies, then coordinate any necessary history rewrite and provider cache cleanup. Refresh clones, remove leaked CI artifacts, and verify the old credential no longer works. Add scanning and reduce credential lifetime and scope; a history rewrite alone does not contain the incident.

### 7. Optimize a multi-gigabyte monorepo

Determine whether the cost is transfer, pack generation, checkout, working-tree scans, or history traversal. Combine blob-filtered partial clones with sparse checkout for suitable clients; use LFS for appropriate binary assets and plan any migration separately. Maintain server packs, graph/index accelerators, and healthy storage. Make CI change detection dependency-aware and supply enough history. Compare measured timings and resource use before and after; do not hide missing-history failures to claim faster builds.

### 8. How do concurrent GitOps pipelines cause deployment races?

An older build can finish after a newer one and submit a valid new commit pointing production backward. Git sees a fast-forward even though application intent is stale. Serialize writes per environment, validate the candidate against current promotion policy immediately before updating, and retain expected-old-value checks for concurrent changes. Record and verify the digest observed by the controller. Cancelling a job does not undo side effects it already made.

### 9. Design repository backup and disaster recovery

Start with business recovery objectives and enumerate Git refs/objects, LFS content, submodule sources, hosting configuration, identity integration, and CI/GitOps dependencies. Keep encrypted, immutable or versioned backups under credentials separate from routine writers. Detect failed backup jobs and verify object integrity. Regularly restore into an isolated host, recover policies and integrations, and prove a representative deployment can fetch the intended revision and artifact within the recovery target.

### 10. CI cannot find a tag or commit in a shallow clone

Inspect the selected ref, `git rev-parse --is-shallow-repository`, `git show-ref --tags`, and the configured fetch refspec. Distinguish a missing tag ref from missing ancestry or an unavailable server object. Fetch the required named branch/tag explicitly and deepen history as needed:

```bash
git fetch origin refs/tags/v1.2.3:refs/tags/v1.2.3
git fetch --deepen=100 origin
git rev-parse --verify 'refs/tags/v1.2.3^{commit}'
```

For jobs needing complete history, use the provider's full-history checkout configuration or `git fetch --unshallow origin` when currently shallow and the source has full history. A tag fetch alone does not supply every merge base, and full depth does not necessarily broaden a restrictive branch refspec. Servers may refuse arbitrary object-ID fetches or may already have pruned the object; fetch an advertised containing ref or restore from a trusted archive. Rerun the exact failed comparison or release command to establish recovery.
