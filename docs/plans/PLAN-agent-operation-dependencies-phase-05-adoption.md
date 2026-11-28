# Agent operation dependencies phase 5: the first example application

## Prompt

This is phase 5 of a cross-repository plan whose plan of record is
`docs/plans/PLAN-agent-operation-dependencies.md` in
`shakenfist/shakenfist`. The plan file lives here because the code
does, following the precedent of phase 3, whose plan file lives in
`shakenfist/client-python`.

Before working on this phase, read the model page the plan shipped in
phase 4,
[developer_guide/agent_operations](https://shakenfist.com/developer_guide/agent_operations/),
and in this repository read `Cluster.execute_and_await()`,
`Cluster.await_idle()` and `Cluster.reap_execute()` in
`shakenfist_client_k3s/cluster.py`, every one of the eleven
`execute_and_await()` call sites, and
`exceptions.CommandFailedError`/`AgentOperationFailed` in
`shakenfist_client_k3s/exceptions.py`. Ground answers in what the code
does rather than in what the master plan's phase row predicted it
does; that row was written before phase 4 surveyed this repository and
this plan corrects it in several places.

## Planning effort

**High**, and the master plan does not specify a review effort for
this phase.

High rather than medium because this phase turns on agent operation
wait loops and on a compatibility decision about a published plugin,
which `PLAN-TEMPLATE.md`'s "In this project" note on planning effort
names explicitly: these are the places where a mistake only shows up
against a live Shaken Fist cluster. The implementation is small -- one
function gains a dependency chain -- but deciding what the chain
hangs off, and what happens when the cluster on the other end cannot
accept it, is not.

## Scope

### In scope

- Fate sharing inside `Cluster.execute_and_await()`: each command a
  call submits to an instance declares a `depends_on` edge on the
  previous command submitted to **that same instance**, so the rest of
  that node's sequence is abandoned rather than run when the operation
  before it comes to nothing. Finding 11 says exactly what "comes to
  nothing" covers, which is narrower than this plan first assumed: the
  earlier operation erroring, expiring, being aborted or being deleted,
  and *not* its command exiting non-zero.
- A capability gate, so a cluster which cannot accept dependency
  edges keeps today's behaviour rather than losing the ability to run
  a command at all.
- Raising this repository's `shakenfist_client` floor to `0.8.5`, the
  release which carries phase 3's surface, in `pyproject.toml`.
  `collection/requirements.txt` carries no floor on the client -- only
  on this plugin, at line 33 -- so it is checked and left alone.
- Unit coverage for the chain, the per-instance keying, and the
  degraded path.
- One sentence in the plan of record's example index, in
  `shakenfist/shakenfist`, which phase 4 wrote as "It does not
  declare dependency edges today". That is a second pull request, in
  that repository, after this one has merged.

### Out of scope, and why

- **A rolling `update_os()`.** The master plan's worked example was a
  fleet-wide rolling update submitted as one chain. Survey finding 9
  confirms this repository has never rolled: `update_os()` calls
  `instance_os_update()` once with every node. Making it roll is a
  behaviour change to a released verb with its own design questions
  (how many nodes at a time, what a drain costs, what happens to a
  cluster left half-updated), and phase 4 already established it
  cannot be one chain at the default wait budget. It is not smuggled
  in behind a fate-sharing change.
- **`runs_after` edges.** Nothing here wants "run after this
  finished, whatever happened to it": every multi-command sequence in
  this repository wants the dependent abandoned when its dependency
  fails, which is `depends_on`.
- **Cross-instance edges.** Decision 2 forbids them, and survey
  finding 5 says why.
- **`settle_seconds`.** No command in this repository needs a delay
  after its dependency completes; see decision 6.
- **The per-link wait budget question.** Phase 4 recorded it as
  future work and said phase 5 is where the use case would be
  concrete. It is not: decision 2 keeps every edge inside one
  instance, and a same-instance edge never starts the wait budget at
  all. The question is left where phase 4 left it, with this phase's
  finding added to it.
- **`client-python`'s stale index row** (survey finding 10). It is a
  third repository and one line; flagged rather than swept.

## What the survey found

Eleven findings. Three correct the master plan's phase 5 row, one
corrects what the plan assumed about this repository's readiness, one
is a trap the obvious implementation falls into, and one blocked the
live proof as well as the code until the release it waited on landed.
The eleventh was found while the phase was being executed rather than
while it was being planned, and it contradicts the premise the phase
was built on; it is recorded here with the rest because that is where
a reader looks for what is true about the edge.

### 1. The gate was met while this phase was being planned

The master plan gates this phase on "a `client-python` release
carrying phase 3's surface and on the plugin's floor being raised to
it". The first half is now met and the second is this phase's own
work.

The survey originally recorded both halves as outstanding, because
when it ran, `client-python`'s newest tag in the local clone was
`v0.8.4` (2026-10-28), three weeks before phase 3's client half merged
as `a2c9d0d1f4`
([client-python#453](https://github.com/shakenfist/client-python/pull/453),
2026-11-18). That was a stale clone rather than a stale release.
`v0.8.5` is the release, and it was published while this plan was
being written:

- `v0.8.5` is commit `9fa1ff2449`, and `git merge-base --is-ancestor
  a2c9d0d1f4 v0.8.5` succeeds, so it carries phase 3's surface:
  `agent_dependency()` and `AGENT_DEPENDENCY_KEYS` are both in its
  `shakenfist_client/apiclient.py` at line 246 onwards.
- `shakenfist-client 0.8.5` is on PyPI, uploaded 2026-11-25T19:26:28
  UTC, which is the morning of 2026-11-26 in Melbourne.
- This repository still floors at `shakenfist_client >= 0.7.7`
  (`pyproject.toml:58`). `collection/requirements.txt:33` carries a
  floor on this plugin rather than on the client, so it is not part of
  the raise.

So nothing gates this phase. Step 5b raises the floor to `0.8.5`, and
the live proof in finding 8 becomes available with it.

### 2. The server side is already live, so nothing waits on a deploy

`sfcbr` advertises `agentoperation-dependencies` today, checked
against the live cluster on 2026-11-26 alongside
`agentoperation-deadlines` and `instance-execute`, all three `True`.
Phases 1 and 2 are deployed. Taken with finding 1, nothing outside
this repository is now outstanding.

### 3. The candidate is confirmed, and it is six call sites rather than one

The master plan's row names one: `execute_and_await()` submits its
second command whether or not the first succeeded. That is true --
`instance_os_update()` at `cluster.py:3450` passes `apt-get update`
and `apt-get dist-upgrade -y` -- and it is not the only one. Six of
the eleven `execute_and_await()` call sites pass a multi-command list
whose commands are strictly sequential:

| Site | What it submits |
|------|-----------------|
| `cluster.py:3451` `instance_os_update()` | `apt-get update`, `apt-get dist-upgrade -y` |
| `cluster.py:3698` first control plane install | installer commands then `sudo apt-get install -y helm` |
| `cluster.py:3735` node join | `_k3s_config_commands()`, `sudo apt-get update`, the `get.k3s.io` installer |
| `cluster.py:3925` metallb rollout | two `kubectl rollout status`, `mkdir -p /etc/sf`, the config write, `kubectl apply` |
| `cluster.py:3944` metallb install | `kubectl create ns`, `helm repo add`, `helm repo update`, `helm upgrade --install` |
| `cluster.py:3980` longhorn install | `helm repo add`, `helm repo update`, `kubectl create namespace`, `helm install`, `kubectl patch storageclass` |

The remaining five sites (`3839`, `5861`, `5873`, `5879`, `6057`) pass
a single command each and gain nothing. Three of those five -- the
cordon, drain and delete inside `remove_worker()` -- are a sequence,
but they are three separate calls rather than one list, so Python's
own control flow already fate shares them: an exception from the
cordon means the drain is never submitted. A chain in the one function
therefore reaches six paths rather than the one the master plan named.

This table deliberately no longer says what a failed early command
costs each site, because finding 11 found the answer is not what this
plan assumed and is not the same for every kind of failure.

### 4. The reporting half is already built, by phase 1's prerequisite

The master plan's phase 1 made "`client-python-k3s` has learned
`abort`" a prerequisite, met by
[client-python-k3s#164](https://github.com/shakenfist/client-python-k3s/pull/164)
in `v0.4.0`. That work went further than the prerequisite required:

- `cluster.py:138` already reads `AGENT_OP_FAILED_STATES = ('error',
  'expired', 'abort')`, so `await_idle()` already fails on an aborted
  operation rather than waiting it out.
- `exceptions.py:1447-1452` already carries a dedicated arm for
  `state == 'abort'` which says "the operation was aborted because
  something it depended on failed" instead of the "probably did not
  run" wording the other terminal states get.

So this phase owes only the submission half. Nothing about reporting
an aborted operation has to be written, and an implementing agent
should not rewrite it.

### 5. The obvious chain is wrong, because of the loop order

`execute_and_await()` (`cluster.py:2859`, as it stood before this
phase) was:

```python
aops = []
for cmd in cmds:
    for instance_uuid in instance_uuids:
        aops.append(self.client.instance_execute(instance_uuid, cmd))
```

The outer loop is over commands and the inner over instances, so
`aops` is ordered `cmd1@A, cmd1@B, cmd2@A, cmd2@B`. Chaining each
operation to its predecessor **in `aops` order** would make
`cmd1@B` depend on `cmd1@A`: a cross-instance edge, between two
operations which have nothing to do with each other, on every
multi-instance call. Two of the six sites in finding 3 are
multi-instance (`instance_os_update()` over every node, and the node
join at `3735`), so this is not a corner case.

It matters beyond tidiness. Phase 4's decision 4, corrected at source
during that phase's execution, established that a same-instance edge
never spends the dependency wait budget -- `_dependencies_met()` is
called only on the operation `agent_operation_next()` returns, and a
dependent queued behind its own dependency on one instance is not
evaluated until that dependency is terminal and retired, by which time
its edge is satisfied. A cross-instance edge **does** spend the
budget. So the naive chain would not merely be meaningless, it would
put every multi-instance call on a clock it does not need to be on,
and serialise a fan-out which is deliberately concurrent.

### 6. The failure which gets reported is already the root cause

`reap_execute()` is called in `aops` order (`cluster.py:2918`),
which by finding 5 is every instance's `cmd1` before any instance's
`cmd2`. So when `cmd1@A` fails and aborts `cmd2@A`, the exception
raised is `cmd1@A`'s -- the real non-zero exit code with its command
line and output -- and the abort is never reached. The existing
ordering already favours the root cause over the cascade, and this
phase does not have to change `reap_execute()` to get a good error
message. Note that `await_idle()` raises on the first failure it sees
while polling, which may be the abort rather than its cause; see
decision 7.

### 7. A chain against an older cluster raises, which would break the plugin

`_add_agentop_dependencies()` in `client-python`
(`apiclient.py:2240-2245` on `develop`) raises `IncapableException`
when the server does not advertise `agentoperation-dependencies`.
That is phase 3's deliberate contested decision, and its docstring
gives the reason: a dropped ordering edge means "run now", which is
the precise outcome the caller declared the edge to prevent, so
silence is worse than a refusal.

For a published plugin that cuts the other way. An unconditional
chain would mean `sf-client k3s create` raises `IncapableException`
and builds nothing at all against any cluster older than phases 1 and
2 -- a hard break of a released verb, to add a safety property. This
repository already has an idiom for a capability it cannot have
(`cluster.py:5372-5380`, where a server which cannot look a network up
within a namespace produces a written explanation and the delete
continues), and that idiom is to degrade, not to raise. Decision 4
follows it, and says why the reasoning phase 3 used does not transfer.

### 8. The functional tier runs the released client, which is now the right one

`tools/ci_deploy_test.sh:293` is `uv pip install shakenfist-client .`
-- the client comes from PyPI, at whatever is released. So the
cluster-deploy tier cannot exercise unreleased client surface at all,
whatever this repository's floor says.

This is the mirror image of what phase 3 found in `shakenfist`, where
`shakenfist/actions`'s `setup-test-environment` checks `client-python`
out at its *default branch*, so the shakenfist half of that phase
could not merge until the client half had. Here the dependency is on
a *release* rather than a branch, which would have been the stronger
gate of the two had finding 1's release not already landed.

Because it has, this costs nothing and buys the live proof for free:
the existing cluster-deploy test drives `create()`, which reaches
`instance_os_update()`, the first control plane install and the node
join -- three of the six sites in finding 3 -- against a PyPI client
which is now `0.8.5`. No new functional test has to be written. See
decision 8.

It does mean the ordering is not optional. Until `0.8.5` is what CI
installs, which it is from the next run, a chained submission in this
repository would reach an older `instance_execute()` and raise
`TypeError` on an unexpected keyword argument. Step 5b's floor raise
is what makes that unreachable for anybody installing the plugin, and
it is in the same pull request as step 5a for that reason.

### 9. The master plan's worked example does not exist here

`update_os()` (`cluster.py:6118-6135`) calls
`instance_os_update(md['control_plane_nodes'] + md['worker_nodes'])`
at `:6134`,
one `execute_and_await()` over every node at once. There is no
rolling update in this repository to adopt edges into, exactly as
phase 4's survey reported. Confirmed rather than re-derived.

### 10. Two registration facts, neither of them an obligation

- `client-python`'s `docs/plans/index.md:21` still records phase 3 as
  `In progress`, while the plan of record records it `Complete` with
  both merge commits. A one-line fix in a third repository, flagged
  here and declined above.
- This repository has no `tools/check-plan-status.py`, and
  `shakenfist/development`'s `plan-index` audit exempts phase plans
  from the "every master plan is listed" rule explicitly ("Phase
  plans are exempt -- they are named after their master plan and
  tracked inside it"). So registering this file in
  `docs/plans/index.md` is a judgement rather than a requirement;
  decision 9 makes it.

### 11. A `depends_on` edge does not notice a non-zero exit code

Found during execution, by step 5c's agent reading the code it was
asked to describe, and confirmed against the server. It contradicts
what this plan, the master plan and this repository's own expectations
all assumed, so it is recorded here as a survey finding rather than
buried in an execution note.

A `depends_on` edge is satisfied by a dependency in
`AgentOperation.STATE_COMPLETE`
(`shakenfist/daemons/sidechannel/main.py:2333-2351`), and it abandons
its dependent only for the four states in `DEPENDENCY_FAILED_STATES`
at `:166`: `error`, `abort`, `expired` and `deleted`. A command's exit
code is not among the inputs. The sidechannel records a non-zero exit
as a result and sets no failure state
(`_handle_execute_reply()` at `:1005-1035`), and the operation then
reaches `STATE_COMPLETE` once its commands have run, whatever they
exited with (`:1684`).

So the canonical example this plan was built around does not hold. An
`apt-get update` which exits 1 leaves a `complete` operation, the edge
is satisfied, and the `dist-upgrade` behind it runs against the stale
index exactly as before. `reap_execute()` still reports it, after the
wait, as it always did; what does not happen is the later commands
being abandoned.

What the edge does catch is real, and it is the failure family this
plan's parent was written for: a side channel which wedged (`error`),
a deadline or progress timeout which fired (`expired`), a cascade from
further up a chain (`abort`), and a dependency someone deleted. Those
are the shapes behind the #3516 flake. They are simply not "the
command ran and failed".

The right fix is not to make a non-zero exit an `error`. This
repository deliberately writes `kubectl create namespace
longhorn-system || true` (`cluster.py:4033`) and
`systemctl show ... || true` (`:840`), so a non-zero exit is an
ordinary result for some commands, and `reap_execute()` and every
health probe read `return-code` off a `complete` operation. Expressing
"only if the command succeeded" needs a per-edge option on the server,
filed as
[shakenfist#5091](https://github.com/shakenfist/shakenfist/issues/5091)
rather than built here: this phase cannot paper over a
gap in the edge's vocabulary from the client side, because every
command in a call is submitted before any result exists.

Michael's decision, 2026-11-27, was to land the phase with every claim
corrected and file the gap. What phase 5 delivers is therefore
narrower than the master plan promised and still worth having, and
every statement of it in this plan, in the code, in this repository's
documentation and in the master plan's row has been rewritten to say
what the edge actually does.

Nothing else in the master plan's phase 5 row was wrong. The row's
central claim -- that the candidate is fate sharing rather than
ordering, one hop, inside the budget -- survives the survey, with
finding 11's correction to what "fate" covers, and findings 3 and 4
make it cheaper to build than it looked.

## Decisions

### 1. The phase builds fate sharing in `execute_and_await()`, and nothing else

The master plan left phase 5's first job as "decide what to build".
This is the answer: one change, in one function, which gives every
multi-command `execute_and_await()` call the property that the rest of
a node's sequence is abandoned when the operation before it never
completes.

It is chosen over the alternatives because it is the only candidate
which is a real improvement to shipped behaviour rather than a
demonstration built to be demonstrated. Six paths submit commands
which assume their predecessor did its job (finding 3), and when a
predecessor's operation never completes -- a wedged side channel, a
fired deadline, a cascade -- every command behind it runs anyway
against a node where nothing happened.

Finding 11 narrows what this buys, and the narrowing is worth stating
in the decision rather than only in the survey: a command which runs
and exits non-zero leaves a `complete` operation, which satisfies the
edge, so that case is unchanged. The decision stands because the cases
it does cover are the ones which waste a whole cluster build on a node
that was never going to work.

### 2. Edges are keyed per instance, never across instances

The chain is built as "the previous operation submitted to this
instance", which requires tracking a per-instance predecessor rather
than reading back through `aops`. Finding 5 is the reason: the loop
order makes `aops` order cross-instance, and a cross-instance edge
spends the dependency wait budget and serialises a fan-out which is
deliberately concurrent.

Concretely, the implementation keeps a `dict` keyed by instance UUID
holding that instance's most recently submitted operation, and each
submission declares `depends_on=[agent_dependency(previous)]` when
that key is present. The loop order itself is **not** changed:
inverting it to `for instance: for cmd:` would build the same chain
and read more naturally, but it would also change the order in which
`reap_execute()` walks the results, which finding 6 shows is load
bearing for which failure a caller is told about. Leaving the loops
alone keeps that property for free.

### 3. `depends_on`, not `runs_after`

Every sequence in finding 3 wants the dependent abandoned when its
dependency fails. `runs_after` would give ordering without fate
sharing, which is what the code already effectively has, since one
executor per instance already serialises a queue whether or not an
edge exists. `runs_after` would therefore add nothing at all here --
and saying that plainly is worth more than exercising a second edge
type for the sake of coverage.

### 4. A missing capability degrades with a note; it does not raise

`execute_and_await()` consults
`self.client.check_capability('agentoperation-dependencies')` once
per call, and submits an unchained sequence when the answer is false,
writing one line through the reporter to say the commands are not
fate shared and why.

This is the decision most likely to be argued with, because it
inverts phase 3's. That phase made the client raise on a missing
capability, on the argument that a dropped edge means "run now",
which is precisely what the caller declared the edge to prevent, and
that silence is worse than a refusal.

The argument does not transfer, for two reasons. First, "run now" is
not a regression here -- it is exactly what this plugin has always
done, and every released version of it. Degrading leaves a caller
with the behaviour they have today, where raising leaves them unable
to build a cluster at all. Second, phase 3's client is a library whose
caller declared the edge and can be told the cluster cannot honour it;
this plugin declares the edge on its caller's behalf, for a caller who
asked for a k3s cluster and never mentioned dependencies. Refusing to
build a cluster because an optimisation is unavailable is the wrong
answer to give that caller, and `cluster.py:5372-5380` is this
repository already having reached that conclusion once.

The note matters: a silent degrade would make "the rest of my
sequence ran on a node whose side channel had wedged" unexplainable.
One line naming the capability is what makes the difference visible
without making it fatal.

### 5. The floor rises to the release which carries the surface, and edges use `agent_dependency()`

`pyproject.toml:58` moves from `shakenfist_client >= 0.7.7` to
`shakenfist_client >= 0.8.5`, the release which contains
`a2c9d0d1f4`, and `collection/requirements.txt` is checked for the
same obligation and found not to carry one.
Edges are built with `apiclient.agent_dependency(op)` rather than by
assembling `{'op_uuid': op['uuid']}` by hand -- the constructor exists
for exactly this, phase 3's docstring says so, and a hand-built dict
would not be validated by
`_agentop_dependency_edges()` in the same way.

Note that the floor and the capability gate answer different
questions, and both are needed. The floor is about the *library*
being new enough to have the keyword argument; the gate is about the
*cluster* being new enough to accept what it sends. A new plugin
against a new client and an old cluster is a real combination.

### 6. No `settle_seconds` anywhere

A settle delay holds an edge unsatisfied for some seconds after the
dependency completes, for work whose effect lands after the command
returns. Nothing in finding 3 is of that shape: `apt-get update`
leaves its index on disk before it exits, `helm repo add` writes its
repository file, and the installer's service is what the *next*
command waits for by its own means. Adding a settle would be guessing
at a problem nobody has, and phase 2 lowered the published ceiling to
the wait budget precisely because an unsatisfiable settle is worse
than none.

### 7. `await_idle()` is left alone, and the first-failure asymmetry is documented

`await_idle()` raises on the first of its own operations it finds in
`AGENT_OP_FAILED_STATES`, which now includes `abort`. Polling order is
not submission order, so it may see `cmd2@A`'s abort before
`cmd1@A`'s error and raise about the abort -- about an operation whose
own command never ran.

This is not worth changing. `exceptions.py:1447-1452` already explains
an abort as "aborted because something it depended on failed", so the
message is not misleading, merely one hop from the cause; and the
alternative -- teaching `await_idle()` to prefer a non-abort failure
among several -- is a wait loop change, which `PLAN-TEMPLATE.md` warns
is where mistakes only show up against a live cluster, in service of a
better error message in a case that already has a comprehensible one.
Step 5c writes the asymmetry down in the docstring instead, so the
next reader does not think it is a bug.

### 8. No new functional test; the existing cluster-deploy tier is the proof

Finding 8 establishes that the functional tier runs the released
client, which since `0.8.5` is one that carries the surface. So no new
functional test is needed: the existing
`tools/ci_deploy_test.sh` drives `create()`, which reaches
`instance_os_update()`, the first control plane install and the node
join -- three of the six chained sites -- on every cluster-deploy run.
A green run after the floor rises is the live proof that chained
submission works against a real cluster.

What that does **not** prove is the abort cascade, because CI's
commands succeed. Proving the cascade needs a deliberately failing
command in a real cluster, which is what
`shakenfist/shakenfist`'s own smoke tier already does -- phase 3
landed exactly that test (cascade to abort, 138s). It is proven there,
once, rather than again here.

### 9. This file is registered in `docs/plans/index.md`, in a new section

Finding 10 says registration is optional. It is done anyway, because
an unregistered plan file in `docs/plans/` is invisible to anyone
reading the index to find out what this repository has planned, and
because `client-python` already settled the same question the same
way: its index says "A plan appears below when this repository holds
the plan file, whether or not the plan of record is ours".

The master plans table here carries a `Phases` column that assumes
the master plan is local, which this one is not, so a second table is
added under a `## Phases of plans in other repositories` heading with
`Date | Plan | Intent | Status` columns -- `Date` then `Plan` first,
as the `plan-index` audit requires of every table.

### 10. The release is confirmed before step 5b, not assumed

Decision 10 originally recorded the release as a `plan-blockers`
blocker against this plan, because a plan which cannot be implemented
for an unknown period and says so only in its own text is a plan which
gets picked up and then abandoned. Finding 1 removed the blocker
before anything ran, and it has been resolved rather than left to
confuse a reader.

What survives is the check, not the blocker: step 5b confirms the tag
it is about to name actually contains `a2c9d0d1f4` before editing
`pyproject.toml`, and the Definition of done confirms the named
version resolves on PyPI. Naming a version PyPI does not have would
make the plugin uninstallable, which is a worse failure than the one
the blocker was guarding against.

## Step plan

Steps run in order and each is its own commit. Steps 5a and 5c are in
this repository; 5d is in `shakenfist/shakenfist` and is a second pull
request, after this one has merged.

Steps 5a and 5b belong in the same pull request and 5b must not be
dropped from it: a chain submitted against a floor of `0.7.7` would
raise `TypeError` on an unexpected keyword argument for anybody whose
installed client predates `0.8.5` (survey findings 1 and 8).

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 5a | high | opus | none | Chain `Cluster.execute_and_await()` (`shakenfist_client_k3s/cluster.py:2859`) so each command submitted to an instance declares a `depends_on` edge on the previous command submitted to **that same instance**. Keep the existing loop order (`for cmd: for instance_uuid:`) exactly as it is -- it is load bearing for which failure `reap_execute()` reports first, see the plan's survey finding 6 -- and track predecessors in a dict keyed by instance UUID rather than by reading back through `aops`, because `aops` order is cross-instance and a cross-instance edge spends the server's dependency wait budget (survey finding 5). Build each edge with `apiclient.agent_dependency(previous_op)`, imported from `shakenfist_client`, not as a hand-built dict. Gate the whole thing on `self.client.check_capability('agentoperation-dependencies')`, evaluated once per call, not once per submission: when it is false, submit exactly as today and write one line through `self.reporter` saying the commands are not fate shared because the cluster does not advertise the capability. Do **not** raise `IncapableException` -- the plan's decision 4 explains at length why this inverts `client-python`'s choice, and the existing idiom to follow is `cluster.py:5372-5380`. Do not touch `await_idle()`, `reap_execute()` or anything in `exceptions.py`: survey finding 4 establishes that `AGENT_OP_FAILED_STATES` at `cluster.py:138` already contains `abort` and that `exceptions.py:1447-1452` already explains an abort as a failed dependency, so the reporting half is done. Add unit coverage in `shakenfist_client_k3s/tests/` in the existing testtools/stestr style with the API mocked, asserting: a two-command two-instance call produces exactly two chains of length two and no edge between instances; a single-command call declares no edges at all; the capability being false produces no `depends_on` keyword in any call and one reporter line. Verifiable by unit tests alone; there is no cluster here. |
| 5b | low | haiku | none | Raise this repository's `shakenfist_client` floor to `0.8.5`, the release carrying `client-python` commit `a2c9d0d1f4`. Edit `pyproject.toml:58` (`"shakenfist_client >= 0.7.7",  # apache2`), keeping the trailing licence comment. Check `collection/requirements.txt` and `tools/mutation-check.py`'s fixture strings for a floor on the same package and raise any that names `shakenfist_client` rather than `shakenfist_client_k3s` -- line 33 of the requirements file and lines 1186-1187 of the mutation helper are `shakenfist_client_k3s` floors, which are this plugin's own version and must **not** be touched. The release is `v0.8.5`, so the new floor is `shakenfist_client >= 0.8.5`; confirm that rather than trusting it, with `git -C ../client-python fetch --tags origin` then `git -C ../client-python merge-base --is-ancestor a2c9d0d1f4 v0.8.5`, and stop if it does not succeed. Fetch first: the clone this plan was surveyed in was three weeks stale and the survey's first finding was wrong because of it. |
| 5c | medium | sonnet | none | Document what step 5a did, in two places. First, `Cluster.execute_and_await()`'s docstring -- which it does not currently have -- gains one: what the call does, that commands submitted to one instance are fate shared with `depends_on` while commands on different instances are independent, that the chain is per instance deliberately because a cross-instance edge would spend the server's dependency wait budget, and that the capability gate degrades rather than raises. It must also record the asymmetry the plan's decision 7 declines to fix: `await_idle()` raises on the first failed operation it sees while polling, which may be a cascaded `abort` rather than the error which caused it, while `reap_execute()` walks submission order and so reports the cause -- so the two can name different operations for the same incident, and that is understood rather than broken. Second, find where this repository's own documentation describes how commands are run on nodes (search `docs/` for `execute_and_await` and for agent operations) and add fate sharing there if there is a natural home; if there is not, say so in the step report rather than inventing a page. Do not restate any configuration default value -- the server's wait budget has a number and this repository is not where it is published. |
| 5d | medium | sonnet | none | **In `shakenfist/shakenfist`, as a second pull request after the first has merged.** Update the example index in `docs/developer_guide/agent_operations.md:348-368`. Its `client-python-k3s` bullet currently ends "It does not declare dependency edges today; the fate sharing above is what a chain would buy it", which stops being true when step 5a ships. Replace that clause with what the plugin now does: each command `Cluster.execute_and_await()` submits to a node declares a `depends_on` edge on the previous command submitted to that same node, so the rest of a node's sequence is abandoned when the operation before it errors, expires or is aborted; edges are never declared across nodes, so a fan-out stays concurrent and no dependency wait budget is spent; and a cluster which does not advertise the capability gets unchained submission with a note rather than a refusal. Name the released plugin version which carries it. **Do not write that a failed `apt-get update` stops the `dist-upgrade` behind it** -- survey finding 11 established that it does not, because a non-zero exit leaves a `complete` operation which satisfies the edge. This step also adds that fact to the page's own model section, which does not state it anywhere today and whose reader would otherwise assume the opposite, and links the issue filed for the per-edge option which would change it. Keep the bullet the length it is -- this is an index, not a second model page -- and do not restate the budget's value. Run `tox -e docs` as well as `pre-commit run --all-files`: the mkdocs build is a separate CI job and not a pre-commit hook, which phase 4 corrected the hard way. |

## Risks and mitigations

- **The naive chain ships anyway.** Survey finding 5's trap is
  invisible in a single-instance test, and four of the six sites in
  finding 3 are single-instance. *Mitigation:* step 5a's brief names
  the trap, and its required unit test is explicitly a **two-command
  two-instance** call asserting no edge between instances. The
  management session checks that assertion exists and fails when the
  keying is removed, not merely that the suite is green.
- **The capability gate is written per submission rather than per
  call.** `check_capability()` is cached in the client, so a per
  submission call would be correct but would read as though the answer
  could change mid-call. *Mitigation:* step 5a's brief says once per
  call; the management session reads the diff for it.
- **The floor names a version PyPI does not have.** `0.8.5` was
  published on 2026-11-25, and this plan's own survey got the release
  question wrong once already by reading a stale clone. A floor naming
  a version PyPI does not serve makes the plugin uninstallable.
  *Mitigation:* step 5b's brief requires a `git fetch --tags` and an
  `--is-ancestor` check before editing, and the Definition of done
  checks the named version resolves on PyPI.
- **The floor raise is dropped from the pull request.** Step 5a alone,
  merged and released, would raise `TypeError` for anybody on a client
  below `0.8.5`. *Mitigation:* the step plan says the two are one pull
  request, and the Definition of done checks the floor in the same
  list as the chain.
- **The degraded path is never exercised, so it rots.** Every cluster
  Michael runs has the capability (finding 2), so nothing will
  naturally take the other branch. *Mitigation:* it is a required
  unit test in step 5a, with the capability mocked false.
- **The two pull requests land out of order.** Step 5d describes
  behaviour that only exists once the plugin half has merged and
  released. *Mitigation:* 5d is explicitly a second pull request
  after the first, and names the released version, which does not
  exist until then.
- **This phase sits unimplemented and is forgotten.** It was planned
  against a gate which cleared the same morning, so there is nothing
  left to wait for and nothing to keep it on a blocked list.
  *Mitigation:* the `plan-blockers` entry which recorded the gate was
  resolved when finding 1 was corrected, so the plan reads as ready
  rather than as waiting.

## Definition of done

Each item is checkable, and the command is given where there is one.
An item which cannot be checked is reported as such, not ticked.

- A `client-python` release contains phase 3's surface. **Met at
  planning time** by `v0.8.5`; re-check rather than assume, because
  the clone this was first checked in was stale: `git -C
  ../client-python fetch --tags origin` then `git -C ../client-python
  merge-base --is-ancestor a2c9d0d1f4 v0.8.5` succeeds.
- `pyproject.toml` floors `shakenfist_client` at that release and no
  lower. Check: `grep -n 'shakenfist_client >=' pyproject.toml` names
  it.
- The floor resolves. Check: `pip index versions shakenfist-client`
  (or `pip download --no-deps shakenfist-client==<T>` into a scratch
  directory) shows the named version published.
- No floor on `shakenfist_client_k3s` was changed. Check: `git diff
  develop -- collection/requirements.txt tools/mutation-check.py`
  shows no change to a line naming `shakenfist_client_k3s`.
- `execute_and_await()` declares edges only within one instance.
  Check: the unit test asserting a two-command two-instance call
  produces two chains of length two and no cross-instance edge exists
  and passes, **and** fails when the per-instance keying is replaced
  by `aops[-1]`. This is the one assertion in the phase worth
  mutation testing, per the `mutation-check` skill; it is run, not
  promised.
- An unchained submission happens when the capability is absent, with
  a note. Check: the unit test with `check_capability` mocked false
  asserts no `depends_on` in any `instance_execute()` call and exactly
  one reporter line naming the capability.
- Nothing in `exceptions.py` or `AGENT_OP_FAILED_STATES` changed.
  Check: `git diff develop -- shakenfist_client_k3s/exceptions.py`
  is empty, and `grep -n "AGENT_OP_FAILED_STATES = " cluster.py`
  still reads `('error', 'expired', 'abort')`.
- `execute_and_await()` has a docstring which states the per-instance
  keying, the reason for it, the degrade-not-raise behaviour and the
  `await_idle()`/`reap_execute()` asymmetry. Not greppable as a whole;
  read by the management session against decision 7.
- No configuration default value is restated. Check: `git diff
  develop | grep -nE '300 seconds|\b300\b.*budget|86400'` finds
  nothing in an added line.
- The plan of record's example index no longer says the plugin
  declares no edges. Check, in `shakenfist/shakenfist`:
  `tr '\n' ' ' < docs/developer_guide/agent_operations.md | grep -c
  'does not declare *dependency edges'` reports `0`. The single-line
  `grep` this item used to name could never pass or fail honestly: the
  clause is wrapped across two lines at
  `docs/developer_guide/agent_operations.md:362`, so the grep finds
  nothing while the clause is still there, and reports the item met
  before step 5d has run. See execution note 6.
- This file is registered in `docs/plans/index.md`, in a table whose
  first two columns are `Date` and `Plan`, with a status from the
  shared vocabulary. Check: read, against
  `shakenfist/development`'s `docs/audits/plan-index.md`.
- `pre-commit run --all-files` passes in this repository, and in
  `shakenfist/shakenfist` for step 5d, where `tox -e docs` is also run
  and must end in `congratulations` with no line naming a file this
  phase changed.
- The cluster-deploy functional tier is green on the run which
  carries 5a and 5b. This is the live proof per decision 8, and it is
  **not** a pull request check: `.github/workflows/functional-tests.yml`
  gates that job on `merge_group || workflow_dispatch`, and a pull
  request runs the smoke tier only. So the proof is either a
  `workflow_dispatch` run on the phase branch before the pull request
  is queued, or the merge-queue run which lands it, and either way the
  run's result goes in the execution notes. Until one of those has
  happened the item is reported as unchecked rather than ticked. See
  execution notes 7 and 10: it was the merge-queue run, it failed on
  under-cloud capacity rather than on this change, and the item is
  **unmet**.
- Phase 4 reads `Complete` in the plan of record's Execution table
  with merge commit `fa6c8e1515`, and `shakenfist`'s
  `docs/plans/index.md` agrees. Check, in that repository:
  `python3 tools/check-plan-status.py`.

## Execution notes

Departures from the plan, recorded as they happened. Dates are AEDT.

1. **2026-11-26. Back brief answered.** All four questions put to
   Michael confirmed the plan as written: the floor goes to `0.8.5`; a
   cluster without the capability degrades with a note rather than
   raising (decision 4); no new functional test (decision 8); and the
   `await_idle()`/`reap_execute()` asymmetry is documented rather than
   fixed (decision 7). The plan's question about declining a rolling
   `update_os()` was not put to him, because phase 4's survey and the
   master plan row had already declined it twice and re-asking would
   have re-litigated a settled decision.

2. **Every line number in this plan was wrong, and the survey was
   read from a stale clone.** The survey was run in the main
   `client-python-k3s` checkout, which was sixteen commits behind
   `origin/develop`; the worktree is at `origin/develop`. Step 5a's
   agent found it immediately, because the function the brief placed
   at `cluster.py:2612` is at `:2859`. Every line number in the
   findings, decisions and step briefs has been corrected against the
   worktree, and the correction changed one substantive count: there
   are **eleven** `execute_and_await()` call sites, not nine. The two
   new ones are single-command, so finding 3's six multi-command sites
   are unchanged -- "six of nine" became "six of eleven". The new
   `kubectl cordon` call at `:5861` is `remove_worker()`'s, from
   [#172](https://github.com/shakenfist/client-python-k3s/pull/172).

   This is the second time in this phase that a stale clone has
   produced a wrong finding: the first was the release in finding 1.
   The lesson recorded for the next reader is to fetch before
   surveying a repository, including the one the worktree is cut from.

3. **Step 5a's note is emitted only when there is more than one
   command**, which the brief did not ask for and which is kept. With
   a single command there is nothing to fate share, so the line would
   be noise on every call against an older cluster, and there is a
   test for the silence. The Definition of done's "exactly one
   reporter line" is checked with a two-command call, so it is
   unaffected.

4. **The existing suite exercises the degraded path, not the chained
   one.** Step 5a tried teaching `fakes.FakeClusterClient` to
   advertise the capability so the lifecycle and smoke tests would
   chain, and reverted it: `agent_dependency()` validates a real
   UUID4 and the fake's operation handles are `aop-001`, which broke
   126 tests. `fakes.py` is therefore unchanged and those tests take
   the unchained branch. The chained branch is covered directly by
   the new test class, and end to end by the cluster-deploy tier per
   decision 8, so this is a gap in breadth rather than in coverage of
   the change. Recorded in Future work rather than fixed here,
   because giving the fake real UUIDs is a fake-wide change.

5. **2026-11-27. The phase's central premise was wrong, and the phase
   landed anyway with every claim rewritten.** Step 5c's agent, asked
   only to describe `execute_and_await()`, read the server and noticed
   that a `depends_on` edge is satisfied by a `complete` dependency
   whatever its command exited with. The management session confirmed
   it against `shakenfist/daemons/sidechannel/main.py` -- `:166`,
   `:1005-1035`, `:1684` and `:2333-2351` -- and it is now survey
   finding 11. Michael's decision was to land the phase with every
   claim corrected and to file the gap, which is
   [shakenfist#5091](https://github.com/shakenfist/shakenfist/issues/5091),
   as a per-edge opt-in rather than by making a non-zero exit an
   error. Corrected at source: finding 3's table, which no longer
   claims what a failed early command costs; decision 1's
   justification; the scope; decision 4's note; step 5d's brief, which
   had told its agent to write the false claim into the plan of
   record's example index; `execute_and_await()`'s docstring; and
   `docs/library-api.md`. Step 5a's commit message asserted it too and
   was rewritten before the branch left this machine.

   Three agents and one management session read this change before
   the premise was questioned. What caught it was an agent asked to
   *describe* the behaviour rather than to implement it, which is an
   argument for keeping a documentation step in a phase even when the
   code looks self-evident.

6. **2026-11-27. A definition-of-done check was broken, and would
   have passed before the step it gates had run.** The item for step
   5d greps the plan of record's example index for "does not declare
   dependency edges". That clause is there, at
   `docs/developer_guide/agent_operations.md:362`, but it is wrapped
   across two lines, so a single-line `grep` for it reports nothing
   found -- which this item reads as the clause having been removed.
   Run against the unmodified page it reports the item met. The check
   now flattens newlines before matching, and counts, so it fails
   while the clause is present and passes only when it is gone.

   This is the second time in this plan's phases that a definition-of-
   done grep has been wrong in exactly this way: phase 4's four-page
   check also looked for text a line wrap had split, and this session
   wrongly read phase 4 as incomplete because of it before checking
   the file. A grep over prose is a check of the grep as much as of
   the tree, and these plans should prefer one that cannot be fooled
   by a wrap.

7. **2026-11-27. The second broken definition-of-done item, found by
   the review rather than by running it.** The item for the live proof
   said the cluster-deploy functional tier is "green on the pull
   request carrying 5a and 5b". It cannot be:
   `.github/workflows/functional-tests.yml:218-220` gates that job on
   `merge_group || workflow_dispatch`, and a pull request runs the
   smoke tier only. So the item named a check which never runs where
   it said, and would have been reported unchecked forever. It now
   names the two runs which can actually carry the proof -- a
   `workflow_dispatch` run on the branch, or the merge-queue run which
   lands it -- and asks for the result here.

   Both this and execution note 6 are the same mistake in different
   clothes: a criterion written from what the author believed about the
   tooling rather than from the tooling. Three of this phase's
   fourteen criteria were wrong when first run.

8. **2026-11-27. Review round 1 on
   [#177](https://github.com/shakenfist/client-python-k3s/pull/177)**
   (1 fix, 2 document, 9 consider). The `fix` was that the
   cross-instance keying this plan calls the one assertion worth
   mutation testing had not been registered in
   `tools/mutation-check.py`, so the proof existed only in a session
   transcript. Three entries are now registered -- the per-instance
   keying, the capability gate and the degrade note's guard -- taking
   that harness from 178 mutations to 181, and all three are caught.
   Registering the first one found a second defect: the test id written
   from memory did not exist, which the harness would have reported as
   an error rather than as a pass.

   Three `consider` items were taken because they are defects in code
   this phase added: the degrade note was written even when no
   instances were passed and so nothing was submitted, which is now
   guarded and tested; `fakes.FakeClusterClient` says why it omits the
   capability and warns that a bare `MagicMock` client advertises
   everything and so opts a future test into chaining by accident; and
   `docs/library-api.md`'s new paragraphs had been inserted between a
   sentence and the one it says "likewise" about, and are moved below
   that discussion. The stale `0.7.7` floor in `README.md` and
   `ARCHITECTURE.md` was the `document` item, and
   `docs/plans/audit/findings-docs.md:392` had already recorded those
   two files as the places that floor is published, which is what made
   it findable. The floor's statement in two unexecuted plans
   (`PLAN-airgap-install.md:161` and
   `PLAN-cumulative-health-signals-phase-01-agent-signals.md:135`) is
   left alone: those are dated premises, and each plan's own survey
   re-verifies its premises before it runs.

9. **2026-11-27. Review round 2** (0 fix, 1 document, 8 consider, 2
   none), which is the exit condition, so the round was addressed and
   then the loop stopped rather than asking for a third.

   The round found a live instance of a trap round 1 had only
   documented, which is the useful thing in it.
   `tests/module_harness.py` builds a bare `mock.MagicMock()` client,
   and it *wraps* the mutating `Cluster` methods rather than replacing
   them, so a module scenario reaching `execute_and_await()` would
   take the chained branch -- a `MagicMock` advertises every capability
   -- and fail inside the real client's UUID validation on a handle the
   mock invented. Round 1 wrote a warning about this shape into
   `fakes.py`; the harness now sets `check_capability.return_value =
   False`, which is both the fix and what the harness did before edges
   existed. The lesson is the one the review skill states: when a
   finding names a shape, enumerate the shape's instances rather than
   annotating the one you were shown.

   Also taken: `docs/usage.md` now carries the behaviour for the CLI
   audience, including the note's exact text, because step 5c's agent
   put it in `docs/library-api.md` and a `sf-client k3s create` user
   reads neither that nor `ARCHITECTURE.md`; a test asserts the note is
   silent when the capability *is* present, which nothing checked; and
   `test_no_settle_is_asked_for` asserted
   `list(edge.keys()) == ['op_uuid']`, which couples this repository's
   suite to `agent_dependency()`'s exact output, and now asserts the
   property it is named for instead. The two new note properties are
   registered as mutations, taking the harness to 182.

   Declined, with the one worth recording: the reviewer asked twice
   whether `shakenfist-client` 0.8.5 still supports this package's
   declared Python floor. It does -- 0.8.5 publishes
   `requires_python ">=3.7"` -- which is checked on PyPI's JSON rather
   than asserted. The rest are observations about code which predates
   this phase, a measurement the parent plan already carries, and a
   fallback for the capability gate disagreeing with the server, where
   the server's refusal is the honest outcome.

10. **2026-11-27. The live proof did not go green, and the phase
    landed anyway.** Execution note 7 established that the
    cluster-deploy tier can only carry the proof on a
    `workflow_dispatch` run or on the merge-queue run which lands the
    change. It was the merge-queue run, and it
    [failed](https://github.com/shakenfist/client-python-k3s/actions/runs/45943905826):
    `Cluster deployment` was refused a worker node with
    `Insufficient Resources: No nodes remaining at scheduling stage
    sufficient_idle_disk` on the under-cloud. That was triaged on
    [#177](https://github.com/shakenfist/client-python-k3s/pull/177) as
    systemic rather than caused by the change -- it is the capacity
    family the parent cluster's own plans track, and nothing in it
    touches an agent operation -- and Michael merged on that triage.

    So the definition-of-done item is **not met**, and is recorded
    unmet rather than ticked. What the phase has instead is the unit
    coverage, the three registered mutations, and the smoke tier, which
    between them exercise the keying, the capability gate and the
    degrade note but not a real chain against a real agent. The first
    cluster-deploy run which passes on a `develop` containing
    `b233e21` is the proof, and it costs nothing to wait for: the tier
    runs on every merge into this repository. Reported in the phase's
    close-out rather than left for the parent plan's push audit to
    rediscover.

11. **2026-11-27, after the phase merged. The gap this phase filed was
    implemented within hours, outside the plan.**
    [shakenfist#5091](https://github.com/shakenfist/shakenfist/issues/5091),
    filed by execution note 5 as the per-edge opt-in which would make
    finding 11's case work, was closed by
    [shakenfist#5099](https://github.com/shakenfist/shakenfist/pull/5099)
    (merge `1e4cad5665`) through that repository's automated issue-fix
    workflow. A `depends_on` edge now takes an optional
    `require_success`, which makes the edge satisfied only if every
    result the dependency recorded reports a zero return code, and
    cascades the dependent to `abort` with a `dependency_failure` of
    `nonzero-exit` otherwise.

    This phase cannot adopt it yet, and the reason is not appetite:
    `apiclient.agent_dependency()` has no such keyword, and
    `AGENT_DEPENDENCY_KEYS` is `op_uuid` and `settle_seconds`, so the
    client raises on the key rather than sending it
    ([client-python#462](https://github.com/shakenfist/client-python/issues/462)).
    So the statement this plan's documentation and the plan of record
    both now make -- that `execute_and_await()` abandons a node's
    sequence for an operation which never completed and not for a
    command which ran and failed -- stays true, and stays true until
    that client gap closes and something here opts in. Michael's
    decision on 2026-11-28 was to record it and not to build it, so
    adoption moves to Future work below rather than reopening this
    phase.

## Future work

- **The per-link dependency wait budget.** Phase 4 recorded this and
  expected phase 5 to make the use case concrete. It does not:
  decision 2 keeps every edge inside one instance, and a same-instance
  edge never starts the budget. The question stays open, and this
  phase adds one data point to it -- the first real adopter wanted
  fate sharing within a node rather than ordering across a fleet, and
  so never met the budget at all.
- **A rolling `update_os()`.** Declined above with reasons. If it is
  ever built it is where the per-link budget question becomes
  concrete, and it will want its own plan.
- **Adopting `require_success`**, which is what would make finding
  11's case work. The server half exists: it was filed as
  [shakenfist#5091](https://github.com/shakenfist/shakenfist/issues/5091)
  and implemented hours later by
  [shakenfist#5099](https://github.com/shakenfist/shakenfist/pull/5099),
  as execution note 11 records. The client half does not --
  `apiclient.agent_dependency()` cannot declare the field and the edge
  normaliser refuses the key
  ([client-python#462](https://github.com/shakenfist/client-python/issues/462))
  -- so nothing here can opt in yet. Until it does,
  `execute_and_await()` abandons a node's sequence for an operation
  which never completed and not for a command which ran and failed,
  and both this repository's documentation and the plan of record say
  so.
- **`client-python`'s index row for phase 3** still reads
  `In progress` (survey finding 10). One line in a third repository.
- **The test fakes cannot carry a dependency edge.**
  `fakes.FakeClusterClient` hands out operation handles like
  `aop-001`, which `apiclient.agent_dependency()` rejects because it
  validates a real UUID4. So the lifecycle and smoke tests cannot
  exercise the chained path, as execution note 4 records. Giving the
  fake real UUIDs is a change across every test that reads a handle,
  and is worth doing on its own rather than inside this phase.

## Back brief

Before executing any step, back brief Michael on the plan as
understood, and get answers to these:

1. **The floor.** Finding 1 says `v0.8.5` carries phase 3's surface
   and is on PyPI, so step 5b floors `shakenfist_client` at `0.8.5`.
   That is a floor raise of two patch releases on a published plugin,
   and anybody pinned below it stops being able to install this one.
   Agree, or should the chain be made conditional on the installed
   client version so the floor can stay where it is?
2. **Decision 4, which is the contested one.** A cluster without the
   capability gets unchained submission and a note, where
   `client-python` raises in the same situation. Agree that a
   published plugin must keep building clusters against an older
   cluster -- or should it raise, and require the capability?
3. **Decision 1's scope.** Fate sharing in `execute_and_await()` and
   nothing else, with the rolling update declined. Agree, or is the
   rolling update what you actually wanted from this phase, in which
   case it needs its own plan and this phase becomes a prerequisite
   of it?
4. **Decision 8.** No new functional test; the existing cluster-deploy
   tier is the proof, and the abort cascade is proven once in
   `shakenfist`'s smoke tier rather than again here. Agree, or do you
   want a deliberately-failing-command test in this repository's
   functional tier?
5. **Decision 7.** `await_idle()` may raise about a cascaded `abort`
   rather than the error which caused it, and the plan documents that
   instead of fixing it. Agree, or is the asymmetry worth a wait-loop
   change?
