# Findings: documentation lens (step 4d)

Returned as text by the sub-agent and saved here, re-wrapped only, by the
management session, because the harness does not let sub-agents write
report files. The management session spot-checked D-1, D-2 and D-4
against the worktree before saving.

Revision judged: worktree HEAD 7f4efb1 (= 3e84907 plus plan-only commits;
`git diff 3e84907 HEAD --stat` touches only docs/plans/).

Read: the phase-04 plan (survey 1-3, decisions 1/5/7/8, the 4d brief),
the audit README.md, scope-files.txt and verification.md; PUSH-AUDIT.md's
Documentation section and its three shared blocks; docs/usage.md (whole),
docs/library-api.md (whole), docs/testing.md (whole), docs/collection.md
(the parts touching create()), README.md, ARCHITECTURE.md, AGENTS.md; to
check the docs against the code: cluster.py (the owned-key constants,
K3S_RELEASE_FLOOR, DEFAULT_NODE_SIZE, validate_node_sizes,
validate_k3s_config, read_k3s_config, check_k3s_release,
_k3s_config_commands, install_control_plane, install_k3s_component,
Cluster.create, show), exceptions.py (the whole class list), __init__.py
(the create options), tools/ci_deploy_test.sh (whole); the master plan
(whole), phase plans 01-04 and docs/plans/index.md; `gh issue list`
(read only).

Note: the constants are named K3S_SERVER_OWNED_KEYS and
K3S_AGENT_OWNED_KEYS, not PLUGIN_OWNED_* as the brief said; the brief's
names do not exist.

## Findings

### D-1. docs/usage.md calls a question "still being settled" that the master plan has answered

docs/usage.md:172-177. Rating: **fix**.

Doc side (:173-177): "such a deployment may want `node-taint: []` on the
servers or must add tolerations. Which of the two is right is still being
settled against a live cluster; see `docs/plans/PLAN-node-customisation.md`."

Plan side (master plan, design 7): "**Answered in phase 3 planning, from
the chart source: it does, as written.** It labels only the control plane
`openstack-control-plane=enabled`, and every chart it deploys ships its
control-plane toleration with `enabled: false`, so every OpenStack control
service would sit Pending." The taint and its opt-out were also observed
live in phase 3 (merge tier run 37246848512).

This is the user-facing statement of the default that most changes where
workloads land, and it sends a reader to a plan to explain current
behaviour. Proposed fix: state the outcome plainly -- for control services
labelled onto the control plane, use `node-taint: []` (or enable each
chart's toleration) -- and drop the "still being settled" sentence and the
plan link. Word it as "what the charts do", since phase 3 survey finding 3
reached it from chart source at an older openstack-helm commit
(99d96acba), not from a live prototype run.

### D-2. docs/library-api.md and exceptions.py say seven classes carry a `reason`; there are eight

docs/library-api.md:304; exceptions.py:41,46. Rating: **fix** (one number
in each place).

Origin: not this plan's code. ClusterMetadataError arrived in #107's
2c90813 after the count was written; the files are in scope, so it is
reported here.

Doc side: "The seven classes that carry a `reason` share a base..." and
"Seven of the classes below describe several distinct failures". The
docstring's own history adds up to seven and says "the count in this
docstring is worth keeping accurate rather than approximate".

Code side: `grep '^class .*(_ReasonedK3sException)'` returns eight:
ClusterInterruptedError, ManifestError, GuestFileError,
ClusterMetadataError, K3sConfigError, UnsupportedReleaseError,
ReleaseLookupError, KubeconfigError. Proposed fix: say eight, or drop the
number from the prose.

### D-3. docs/library-api.md says an unreadable `sshkey` propagates unwrapped, but its own table says it raises SshKeyError

docs/library-api.md:251-255 against :285. Rating: **document**.

Origin: not this plan's code (SshKeyError dates from 5126209); the file is
in scope, so it is reported here.

Doc side (:253-255): "`OSError`/`yaml.YAMLError` from local file and
subprocess work (an unreadable `sshkey` path, a `~/.kube/config` write, a
malformed kubeconfig), propagate unchanged rather than being wrapped."

Table side (:285): "`SshKeyError` | `create(sshkey=...)` is given a path
that cannot be read or decoded as UTF-8". Code side: create() catches
`(OSError, UnicodeDecodeError)` around `open(sshkey)` and raises
`SshKeyError.unreadable`. Proposed fix: remove "an unreadable `sshkey`
path" from the parenthetical. The "`~/.kube/config` write" in the same
parenthetical may also conflict with the KubeconfigError row at :294; not
verified.

### D-4. The `--agent-config` row says "the same keys" are refused for agents; the agent list is a subset

docs/usage.md:47. Rating: **document**.

Doc side: "The same keys are refused for the agent role as listed below."
The table at :119-128 lists only six agent keys (data-dir, node-name,
with-node-id, server, token, token-file). Code side: K3S_AGENT_OWNED_KEYS
has exactly those six; K3S_SERVER_OWNED_KEYS has eleven, adding
cluster-init, https-listen-port, tls-san, write-kubeconfig and
write-kubeconfig-mode. The table is right and the row sentence is wrong.
Suggested wording: "A smaller set of keys is refused for the agent role;
see 'k3s configuration', below."

### D-5. The "Behaviour changes" list in usage.md restates three facts already on the page

docs/usage.md:179-190. Rating: **consider**.

servicelb disabled with MetalLB is stated at :100-102 and :108-111, then
again at :182. The control plane taint with its zero-worker exception is
at :132-140, then again at :183-184. The release floor is at :144-149,
then again at :189-190. Only the Longhorn replica-count note (:185-188)
is stated nowhere else. The block works as upgrade notes, and says these
apply to every new cluster whether or not the options are used. The cost
is three places to edit on any change. This lens would leave it, or cut
each item to a pointer.

### D-6. Deferred work the master plan's Future work does not record

Master plan Future work; phase-03 plan decision 2 and survey finding 3.
Rating: **document** (for step 4g).

(a) Collection follow-on. Survey finding 3 is confirmed: no issue exists
(`gh issue list --state all` searched for sizing, server-config,
collection options and taint returns only #89, #91, #96). A new point for
the issue text: docs/collection.md:82-88's worked playbook ("Ensure a CI
runner cluster exists, with workers left to the conductor") uses
`initial_workers: 0`, which the module passes straight to
`create(worker_count=...)` (collection/plugins/modules/sf_k3s_cluster.py:490-491).
A zero-worker create is never tainted, and usage.md:137-140 says it
"stays untainted after `expand-workers` adds workers". So the
collection's documented CI-runner shape builds a control plane that runs
workloads -- the exact situation design 7 added the taint to prevent --
and the module cannot set `node-taint` or a size to compensate. It is the
same consumer as 33fl's CI runner plan. Put this in the follow-on issue,
and consider a note in docs/collection.md until the options land.

(b) Prototype notes. Confirmed as survey finding 3 says: phase 3 survey
finding 3 records the notes (private homelab-deployments-lfs) as wrong
on servicelb and on Longhorn replicas, "reported to the operator rather
than edited". The master plan has no bullet; its six Future work bullets
do not name the prototype. Decision 8 already assigns this a bullet and
no issue.

(c) Not previously named: the zero-worker taint exception has no live
coverage. Phase 3 decision 2: "the zero-worker exception is no longer
exercised on any cluster. That is acceptable: ... fully unit tested".
That reasoning appears only in the phase 3 plan -- not in Future work,
docs/testing.md or an issue. It was a deliberate decline, so `consider`;
the tests lens judges whether it still holds. The point here is that a
declined coverage gap should be findable from the master plan.

### D-7. docs/testing.md does not say what the merge tier does not check

docs/testing.md:63-76. Rating: **consider**.

Accuracy is fine (see the clean list). The paragraph lists what the
script asserts and is silent on what it does not. Not covered end to end:
the release-floor refusal, the owned-key refusal, the three files and
their order on the node, and the zero-worker exception. Evidence:
ci_deploy_test.sh never reads `/etc/rancher/k3s/config.yaml*` on a node,
has no create that is expected to fail, and `--worker-count 0` appears
nowhere (the minimal cluster uses 1, script line 520). Proposed fix: one
sentence saying these are covered by unit tests only. Lower priority than
D-1 to D-4.

### D-8. ARCHITECTURE.md "Cluster assembly" still holds but omits the new step

ARCHITECTURE.md, "Cluster assembly" bullet. Rating: **consider**.

It still holds. But every node now gets its k3s config files written
before its installer runs (install_control_plane() calls
`_k3s_config_commands()` at cluster.py:1555; install_k3s_component() at
:1646-1649), and sizes and both config mappings are recorded in metadata.
PUSH-AUDIT asks about "the cluster assembly flow", and this added one
step. At most one clause plus a link to docs/usage.md "k3s
configuration"; do not restate the three-file layout there (one canonical
home per fact). No module or command was added, so this is not required.

### D-9. Master plan Success criteria: all true at 3e84907, but one needs a recorded reading

Master plan, Success criteria. Rating: **document** (for step 4g).

- tox, flake8 and pre-commit pass per verification.md (565 passed);
  `tox -eflake8` was vacuous, but a direct flake8 over the scope passed.
- Python >= 3.7 holds by reading only: no walrus, removesuffix or match
  statement in cluster.py, exceptions.py or __init__.py (cluster.py:552
  notes why it uses rstrip). #82 says nothing tests the floor.
- Unit tests for new behaviour: true.
- 120 columns, single quotes, no `'''`: true.
- Live validation: true (run 37246848512, #108).
- "ARCHITECTURE.md, README.md, and AGENTS.md have been updated if the
  change adds or modifies modules or CLI commands" needs a reading. None
  of the three changed in any of the three merges. The change added
  eleven options to `k3s create` but no module or command. That is
  defensible, but "modifies CLI commands" is arguably met, so 4g should
  state which reading it took instead of ticking the box. The README
  correctly did not grow; D-8 is the only candidate change.

## Existing issues

- **#58 (Consistency: Plan phase references).** Its body lists
  docs/collection.md:21 and docs/library-api.md:100, :126, :172, :264.
  `git grep -n -i 'phase [0-9]' -- README.md docs ':!docs/plans'` returns
  nothing now, so those references are already fixed and the body is a
  stale snapshot, not an open rediscovery; the phase-4 definition of done
  on this is met today. Not matched by that grep: docs/library-api.md:175
  and :316 cite `PLAN-library-api-and-collection-phase-03-missing-verbs.md`
  by filename (hyphenated) -- library-plan history that predates this
  plan. Code docstrings (cluster.py:646, __init__.py:242, etc.) cite
  "phase 2/3 plan" decisions; they are outside the README/docs grep and
  belong to the code lens.
- **#99 (Links out of docs/ are absolute).** Its only finding is in
  docs/plans/PLAN-cumulative-health-signals.md. No relative links out of
  docs/ exist in README or the non-plan docs. Not re-raised.
- #101, #102 and #97 are in phase 3's Out list and were not touched by
  this lens.
- No existing issue covers D-6 (a), (b) or (c).

## Checked and clean

- **Release floor v1.21.1+k3s1.** K3S_RELEASE_FLOOR is (1, 21, 1)
  (cluster.py:274). usage.md:144 and :189, library-api.md:288, the
  UnsupportedReleaseError docstring and the master plan all agree; no page
  states a different floor. "expand-workers does not check" is true:
  check_k3s_release is called only at cluster.py:2049. usage.md omits the
  "unparseable release" refusal that library-api.md:288 states; too minor
  to rate.
- **Refused-keys list.** The usage.md:119-128 table matches both
  constants exactly (eleven server keys, six agent keys). The `tls-san+`
  exception and the `+` rule match. Master plan design 3 ("five, plus six
  more") matches. The only defect is the D-4 wording.
- **Three files and their order.** usage.md:91-102 matches
  `_k3s_config_commands()` (cluster.py:1485-1503): config.yaml, then
  50-sf-client-k3s.yaml only when the mapping is non-empty, then
  90-sf-client-k3s-enforced.yaml with `disable+: [servicelb]` on servers
  only and only with MetalLB. Workers get a single comment line in
  config.yaml. "Written before the installer first runs" is true for both
  install paths. The server config.yaml contents (kubeconfig mode,
  floating-address SAN, cluster-init on the first server only, taint)
  match.
- **Taint and zero-worker exception.** usage.md:132-143 matches the
  `md.get('worker_nodes')` test, the in-code comment and master plan
  design 7.
- **Sizing.** Defaults 2/2048/50 match DEFAULT_NODE_SIZE. The 4096 MB
  floor and the 709 MB / 30 s figures match create()'s docstring, the CLI
  help and master plan open question 3. Positive-integer-only validation
  matches validate_node_sizes and `IntRange(min=1)`. `expand-workers`
  using the recorded size matches `_node_size()`.
- **`show` and `expand-workers` sections of usage.md.** They match
  Cluster.show() (fills for old clusters, nothing written back) and the
  expand_workers behaviour.
- **library-api.md against Cluster.create().** The method row, the
  remaining-keyword list, the six sizing args and defaults, and the
  server_config/agent_config description match the signature at
  cluster.py:1853-1864. read_k3s_config(path, role) matches its
  definition.
- **Exceptions table against exceptions.py.** All 19 public classes have
  a row, with no extras. The K3sConfigError row matches its six
  classmethods. NodeSizeError and UnsupportedReleaseError match their
  docstrings. The only slips are D-2 and D-3.
- **docs/testing.md against ci_deploy_test.sh.** Every claimed assertion
  is in the script: sizes in metadata and in Shaken Fist on a control
  plane node, a worker and the `expand-workers` worker; no Traefik
  HelmChart or pods, and no `svclb-` pods; ci-role server x1 and agent x2,
  then agent x3 after expand; the NoSchedule taint on every control plane
  node; a minimal cluster with one worker and `node-taint: []`, asserting
  no taints; the same cluster as positive control for Traefik and
  `svclb-traefik-`. Nothing is overstated; omissions are D-7.
- **README.md.** Unchanged by the three merges, still a pitch. No
  readme-discipline issue.
- **AGENTS.md.** No convention changed. The quoting rule it cites is
  still at the top of cluster.py. The three test classes it names exist
  (ShellQuotingTestCase, HeredocDelimiterTestCase,
  FileEncodingIsStatedTestCase). `read_k3s_config` states
  `encoding='utf-8'` and catches UnicodeDecodeError, as AGENTS requires.
  Neither AGENTS.md nor ARCHITECTURE.md grew (llm-doc-discipline: no
  growth finding).
- **`phase <number>` outside docs/plans.** None. The remaining "phase"
  hits in docs and the architecture files are the numbered create-progress
  phases, which plan-phase-references permits.
- **Execution table, docs/plans/index.md and the phase plans.** They
  agree. Master plan: phases 1-3 Complete with Merged cells ddb1f3b
  (#92), b791364 (#98), 51ff6f4 (#108); phase 4 In progress with an empty
  Merged cell. Index: the master plan In progress, linking all four phase
  plans. The three merge commits exist, are merges naming #92, #98 and
  #108, and have the expected first parents. The phase plans carry no
  status or merge field, so there is nothing to disagree with.
- **Deferred work in the phase plans.** Phase 1 and 2 Out items
  (expand-workers overrides, collection, Longhorn replicas) are in Future
  work. Phase 3's Out items are #101, #102 and #97, all existing issues.
  The only gaps are D-6 (a)-(c).
- **"Bad file, refused key or old release reported before anything is
  built or the name registered."** True: the CLI calls read_k3s_config
  before binding the namespace, validate_* run at the top of create(),
  and check_k3s_release precedes registering the name.

Not verified: usage.md:147-148's claim that Longhorn's chart refuses
channels v1.16-v1.20 and `testing` (taken from phase 2 survey finding 3,
chart kubeVersion >=1.21.0-0, not re-checked against the chart);
testing.md's "20-30 minutes"; whether KubeconfigError wraps a plain
`~/.kube/config` write (the tail of D-3).

## Summary

Nine findings: 2 fix (D-1, D-2), 3 document (D-3, D-4, D-6), 1 document
for step 4g (D-9), 3 consider (D-5, D-7, D-8), 0 none. D-1 matters most.
D-2 and D-3 are stale facts in library-api.md and exceptions.py that came
from #107 rather than from this plan; both files are in scope. D-6
confirms survey finding 3, and adds that the collection's documented
`initial_workers: 0` playbook builds an untainted control plane, and that
the zero-worker exception's missing live coverage is recorded only in the
phase 3 plan. #58 is a stale snapshot (the grep is clean now); #99 is
unrelated to this scope. Everything else on the checklist came back
clean.
