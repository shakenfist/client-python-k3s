# Lens 4d findings: documentation

Returned as text by the step 4d lens (sonnet, medium effort), as the
plan's decision 5 asks, and saved here by the management session. The
management session confirmed DOC-1 (`cluster.py:4228`), DOC-2
(`__init__.py:648`) and DOC-3 (`docs/usage.md:635`) against the
worktree before saving.

Judged at `bd9bead`. #121-only content (name and count validation,
kubeconfig delete, the `>=0.3.0` floor) was skipped.

Headline: the docs are accurate against the code, key by key. One real
discrepancy (DOC-1) and a few small `consider` items. No `fix`
findings.

## Findings

### DOC-1 (document): `health()` docstring still hedges on a behaviour phase 3 confirmed live

- File: `shakenfist_client_k3s/cluster.py:4228-4229`.
- Docstring: "...which `boot_id` detects; systemd **is understood to**
  clear ``k3s_restarts`` when an operator restarts the unit by hand,
  which only the lower-than-baseline half catches."
- `docs/library-api.md` (around line 480) states it as fact: "a stop
  followed by a start by hand resets it to 0."
- Phase 3's *Live results* observed it.
- Phase 3's Definition of done ticked "No page hedges about
  `NRestarts`", but its grep only covered `docs/library-api.md`. The
  docstring, the other prose definition, was missed, so the two now
  state the same fact differently.
- Proposed fix: a one-phrase edit stating the observed reset.

### DOC-2 (consider): `docs/usage.md` omits some of what the `health` renderer prints

- `docs/usage.md` (around lines 556-566) describes the `kubernetes:`
  line as `Ready since <time>` or `NotReady (False)` /
  `NotReady (Unknown)`, then each true pressure condition or
  `no pressure`.
- `_render_kubernetes` (`__init__.py:648`) also prints
  `readiness unknown` when `ready` is None on a registered node, and
  `<Condition> unknown` for a pressure status that is neither `True`
  nor `False`, in which case it never says `no pressure`.
- The `signals` description does not mention that an unread reading
  renders as `unknown`, or that a probe error is appended in
  parentheses.
- None of it is wrong; one sentence would close the gap.

### DOC-3 (consider): current-state docs carry release-history phrasing

- `docs/usage.md:635`: "changed in the release after v0.2.0, which
  ignored readiness (#76)". Vague, since the only tags are v0.1.0 and
  v0.2.0.
- `docs/library-api.md` (around lines 595-599): "Terms 4 and 5 were
  added by #76: before them, a cluster whose kubelets were all
  `NotReady` reported healthy...".
- `collection/plugins/modules/sf_k3s_cluster.py` (around lines
  392-394, module `RETURN`): "Before this release it did not consider
  Kubernetes at all...".
- These are migration notes with real value to callers whose
  `healthy` flips. They brush against "no changelog in docs", but they
  are not phase references.

### DOC-4 (consider): `docs/testing.md` is accurate but incomplete about the new merge tier step

- `docs/testing.md:81-85` matches `tools/ci_health_signals.py`
  (`KILL_COMMAND`, `STOP_COMMAND`, `check_health_exit_codes`).
- It omits the etcd snapshot step (`SNAPSHOT_COMMAND`,
  `check_snapshot_saved`), which is not damage, so this is defensible.
- It does not say the step runs last, just before the delete, or that
  it leaves the disk full. The tool's docstring says both.
- It does not say that `health --strict` at `ci_deploy_test.sh:423` and
  `:675` now also asserts Kubernetes readiness, since `healthy` gained
  the Ready term.

### DOC-5 (none, noted): facts stated in several places currently agree

- The Ready wait (24 attempts at 5 s, plus a 300 s `kubectl wait`,
  about 540 s worst case inside the 600 s agent deadline) agrees across
  `ARCHITECTURE.md`, `library-api.md`, `usage.md` and the
  `await_nodes_ready` docstring.
- The probe budget, the abandoned-operation count and the thirty
  seconds (`HEALTH_PROBE_TIMEOUT_SECONDS = 30`) agree across the
  `health()` docstring, `library-api.md` and `usage.md`.
- `health()`'s docstring restates much of `library-api.md` (baseline
  rules, None-versus-`[]`, the `healthy` terms), then says
  `docs/library-api.md` defines them in full. Phase 2's "defined in
  exactly one prose place" box is therefore not literally true.
- Small wording difference: the docstring says `k3s_state` is
  "lowercase letters and hyphens"; `library-api.md` adds "at most 32
  characters", matching `NODE_SIGNAL_STATE_RE = [a-z-]{1,32}`.
- Plan-decision citations in the docstring and comments are left to
  the code quality lens.

### DOC-6 (none): `library-api.md` is silent on the status of the new module-level functions

`node_signals_command`, `parse_node_signals`,
`parse_kubernetes_readings`, `NODE_SIGNAL_KEYS` and
`KUBERNETES_NODE_KEYS` are module-level and named in the `health()`
docstring. The page's "stable surface" paragraph implies everything it
does not name is unstable, so this is acceptable.

## Checklist

- **`library-api.md` `signals` keys against code:** nothing found. 12
  keys (`probed`, `error`, and the 10 in `NODE_SIGNAL_KEYS`), matching
  the docstring schema and the parsers in every specific checked.
- **`library-api.md` `kubernetes` keys against code:** nothing found.
  7 node keys (`KUBERNETES_NODE_KEYS`), matching `_kubernetes_from_probe`
  on `registered`, `oom_killed`, unnamed and duplicate-named nodes,
  `unmatched_nodes` and the top-level keys.
- **Top-level `healthy` terms:** nothing found. Five terms, identical
  in `library-api.md`, `usage.md`, the docstring and the module
  `RETURN`. Node-level `healthy` excludes `ready` in all of them.
- **`usage.md` `health` and `--strict` output and exit codes:** nothing
  found beyond DOC-2.
- **`usage.md` create and expand-workers Ready wait:** nothing found;
  the phase text, order and phase count (9) match the code.
- **`docs/testing.md` accuracy:** accurate; DOC-4 for omissions.
- **`ARCHITECTURE.md` stayed a map:** nothing found.
- **`AGENTS.md` should change:** nothing found; no convention changed.
- **`README.md`:** unchanged in scope, and not required to change.
- **`git grep -n -i 'phase [0-9]' -- README.md docs ':!docs/plans'`:**
  zero hits.
- **`diagram-discipline`, `readme-discipline`:** nothing found.
- **Deferred work:** survey finding 4 confirmed, not re-raised. One
  extra item for 4g's Future work: phase 3's *Deviations* says the
  `--kill-who=main` run "did not check that the containers survived".
- **Execution table, `index.md` and the four phase plans agree:**
  nothing found; both shortstat tables regenerate to the plan's
  numbers.
- **Definition of done boxes against the tree:** consistent except
  phase 3's `NRestarts` box (DOC-1) and phase 2's "one prose place" box
  (DOC-5).
