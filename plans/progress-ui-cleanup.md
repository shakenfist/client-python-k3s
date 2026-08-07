# Plan: startup progress tracking UI cleanup

## Problem

`sf-client k3s create` emits hundreds of near-identical lines while waiting
for instances to boot and agent operations to complete. Specific issues,
identified from a real `k3s create` transcript:

1. The wait loops (`await_boot`, `await_idle`, `await_fetch`) reprint their
   header and per-instance status every poll iteration, even when nothing has
   changed.
2. There is no phase numbering and no elapsed time, so the user cannot tell
   whether a long wait (for example the metallb `kubectl wait`, which can
   legitimately block for five minutes) is normal or a hang.
3. `await_boot()` silently runs an OS update at the end, so the "waiting to be
   idle" block after boot is actually an unlabelled `apt-get dist-upgrade`.
   Agent operation waits report a bare count rather than what is running.
4. `install_workers()` is called twice in `k3s_create` (once before and once
   after the kubecfg fetch), duplicating a whole wait block for no benefit.
5. Minor polish: "1 instances", inconsistent capitalisation.

## Approach

Add a small dependency-free progress reporter module,
`shakenfist_client_k3s/progress.py`, and convert all polling loops and phase
announcements to use it.

### `progress.py`

- `format_elapsed(seconds)` — humanised elapsed time ("45s", "2m05s",
  "1h03m").
- `count_str(count, noun)` — "1 instance" / "2 instances".
- `class Progress`:
  - Constructed with an optional `total_phases` and a `verbose` flag; put in
    the click context as `ctx.obj['PROGRESS']`. `get_progress(ctx)` returns
    it, lazily creating an unnumbered one for code paths which did not set
    one up.
  - `phase(name)` prints a `[n/total] Name` header once and starts a
    per-phase elapsed timer.
  - `note(msg)` prints a one-off indented informational line.
  - `update(key, status)` reports the current status of one item (an
    instance, a fetch operation) inside a wait loop:
    - Interactive mode (stdout is a TTY and not verbose): renders one status
      line per item, updated in place using ANSI cursor movement, including
      per-phase elapsed time. Lines are truncated to the terminal width so
      cursor arithmetic stays correct.
    - Line mode (non-TTY, CI, pipes, or `--verbose`): prints only when an
      item's status *changes*, plus a heartbeat reprint per item every 60
      seconds so logs still show liveness.
  - `wait_done()` ends a wait block (resets per-item state; in interactive
    mode the final statuses remain on screen as history).
  - `finish(msg)` prints a completion line with total elapsed time.

### `primitives.py`

- `await_boot`, `await_idle`, `await_fetch` use `Progress.update()` instead
  of printing every iteration.
- `await_idle` describes what it is waiting on: a new `_describe_agent_op()`
  returns the currently-executing command (truncated) from the operation's
  `commands`/`results`, so statuses read
  `running 'apt-get dist-upgrade -y' (1 operation remaining)`.
- Move the implicit `instance_os_update()` call out of `await_boot()` into
  `create_and_await_instances()`, labelled with a note ("updating base OS
  packages"), so the post-boot wait is no longer mislabelled.
- Phase announcements move into the primitives which own them
  (`create_and_await_instances`, `install_control_plane`,
  `install_extra_control_plane`, `install_workers`, `setup_metallb`,
  `setup_longhorn`), so every command using them gets consistent output.

### `__init__.py`

- `k3s create` constructs a `Progress` with a computed phase total (base 8:
  control plane nodes, worker nodes, install control plane, install workers,
  fetch credentials, metallb, longhorn, local kubeconfig; +1 when creating a
  node network; +1 when there is more than one control plane node) and
  finishes with `Cluster <name> is ready (<elapsed> total)`.
- Fix the duplicate `install_workers()` call (the second call, after the
  kubecfg fetch, is removed).
- `expand-workers`, `expand-addresses` and `update-os` also get `Progress`
  instances with small phase totals. `delete` already only emits debug
  output and is left alone.

### Tests

New `shakenfist_client_k3s/tests/test_progress.py` in the existing
testtools/stestr style covering: elapsed formatting, pluralisation, phase
numbering, change-only printing and the 60s heartbeat in line mode,
interactive-mode in-place rendering (ANSI escapes), and
`_describe_agent_op()` (execute ops mid-sequence, non-execute ops,
truncation). A test also verifies `await_boot()` no longer triggers the OS
update itself.

### Documentation

Update AGENTS.md (key files table) and ARCHITECTURE.md (module structure and
a short progress-reporting section). There is no `docs/` directory in this
repository and this change does not alter the README pitch, install story or
CLI arguments, so no further documentation changes.

## Steps

1. Write `progress.py` with tests.
2. Convert `primitives.py`, fixing the hidden OS-update labelling.
3. Convert `__init__.py`, fixing the duplicate `install_workers()` call.
4. Update AGENTS.md and ARCHITECTURE.md.
5. Run `tox -epy3` and `pre-commit run --all-files`.
6. Propose a commit for review.

## Addendum: failure visibility

A live `k3s create` run against a real cluster exposed a failure mode the
first pass did not cover: the metallb `helm upgrade` agent operation entered
the `error` state (the in-guest agent rejected the `KUBECONFIG=... helm`
commandline because it validates the first token as an executable), and the
create sat on `running 'KUBECONFIG=...helm upgrade...'` for a quarter of an
hour because:

- `await_idle()` treated any operation with `state != 'complete'` as still
  in flight, and an errored operation never completes; and
- the elapsed time shown was the per-phase timer, so there was no signal
  distinguishing one slow command from a wedged one.

Follow-up changes, on the same branch:

1. **Fail fast on errored operations.** `await_idle()`, `await_fetch()` and
   `reap_execute()` abort via a shared `_abort_agent_op_error()` which
   reports the instance, operation uuid, the command it was on, any recorded
   results, and points at `sf-client instance events` for the agent-side
   error. `await_idle()` snapshots operations already in the error state
   before it starts waiting and ignores them, so a historical failure can
   neither wedge nor incorrectly abort a later wait.
2. **Per-status elapsed times.** `Progress.update()` now shows how long each
   item has had its *current* status (resetting on change) rather than the
   phase elapsed time, so a stalled command reads as a growing number.
3. **Stall notes.** If a single agent command runs for more than
   `STALL_WARNING_SECONDS` (five minutes), `await_idle()` emits a one-off
   note naming the operation uuid and the events command to inspect it.
4. **Avoid the trigger.** The metallb and longhorn helm commandlines use
   `helm --kubeconfig ...` instead of the environment-variable prefix the
   agent rejects.
