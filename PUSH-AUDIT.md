Thanks for your work on this. I appreciate it. Some final checks
before I push:

## Code quality

 * Did the changes introduce any significant amount of duplicated
   code? Are there any missed opportunities for code reuse or
   refactoring?
 * Should any new code be extracted into a shared helper in
   `primitives.py` or `progress.py`? Look for logic that a second
   command or wait loop would likely need.
 * Are there any TODO comments we should address as part of this
   work?
 * Please ensure all source code is wrapped at 120 characters.
 * Use single quotes for strings, double quotes for docstrings,
   and never triple single quotes.
<!-- shared-block: comment-proportion v1 -->
Comment proportion (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/comment-proportion.md`):

- A comment or docstring earns its length by saying what the code
  cannot: the contract, the units, the failure modes, the reason a
  surprising choice is correct. Restating the code in prose is not
  documentation.
- Treat as candidates any added comment or docstring that is longer
  than the code it documents, and any comment block over roughly
  fifteen lines attached to a body under ten. These are candidates,
  not verdicts -- a subtle algorithm, a public API contract, or a
  hard-won bug explanation can justify the length.
- Where the length is not justified the finding is advisory, and
  the fix is to cut the restatement rather than delete the comment:
  keep the why, drop the line-by-line narration of the what.
- Prose that documents user-visible behaviour rather than the
  implementation usually belongs in `docs/`, with the comment
  reduced to a pointer.
<!-- shared-block-end -->

## Style conformance

<!-- shared-block: python-version-discipline v1 -->
Python version and typing (shared block; do not edit -- the
canonical copy lives in shakenfist/development at
`templates/shared-blocks/python-version-discipline.md`):

- No syntax or standard library API newer than the floor in
  `requires-python`. Structural pattern matching, `X | Y` unions in
  annotations evaluated at runtime, `tomllib`, and
  `datetime.UTC` each raise on an interpreter the package still
  claims to support, and none of them fail in CI when CI runs only
  the newest version. This is the finding to look for first: it is
  a real break on a real user's machine, not a style point.
- New and modified code carries type hints, and mypy is expected to
  be clean over it. A project part way through a staged rollout is
  held to the new code, not to the whole tree.
- Prefer the walrus operator and f-strings where they make the code
  read better, subject to the floor above.
- Raising the floor in `requires-python` is a supported-platforms
  decision, not a convenience: it drops users. If it is genuinely
  right, the platforms table, `requires-python` and
  `constraints.python` in `renovate.json` all move together.
<!-- shared-block-end -->

!!! note "In this project"

    The floor is `requires-python = ">=3.7"` in `pyproject.toml`.
    That is the oldest Python a Shaken Fist client is expected to
    run on, and it is why version lookups go through the
    `importlib-metadata` backport. There is no platforms table to
    move with it, and no `constraints.python` in `renovate.json`
    -- raising the floor is `pyproject.toml` and `AGENTS.md`.
    At 3.7 that rules out the walrus operator and `match`, so
    the block's preference for the walrus does not apply here
    until the floor moves.

 * Does the code follow the project conventions in `AGENTS.md`?
   Check in particular:
   - Click command conventions (commands attached to the `k3s`
     group, `--namespace` handling, state via `ctx.obj`).
   - Long running work reports through the `Progress` reporter
     (`ctx.obj['PROGRESS']` / `progress.get_progress(ctx)`), not
     bare prints.
 * The plugin must never break `sf-client` startup: top-level
   imports must remain cheap and reliable, and
   `python3 -c 'import shakenfist_client_k3s'` must succeed.
 * Cluster state belongs in namespace metadata
   (`orchestrated_k3s_cluster_*` keys), not local files.

## Tests

 * Is there unit test coverage for the changes? This should
   include normal and adversarial cases, especially around
   external API responses (the k3s update API, the GitHub
   releases API, agent operation payloads) which can change
   shape over time.
 * All tests should pass. We need to fix any failing tests now
   before we push. Run `tox -epy3`.
 * What tests are skipped? Could we reduce that number?
 * Run `tox -eflake8` (it diffs against HEAD~, so stage or
   commit first) and confirm clean output.
 * Run `pre-commit run --all-files` and confirm all hooks pass.
 * Does the change alter orchestration behaviour that unit tests
   cannot reach? If so, has it been exercised against a live
   Shaken Fist cluster (manually or via the functional CI)?

<!-- shared-block: functional-test-coverage v1 -->
Functional test coverage (shared block; do not edit -- the
canonical copy lives in shakenfist/development at
`templates/shared-blocks/functional-test-coverage.md`):

- The standard is "do we run the code to do the real thing, and
  does it work as intended". Every subcommand exposed on the command
  line, and every endpoint exposed by an API, should have a test
  that exercises it for real rather than against a mock of itself.
- For a change that adds or alters user-visible behaviour, the
  question to answer is which functional test would have failed
  before it and passes after. If there is none, that is the finding,
  and it is a finding about this change rather than a note for
  later.
- Unit tests are held to no coverage percentage, but a branch that
  is reachable from outside the process and has no test is worth
  naming. Error paths and argument validation are where this bites:
  they are the code most often written once and never run again.
- Mocking the system under test proves nothing. Mock the boundary --
  the network, the clock, the hypervisor -- and let the code being
  tested actually run.
- Where a gap is real but out of scope for the change in hand, say
  so plainly and record it, rather than silently widening the
  change or silently leaving it unsaid.
<!-- shared-block-end -->

!!! note "In this project"

    The functional tier is `tools/ci_deploy_test.sh`, run from the
    merge queue and by `workflow_dispatch`. It is the only place a
    `k3s` subcommand runs for real, so "which functional test would
    have failed before this change" is a question about that
    script. A new subcommand that it never invokes has no
    functional coverage, whatever its unit tests say.

## Documentation

 * Has `ARCHITECTURE.md` been updated if this change adds or
   modifies modules, commands, or the cluster assembly flow?
 * Has `AGENTS.md` been updated?

<!-- shared-block: llm-doc-discipline v1 -->
AGENTS.md and ARCHITECTURE.md discipline (shared block; do not
edit -- the canonical copy lives in shakenfist/development at
`templates/shared-blocks/llm-doc-discipline.md`):

- `AGENTS.md` is a working guide: the conventions, invariants and
  gotchas an agent cannot infer by reading the code, plus curated
  links into `docs/`. It is loaded into every session, so every
  line costs context on every task.
- `ARCHITECTURE.md` is a map: the component inventory, how data
  moves between components, and why the shape is the way it is.
  A deep dive on one subsystem belongs in `docs/`, where humans
  benefit from it too.
- One canonical home per fact. If `docs/` covers it, link to it
  instead of restating it -- and the same rule applies between
  `AGENTS.md` and `ARCHITECTURE.md`.
- Neither file is a reference manual, a runbook, or a changelog.
  CLI flags, configuration keys, wire protocols, step-by-step
  procedures and plan history go to `docs/`.
- Growth in either file is itself a finding: if the diff adds
  content that belongs in `docs/`, flag it as blocking and move
  it.
<!-- shared-block-end -->

<!-- shared-block: readme-discipline v1 -->
README discipline (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/readme-discipline.md`):

- New user-visible features are documented in `docs/` (and
  `ARCHITECTURE.md` / `AGENTS.md` where appropriate), not by
  adding bullets to `README.md`.
- `README.md` is a pitch: what the project is, who it is for,
  minimal installation instructions, a small number of usage
  examples, and curated absolute links into `docs/`. It only
  changes when the pitch, the install story, or the
  documentation links change.
- README growth is itself a finding: if the diff adds README
  content that belongs in `docs/`, flag it as blocking and
  move it.
<!-- shared-block-end -->

<!-- shared-block: plan-phase-references v1 -->
Plan phase references (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-phase-references.md`):

- Documentation outside plans directories describes the current
  state of the software, not the history of how it was built. Do
  not write "implemented in phase 5" or "since phase 3 of the
  two-tier CI plan": a reader wants to know whether a feature
  exists, not which phase of which plan delivered it.
- If a documented behaviour is implemented, describe it plainly.
  If it is planned but not yet implemented, link to the master
  plan in `docs/plans/` instead of citing a phase number.
- Reserve the word "phase" for plan documents. A procedural
  document describing a live multi-stage process (a release
  runbook, say) should call its stages "steps" or "stages", so
  that a phase reference in `docs/` is always a plan smell.
- The consistency audit greps `README.md` and `docs/` (excluding
  plans directories) for "phase <number>". Append
  `<!-- audit-ok: phase-reference -->` to a line only when the
  reference is genuinely not about an implementation plan.
<!-- shared-block-end -->

 * Is all deferred work and pre-existing errors listed in a plan
   file under `docs/plans/`?
 * If this work added a plan, is it registered in
   `docs/plans/index.md` with a status from the shared vocabulary?

## Security review

 * Review these changes as both a security reviewer and an
   experienced developer and correct any errors you find.
 * Are any user- or upstream-controlled values (cluster names,
   release channel data from the k3s update API, tag names from
   the GitHub releases API, namespace metadata) interpolated into
   agent command lines, file paths, or YAML written to guests
   without sanitization? Agent execute commands run through a
   shell on the guest.
 * Do any changes leak secrets (node tokens, kubeconfigs, SSH
   keys) into logs, progress output, or commit history?

<!-- shared-block: path-traversal-review v1 -->
Path construction from outside data (shared block; do not edit --
the canonical copy lives in shakenfist/development at
`templates/shared-blocks/path-traversal-review.md`):

- Treat as a candidate any filesystem path built from a value the
  process did not choose: a request parameter, an image name, tag or
  digest, a layer path, an archive member name, a filename out of a
  configuration file or a database row.
- The question is not whether the value looks dangerous but whether
  the resulting path is *proved* to stay inside its intended base
  directory. Resolve the joined path with `os.path.realpath()` and
  verify it still starts with the base; a check on the untrusted
  component alone is defeated by symlinks and by encodings the
  check did not anticipate.
- Prefer a helper that cannot be forgotten at a call site --
  `safe_path_join()` in occystrap, or the framework's own
  (`send_from_directory` in Flask) -- over an inline guard repeated
  at each join.
- Archive extraction is the case most often missed: a member name
  inside a tarball or zip is attacker-controlled in exactly the same
  way as a request parameter.
- Where a bare join is correct because every component is
  process-chosen, say so in a comment rather than leaving the
  reader to re-derive it.
<!-- shared-block-end -->

!!! note "In this project"

    The paths built here are local: the kubeconfig merge writes
    under `~/.kube/`, and the temporary files the create path uses
    come from `tempfile`. The untrusted components to watch are the
    cluster name and the namespace, both of which reach filenames
    and kubectl context names.

## Build verification

 * Does `pip install -e .` succeed in a fresh venv alongside
   `shakenfist-client`?
 * Does `tox` pass?
 * Has `shakenfist_client_k3s/_version.py` stayed out of the
   commit? It is generated and gitignored.
