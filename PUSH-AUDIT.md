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

 * Does the code follow the project conventions in `AGENTS.md`?
   Check in particular:
   - Python >= 3.7 compatibility (no walrus operators, no
     match statements, `importlib-metadata` backport where
     version lookups are needed).
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

## Documentation

 * Has `ARCHITECTURE.md` been updated if this change adds or
   modifies modules, commands, or the cluster assembly flow?
 * Has `AGENTS.md` been updated?
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

 * Is all deferred work and pre-existing errors listed in a plan
   file under `docs/plans/`?

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

## Build verification

 * Does `pip install -e .` succeed in a fresh venv alongside
   `shakenfist-client`?
 * Does `tox` pass?
 * Has `shakenfist_client_k3s/_version.py` stayed out of the
   commit? It is generated and gitignored.
