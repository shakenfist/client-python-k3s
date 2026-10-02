# First release phase 4: get on PyPI without lying

## Prompt

Before responding to questions or discussion points in this document,
read `pyproject.toml`, `.github/workflows/release.yml` and
`RELEASE-SETUP.md` in full, and build the package from a clean clone to
see what it actually produces. Ground your answers in the built
artefacts rather than in what the metadata says it will build.

Two things about this phase are unlike the three before it. Most of the
machinery already exists: `release.yml` is the shared release template
from `shakenfist/development`, complete and annotated, so this phase is
mostly about the metadata it will publish and the accounts it will
publish through. And two of the required steps cannot be done by an
agent at all -- a PyPI trusted publisher and a GitHub environment are
both created through a logged-in browser -- so the phase has a hard
human gate in the middle of it.

The thing to keep in front of you is that a release is not revertable.
Once `0.1.0` is on PyPI it is there for good: PyPI does not allow a
version to be re-uploaded, and yanking does not remove the file. Every
metadata defect this phase does not fix before the tag is a defect that
needs `0.1.1` to fix. That asymmetry is why the packaging work comes
first and the tag comes last.

## Planning effort

**Medium.** The master plan's "In this project" note reserves high
effort for cluster assembly ordering, agent operation wait loops and
the namespace-metadata representation of cluster state. This phase
touches none of those: it is packaging metadata, a runbook correction,
and two web-console setup steps, against a release workflow that
already exists and is shared with other repositories. The judgement in
it is about ordering and irreversibility, not about correctness under
concurrency.

## Scope

In scope:

1. Fix what the wheel contains and what its metadata claims, before
   anything is published.
2. Correct `RELEASE-SETUP.md`, which cannot be followed as written for
   a project that does not exist on PyPI yet.
3. The two human setup steps: the PyPI pending trusted publisher, and
   the `release` GitHub environment with required reviewers.
4. Tag `v0.1.0`, approve the release, and confirm it landed.
5. Verify the install path a user actually takes, from PyPI into a
   clean virtualenv.
6. The tag protection ruleset, after the first release rather than
   before it (decision 5).

Explicitly out of scope:

- **The collection.** Phase 5. Its `build-collection` and
  `publish-collection` jobs go into the same `release.yml`, but adding
  them now would mean a release workflow whose new jobs have never
  run, on the one release where that matters most.
- **Raising the Python floor.** `requires-python = ">=3.7"` is a
  master plan constraint and matches what `shakenfist_client` itself
  declares. Survey finding 9 records that nothing tests below the
  runner's default; that wants a CI matrix and an ecosystem decision,
  not a release chore. Filed as an issue instead (decision 6).
- **A changelog.** `release.yml` passes `generate_release_notes: true`
  to `softprops/action-gh-release`, so GitHub writes the notes from
  the commit range. A hand-maintained `CHANGELOG.md` would be a second
  source of the same facts.
- **Moving `RELEASE-SETUP.md` into `docs/`.** It is a runbook and the
  documentation policy would put it there, but it is a copy of a
  shared template and three other repositories carry it at the root.
  Diverging unilaterally makes the next template sync harder than the
  misfiling costs. Decision 2 fixes it in place and pushes the fix
  upstream.

## What the survey found

Verified against the tree at `d51cf59` and against a clean-clone build,
not read off the plan.

### The release machinery is already there

`.github/workflows/release.yml` is the full shared template: a `build`
job producing sdist and wheel and running `twine check`, then
`sign-tag` (gitsign/Sigstore), `publish-pypi` (trusted publisher OIDC
plus build provenance attestations) and `github-release`. All three
publishing jobs are guarded `github.event_name == 'push' &&
startsWith(github.ref, 'refs/tags/v')`, with a comment explaining that
the event is tested as well as the ref so a `workflow_dispatch` aimed
at a tag cannot re-sign and force-push it. So a dispatch run exercises
the build and the `twine check` and nothing else, which makes it a free
dry run. Phase 4 writes almost none of this.

### 1. The `release` environment does not exist

```
$ gh api repos/shakenfist/client-python-k3s/environments
{"total_count":0,"environments":[]}
```

The repository has no environments at all. Both `sign-tag` and
`publish-pypi` declare `environment: release`. GitHub creates an
environment implicitly the first time a workflow references one, with
**no protection rules**, so the release would run start to finish
without ever asking for the approval `RELEASE-SETUP.md` step 2
promises. This is the failure most likely to pass unnoticed, because
nothing fails: the release simply succeeds when it should have paused.

### 2. No tag ruleset exists

```
$ gh api repos/shakenfist/client-python-k3s/rulesets --jq '.[] | [.name,.target] | @tsv'
Develop branch	branch
```

`RELEASE-SETUP.md` step 3 (a `Release tags` ruleset over `v*`,
restricting creations and deletions, with **GitHub Actions** on the
bypass list) has not been done. Its absence is not a release blocker:
the `sign-tag` job's `git push origin "${TAG_NAME}" --force` succeeds
precisely because nothing restricts it. But the step is half a
mechanism -- adding the ruleset *without* the Actions bypass breaks
every release at the signing step, which is what that paragraph in
`RELEASE-SETUP.md` warns about.

### 3. `RELEASE-SETUP.md` step 1 cannot be followed as written

It says "Navigate to your project: `shakenfist_client_k3s`". There is
no such project:

```
$ curl -s -o /dev/null -w '%{http_code}\n' https://pypi.org/pypi/shakenfist_client_k3s/json
404
$ curl -s -o /dev/null -w '%{http_code}\n' https://pypi.org/pypi/shakenfist-client/json
200
```

For a distribution that has never been published, a project-scoped
trusted publisher cannot be created, because there is no project to
scope it to. PyPI's flow for this case is a **pending publisher**,
created from the account's own publishing page, which converts into a
project-scoped one on first successful upload. The document was copied
from a template written for repositories whose package already exists,
and this is the one piece of its staleness that stops the phase dead
rather than merely reading oddly.

### 4. The wheel ships the entire test suite

`pyproject.toml` declares:

```toml
[tool.setuptools.packages.find]
include = ["shakenfist_client_k3s*"]
exclude = ["shakenfist_client_k3s.tests*"]
```

and that exclusion works, as a direct check confirms:

```
include only: ['shakenfist_client_k3s', 'shakenfist_client_k3s.tests']
with exclude: ['shakenfist_client_k3s']
```

The built wheel nevertheless contains `shakenfist_client_k3s/tests/`
in full -- `__init__.py`, `fakes.py`, eleven `test_*.py` and thirteen
`cli_contract/*.txt` -- 39 entries where 12 would do. The exclusion
drops the *package*; `setuptools_scm`'s file finder plus
`include_package_data`, which defaults to true under `pyproject.toml`,
then re-adds every git-tracked file under the package directory as
package *data*. Adding

```toml
[tool.setuptools]
include-package-data = false
```

takes the wheel to 12 entries with no `tests/` path in it, verified by
rebuilding. Nothing in the package reads a data file at runtime, so
there is nothing legitimate for `include_package_data` to be carrying.

The count is 12 and not 13 because step 4a also removes `write_to`, so
`_version.py` is no longer generated and no longer shipped. This passage
said 13 in both places until the review of #81 noticed that it disagreed
with `MAX_ENTRIES` in `tools/check-dist.sh`; the script was right. The
twelve are six modules under `shakenfist_client_k3s/` and six under
`.dist-info`: `METADATA`, `WHEEL`, `entry_points.txt`, `top_level.txt`,
`RECORD` and `licenses/LICENSE`, the last of those being a consequence
of setting `license-files`.

### 5. Two deprecation warnings in the distribution metadata

The build emits both:

- `project.license` as a TOML table is deprecated. "By 2027-Feb-18,
  you need to update your project and remove deprecated calls or your
  builds will no longer be supported." `pyproject.toml:15` has
  `license = {text = "Apache-2.0"}`; the supported spelling is a bare
  SPDX string plus `license-files`.
- The `License :: OSI Approved :: Apache Software License` classifier
  is deprecated in favour of that SPDX expression.

Neither breaks anything today. Both are in the metadata this phase is
about to publish, and both are one-line fixes.

### 6. `write_to` produces a file nothing reads

`pyproject.toml:55` sets `write_to =
"shakenfist_client_k3s/_version.py"`. Nothing imports `_version`. The
runtime version comes from `importlib.metadata.version()` --
`cluster.py:38-40` for the import with its `importlib_metadata`
backport fallback, and `cluster.py:1397` for the single use, which
stamps `plugin_version` into every cluster's namespace metadata at
create time. `ARCHITECTURE.md:208-210` describes `_version.py` as
though it were the version source.

That last detail is the phase's most concrete user-visible effect:
today a cluster created from a checkout records
`plugin_version: 0.1.dev182+gd51cf5924`. After this phase it records
`0.1.0`.

### 7. The build works, and produces an unpublishable version

From a clean clone of `develop`, in a fresh virtualenv:

```
Successfully built shakenfist_client_k3s-0.1.dev182+gd51cf5924.tar.gz
  and shakenfist_client_k3s-0.1.dev182+gd51cf5924-py3-none-any.whl
Checking dist/...whl: PASSED
Checking dist/...tar.gz: PASSED
```

`git describe --dirty --tags --match v* --first-parent` fails outright
("No names found"), and `setuptools_scm` falls back to
`0.1.dev<N>+g<sha>`. The `+g...` local version segment is one PyPI
rejects, which is a useful accident: an untagged build cannot be
uploaded even if something tried.

### 8. Two claims the phase makes true rather than corrects

- `README.md:15` already tells users `pip install
  shakenfist_client_k3s`.
- `ARCHITECTURE.md:211` already says "published to PyPI as
  `shakenfist_client_k3s`".

Both are false today. Neither needs editing; they need the release.

### 9. The Python floor is not false, but it is untested

Every module parses under `ast.parse(..., feature_version=(3, 7))`.
There is no walrus operator, no `match`, no dict union, no
`removeprefix`/`removesuffix` and no builtin generics in annotations.
`shakenfist_client` itself declares `requires-python >= 3.7`. But no CI
job runs any interpreter other than the runner's default, and the
classifier list stops at `3.7` while the suite is exercised on 3.13, so
">=3.7" is an assertion nothing checks.

### 10. Corrections made at source

Three claims in the master plan were stale from phase 3's execution
rather than from this phase's, and are corrected in this phase's first
commit so a later reader does not trip over them:

- Open question 2 recommended acting on the Kubernetes Node through
  the API rather than a `kubectl` subprocess. Phase 3 did the
  opposite, and for a better reason than the recommendation had: it
  runs `kubectl` *in the cluster* through the agent, so the caller
  needs no cluster credentials at all.
- Open question 3 is settled as recommended: both flags shipped.
- "Bugs fixed during this work" said "Nothing yet". Phase 3 fixed
  four, and none had an issue of its own.

The master plan's Situation claim that the package has never been
released, with no tags and both names 404 on PyPI, is still exactly
true. Nothing else in the phase 4 row needed correcting; it is terse
rather than wrong.

## Decisions this plan already takes

1. **All packaging fixes land before the tag, in one commit, and the
   tag is the last thing that happens.** PyPI will not accept a
   re-upload of a version, and yanking leaves the file in place. A
   wheel carrying the test suite, or metadata whose license
   declaration stops building in 2027, is permanent once published.
   Every fix in survey findings 4, 5 and 6 is a one-line change and
   costs nothing now.

2. **`RELEASE-SETUP.md` is corrected in place, and the same correction
   is filed upstream against `shakenfist/development`.** The
   pending-publisher hole is not specific to this repository: it
   affects every repository that copied the release template and has
   not yet published its first version. Fixing only the local copy
   leaves the next repository to rediscover it, and moving the file
   into `docs/` -- which the documentation policy would otherwise
   prefer for a runbook -- would put this copy out of step with the
   template it is synced from.

3. **The human setup steps are a hard gate, not a step in the table.**
   Creating a PyPI pending publisher and a GitHub environment with
   required reviewers both need Michael's logged-in browser. No agent
   can do either. The plan's job is to make the gate explicit, say
   exactly what to create, and give a command that verifies it
   afterwards -- rather than a step brief that an agent will attempt
   and fail.

4. **Both halves of the gate are verified by command, not by having
   been done.** `gh api repos/.../environments` must list `release`
   and report a `required_reviewers` protection rule. A trusted
   publisher cannot be read back through the API at all, so the
   verification for that half is a `workflow_dispatch` dry run of
   `release.yml`, which by design builds and `twine check`s without
   publishing, followed by reading the failure mode if the real run
   rejects the OIDC claim.

5. **The tag protection ruleset comes after the first release, not
   before it.** This is the decision most likely to be argued with,
   and the argument against it is good: restricting who may create a
   `v*` tag is worth having *before* anyone can create one. The reason
   to defer is survey finding 2. The ruleset is only safe in
   combination with a GitHub Actions bypass, because `sign-tag`
   force-pushes the tag it just signed; a ruleset added with that
   bypass wrong fails the release at the signing step, after the
   artefacts are built and after the tag is public. The first release
   is the one run where nothing about this pipeline has ever executed,
   and adding an untested gate to it trades a real risk for a
   theoretical one -- the set of people who can push a tag to this
   repository today is the set of people who can merge to `develop`.

6. **`requires-python` is not changed, and the gap is filed.** Survey
   finding 9 is a real hole, but closing it means deciding what this
   package supports, adding a CI matrix to prove it, and staying
   consistent with `shakenfist_client`'s own floor. That is not a
   release chore, and doing it inside the release phase would mean
   publishing `0.1.0` with a compatibility claim changed in the same
   commit range that first made compatibility observable.

   Filed as shakenfist/client-python-k3s#82, which the review of #81
   prompted -- the issue had not actually been created, and the review
   also sharpened what it needs to say. Raising the setuptools floor to
   77.0.1 means 3.7 and 3.8 are wheel-only: the wheel is `py3-none-any`
   and installs and runs there, but setuptools has needed Python 3.9 or
   newer since 76.0.0, so anything building from the sdist on those
   versions cannot resolve a build backend. The floor is therefore not
   one claim any more but two, and the issue records both. Keeping
   `license = {text = ...}` to avoid this was the alternative; it is
   deprecated with a 2027-02-18 removal, so it would have traded a
   documented wheel-only caveat for metadata which has to be changed
   again before then anyway.

7. **`write_to` is removed rather than documented.** Keeping it means
   `ARCHITECTURE.md` has to explain a generated file that no code
   reads; removing it means `ARCHITECTURE.md` describes `setuptools_scm`
   as what it is, a version source for the build. `[tool.setuptools_scm]`
   on its own is enough for the version to be derived and stamped into
   the distribution metadata, which is where `importlib.metadata` reads
   it from.

8. **If the publish step fails after the tag is pushed, the next
   attempt is `v0.1.1`, not a re-push of `v0.1.0`.** `sign-tag`
   force-pushes the tag, so re-running a release rewrites a signed tag
   object someone may already have fetched and verified, and the
   workflow's own comment says why the guards exist to prevent exactly
   that. A burned version number costs nothing.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 4a | medium | sonnet | none | **Commit subject: `Publish the package, not the test suite.`** Fix survey findings 4, 5 and 6 in `pyproject.toml`, all before anything is published. Add a `[tool.setuptools]` table with `include-package-data = false` -- place it above `[tool.setuptools.packages.find]`, since the existing exclusion there is correct and is not what is failing; the tests arrive as package *data* through `setuptools_scm`'s file finder, so say that in a comment or the next reader will "fix" the exclusion instead. Replace `license = {text = "Apache-2.0"}` with `license = "Apache-2.0"` and add `license-files = ["LICENSE"]`, and remove the `License :: OSI Approved :: Apache Software License` classifier; both are deprecated and the build warns about both. Remove the `write_to` line from `[tool.setuptools_scm]`, leaving the table present but empty of it, and delete `shakenfist_client_k3s/_version.py` from `.gitignore` only if nothing else needs it there -- check, do not assume. Then correct `ARCHITECTURE.md:206-212`, whose Versioning bullet says `setuptools_scm` "writes `shakenfist_client_k3s/_version.py` at build time (that file is gitignored and must never be committed)": say instead that it derives the version from git tags into the distribution metadata, which is where `importlib.metadata.version()` reads it at `cluster.py:1397`. Leave the Distribution bullet alone -- it claims PyPI publication, which step 4d makes true. Write `tools/check-dist.sh`, taking a wheel path, which fails if any entry contains `/tests/` or if the wheel has more than a stated number of entries, and call it from `release.yml`'s `build` job immediately after `twine check`. Prove it works before committing: revert `include-package-data`, confirm the script fails, restore, and say so in the commit message. Do not touch `README.md`, and do not create a tag. |
| 4b | low | sonnet | none | **Commit subject: `Fix the release runbook for a first release.`** `RELEASE-SETUP.md` step 1 tells the reader to navigate to a PyPI project that does not exist (survey finding 3). Rewrite it to give the pending-publisher flow for a first release -- PyPI account settings, Publishing, "Add a new pending publisher", with the same four values the current step lists (owner `shakenfist`, repository `client-python-k3s`, workflow `release.yml`, environment `release`) and a sentence saying it converts to a project-scoped publisher on first upload. Keep the existing project-scoped instructions for subsequent releases rather than deleting them; label which case each applies to. In step 2, add that GitHub creates the environment implicitly with no protection rules the first time a workflow references it, so a release run before the environment exists succeeds *without* pausing for approval -- that is survey finding 1 and it is the whole reason step 2 is not optional. In step 3, make the ordering explicit: the ruleset and the GitHub Actions bypass go in together or neither goes in, per decision 5. Then file an issue against `shakenfist/development` describing the pending-publisher hole in `templates/release-automation/`, since every repository that copied the template and has not published has it, and reference that issue from the corrected step. Documentation and one issue only; no code. |
| -- | -- | -- | -- | **Human gate. Satisfied 2026-10-01; see the note below this table.** Michael creates (i) a PyPI pending publisher per the corrected `RELEASE-SETUP.md` step 1, and (ii) the `release` environment with required reviewers per step 2. Then verify: `gh api repos/shakenfist/client-python-k3s/environments --jq '.environments[] \| [.name, ([.protection_rules[].type] \| join(","))] \| @tsv'` must print `release` with `required_reviewers` among its rules. Do not proceed on the strength of the web UI having been visited. |
| 4c | low | -- | none | **Management session, no commit. Done: run [36833525105](https://github.com/shakenfist/client-python-k3s/actions/runs/36833525105).** Dry run `release.yml`, then confirm from the run that `build` passed, `twine check` passed, `tools/check-dist.sh` passed, and that `sign-tag`, `publish-pypi` and `github-release` were all skipped -- the event guard is what makes that safe, so if any of the three ran, stop and report rather than tagging. Dispatch it against **this phase's branch**, not `develop`: the wheel check is added by 4a and so does not exist on `develop` until this phase lands, and a dispatch against `develop` would pass while proving nothing about the wheel. This brief originally said `--ref develop`, which was wrong for that reason. The run also showed why an untagged dispatch cannot publish even if a guard were wrong: `setuptools_scm` derived `0.1.dev187+gf69b52de4`, a local version identifier, which PyPI refuses. |
| 4d | low | -- | none | **Management session, no commit.** Tag and release. `git tag v0.1.0 <develop head>` and push it; approve the `release` environment when GitHub asks; then confirm all four jobs succeeded. If `publish-pypi` fails, do not re-push the tag -- decision 8 -- report and plan `v0.1.1`. |
| 4e | low | sonnet | none | **Commit subject: `Record the first release.`** Verification and close-out, after 4d has succeeded. In a throwaway virtualenv outside the repository, `pip install shakenfist_client_k3s`, then assert three things and record the output in the commit message: the installed version is `0.1.0`; `python -c 'import shakenfist_client_k3s'` is silent; and the wheel that pip fetched has no `tests/` path in it (`pip download --no-deps` and list the archive). Then set the master plan's phase 4 row to `Complete` with `v0.1.0` in the `Merged` cell, and update its `docs/plans/index.md` Phases cell. Check whether `ARCHITECTURE.md`'s "published to PyPI" bullet and `README.md:15`'s `pip install` line are now true and say so in the commit message; neither should need editing, and if either does, that is a finding worth stating rather than a silent fix. |
| 4f | low | -- | none | **Management session, no commit.** Add the `Release tags` ruleset per the corrected `RELEASE-SETUP.md` step 3, with GitHub Actions on the bypass list, now that a release has run once without it. Then verify the bypass by dispatching `release.yml` once more -- it must still build and skip -- and record that the next real release will exercise the signing path against the ruleset for the first time. |

Only half of that gate was genuinely un-automatable, which this plan
originally got wrong. The PyPI pending publisher has no API and had to be
created in a browser. The `release` environment did not: `PUT
/repos/{owner}/{repo}/environments/{name}` takes `reviewers`, `wait_timer`
and `deployment_branch_policy`, and the configuration did not need deciding
either, because `client-python`, `agent-python` and `occystrap` already
agree on it -- required reviewer `mikalstill`, `prevent_self_review: false`,
and a custom deployment branch policy of `tag:v*`. This repository was
given the same, read back from `client-python` rather than invented. It
verifies as:

```
$ gh api repos/shakenfist/client-python-k3s/environments \
    --jq '.environments[] | [.name, ([.protection_rules[].type] | join(","))] | @tsv'
release	required_reviewers,branch_policy
$ gh api repos/shakenfist/client-python-k3s/environments/release/deployment-branch-policies \
    --jq '.branch_policies[] | .type + ":" + .name'
tag:v*
```

The branch policy matters on its own account: `branch_policy` appears as a
protection rule as soon as `custom_branch_policies` is set, but until a
policy is actually attached it permits nothing, so the two calls are one
change and not two.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| The `release` environment is referenced before it exists, GitHub creates it unprotected, and the release publishes to PyPI with no approval. Nothing fails, so nothing draws attention to it. | The human gate's verification command, run before 4d rather than after. This is survey finding 1 and it is why the gate is a gate. |
| The PyPI pending publisher is created with a value that does not match the workflow -- filename, environment name, or owner -- and `publish-pypi` fails after `sign-tag` has already pushed a signed `v0.1.0`. | Decision 8: burn the version and go to `v0.1.1` rather than force-pushing over a signed tag. The dry run in 4c cannot catch this, because the guards deliberately skip the publishing jobs, so the mitigation is the recovery plan rather than prevention. |
| `tools/check-dist.sh` asserts an entry count that drifts the first time a legitimate module is added, and someone raises the number without looking. | Make the failure message say what to check rather than what number to change, and have the script name the offending paths. The `/tests/` assertion is the load-bearing half; the count is a tripwire. |
| Removing `write_to` breaks something that reads `_version` in a way grep missed. | `grep -rn '_version' --include='*.py'` over the tree finds only `pyproject.toml` and `k3s_version`/`plugin_version` metadata keys. 4a's brief says to check `.gitignore` rather than assume; the same care applies here, and the smoke tier plus `python -c 'import shakenfist_client_k3s'` would catch an import error immediately. |
| The tag ruleset is added in 4f with the bypass misconfigured, and the *next* release fails at signing -- the failure this phase deferred rather than removed. | 4f's dispatch verification, plus the note it records. A release that fails at `sign-tag` has not published, so the recovery is to fix the bypass and re-run, not to burn a version. |
| `0.1.0` implies more stability than a package whose library API is three phases old actually has. | Out of this phase's hands and deliberately not solved with a version number: the master plan's phase 4 row says `v0.1.0`, and `0.x` already signals it. Phase 3's risk table established there are no existing library callers to break. |

## Open questions

1. **Does the collection want its own version line?** Phase 5 adds
   `build-collection` and `publish-collection` to the same
   `release.yml`, driven by the same `v*` tag, so the collection and
   the plugin would share a version. `shakenfist/shakenfist` has the
   same shape for `shakenfist.shakenfist`. Worth confirming that is
   intended before phase 5 wires it, because splitting them later
   means two tag patterns.
2. **Should `check-dist.sh` also assert the sdist?** The sdist
   legitimately contains `.github/`, `docs/` and the tests, which is
   normal and harmless. But it is the artefact from which anyone
   building from source starts, and nothing currently states what it
   should contain.

## Definition of done

Each of these is checkable, and most are one command:

- `gh api repos/shakenfist/client-python-k3s/environments --jq
  '.environments[].name'` prints `release`, and that environment's
  protection rules include `required_reviewers`.
- `curl -s https://pypi.org/pypi/shakenfist_client_k3s/json` returns
  200 and its `.info.version` is `0.1.0`.
- In a virtualenv created outside this repository, `pip install
  shakenfist_client_k3s` succeeds and
  `python -c "from importlib.metadata import version;
  print(version('shakenfist_client_k3s'))"` prints `0.1.0`.
- The wheel PyPI serves contains no path matching `/tests/`, and
  `tools/check-dist.sh` exits non-zero when given a wheel that does.
  The review of #81 replaced the original hand-demonstration of this
  with `tests/test_check_dist.py`, so the criterion is now
  `stestr run test_check_dist` passing, and `tools/check-wheel-build.sh`
  running in the pull request tier rather than only at release time.
- `python -m build` emits no warning mentioning `project.license` or
  license classifiers.
- `git describe --tags --match 'v*'` on `develop` prints `v0.1.0`, and
  `git tag -v v0.1.0` shows a Sigstore signature.
- `grep -nE 'write_to|_version\.py' pyproject.toml` returns nothing,
  and no line of `ARCHITECTURE.md` claims `_version.py` is where the
  version is read from. (The pattern is not plain `_version`, which
  matches the `python_version` environment marker in `dependencies`.)
- `RELEASE-SETUP.md` contains the phrase "pending publisher", and no
  step instructs the reader to navigate to a PyPI project as the only
  way to add a trusted publisher.
- The issue filed against `shakenfist/development` about the release
  template is linked from `RELEASE-SETUP.md`.
- A `Release tags` ruleset exists over `v*` with GitHub Actions on its
  bypass list, and a `workflow_dispatch` run of `release.yml` after it
  was added still builds and still skips all three publishing jobs.
- `pre-commit run --all-files` and `tox -epy3` pass.
- The master plan's phase 4 row is `Complete` with `v0.1.0` recorded,
  and `docs/plans/index.md` links this file.

## Back brief

Restate, before starting 4a: which two steps of this phase no agent can
perform and why; what happens if `release.yml` runs while the `release`
environment does not exist; and why the tag protection ruleset is
scheduled after the first release rather than before it. If any of those
three readings differ from this plan, say so before touching
`pyproject.toml`.

The human gate is a hard stop. Do not push a tag, and do not treat the
web console having been visited as evidence: run the verification
command and report its output before starting 4c.
