# Release Infrastructure Setup

This document describes how to configure PyPI and GitHub to enable automated
releases using GitHub Actions with Sigstore signing.

## Overview

The release process uses:

- **PyPI Trusted Publishers (OIDC)**: No API tokens needed; PyPI trusts the
  GitHub Actions workflow directly
- **Sigstore/gitsign**: Keyless signing for git tags (no GPG private key
  management)
- **GitHub Environments**: Required reviewer approval before releases proceed
- **Protected Tags**: Restrict who can create release tags

## One-Time Setup Steps

### 1. Configure PyPI Trusted Publisher

This allows the GitHub Actions workflow to publish to PyPI without storing any
API tokens. Which of the two flows below applies depends on whether the
package has ever been published before.

**First release (no project on PyPI yet)**: there is no project page to
navigate to, so a project-scoped publisher cannot be created -- there is
nothing to scope it to. Use PyPI's **pending publisher** flow instead:

1. Log in to [pypi.org](https://pypi.org) with your account
2. Go to <https://pypi.org/manage/account/publishing/>, the
   account-level publishing settings -- not a project page, since the
   project does not exist yet. The direct link is given because this
   form sits under account settings rather than anywhere under your
   projects, which is where people look for it
3. Click **Add a new pending publisher**
4. Fill in the form:
   - **PyPI Project Name**: `shakenfist_client_k3s`
   - **Owner**: `shakenfist`
   - **Repository name**: `client-python-k3s`
   - **Workflow name**: `release.yml`
   - **Environment name**: `release` (must match the workflow)
5. Click **Add**

The pending publisher converts automatically into an ordinary
project-scoped trusted publisher the first time a release successfully
uploads under that project name. Nothing further needs to be done after
that first upload.

**Subsequent releases (the project already exists on PyPI)**: add a
project-scoped publisher from the project itself:

1. Log in to [pypi.org](https://pypi.org) with your account
2. Navigate to your project: `shakenfist_client_k3s`
3. Go to **Settings** (or **Your projects** > **Manage**)
4. Click **Publishing** in the left sidebar
5. Under **Trusted Publishers**, click **Add a new publisher**
6. Fill in the form:
   - **Owner**: `shakenfist`
   - **Repository name**: `client-python-k3s`
   - **Workflow name**: `release.yml`
   - **Environment name**: `release` (must match the workflow)
7. Click **Add**

The workflow will now be able to publish without any stored credentials.

**Note**: If the `shakenfist_client_k3s` package already exists on PyPI under
a different publishing method, you can add the trusted publisher alongside the
existing setup and then remove the old API token once verified.

**Getting any of these values wrong** (the PyPI project name, owner,
repository, workflow filename or environment name -- five on the
pending-publisher form, four when scoping to an existing project, which
supplies its own name), under either flow, does not fail immediately: `build` and `sign-tag` both succeed first, so by the time
`publish-pypi` rejects the OIDC claim, `sign-tag` has already signed and
force-pushed the release tag. Because that push is a force-push, the
tag cannot simply be corrected and re-pushed -- see "Tag Signature
Verification Fails" below; the recovery is to fix the publisher
configuration and release the next version instead.

This gap in the shared release template -- step 1 as originally written
only covered the project-scoped case -- is tracked upstream as
[shakenfist/development#188](https://github.com/shakenfist/development/issues/188).

### 2. Create GitHub Environment with Required Reviewers

This ensures releases only happen after explicit approval, and it is
**not optional**, even though nothing in the workflow enforces it.

`sign-tag` and `publish-pypi` both declare `environment: release`. If no
`release` environment exists yet, GitHub creates one implicitly the
first time the workflow references it -- **with no protection rules at
all**. A release run before this step has been done does not fail: it
simply runs straight through to PyPI without ever pausing for approval.
Because nothing fails, there is nothing to notice; this step has to be
verified by command, not by having visited the UI (see below).

1. Go to the repository on GitHub: `shakenfist/client-python-k3s`
2. Click **Settings** > **Environments**
3. Click **New environment**
4. Name it: `release`
5. Click **Configure environment**
6. Under **Environment protection rules**:
   - Check **Required reviewers**
   - Add yourself (and any other trusted maintainers)
   - Optionally add a **Wait timer** (e.g., 5 minutes) for additional safety
7. Under **Deployment branches and tags**:
   - Select **Selected branches and tags**
   - Add a rule: `v*` (to only allow release tags)
8. Click **Save protection rules**

**Alternative: the API.** `PUT /repos/{owner}/{repo}/environments/{name}`
accepts `reviewers`, `wait_timer` and `deployment_branch_policy`, so the
environment (including its protection rules) can be created in one call
instead of through the UI. This is the more reproducible route -- it can
be scripted and re-applied -- but *who* the reviewers should be is a
human decision either way, so the API does not remove the need for a
person to make that call; it only removes the need to click through the
UI to record it.

**Verify this step by command, not by memory of having done it**:

```bash
gh api repos/shakenfist/client-python-k3s/environments \
  --jq '.environments[] | [.name, ([.protection_rules[].type] | join(","))] | @tsv'
```

This must print a line for `release` with `required_reviewers` among
its protection rules. If it prints nothing, or prints `release` with an
empty second column, the environment either does not exist or exists
unprotected, and no release should be tagged until it is fixed.

### 3. Configure Protected Tags (Recommended)

This prevents unauthorized users from creating release tags. **The
ruleset and the GitHub Actions entry on its bypass list are one
change, not two: they go in together, or neither goes in.** The
release workflow's `sign-tag` job re-creates and force-pushes the
release tag as `github-actions[bot]` using `GITHUB_TOKEN`. Adding the
ruleset without also adding the Actions bypass is worse than not adding
the ruleset at all -- it rejects that push and fails every release at
the signing step, silently turning a working release pipeline into a
broken one. Add both parts of step 3 in the same sitting.

For the same reason, do this only once at least one release has
succeeded without it. The first release exercises a signing path that
has never run against this repository before; adding an untested gate
in front of that first run trades a real risk (a mistake in the
ruleset or its bypass list breaking the only release that has ever
happened) for a theoretical one (an unprotected tag namespace for the
short window before the second release). The consequence to be aware of
is that the first release run which exercises the signing path *against*
the ruleset is then the second release, not the first.

1. Go to **Settings** > **Rules** > **Rulesets**
2. Click **New ruleset** > **New tag ruleset**
3. Configure:
   - **Ruleset name**: `Release tags`
   - **Enforcement status**: `Active`
   - **Target tags**: Add pattern `v*`
   - **Rules**: Check **Restrict creations** and **Restrict deletions**
   - **Bypass list**: Add repository admins or specific maintainers,
     **and GitHub Actions** (add the "GitHub Actions" app to the bypass
     list). The release workflow's sign-tag job re-creates and
     force-pushes the release tag as `github-actions[bot]` using
     `GITHUB_TOKEN`; without the Actions bypass that push is rejected
     by this ruleset and every release fails at the signing step.
4. Click **Create**

### 4. Verify Sigstore/Rekor Access

No configuration needed. Sigstore is a public service that:

- Signs artifacts using OIDC identity (the GitHub Actions workflow identity)
- Records signatures in a public transparency log (Rekor)
- Requires no key management

Verification can be done by anyone using `cosign` or `gitsign verify`.

## How Releases Work

1. A maintainer pushes a tag matching `v*` (e.g., `v0.1.0`)
2. The `release.yml` workflow triggers
3. The workflow builds the package and waits for environment approval
4. A required reviewer approves the release in GitHub's UI
5. The workflow:
   - Creates a signed git tag using gitsign (Sigstore)
   - Generates Sigstore attestations for the built artifacts
   - Publishes to PyPI using OIDC (no tokens)
   - Creates a GitHub Release with the artifacts

## Verifying Releases

### Verify Git Tag Signature

```bash
# Install gitsign
go install github.com/sigstore/gitsign@latest

# Verify a tag
gitsign verify --certificate-identity-regexp='.*' \
    --certificate-oidc-issuer='https://token.actions.githubusercontent.com' \
    v0.1.0
```

### Verify PyPI Package Attestation

```bash
# PyPI shows attestation status on the package page
# Look for the "Provenance" section
```

### Verify with Cosign

```bash
# Install cosign
go install github.com/sigstore/cosign/v2/cmd/cosign@latest

# Verify artifact attestation
cosign verify-attestation \
    --certificate-identity-regexp='.*' \
    --certificate-oidc-issuer='https://token.actions.githubusercontent.com' \
    shakenfist_client_k3s-0.1.0.tar.gz
```

## Troubleshooting

### "Environment not found" Error

Ensure the environment name in the workflow (`release`) exactly matches the
environment created in GitHub Settings.

### "Publisher not found" Error on PyPI

- Verify the workflow filename matches exactly (case-sensitive)
- Verify the environment name matches exactly
- Ensure you're using the correct PyPI account (not TestPyPI)

### Tag Signature Verification Fails

- Ensure you're checking against the correct OIDC issuer
- The certificate identity will be the workflow's identity, not a personal
  email

### Approval Not Requested

- Ensure the tag matches the deployment branch/tag rules (e.g., `v*`)
- Check that required reviewers are configured on the environment

## Security Considerations

- **No long-lived secrets**: Neither GPG keys nor PyPI tokens are stored
- **Audit trail**: All releases are logged in GitHub Actions and Sigstore's
  Rekor transparency log
- **Multi-party approval**: Required reviewers prevent unilateral releases
- **Immutable provenance**: Sigstore attestations cryptographically link
  artifacts to the exact source commit
