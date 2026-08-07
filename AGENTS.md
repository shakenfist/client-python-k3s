# Agents Guide

## Project Overview

This is a plugin for
[sf-client](https://github.com/shakenfist/client-python), the command
line client for [Shaken Fist](https://github.com/shakenfist/shakenfist).
It adds a `k3s` command group which orchestrates
[k3s](https://k3s.io/) Kubernetes clusters on top of Shaken Fist
instances, including [MetalLB](https://metallb.io/) for load balancer
addresses and [Longhorn](https://longhorn.io/) for persistent storage.

## Quick Start

```bash
# Install in a venv alongside sf-client
pip install -e .

# The plugin registers via the shakenfist_client.plugin entry point
# group, so once installed the commands appear automatically:
sf-client k3s --help
```

Unit tests live in `shakenfist_client_k3s/tests/` and follow the
client-python pattern (testtools + stestr, external APIs mocked). Run
them with `tox -epy3`. Orchestration against a live Shaken Fist
cluster is still tested manually.

## Key Files

| File | Purpose |
|------|---------|
| `shakenfist_client_k3s/__init__.py` | Click command group (`k3s ...`), plugin entry point |
| `shakenfist_client_k3s/primitives.py` | Cluster orchestration primitives and version caches |
| `shakenfist_client_k3s/progress.py` | Phase and wait-loop progress reporting for long running commands |
| `shakenfist_client_k3s/tests/` | Unit tests (stestr, see `.stestr.conf`) |
| `docs/plans/` | Implementation plans, committed with the work they describe |
| `pyproject.toml` | Package metadata and dependencies |

## Code Conventions

- Python >= 3.7 compatibility (conservative for broad client support)
- Single quotes for strings, double quotes for docstrings
- Max line length: 120 characters
- Trim trailing whitespace
- The plugin must never break `sf-client` startup: it is imported
  unconditionally by the plugin loader, so top-level imports must be
  cheap and reliable

## Planning and Pre-push Review

- Substantial work starts from a plan file in `docs/plans/`, based on
  `PLAN-TEMPLATE.md`, committed on the branch that becomes the pull
  request.
- Before pushing, work through the checks in `PUSH-AUDIT.md`.

## When Making Changes

- Run the unit tests with `tox -epy3` and add coverage for new
  parsing or error-handling behavior, especially around external API
  responses which can change shape over time
- Ensure changes work with Python >= 3.7
- Verify the plugin still imports cleanly (`python -c 'import
  shakenfist_client_k3s'`) -- a broken import takes the whole
  `sf-client` CLI down with it
- Update ARCHITECTURE.md if the orchestration flow changes
  significantly
- Larger changes start with a plan in `docs/plans/`, committed on the
  same branch as the work it describes. Plans are retained afterwards
  as a historical record of intent -- durable design detail belongs in
  ARCHITECTURE.md, with the plan recording how we got there
