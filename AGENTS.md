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

There are no unit tests yet; testing is manual against a live Shaken
Fist cluster.

## Key Files

| File | Purpose |
|------|---------|
| `shakenfist_client_k3s/__init__.py` | Click command group (`k3s ...`), plugin entry point |
| `shakenfist_client_k3s/primitives.py` | Cluster orchestration primitives and version caches |
| `pyproject.toml` | Package metadata and dependencies |

## Code Conventions

- Python >= 3.7 compatibility (conservative for broad client support)
- Single quotes for strings, double quotes for docstrings
- Max line length: 120 characters
- Trim trailing whitespace
- The plugin must never break `sf-client` startup: it is imported
  unconditionally by the plugin loader, so top-level imports must be
  cheap and reliable

## When Making Changes

- Ensure changes work with Python >= 3.7
- Verify the plugin still imports cleanly (`python -c 'import
  shakenfist_client_k3s'`) -- a broken import takes the whole
  `sf-client` CLI down with it
- Update ARCHITECTURE.md if the orchestration flow changes
  significantly
