# The `shakenfist.k3s` Ansible collection

`collection/` ships one native Ansible module, `sf_k3s_cluster`, which
drives the same orchestration `docs/library-api.md` describes --
`shakenfist_client_k3s.cluster.Cluster` -- from a playbook instead of
Python or the `sf-client k3s` command line. The module contains no
orchestration of its own; it is argument parsing over `Cluster`, in
the same shape the command line already is. This page is the operator
reference. The module's own `DOCUMENTATION`, `EXAMPLES` and `RETURN`
strings in `collection/plugins/modules/sf_k3s_cluster.py` are the
source of truth for every parameter and return value below --
`ansible-doc -t module shakenfist.k3s.sf_k3s_cluster` renders them
directly, and this page agrees with that output rather than
paraphrasing it independently.

## Status: not yet published

`ansible-galaxy collection install shakenfist.k3s` **does not work
today**. The collection has not been published to Ansible Galaxy --
the credential and the first tagged release are both outstanding, and
each needs a human to act (see the master plan's phase 5 row). Running
that command now fails with "Could not satisfy the following
requirements... shakenfist.k3s:\* (direct request)", because Galaxy
has no `shakenfist.k3s` collection version to resolve.

Until the first release, install from a locally built tarball instead:

```bash
python3 tools/build-collection.py
ansible-galaxy collection install dist-collection/shakenfist-k3s-*.tar.gz
pip install shakenfist_client_k3s
```

`tools/build-collection.py` rather than `ansible-galaxy collection build`
directly: the version in `collection/galaxy.yml` is a `0.0.0`
placeholder, and that script is what replaces it with the real version
derived from the git tags before building. Building the directory
yourself produces a tarball which installs, and reports its version as
`0.0.0` -- which is confusing on its own and worse next to a published
version later. The script restores the placeholder afterwards, so it
leaves the checkout as it found it.

That path works today, against any checkout of this repository, and
is how to try the module before the first Galaxy release. Once a
release is published, replace the two commands above with
`ansible-galaxy collection install shakenfist.k3s`.

## Installing

Two separate installs are needed, usually on the same machine but
conceptually different things:

```bash
ansible-galaxy collection install shakenfist.k3s   # not yet usable -- see above
pip install shakenfist_client_k3s
```

The collection is Ansible content: the module's argument spec,
documentation and example playbooks. It carries no code that speaks
to the Shaken Fist API. `sf_k3s_cluster.py` imports
`shakenfist_client_k3s` -- the same Python package `docs/library-api.md`
documents -- to reach `Cluster` and `make_client()`, and that import
has to resolve on whichever machine actually runs the module (the
control node, when the task uses `delegate_to: localhost` or a local
connection, as every example on this page does). `collection/requirements.txt`
names `shakenfist_client_k3s>=0.1.0` for exactly this reason: it is a
fact about what the module's Python source imports, not about what
Ansible needs, so Ansible Galaxy -- which only ever installs
collections, never their Python dependencies -- cannot install it for
you. There is no server-side half of this: the plugin talks to the
Shaken Fist REST API directly over HTTP, so nothing related to this
collection is ever installed on a Shaken Fist server or on the
cluster's own nodes.

```bash
pip install -r collection/requirements.txt
```

is the same install, run from this repository's checkout rather than
from memory of the version floor.

## A worked playbook

```yaml
- hosts: localhost
  gather_facts: false
  tasks:
    - name: Ensure a CI runner cluster exists, with workers left to the conductor
      shakenfist.k3s.sf_k3s_cluster:
        name: ci-runners
        namespace: ci
        state: present
        control_plane_count: 1
        initial_workers: 0
      register: cluster

    - name: Fail the play if the cluster is not healthy
      ansible.builtin.assert:
        that: cluster.health.healthy
        fail_msg: "{{ cluster.health }}"
```

This and the module's own `EXAMPLES` string are the same playbook
shapes; `EXAMPLES` additionally shows an administrator connecting with
explicit credentials, a manifest applied at first boot, a check-mode
run, and `state: absent`. Every task needs `delegate_to: localhost` (or
an equivalent local connection) when the play's own hosts are not the
control node, because the module talks to the Shaken Fist API from
wherever it runs, not from a target host's network.

No example on this page has been run against a live Shaken Fist
cluster as part of writing it -- no cluster was available in this
environment. `shakenfist_client_k3s/tests/` exercises the module
against the fake client `docs/library-api.md` describes, which is as
far as this repository's own test suite reaches. The merge tier of
CI (`.github/workflows/functional-tests.yml`'s `cluster_deploy` job,
run from `tools/ci_deploy_test.sh`) does deploy, expand and delete a
real cluster on every merge, but through the `sf-client k3s` command
line directly, not through this collection or this module -- that
script installs neither Ansible nor the collection, by design, to
keep the job lighter than a full ansible run. So the merge tier backs
the orchestration this module calls into, but nothing in this
repository's CI exercises `sf_k3s_cluster` itself against a real
cluster yet; the playbooks on this page are the module's documented
shapes, not an independently verified run of the module.

## The namespace the cluster lives in is not the identity you authenticate as

`sf_k3s_cluster` takes two namespace-shaped parameters that must not be
confused, and the confusion is easy to make if you have used the
`shakenfist.shakenfist` collection before:

- `namespace` (required) -- the Shaken Fist namespace the cluster
  itself lives in. The module does not create this namespace;
  `sf_namespace` from `shakenfist.shakenfist` is how a play usually
  does.
- `auth_namespace` (optional) -- the identity the module authenticates
  *as* when it talks to the Shaken Fist API.

In every module of the `shakenfist.shakenfist` collection except
`sf_claim`, `namespace` *is* the identity -- there is only one
namespace-shaped idea in play, so one parameter names it. Here there
are genuinely two: an administrator routinely builds a cluster inside
a namespace whose key they do not hold (a CI namespace, a tenant's
namespace), authenticating instead as `system` or some other identity
with permission to act there. `namespace` could not be reused for
both meanings, so the identity needed its own name, and
`auth_namespace` is that name -- the same answer `sf_claim` already
gives to the same collision in the server collection. A reader
arriving from `sf_namespace`, `sf_instance` or any of the other
`shakenfist.shakenfist` modules should expect `namespace` to be the
authenticating identity there and should *not* assume the same here;
`auth_namespace` is the parameter this module uses for that instead.

## The connection-parameter rule: all three, or none

`api_url`, `auth_namespace` and `key` are supplied together or not at
all:

- Supply none of them, and the module auto-discovers credentials the
  way `sf-client` does -- from the environment, then `sfrc`,
  `~/.shakenfist` and `/etc/sf/shakenfist.json`. This is the normal
  way to call the module, letting the control node's existing
  configuration decide which cloud it talks to.
- Supply all three, and they are used verbatim, with discovery
  suppressed entirely. This is how an administrator points the module
  at a specific `api_url` while authenticating as an identity other
  than the one the control node would otherwise discover.
- Supply **some but not all three**, and the module fails rather than
  filling in the rest from discovery. The failure names which
  parameters were actually supplied.

That third case is deliberately a hard failure rather than a partial
fall-back, and the reasoning is the same reasoning
`shakenfist_client_k3s/client.py`'s `make_client()` gives: the values a
caller did pass would otherwise be silently discarded in favour of
whatever discovery found, which can be a different cloud entirely from
the one named in the task. A play built from templated variables --
`api_url` from one source, `key` from a vault that failed to decrypt
-- is exactly the caller most likely to end up with a partial set by
accident, and "pointed at the wrong cloud with no error" is a worse
failure mode for that caller than a loud one naming the problem. The
module's own translation of this adds one clause to the underlying
`ValueError`'s message, to say which of its own parameter names
corresponds to the library's `namespace` argument -- see
"The namespace the cluster lives in is not the identity you
authenticate as" above for why that mapping exists at all.

## Check mode

`sf_k3s_cluster` supports check mode (`supports_check_mode=True`) and
it is how this module's idempotency is meant to be verified: run a
play twice, the second time with `--check`, and a cluster which already
exists and has finished building reports `changed: false` -- whatever
its shape -- without touching anything. "Whatever its shape" is not a
hedge: shape is never compared, for the reason the next section gives.
Specifically:

- `state: present` against a cluster that does not exist reports
  `changed: true` in check mode, and creates nothing -- no instance,
  no network, no namespace metadata write.
- `state: present` against a cluster that already exists and finished
  building reports `changed: false`, identically in check mode and in
  a real run, whatever the cluster's current worker count or other
  shape is (see the next section).
- `state: absent` against a cluster that does not exist reports
  `changed: false` in both modes; against one that does exist it
  reports `changed: true` in check mode without deleting anything.

The client connection is still established in check mode -- building
it happens before the module branches on `check_mode` at all, so a bad
connection fails identically on a check-mode run and a real one,
rather than a play being told a check run would have succeeded with
credentials the real run would refuse.

## The module does not manage worker count

There is no `worker_count` parameter, and this is deliberate rather
than a gap: `sf_k3s_cluster` manages a cluster's *existence*, not its
*size*. `initial_workers` is the one exception, and it is scoped as
narrowly as its name says -- it is read only on the path that creates
a cluster, passed straight through to `Cluster.create()`'s
`worker_count` argument, and never read again. An existing cluster
whose worker count differs from `initial_workers` is not a change, is
not reported as a difference, and is never resized, in check mode or
otherwise.

The reason is a race, not a missing feature. Cluster state is a
single Shaken Fist namespace metadata document, read and rewritten
whole with no locking -- `docs/library-api.md` describes the same
cache-once-write-through behaviour from the library side. Worker
membership already has an owner in the deployments this module is
built for: a CI conductor scaling runners up and down as load demands,
adding and removing workers against that same document. If this
module also reconciled worker count on every run, two processes would
be writing the same unlocked document from two different ideas of
what it should contain, and whichever wrote last would win,
silently undoing the other's most recent change. `initial_workers`
avoids that by existing only once, at the one moment -- creation --
when there is no other writer yet: its default of `0` builds a
cluster with control plane nodes only, which is what a play that hands
the cluster straight to such a scaler wants, rather than the command
line's default of `2`.

Every other shape parameter the module takes -- `control_plane_count`,
`metal_address_count`, `network`, `release_channel`, `sshkey`,
`install_metallb`, `install_longhorn` and `manifests` -- is creation-time
only in the same sense, but for a simpler reason: none of them is a
verb this library has. There is no call that adds a control plane
node to a built cluster or swaps its network, so there is nothing for
the module to reconcile even if it wanted to. Workers are the one
singled out above because they are the one with a competing writer.

## What an interrupted cluster does

A cluster whose earlier `create()` run was interrupted partway through
-- the control node died, the task timed out, the play was cancelled
-- is neither "present" nor "absent" in any sense the module can
safely act on: its nodes, k3s node token and kubeconfig are in an
unknown combination of present and absent. Rather than guess, or
report a half-built cluster as a successfully present one,
`state: present` against an interrupted cluster **fails**, naming the
state the cluster was actually found in and that an earlier run was
interrupted while building it.

The remedy the failure message gives is the only one this module
supports: run the task again with `state: absent` to tear the
interrupted cluster down (this works on an interrupted cluster
specifically because it is the one state `state: absent` is built to
clear), then run `state: present` again to rebuild it from nothing.
There is no partial-resume path -- `docs/library-api.md`'s exception
table documents the same restriction at the library level, as a
deliberate choice rather than something this module works around.

## Secrets never come back in the module's output

The `health` return value never carries the cluster's raw namespace
metadata, and in particular never carries its kubeconfig, its k3s node
token, or any SSH key the cluster was built with. This is deliberate,
for the same reason `docs/library-api.md` gives for `Cluster`'s own
public surface: a return value flows into a play's registered
variables, and from there into whatever logs or stores them, and
secrets reaching that path by default is a worse failure mode than
having to ask for them on purpose. Fetch the kubeconfig with
`sf-client k3s getconfig` instead, outside the play, where it does not
pass through Ansible's own variable and logging machinery.

`log`, the other always-returned field, is the orchestration's
progress output collected rather than printed -- useful for debugging
a create that failed partway through -- and carries no more secret
material than the cluster's own progress announcements already do
(none; `verbose` is left off as a second line of defence, since a
`create()` or `delete()` run at debug level logs the whole metadata
document with its secret values replaced by `<redacted>`).

`msg` is covered too, which takes more than `no_log` to arrange. The
`key` parameter is `no_log`, so Ansible's own scrubbing removes it from
anything this module returns -- but the cluster's k3s node token is not
a module parameter, and an agent command has to carry it on its command
line for the k3s installer to join the right cluster. A worker install
which exits non-zero therefore used to put that command line, token and
all, into the failure message. The library now redacts secret
environment assignments inside the two exceptions which report a failed
agent command, so no raiser or caller has to remember; see
`progress.redact_command_line()`.

## See also

- `docs/library-api.md` -- the Python API this module is a thin
  translation over, including the full exception table, the reporter
  mechanism, and why the library itself refuses a partial connection.
- `docs/usage.md` -- the `sf-client k3s` command line, for the same
  verbs run interactively rather than from a playbook.
- `collection/README.md` -- the collection's own short-form install
  note, consistent with the fuller version above.
- `RELEASE-SETUP.md` -- the Galaxy token and namespace permission this
  collection's first release needs, which is the human gate blocking
  publication.
