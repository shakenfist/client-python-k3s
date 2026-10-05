# Audit findings: security review

Lens 6f of the phase 6 push audit. Scope is
`docs/plans/audit/scope-files.txt` with `docs/plans/` excluded, read as
the ten per-merge diffs beside it. Read-only: nothing in this review
changed a source file.

## Summary

| Action | Count |
|--------|-------|
| fix | 1 |
| document | 2 |
| consider | 7 |
| none | 3 |
| **total** | **13** |

The single `fix` is F1: the k3s node token is interpolated into an
agent command line, and that command line is rendered verbatim into
two exception messages. Those messages reach the terminal, the public
functional-CI job log, and -- new in phase 5 -- `fail_json(msg=...)`,
which puts the token into an Ansible play's registered variables. That
last path directly contradicts `docs/collection.md`'s "Secrets never
come back in the module's output".

Everything else is hardening, documentation, or informational. The
shell-quoting discipline in `cluster.py` is genuinely good: the two
rules at the top of the module are real rules, they are enforced by
tests, and the one place they do not reach (F2) is a gap in the rule's
*statement*, not a site somebody forgot.

## Taint traces

### Cluster names

**Where it enters.** `click.argument('name', type=click.STRING)` on
eight commands in `shakenfist_client_k3s/__init__.py` (lines 154, 248,
296, 310, 330, 428, 447, 462, 483, 498); `'name': {'required': True,
'type': 'str'}` in `collection/plugins/modules/sf_k3s_cluster.py:561`;
and `Cluster(client, name, namespace)` for any library caller.
**There is no validation anywhere on that path** -- the only
validating regex in the non-test source is
`K3S_MANIFEST_BASENAME_RE` (`cluster.py:149`), which is about manifest
filenames.

Sinks reached:

1. **Namespace metadata key** -- `METADATA_KEY % self.name` at
   `cluster.py:355`. Safe at the API layer (it is a JSON object key,
   not a path or a command), but see F5: two names collide exactly
   with the reserved version-cache keys in `primitives.py`.
2. **Shaken Fist network name** -- `'k3s-%s-node' % self.name` at
   `cluster.py:1519`, passed as an API parameter. Not a shell, not a
   path. Server-side validated, which is why a hostile name fails
   mid-create rather than doing anything (an argument-validation gap,
   adjacent to #96).
3. **Shaken Fist instance name** -- `'k3s-%s-node-%03d' %
   (md['name'], md['node_serial'])` at `cluster.py:518`, an API
   parameter. Same answer.
4. **An agent command line, indirectly** -- `remove_worker()` reads
   the instance name back off the API (`cluster.py:2316`) and builds
   `kubectl drain`, `kubectl delete node` and `kubectl uncordon` from
   it (lines 2361, 2367, 2452). **Safe, and provably so:** the value
   goes through `shlex.quote()` once at `cluster.py:2357` and only
   the quoted form is interpolated, including into `_uncordon()`,
   which takes it as a separate `quoted_node_name` parameter rather
   than re-quoting. `ShellQuotingTestCase.test_the_drained_node_name_is_quoted`
   (`tests/test_cluster.py:1973`) pins this with a name containing
   `;touch /pwned`.
5. **A `kubectl config` argument** -- `fqcn = '%s.%s' % (self.name,
   self.namespace)` at `cluster.py:1652` and `2112`, then
   `['kubectl', 'config', 'unset', config_elem]` at `cluster.py:2122`.
   **Safe from injection, and for the right reason:** an argument
   list with no `shell=True`, so no shell parses the value at all.
   The comment at `cluster.py:2116-2121` says exactly this and names
   the three entry points with no validation. Not safe from
   *confusion*: see F7.
6. **A kubeconfig YAML document** -- `kc['clusters'][0]['name']` and
   five siblings at `cluster.py:1653-1658`, serialised with
   `yaml.dump()`. Safe: the emitter quotes and escapes whatever it is
   given, so no name can break out of its scalar.
7. **A local filesystem path** -- **it reaches none.** This
   contradicts the "In this project" note under the path-traversal
   block, which says the cluster name reaches filenames. Every local
   path in the non-test source is `~/.kube`, `~/.kube/config`,
   `<tempdir>/config`, or a path the caller handed in whole
   (`--sshkey`, `--manifest`); none interpolates the name or the
   namespace. Verified by enumerating every `os.path.join`,
   `os.makedirs`, `open(`, `pathlib.Path` and `tempfile` call outside
   `tests/`.

### Release channel data from the k3s update API

**Where it enters.** `requests.request('GET',
'https://update.k3s.io/v1-release/channels')` at
`primitives.py:69`, parsed as JSON at line 79, and
`releases[reldata['name']] = reldata['latest']` at line 90.
`reldata['latest']` is an arbitrary third-party string. It is written
into namespace metadata at `primitives.py:102`, and read back from
there on a later run at line 105, so **namespace metadata is a second
entry point for the same value.** It becomes `md['k3s_version']`
(`cluster.py:1551`).

Sinks reached:

1. **An agent command line, twice** --
   `'INSTALL_K3S_CHANNEL=%s sh -s - server' %
   shlex.quote(md['k3s_version'])` at `cluster.py:1110-1112`, and the
   same value at `cluster.py:1156` inside
   `install_k3s_component()`'s installer pipeline. **Safe:** quoted
   at both sites, and `ShellQuotingTestCase.test_the_k3s_channel_is_quoted`
   (`tests/test_cluster.py:1962`) asserts the quoted form appears for
   a channel value of `v1.33; touch /pwned #`.
2. **An exception message** --
   `ReleaseLookupError.http_status(..., r.text)` at
   `primitives.py:76` and `exceptions.py:576`, which interpolates the
   *untruncated* response body. See F6.
3. **No file path and no YAML written to a guest.** The channel
   *name* the user asks for (`release_channel`) is only ever a
   dictionary key (`primitives.py:105`) and an error-message value
   (`exceptions.py:597`); it never reaches a command line.

### Tag names from the GitHub releases API

**Where it enters.** `requests.request('GET',
'https://api.github.com/repos/longhorn/longhorn/releases?page=N')` at
`primitives.py:137`, then `tagname =
reldata['tag_name'].lstrip('v')` at line 154.

Sinks reached:

1. **An agent command line** -- `'--version %s' %
   shlex.quote(version)` at `cluster.py:1311`. **Safe twice over,
   and the second reason is the stronger one:** the value is quoted,
   *and* it is not the raw tag. `primitives.py:163-176` parses each
   tag with `packaging.version.Version()`, skips anything
   unparsable, and stores `str(latest)` -- the PEP 440 *normalised*
   spelling, which cannot contain a character outside
   `[0-9a-z.!+*-]`. A tag of `1.0; rm -rf /` raises
   `InvalidVersion` and is skipped at line 164.
2. **Namespace metadata** -- `version_cache['releases']` keeps the raw
   tag strings as keys and `tarball_url` values
   (`primitives.py:155`, written at 178). Those are stored and never
   read again: only `version_cache['latest']` is returned (line 181).
   A hand-edited `latest` would be an arbitrary string, which lands
   at the `shlex.quote()` in sink 1 and is therefore still safe.
3. **No file path and no YAML written to a guest.**

### Namespace metadata (`orchestrated_k3s_cluster_*`)

**Where it enters.** `client.get_namespace_metadata()` at
`cluster.py:367` and `primitives.py:42`, `52`, `118`. Written by this
client, by conductor, and by anything holding the namespace's
credentials. `cluster.py:493-495` and `show()`'s docstring both
already say out loud that this document is editable by a third party.

Sinks reached:

1. **An agent command line, quoted** -- `md['k3s_version']`
   (`cluster.py:1112`, `1156`), `join_address` (`1157`), `token`
   (`1157`), `node_role` (`1158`). All through `shlex.quote()`.
   Safe.
2. **A YAML file written to a guest, UNQUOTED** --
   `md['api_address_floating']` into
   `/etc/rancher/k3s/config.yaml` at `cluster.py:1049-1055`, and
   `md['routed_addresses']` into
   `/etc/sf/metallb-range-allocation.yaml` at
   `cluster.py:1216-1232`. Both are heredoc bodies with a quoted
   delimiter, so the remote shell expands nothing inside them -- but
   a value containing a newline followed by a line that is exactly
   `EOF` ends the heredoc early, and the rest is shell. **This is
   the one sink in the whole review that is neither quoted,
   escaped, nor validated.** See F2.
3. **Instance and network names** -- `md['name']`,
   `md['node_serial']`, `md['node_network']` at `cluster.py:518-539`.
   API parameters, not shells or paths.
4. **Node sizes** -- `md['node_sizes']` through `_node_size()`
   (`cluster.py:502`). API parameters, and `validate_node_sizes()`
   covers the create path only; `_node_size()`'s per-field fallback
   is documented at `cluster.py:491-497` as being there precisely
   because the document is third-party writable.
5. **Progress output and debug output** -- the whole document is
   written line by line at debug level in `delete()`
   (`cluster.py:2009-2011`), including `node_token`, `server_token`,
   `kubeconfig` and `ssh_key`. See F4.
6. **Standard output** -- the whole document, unredacted, by
   `k3s show` (`__init__.py:321-323`). See F12.
7. **No local filesystem path.**

## Checklist questions answered

### "Review these changes as both a security reviewer and an experienced developer and correct any errors you find."

Reviewed; nothing corrected, because this step is read-only by
decision 4 of the phase plan. The thirteen findings below are what
6g has to triage. The three things worth saying as a reviewer rather
than as a list of defects:

- The two shell rules at `cluster.py:161-176` are the right shape:
  stated as rules rather than per-site judgements, enforced by
  `ShellQuotingTestCase` and `HeredocDelimiterTestCase`, and
  explicitly declining to treat the Shaken Fist API's own input
  validation as this package's trust boundary. That last point is
  the one most reviews get wrong, and it is right here.
- Phases 1-5 *removed* a live shell injection. Before `7fb29e5`,
  `delete` ran `subprocess.run('kubectl config unset %s' %
  config_elem, shell=True)` (pre-phase-1 `__init__.py:461-462`) with
  the cluster name interpolated into a shell string. It is now an
  argument list (`cluster.py:2122`). That is a real security
  improvement in this diff and should be recorded as such.
- The gap that remains is a *stated-rule* gap, not an
  implementation slip: rule 2 says a quoted delimiter means "the
  remote shell expands nothing inside the body", which is true and
  is not the whole of what a heredoc body can do. `read_manifests()`
  already knows this (it refuses a delimiter collision,
  `cluster.py:270`); the two config writes do not.

### "Are any user- or upstream-controlled values (cluster names, release channel data from the k3s update API, tag names from the GitHub releases API, namespace metadata) interpolated into agent command lines, file paths, or YAML written to guests without sanitization?"

**Agent command lines: no.** Every interpolated value at every site
goes through `shlex.quote()` -- `cluster.py:1105` (manifest
destination), `1112` (k3s channel), `1156-1158` (channel, join
address, token, role), `1311` (Longhorn version), `2357` (node name,
reused at 2364, 2368 and 2453). The only unquoted interpolations into
command strings are module literals (`K3S_MANIFEST_DIR`,
`K3S_MANIFEST_DELIMITER`, `KUBECTL_DRAIN_TIMEOUT`).

**File paths: no, and the stronger answer is that none is built.** No
local path interpolates any of the four values. The one remote path
built from an outside value is
`'%s/%s' % (K3S_MANIFEST_DIR, basename)` at `cluster.py:1105`, where
`basename` has already been forced to match
`^[A-Za-z0-9][A-Za-z0-9._-]*$`.

**YAML written to guests: yes, twice, and this is the finding.**
`cluster.py:1052` and `cluster.py:1224` interpolate namespace
metadata into heredoc bodies with no quoting, escaping or
validation. F2.

### "Do any changes leak secrets (node tokens, kubeconfigs, SSH keys) into logs, progress output, or commit history?"

**Commit history: no.** A scan of the scope for
`BEGIN (RSA|OPENSSH|EC|PRIVATE)`, `ssh-rsa AAAA`,
`client-key-data`, `api-key` and `password` found only test
placeholders (`SECRET-K3S-NODE-TOKEN` and friends in
`tests/module_harness.py:65-67`, `SECRET-SHAKENFIST-API-KEY` in
`tests/test_ansible_module.py:50`) and two synthetic public keys in
`tests/test_cluster.py:3253` and `:3261`. Nothing real.

**Logs: yes, three ways.** F1 (node token in two exception
messages, reaching stderr, the Ansible `msg` and the public CI job
log), F4 (the whole metadata document at debug level in `delete()`),
F12 (`k3s show` on stdout, by design).

**Progress output: no.** `Progress.phase()`, `note()`, `update()`
and `finish()` only ever receive values the call site built, and the
one that could carry a token does not: `await_idle()` passes
`primitives._describe_agent_op(aop)` with the default
`max_len=60` (`cluster.py:661`, used at `664` and `693`), and the
worker install command line reaches `K3S_TOKEN=` only at about
offset 66 at the earliest -- after
`curl -sfL https://get.k3s.io | ` (31 characters),
`INSTALL_K3S_CHANNEL=` (20), the quoted channel, and
` K3S_URL=https://<addr>:6443`. The truncation at
`primitives.py:207-208` therefore cuts before the token on every
reachable input. That is luck rather than design -- nothing states
or tests the margin -- but the margin is real.

### `key` in `sf_k3s_cluster.py`: is it `no_log: True`, and does anything re-emit it?

Yes, at `collection/plugins/modules/sf_k3s_cluster.py:582`
(`'key': {'required': False, 'type': 'str', 'no_log': True}`).
Nothing re-emits it, and I checked each of the four channels the
brief names:

- **`invocation`.** Covered, and *tested*:
  `test_the_authentication_key_is_scrubbed_from_the_invocation`
  (`tests/test_ansible_module.py:345`) asserts
  `invocation.module_args['key'] ==
  'VALUE_SPECIFIED_IN_NO_LOG_PARAMETER'`, and asserts the other two
  connection parameters come back verbatim so that the first
  assertion is a statement about `no_log` rather than about the key
  failing to arrive. It passes `_ansible_inject_invocation=True`
  deliberately, to pin the worst case across the supported
  ansible-core range rather than whichever version is installed.
- **Exception messages.** The only place `module.params['key']` is
  read is line 652, passing it to `make_client()`.
  `make_client()`'s `ValueError` (`client.py:52-55`) names *which*
  of the three parameters were supplied and interpolates none of
  their values, and the module passes that message through at line
  658-661 adding only a parenthetical about naming. The
  `UnconfiguredException` and `APIException` handlers (lines 663-692)
  interpolate `module.params['api_url']` and the exception, never
  the key.
- **`module.warn()`.** One call, at line 445, whose only
  interpolation is `%s` of a `health()` exception.
- **Debug output.** `make_client()` sets `'verbose': False`
  (`client.py:58`), so `apiclient` does not log the request headers
  the key travels in.

Belt and braces: `AnsibleModule.fail_json()` and `exit_json()` run
`remove_values(kwargs, self.no_log_values)` over the whole result, so
even a message that *did* contain the key would be scrubbed. That is
why F1 is about the node token and not the API key -- the node token
is not a module parameter, so it is not in `no_log_values` and
nothing scrubs it.

### `exceptions.py` (762 new lines): does any exception message interpolate a token, a key, or a kubeconfig?

Yes -- two, and only two, and both through the same indirection
rather than by naming a secret.

- `AgentOperationError.__str__` renders
  `'  command: %s' % self.command_description`
  (`exceptions.py:646-647`). `command_description` is
  `primitives._describe_agent_op(aop, max_len=None)`
  (`cluster.py:558`), i.e. the whole command line.
- `CommandFailedError.__str__` renders
  `'  command: %s' % self.commandline`
  (`exceptions.py:681`), built from
  `aop['commands'][0]['commandline']` (`cluster.py:762`).

Both become the node token whenever the failing command is the k3s
agent or extra-server install. F1. `AgentOperationError` also dumps
`json.dumps(self.results)` (`exceptions.py:649`), which is the
agent's captured stdout and stderr -- a secondary channel whose
contents depend on what `get.k3s.io`'s installer prints; I did not
confirm that it ever echoes the token, so the finding rests on the
command line.

**Confirmed later, during the pull request review of this phase**, so
the sentence above stands as what the lens knew and this is the answer
to it. The installer fetched from `https://get.k3s.io` on 2026-10-05
does not print the token in any form. It reads `K3S_TOKEN` once, to
test it for emptiness when `K3S_URL` is set (`:181`), and otherwise
only passes it through: `create_env_file()` writes the `K3S_*`
environment to `/etc/systemd/system/k3s.service.env` through
`tee ... >/dev/null`, after `chmod 0600`. Its four `set -x` calls are
all inside the killall and uninstall scripts it *generates*, not in its
own execution, and none of those scripts reads the token. So the
secondary channel is empty for this installer, and `K3S_TOKEN=` on the
command line was the whole of the leak. A future installer version
could change that, which is the argument for the redaction living in
the exception constructors rather than at any one raise site.

Everything else in the file is clean. The `__init__` methods of
`ManifestError`, `ReleaseLookupError` and `KubeconfigError` declare
their field unions explicitly (`exceptions.py:376-378`, `557-561`,
`723-724`) so `getattr` is total, and no classmethod interpolates
file *content* -- `ManifestError` carries paths, basenames, suffixes
and parser messages, never the manifest body.
`SshKeyError.unreadable()` carries the path and the `OSError` string,
not the key. `KubeconfigError.merge_failed()` and `unset_failed()`
carry `kubectl`'s stderr, which is an error string and not
certificate material. The one message that carries more third-party
text than it should is `ReleaseLookupError.http_status()` -- F6, and
not a secret.

### The `Progress` reporter and anything writing to stdout or stderr

`progress.py` writes only what its caller hands it. `Reporter.write()`
goes to `sys.stdout` (`progress.py:51`), `Reporter.debug()` writes
only when `verbose` (line 67), and `CollectingReporter` accumulates
instead (line 93). None of them inspects or formats a value, so every
leak question is a question about the call site. The call sites that
write a value derived from cluster state:

| Site | What it writes | Verdict |
|------|----------------|---------|
| `cluster.py:2009-2011` | whole metadata document, debug | **F4** |
| `cluster.py:1201-1206` | routed addresses | not secret |
| `cluster.py:1082-1085` | manifest basenames | not secret |
| `cluster.py:661-695` | `_describe_agent_op(aop)`, max_len 60 | truncates before the token; see above |
| `cluster.py:855` | instance uuid | not secret |
| `cluster.py:2052-2058` | routed address, network uuid | not secret |
| `cluster.py:2408-2413` | node name, cluster name, uuid | not secret |
| `cluster.py:2446-2458` | node name, uncordon exception | not secret |
| `__init__.py:321-323` | whole metadata document, stdout | **F12** |
| `__init__.py:124` | `str(e)` to stderr | **F1** |
| `__init__.py:377-421` | health report, incl. kubectl output | not secret |

### Path traversal: is each joined path *proved* to stay inside its base?

There are five path constructions in the non-test scope and none of
them can traverse, but only one of them says why.

| Path | Components | Proof |
|------|-----------|-------|
| `cluster.py:1676` `~/.kube` | `expanduser('~')` + literal | both process-chosen; **F9**, unexplained |
| `cluster.py:1677` `~/.kube/config` | the above + literal | same; **F9** |
| `cluster.py:1691` `<tempdir>/config` | `TemporaryDirectory()` + literal | same; **F9** |
| `cluster.py:236` `open(path)` | the caller's path, whole | no join, so no base to escape; the caller named the file it wants read |
| `cluster.py:1540` `open(sshkey)` | the caller's path, whole | same |
| `cluster.py:1105` `K3S_MANIFEST_DIR/basename` (remote) | literal + validated basename | `K3S_MANIFEST_BASENAME_RE` forbids `/` and any leading `.`, so neither `..` nor an absolute path can match; **F10**, the comment proves shell safety but not containment |

`os.path.realpath()` is not used anywhere, and for these six sites it
is not needed: three joins have only process-chosen components, two
are not joins at all, and the sixth is a *remote* path on a guest
filesystem, where a local `realpath()` would prove nothing. The
shared block's robust form matters when an untrusted component is
joined onto a local base and then opened; that shape does not occur
here. What the block *does* demand and this code does not supply is
the comment: the first three are correct-but-unexplained joins, which
the block calls a `document` finding outright.

The one thing that would change this answer is a future change that
puts the cluster name into a filename -- a per-cluster kubeconfig
cache, say. There is nothing today.

### The `kubectl config` invocations, including `unset` in the delete path

Three invocations, all reviewed:

- `cluster.py:1694-1697`, `kubectl config view --flatten` with
  `shell=True`. The command string is a module literal with no
  interpolation, so there is nothing for a shell to find. The two
  paths reach it through `env={**os.environ, 'KUBECONFIG': ...}`, as
  environment values rather than as argv, so even a path with a
  space or a quote in it (an unusual `$HOME`) cannot be
  re-parsed. Safe -- but `shell=True` for a constant is against the
  module's own third rule at `cluster.py:175-176`. **F8.**
- `cluster.py:2122-2124`, `['kubectl', 'config', 'unset',
  config_elem]`, three times in the delete path. Argument list, no
  shell. **Safe from injection.** `config_elem` is
  `'users.' + name + '.' + namespace` and friends, and the comment at
  2116-2121 states both the risk and why the argument list answers
  it. Not safe from kubectl's own dot-separated path grammar: **F7.**
- `cluster.py:1686`, `shutil.which('kubectl')`. A literal.

### Secrets reaching committed files

Nothing. See the third checklist answer above.

### Anything looked for and not found

Stated explicitly so the absences are on the record: no
`yaml.load()` without `safe_` (both loads are `yaml.safe_load`,
`cluster.py:1651` and `1712`, and the manifest parse is
`yaml.safe_load_all`, line 266); no `eval`, `exec`, `os.system`, or
`pickle` anywhere in the scope; no `verify=False` or other TLS
verification override on either `requests` call; no archive
extraction of any kind, so the case the shared block calls the most
often missed does not arise; no `tarfile`, `zipfile` or
`shutil.unpack_archive`; no temporary file created with a predictable
name (`tempfile.TemporaryDirectory()` is the only use, and it is
0700); no secret interpolated into a workflow `run:` script body
(`release.yml:274-279` routes `ANSIBLE_GALAXY_TOKEN` through `env:`
and says why at 265-272); and no `${{ }}` expansion of untrusted
GitHub context into a shell step in either workflow.

## Findings

### F1: The k3s node token is rendered into two exception messages, and so reaches stderr, the Ansible result, and the public CI log

- **File**: `shakenfist_client_k3s/cluster.py:1155`, with the
  renderings at `shakenfist_client_k3s/exceptions.py:647` and
  `shakenfist_client_k3s/exceptions.py:681`, raised from
  `cluster.py:558` and `cluster.py:762`, and reaching Ansible at
  `collection/plugins/modules/sf_k3s_cluster.py:734`
- **Action**: fix
- **Severity**: Discloses the cluster's k3s node registration token,
  and on the extra-control-plane path the *server* token, to whoever
  can read the error output. With a node token an attacker registers
  an agent into the cluster, which is scheduling rights on it; with a
  server token they join a control plane node, which is cluster
  admin. What the attacker must already have is read access to one
  of three sinks, and they are not equally hard:
  - the terminal of whoever ran the command -- no gain, they own
    the cluster;
  - **the Ansible job output**, which is a materially wider
    audience than the namespace's credential holders: `msg` flows
    into registered variables, callback plugins, syslog, and an
    AWX/Tower job view that is routinely readable by people with no
    Shaken Fist credentials at all;
  - **the functional-CI job log**, which
    `tools/ci_deploy_test.sh:42-44` states is "visible to anyone who
    can see the repository" -- a public repository. The token is live
    until the job's namespace dies with the runner, and the log is
    permanent.

  Triggering it needs no privilege: any worker install that exits
  non-zero does it. A transient `apt` failure is enough.
- **Claim**: `install_k3s_component()` builds a command line
  containing `K3S_TOKEN=<token>`, and both exception classes that
  report a failed agent command render that command line verbatim,
  so a failed k3s install publishes the token wherever the error
  goes.
- **Evidence**: `cluster.py:1152-1159` builds
  `'curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=%s
  K3S_URL=https://%s:6443 K3S_TOKEN=%s sh -s - %s'` with
  `shlex.quote(token)` as the third value -- correctly quoted for
  the shell, which is a separate question from whether it should be
  printed. `install_workers()` passes `md['node_token']`
  (`cluster.py:1184`); `install_extra_control_plane()` passes
  `md['server_token']` (`cluster.py:1170`).

  The API echoes the submitted command line back in
  `commands[0]['commandline']` (confirmed against the fakes at
  `tests/fakes.py:170` and `:282`). `reap_execute()` reads exactly
  that field into `CommandFailedError` on a non-zero return code
  (`cluster.py:758-765`), and `CommandFailedError.__str__` prints
  `'  command: %s' % self.commandline` (`exceptions.py:681`).
  `_agent_op_error()` takes the same text by way of
  `primitives._describe_agent_op(aop, max_len=None)`
  (`cluster.py:558`) -- the `max_len=None` is what matters, because
  the default 60 would truncate before the token -- and
  `AgentOperationError.__str__` prints it at `exceptions.py:647`.

  From there: `GroupCatchClusterExceptions.invoke()` prints `str(e)`
  to stderr (`__init__.py:124`), and
  `sf_k3s_cluster.py:732-735` puts `'%s %s' % (e,
  mutation.advice())` into `fail_json(msg=...)`. The node token is
  not a module parameter, so it is not in `no_log_values` and
  Ansible's `remove_values()` does not scrub it.

  Two things make this a finding about *this* work rather than a
  pre-existing one. First, `docs/collection.md:275-294` asserts
  "Secrets never come back in the module's output" and explains at
  length why `verbose` is left off; the `fail_json()` path added in
  phase 5 falsifies the heading. `SecretsTestCase`
  (`tests/test_ansible_module.py:314-343`) tests the delete and
  health paths for exactly these three secrets and has no case for
  the failure path, so the gap is a missing member of an existing
  test class. Second, phase 1 added `AgentOperationError`, a second
  rendering site that the existing note in
  `docs/plans/PLAN-functional-ci.md:199-202` -- which records the
  `reap_execute()` half as future work -- does not cover.
- **Proposed change**: redact at the boundary rather than at each
  call site, so a future command carrying a credential is covered
  without anyone remembering. Give `primitives._describe_agent_op()`
  a substitution over the environment-variable assignments that
  carry secrets (`K3S_TOKEN=`, and anything else added later) and
  apply the same substitution to the `commandline` that
  `reap_execute()` hands `CommandFailedError`; both exceptions then
  render `K3S_TOKEN=<redacted>`. Keep the quoting as it is -- the
  shell side is correct. Add the missing `SecretsTestCase` member:
  drive a create whose worker install returns non-zero and assert
  `module_harness.SECRET_NODE_TOKEN` is absent from `run.stdout`,
  which is the test that would have caught this and would catch the
  next rendering site. Then correct the claim in
  `docs/collection.md` if any residual path remains, and close out
  the `PLAN-functional-ci.md` future-work bullet, which this
  supersedes.

### F2: Namespace metadata is interpolated unquoted into two heredoc bodies, which a newline plus a delimiter line turns into a root shell

- **File**: `shakenfist_client_k3s/cluster.py:1049-1055` and
  `shakenfist_client_k3s/cluster.py:1216-1232`
- **Action**: consider
- **Severity**: Arbitrary command execution as root on the first
  control plane node. What the attacker must already have is the
  ability to put a newline into `md['api_address_floating']` or into
  an element of `md['routed_addresses']`, which means either
  controlling the Shaken Fist API's response for an instance's
  interfaces or a routed address, or write access to the namespace
  metadata document. **A principal holding the namespace's
  credentials gains nothing**, because they can already call
  `instance_execute()` on those instances directly -- which is why
  this is a defence-in-depth gap and not an escalation. It becomes a
  real escalation only where the writer of the metadata and the
  owner of the nodes differ: conductor writing into a document a
  tenant's run then reads, or a tenant-poisoned document read by an
  operator's `--namespace` run. Those are the cases rule 1 at
  `cluster.py:166-169` already says it refuses to assume away ("a
  remote API's input validation is not this package's trust
  boundary").
- **Claim**: Rule 2 at `cluster.py:170-173` guarantees that a quoted
  heredoc delimiter stops the remote shell expanding the body, which
  is true and is not sufficient: it does not stop an interpolated
  value from *ending* the heredoc, and these are the only two
  heredocs whose bodies are not checked for a delimiter collision.
- **Evidence**: `cluster.py:1048-1055` emits
  `"cat - > /etc/rancher/k3s/config.yaml << 'EOF'\n...  - \"%s\"\n...EOF\n"
  % md['api_address_floating']`, and `cluster.py:1216-1232` emits
  the metallb pool with
  `% '/32\n  - '.join(md['routed_addresses'])`. Neither value is
  quoted, escaped or pattern-checked anywhere between the API
  response and the heredoc.

  The input shape is a value whose text contains a newline, then a
  line that is exactly `EOF`, then the commands to run, then a fresh
  `cat > /dev/null << 'EOF'` so that the template's own trailing
  lines become a second heredoc body and the whole command string
  still parses. The agent's requirement that the first token be an
  executable (noted at `cluster.py:1274-1276`) is satisfied, because
  the first token is still `cat`. I have not written or run this.

  That the project already understands the mechanism is the point:
  `read_manifests()` refuses a manifest containing a line equal to
  the delimiter (`cluster.py:270-272`,
  `ManifestError.delimiter_collision`), and the comment at
  `cluster.py:151-158` explains exactly why. The protection was
  built for caller-supplied manifest content and not extended to
  the two heredocs that carry metadata. `HeredocDelimiterTestCase`
  (`tests/test_cluster.py:2092-2117`) asserts only that the
  delimiter is quoted, so nothing in the suite looks at the body.
- **Proposed change**: two parts, and the second is the one that
  lasts. Refuse the value: reject any interpolated heredoc value
  containing a newline, which for an IP address is a check that can
  never fire in normal operation -- or validate these two as
  addresses outright, since that is what they are. Then widen rule 2
  and its test: state in the comment at `cluster.py:170-173` that a
  heredoc body carrying interpolated content must also be unable to
  contain the delimiter line, and extend `HeredocDelimiterTestCase`
  with a case that interpolates a hostile address and asserts the
  generated body has no line equal to the delimiter. Reusing
  `ManifestError.delimiter_collision`'s check as a shared helper
  would make it one mechanism rather than two.

### F3: A newly created `~/.kube/config` is world-readable, and so is `~/.kube`

- **File**: `shakenfist_client_k3s/cluster.py:1678` and
  `shakenfist_client_k3s/cluster.py:1683`
- **Action**: consider
- **Severity**: Any local user on the machine that ran
  `k3s create` reads the cluster's admin credentials. A k3s
  kubeconfig embeds `client-certificate-data` and `client-key-data`
  for a `cluster-admin` identity, so this is full control of the
  cluster, not merely read access. The attacker must already have a
  shell account on the same host -- which is the normal situation on
  a shared jump box or a CI runner, and the normal situation is the
  one that matters for a file-mode finding.
- **Claim**: `os.makedirs()` and `open(..., 'w')` take the process
  umask, so on a default `umask 022` host the directory is created
  0755 and the new kubeconfig 0644.
- **Evidence**: `cluster.py:1678` is
  `os.makedirs(kube_dir, exist_ok=True)` with no `mode`, and
  `cluster.py:1683` is
  `with open(main_config_path, 'w', encoding='utf-8') as f`, on the
  branch taken when `main_config_path` does not already exist
  (guarded at line 1680). Nothing calls `os.chmod()` or `os.umask()`
  anywhere in the package.

  Scope is precisely the create-from-nothing branch. The merge
  branch's temporary copy at `cluster.py:1692` is inside
  `tempfile.TemporaryDirectory()`, which is 0700, so it is already
  protected; and the rewrite at `cluster.py:1714` truncates a file
  that already exists, which preserves whatever mode the user's own
  kubeconfig had. So only line 1683 creates a file, and only it
  chooses the mode.

  Pre-existing rather than introduced: the same two lines are at
  `__init__.py:239-246` before `7fb29e5`, without the
  `encoding='utf-8'` that phase 1 added. It is in scope because the
  diff touched the lines, and it is cheap, which is why it is
  `consider` and not `none`. Note that `cluster.py:1050` separately
  sets `write-kubeconfig-mode: "0644"` for the *node's*
  `/etc/rancher/k3s/k3s.yaml`; that is a different file with a
  different threat model and is not this finding.
- **Proposed change**: create the directory at 0700 and the file at
  0600 -- `os.makedirs(kube_dir, mode=0o700, exist_ok=True)`, and
  write through `os.open(main_config_path, os.O_WRONLY | os.O_CREAT
  | os.O_TRUNC, 0o600)` so the mode is set at creation rather than
  chmod'd after a window in which the file was readable. Note in the
  comment that the merge branch inherits the existing file's mode by
  design, so a user who has already tightened theirs keeps it.

### F4: `delete()` writes the whole metadata document -- tokens, kubeconfig and SSH key -- at debug level

- **File**: `shakenfist_client_k3s/cluster.py:2009-2011`
- **Action**: consider
- **Severity**: `sf-client k3s delete -v` prints the cluster's node
  token, server token, complete admin kubeconfig and any SSH key it
  was built with to stdout, where a shell's scrollback, a `tee`, a
  `script` session or a CI log keeps them. The attacker must be able
  to read that output, and the credentials belong to the person who
  ran the command -- so the direct disclosure is self-inflicted. The
  reason it still matters is secondary exposure: verbose output is
  what people paste into bug reports, and `-v` is exactly the flag
  somebody adds when a delete is failing.
- **Claim**: The loop dumps every metadata key unconditionally, with
  no redaction of the four keys the project elsewhere identifies as
  secret.
- **Evidence**: `cluster.py:2009-2011` is
  `self.reporter.debug('Cluster metadata:')` followed by
  `for k in md: self.reporter.debug('    %s = %s' % (k, md[k]))`.
  `md` at that point still holds `node_token`, `server_token`,
  `kubeconfig` and `ssh_key`; the clearing at `cluster.py:2044-2045`
  happens afterwards, and clears `node_token` and `kubeconfig` but
  not `server_token` or `ssh_key`. `Reporter.debug()` writes to
  `sys.stdout` when `verbose` (`progress.py:65-69`), and `verbose`
  comes from the parent CLI's flag via
  `ctx.obj.get('VERBOSE', False)` (`__init__.py:50`).

  This is known and is load-bearing elsewhere, which is the
  argument for fixing it rather than for leaving it:
  `sf_k3s_cluster.py:636-641` leaves the module's reporter
  non-verbose and calls that "a security property rather than a
  volume preference", citing this exact loop;
  `docs/collection.md:291-294` repeats the reasoning; and
  `SecretsTestCase.test_no_cluster_secret_reaches_the_result_of_a_delete`
  (`tests/test_ansible_module.py:326`) exists only because of it. A
  security property that holds because one caller remembered to
  leave a flag alone is one flag away from not holding. Pre-existing
  (`__init__.py:388` before `7fb29e5`).
- **Proposed change**: redact in the loop rather than relying on
  callers: keep a module-level set of secret metadata keys
  (`node_token`, `server_token`, `kubeconfig`, `ssh_key`) and print
  `<redacted>` for those, leaving everything else as it is. That
  makes `-v` safe, makes the Ansible module's non-verbose reporter a
  preference again rather than a security control, and gives
  `k3s show` (F12) the same set to use when it grows redaction.

### F5: Two cluster names collide exactly with the reserved version-cache metadata keys

- **File**: `shakenfist_client_k3s/cluster.py:49` with
  `shakenfist_client_k3s/primitives.py:30-31`
- **Action**: consider
- **Severity**: Low, and self-inflicted through the CLI. A user who
  can name a cluster in their own namespace -- which is every user
  -- gets, for two specific names, a create refused with a message
  about an interrupted build that never happened, and a delete that
  ends in an unhandled `KeyError` traceback outside the exception
  hierarchy. No secret is disclosed and no resource is lost. A
  *library* caller is worse off: calling `set_metadata()` directly
  on such a name overwrites the namespace's shared version cache,
  and the reverse write at `primitives.py:102` would overwrite the
  cluster document.
- **Claim**: The metadata key is `'orchestrated_k3s_cluster_%s' %
  name` with no validation of `name`, and the two version caches use
  literal keys that are in that same namespace of strings, so the
  names `k3s_version_cache` and `longhorn_version_cache` produce
  byte-identical keys.
- **Evidence**: `cluster.py:49` is
  `METADATA_KEY = 'orchestrated_k3s_cluster_%s'`, used by
  `_metadata_key()` at `cluster.py:355`.
  `primitives.py:30-31` are
  `K3S_VERSION_CACHE_KEY = 'orchestrated_k3s_cluster_k3s_version_cache'`
  and
  `LONGHORN_VERSION_CACHE_KEY = 'orchestrated_k3s_cluster_longhorn_version_cache'`.
  Confirmed with the interpreter that both collide and that
  `CLUSTER_LIST` (`orchestrated_k3s_clusters`) does not.

  The CLI consequence, traced: `create()` calls `get_k3s_release()`
  first (`cluster.py:1481`), which writes the cache if it is absent
  or stale, so by the time `self.get_metadata()` runs at
  `cluster.py:1489` the key exists and holds the cache document.
  That is truthy, `_interrupted_state()` finds no `state` key and
  answers `'unknown'` (`cluster.py:413`), and create raises
  `ClusterInterruptedError.mid_create` -- so the name is
  permanently unusable and the explanation is wrong. `delete()` on
  the same name reaches `md['control_plane_nodes']` at
  `cluster.py:2015` and raises `KeyError`, which
  `GroupCatchClusterExceptions` does not catch, so the user sees a
  traceback. There is no name validation anywhere to prevent it: the
  only validating regex in the non-test source is
  `K3S_MANIFEST_BASENAME_RE`.
- **Proposed change**: the narrow fix is to refuse the two reserved
  names where the key is derived, in `_metadata_key()`, with an
  exception from the hierarchy. The better fix is the one #96 is
  already about: validate the cluster name once, on the way in,
  against something conservative enough to also satisfy the Shaken
  Fist instance-name guard (letters, digits and hyphens), which
  excludes underscores and so makes both collisions unreachable as
  a side effect -- and turns every "hostile name fails mid-create
  after the network is allocated" case into an argument error. Worth
  an occurrence comment on #96 rather than a separate issue.

### F6: `ReleaseLookupError.http_status()` interpolates the untruncated third-party response body into a message

- **File**: `shakenfist_client_k3s/exceptions.py:576-585`, called
  from `shakenfist_client_k3s/primitives.py:76-77` and
  `shakenfist_client_k3s/primitives.py:145-146`
- **Action**: consider
- **Severity**: Low. Whoever controls the HTTP response from
  `update.k3s.io` or `api.github.com` -- the upstream itself, or a
  proxy holding a certificate the client trusts -- decides how many
  bytes, and which bytes, are written to the user's terminal and
  into an Ansible `msg`. The practical harms are terminal escape
  sequences rendered by the user's terminal emulator and an
  unbounded error message in a log; not code execution. An
  unauthenticated third party on the network cannot reach it,
  because both calls are HTTPS with verification left on.
- **Claim**: The message embeds `r.text` whole, where the sibling
  classmethod in the same class truncates deliberately.
- **Evidence**: `exceptions.py:576-585` interpolates
  `response_text` with no bound, and
  `primitives.py:77` and `:146` pass `r.text`. Two lines below,
  `no_usable_k3s_channels()` (`exceptions.py:587`) documents its
  argument as "the caller's already-truncated
  `json.dumps(d)[:512]`" and `primitives.py:98` does truncate. The
  class is handling the same kind of value two different ways.
- **Proposed change**: truncate at the call sites to match the
  sibling -- `r.text[:512]` at `primitives.py:77` and `:146` -- and
  say in the `http_status()` docstring that the body is bounded
  because it is third-party text. If terminal escapes are worth
  defending against more generally, that is a change to the reporter
  and belongs in its own issue rather than here.

### F7: A cluster name or namespace containing a dot breaks `kubectl config unset` and leaves the delete reporting failure

- **File**: `shakenfist_client_k3s/cluster.py:2112-2124`
- **Action**: consider
- **Severity**: Not an injection -- the argument list rules that out.
  A user who can name a cluster in their own namespace gets a delete
  that destroys everything correctly and then fails at its last
  step, with stale entries stranded in `~/.kube/config` and no
  recovery, because the metadata document is already gone by then so
  re-running the delete raises `ClusterNotFoundError`. Low, and
  reachable by accident: `my.cluster` is a name somebody will type.
- **Claim**: `kubectl config unset` takes a dot-separated path into
  the config structure, so a name containing a dot produces a path
  with an extra segment that kubectl cannot resolve, and the
  resulting non-zero exit is turned into a raised `KubeconfigError`.
- **Evidence**: `cluster.py:2112` builds
  `fqcn = '%s.%s' % (self.name, self.namespace)` and lines 2113-2115
  build `users.<fqcn>`, `contexts.<fqcn>`, `clusters.<fqcn>`.
  `kubectl config unset` resolves that string as a path -- `users`
  is a map, the next segment is the key, and any further segment is
  a field of the resulting struct -- so `users.my.cluster.ns` looks
  for a field `cluster` on an `AuthInfo` and errors.
  `cluster.py:2136-2146` treats a non-zero return code as fatal and
  raises `KubeconfigError.unset_failed`. By that point
  `self.delete_metadata()` has already run (`cluster.py:2106`), and
  the cleanup is the last thing the method does, so the cluster is
  gone and the error is unrecoverable by re-running. The same `fqcn`
  is written into the kubeconfig at `cluster.py:1652-1658`, where
  YAML quoting makes it harmless, so the mismatch is only in the
  `unset` grammar.
- **Proposed change**: escape the dots kubectl's grammar reserves --
  the documented form is a backslash before a literal dot in a path
  segment -- or, preferably, fix it where F5's fix goes: a
  conservative cluster-name validator excludes dots, and the
  namespace is already constrained by Shaken Fist. Either way, this
  is worth stating in a comment next to the argument-list comment
  that is already there, because the next reader will reasonably
  conclude from that comment that the name needs no further
  thought.

### F8: `kubectl config view --flatten` runs with `shell=True` where an argument list is available

- **File**: `shakenfist_client_k3s/cluster.py:1694-1697`
- **Action**: consider
- **Severity**: None today. The command string is a module literal
  with no interpolation, and the two paths travel as environment
  values rather than as argv, so there is nothing a shell can
  re-parse. This is a hardening and consistency finding, not a
  vulnerability.
- **Claim**: The third of the module's own shell rules says that
  "where a real argument list is available, it is used instead", and
  one is available here.
- **Evidence**: `cluster.py:1695` is
  `'kubectl config view --flatten', shell=True`. The rule is at
  `cluster.py:175-176`, and `cluster.py:2122` -- the other
  `subprocess` call in the file -- follows it. A reader comparing
  the two sites has to work out for themselves that the difference
  does not matter, which is the cost. It also spawns a shell for
  nothing.
- **Proposed change**: `['kubectl', 'config', 'view', '--flatten']`
  with `shell=True` dropped. Behaviour is identical, the two call
  sites then agree, and the rule at 175-176 becomes true without
  exception.

### F9: The three `~/.kube` path joins are correct for reasons nothing states

- **File**: `shakenfist_client_k3s/cluster.py:1676`,
  `shakenfist_client_k3s/cluster.py:1677`,
  `shakenfist_client_k3s/cluster.py:1691`
- **Action**: document
- **Severity**: None. All three joins have only process-chosen
  components and cannot escape anything.
- **Claim**: The `path-traversal-review` block's last clause says
  that where a bare join is correct because every component is
  process-chosen, that should be said in a comment rather than left
  for the reader to re-derive. These three say nothing.
- **Evidence**: `cluster.py:1676-1677` join
  `os.path.expanduser('~')` with the literals `.kube` and `config`;
  `cluster.py:1691` joins `tempfile.TemporaryDirectory()`'s path
  with the literal `config`. No outside value appears in any of
  them, and there is no `realpath()` guard -- correctly, because
  there is no untrusted component to guard. The surrounding comments
  (lines 1669-1673, 1680-1682, 1708-1711) cover why the write is
  gated, why the no-merge branch needs no kubectl, and why
  `current-context` is reset; none mentions the paths.
- **Proposed change**: one sentence above line 1676 saying that
  every component of these paths is chosen by this module -- the
  user's home directory, a fixed directory name, a fixed file name,
  and a `tempfile` directory -- so the joins need no containment
  check, and that an outside value appearing in one later would.
  This is a comment only; the code is right.

### F10: The manifest destination's containment is proved by the basename regex, but the comment only claims shell safety

- **File**: `shakenfist_client_k3s/cluster.py:138-149` with
  `shakenfist_client_k3s/cluster.py:1105`
- **Action**: document
- **Severity**: None. `K3S_MANIFEST_BASENAME_RE` forbids `/` and any
  leading `.`, so no accepted basename can contain a path separator
  or be `.` or `..`, and the join cannot leave
  `/var/lib/rancher/k3s/server/manifests`.
- **Claim**: The containment argument is sound but is nowhere
  written down. The long comment at 138-149 explains the regex as a
  defence against *shell* metacharacters in a filename, and the
  comment at 1094-1102 explains the quoting; neither says that the
  regex is also what keeps the remote path inside its directory,
  which is the question the shared block asks. A reader tightening
  or relaxing that character class for readability reasons would not
  know they were also changing a traversal guard.
- **Evidence**: `cluster.py:149` is
  `re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]*$')`, checked at
  `cluster.py:215` against `os.path.basename(path)`.
  `cluster.py:1105` builds `'%s/%s' % (K3S_MANIFEST_DIR, basename)`
  and quotes it. The comment at 138-149 covers the suffix check, the
  shell risk with
  `os.path.basename('/tmp/a;touch /pwned.yaml')` as its worked
  example, the log-readability argument, and the leading-character
  restriction -- but frames that last one as "cannot begin with a
  dot or a hyphen" rather than as "cannot be `..`".
- **Proposed change**: add a clause to the existing comment saying
  that the character class is also the containment proof for the
  join at line 1105 -- no `/`, so no path separator; no leading dot,
  so not `..` -- and that relaxing it would need that join
  reconsidered. Comment only.

### F11: The two release lookups have no HTTP timeout

- **File**: `shakenfist_client_k3s/primitives.py:69-74` and
  `shakenfist_client_k3s/primitives.py:137-142`
- **Action**: none
- **Severity**: Availability only, and only for the caller's own
  process. A host that accepts the connection and never answers
  hangs `k3s create` before anything is built, with no output. No
  disclosure, no execution, nothing left behind.
- **Claim**: Neither `requests.request()` call passes `timeout`, so
  both default to waiting indefinitely.
- **Evidence**: `primitives.py:69-74` and `:137-142` pass only
  `headers`. Noted here rather than filed because it is robustness
  rather than security, and because it is the kind of thing that
  disappears if nobody writes it down.
- **Proposed change**: a `timeout=` on both calls. One line each.
  Mention it in 6g's triage table as declined-or-taken rather than
  treating it as a security fix.

### F12: `k3s show` prints cluster-admin credentials to stdout

- **File**: `shakenfist_client_k3s/__init__.py:321-323`
- **Action**: none
- **Severity**: The output contains the node token, the server
  token, the complete admin kubeconfig and any SSH key. It goes to
  the person who asked for it, about a cluster they own, and
  `getconfig` hands them the kubeconfig anyway, so there is no
  privilege gain. The exposure is secondary: shell history,
  scrollback, and pasted bug reports.
- **Claim**: Informational, and already recorded in two places, so
  this review adds an occurrence rather than a finding.
- **Evidence**: `__init__.py:321-323` prints every metadata key.
  `docs/usage.md:295-297` warns the reader in terms ("the output is
  cluster-admin credentials. Do not paste it into a bug report"),
  `tools/ci_deploy_test.sh:39-49` deliberately does *not* dump
  `k3s show` in its failure diagnostics and explains why, and
  `docs/plans/PLAN-functional-ci.md:194-198` records the intended
  fix as future work (teach `show` to honour `--json` and redact
  `node_token`, `kubeconfig`, `ssh_key`). Note that the recorded key
  list omits `server_token`, which the metadata also holds.
- **Proposed change**: nothing in this phase. When the recorded
  future work is taken, it should share the secret-key set F4
  proposes, and should include `server_token`.

### F13: The Galaxy token is passed on `ansible-galaxy`'s command line

- **File**: `.github/workflows/release.yml:274-279`
- **Action**: none
- **Severity**: The token is visible in `/proc/<pid>/cmdline` to
  other processes on the self-hosted runner for the life of the
  publish. The attacker must already be able to run code on that
  runner during that window, at which point they have the whole
  release environment.
- **Claim**: Informational. The step is already doing the harder
  half correctly, and `ansible-galaxy collection publish` offers no
  environment-variable equivalent to `--api-key`; the alternative is
  writing the token to a config file, which is not obviously better.
- **Evidence**: `release.yml:274-275` brings the secret in through
  `env:` and the comment at 265-272 explains that interpolating
  `${{ }}` into the script body would write it into the generated
  script file on a persistent self-hosted disk. Line 279 then passes
  `--api-key "${ANSIBLE_GALAXY_TOKEN}"`.
- **Proposed change**: none. Recorded so that the decision is
  visible rather than accidental.
