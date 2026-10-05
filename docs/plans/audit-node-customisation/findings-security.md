# Findings: security (step 4e)

Returned as text by the sub-agent and saved here by the management
session, because the harness does not let sub-agents write report files.
The management session spot-checked S-1, S-2 and S-4 against the
worktree before saving. The snippets the lens ran (`t1.py`, `t2.py`,
`t3.py`) and the k3s sources it fetched stayed in the session scratchpad
and are not part of this record.

Lens: `PUSH-AUDIT.md`'s *Security review* section and the
`path-traversal-review v1` block, over the files in `scope-files.txt`,
with the threat model in step 4e of
`PLAN-node-customisation-phase-04-push-audit.md`.

## What was read, and at which revision

* The worktree at `7f4efb1`, which is `3e84907` plus two plan-only
  commits. No file outside `docs/plans/` differs from `3e84907`. Line
  numbers below are at that revision.
* `git diff <m>^1 <m>` for `ddb1f3b`, `b791364` and `51ff6f4`, to locate
  what the plan added; then the current files.
* `2aba0e6`'s commit message. Its fixes are not re-raised: the exception
  base, `heredoc()` (quoting the path and refusing a body that ends its
  own heredoc), and the hostile `api_address_floating` case.
* `shakenfist_client_k3s/cluster.py`: the two shell rules (lines 278-299)
  and `heredoc()`; `validate_k3s_config()`, `read_k3s_config()` and
  `check_k3s_release()`; `_k3s_config_commands()` and the install
  methods; `create()`'s validation, metadata and credential fetch;
  `show()` and `delete()`.
* `exceptions.py` (`K3sConfigError`, `UnsupportedReleaseError`),
  `progress.py` (the redaction helpers), `__init__.py` (`create` and
  `show`), `tools/ci_deploy_test.sh`, `docs/usage.md`,
  `docs/library-api.md`, and the collection module's failure path.
* k3s upstream, fetched read-only with `gh api` at `k3s-io/k3s` `main`
  `bdb2a3e` (2026-10-02): `pkg/configfilearg/parser.go`,
  `pkg/cli/cmds/{server,agent,config}.go`, `pkg/server/server.go` and
  `pkg/daemons/config/types.go`; also `k3s-io/docs` `docs/cli/token.md`.
  These answer the "which keys defeat the plugin" questions. Older k3s
  releases down to the `v1.21.1` floor were not checked, and may differ.
* Snippets were run with the worktree's `.tox/py3` interpreter (PyYAML
  6.0.3, Python 3.13), from the session scratchpad.

## Summary of ratings

| Id | Rating | One line |
|---|---|---|
| S-1 | consider | The owned-key refusal is bypassed by k3s's one-letter aliases (`t`, `d`, `s`, `o`) and by a key containing `=` |
| S-2 | consider | A caller's `bind-address` silently makes the kubeconfig `getconfig` hands out point at the wrong host |
| S-3 | document | The refused-key list guards against mistakes. It is not a trust boundary, and the docs suggest uses where it would be taken for one |
| S-4 | document | Caller config is stored and shown in clear: `k3s show`, unredacted `delete -v`, a 0644-by-umask file on each node |
| S-5 | consider | YAML alias amplification: a 350-byte file costs 80 s and 175 MB, through a public function |
| S-6 | none | A CLI config path can be a FIFO, a device or a symlink. Self-inflicted only |
| S-7 | none | Error messages echo a fragment of the config file, or a refused value |

Nothing found lets caller YAML reach the guest shell unquoted, or end the
heredoc. See "Safe by construction".

## Findings

### S-1. The owned-key refusal is bypassed by k3s's aliases and by `=` in a key

* **Where:** `shakenfist_client_k3s/cluster.py:207-262`
  (`K3S_SERVER_OWNED_KEYS`, `K3S_AGENT_OWNED_KEYS`) and `:556` (the
  comparison). Documented as a guarantee at `docs/usage.md:115-131`.
* **Claim:** `validate_k3s_config()` compares each key, with any trailing
  `+` stripped, against the long flag names only. k3s accepts two other
  spellings of the same flags in a config file:
  1. **Aliases.** k3s puts every name a flag has, aliases included, into
     the valid-flag set, and turns a one-character key into `-k=v`. The
     aliases of owned flags are, on servers, `t` (token), `d`
     (data-dir), `s` (server) and `o` (write-kubeconfig); on agents, `t`,
     `s` and `d`.
  2. **`=` inside a key.** k3s builds each argument as `"--" + key + "="
     + value`. Its validity filter takes the flag name as everything up to
     the first `=`. So `token=abc: def` becomes `--token=abc=def`, which
     passes the filter as `token`, and Go's flag parser, which also splits
     on the first `=`, reads it as token `abc=def`. This reaches every
     owned key, `node-name`, `with-node-id`, `https-listen-port` and
     `write-kubeconfig-mode` included.

  Config-file arguments beat the `K3S_TOKEN` and `K3S_URL` environment
  variables the installer sets: they are arguments to urfave/cli, and an
  argument beats an environment default.
* **Impact:** a correctness hazard, not an escalation. The caller already
  has root on every node through the same agent, and cluster-admin. But
  the docs promise that these keys "are refused before anything is
  built", and that promise is false. For example, `d:` on a server sends
  the plugin's token fetches (`:1617`, `:1623`) to the wrong place, so
  create fails after building everything; `t:` on an agent makes the join
  fail; `s:` sends a node to another server; `node-name=x:` defeats
  `remove-worker`'s lookup. `o` is milder than its comment says on current
  k3s: when `write-kubeconfig` is set, `writeKubeConfig()` leaves
  `/etc/rancher/k3s/k3s.yaml` as a symlink to it. That was not checked at
  the `v1.21.1` floor.
* **Proposed fix (small):** add the aliases to the two frozensets, with a
  comment naming them as aliases; and refuse any key containing `=`,
  since no k3s flag name contains one.
* **Evidence:**
  * Through `read_k3s_config()` and `validate_k3s_config()`: `{'t':
    'abc'}`, `{'d': '/tmp/x'}` and `{'token=abc': 'def'}` (written as
    `token=abc: def`) are all accepted.
  * k3s `parser.go`, `stripInvalidFlags()`: `for _, s := range f.Names()
    { validFlags[s] = true }` and `regexp.Compile("^-+([^=]*)=")`.
  * k3s `parser.go`, `readConfigFile()`: `prefix := "--"; if len(k) == 1
    { prefix = "-" } ... prefix+k+"="+str`.
  * k3s `server.go`: `data-dir` has alias `d`, `token` has `t`,
    `write-kubeconfig` has `o`, `server` has `s`. `agent.go` has `t`, `s`
    and `d`.

### S-2. A caller's `bind-address` silently breaks the kubeconfig `getconfig` hands out

* **Where:** `cluster.py:2227-2228`, the credential fetch: `kubeconfig =
  self.await_fetch(aop).replace('127.0.0.1', md['api_address_floating'])`.
  Pre-existing code, made reachable by phase 2's `server_config`.
* **Claim:** k3s writes its kubeconfig's server URL from
  `BindAddressOrLoopback()`: the configured `bind-address` when one is
  set, and `127.0.0.1` only when none is. `bind-address` is not an owned
  key. A `server_config` carrying it therefore makes the `.replace()` a
  no-op, and nothing fails: the plugin's own on-node `kubectl` and `helm`
  keep working, so MetalLB and Longhorn install and create completes,
  while the kubeconfig recorded in metadata, printed by `getconfig` and
  merged into `~/.kube/config` points at the bind address, not the
  floating one. An IPv6-only `service-cidr` does the same through
  `[::1]`, and the hidden `disable-apiserver` changes the port.
* **Impact:** a correctness hazard for `getconfig` and the local merge,
  not an escalation. Before phase 2 no caller could reach it.
* **Proposed fix:** either refuse `bind-address` on servers, or (more
  robust) set `kc['clusters'][0]['cluster']['server'] =
  'https://<floating>:6443'` structurally instead of substituting text.
* **Evidence:** k3s `server.go`, `writeKubeConfig()`: `ip :=
  config.ControlConfig.BindAddressOrLoopback(false, true); port :=
  config.ControlConfig.HTTPSPort; ... url :=
  fmt.Sprintf("https://%s:%d", ip, port)`. k3s `types.go`,
  `BindAddressOrLoopback`: `ip := c.BindAddress ... else if ip != "" {
  return ip }; return c.Loopback(urlSafe)`.

### S-3. The refused-key list is a guard against mistakes, not a trust boundary, and the docs do not say so

* **Where:** `docs/usage.md:115-131`, and `docs/library-api.md:115-130`,
  which offers `read_k3s_config()` to "a form which wants to reject a
  file" (`:122`).
* **Claim:** whoever supplies `server_config` or `agent_config` controls
  the security posture of every node of that role. Each of these is
  unowned and accepted: `kube-apiserver-arg: [anonymous-auth=true,
  authorization-mode=AlwaysAllow]` makes the API on the floating address
  unauthenticated cluster-admin; `kubelet-arg` can do the same to every
  kubelet; `etcd-arg` and `etcd-expose-metrics` can expose etcd; `*-file`
  keys and `private-registry` make k3s read node paths as root;
  `etcd-s3-*` ships snapshots to any endpoint. For the CLI and for a
  library caller passing its own configuration, this is a privilege the
  caller already has (root on the nodes through the agent, and
  cluster-admin). It becomes an escalation only if a wrapper -- a form, a
  self-service playbook -- passes through configuration from someone less
  trusted.
* **Proposed:** a sentence or two in `usage.md`'s k3s configuration
  section, linked from `library-api.md`'s `read_k3s_config()` paragraph,
  saying the refusals protect the plugin's own operations, that the
  supplier effectively has root, and that a wrapper must apply its own
  allowlist.
* **Evidence:** `validate_k3s_config()`'s docstring (`:514-521`) already
  says the configuration "is not interpreted". The flag names come from
  k3s `server.go` and `agent.go`.

### S-4. Caller configuration is stored and displayed in clear, including by the redacted `delete -v` path

* **Where:** `cluster.py:2173-2174` records the mappings in namespace
  metadata; `__init__.py:330-331`: `show` prints every key;
  `cluster.py:2648-2652`: `delete()`'s debug loop redacts only
  `SECRET_METADATA_KEYS`; `cluster.py:1490-1493` writes the drop-in with
  `cat - > path`, with no umask or chmod.
* **Claim:** k3s accepts several credentials inline as config keys, none
  of them owned: `agent-token`; `etcd-s3-access-key`,
  `etcd-s3-secret-key`, `etcd-s3-session-token`; `datastore-endpoint`, a
  DSN that can carry a password; `vpn-auth`. Put in `--server-config` or
  `--agent-config`, such a credential is stored in the namespace metadata;
  printed by `k3s show`; printed by `delete -v`, the path whose own
  comment says it redacts because "-v is exactly the flag somebody adds
  when a delete is failing"; and written to a node file whose mode follows
  the agent shell's umask, normally 0644.

  Two of those are not new exposure. `k3s show` already prints the node
  token and kubeconfig, and `usage.md:420-422` says so. On servers,
  `write-kubeconfig-mode: 0644` already makes the cluster-admin
  kubeconfig world-readable on the node. `delete -v` is the new part:
  its redaction is now incomplete, and S3 or datastore credentials reach
  beyond the cluster.
* **Proposed:** `document` that the mappings are recorded and shown, so
  credentials should not go in them inline. Optionally `consider`
  redacting `server_config` and `agent_config` wholesale in the `:2649`
  condition -- a one-liner. A per-key list would go stale.
* **Evidence:** the loop is `if k in SECRET_METADATA_KEYS and md[k] is not
  None: ... REDACTED / else: self.reporter.debug('    %s = %s' % (k,
  md[k]))`, and `SECRET_METADATA_KEYS = ('node_token', 'server_token',
  'kubeconfig', 'ssh_key')`.

### S-5. YAML alias amplification through `read_k3s_config()`

* **Where:** `cluster.py:617-618` (`yaml.safe_load(f.read())`), then
  `:565` and `:575`. Repeated in `create()` at `:2014-2015`, and in
  `_k3s_config_commands()` per node.
* **Claim:** `safe_load` builds aliases as shared references, cheaply. The
  JSON round trip and the dump then expand them, exponentially. If it got
  that far, the expanded text would be stored in metadata and sent to
  every node. Recursive aliases are refused correctly, as
  `not_representable`.
* **Impact:** not an escalation for the CLI. But `read_k3s_config()` is
  public and documented for form use (S-3), where an uploaded file is a
  cheap denial of service.
* **Proposed (`consider`):** refuse aliases. A `SafeLoader` subclass whose
  `compose_node` refuses an `AliasEvent` is about five lines, and k3s
  configuration has no use for anchors. Alternatively, cap the file size,
  which also bounds S-6.
* **Evidence:** each level is a list of ten aliases to the previous one.

| Levels | Input | read+validate | validate again | YAML out |
|---|---|---|---|---|
| 4 | 200 B | 0.11 s | 0.08 s | 108,656 B |
| 5 | 250 B | 0.93 s | 1.04 s | 1,308,660 B |
| 6 | 300 B | 11.04 s | 9.78 s | 15,308,664 B |
| 7 | 350 B | 80.84 s | 69.56 s | 175,308,668 B |

### S-6. A CLI config path can name a FIFO, a device or a symlink

* **Where:** `__init__.py:210-218` (`click.Path(exists=True,
  dir_okay=False)`) and `cluster.py:617-618`.
* **Claim:** only directories are refused. `/dev/zero` is accepted, and
  reading it grows until `MemoryError`, which is not an `OSError`, so it
  escapes the hierarchy as a traceback. A FIFO blocks. A symlink is
  followed. All run with the user's own privileges, against a path the
  user typed. Nothing is joined to the path, so there is nothing for
  `path-traversal-review` to prove. Process substitution (`<(...)`, a
  `/dev/fd` FIFO) is a legitimate use.
* **Rating:** none.
* **Evidence:** under `ulimit -v 600000`, `read_k3s_config('/dev/zero')`
  raised `MemoryError`. `click.Path(...).convert('/dev/zero')` accepted
  it.

### S-7. Error messages echo a fragment of the file, or a refused value

* **Where:** `exceptions.py:733-743` (`not_representable` uses `%r` of the
  value) and `:769-771` (`unreadable` includes PyYAML's problem mark,
  which quotes the bad line).
* **Claim:** a credential on a malformed line, or a value YAML reads as a
  date or as `!!binary`, is printed to the supplier's own stderr. The
  collection module renders only `str(e)`, and does not pass
  `server_config` today. No message names the plugin's tokens or the
  kubeconfig. `owned_key` prints the key, never the value.
* **Rating:** none.
* **Evidence:** `K3sConfigError.unreadable: ... could not determine a
  constructor for the tag
  'tag:yaml.org,2002:python/object/apply:os.system' ... line 1, column 4:
  a: !!python/o...`; `K3sConfigError.not_representable: ... as JSON:
  b'hello'.`

## Existing issues

* **#104 (release lookups thin against unexpected upstream shapes).**
  `check_k3s_release()` is new code, and three rough edges were
  rediscovered in it. All need a malformed or hostile update API, or an
  edited namespace metadata cache:
  * `\d` matches non-ASCII digits, so `'v１.２１.１'` is accepted. A
    component of more than 4300 digits raises an uncaught `ValueError`
    from `int()`. Fix: `[0-9]{1,9}`.
  * `too_old()` renders the release with `%s`, so control characters
    after a valid prefix reach the terminal raw. `unparseable()` uses
    `%r`.
  * A release with a newline after a valid prefix is accepted. It reaches
    the installer only through `shlex.quote()` (`:1606`, `:1656`), so the
    shell is safe.

  Worth a comment on #104.

  | Release | Result |
  |---|---|
  | `'v1.20.0\x1b[2J+k3s1'` | `too_old`, with the escape printed raw |
  | `'v１.２１.１'` | accepted |
  | `'v1.33.4\nINJECT'` | accepted |
  | 5000-digit component | uncaught `ValueError` (exceeds the 4300-digit limit) |
* None of #82, #93, #96, #97, #100, #101, #102 or #105 was rediscovered.
* **Outside scope, for the record.** `SECRET_METADATA_KEYS`'s comment
  (`cluster.py:51-58`) says `node_token` "registers an agent, which is
  scheduling rights". In k3s, `node-token` is a symlink to the server
  `token` (`server.go` `printTokens()`), so the two are the same
  credential. Both are redacted, so nothing leaks. The same code shows
  that a caller's `agent-token` does not break the plugin's worker joins:
  the server token is documented as valid "to join both server and agent
  nodes".

## Safe by construction

**Caller YAML cannot reach the guest shell as anything but heredoc body,
and cannot end the heredoc.** Five independent layers stand in the way,
and any one of them would do:

1. **The delimiter is quoted.** `heredoc()` emits `<< 'SFK3SCONFIG'`, so
   the shell expands nothing in the body, and the paths are module
   literals passed through `shlex.quote()`.
2. **The body is what PyYAML emits from plain JSON types:**
   `yaml.safe_dump(json.loads(json.dumps(config)))`. `allow_unicode` is
   off, so every non-ASCII or non-printable character is escaped in a
   double-quoted scalar: CR, NUL, NEL, LS, PS, BOM, ESC and emoji.
   Multi-line scalars are single-quoted with indented continuation lines,
   in keys and list items as well as values. The top level is always a
   mapping, so no unindented line can be the bare delimiter.
3. **`validate_k3s_config()` checks the dumped text** for a delimiter
   line (`:586`).
4. **`heredoc()` refuses such a body again** at the write.
5. **The metadata path is re-checked.** `_k3s_config_commands()`
   re-validates what it reads back, which covers `expand-workers` and
   direct library callers.

Fuzzing confirms layer 2 by itself: 100,000 random configurations, up to
three deep, built from the delimiter, `\n`, `\r`, `\x85`, ` `,
` `, `\x00`, `\v`, `\f`, `\x1b`, quotes, `$(id)`, backticks, `---`,
`...`, YAML indicators and 90-character runs. Every one produced exactly
one `SFK3SCONFIG` line (the terminator), only printable ASCII plus `\n`,
and text that round-tripped: `written 100000 refused 0`.

**Hostile YAML constructs are refused or neutralised:** `!!python/*`
tags are `unreadable`; `!!binary`, `!!set`, dates, integer keys inside
values, NaN and recursive aliases are `not_representable`; multi-document
files, a trailing `---` included, are `unreadable`; non-UTF-8 bytes and
C0 control characters are `unreadable`; `true:` and `~:` keys are
`non_string_key`; a BOM is stripped, and merge keys are expanded into
plain mappings. Only the alias bomb (S-5) gets through, and it costs CPU
and memory, not injection.

**What the caller cannot override:**

* **The servicelb disable.** k3s's `readConfigFile()` reads
  `config.yaml`, then `config.yaml.d/*` in sorted `os.ReadDir` order, and
  a `+` key appends. The `90-` file's `disable+: [servicelb]` appends to
  any caller `disable`, and k3s has no re-enable flag.
* **Which files k3s loads.** A `config` or `c` key cannot redirect them,
  because `findConfigFileFlag()` reads only argv and `K3S_CONFIG_FILE`,
  before any file is parsed.

**Release strings** reach a shell only through `shlex.quote()`.

## Checked and clean

* **Shell rules 1 and 2** hold for every command phase 2 added. The
  `mkdir` is a literal, the three writes go through `heredoc()` with
  literal paths, and the installer lines quote `k3s_version`,
  `join_address`, `token` and `node_role`.
* **Progress output.** No new `note`, `phase` or `debug` line echoes
  configuration. Heredoc commands are described by their first line only
  (`describe_agent_op()` splits on `\n`, and `_agent_op_error()` uses
  it), so neither the body nor a token reaches progress output or
  `AgentOperationError`. The plugin's own `config.yaml` holds no secret.
* **Exception messages** name no token or kubeconfig.
* **Path construction.** No path is built from caller data.
* **`tools/ci_deploy_test.sh` (phase 3):** there is no `set -x`; `show`
  output is captured and never echoed whole; `assert_node_sizes` and
  `assert_instance_size` print only their own values; the new
  `dump_state` lines (labels, taints) carry no secret; the CI config
  files hold no credential and are removed after create; `ast.literal_eval`
  evaluates no code.
* **Kubeconfig file handling** is unchanged by these phases.
* **`etcd-*` and `datastore-*` keys.** The plugin's later operations
  (`getconfig`, `expand-workers`, `remove-worker`, `health`) use only the
  on-node kubeconfig and the two token files, never the datastore. These
  keys can break the caller's own cluster but cannot mislead the plugin,
  apart from S-1's and S-2's cases.
* **Not checked:** the hidden `supervisor-port` and `apiserver-port`, and
  `advertise-port`, might do to the join what an owned `https-listen-port`
  would; that was not verified on a node or in k3s's source, so it is not
  raised. Nor was whether `kubelet-arg: hostname-override=...` defeats the
  `node-name` refusal.

## Summary

Seven findings: 0 `fix`, 2 `document` (S-3, S-4, the latter with an
optional one-line redaction), 3 `consider` (S-1, S-2, S-5), 2 `none`
(S-6, S-7), plus a #104 rediscovery. The central question is clean:
caller YAML cannot reach the guest shell unquoted or end the heredoc.
None of the findings is an escalation for the CLI or a library caller
passing its own configuration.
