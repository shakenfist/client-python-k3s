"""One k3s cluster, and the orchestration which drives it.

This replaces the click context the orchestration primitives used to be
handed. Anything which reads or writes cluster metadata, or drives this
cluster's nodes, is a method here (see decision 2 in
``docs/plans/PLAN-library-api-and-collection-phase-01-library-api.md``), so
that a library caller does not have to fabricate a click context to reach
it. What remains in ``primitives`` is namespace scoped or stateless: the
two release lookups, whose caches live in namespace metadata rather than
in any cluster's, and the pure helpers.

Imports between the two modules are deliberately one directional: this
module imports ``primitives``, and ``primitives`` must never import this
one.

Because ``shakenfist_client_k3s`` is imported unconditionally by the
``sf-client`` plugin loader, this module imports nothing beyond the
standard library and modules this package already imports. That includes
the ``importlib.metadata`` guard below, which moved here with
``Cluster.create()``: it is the same try/except the package __init__
carried, for the same reason (``importlib.metadata`` is only in the
standard library from Python 3.8, and this package supports 3.7).
"""

import copy
import ipaddress
import json
import os
import re
from shakenfist_client import apiclient
import shlex
import shutil
import subprocess
import tempfile
import time
import yaml

try:
    from importlib.metadata import version as distribution_version
except ImportError:
    from importlib_metadata import version as distribution_version

from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress


# The namespace metadata key a cluster's state is stored under, one
# document per cluster.
METADATA_KEY = 'orchestrated_k3s_cluster_%s'

# The keys in that document whose values are credentials. node_token
# registers an agent, which is scheduling rights on the cluster;
# server_token joins a control plane node, which is cluster admin;
# kubeconfig carries a cluster-admin client certificate and key; ssh_key
# is whatever the cluster was built with. Named here rather than at each
# site which must not print them, so that a site added later has
# somewhere to ask rather than a list to rediscover.
SECRET_METADATA_KEYS = ('node_token', 'server_token', 'kubeconfig',
                        'ssh_key')

BASE_OS_VERSION = 'debian:12'

# The size every node is built at unless create() is told otherwise, per
# role. Memory is in MB and disk in GB, because that is what the Shaken
# Fist API takes and translating units here would only give the docs a
# second set of numbers to disagree with. This is the one source of truth
# for these numbers: create()'s keyword argument defaults read it, so do
# the command line's option defaults, and so does the metadata fallback in
# Cluster._node_size() and Cluster.show() for clusters created before
# node_sizes was recorded. Three copies of 2048 is how the documentation
# and the code come to disagree.
#
# These are the sizes every node was built at before they could be
# chosen, and they stay the default so that an existing invocation builds
# exactly what it built before. That is not the same as saying they are
# enough: see create()'s docstring for what 2048 MB does to a control
# plane under load.
DEFAULT_NODE_SIZE = {'cpus': 2, 'memory': 2048, 'disk': 50}

# How long, in seconds, a single agent command can run before the wait loop
# notes that it might be stalled.
STALL_WARNING_SECONDS = 300

# The agent operation states an operation can still move out of, the ones
# which mean it finished without doing the work, and the ones which mean it
# is over and nothing is owed.
#
# Waiting "while the operation can still progress" rather than "until it is
# complete or error" is the load bearing part. Shaken Fist gives every
# agent operation a wall clock budget -- AGENT_OPERATION_DEFAULT_DEADLINE,
# 600 seconds unless the creator asked for something else -- and an
# operation which runs out of it moves to 'expired', which the server
# documents as deliberately distinct from 'error'. Every wait loop here
# used to test for 'complete' and 'error' by name, so an expired operation
# satisfied neither and the loop spun for as long as the process was left
# running. 'deleted' is reachable from every state and had the same effect.
# Enumerating the states instead means the server can be the authority on
# what an operation is doing.
#
# A state none of these three name is one Shaken Fist added after this
# release, and the two kinds of wait here answer that differently on
# purpose. await_idle() asks "may this instance be given another command",
# and waits, because an unrecognised state is far more likely to be a new
# way of being in flight than a new ending, and running the next install
# step over a command which is still executing corrupts the node. It cannot
# wedge the way 'expired' and 'deleted' did, because the server moves every
# operation out of whatever state it is in within its deadline. await_fetch()
# and reap_execute() ask "did this command run", and raise, because the
# answer they need is only available from 'complete' and anything else is a
# command whose output does not exist. Waiting is the safe default for the
# first question and refusing is the safe default for the second, which is
# why the asymmetry is deliberate rather than an oversight.
AGENT_OP_PENDING_STATES = ('initial', 'preflight', 'queued', 'executing')
AGENT_OP_FAILED_STATES = ('error', 'expired')
AGENT_OP_FINISHED_STATES = ('complete', 'deleted')
AGENT_OP_KNOWN_STATES = (AGENT_OP_PENDING_STATES + AGENT_OP_FAILED_STATES
                         + AGENT_OP_FINISHED_STATES)

# How long health()'s read only probe waits for its one command before it
# reports a timeout as a finding. The server's own deadline would bound it
# eventually, but ten minutes of silence is not a health check: a
# 'kubectl get nodes' on a cluster which is answering returns in well
# under a second, so a cluster which has not answered in thirty is the
# answer rather than a slow one.
HEALTH_PROBE_TIMEOUT_SECONDS = 30

# The timeout passed to 'kubectl drain'. Without it a drain which cannot
# evict a pod blocks until the agent operation's own deadline expires,
# which reports the operation rather than the reason, and leaves the node
# cordoned for the whole wait. With it kubectl gives up and says why, and
# remove_worker() uncordons the node before re-raising.
KUBECTL_DRAIN_TIMEOUT = '300s'

# Where k3s looks for manifests to apply itself, and the filename suffixes
# it will look at. Everything in this directory is applied when the server
# starts and again whenever a file in it changes, which is what makes
# staging a manifest before k3s is installed work at all. The directory
# does not exist at that point -- k3s creates it when the server starts --
# so install_control_plane() creates it, and k3s finds our files already
# there the first time it runs. Only .yaml, .yml and .json are looked at
# (k3s pkg/deploy/controller.go matches those three suffixes, case
# insensitively), and anything else in the directory is ignored without
# comment, which is why read_manifests() refuses such a file rather than
# let a payload go quietly missing.
K3S_MANIFEST_DIR = '/var/lib/rancher/k3s/server/manifests'
K3S_MANIFEST_SUFFIXES = ('.yaml', '.yml', '.json')

# Destination filenames are checked against this before they are used.
# The suffix check above asks whether k3s will read the file; this asks
# whether the shell will read the name. read_manifests() takes local paths
# from a library caller, and os.path.basename('/tmp/a;touch /pwned.yaml')
# is 'a;touch /pwned.yaml', which as a redirection target on the control
# plane node runs as root. shlex.quote() at the write site makes that
# harmless on its own, and the write site quotes; this refuses it as well,
# because a filename containing a newline, a quote or a semicolon is
# unreadable in the agent operation log and is not a filename anybody
# meant to use. The leading character is restricted separately so that a
# name cannot begin with a dot or a hyphen.
#
# This character class is also what keeps the remote destination inside
# K3S_MANIFEST_DIR, which is a second job it does and the one a reader is
# least likely to notice. install_control_plane() joins the directory and
# the basename to build the path it writes on the node: no '/' can match,
# so the basename cannot carry a path separator or be an absolute path,
# and no leading dot can match, so it cannot be '..'. Relaxing the class
# for readability -- to allow a space, say -- would need that join
# reconsidered, not only the quoting at the write site.
K3S_MANIFEST_BASENAME_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]*$')

# The heredoc delimiter manifest content is handed to a node with.
# Manifest content belongs to the caller and routinely contains $ and
# backticks -- a container command, a shell script in a ConfigMap -- which
# the shell would expand inside an unquoted heredoc. Quoting the delimiter
# turns all of that off. A line of the manifest which is exactly the
# delimiter would still end the heredoc early, so read_manifests() refuses
# one; base64 would avoid that question entirely, at the cost of making
# every staged manifest unreadable in the agent operation log.
K3S_MANIFEST_DELIMITER = 'SFK3SMANIFEST'

# The heredoc delimiter k3s configuration files are handed to a node with,
# quoted for the same reason as the manifest one: a caller's configuration
# is theirs, and a node-label or kubelet-arg value can hold a $ the shell
# must not expand. validate_k3s_config() refuses configuration whose YAML
# has a line which is exactly this. A separate constant rather than the
# manifest delimiter again, so that each heredoc's terminator says which
# kind of file it ends when it is read in the agent operation log.
K3S_CONFIG_DELIMITER = 'SFK3SCONFIG'

# The k3s configuration keys a caller's server_config and agent_config may
# not set, because the plugin sets them itself or depends on k3s leaving
# them at their default. validate_k3s_config() compares each key with any
# trailing '+' stripped, because k3s's '+' on a string key appends to the
# value rather than leaving it alone; tls-san+ is the one exception, and
# is how a caller adds SANs.
#
# Each key names the code which depends on it, so that the next person to
# wonder whether one really has to be refused can check rather than
# re-derive it (survey finding 5 of
# docs/plans/PLAN-node-customisation-phase-02-k3s-config.md). If that code
# moves or stops depending on the key, the key should leave this list.
# Keys k3s does not recognise are not refused: k3s logs and ignores those
# itself. k3s does recognise every name a flag has in a configuration
# file, its one-letter aliases included, so an alias of an owned key is
# owned too. The aliases were checked against pkg/cli/cmds/server.go and
# agent.go at v1.21.1+k3s1 and at k3s commit bdb2a3e, and are the same in
# both. node-taint and disable are absent on purpose. The default taint
# goes in config.yaml so that a caller's node-taint replaces it, and
# servicelb's disable+ goes in a file read after the caller's so that it
# appends to their disable rather than being replaced by it.
K3S_SERVER_OWNED_KEYS = frozenset([
    # Set in the plugin's config.yaml on the first server by
    # install_control_plane(): it is what makes that node the embedded
    # etcd cluster the other servers join.
    'cluster-init',
    # The registration token fetches in install_control_plane() and
    # K3S_MANIFEST_DIR both hardcode /var/lib/rancher/k3s.
    'data-dir',
    # k3s's alias for data-dir.
    'd',
    # K3S_URL in install_k3s_component() hardcodes port 6443.
    'https-listen-port',
    # remove_worker() addresses a k3s node by its lowercased instance
    # name. A fixed node-name would also give every server the same one.
    'node-name',
    # install_k3s_component() points extra servers at the first one
    # through K3S_URL, and a server key would point them somewhere else.
    'server',
    # k3s's alias for server.
    's',
    # Set in the plugin's config.yaml by install_control_plane() to the
    # floating API address, which is the server address in every
    # kubeconfig the plugin hands out. A bare tls-san in a later file
    # would replace it; tls-san+ appends to it.
    'tls-san',
    # install_k3s_component() joins nodes with K3S_TOKEN, the token
    # install_control_plane() fetched from the first server.
    'token',
    # k3s's alias for token.
    't',
    # The same thing as token, by another route.
    'token-file',
    # Appends an id to the node name, which breaks remove_worker()'s
    # lookup for the same reason node-name does.
    'with-node-id',
    # Every kubectl and helm command the plugin runs on a server, and the
    # credential fetch in create(), read /etc/rancher/k3s/k3s.yaml.
    'write-kubeconfig',
    # k3s's alias for write-kubeconfig.
    'o',
    # Set to 0644 in the plugin's config.yaml by install_control_plane(),
    # which is what lets the credential fetch read the kubeconfig.
    'write-kubeconfig-mode',
])
K3S_AGENT_OWNED_KEYS = frozenset([
    # Nothing in the plugin reads an agent's data directory today. It is
    # refused so that every node keeps the layout the server-side reads of
    # /var/lib/rancher/k3s assume, rather than the two roles differing.
    'data-dir',
    # k3s's alias for data-dir.
    'd',
    # remove_worker() drains and deletes the k3s node by the worker's
    # lowercased instance name, and agent_config is applied to every
    # worker, so a node-name would also give them all the same one.
    'node-name',
    # install_k3s_component() joins the agent to the first server through
    # K3S_URL.
    'server',
    # k3s's alias for server.
    's',
    # install_k3s_component() joins the agent with K3S_TOKEN, the node
    # token install_control_plane() fetched from the first server.
    'token',
    # k3s's alias for token.
    't',
    # The same thing as token, by another route.
    'token-file',
    # Appends an id to the node name, which breaks remove_worker()'s
    # lookup for the same reason node-name does.
    'with-node-id',
])

# The oldest k3s release create() will build. The plugin writes k3s
# configuration as files in /etc/rancher/k3s/config.yaml.d/, and relies on
# k3s's '+' key suffix to append to a list rather than replace it. Drop-in
# directory support is k3s commit a0a1071aa5 (k3s#3162), first released in
# v1.21.0+k3s1; the '+' suffix is commit 8f1a20c0d3, first released in
# v1.21.1+k3s1. An older k3s ignores both silently, so check_k3s_release()
# refuses one rather than build a cluster which quietly lacks its
# configuration. A (major, minor, patch) tuple, compared with the one
# check_k3s_release() parses from the release string.
K3S_RELEASE_FLOOR = (1, 21, 1)

# Every command this module builds is a shell command line, run as root on
# a cluster node by the in-guest agent. Two rules keep that safe, and they
# are rules rather than case by case judgements because the next reader
# cannot be expected to re-derive which values are attacker reachable:
#
# 1. Any value interpolated into a command line is passed through
#    shlex.quote(), unless it is a literal defined in this module. That
#    includes values which arrive from the Shaken Fist API, which validates
#    instance names but is not this package's trust boundary.
# 2. Any heredoc carrying interpolated content uses a quoted delimiter, so
#    the remote shell expands nothing inside the body. Python has already
#    substituted the values by the time the shell sees them, so a quoted
#    delimiter costs nothing and removes the whole question. A quoted
#    delimiter is necessary and not sufficient: it does not stop an
#    interpolated value from *ending* the heredoc, which a value containing
#    a newline followed by a line equal to the delimiter does, and then the
#    rest of the body is read by the shell as commands. So every heredoc is
#    built by heredoc() below, which refuses such a body.
#
# 3. Where a real argument list is available, it is used instead of a
#    shell command line, so that no shell parses the value at all. Both
#    local kubectl invocations -- create()'s merge and delete()'s unset
#    calls -- are argument lists. The agent commands cannot be, because
#    the agent takes a command line.


def _is_address(value):
    """True if value is a string holding an IP address.

    The string check is not redundant with ip_address(). That accepts an
    int as a packed address, so ip_address(1) is 0.0.0.1 and
    ip_address(True) is too -- and these values are not used as addresses
    but interpolated into YAML as text, where an int reaches
    str.join() and raises TypeError from somewhere that cannot say which
    metadata key was wrong. The metadata document is JSON, so an int in
    an address list is a thing a writer can actually put there.

    ip_address() rather than a regular expression so that this stays a
    question about addresses rather than about the shapes seen so far:
    IPv6 passes, and a cluster using it is not refused by its own
    validator.
    """
    if not isinstance(value, str):
        return False
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def heredoc(remote_path, body, delimiter='EOF'):
    """Build the command which writes body to remote_path on a cluster node.

    The in-guest agent runs a shell command line and nothing else, so a
    file reaches a node as the body of a heredoc. Every such command in
    this module is built here, for the reason rule 2 above gives: the
    quoted delimiter is what stops the remote shell expanding the body,
    and refusing a body which contains the delimiter on a line of its own
    is what stops the body ending the heredoc and becoming commands. Both
    halves have to hold for either to be worth anything, so they live
    together rather than at each call site.

    The path is quoted per rule 1, which is free for the literal paths and
    is the point for the manifest destination, whose basename is the
    caller's. Exactly one trailing newline, because the delimiter needs a
    line of its own and a body which already ends in a newline must not
    gain a blank line.
    """
    if body and not body.endswith('\n'):
        body += '\n'
    if delimiter in body.split('\n'):
        raise exceptions.GuestFileError.delimiter_collision(
            remote_path, delimiter)
    return ("cat - > %s << '%s'\n%s%s\n"
            % (shlex.quote(remote_path), delimiter, body, delimiter))


def read_manifests(paths):
    """Read the manifest files named by paths, and return them ready to stage.

    Returns a list of (basename, content) pairs in the order the paths were
    given. The destination filename on the node is the source basename, and
    nothing is templated, reordered or otherwise interpreted (decision 8 of
    the phase 3 plan), so all this does is read the files and refuse the
    ones which cannot be staged.

    Every path is checked before any of them is used, which is the pattern
    step 3b established for remove_worker(): create() calls this before it
    allocates a network or boots an instance, so an unusable manifest costs
    the caller an error instead of a half built cluster to tear down. It is
    also why the existence check lives here and not only in the click
    layer: ``--manifest`` is a click.Path(exists=True), but a library caller
    hands over paths which nothing has looked at, and would otherwise reach
    a bare IOError from outside this package's exception hierarchy.

    Raises exceptions.ManifestError, whose docstring enumerates every way
    a file is refused and why each of them is a refusal rather than
    something to discover on the cluster afterwards. Deliberately no count
    here: there were two and they disagreed as soon as one was added.
    """
    manifests = []
    by_basename = {}

    for path in paths or []:
        basename = os.path.basename(path)

        if not basename.lower().endswith(K3S_MANIFEST_SUFFIXES):
            raise exceptions.ManifestError.not_a_manifest(
                path, K3S_MANIFEST_SUFFIXES)

        # After the suffix check rather than before it, because a caller
        # who pointed at the wrong file is better served by being told
        # what k3s applies than by a message about the character set.
        if not K3S_MANIFEST_BASENAME_RE.match(basename):
            raise exceptions.ManifestError.unsafe_basename(
                path, basename)

        if basename in by_basename:
            raise exceptions.ManifestError.duplicate_basename(
                basename, by_basename[basename], path)

        # encoding is stated rather than inherited from the locale.
        # Without it the file is decoded with locale.getpreferredencoding(),
        # so the same manifest is a different string on a UTF-8 host and an
        # ASCII or latin-1 one -- either a UnicodeDecodeError, or worse, a
        # successful mis-decode which stages bytes the caller did not
        # supply. YAML is UTF-8 by specification and so is JSON, so there is
        # one right answer and it does not depend on where this runs.
        #
        # UnicodeDecodeError is a ValueError, not an OSError, so it is named
        # here explicitly: without it a manifest which cannot be decoded
        # escapes the K3sClusterException hierarchy entirely, which is the
        # one thing unreadable() exists to prevent for a library caller.
        try:
            with open(path, encoding='utf-8') as f:
                content = f.read()
        except (OSError, UnicodeDecodeError) as e:
            raise exceptions.ManifestError.unreadable(path, str(e))

        # Parsed and thrown away: this asks whether k3s will be able to
        # read the file at all, not what it declares. A manifest which
        # does not parse is applied by nobody and reported to nobody --
        # k3s logs it on the node and carries on -- so the cluster comes
        # up healthy with the payload missing unless we refuse it here.
        #
        # Which parser to use is decided by the content and not by the
        # suffix, because that is how k3s decides. Its deploy controller
        # (pkg/deploy/controller.go) hands each document to ToJSON() in
        # k8s.io/apimachinery/pkg/util/yaml, which returns the bytes
        # untouched when IsJSONBuffer() says that, with leading whitespace
        # trimmed, they start with '{', and only otherwise runs them
        # through a YAML parser. Branching on the suffix instead would
        # refuse a tab indented .json file that k3s applies happily:
        # PyYAML implements YAML 1.1, which forbids tabs where JSON
        # permits them, and json.dump(indent='\t') writes exactly that.
        # safe_load_all for the YAML case because a manifest is routinely
        # several documents.
        if content.lstrip().startswith('{'):
            try:
                json.loads(content)
            except ValueError as e:
                raise exceptions.ManifestError.invalid_json(path, str(e))
        else:
            try:
                list(yaml.safe_load_all(content))
            except yaml.YAMLError as e:
                raise exceptions.ManifestError.invalid_yaml(path, str(e))

        if K3S_MANIFEST_DELIMITER in content.split('\n'):
            raise exceptions.ManifestError.delimiter_collision(
                path, K3S_MANIFEST_DELIMITER)

        by_basename[basename] = path
        manifests.append((basename, content))

    return manifests


def validate_node_sizes(sizes):
    """Refuse a node size which is not a positive integer, before anything is built.

    sizes is the mapping create() records in the metadata as
    ``node_sizes``: ``{'control_plane': {'cpus', 'memory', 'disk'},
    'worker': {...}}``. It returns nothing, and raises
    exceptions.NodeSizeError naming the role, the field and the value for
    the first value which is not usable, in the order the mapping holds
    them.

    Only positive integers are enforced, on purpose. A realistic floor --
    2048 MB runs a control plane but does not hold up under load, and
    4096 MB is the figure the documentation gives -- depends on workloads
    this plugin cannot see, so a hard minimum here would be a guess
    presented as a rule. A size of zero, a negative size or a size which
    is not a whole number is not a judgement call, and neither is a type
    the Shaken Fist API was never going to accept.

    bool is refused explicitly, because True is an int in Python:
    ``isinstance(True, int)`` holds and ``True >= 1`` is true, so without
    the check ``cpus=True`` from a library caller -- or from a YAML
    document where somebody wrote ``yes`` -- would build a one vCPU node
    and say nothing. A float is refused rather than truncated for the same
    reason: 2.5 GB of disk is a request this cannot honour, and rounding
    it quietly in either direction is not what the caller asked for.

    A pure function, like read_manifests(), so that it can be tested
    without a client and so that create() can call it before it has talked
    to the API at all.

    It checks the values it is given and not the shape they come in: a
    missing role or field is not reported. Its one caller, create(), always
    builds the complete mapping, so there is nothing to catch today; a new
    caller handing it something partial has to check the shape itself.
    """
    for role, size in sizes.items():
        for field, value in size.items():
            if (not isinstance(value, int) or isinstance(value, bool)
                    or value < 1):
                raise exceptions.NodeSizeError.not_positive_integer(
                    role, field, value)


def validate_k3s_config(config, role):
    """Refuse k3s configuration which cannot be written onto a node, before anything is built.

    config is a caller's server_config or agent_config: a mapping of k3s
    configuration keys to values, which ends up on every node of role as a
    drop-in file k3s reads after the plugin's own config.yaml. role is
    ``'server'`` or ``'agent'``, as k3s spells them, and decides which keys
    the plugin owns. None is treated as an empty mapping, because an empty
    configuration is a legitimate request for nothing, and None is what a
    library caller's default and an empty file both produce.

    Returns the YAML text the drop-in will hold, or '' for an empty mapping,
    which the caller writes no file for. Raises exceptions.K3sConfigError,
    whose docstring enumerates every refusal and why each is a refusal
    rather than something to find out on the node; checks run key by key,
    in the order the mapping holds them, and the first failure is raised.

    The configuration is not interpreted. Keys are not checked against
    k3s's flags, because that list would be a copy of k3s's which goes
    stale, and k3s already logs and ignores a key it does not recognise for
    the role. What is checked is what the plugin needs: that the keys it
    owns are left alone, that the mapping can be recorded in the metadata
    (which is JSON) unchanged, and that the text can be written through a
    quoted heredoc without ending it early.

    The text is dumped from the JSON round trip of config rather than from
    config itself. The two compare equal -- that is what the representable
    check establishes -- but a library caller's dict subclass, an
    OrderedDict say, is one yaml.safe_dump() refuses to represent, and the
    round trip is plain dicts and lists, which is also exactly what the
    metadata will hand back to expand-workers later.

    A pure function, like validate_node_sizes(), so that it can be tested
    without a client and so that create() can call it before it has talked
    to the API at all.
    """
    if role == 'server':
        owned = K3S_SERVER_OWNED_KEYS
    elif role == 'agent':
        owned = K3S_AGENT_OWNED_KEYS
    else:
        # A programming error rather than a caller's: role is never
        # something a user supplies.
        raise ValueError("role must be 'server' or 'agent', not %r" % (role,))

    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise exceptions.K3sConfigError.not_a_mapping(role, config)

    for key, value in config.items():
        if not isinstance(key, str):
            raise exceptions.K3sConfigError.non_string_key(role, key)

        # k3s passes each key on as --key=value and takes the flag name to
        # be everything before the first '=', so token=abc: x would set
        # token to abc=x without being the key token. No k3s flag name
        # contains an '=', so any key which does is refused.
        if '=' in key:
            raise exceptions.K3sConfigError.key_contains_equals(role, key)

        # rstrip rather than removesuffix, which is Python 3.9: a key
        # ending in more than one '+' is not a k3s spelling of anything,
        # and refusing it with the key it was presumably meant to be is
        # the more useful answer.
        if key != 'tls-san+' and key.rstrip('+') in owned:
            raise exceptions.K3sConfigError.owned_key(role, key)

        # json.dumps() raises TypeError for a type it cannot serialise at
        # all (datetime.date, bytes, set) and ValueError for a circular
        # reference, and with allow_nan=False for NaN and both infinities
        # too, which YAML reads from .nan and .inf: by default it would
        # write them as NaN and Infinity, which are not JSON, and an
        # infinity would even survive the comparison below. A type it can
        # serialise but not give back -- a tuple, a non-string key inside a
        # value -- comes back different and fails the comparison instead.
        try:
            representable = json.loads(
                json.dumps(value, allow_nan=False)) == value
        except (TypeError, ValueError):
            representable = False
        if not representable:
            raise exceptions.K3sConfigError.not_representable(
                role, key, value)

    if not config:
        return ''

    # Sorted, so the file on a node reads the same whatever order the
    # mapping was in. safe_dump() sorts by default; sort_keys is not passed
    # because the keyword only exists from PyYAML 5.1, which pyproject.toml
    # does not require, and older releases sort unconditionally anyway.
    text = yaml.safe_dump(json.loads(json.dumps(config)),
                          default_flow_style=False)

    # The same check read_manifests() makes, on the text that will be
    # written rather than on the values it came from. PyYAML indents every
    # continuation line of a value inside a mapping, so a value holding the
    # delimiter on a line of its own does not produce one here, and
    # refusing on the input would refuse configuration which can be
    # written perfectly well. That also makes this hard to trip with
    # today's dumper; it is here so that the guarantee does not rest on
    # how PyYAML happens to format its output.
    if K3S_CONFIG_DELIMITER in text.split('\n'):
        raise exceptions.K3sConfigError.delimiter_collision(
            role, K3S_CONFIG_DELIMITER)

    return text


class _NoAliasSafeLoader(yaml.SafeLoader):
    """yaml.SafeLoader, except that a YAML alias is a parse error.

    safe_load() builds an alias as a second reference to the node it names,
    which is cheap. validate_k3s_config()'s JSON round trip and its dump
    then write out every reference in full, so each level of nested
    aliases multiplies the text: a 350 byte file of them took 80 seconds
    and 175 MB to validate, and would have been stored in the metadata and
    written to every node. k3s configuration has no use for aliases, so
    read_k3s_config() refuses them rather than bounding what they cost. An
    anchor which nothing refers to is harmless and still parses.
    """

    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            event = self.peek_event()
            raise yaml.composer.ComposerError(
                None, None,
                'found an alias, which k3s configuration may not use',
                event.start_mark)
        return super(_NoAliasSafeLoader, self).compose_node(parent, index)


def read_k3s_config(path, role):
    """Read a k3s configuration file for role, and return the mapping it holds.

    This is what ``--server-config`` and ``--agent-config`` read their
    files with. It returns the parsed mapping, not the YAML text
    validate_k3s_config() produces, because the mapping is what create()
    takes and records; create() validates it again, which costs nothing
    and is what covers a library caller who built the mapping without a
    file.

    An empty file is an empty mapping, as None is to
    validate_k3s_config(). A file holding more than one YAML document is
    refused as unreadable rather than read for its first document, because
    k3s reads a configuration file as a single mapping and a second
    document is more likely a mistake than a request to ignore it. So is a
    file which uses a YAML alias, for the reason _NoAliasSafeLoader gives.
    A library caller which passes create() a mapping it built itself is
    not parsing YAML, and is not affected.

    The file is read as UTF-8, and OSError, UnicodeDecodeError and
    yaml.YAMLError are all turned into exceptions.K3sConfigError.unreadable,
    for the reasons read_manifests() gives for its own read: the encoding
    is YAML's by specification rather than the locale's, and a library
    caller which catches K3sClusterException should not also have to catch
    builtins and PyYAML's errors to survive a bad path.
    """
    try:
        with open(path, encoding='utf-8') as f:
            config = yaml.load(f.read(), Loader=_NoAliasSafeLoader)
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as e:
        raise exceptions.K3sConfigError.unreadable(path, str(e))

    if config is None:
        config = {}
    validate_k3s_config(config, role)
    return config


def check_k3s_release(release, channel):
    """Refuse a k3s release older than K3S_RELEASE_FLOOR, before anything is built.

    release is the version get_k3s_release() resolved channel to, such as
    ``v1.33.4+k3s1``; channel is only used in the error, which names both
    so that the caller knows which option to change. Returns nothing.

    Only the leading ``vMAJOR.MINOR.PATCH`` is read. Everything after it --
    the ``+k3sN`` build suffix, or a pre-release such as ``-rc3`` -- is
    ignored, so ``v1.18.2-rc3+k3s1``, which the ``testing`` channel has
    resolved to, is compared as 1.18.2. That is lenient in one direction
    only: a pre-release of the floor release itself would be accepted.
    That needs a channel which resolves to exactly one, and the v1.21
    channel resolves to v1.21.14+k3s1.

    What follows the version does have to look like a suffix, though: a
    '-' or '+' and then only ASCII letters, digits, '.', '-' and '+'. The
    release comes from the k3s update API, or from the namespace's cache
    of it, which is third-party writable. A string with a control
    character or a newline after a good prefix would otherwise be accepted
    and land in too_old()'s message as it stands, and a component of
    thousands of digits makes int() raise on newer Pythons. Each component
    is ASCII digits for the same reason: the regular expression digit class
    also matches digits from other scripts, which int() reads.

    A release this cannot parse raises
    exceptions.UnsupportedReleaseError.unparseable rather than being let
    through, because a version which cannot be read is not one the plugin
    can promise drop-in configuration on (decision 6 of
    docs/plans/PLAN-node-customisation-phase-02-k3s-config.md).
    """
    match = None
    if isinstance(release, str):
        match = re.match(
            r'v([0-9]{1,9})\.([0-9]{1,9})\.([0-9]{1,9})(?:[-+][0-9A-Za-z.+-]*)?\Z',
            release)
    if not match:
        raise exceptions.UnsupportedReleaseError.unparseable(release, channel)

    version = tuple(int(part) for part in match.groups())
    if version < K3S_RELEASE_FLOOR:
        raise exceptions.UnsupportedReleaseError.too_old(
            release, channel, K3S_RELEASE_FLOOR)


class Cluster:
    """One k3s cluster, and everything the orchestration needs to reach it.

    A Cluster is always named: it is the cluster's state, and its metadata
    key is derived from the name. Work which is namespace scoped rather
    than cluster scoped -- listing clusters, and the two release lookups
    behind ``query-k3s-version`` and ``query-longhorn-version`` -- builds
    no Cluster at all and calls the module level functions in
    ``primitives`` instead.
    """

    def __init__(self, client, name, namespace, reporter=None):
        self.client = client
        self.name = name
        self.namespace = namespace
        self.reporter = reporter if reporter is not None else progress.Reporter()

        # The Progress reporter for the operation in flight, if one has
        # been built. get_progress() makes a default on demand, which is
        # what the orchestration relies on when a caller has not made one.
        self.progress = None

        # This cluster's namespace metadata, cached for the life of the
        # object. Namespace metadata is a single document which conductor
        # also writes, so every read is an opportunity to lose someone
        # else's update; the cache exists to keep the number of reads (and
        # therefore the width of that window) the same as it was when this
        # state lived in ctx.obj.
        self._metadata = {}

    def _metadata_key(self):
        """Return the namespace metadata key this cluster's state is stored under."""
        return METADATA_KEY % self.name

    def get_metadata(self):
        """Return this cluster's metadata, fetching it once and then caching it.

        A miss is cached as well as a hit: a cluster which does not exist
        stores None, so asking twice does not fetch twice. Create asks
        before it writes, and the number of namespace metadata reads a
        create performs is behaviour worth preserving exactly.
        """
        md_key = self._metadata_key()
        if md_key not in self._metadata:
            namespace_md = self.client.get_namespace_metadata(self.namespace)
            self._metadata[md_key] = namespace_md.get(md_key)
        return self._metadata[md_key]

    def set_metadata(self, md):
        """Write this cluster's metadata through to the API, updating the cache."""
        md_key = self._metadata_key()
        self._metadata[md_key] = md
        self.client.set_namespace_metadata_item(self.namespace, md_key, md)

    def delete_metadata(self):
        """Remove this cluster's metadata from both the cache and the API.

        Deleting metadata which was never read raises KeyError, as it
        always has: the only caller deletes a cluster it has just read and
        updated, so a cold cache here means the caller is confused rather
        than that there is nothing to do.
        """
        md_key = self._metadata_key()
        del self._metadata[md_key]
        self.client.delete_namespace_metadata_item(self.namespace, md_key)

    def _interrupted_state(self, md):
        """Return md's cluster state if it never finished being built, else None.

        ``md['state']`` has been written since this package's first commit
        and, until now, read nowhere: every other ``['state']`` in the
        package is an instance or agent operation state from the API. It is
        ``initial`` from the moment ``create()`` writes the metadata
        document, ``created`` once ``create()`` has finished, and
        ``deleted`` for the few lines between ``delete()`` destroying
        everything and removing the metadata. Anything which is not
        ``created`` therefore describes a cluster some earlier run stopped
        in the middle of, whose nodes, tokens and kubeconfig are in an
        unknown combination of present and absent.

        Metadata carrying no state at all answers ``'unknown'`` rather than
        None. This package has always written the key, so its absence means
        the document was written by something which is not this package,
        and guessing "finished" would be guessing in the direction which
        drives k3s installs at half built clusters.

        This returns the state rather than a boolean because every caller
        names it: an error a human can act on has to say which of the three
        it found.
        """
        state = md.get('state', 'unknown')
        if state == 'created':
            return None
        return state

    def _require_usable(self, md, verb):
        """Refuse to run verb against a cluster which never finished being built.

        The verbs which change a built cluster all assume the things
        ``create()`` records on its way through: a first control plane node
        to run kubectl on, and a node token to join new workers with. On an
        interrupted cluster those are an empty list and None, which fail as
        an ``IndexError`` and as a k3s agent install against the literal
        token ``None``. Both are worse than being told the cluster is
        rubbish and how to remove it, which is all this does.
        """
        state = self._interrupted_state(md)
        if state:
            raise exceptions.ClusterInterruptedError.not_usable(
                self.name, state, verb)

    def _require_addresses(self, md, key):
        """Refuse to run on metadata whose key does not hold IP addresses.

        ``configure_metallb_addresses()`` interpolates these into a YAML
        body written onto a node through ``heredoc()``, which refuses a
        body carrying a line equal to its delimiter -- so a tampered
        address cannot run commands on the node. What it can do is
        arrive too late: ``expand_addresses()`` routes new floating
        addresses, which are charged for, and commits them to the
        metadata before that write happens, so the refusal lands after
        the spending. The same function already refuses a cluster
        without metallb up front for exactly that reason, and its
        docstring gives the argument.

        So the document is checked before anything is spent rather than
        at the write site. ``heredoc()`` keeps its refusal, which covers
        the sinks this check does not know about and any added later.
        """
        # An absent, empty or None key checks nothing and that is the
        # right answer, not a skipped one: create() has always written
        # this key as a list, and a cluster whose first expand-addresses
        # has not run yet legitimately has none. A value which is not a
        # list is not let through -- iterating a string yields characters
        # and a mapping yields keys, neither of which is an address, so
        # the refusal still fires.
        for value in md.get(key) or []:
            if not _is_address(value):
                raise exceptions.ClusterMetadataError.not_an_address(
                    self.name, key, value)

    def start_progress(self, total_phases):
        """Begin progress reporting for an operation of total_phases phases.

        The one place a Progress is constructed. What is repetitive about
        it is not the constructor but the wiring -- the reporter is both
        the stream written to and the source of the verbose flag, which is
        Cluster's knowledge rather than Progress's -- and every entry point
        which knows its own phase count needs exactly that wiring. Five of
        them wrote it out by hand, so the sixth was going to as well.

        This replaces whatever Progress is already there, which is what an
        entry point wants: it is starting an operation, and the count it
        knows is the right one. get_progress() is the other half of the
        arrangement and deliberately does not replace.
        """
        self.progress = progress.Progress(
            total_phases=total_phases, verbose=self.reporter.verbose,
            stream=self.reporter)
        return self.progress

    def get_progress(self, total_phases=1):
        """Return the Progress reporter for this operation, making a default if needed.

        Commands which know how many phases they have call
        start_progress(); everything else gets one lazily from here, so a
        method called directly by a library caller still reports progress
        somewhere sensible.

        total_phases is the count the lazy default is built with, and is
        ignored when there is already a Progress to return -- it says how
        many phases *this* method is about to open, not how many the
        operation has. It defaults to 1 because the methods which call
        get_progress() themselves, rather than inheriting a Progress an
        entry point already started, open exactly one phase and do their
        work inside it. 1 is therefore the true count for those callers
        and not a placeholder guess, which is what gives a library caller
        who invokes one of them directly an honest "[1/1]" instead of the
        un-numbered "[n]" this used to print. install_control_plane() is
        the exception: it opens a second phase through
        install_extra_control_plane() when the cluster has more than one
        control plane node, so it works its count out from the metadata
        rather than taking the default.

        This is one Progress per Cluster instance, cached for its life
        (see __init__), so it is only accurate for a single such call. A
        library caller who invokes two of these methods in sequence on the
        same Cluster shares the one lazily built Progress between them --
        the second call's phase header becomes "[2/1]", which is worse
        than un-numbered. A caller doing that should call start_progress()
        with the real total first, the way create() and expand_workers()
        do.
        """
        if not self.progress:
            self.start_progress(total_phases)
        return self.progress

    def _node_size(self, md, node_type):
        """Return the size this cluster builds node_type nodes at, as a new dict.

        node_type is 'control_plane' or 'worker', which are the keys of
        ``md['node_sizes']`` as create() records it and the values
        create_and_await_instances() is handed.

        A cluster created before node_sizes was recorded has no such key,
        and falls back to DEFAULT_NODE_SIZE the way install_k3s_component()
        falls back from ``join_address`` to ``api_address_inner``. The
        fallback is exact rather than a guess: before node_sizes existed
        every node was built at the default, and there was no way to build
        one at any other size, so an expand-workers on such a cluster builds
        exactly what its create did.

        The fallback is per field rather than per role. The plugin only
        ever records all three fields for both roles, but namespace
        metadata is editable by anything holding the namespace's
        credentials, and a role recorded without one of its fields would
        otherwise be a bare KeyError from the middle of an expand-workers.
        Filling the gap from the default is the same answer this method
        gives for a cluster with no record at all.

        A copy, so that a caller which adjusts what it is handed changes
        neither the cached metadata nor the module's default.
        """
        return dict(DEFAULT_NODE_SIZE, **md.get('node_sizes', {}).get(node_type, {}))

    def create_instance(self, node_type):
        """Create one node of node_type, sized as this cluster records for that role.

        node_type is required, with no default, on purpose. A default of
        'worker' would be right for most callers and would silently build a
        control plane node at worker size the day somebody adds a caller
        and forgets the argument -- which on a cluster sized the way the
        documentation recommends is a control plane with half the memory it
        was meant to have, discovered under load. Its one caller,
        create_and_await_instances(), already knows the role.
        """
        md = self.get_metadata()
        size = self._node_size(md, node_type)

        node_name = 'k3s-%s-node-%03d' % (md['name'], md['node_serial'])
        inst = self.client.create_instance(
            node_name, size['cpus'], size['memory'],
            [
                {
                    'network_uuid': md['node_network'],
                    'macaddress': None,
                    'model': 'virtio',
                    'float': True
                }
            ],
            [
                {
                    'size': size['disk'],
                    'base': BASE_OS_VERSION,
                    'bus': None,
                    'type': 'disk'
                }
            ],
            md.get('ssh_key'), None,
            side_channels=['sf-agent2'],
            namespace=md['namespace']
        )
        return inst

    def _agent_op_error(self, aop):
        """Build the exception for an agent operation which did not do its work.

        This builds the exception rather than raising it so that the
        ``raise`` is visible at each of the three call sites. The previous
        version of this method exited the process and so never returned,
        and all three callers have code immediately after the call which is
        only correct because it is never reached: the results dict they go
        on to index is absent or unusable on an errored operation.
        Returning the exception keeps that control flow explicit rather
        than resting on a helper's promise not to come back.
        """
        inst = self.client.get_instance(aop['instance_uuid'])
        return exceptions.AgentOperationError(
            inst['name'], aop['instance_uuid'], aop['uuid'],
            progress.describe_agent_op(aop, max_len=None),
            aop.get('results', {}) or {},
            state=aop.get('state'))

    def await_boot(self, instances):
        p = self.get_progress()
        waiting = copy.copy(instances)
        while waiting:
            for instance_uuid in copy.copy(waiting):
                inst = self.client.get_instance(instance_uuid)
                agent_state = inst['agent_state'] if inst['agent_state'] else 'not yet contactable'
                p.update(inst['name'], 'state %s, agent %s' % (inst['state'], agent_state))
                if inst['state'] == 'created' and inst['agent_state'] == 'ready':
                    waiting.remove(instance_uuid)

            if not waiting:
                break
            time.sleep(5)
        p.wait_done()

    def await_idle(self, instances, own_operations):
        """Wait until these instances are running no agent commands.

        Waits for *every* agent operation on each instance, including ones
        another process submitted: the next command must not race one which
        is still executing on the node, and there is no way to ask the node
        to serialise on our behalf.

        Judges only the operations named in own_operations. Agent operations
        stay associated with an instance forever, so an instance carries
        every command anybody ever ran on it, and a failure among those is
        not this call's business: raising for one would abort a command over
        somebody else's, naming an operation the caller never submitted.
        health()'s abandoned probe is the concrete case -- it leaves a
        'kubectl get nodes' queued, which the server later moves to
        'expired' -- and before own_operations existed, a snapshot of the
        operations which had *already* failed was the defence. That snapshot
        could not see an operation which was still queued when the wait
        started and failed while it ran, which is exactly the shape the
        probe creates.

        own_operations has no default, deliberately. A caller which wants
        only "is this instance busy" passes an empty list and says so, where
        a default would let a caller which meant to be judged silently not
        be -- and a wait which quietly stops failing on a failed install is
        the kind of bug this argument exists to prevent, not to introduce.
        execute_and_await() passes the operations it just submitted, and
        reap_execute() is still what turns each of those into an exception
        with its command and output; the raise here is to fail on the first
        failure rather than after waiting out the rest.
        """
        p = self.get_progress()
        waiting = copy.copy(instances)
        own = set(own_operations)

        running_since = {}
        stall_warned = set()
        state_warned = set()

        while waiting:
            for instance_uuid in copy.copy(waiting):
                inst = self.client.get_instance(instance_uuid)
                agent_ops = self.client.get_instance_agentoperations(
                    instance_uuid, all=True)

                failed = [aop for aop in agent_ops
                          if aop['state'] in AGENT_OP_FAILED_STATES
                          and aop['uuid'] in own]
                if failed:
                    raise self._agent_op_error(failed[0])

                # Still in flight is "in neither the finished nor the failed
                # set", which is not the same as "pending": an operation
                # somebody deleted, or which the server expired, is over and
                # must not be waited for, while one in a state this version
                # has never heard of must be, because declaring the instance
                # idle would run the next install step over a command which
                # may still be executing. Someone else's failed operation
                # ends this wait without raising, which is the whole point
                # of separating the two questions -- and it is why 'expired'
                # has to be excluded here as well as checked above, or an
                # abandoned probe would wedge the wait it used to abort.
                incomplete = [aop for aop in agent_ops
                              if aop['state'] not in AGENT_OP_FINISHED_STATES
                              and aop['state'] not in AGENT_OP_FAILED_STATES]

                # Said once per unrecognised state rather than per poll, and
                # said at all because a wait which silently treats a new
                # state as "still running" is indistinguishable from a hang
                # until it ends.
                for state in sorted({aop['state'] for aop in agent_ops
                                     if aop['state'] not in AGENT_OP_KNOWN_STATES}):
                    if state not in state_warned:
                        state_warned.add(state)
                        p.note('agent operation state %s is not one this version of '
                               'the k3s plugin knows about; waiting for it as though '
                               'the command were still running' % state)

                if not incomplete:
                    p.update(inst['name'], 'idle')
                    waiting.remove(instance_uuid)
                else:
                    aop = incomplete[0]
                    desc = progress.describe_agent_op(aop)
                    remaining = progress.count_str(len(incomplete), 'operation')
                    if desc:
                        p.update(inst['name'], "running '%s' (%s remaining)" % (desc, remaining))
                    else:
                        p.update(inst['name'], '%s remaining' % remaining)

                    # Note once per command if it has been running suspiciously
                    # long. The progress elapsed times show the same thing, but
                    # this note includes the operation uuid and where to look
                    # for more detail, and persists in scrollback.
                    #
                    # time.monotonic() rather than time.time() here and in
                    # await_execute(), because both are measuring how long
                    # something has been going rather than what the time is.
                    # A wall clock can step -- ntp correcting a drifted
                    # clock, or somebody setting the date -- and a step
                    # backwards through a comparison against time.time()
                    # silently extends a bound, while a step forwards fires
                    # a stall warning for a command which has been running
                    # for seconds. The release caches in primitives.py do
                    # use time.time(), correctly: they record when something
                    # happened, which has to survive a restart, and a
                    # monotonic clock is meaningless across processes.
                    now = time.monotonic()
                    command_key = (aop['uuid'], len(aop.get('results', {}) or {}))
                    running_since.setdefault(command_key, now)
                    if (now - running_since[command_key] >= STALL_WARNING_SECONDS
                            and command_key not in stall_warned):
                        stall_warned.add(command_key)
                        p.note("%s has been running '%s' for %s and may be stalled; operation %s, "
                               "'sf-client instance events %s' may show why" % (
                                   inst['name'], desc or 'a command',
                                   progress.format_elapsed(now - running_since[command_key]),
                                   aop['uuid'], inst['name']))

            if not waiting:
                break
            time.sleep(5)
        p.wait_done()

    def await_fetch(self, aop):
        p = self.get_progress()
        while aop['state'] in AGENT_OP_PENDING_STATES:
            p.update('fetch operation', 'state %s' % aop['state'])
            time.sleep(1)
            aop = self.client.get_agent_operation(aop['uuid'])
        p.wait_done()

        if aop['state'] != 'complete':
            raise self._agent_op_error(aop)

        blob_uuid = aop['results']['0']['content_blob']
        data = b''
        for chunk in self.client.get_blob_data(blob_uuid):
            data += chunk
        return data.decode('utf-8')

    def await_execute(self, aop, timeout=None):
        """Wait for an execute agent operation to finish, and return it.

        Split out of reap_execute() so that a caller which wants a command's
        return code as data rather than as an exception can wait for the
        command without also being made to raise on it. health() is that
        caller, and the only one: a command which ran and failed is the
        finding health() exists to report, so it needs the wait without the
        judgement.

        timeout is a budget in seconds measured on the monotonic clock,
        after which the operation
        as last seen is returned with whatever state it had. It is None for
        every caller but health()'s probe: an install which takes eleven
        minutes is a slow install rather than a failed one, and abandoning
        it would leave the caller believing a command it can still see
        running did not happen. Abandoning a read only 'kubectl get nodes'
        costs nothing, which is why that one caller can. A caller passing a
        timeout has to be prepared for a pending state in the returned
        operation; reap_execute() does not pass one, and so its own state
        check is exhaustive.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while aop['state'] in AGENT_OP_PENDING_STATES:
            if deadline is not None and time.monotonic() >= deadline:
                return aop
            time.sleep(1)
            aop = self.client.get_agent_operation(aop['uuid'])
        return aop

    def reap_execute(self, aop):
        aop = self.await_execute(aop)

        # Not 'state == error': await_execute() was called with no timeout,
        # so the operation is in one of its terminal states, and anything
        # which is not 'complete' is one which did not run the command.
        if aop['state'] != 'complete':
            raise self._agent_op_error(aop)

        if aop['results']['0']['return-code'] != 0:
            inst = self.client.get_instance(aop['instance_uuid'])
            raise exceptions.CommandFailedError(
                inst['name'], aop['instance_uuid'],
                aop['commands'][0]['commandline'],
                aop['results']['0']['return-code'],
                aop['results']['0']['stdout'],
                aop['results']['0']['stderr'])

    def create_and_await_instances(self, count, node_type):
        """Create count nodes of node_type, wait for them, and return their UUIDs.

        The returned list is the instances this call created, which is not
        the same thing as the cluster's node list for that type: an expand
        appends to metadata which already holds nodes. Callers which go on
        to install software must use the returned list, not the metadata.
        """
        p = self.get_progress()
        md = self.get_metadata()

        display_type = node_type.replace('_', ' ')
        p.phase('Creating %s' % progress.count_str(count, '%s node' % display_type))

        new_nodes = []
        for i in range(count):
            inst = self.create_instance(node_type)
            new_nodes.append(inst['uuid'])
            md['node_serial'] += 1
            md[f'{node_type}_nodes'].append(inst['uuid'])
            self.set_metadata(md)
            p.note(f'created {inst["name"]} (uuid {inst["uuid"]})')

        self.await_boot(new_nodes)
        p.note('updating base OS packages')
        self.instance_os_update(new_nodes)
        self.set_metadata(md)
        return new_nodes

    def execute_and_await(self, instance_uuids, cmds):
        aops = []
        for cmd in cmds:
            for instance_uuid in instance_uuids:
                aops.append(self.client.instance_execute(
                    instance_uuid, cmd))

        # Wait for instances to be idle and check results. The operations
        # this call submitted are named, so a failure among them aborts here
        # and a failure among anybody else's does not.
        self.await_idle(instance_uuids,
                        own_operations=[aop['uuid'] for aop in aops])
        for aop in aops:
            self.reap_execute(aop)

    def _probe_k3s_api(self, instance_uuid):
        """Ask a control plane node whether its k3s API answers, and report the answer.

        This is execute_and_await()'s read only sibling, and exists because
        that method cannot be used here. It submits every command and only
        then reaps the results, and its reaping raises: await_idle() raises
        AgentOperationError for an operation which entered the error state,
        and reap_execute() raises CommandFailedError for a non-zero return
        code. A failing kubectl is the finding health() exists to report, so
        raising on it would defeat the verb. This therefore submits the
        command itself, waits for that one operation with await_execute(),
        and reads the return code as data.

        await_idle() is deliberately not called either, for a second reason:
        it waits for *every* agent operation on the instance to complete,
        including ones another process queued, and a read only health check
        must not block on somebody else's k3s install.

        Returns the dict health() reports under ``api``; see health()'s
        docstring for the keys. Nothing here raises for an unhealthy answer.
        apiclient.APIException is caught, rather than only its
        ResourceNotFoundException subclass, because every way the API can
        refuse to run a command on this node -- the instance is gone, it is
        in a state which cannot accept agent operations, the cluster is
        unwell enough to return a 500 -- is a fact about this cluster's
        health rather than a bug in this code. An authentication or
        authorisation failure would already have stopped get_metadata()
        before we got here.
        """
        # --kubeconfig explicitly, matching remove_worker(): bare kubectl
        # works on these nodes today, and being consistent about saying so
        # keeps the next reader from wondering which spelling matters.
        command = 'kubectl get nodes --kubeconfig /etc/rancher/k3s/k3s.yaml'
        probe = {
            'probed': True,
            'answered': False,
            'instance_uuid': instance_uuid,
            'command': command,
            'return_code': None,
            'stdout': None,
            'stderr': None,
            'error': None
        }

        self.reporter.debug('Asking %s whether the k3s API answers' % instance_uuid)
        try:
            aop = self.await_execute(
                self.client.instance_execute(instance_uuid, command),
                timeout=HEALTH_PROBE_TIMEOUT_SECONDS)
        except apiclient.APIException as e:
            # apiclient's exceptions never pass their message to
            # Exception.__init__(), so str() on one is the empty string and
            # the only way to the explanation is the attribute.
            detail = getattr(e, 'message', None) or str(e) or 'no detail given'
            probe['probed'] = False
            probe['error'] = ('the command could not be run on instance %s: %s: %s'
                              % (instance_uuid, e.__class__.__name__, detail))
            return probe

        if aop['state'] in AGENT_OP_PENDING_STATES:
            # The wait gave up. This is the state a node whose agent is not
            # connected leaves the operation in: the API accepted it, so
            # nothing raised, and it then sits queued. health() skips the
            # probe when it can already see that from the instance, so
            # reaching here means the instance looked well and the command
            # still did not run.
            # The operation uuid is in the message because this is the one
            # outcome which leaves something behind on the cluster: the
            # command is still queued against the instance, and this is
            # where an operator or a polling caller finds out which one to
            # look at. See health()'s docstring for what that costs.
            probe['probed'] = False
            probe['error'] = (
                "'%s' had not finished after %s seconds (agent operation %s "
                'is still %s), so the wait was abandoned'
                % (command, HEALTH_PROBE_TIMEOUT_SECONDS, aop['uuid'],
                   aop['state']))
            return probe

        if aop['state'] in AGENT_OP_FAILED_STATES:
            probe['error'] = (
                'the agent operation for %s entered the %s state'
                % (progress.describe_agent_op(aop, max_len=None) or command,
                   aop['state']))
            return probe

        # Anything left is an ending this version does not know about:
        # await_execute() returns as soon as the state leaves the pending
        # set, and the two branches above cover every state
        # AGENT_OP_KNOWN_STATES names except 'complete'. Without this the
        # fall-through reported "completed but recorded no result", which is
        # the one thing in the report that would definitely not be what
        # happened -- on the verb whose job is describing unusual states
        # accurately. The rest of this module was changed to expect a state
        # Shaken Fist adds later; this is the place that still assumed the
        # list was closed.
        if aop['state'] != 'complete':
            probe['error'] = (
                'the agent operation is in state %s, which this version of '
                'the k3s plugin does not recognise' % aop['state'])
            return probe

        # An operation which completed without recording a result for its
        # only command should not happen, and health() is the one method
        # which must not turn "should not happen" into a traceback.
        result = (aop.get('results') or {}).get('0')
        if not result:
            probe['error'] = 'the agent operation completed but recorded no result'
            return probe

        probe['return_code'] = result.get('return-code')
        probe['stdout'] = result.get('stdout')
        probe['stderr'] = result.get('stderr')
        probe['answered'] = probe['return_code'] == 0
        if not probe['answered']:
            probe['error'] = "'%s' exited %s" % (command, probe['return_code'])
        return probe

    def _unprobed(self, instance_uuid, error):
        """Build health()'s ``api`` report for a probe which was not run.

        Two callers, and the same shape for both, because a caller reading
        the report must not have to tell "no control plane node to ask"
        apart from "the node we would have asked is down" by which keys
        are present. ``probed`` is False and ``error`` says which.
        """
        return {
            'probed': False,
            'answered': False,
            'instance_uuid': instance_uuid,
            'command': None,
            'return_code': None,
            'stdout': None,
            'stderr': None,
            'error': error
        }

    def _node_health(self, instance_uuid, role):
        """Report the Shaken Fist state of one node, whether or not it still exists.

        Returns one element of health()'s ``nodes`` list; see health()'s
        docstring for the keys. The two states compared against are the two
        await_boot() waits for, so "healthy" here means the same thing as
        "finished booting" there.

        ResourceNotFoundException is caught for the same reason delete()
        catches it per instance: cluster metadata can name an instance which
        somebody has since deleted out from under it, and on a verb whose
        job is to say what is wrong that is an answer rather than a failure.
        Every field is read with .get() rather than subscripted, because a
        health check which crashes on an instance representation missing a
        field is a health check which cannot report the instance it most
        needs to.
        """
        node = {
            'uuid': instance_uuid,
            'role': role,
            'name': None,
            'exists': False,
            'state': None,
            'agent_state': None,
            'healthy': False
        }

        try:
            inst = self.client.get_instance(instance_uuid)
        except apiclient.ResourceNotFoundException:
            return node

        node['exists'] = True
        node['name'] = inst.get('name')
        node['state'] = inst.get('state')
        node['agent_state'] = inst.get('agent_state')
        node['healthy'] = (node['state'] == 'created'
                           and node['agent_state'] == 'ready')
        return node

    def instance_os_update(self, instance_uuids):
        self.execute_and_await(
            instance_uuids,
            [
                'apt-get update',
                'apt-get dist-upgrade -y'
            ]
        )

    # Every node is given up to three k3s configuration files, written
    # before its installer runs so that they are there when the installer
    # first starts the service. k3s reads config.yaml and then every file
    # in config.yaml.d/ in lexical order, and a key in a later file
    # replaces the same key in an earlier one, unless the later key ends
    # in '+', in which case it appends to it.
    #
    # 1. /etc/rancher/k3s/config.yaml holds what the plugin sets or
    #    defaults for the node. On a server that is the kubeconfig mode,
    #    the floating API address as a SAN, cluster-init on the first
    #    server only, and the control plane taint. On an agent it is a
    #    single comment line: the plugin sets nothing there, but k3s
    #    releases from before mid-2024 ignored the drop-ins for the handful
    #    of keys k3s looks up before parsing its configuration whenever
    #    config.yaml itself was missing (survey finding 3 of
    #    docs/plans/PLAN-node-customisation-phase-02-k3s-config.md). A main
    #    file on every node means that never arises, and that anyone
    #    looking at a node can find where its configuration comes from.
    # 2. config.yaml.d/50-sf-client-k3s.yaml holds the caller's
    #    server_config or agent_config, as validate_k3s_config() dumped it,
    #    and is written only when that mapping is non-empty. Read after
    #    config.yaml, so a caller's key replaces the plugin's default:
    #    node-taint: [] is how a caller removes the taint.
    # 3. config.yaml.d/90-sf-client-k3s-enforced.yaml holds what must
    #    survive the caller's file. Today that is disable+: [servicelb], on
    #    servers, when MetalLB is installed: MetalLB answers for
    #    LoadBalancer services, and servicelb alongside it is a second
    #    controller doing nothing useful. It cannot go in config.yaml,
    #    because a caller's disable: [traefik] in the 50 file would replace
    #    it and silently bring servicelb back (survey finding 4). Nor on
    #    the installer's command line, where k3s lets a repeatable argument
    #    replace every file's value, the caller's disable included. Read
    #    last and written with '+', it appends to whatever the caller
    #    wrote, so disable: [traefik] becomes [traefik, servicelb]. A
    #    caller who wants servicelb wants --no-metallb, which turns this
    #    off.
    #
    # The control plane taint is only written when the cluster has workers.
    # MetalLB's controller does not tolerate it, and create() waits for
    # that controller to roll out, so tainting the only node a zero-worker
    # cluster has fails every such create after five minutes (survey
    # finding 7). md['worker_nodes'] is the test because create() builds
    # every instance before installing k3s on any of them, so it is already
    # populated when the control plane is installed. The cost is that a
    # cluster created without workers stays untainted once expand-workers
    # adds some; nothing rewrites a running server's configuration.
    #
    # Every file is written through heredoc(), per rule 2 above
    # read_manifests(). It quotes the path, although the paths are literals
    # of this method, and refuses a body which would end its own heredoc:
    # validate_k3s_config() has already refused that for a caller's
    # configuration, as a K3sConfigError, so the helper's refusal is what
    # covers the two bodies this method composes itself. The floating
    # address is substituted into a YAML document rather than into a
    # command line, and yaml.safe_dump() quotes it as YAML needs.
    def _k3s_config_commands(self, md, role, first_server=False):
        """The shell commands which write a node's k3s configuration files.

        role is ``'server'`` or ``'agent'``, as k3s spells them, and
        first_server marks the server which initialises the cluster.
        Returns a list of commands, the directory first, for the caller to
        run before the k3s installer; the comment above this method says
        what each file holds and why.

        The role's configuration is validated again here, although create()
        validated it before anything was built. A library caller can reach
        the install methods without going through create(), with metadata
        nothing has checked, and the delimiter check has to hold for the
        text actually written rather than for text written somewhere else.
        For such a caller that means a K3sConfigError part way through an
        install, which is intended: it is better than a node whose
        configuration ended its heredoc early.
        """
        if role == 'server':
            plugin_config = {
                # A string, so that safe_dump quotes it. Unquoted, YAML
                # would read 0644 as a number, and k3s wants the mode.
                'write-kubeconfig-mode': '0644',
                # The floating address is the server address in every
                # kubeconfig the plugin hands out, so it has to be in the
                # serving certificate. Extra servers carry it too, so that
                # whichever server answers presents a certificate which
                # matches it.
                'tls-san': [md['api_address_floating']],
            }
            if first_server:
                plugin_config['cluster-init'] = True
            if md.get('worker_nodes'):
                plugin_config['node-taint'] = [
                    'node-role.kubernetes.io/control-plane:NoSchedule']
            main = yaml.safe_dump(plugin_config, default_flow_style=False)
        elif role == 'agent':
            main = ('# Written by shakenfist_client_k3s; caller '
                    'configuration is in config.yaml.d/.\n')
        else:
            # A programming error rather than a caller's, as in
            # validate_k3s_config().
            raise ValueError(
                "role must be 'server' or 'agent', not %r" % (role,))

        def write(path, body):
            return heredoc(path, body, delimiter=K3S_CONFIG_DELIMITER)

        cmds = ['mkdir -p /etc/rancher/k3s/config.yaml.d',
                write('/etc/rancher/k3s/config.yaml', main)]

        caller_config = validate_k3s_config(
            md.get(role + '_config', {}), role)
        if caller_config:
            cmds.append(write(
                '/etc/rancher/k3s/config.yaml.d/50-sf-client-k3s.yaml',
                caller_config))

        # Missing means True, as create()'s comment on the key says: a
        # cluster built before the key existed has MetalLB.
        if role == 'server' and md.get('metallb_installed', True):
            cmds.append(write(
                '/etc/rancher/k3s/config.yaml.d/'
                '90-sf-client-k3s-enforced.yaml',
                yaml.safe_dump({'disable+': ['servicelb']},
                               default_flow_style=False)))

        return cmds

    def install_control_plane(self, manifests=None, staged=None):
        """Prepare the first control plane node, and install k3s on it.

        manifests is a list of local file paths, each of which is written
        into k3s's auto-apply directory on this node before k3s is
        installed, so that k3s applies it itself the first time the server
        starts.

        staged is the same thing already read: the list of
        ``(basename, content)`` pairs read_manifests() returns. create()
        passes it, and that is the point of the argument. create() reads the
        manifests before it builds anything, so that a bad path costs an
        error rather than a network and a handful of instances, and ten to
        twenty minutes of network allocation, instance creation, boot and OS
        update then pass before this method runs. Re-reading the paths here
        would throw that guarantee away: a file edited, moved or deleted in
        that window would raise from here, with the cluster name claimed and
        its metadata document stuck in 'initial', which is exactly the
        outcome the early read exists to prevent. What gets staged is
        therefore what was validated, byte for byte.

        The paths are still read here when staged is not given, because this
        method is callable on its own and a direct caller has had nothing
        check its arguments. Passing both is not an error and staged wins;
        manifests is then only documentation of where it came from.
        """
        md = self.get_metadata()

        # The metadata is read before the Progress rather than after it
        # because it is what says how many phases this method has: the
        # extra control plane nodes below are a second phase, opened on
        # this same Progress by install_extra_control_plane(). Without
        # this a library caller who invokes install_control_plane()
        # directly on an HA cluster is told "[2/1]".
        p = self.get_progress(
            total_phases=2 if len(md['control_plane_nodes']) > 1 else 1)
        if staged is None:
            staged = read_manifests(manifests)
        cmds = []

        p.phase('Installing k3s on the first control plane node')

        # Write the k3s configuration files before k3s is installed. Among
        # other things they carry the floating address as a SAN, which is
        # needed so that the TLS certificate the API serves includes the
        # external address every kubeconfig the plugin hands out points
        # at; and cluster-init, which makes this node the embedded etcd
        # cluster the other servers join. The comment above
        # _k3s_config_commands() has the rest.
        cmds.extend(self._k3s_config_commands(md, 'server', first_server=True))

        # Stage the caller's manifests, before the install below rather
        # than after it: k3s applies this directory when the server first
        # starts, so a manifest which arrives afterwards is one which is
        # applied on the next restart instead of on this one.
        #
        # The directory is ours to create, because k3s has not run yet, and
        # it is created with the mode k3s would have used rather than
        # mkdir's default. k3s creates its data directory with
        # os.MkdirAll(dataDir, 0700) and Go's MkdirAll does not tighten a
        # directory it finds already there, so leaving these at 0755 would
        # permanently loosen /var/lib/rancher/k3s/server, which is about to
        # hold this cluster's registration tokens and TLS keys. -m applies
        # to each directory named as an operand and not to parents -p
        # invents, which is why the parents are named too. An existing
        # directory keeps whatever mode it already has, here as in k3s.
        #
        # /var/lib/rancher is named too, and at 0700, because that is what
        # k3s leaves it at: os.MkdirAll's documented behaviour is that "the
        # permission bits perm (before umask) are used for all directories
        # that MkdirAll creates", so k3s creating /var/lib/rancher/k3s on a
        # fresh node creates its parent at 0700 as well. Staging manifests
        # therefore does not leave a cluster with different modes from one
        # created without them, which is worth saying because the shape of
        # this command invites the opposite conclusion.
        if staged:
            p.note('staging %s into %s: %s'
                   % (progress.count_str(len(staged), 'manifest'),
                      K3S_MANIFEST_DIR,
                      ', '.join(basename for basename, _ in staged)))
            cmds.append(
                'mkdir -p -m 0700 /var/lib/rancher /var/lib/rancher/k3s '
                '/var/lib/rancher/k3s/server %s' % K3S_MANIFEST_DIR)
            for basename, content in staged:
                # heredoc() quotes the destination per rule 1 and
                # normalises the trailing newline. The basename is the
                # caller's, by way of os.path.basename() of a path nothing
                # else has looked at; read_manifests() refuses the shapes
                # which would be unreadable in the operation log, and the
                # quoting makes the ones it allows unable to mean anything
                # to the shell. A basename of plain filename characters
                # quotes to itself, so the common case is byte for byte
                # what it was.
                cmds.append(heredoc(
                    '%s/%s' % (K3S_MANIFEST_DIR, basename), content,
                    delimiter=K3S_MANIFEST_DELIMITER))

        # Instruct the first control plane node to install k3s and helm
        cmds.append('curl -sfL https://get.k3s.io | '
                    'INSTALL_K3S_CHANNEL=%s sh -s - server'
                    % shlex.quote(md['k3s_version']))
        cmds.append('sudo apt-get install -y extrepo')
        cmds.append('sudo extrepo enable helm')
        cmds.append('sudo apt-get update')
        cmds.append('sudo apt-get install -y helm')

        self.execute_and_await([md['control_plane_nodes'][0]], cmds)

        # Fetch the server and node tokens from the first control plane node
        p.note('fetching control plane registration token')
        aop = self.client.instance_get(
            md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/token')
        md['server_token'] = self.await_fetch(aop).rstrip()
        self.set_metadata(md)

        p.note('fetching node registration token')
        aop = self.client.instance_get(
            md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/node-token')
        md['node_token'] = self.await_fetch(aop).rstrip()
        self.set_metadata(md)

        # If there is more than one control plane node, then install the others
        if len(md['control_plane_nodes']) > 1:
            self.install_extra_control_plane()

    def install_k3s_component(self, instance_uuids, token, node_role):
        md = self.get_metadata()

        # Nodes must join via an address inside the node network: the network
        # node neither hairpins floating addresses nor routes in-network
        # traffic to the network's own routed addresses (see
        # shakenfist/shakenfist#3662). Clusters created before join_address
        # existed only have api_address_inner.
        join_address = md.get('join_address', md['api_address_inner'])

        # The k3s configuration files go first, so that they are on the
        # node when the installer starts the service: k3s reads its
        # configuration at startup, and a file which arrives afterwards
        # is not read until something restarts it. node_role is k3s's own
        # name for the subcommand, 'server' or 'agent', which is also how
        # _k3s_config_commands() names a role.
        self.execute_and_await(
            instance_uuids,
            self._k3s_config_commands(md, node_role) + [
                'sudo apt-get update',
                (
                    'curl -sfL https://get.k3s.io | '
                    'INSTALL_K3S_CHANNEL=%s '
                    'K3S_URL=https://%s:6443 '
                    'K3S_TOKEN=%s sh -s - %s'
                    % (shlex.quote(md['k3s_version']),
                       shlex.quote(join_address), shlex.quote(token),
                       shlex.quote(node_role))
                )
            ]
        )

        self.set_metadata(md)

    def install_extra_control_plane(self):
        p = self.get_progress()
        md = self.get_metadata()
        p.phase('Installing k3s on the additional control plane nodes')
        self.install_k3s_component(
            md['control_plane_nodes'][1:], md['server_token'], 'server')

    def install_workers(self, instance_uuids):
        """Install the k3s agent on the worker instances named by instance_uuids.

        There is deliberately no default. Running the installer on a worker
        which is already in the cluster re-runs the k3s agent install on a
        node carrying workloads, so a caller which means "every worker" has
        to say so: create() does, because at create time every worker is
        new, and expand_workers() passes only the instances it just made.
        """
        p = self.get_progress()
        md = self.get_metadata()
        p.phase('Installing k3s on the worker nodes')
        self.install_k3s_component(instance_uuids, md['node_token'], 'agent')

    def allocate_metallb_addresses(self, metal_address_count):
        p = self.get_progress()
        md = self.get_metadata()
        node_network = self.client.get_network(md['node_network'])

        # setdefault rather than [], because delete() and
        # _require_addresses() both already read this key tolerantly and
        # this was the one site which raised KeyError on a document
        # missing it. create() has always written it, so no real cluster
        # reaches that -- but "the document says what we expect" is the
        # assumption rule 1 exists to refuse.
        md.setdefault('routed_addresses', [])

        allocated = []
        for i in range(metal_address_count):
            addr = self.client.route_network_address(node_network['uuid'])
            if addr:
                # Rule 1 at the top of this module puts the Shaken Fist
                # API outside this package's trust boundary, so what it
                # hands back is checked before it is recorded rather
                # than after. Checked here and not only in
                # _require_addresses() because this is the one point at
                # which a bad value can be stopped from entering the
                # document at all.
                if not _is_address(addr):
                    raise exceptions.ClusterMetadataError.not_an_address(
                        self.name, 'routed_addresses', addr)
                md['routed_addresses'].append(addr)
                allocated.append(addr)

        if not allocated:
            p.note('no routed addresses were available (requested %d)' % metal_address_count)
        else:
            msg = 'allocated %s: %s' % (
                progress.count_str(len(allocated), 'routed address'), ', '.join(allocated))
            if len(allocated) < metal_address_count:
                msg += ' (requested %d)' % metal_address_count
            msg += '; the cluster now has %d' % len(md['routed_addresses'])
            p.note(msg)
        self.set_metadata(md)

    def configure_metallb_addresses(self):
        md = self.get_metadata()

        # Setup metallb for traffic ingress, guided by
        # https://itnext.io/kubernetes-loadbalancer-service-for-on-premises-6b7f75187be8
        #
        # Through heredoc(), per rule 2 at the top of this module. The
        # addresses are namespace metadata values.
        metal_lb_config = heredoc(
            '/etc/sf/metallb-range-allocation.yaml',
            'apiVersion: metallb.io/v1beta1\n'
            'kind: IPAddressPool\n'
            'metadata:\n'
            '  name: empty\n'
            '  namespace: metallb-system\n'
            'spec:\n'
            '  addresses:\n'
            '  - %s/32\n'
            '---\n'
            'apiVersion: metallb.io/v1beta1\n'
            'kind: L2Advertisement\n'
            'metadata:\n'
            '  name: empty\n'
            '  namespace: metallb-system\n'
            % '/32\n  - '.join(md['routed_addresses']))

        # Wait on the two workloads rather than on the pods they own. A
        # pod wait resolves its label selector once and then spends a
        # single timeout budget across everything it matched, so one pod
        # which can never report Ready costs the whole five minutes and
        # then fails naming the healthy pods it never reached. That is not
        # hypothetical: remove_worker() drains with --ignore-daemonsets
        # and then deletes the node, which leaves the speaker pod from
        # that node in the API, orphaned and unable to become Ready, until
        # the pod garbage collector catches up. rollout status is computed
        # from the workload's own status counts, which the node's deletion
        # corrects, so it never sees the orphan. Both workloads use the
        # RollingUpdate strategy the chart defaults to, which is what
        # rollout status requires.
        self.execute_and_await(
            [md['control_plane_nodes'][0]],
            [
                ('kubectl rollout status --kubeconfig /etc/rancher/k3s/k3s.yaml '
                 '-n metallb-system deployment/metallb-controller --timeout=300s'),
                ('kubectl rollout status --kubeconfig /etc/rancher/k3s/k3s.yaml '
                 '-n metallb-system daemonset/metallb-speaker --timeout=300s'),
                'mkdir -p /etc/sf',
                metal_lb_config,
                'kubectl apply -f /etc/sf/metallb-range-allocation.yaml'
            ]
        )

    def setup_metallb(self, metal_address_count):
        p = self.get_progress()
        md = self.get_metadata()

        p.phase('Setting up metallb')
        self.allocate_metallb_addresses(metal_address_count)
        self.execute_and_await(
            [md['control_plane_nodes'][0]],
            [
                'kubectl create ns metallb-system',
                # The official metallb chart is used here because Bitnami
                # stopped publishing versioned images to docker.io/bitnami in
                # 2025, so the bitnamicharts/metallb chart installs pods which
                # can never pull their images. Note also that we can't use the
                # KUBECONFIG=... environment variable prefix idiom: the
                # in-guest agent validates the first token of the command line
                # as an executable before running the command.
                'helm repo add metallb https://metallb.github.io/metallb',
                'helm repo update',
                ('helm --kubeconfig /etc/rancher/k3s/k3s.yaml '
                 'upgrade --install -n metallb-system metallb metallb/metallb'),
            ])

        # Give helm's objects a moment to appear. The readiness wait in
        # configure_metallb_addresses() asks after the controller
        # deployment and the speaker daemonset by name, and a name which
        # does not exist yet is an immediate error rather than something
        # the wait sits through.
        time.sleep(5)

        # Add addresses
        self.configure_metallb_addresses()

    def setup_longhorn(self):
        p = self.get_progress()
        md = self.get_metadata()

        # The newest chart this cluster's k3s can run, rather than the
        # newest chart: helm install refuses one whose kubeVersion excludes
        # the cluster (#118).
        version = primitives.get_longhorn_release(
            self.client, self.namespace, self.reporter,
            k3s_version=md['k3s_version'])
        p.phase(f'Setting up longhorn version {version}')

        self.execute_and_await(
            [md['control_plane_nodes'][0]],
            [
                'helm repo add longhorn https://charts.longhorn.io',
                'helm repo update',
                'kubectl create namespace longhorn-system || true',
                (
                    'helm --kubeconfig /etc/rancher/k3s/k3s.yaml '
                    'install longhorn longhorn/longhorn '
                    '--namespace longhorn-system '
                    '--version %s' % shlex.quote(version)
                ),
                (
                    'kubectl patch storageclass local-path -p '
                    '\'{"metadata": {"annotations":{'
                    '"storageclass.kubernetes.io/is-default-class":"false"}}}\''
                )
            ])

    # The methods below are the whole of a k3s command: each one was the
    # body of a Click command in shakenfist_client_k3s/__init__.py, and the
    # command is now argument parsing plus one call into here. They return
    # values rather than printing them, so that a library caller gets the
    # result and the CLI keeps the formatting.
    #
    # Their arguments are what the corresponding command line options carry,
    # minus the name and namespace, which are the Cluster's own. Where an
    # argument is genuinely optional it keeps the option's default, so that
    # omitting it gives the command line's behaviour; where it is not -- the
    # three counts create needs -- it is required, because None is not a
    # workable value for any of them and there is no sensible default for
    # the shape of somebody else's cluster.

    def create(self, control_plane_count, worker_count, metal_address_count,
               network=None, refresh_version_cache=False,
               release_channel='stable', sshkey=None, install_metallb=True,
               install_longhorn=True, write_kubeconfig=False,
               manifests=None,
               control_plane_cpus=DEFAULT_NODE_SIZE['cpus'],
               control_plane_memory=DEFAULT_NODE_SIZE['memory'],
               control_plane_disk=DEFAULT_NODE_SIZE['disk'],
               worker_cpus=DEFAULT_NODE_SIZE['cpus'],
               worker_memory=DEFAULT_NODE_SIZE['memory'],
               worker_disk=DEFAULT_NODE_SIZE['disk'],
               server_config=None, agent_config=None):
        """Build this cluster, from nothing to a working k3s.

        The namespace must already exist. The command line creates it when
        --namespace named one which does not, because only the command line
        knows whether the option was passed at all; see
        _bind_new_cluster_context() in this package's __init__.

        write_kubeconfig is the one parameter here whose default is not the
        command line's behaviour. Writing ~/.kube/config, and shelling out
        to kubectl to merge into an existing one, are side effects on the
        calling machine rather than on the cluster, and a library whose
        default is to rewrite the caller's ~/.kube/config is surprising.
        ``k3s create`` passes True unless --no-kubeconfig was given, so the
        command line is unchanged. Decision 6 of the phase 3 plan records
        why the asymmetry is worth it, and why nothing breaks: this package
        has never been released, so the CLI is the only caller there is.

        The cluster's kubeconfig is fetched and recorded in the metadata
        either way. write_kubeconfig only governs the local file:
        get_kubeconfig() serves what was fetched, and a caller which wants
        the credentials without the side effect asks for them there.

        install_metallb and install_longhorn default to True, matching the
        behaviour before this parameter existed. Which way they went is
        recorded in the metadata as ``metallb_installed`` and
        ``longhorn_installed``, so that a later verb can refuse to drive a
        component this cluster does not have rather than discovering it as
        a timeout: see ``expand_addresses()``. metal_address_count is
        still a required positional argument even when install_metallb is
        False, in which case it is accepted and ignored -- see
        k3s_create()'s --metal-address-count help in __init__.py for why
        that combination is not an error.

        manifests is a list of local file paths, written verbatim into
        k3s's auto-apply directory on the first control plane node before
        k3s is installed there, so that k3s applies them when the server
        first starts. Nothing about them is templated, and their order is
        k3s's business rather than this method's; a payload which needs
        either belongs in a chart the caller installs afterwards (decision
        8 of the phase 3 plan). Staging them is not a phase of its own --
        the writes are extra commands inside the phase which installs k3s
        on that node, so total_phases below does not move with this
        argument.

        control_plane_cpus, control_plane_memory and control_plane_disk
        size every control plane node, and worker_cpus, worker_memory and
        worker_disk every worker. cpus is a count of vCPUs, memory is in MB
        and disk in GB, which are the units the Shaken Fist API takes. They
        are six flat arguments rather than one mapping so that each mirrors
        the command line option of the same name; the nested shape is
        what the metadata records, as ``node_sizes``, and that record is
        why expand_workers() builds new workers at the size this create
        built the first ones rather than at whatever the default is by
        then.

        The defaults are DEFAULT_NODE_SIZE, 2 vCPUs, 2048 MB and 50 GB for
        both roles, which is what every node was built at before these
        arguments existed. 2048 MB is a size a control plane node runs at,
        not one it holds up at; the "Sizing" section of docs/usage.md gives
        the measurement and recommends 4096 MB. It is still the default,
        because changing it would change what an existing invocation
        builds, and validation still accepts any positive integer, because
        a hard minimum would be a guess about workloads this method cannot
        see.
        Nothing here stops a caller building something too small to be
        useful; validate_node_sizes() only stops one which cannot be built
        at all.

        server_config and agent_config are mappings of k3s configuration
        keys, as k3s's own config.yaml spells them (``disable``,
        ``node-label``, ``kubelet-arg`` and so on). server_config is
        written onto every control plane node and agent_config onto every
        worker, as a drop-in in /etc/rancher/k3s/config.yaml.d/ which k3s
        reads after the plugin's own config.yaml, so a caller's key
        replaces the plugin's default for it and a key written with a
        trailing ``+`` appends to it instead. The plugin's defaults include
        a ``node-role.kubernetes.io/control-plane:NoSchedule`` taint on
        every control plane node when the cluster has workers, which
        ``node-taint: []`` in server_config removes. The one default a
        caller cannot override is that servicelb is disabled when MetalLB
        is installed: a further drop-in, read after the caller's, appends
        servicelb to ``disable``, so a caller's ``disable`` is extended
        rather than bringing servicelb back. None, the default, and an
        empty mapping both mean no caller drop-in at all. Both are checked
        by validate_k3s_config() before anything is built: a mapping is
        refused if it sets a key the plugin sets itself or depends on
        (K3S_SERVER_OWNED_KEYS and K3S_AGENT_OWNED_KEYS name each one and
        why; ``tls-san+`` is allowed, and is how to add SANs), if it cannot
        be stored as JSON unchanged, or if it cannot be written through
        the heredoc that carries it. Keys are not checked against k3s's
        flags; k3s ignores one it does not know. Both are recorded in the
        metadata as ``server_config`` and ``agent_config``, which is why
        expand_workers() configures the workers it adds with the
        agent_config this create was given.

        Every create also refuses a k3s release older than
        K3S_RELEASE_FLOOR, v1.21.1, whichever configuration it was given:
        older releases ignore the drop-in directory, or the ``+`` suffix,
        without saying so. check_k3s_release() has the detail.
        """
        # Read the manifests before anything else happens, which is
        # earlier than this function checks any of its other arguments --
        # sshkey is read after the name and the network have been settled.
        # A bad path or a duplicate basename discovered once a network has
        # been allocated and several instances booted is a cluster the
        # caller has to delete before the name can be used again, and the
        # only thing between a library caller and that is this line.
        #
        # The result is kept and handed to install_control_plane() below,
        # rather than letting it read the paths again. There are ten to
        # twenty minutes between here and there, and a file which changed
        # in that window would make this check a check of something else.
        #
        # The node sizes are checked here for the same reason, and the
        # placement matters more than it looks. The name is registered in
        # the cluster list a few lines below, and the metadata document
        # written in state 'initial' shortly after; a size which can never
        # be valid (zero, a string, True) discovered once those exist
        # leaves a claimed name and a document stuck in 'initial' that only
        # a delete clears. Moving this one line later turns a typo into
        # that, which is what test_an_invalid_size_registers_nothing pins.
        # This is not a check of what Shaken Fist will accept: a valid size
        # the API still refuses, for quota or because no hypervisor has the
        # room, fails mid-create, as any other API refusal there does.
        #
        # The k3s configuration is checked here too, for the same reason
        # and with one more: it is recorded in the initial metadata below,
        # which is JSON, so a value set_metadata() cannot store would
        # otherwise surface as a failed write after the name is claimed.
        # The text validate_k3s_config() returns is not kept: what is
        # recorded is the mapping, so that show displays its structure
        # rather than a block of YAML.
        staged_manifests = read_manifests(manifests)
        node_sizes = {
            'control_plane': {
                'cpus': control_plane_cpus,
                'memory': control_plane_memory,
                'disk': control_plane_disk,
            },
            'worker': {
                'cpus': worker_cpus,
                'memory': worker_memory,
                'disk': worker_disk,
            },
        }
        validate_node_sizes(node_sizes)
        validate_k3s_config(server_config, 'server')
        validate_k3s_config(agent_config, 'agent')

        # Phases: create control plane nodes, create workers, install control
        # plane, install workers, fetch credentials, metallb, longhorn, and
        # update the local kubeconfig. Creating a node network and installing
        # additional control plane nodes only sometimes happen; metallb and
        # longhorn are each skipped -- and their phase uncounted -- when the
        # corresponding install_* flag is False; and the local kubeconfig
        # update goes the same way when write_kubeconfig is False. Note that
        # this last one is subtracted by default, because that flag defaults
        # to False rather than to True.
        total_phases = 8
        if not network:
            total_phases += 1
        if control_plane_count > 1:
            total_phases += 1
        if not install_metallb:
            total_phases -= 1
        if not install_longhorn:
            total_phases -= 1
        if not write_kubeconfig:
            total_phases -= 1
        p = self.start_progress(total_phases)

        self.reporter.debug('Looking up k3s versions')
        target_release = primitives.get_k3s_release(
            self.client, self.namespace, self.reporter,
            force_cache_update=refresh_version_cache,
            release_channel=release_channel)
        # Refused here, straight after the lookup and still before the
        # name is registered below, for the same reason as the checks at
        # the top of this method. It cannot join them there: the release
        # is not known until the channel has been resolved, and this is the
        # first point at which it is.
        check_k3s_release(target_release, release_channel)

        # Ensure this name isn't already taken
        namespace_md = self.client.get_namespace_metadata(self.namespace)
        all_clusters = namespace_md.get(primitives.CLUSTER_LIST, [])
        md = self.get_metadata()

        # The metadata is consulted before the cluster list, which is a
        # change of order rather than of behaviour: both checks raised the
        # same ClusterExistsError, so which fired first did not matter
        # until now. It matters now because only the metadata knows whether
        # the cluster holding this name ever finished being built, and an
        # interrupted create leaves the name in the cluster list too. Asking
        # the list first would answer "that name is taken" for the one case
        # which has a more useful answer than that.
        if md:
            interrupted = self._interrupted_state(md)
            if interrupted:
                raise exceptions.ClusterInterruptedError.mid_create(
                    self.name, interrupted)
            raise exceptions.ClusterExistsError(self.name)
        if self.name in all_clusters:
            raise exceptions.ClusterExistsError(self.name)
        all_clusters.append(self.name)
        self.client.set_namespace_metadata_item(
            self.namespace, primitives.CLUSTER_LIST, all_clusters)

        # Create a network for nodes
        if network:
            node_network = self.client.get_network(network)
            if not node_network:
                raise exceptions.NetworkNotFoundError(network)
        else:
            p.phase('Creating node network')
            node_network = self.client.allocate_network(
                '10.0.0.0/16', True, True, 'k3s-%s-node' % self.name,
                namespace=self.namespace)
            p.note('created %s (uuid %s)' % (node_network['name'], node_network['uuid']))
            while True:
                node_network = self.client.get_network(node_network['uuid'])
                p.update(node_network['name'], 'state %s' % node_network['state'])
                if node_network['state'] == 'created':
                    break
                time.sleep(1)
            p.wait_done()

        # Read the ssh key if any. Guarded and with the encoding stated for
        # the same two reasons read_manifests() is: this is the other local
        # path a caller hands in, an OpenSSH public key's comment field can
        # hold any bytes the user put in it, and a library caller which
        # catches K3sClusterException should not have to catch OSError as
        # well to survive a path which does not exist. The CLI's own
        # click.Path(exists=True) covers only the CLI.
        ssh_key_content = None
        if sshkey:
            try:
                with open(sshkey, encoding='utf-8') as f:
                    ssh_key_content = f.read()
            except (OSError, UnicodeDecodeError) as e:
                raise exceptions.SshKeyError.unreadable(sshkey, str(e))

        # Initialise the metadata
        self.reporter.debug('Initialize cluster metadata')
        md = {
            'name': self.name,
            'namespace': self.namespace,
            'type': 'k3s',
            'k3s_version': target_release,
            'k3s_version_history': [target_release],
            'plugin_version': distribution_version('shakenfist_client_k3s'),
            'state': 'initial',
            'node_serial': 1,
            'node_network': node_network['uuid'],
            'node_token': None,
            'control_plane_nodes': [],
            'worker_nodes': [],
            'routed_addresses': [],
            'ssh_key': ssh_key_content,

            # What this cluster has, not what this call was asked for:
            # every verb which drives one of these components has to know
            # whether it is there, and the metadata is the only thing
            # which outlives the call. They are recorded here rather than
            # next to the setup_*() calls below so that a create which
            # never reaches those still describes the cluster it was
            # building. Readers must treat a missing key as True, because
            # every cluster built before this key existed has both.
            #
            # Only metallb_installed has readers today: expand_addresses(),
            # and _k3s_config_commands(), which disables servicelb on the
            # servers of a cluster with MetalLB. longhorn_installed is
            # written for the same reason and read by nothing, because no
            # verb drives Longhorn after create(); health() growing a
            # storage check is the obvious first reader. That is the
            # position md['state'] was in for this package's whole history
            # until phase 3 found it (survey finding 2), so it is said out
            # loud here rather than left for somebody to rediscover.
            'metallb_installed': install_metallb,
            'longhorn_installed': install_longhorn,

            # The size of each role's nodes, recorded here with the rest
            # of the initial metadata for the same reason as the two flags
            # above: before any instance exists, so that an interrupted
            # create still describes what it was building. Its reader is
            # create_instance(), through _node_size(), which is what makes
            # expand-workers build new workers at the size this create
            # chose rather than at the default. Readers must fall back to
            # DEFAULT_NODE_SIZE when the key is missing, because every
            # cluster built before it existed has none -- and that fallback
            # is exact rather than a guess, because those clusters could
            # only ever have been built at the default. show() reports the
            # fallback for the same reason.
            'node_sizes': node_sizes,

            # The caller's k3s configuration for each role, as the mapping
            # it was given rather than the YAML it is written as, so that
            # show displays structure. Recorded here for the same reasons
            # as node_sizes, and read the same way: by role, when k3s is
            # installed on a node, which is what makes expand-workers
            # configure the workers it adds with the agent_config this
            # create was given. Readers must treat a missing key as {},
            # and that too is exact rather than a guess, because a cluster
            # built before these keys existed had no way to be given a
            # configuration. show() reports the fill for the same reason.
            'server_config': server_config or {},
            'agent_config': agent_config or {},
        }
        self.set_metadata(md)

        # We really should do a pre-fetch on the disk image and wait for it to
        # download before starting instances. That way the point of slowness is
        # more obvious. That requires cluster operations to exist though.

        # I'd prefer to wait for these as one thing, but that's not currently a thing
        # the code supports.
        self.create_and_await_instances(control_plane_count, 'control_plane')
        self.create_and_await_instances(worker_count, 'worker')

        # Pick the metadata up again, here and at the two other points
        # below where a method this function called has written to it.
        # These three lines are no-ops today and deliberately so:
        # get_metadata() caches the dictionary, set_metadata() stores that
        # same object, so the md this function is holding is already the
        # one create_and_await_instances() appended the new nodes to. That
        # aliasing is what makes the control_plane_nodes read below and the
        # worker_nodes argument to install_workers() correct, and nothing
        # said so. Re-reading costs no API call -- the cache answers -- and
        # leaves this function correct whether the cache aliases or copies,
        # which the alternative (one md held across the whole create, with
        # a set_metadata() at the end writing it back over everybody else's
        # work) is not. Recorded for this step by 3a's commit message.
        md = self.get_metadata()

        # Record the node network address for the first control plane node as the API
        # address
        interfaces = self.client.get_instance_interfaces(md['control_plane_nodes'][0])
        md['api_address_inner'] = interfaces[0]['ipv4']
        md['api_address_floating'] = interfaces[0]['floating']

        # The join address is the address new nodes register through, and is
        # deliberately mutable cluster state rather than "the first control
        # plane node's address": a future control plane replacement joins the
        # new server via the old address, updates join_address, and then reaps
        # the old node. k3s agents only need this address at registration time.
        md['join_address'] = interfaces[0]['ipv4']
        self.set_metadata(md)

        self.install_control_plane(manifests=manifests,
                                   staged=staged_manifests)
        self.install_workers(md['worker_nodes'])

        # install_control_plane() recorded the two registration tokens.
        md = self.get_metadata()

        # Fetch kubecfg, correct IP, and include cluster name instead of "default"
        p.phase('Fetching cluster credentials')
        aop = self.client.instance_get(
            md['control_plane_nodes'][0], '/etc/rancher/k3s/k3s.yaml')
        kc = yaml.safe_load(self.await_fetch(aop))

        # Set rather than rewritten. k3s writes https://127.0.0.1:6443 here
        # only when no bind-address is configured, and the bind address when
        # one is, which a caller's server_config can do; substituting for
        # 127.0.0.1 would then leave the kubeconfig pointing at the bind
        # address, with nothing failing. 6443 is the port K3S_URL assumes,
        # and https-listen-port is an owned key.
        kc['clusters'][0]['cluster']['server'] = (
            'https://%s:6443' % md['api_address_floating'])
        fqcn = '%s.%s' % (self.name, self.namespace)
        kc['clusters'][0]['name'] = fqcn
        kc['contexts'][0]['name'] = fqcn
        kc['contexts'][0]['context']['cluster'] = fqcn
        kc['contexts'][0]['context']['user'] = fqcn
        kc['users'][0]['name'] = fqcn
        kc['current-context'] = fqcn
        md['kubeconfig'] = yaml.dump(kc)
        self.set_metadata(md)

        # Install metallb and longhorn, unless the caller opted out of one
        # or both of them.
        if install_metallb:
            self.setup_metallb(metal_address_count)
        if install_longhorn:
            self.setup_longhorn()

        # Install the kubeconfig we fetched earlier, if the caller wants the
        # local side effect. The fetch above is unconditional -- the cluster
        # credentials are in the metadata either way -- and only the write to
        # ~/.kube/config and the kubectl merge into an existing one are gated
        # here. See create()'s docstring, and decision 6 of the phase 3 plan.
        if write_kubeconfig:
            p.phase('Updating local kubeconfig')
            # Every component of these paths is chosen by this module -- the
            # user's home directory, a fixed directory name, a fixed file
            # name, and below a tempfile directory -- so the joins need no
            # containment check and there is nothing for a realpath() guard
            # to prove. An outside value appearing in one of them later
            # would need both.
            kube_dir = os.path.join(os.path.expanduser('~'), '.kube')
            main_config_path = os.path.join(kube_dir, 'config')

            # 0700, rather than whatever the process umask makes of 0777. A
            # k3s kubeconfig embeds client-certificate-data and
            # client-key-data for a cluster-admin identity, so on a default
            # umask 022 host this directory and the file below were readable
            # by every local user -- which is the normal situation on a
            # shared jump box or a CI runner. exist_ok leaves an existing
            # directory's mode alone, so a user who has already tightened
            # theirs keeps it, and one who has loosened it is not silently
            # overridden.
            os.makedirs(kube_dir, mode=0o700, exist_ok=True)

            if not os.path.exists(main_config_path):
                # There is no existing configuration to preserve, so no merge is
                # required and we don't need a local kubectl.
                #
                # os.open() with an explicit mode rather than open() and a
                # chmod afterwards, so that the file is never briefly
                # world readable between being created and being tightened.
                # Only this branch creates the file: the merge branch below
                # rewrites one which already exists, which keeps the mode
                # the user's own kubeconfig had.
                fd = os.open(main_config_path,
                             os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with open(fd, 'w', encoding='utf-8') as f:
                    f.write(yaml.dump(kc))
            else:
                if not shutil.which('kubectl'):
                    raise exceptions.KubeconfigError.missing_kubectl(
                        main_config_path, self.name)

                with tempfile.TemporaryDirectory() as tempdir:
                    new_config_path = os.path.join(tempdir, 'config')
                    with open(new_config_path, 'w', encoding='utf-8') as f:
                        f.write(yaml.dump(kc))
                    # An argument list, per the third form of the rule
                    # at the top of this module. Nothing here is
                    # interpolated, so the shell had nothing to find and
                    # this is consistency rather than a fix -- but a reader
                    # comparing this with delete()'s unset calls should not
                    # have to work out for themselves that the difference
                    # does not matter, and spawning a shell to run a
                    # constant buys nothing. The two paths travel as
                    # environment values rather than as argv either way.
                    merged = subprocess.run(
                        ['kubectl', 'config', 'view', '--flatten'],
                        capture_output=True,
                        env={**os.environ,
                             'KUBECONFIG': '%s:%s' % (main_config_path, new_config_path)})
                    if merged.returncode != 0:
                        # kubectl's stderr arrives as bytes, and was decoded at the
                        # point it was printed; decode it here so the exception
                        # renders exactly the same text.
                        stderr = None
                        if merged.stderr:
                            stderr = merged.stderr.decode('utf-8', errors='replace')
                        raise exceptions.KubeconfigError.merge_failed(
                            main_config_path, merged.returncode, stderr)

                    # kubectl's merge keeps the pre-existing file's current-context,
                    # which would leave kubectl pointed at whatever cluster was
                    # active before this create. Select the new cluster, matching
                    # the no-merge path above.
                    merged_kc = yaml.safe_load(merged.stdout)
                    merged_kc['current-context'] = fqcn
                    with open(main_config_path, 'w', encoding='utf-8') as f:
                        f.write(yaml.dump(merged_kc))

        # setup_metallb() recorded the routed addresses it allocated, and
        # this is the write which would otherwise put a stale local
        # dictionary back over them.
        md = self.get_metadata()
        md['state'] = 'created'
        self.set_metadata(md)
        p.finish(f'Cluster {self.name} is ready')

    def get_kubeconfig(self):
        """Return this cluster's kubeconfig, as a string.

        This is the body of ``sf-client k3s getconfig``, which prints what
        this returns.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.unknown_cluster(self.name)

        kubeconfig = md.get('kubeconfig')
        if not kubeconfig:
            # The cluster exists, it is just not finished, which is a
            # different thing to it not existing at all.
            raise exceptions.ClusterIncompleteError(self.name)

        return kubeconfig

    def show(self):
        """Return this cluster's metadata, or raise if there is no such cluster.

        This is the body of ``sf-client k3s show``, which formats what this
        returns. It differs from get_metadata() only in insisting that the
        cluster exists, and in the error it raises when it does not.

        A cluster which never finished being built is reported rather than
        refused: show is the verb for looking at a cluster which is not
        working, so raising here would take away the one tool which can say
        why. The state is in the returned metadata as ``state``, and is all
        a library caller needs; the note this writes to the reporter is for
        the human running ``sf-client k3s show``, who would otherwise have
        to know that ``state = initial`` in a screenful of key/value pairs
        is the line that matters and that the answer to it is a delete.

        ``node_sizes``, ``server_config`` and ``agent_config`` are the three
        keys this reports which may not be stored. A cluster created before
        node sizes were recorded has no ``node_sizes``, and for one of those
        this returns a copy of the metadata with ``node_sizes`` filled in
        from DEFAULT_NODE_SIZE for both roles.
        That departs from "show reports what is stored", and it is right
        here because the filled in values are a statement of fact rather
        than a guess: before the key existed there was no way to build a
        node at any size but the default, so every node such a cluster has
        was built at exactly that. It is also what expand-workers will
        build for it, through _node_size(), so show and the next expand
        agree. Nothing is written back -- show stays read only, and the
        stored document is neither changed nor rewritten -- which is why
        the fill is on a deep copy rather than on the cached dictionary.

        A record which is present but partial -- which the plugin never
        writes, but anything holding the namespace's credentials could -- is
        completed per field in the same way, for the same reason: it is
        what expand-workers would build. When the record is complete it is
        returned as stored, and the metadata is the cached dictionary
        itself, exactly as it was before this key existed.

        ``server_config`` and ``agent_config`` are filled the same way, with
        ``{}`` when absent, and for the same reason: a cluster created
        before they were recorded had no way to be given a k3s
        configuration, so an empty one is what it has, and what
        expand-workers will write for it. The fills share one deep copy
        with ``node_sizes``, and a cluster which has all three stored is
        still returned as the cached dictionary.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.does_not_exist(self.name)

        interrupted = self._interrupted_state(md)
        if interrupted:
            self.reporter.write(
                "Cluster %s is in state '%s' rather than 'created': it was "
                'interrupted while it was being built, and is not usable.\n'
                "Remove what is left of it with 'sf-client k3s delete %s'.\n"
                % (self.name, interrupted, self.name))

        # Filled through _node_size() rather than from DEFAULT_NODE_SIZE
        # directly, so that what show reports and what create_instance()
        # builds come from the same line -- for an old cluster with no
        # record, and for a record somebody else left partial, which
        # _node_size() completes per field.
        node_sizes = {
            node_type: self._node_size(md, node_type)
            for node_type in ('control_plane', 'worker')
        }
        fills = {}
        if md.get('node_sizes') != node_sizes:
            fills['node_sizes'] = node_sizes
        for key in ('server_config', 'agent_config'):
            if key not in md:
                fills[key] = {}

        # One deep copy for all of the fills rather than one per key: the
        # copy exists so that the cached dictionary is not changed, and
        # copying it again for each key would protect nothing further.
        if fills:
            md = copy.deepcopy(md)
            md.update(fills)

        return md

    def health(self):
        """Report the state of this cluster and of every node in it, and repair nothing.

        This is the body of ``sf-client k3s health``, which renders what
        this returns. Per decision 7 of
        ``docs/plans/PLAN-library-api-and-collection-phase-03-missing-verbs.md``
        it returns structured data rather than text, so that phase 5's
        Ansible module can branch on it without parsing anything, and it
        performs no repair: a verb which silently fixes things cannot be
        used to decide whether to fix things.

        Nothing about an unhealthy cluster raises. An instance in the error
        state, an instance the metadata names which no longer exists, a
        cluster which was interrupted mid-create, a cluster with no control
        plane node at all, and a kubectl which exits non-zero are all
        findings in the returned report. The only thing which raises is a
        cluster this namespace has no metadata for, which is not an
        unhealthy cluster but a question about a cluster that does not
        exist.

        Nor does it hang. The k3s API probe is only attempted when the node
        it would be run on looks able to answer -- the node entry this
        method has just built says whether the instance exists, is created
        and has a ready agent -- and it carries a wall clock timeout even
        then. An agent operation queued against an instance whose agent is
        not connected never leaves its queued state, so a probe which is
        attempted anyway waits forever on exactly the cluster this verb
        exists to describe. Every one of those outcomes is ``probed``
        False with an ``error`` saying which, so a caller never has to
        tell them apart by which keys are present.

        The report is::

            {
                'name': str,                # this cluster's name
                'namespace': str,           # the namespace it lives in
                'state': str,               # md['state'], or 'unknown'
                'interrupted': bool,        # state is not 'created'
                'nodes': [
                    {
                        'uuid': str,            # the Shaken Fist instance uuid
                        'role': str,            # 'control_plane' or 'worker'
                        'name': str or None,    # the instance name, None if gone
                        'exists': bool,         # the instance still exists
                        'state': str or None,   # the instance state
                        'agent_state': str or None,
                        'healthy': bool         # created, and its agent ready
                    },
                    ...
                ],
                'api': {
                    'probed': bool,             # the command was run at all
                    'answered': bool,           # ...and it exited zero
                    'instance_uuid': str or None,
                    'command': str or None,
                    'return_code': int or None,
                    'stdout': str or None,      # 'kubectl get nodes' output
                    'stderr': str or None,
                    'error': str or None        # why it did not answer
                },
                'healthy': bool             # all of the above agree
            }

        ``nodes`` lists the control plane nodes first and then the workers,
        each group in the order the metadata holds them, which is the order
        they were created in. ``agent_state`` is the raw API value, so it is
        None on an instance the agent has never reached; rendering that as
        'not yet contactable' is the caller's business, as it is in
        await_boot().

        The top level ``healthy`` is the conjunction a caller would
        otherwise have to write itself: the cluster finished being built,
        every node in it exists and is up, and the k3s API answered. The
        ``all()`` over an empty node list is True, and there is deliberately
        no separate "and it has at least one node" term, because there is no
        report in which that term could change the answer: the probe runs on
        ``md['control_plane_nodes'][0]``, so ``api['answered']`` can only be
        True for a cluster which has at least one control plane node, and
        therefore at least one node. A cluster with no nodes reports
        unhealthy because there was no k3s API to ask, which is the same
        answer for the more informative reason.

        Unlike expand_workers(), remove_worker() and expand_addresses() this
        does not call _require_usable(): reporting on a cluster which never
        finished being built is exactly what the verb is for, so an
        interrupted cluster is described rather than refused. It must
        therefore not assume anything create() records, which is why
        md['control_plane_nodes'] being empty is a finding about the API
        probe rather than an IndexError.

        One thing this leaves behind, which matters to a caller polling it in
        a loop: the probe submits an agent operation, and when it gives up
        waiting the operation is still queued against the control plane node.
        Nothing here reaps it, because there is nothing to reap it with -- the
        command may yet run -- so the server's own deadline ends it, and until
        then an await_idle() in a later expand-workers or update-os waits for
        it along with everything else. The uuid of an abandoned operation is in
        ``api['error']`` so that wait can be accounted for rather than
        guessed at. This is bounded rather than free: a reconcile loop
        polling health() against a node whose agent is intermittently slow
        pays for it in a delayed later verb, not in a hang.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.does_not_exist(self.name)

        nodes = []
        for role, md_key in (('control_plane', 'control_plane_nodes'),
                             ('worker', 'worker_nodes')):
            for instance_uuid in md.get(md_key) or []:
                nodes.append(self._node_health(instance_uuid, role))

        # The k3s API is asked through the first control plane node, which an
        # interrupted create may never have made. That is a finding rather
        # than an error, so it is reported the same way a kubectl which
        # exits non-zero is.
        control_plane = md.get('control_plane_nodes') or []
        first = nodes[0] if control_plane else None
        if first and first['exists'] and first['healthy']:
            api = self._probe_k3s_api(control_plane[0])
        elif not control_plane:
            api = self._unprobed(
                None,
                'this cluster has no control plane node to ask: its metadata '
                'lists none, so there is no k3s API')
        else:
            # The probe is skipped rather than attempted, because the
            # attempt is what used to hang: an agent operation queued
            # against an instance whose agent is not connected never
            # leaves its queued state, and the node entry above has
            # already read the state and agent_state which say so. There
            # is nothing to learn from asking, and this is the cluster the
            # verb most needs to answer about.
            api = self._unprobed(
                control_plane[0],
                'the first control plane node is not in a state which can '
                'answer: instance %s, agent %s'
                % (first['state'] or 'gone',
                   first['agent_state'] or 'not contactable'))

        # _interrupted_state() answers 'unknown' rather than None for
        # metadata carrying no state at all, so interrupted is True for that
        # case too, which is what it should be: a document this package did
        # not write describes a cluster we cannot vouch for.
        interrupted = self._interrupted_state(md) is not None

        return {
            'name': self.name,
            'namespace': self.namespace,
            'state': md.get('state', 'unknown'),
            'interrupted': interrupted,
            'nodes': nodes,
            'api': api,
            'healthy': (not interrupted
                        and all(node['healthy'] for node in nodes)
                        and api['answered'])
        }

    def delete(self, update_kubeconfig=False):
        """Destroy this cluster and everything created alongside it.

        This is the body of ``sf-client k3s delete``.

        update_kubeconfig governs one thing: whether this cluster's entries
        are removed from the local ~/.kube/config. It is the counterpart of
        ``create()``'s write_kubeconfig and it defaults off for the same
        reason -- the calling machine's kubectl configuration is not part of
        the cluster, and a library should not edit it unasked. ``k3s
        delete`` passes True unless --no-kubeconfig was given, so the
        command line is unchanged. Decision 6 of the phase 3 plan has the
        argument.

        Leaving it off strands whatever ``create(write_kubeconfig=True)``
        wrote, which is why the two are symmetrical rather than
        independently defaulted: a caller which asked for the write asks for
        the cleanup too.

        This works on a cluster which never reached ``created``, and that
        is the only way out of an interrupted create: decision 5 of the
        phase 3 plan scopes the state machine to detection and teardown, so
        ``create()`` refuses such a name and points here. Nothing below
        assumes the cluster was ever finished -- the node lists are empty
        rather than absent, the network teardown is already guarded, and
        the local kubectl cleanup is idempotent -- so the only change this
        needed was to say what it is doing.
        """
        # Ensure this name exists
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.does_not_exist(self.name)

        # Not a debug line: a delete which follows a failed create is the
        # supported recovery, and an operator running it wants to be told
        # that this is the cluster they think it is before their instances
        # go away. A cluster which reached 'created' says nothing new.
        interrupted = self._interrupted_state(md)
        if interrupted:
            self.reporter.write(
                "Cluster %s is in state '%s' rather than 'created': it never "
                'finished being built.\nRemoving whatever it did create.\n'
                % (self.name, interrupted))

        # Redacted here rather than relied on not being reached. The
        # Ansible module leaves its reporter non-verbose and calls that a
        # security property because of this loop, which makes a security
        # property that holds only while one caller remembers a flag; and
        # -v is exactly the flag somebody adds when a delete is failing,
        # which is also when they paste the output into a bug report.
        #
        # The caller's k3s configuration is redacted whole whenever there is
        # any. k3s takes credentials inline as configuration keys
        # (etcd-s3-secret-key, agent-token, a datastore-endpoint carrying a
        # password), and a list of those here would go stale with k3s.
        self.reporter.debug('Cluster metadata:')
        for k in md:
            if ((k in SECRET_METADATA_KEYS and md[k] is not None)
                    or (k in ('server_config', 'agent_config') and md[k])):
                self.reporter.debug('    %s = %s' % (k, progress.REDACTED))
            else:
                self.reporter.debug('    %s = %s' % (k, md[k]))

        # Delete instances
        waiting = []
        for instance_uuid in set(md['control_plane_nodes'] + md['worker_nodes']):
            try:
                inst = self.client.get_instance(instance_uuid)
                self.reporter.debug('...Deleting instance %s with uuid %s'
                                    % (inst['name'], instance_uuid))
                self.client.delete_instance(instance_uuid)
                waiting.append(instance_uuid)
            except apiclient.ResourceNotFoundException:
                pass

        while waiting:
            self.reporter.debug(
                '...Waiting for %d instances to be deleted' % len(waiting))
            for instance_uuid in copy.copy(waiting):
                try:
                    i = self.client.get_instance(instance_uuid)
                    if i['state'] == 'deleted':
                        waiting.remove(instance_uuid)
                except apiclient.ResourceNotFoundException:
                    waiting.remove(instance_uuid)

            if waiting:
                time.sleep(1)

        md['control_plane_nodes'] = []
        md['worker_nodes'] = []
        # api_address_floating and api_address_inner, which is what create()
        # writes and what install_control_plane() and install_k3s_component()
        # read. This used to clear api_floating_address and
        # api_inner_address -- the words transposed -- so it invented two
        # keys nothing else in the package has ever used and left the two
        # real ones in the document.
        md['api_address_floating'] = None
        md['api_address_inner'] = None
        md['k3s_version'] = None
        md['kubeconfig'] = None
        md['node_token'] = None
        self.set_metadata(md)

        if md.get('node_network'):
            # Free any routed ips
            for addr in md.get('routed_addresses', []):
                try:
                    self.reporter.debug('Unrouting address %s from network %s'
                                        % (addr, md['node_network']))
                    self.client.unroute_network_address(
                        md['node_network'], addr)
                except apiclient.UnauthorizedException:
                    self.reporter.debug(
                        '...Address %s was not routed to this network' % addr)

            # Delete node network. This deletes the node network whether or
            # not create allocated it, so a network handed to
            # "create --network" is destroyed along with the cluster which
            # borrowed it. That is shakenfist/client-python-k3s#41, and it is
            # preserved here deliberately: this step moves code without
            # changing what it does, and the fix belongs in its own change.
            self.client.delete_network(md['node_network'])
            # None, not []: everywhere else this key holds a network uuid
            # string, and create_instance() reads it as one.
            md['node_network'] = None

        md['state'] = 'deleted'
        self.set_metadata(md)

        # Then release the name, and only then remove the metadata
        # document. These two writes are not atomic and this is the order
        # which makes an interruption between them recoverable.
        #
        # The other order -- which this did until now -- leaves the name
        # in the cluster list with no metadata document behind it, and
        # that combination is unusable forever: delete() raises
        # ClusterNotFoundError because there is no metadata, and create()
        # raises ClusterExistsError because the name is in the list. This
        # order leaves a metadata document in state 'deleted' whose name
        # is no longer listed, which delete() runs to completion over --
        # the instances and the network are already gone, and the steps
        # above tolerate that -- so the recovery is to run the delete
        # again. That is the same recovery an interrupted create has, and
        # the same one docs/usage.md documents for
        # shakenfist/client-python-k3s#72.
        namespace_md = self.client.get_namespace_metadata(self.namespace)
        all_clusters = namespace_md.get(primitives.CLUSTER_LIST, [])

        # Tested for rather than removed unconditionally: list.remove()
        # raises a bare ValueError, which is not a K3sClusterException, so
        # the group handler does not catch it and the user sees a
        # traceback. A second delete of a cluster this one has already
        # unlisted is exactly the recovery described above, and two
        # concurrent deletes reach it as well.
        if self.name in all_clusters:
            all_clusters.remove(self.name)
        if not all_clusters:
            self.client.delete_namespace_metadata_item(
                self.namespace, primitives.CLUSTER_LIST)
        else:
            self.client.set_namespace_metadata_item(
                self.namespace, primitives.CLUSTER_LIST, all_clusters)

        self.delete_metadata()

        # And remove the local config, if the caller wants the local side
        # effect. Everything above this point is the cluster; this is the
        # calling machine's kubectl configuration.
        if update_kubeconfig:
            fqcn = '%s.%s' % (self.name, self.namespace)
            for config_elem in ['users.%s' % fqcn,
                                'contexts.%s' % fqcn,
                                'clusters.%s' % fqcn]:
                # An argument list, not a shell string: config_elem
                # interpolates the cluster name, which arrives from a
                # click.STRING argument, an Ansible playbook variable or an
                # API request with no validation anywhere on the path, so a
                # name containing shell metacharacters would otherwise run
                # as a command.
                #
                # That settles injection and not kubectl's own grammar,
                # which is a separate question the paragraph above should
                # not be read as answering. 'kubectl config unset' resolves
                # its argument as a dot separated path into the config
                # structure -- 'users' is a map, the next segment is the
                # key, and a further segment is a field of the result -- so
                # a cluster name containing a dot produces a path with an
                # extra segment that kubectl cannot resolve, and the
                # non-zero exit below becomes a KubeconfigError. By then
                # the cluster really is gone and delete_metadata() has run,
                # so re-running the delete raises ClusterNotFoundError and
                # the stale entries stay in ~/.kube/config. 'my.cluster' is
                # a name somebody will type. Validating the cluster name on
                # the way in is what fixes it, and is
                # shakenfist/client-python-k3s#96.
                unset = subprocess.run(
                    ['kubectl', 'config', 'unset', config_elem],
                    capture_output=True)

                # capture_output is what stops kubectl's three 'Property
                # "..." unset.' lines going to the process's file descriptor
                # 1, which the reporter does not own and a caller emitting
                # JSON there cannot afford. They are not thrown away: the
                # reporter gets them, at debug level, because they only
                # confirm something the caller asked for.
                if unset.stdout:
                    self.reporter.debug(
                        unset.stdout.decode('utf-8', errors='replace').rstrip())

                if unset.returncode != 0:
                    # And this is the other half of capturing the output:
                    # kubectl's explanation of the failure used to reach the
                    # terminal on its own, so it now has to be carried by
                    # the exception. Decoded at the raise, matching
                    # merge_failed() on the create side.
                    stderr = None
                    if unset.stderr:
                        stderr = unset.stderr.decode('utf-8', errors='replace')
                    raise exceptions.KubeconfigError.unset_failed(
                        config_elem, stderr)

    def expand_workers(self, worker_count):
        """Add worker nodes to this cluster.

        This is the body of ``sf-client k3s expand-workers``. The cluster
        must have finished being built: the new workers join with
        ``md['node_token']``, which an interrupted create may never have
        fetched, and a k3s agent install carrying a token of None builds
        instances which are charged for and can never join anything.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.not_found(self.name)
        self._require_usable(md, 'expand-workers')

        p = self.start_progress(2)
        new_workers = self.create_and_await_instances(worker_count, 'worker')
        self.install_workers(new_workers)
        p.finish(f'Added {worker_count} workers to cluster {self.name}')

    def remove_worker(self, instance_uuids):
        """Remove worker nodes from this cluster, draining each one first.

        This is the body of ``sf-client k3s remove-worker``. Each worker is
        drained and removed from k3s before its Shaken Fist instance is
        destroyed: deleting the instance first leaves a NotReady node object
        in the cluster forever, and the workloads which were running on it
        are only rescheduled once the node controller's eviction timeout
        expires. See decision 3 in
        ``docs/plans/PLAN-library-api-and-collection-phase-03-missing-verbs.md``.

        The drain needs a cluster to drain the node out of, so a cluster
        which never finished being built is refused rather than allowed to
        fail on ``md['control_plane_nodes'][0]``, or on a drain aimed at a
        node where k3s was never installed.

        Every uuid is checked against this cluster's worker list, and every
        node name resolved from its instance, before anything is drained or
        deleted, so a typo in the third of three arguments fails the call
        rather than destroying the first two. Workers are then removed one
        at a time, each committed to metadata before the next is started, so
        an interrupted run leaves the metadata describing the cluster which
        actually exists.

        Removing the last worker is allowed, and is the caller's business:
        a k3s server node is schedulable, so a cluster with no workers is a
        working cluster, and conductor's workers are ephemeral CI runners
        which legitimately go to zero.

        A drain which cannot finish is bounded and undone. ``kubectl
        drain`` blocks while a pod has nowhere else to go -- a
        PodDisruptionBudget which refuses the eviction, an unmanaged pod
        which needs ``--force``, the last worker of a cluster with
        workloads pinned to it -- so it is given ``--timeout``
        (KUBECTL_DRAIN_TIMEOUT), and it then exits non-zero and says why
        rather than sitting there until Shaken Fist takes the agent
        operation's deadline away. Either way the node is left cordoned,
        because cordoning is the drain's first act, so this uncordons it
        before re-raising: a refused removal has to leave the cluster as it
        found it, not one node short of schedulable capacity with nothing
        in this package which would put it back. The ``kubectl delete
        node`` which follows a successful drain is inside the same guard,
        because by then the node is not only cordoned but empty, so a
        failure there is the case which most needs the uncordon.

        Only one failure is not undone, and it is the one where there is
        nothing left to undo: a ``delete_instance`` which fails after the
        node object is gone. The worker stays in this cluster's metadata so
        that ``delete`` still destroys the instance and health() still
        reports it, and the message says which instance to delete by hand.

        An instance in ``md['worker_nodes']`` which no longer exists is
        removed from the metadata rather than refused. There is no node to
        drain and no instance to destroy, so both are skipped and the entry
        goes; this is the same state health() reports as a finding and
        delete() tolerates per instance, and remove-worker is the only verb
        which can clear it.

        An empty list is an accepted no-op, so a caller computing the list
        programmatically -- phase 5's Ansible module, a conductor reconcile
        loop -- does not have to guard the call. It returns before building
        a Progress, because a Progress with no phases prints a completion
        line for work nobody asked for.

        This does not wait for the instances to reach the deleted state.
        The drain is what makes the removal safe for the cluster, and it
        has already completed by the time the instance is destroyed.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.not_found(self.name)
        self._require_usable(md, 'remove-worker')

        # Repeating --worker with the same uuid would otherwise drain a node
        # which has already been removed, and then fail on the second
        # list.remove(). Deduplicate rather than reject: the caller asked
        # for that worker to be gone, and it will be.
        wanted = []
        for instance_uuid in instance_uuids:
            if instance_uuid not in wanted:
                wanted.append(instance_uuid)

        # Validate all of them before touching anything. This is the whole
        # reason the loop below is not the only loop in this method.
        unknown = [instance_uuid for instance_uuid in wanted
                   if instance_uuid not in md['worker_nodes']]
        if unknown:
            raise exceptions.WorkerNotFoundError(self.name, unknown)

        if not wanted:
            return

        p = self.start_progress(len(wanted))

        # Every node name is resolved before anything is drained or deleted,
        # for the reason the uuid check above runs first: this loop destroys
        # things, and a run which discovers a problem on its third worker
        # has already destroyed the first two. Reading three instances costs
        # three API calls and turns "half the workers are gone and the
        # command failed" into a refusal.
        #
        # k3s names a node after the hostname of the machine it runs on, and
        # Shaken Fist derives the guest's hostname from the instance's name:
        # the config drive it builds sets meta_data.json's "hostname" to
        # "<instance name>.local" (see shakenfist/instance.py), which
        # cloud-init applies as the short hostname. There is no separate
        # hostname field in the instance API representation to read instead,
        # so 'name' is the field, and it is read from the instance rather
        # than rebuilt from md['node_serial'] so that a node this plugin did
        # not name is still drained by the name k3s knows it by.
        #
        # Lowercased, because a Kubernetes node name is a DNS subdomain name
        # and those are lowercase: kubelet lowercases the hostname before it
        # registers, and the API server would refuse an uppercase name if it
        # did not. Shaken Fist does not lowercase -- its instance name guard
        # permits "a-z, A-Z, 0-9, or hyphen (-)", in the POST handler in
        # shakenfist/external_api/instance.py -- so "k3s-MyCluster-node-002"
        # is a real instance name whose node k3s knows as
        # "k3s-mycluster-node-002". Without this, remove-worker is unusable
        # on any cluster whose name has a capital letter in it: the drain
        # fails to find the node and raises CommandFailedError, which at
        # least fails before anything is destroyed.
        #
        # ResourceNotFoundException is caught for the reason delete()
        # catches it per instance: cluster metadata can name an instance
        # somebody has since deleted out from under it. health() reports
        # that as a finding and delete() tolerates it, and this is the only
        # verb which can take the entry out of the metadata, so refusing it
        # would make a stale entry unfixable short of deleting the whole
        # cluster. A None name below means that case and only that case.
        #
        # .get() rather than a subscript, matching _node_health(), and then
        # refused rather than worked around: an instance representation with
        # no name is not one this verb can drain, and guessing would drain
        # the wrong node. It is not reachable from the API as it stands,
        # which is why it is a check and not a code path with a story.
        resolved = []
        for instance_uuid in wanted:
            try:
                inst = self.client.get_instance(instance_uuid)
            except apiclient.ResourceNotFoundException:
                resolved.append((instance_uuid, None))
                continue

            node_name = inst.get('name')
            if not node_name:
                raise exceptions.WorkerUnnamedError(self.name, instance_uuid)
            resolved.append((instance_uuid, node_name.lower()))

        for instance_uuid, node_name in resolved:
            if node_name is None:
                p.phase('Removing worker %s, whose instance is already gone'
                        % instance_uuid)
                md['worker_nodes'].remove(instance_uuid)
                self.set_metadata(md)
                p.note('instance %s no longer exists, so there was nothing to '
                       'drain or delete; removed it from the cluster metadata'
                       % instance_uuid)
                continue

            p.phase('Removing worker %s (uuid %s)' % (node_name, instance_uuid))

            # Two agent operations rather than one, so that a drain which
            # fails raises before the node object is removed and the
            # instance destroyed. A single execute_and_await() submits both
            # commands and only then checks their return codes, which would
            # delete a node still running the pods the drain could not move.
            #
            # Both commands carry --kubeconfig explicitly, even though bare
            # kubectl already works on these nodes today (setup_metallb()
            # and setup_longhorn() run kubectl without it, against real
            # clusters, in this repo's functional CI). The two commands here
            # would otherwise differ only in whether they name the
            # kubeconfig, which invites the next reader to wonder which
            # spelling is load-bearing. Being explicit keeps this working
            # if these are ever run as a user whose default kubeconfig is
            # not k3s's.
            #
            # The node name is quoted per rule 1 at the top of this
            # module. It cannot contain a shell metacharacter today --
            # the instance it names was accepted by the Shaken Fist API,
            # whose name check allows only letters, digits and hyphens --
            # but a remote API's input validation is not this package's
            # trust boundary, and delete() quotes the same string for the
            # same reason.
            quoted = shlex.quote(node_name)
            try:
                self.execute_and_await(
                    [md['control_plane_nodes'][0]],
                    ['kubectl drain %s --ignore-daemonsets '
                     '--delete-emptydir-data --timeout=%s '
                     '--kubeconfig /etc/rancher/k3s/k3s.yaml'
                     % (quoted, KUBECTL_DRAIN_TIMEOUT)])
                self.execute_and_await(
                    [md['control_plane_nodes'][0]],
                    ['kubectl delete node %s --kubeconfig /etc/rancher/k3s/k3s.yaml'
                     % quoted])
            except (exceptions.K3sClusterException,
                    apiclient.APIException):
                # Both hierarchies, because the question is whether the
                # node might now be cordoned rather than which kind of
                # failure stopped the command. An APIException from the
                # submission means it never ran and there is nothing to
                # undo, and an uncordon of a node which was never
                # cordoned is a no-op, so covering both costs one
                # harmless command in the case which does not need it.
                #
                # Both commands are inside the guard, because a drain
                # which succeeded has already cordoned the node and
                # evicted every pod on it. A 'kubectl delete node' which
                # then fails would otherwise leave the cluster one
                # schedulable node short with nothing in this package
                # willing to put it back -- the outcome this method's
                # docstring promises not to produce. The delete is still
                # a separate submission from the drain rather than a
                # second command in the same one, because
                # execute_and_await() submits everything it is given and
                # only then reads the return codes, which would delete a
                # node still running the pods the drain could not move.
                self._uncordon(md['control_plane_nodes'][0], node_name, quoted)
                raise

            p.note('drained %s and removed it from k3s' % node_name)

            # Past the point an uncordon could help: the node object is gone
            # from k3s, so there is nothing left to put back into service,
            # and the entry in md['worker_nodes'] is deliberately left alone
            # rather than removed before the instance is. Keeping it means
            # 'k3s delete' still destroys this instance and health() still
            # reports it; dropping it first would trade a visible stale
            # entry for an instance nothing in this package can see, which
            # is the worse of the two. So the recovery is stated rather than
            # attempted, and the original failure is the one raised.
            try:
                self.client.delete_instance(instance_uuid)
            except apiclient.APIException:
                self.reporter.write(
                    'Node %s has been removed from k3s but its instance could '
                    'not be deleted, so cluster %s still lists it as a worker. '
                    'Delete instance %s and run remove-worker for it again to '
                    'take it out of the cluster metadata.\n'
                    % (node_name, self.name, instance_uuid))
                raise

            # list.remove() rather than a rebuilt list: the survivors keep
            # the order they were created in, which is the order every other
            # reader of md['worker_nodes'] sees them in.
            md['worker_nodes'].remove(instance_uuid)
            self.set_metadata(md)
            p.note('deleted instance %s' % instance_uuid)

        p.finish('Removed %s from cluster %s' % (
            progress.count_str(len(wanted), 'worker'), self.name))

    def _uncordon(self, control_plane_uuid, node_name, quoted_node_name):
        """Put a node back in service after a removal which did not finish.

        ``kubectl drain`` cordons before it evicts, so every way the drain
        can fail leaves the node unschedulable -- and so does a successful
        drain whose ``kubectl delete node`` then fails, which is the worse
        of the two because the node is empty as well. Nothing else in this
        package would put it back, which would make a refused remove-worker
        worse for the cluster than not running it.

        The original failure is the one the caller needs, so this never
        raises: an uncordon which itself fails is reported and swallowed,
        because replacing "the drain was refused because of a disruption
        budget" with "the uncordon failed" loses the reason. The node is
        named in that report so an operator can run the one command. Both
        exception hierarchies are caught for that reason and not because
        either is expected -- apiclient's exceptions do not descend from
        K3sClusterException, so catching only ours would let an
        unreachable API mask the original explanation.
        """
        self.reporter.write(
            'The removal of %s did not finish, so it may still be cordoned. '
            'Uncordoning it.\n' % node_name)
        try:
            self.execute_and_await(
                [control_plane_uuid],
                ['kubectl uncordon %s --kubeconfig /etc/rancher/k3s/k3s.yaml'
                 % quoted_node_name])
        except (exceptions.K3sClusterException, apiclient.APIException) as e:
            self.reporter.write(
                'Uncordoning %s failed as well, so it is still unschedulable. '
                "Run 'kubectl uncordon %s' against the cluster to put it back. "
                'The uncordon said:\n%s\n' % (node_name, node_name, e))

    def expand_addresses(self, address_count):
        """Route more floating addresses into this cluster for metallb to hand out.

        This is the body of ``sf-client k3s expand-addresses``. The cluster
        must have finished being built: the new addresses are written into
        metallb's configuration through the first control plane node, which
        an interrupted create may not have made, let alone installed
        metallb on.

        It must also be a cluster which has metallb, which is checked
        before an address is routed rather than after. The two halves of
        this are allocate_metallb_addresses(), which routes floating
        addresses and commits them to the metadata, and
        configure_metallb_addresses(), whose first commands ask metallb's
        own workloads whether they have rolled out. On a cluster created
        with install_metallb=False there are no such workloads, so that
        fails, and it fails with the addresses already routed and charged
        for and nothing able to hand them out. Refusing up front costs the
        caller an error and nothing else.

        The addresses already recorded are checked up front for the same
        reason. They reach metallb's configuration only after the new
        ones have been routed, and ``heredoc()`` refuses a body one of
        them could end -- a refusal which is correct and which would
        otherwise arrive after the allocation.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.not_found(self.name)
        self._require_usable(md, 'expand-addresses')

        # Absent means True: clusters built before create() recorded this
        # all have metallb.
        if not md.get('metallb_installed', True):
            raise exceptions.ComponentNotInstalledError(
                self.name, 'metallb', 'expand-addresses')

        # And the document's contents for the same reason as its flags;
        # _require_addresses() carries the argument.
        self._require_addresses(md, 'routed_addresses')

        p = self.start_progress(1)
        p.phase('Adding metallb addresses')
        self.allocate_metallb_addresses(address_count)
        self.configure_metallb_addresses()
        p.finish(f'Added {address_count} metallb addresses to cluster {self.name}')

    def update_os(self):
        """Update the base OS packages on every node in this cluster.

        This is the body of ``sf-client k3s update-os``. Unlike the other
        expansion verbs this does not require a cluster which finished
        being built: it talks to the instances in the metadata and nothing
        else, so on an interrupted cluster it updates whichever nodes exist
        and does nothing at all when none do. That is a truthful answer
        rather than a failure, so it is left alone.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.not_found(self.name)

        p = self.start_progress(1)
        p.phase('Updating the OS on all cluster nodes')
        self.instance_os_update(md['control_plane_nodes'] + md['worker_nodes'])
        p.finish(f'Updated the OS on all nodes in cluster {self.name}')
