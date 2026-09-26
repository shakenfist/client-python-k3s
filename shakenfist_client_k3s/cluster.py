"""One k3s cluster, and the orchestration which drives it.

This replaces the click context the orchestration primitives used to be
handed. Anything which reads or writes cluster metadata, or drives this
cluster's nodes, is a method here (see decision 2 in
``docs/plans/library-api-and-collection-phase-01-library-api.md``), so
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

BASE_OS_VERSION = 'debian:12'

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
#    delimiter costs nothing and removes the whole question.
#
# Cluster.delete()'s kubectl invocation is the third form of the same rule:
# where a real argument list is available, it is used instead.


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

    def get_progress(self, total_phases=1):
        """Return the Progress reporter for this operation, making a default if needed.

        Commands which know how many phases they have build their own and
        assign it; everything else gets one lazily, so a method called
        directly by a library caller still reports progress somewhere
        sensible.

        total_phases is the count the lazy default is built with, and is
        ignored when there is already a Progress to return -- it says how
        many phases *this* method is about to open, not how many the
        operation has. It defaults to 1 because all but one of the methods
        which call get_progress() themselves -- rather than inheriting a
        Progress an entry point like create() or expand_workers() already
        built -- open exactly one phase and do their work inside it:
        create_and_await_instances(), install_extra_control_plane(),
        install_workers(), setup_metallb() and setup_longhorn(). 1 is
        therefore not a placeholder guess but the true count for those
        callers, giving a library caller who invokes one of them directly
        an honest "[1/1]" instead of the un-numbered "[n]" this used to
        print.

        The exception is install_control_plane(), which opens a second
        phase through install_extra_control_plane() when the cluster has
        more than one control plane node, and so passes the count it works
        out from the metadata rather than taking the default.

        This is one Progress per Cluster instance, cached for its life
        (see __init__), so it is only accurate for a single such call. A
        library caller who invokes two of these methods in sequence on the
        same Cluster shares the one lazily built Progress between them --
        the second call's phase header becomes "[2/1]", which is worse
        than un-numbered. A caller doing that should build its own
        progress.Progress with the real total and assign it to
        self.progress first, the way create() and expand_workers() do.
        """
        if not self.progress:
            self.progress = progress.Progress(
                total_phases=total_phases, verbose=self.reporter.verbose,
                stream=self.reporter)
        return self.progress

    def create_instance(self):
        md = self.get_metadata()

        node_name = 'k3s-%s-node-%03d' % (md['name'], md['node_serial'])
        inst = self.client.create_instance(
            node_name, 2, 2048,
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
                    'size': 50,
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
            primitives._describe_agent_op(aop, max_len=None),
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
                    desc = primitives._describe_agent_op(aop)
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
            inst = self.create_instance()
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
                % (primitives._describe_agent_op(aop, max_len=None) or command,
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

        # Write a configuration file with the external address to the first control
        # plane node. This is needed so that the SSL certificate includes this
        # external name.
        #
        # The heredoc delimiter is quoted, per rule 2 at the top of this
        # module: Python has already substituted the address by the time
        # the remote shell sees this, so there is nothing here the shell
        # should be expanding.
        cmds.append('mkdir -p /etc/rancher/k3s/')
        cmds.append(
            "cat - > /etc/rancher/k3s/config.yaml << 'EOF'\n"
            'write-kubeconfig-mode: "0644"\n'
            'tls-san:\n'
            '  - "%s"\n'
            'cluster-init: true\n'
            'EOF\n'
            % md['api_address_floating'])

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
                # Exactly one trailing newline, because the heredoc needs
                # its delimiter on a line of its own and a file which
                # already ends in a newline must not gain a blank line.
                body = content if content.endswith('\n') else content + '\n'
                # Quoted per rule 1 at the top of this module. The
                # basename is the caller's, by way of os.path.basename()
                # of a path nothing else has looked at;
                # read_manifests() refuses the shapes which would be
                # unreadable in the operation log, and this makes the
                # ones it allows unable to mean anything to the shell.
                # A basename of plain filename characters quotes to
                # itself, so the common case is byte for byte what it
                # was.
                cmds.append(
                    "cat - > %s << '%s'\n%s%s\n"
                    % (shlex.quote('%s/%s' % (K3S_MANIFEST_DIR, basename)),
                       K3S_MANIFEST_DELIMITER, body,
                       K3S_MANIFEST_DELIMITER))

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

        self.execute_and_await(
            instance_uuids,
            [
                'sudo apt-get update',
                'sudo apt-get install -y',
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

        allocated = []
        for i in range(metal_address_count):
            addr = self.client.route_network_address(node_network['uuid'])
            if addr:
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
        # Quoted delimiter per rule 2 at the top of this module.
        metal_lb_config = ("cat - > /etc/sf/metallb-range-allocation.yaml << 'EOF'\n"
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
                           'EOF\n'
                           % '/32\n  - '.join(md['routed_addresses']))

        self.execute_and_await(
            [md['control_plane_nodes'][0]],
            [
                ('kubectl wait --kubeconfig /etc/rancher/k3s/k3s.yaml -n metallb-system pod '
                 '--for=condition=Ready -l app.kubernetes.io/name=metallb --timeout=300s'),
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

        # Let the metallb pods start
        time.sleep(5)

        # Add addresses
        self.configure_metallb_addresses()

    def setup_longhorn(self):
        p = self.get_progress()
        md = self.get_metadata()

        version = primitives.get_longhorn_release(
            self.client, self.namespace, self.reporter)
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
               manifests=None):
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
        staged_manifests = read_manifests(manifests)

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
        p = progress.Progress(
            total_phases=total_phases, verbose=self.reporter.verbose,
            stream=self.reporter)
        self.progress = p

        self.reporter.debug('Looking up k3s versions')
        target_release = primitives.get_k3s_release(
            self.client, self.namespace, self.reporter,
            force_cache_update=refresh_version_cache,
            release_channel=release_channel)

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
            # Only metallb_installed has a reader today, in
            # expand_addresses(). longhorn_installed is written for the
            # same reason and read by nothing, because no verb drives
            # Longhorn after create(); health() growing a storage check is
            # the obvious first reader. That is the position md['state']
            # was in for this package's whole history until phase 3 found
            # it (survey finding 2), so it is said out loud here rather
            # than left for somebody to rediscover.
            'metallb_installed': install_metallb,
            'longhorn_installed': install_longhorn,
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
        kubeconfig = self.await_fetch(aop).replace(
            '127.0.0.1', md['api_address_floating'])

        kc = yaml.safe_load(kubeconfig)
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
            kube_dir = os.path.join(os.path.expanduser('~'), '.kube')
            main_config_path = os.path.join(kube_dir, 'config')
            os.makedirs(kube_dir, exist_ok=True)

            if not os.path.exists(main_config_path):
                # There is no existing configuration to preserve, so no merge is
                # required and we don't need a local kubectl.
                with open(main_config_path, 'w', encoding='utf-8') as f:
                    f.write(yaml.dump(kc))
            else:
                if not shutil.which('kubectl'):
                    raise exceptions.KubeconfigError.missing_kubectl(
                        main_config_path, self.name)

                with tempfile.TemporaryDirectory() as tempdir:
                    new_config_path = os.path.join(tempdir, 'config')
                    with open(new_config_path, 'w', encoding='utf-8') as f:
                        f.write(yaml.dump(kc))
                    merged = subprocess.run(
                        'kubectl config view --flatten', shell=True, capture_output=True,
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

        return md

    def health(self):
        """Report the state of this cluster and of every node in it, and repair nothing.

        This is the body of ``sf-client k3s health``, which renders what
        this returns. Per decision 7 of
        ``docs/plans/library-api-and-collection-phase-03-missing-verbs.md``
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

        self.reporter.debug('Cluster metadata:')
        for k in md:
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
        md['api_floating_address'] = None
        md['api_inner_address'] = None
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
            md['node_network'] = []

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

        p = progress.Progress(
            total_phases=2, verbose=self.reporter.verbose, stream=self.reporter)
        self.progress = p
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
        ``docs/plans/library-api-and-collection-phase-03-missing-verbs.md``.

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

        p = progress.Progress(
            total_phases=len(wanted), verbose=self.reporter.verbose,
            stream=self.reporter)
        self.progress = p

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
        configure_metallb_addresses(), whose first command waits five
        minutes for a metallb pod. On a cluster created with
        install_metallb=False that is five minutes of waiting for a
        namespace which does not exist, ending in a CommandFailedError,
        with the addresses already routed and charged for and nothing able
        to hand them out. Refusing up front costs the caller an error
        instead.
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

        p = progress.Progress(
            total_phases=1, verbose=self.reporter.verbose, stream=self.reporter)
        self.progress = p
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

        p = progress.Progress(
            total_phases=1, verbose=self.reporter.verbose, stream=self.reporter)
        self.progress = p
        p.phase('Updating the OS on all cluster nodes')
        self.instance_os_update(md['control_plane_nodes'] + md['worker_nodes'])
        p.finish(f'Updated the OS on all nodes in cluster {self.name}')
