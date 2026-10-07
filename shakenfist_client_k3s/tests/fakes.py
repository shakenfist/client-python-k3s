"""Scripted Shaken Fist clients, good enough to drive a cluster end to end.

A bare ``mock.MagicMock`` cannot do this job. The orchestration's wait
loops compare dictionary values against literals -- ``'created'``,
``'ready'``, ``'complete'``, ``'deleted'`` -- and a MagicMock's attributes
and subscripts compare equal to none of them, so every wait spins forever.
This fake returns the shapes the orchestration actually reads instead, and
lives here rather than in one test module because both the CLI level
create smoke test and the library level lifecycle test need it. The same
goes for HealthClient below, which the library and command line tests for
the health verb both drive.
"""

import io

from shakenfist_client import apiclient

from shakenfist_client_k3s import cluster as cluster_module


class FakeTty(io.StringIO):
    """A stream which claims to be a terminal.

    Progress chooses between in place ANSI updates and line mode by
    asking its stream, so this is how a test exercises the interactive
    format without a pty. Here rather than in one test module because
    both the progress tests and the cluster tests need it.
    """

    def isatty(self):
        return True


def not_found(instance_uuid):
    """Build the exception the API client raises for an instance which is gone."""
    return apiclient.ResourceNotFoundException(
        'instance not found', 'GET', '/instances/%s' % instance_uuid, 404,
        'instance not found')


# A minimal kubeconfig in the shape k3s writes, pointing at the loopback
# address the way the real file does before create rewrites it.
KUBECONFIG = """apiVersion: v1
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
kind: Config
users:
- name: default
  user:
    token: banana
"""


class FakeClusterClient:
    """Enough of the sf-client API surface for a whole cluster lifecycle.

    Instances boot instantly, every agent operation completes at
    submission -- successfully, unless a test has named a command to fail
    with failing_command -- file fetches return canned content, and
    deleting an instance moves it straight to the deleted state so delete's
    wait loop terminates.

    Every command succeeding includes the readiness waits create() and
    expand_workers() run (nodes_ready_command()): exit 0 is what the real
    command says once the node is Ready, so a fake which says it at once
    is a cluster whose nodes were Ready immediately, which is the case the
    lifecycle tests mean.
    """

    def __init__(self):
        self.namespace = 'testns'
        self.metadata = {}
        self.instances = {}
        self.instance_serial = 0
        self.instance_sshkeys = []

        # (name, cpus, memory, disk size) for every instance the caller
        # asked for, in the order it asked. Beside the instance rather than
        # in it for the same reason as instance_sshkeys: the representation
        # the fake hands back is the API's shape, and a size recorded there
        # would be a field for a test to assert on that the orchestration
        # never reads back.
        self.instance_sizes = []

        self.aop_serial = 0
        self.routed_serial = 0

        # Every command the client was asked to run, in the order it was
        # asked, so a test can assert both what ran and what ran before
        # what. Ordering claims -- a file written before the installer
        # which reads it, a node drained before its instance is destroyed
        # -- cannot be made from separate per-call lists, because neither
        # knows where in the other its own calls fell.
        self.executed = []

        # A substring which, when a command line contains it, makes that
        # command complete with a non-zero return code and failing_stderr,
        # which is what reap_execute() turns into CommandFailedError. None,
        # the default, fails nothing. A substring rather than a whole
        # command, because the commands worth failing are long and built
        # from module constants, and what a test means is "the wait for
        # this node" or "the k3s install", not every byte of either.
        self.failing_command = None
        self.failing_stderr = 'scripted failure'

        # What the caller asked us to destroy, so a test can assert on the
        # teardown as well as the build. Network allocation is recorded as
        # well, because "nothing was built" has to be able to say networks
        # too, and an allocation is not otherwise visible anywhere.
        self.allocated_networks = []
        self.deleted_networks = []
        self.unrouted_addresses = []

    def get_namespace(self, namespace):
        return {'name': namespace}

    def create_namespace(self, namespace):
        return {'name': namespace}

    def get_namespace_metadata(self, namespace):
        return dict(self.metadata)

    def set_namespace_metadata_item(self, namespace, key, value):
        self.metadata[key] = value

    def delete_namespace_metadata_item(self, namespace, key):
        self.metadata.pop(key, None)

    def allocate_network(self, netblock, provide_dhcp, provide_nat, name,
                         namespace=None):
        self.allocated_networks.append(name)
        return {'uuid': 'net-1', 'name': name, 'state': 'created'}

    def get_network(self, network_ref):
        return {'uuid': 'net-1', 'name': 'k3s-banana-node', 'state': 'created'}

    def delete_network(self, network_ref):
        self.deleted_networks.append(network_ref)

    def create_instance(self, name, cpus, memory, networks, disks, sshkey,
                        userdata, side_channels=None, namespace=None):
        # Recorded beside the instance rather than in it, because the real
        # API does not return the key in an instance representation and a
        # fake which did would let a test assert on a field that does not
        # exist.
        self.instance_sshkeys.append(sshkey)
        self.instance_sizes.append((name, cpus, memory, disks[0]['size']))
        self.instance_serial += 1
        instance_uuid = 'inst-%03d' % self.instance_serial
        self.instances[instance_uuid] = {
            'uuid': instance_uuid, 'name': name, 'state': 'created',
            'agent_state': 'ready'}
        return self.instances[instance_uuid]

    def get_instance(self, instance_ref):
        # The API client's exception, not a KeyError. Three places in the
        # orchestration catch ResourceNotFoundException for an instance the
        # metadata names which somebody has deleted out from under it --
        # delete(), _node_health() and remove_worker() -- and a fake which
        # raises KeyError instead cannot exercise any of them, which is how
        # a test asserting that behaviour passes for the wrong reason.
        if instance_ref not in self.instances:
            raise not_found(instance_ref)
        return self.instances[instance_ref]

    def delete_instance(self, instance_ref):
        self.instances[instance_ref]['state'] = 'deleted'

    def get_instance_interfaces(self, instance_ref):
        return [{'ipv4': '10.0.0.4', 'floating': '192.168.10.100'}]

    def get_instance_agentoperations(self, instance_ref, all=False):
        return []

    def get_agent_operation(self, operation_uuid):
        # The wait loops re-read an operation until it leaves its pending
        # states. Everything this fake hands out is already complete, so a
        # re-read is only reached by a test which built a pending operation
        # itself; such a test overrides this.
        return {'uuid': operation_uuid, 'state': 'complete', 'results': {}}

    def _complete_aop(self, instance_ref, commands, results):
        self.aop_serial += 1
        return {
            'uuid': 'aop-%03d' % self.aop_serial,
            'instance_uuid': instance_ref,
            'state': 'complete',
            'commands': commands,
            'results': results
        }

    def instance_execute(self, instance_ref, commandline):
        self.executed.append((instance_ref, commandline))
        result = {'return-code': 0, 'stdout': '', 'stderr': ''}
        if self.failing_command and self.failing_command in commandline:
            result = {'return-code': 1, 'stdout': '',
                      'stderr': self.failing_stderr}
        return self._complete_aop(
            instance_ref,
            [{'command': 'execute', 'commandline': commandline}],
            {'0': result})

    def instance_get(self, instance_ref, path):
        return self._complete_aop(
            instance_ref,
            [{'command': 'get-file', 'path': path}],
            {'0': {'content_blob': path}})

    def get_blob_data(self, blob_uuid):
        if blob_uuid.endswith('k3s.yaml'):
            yield KUBECONFIG.encode('utf-8')
        else:
            yield b'not-a-real-token\n'

    def route_network_address(self, network_uuid):
        self.routed_serial += 1
        return '192.168.10.%d' % self.routed_serial

    def unroute_network_address(self, network_uuid, address):
        self.unrouted_addresses.append((network_uuid, address))


# What node_signals_command() prints on a healthy node of each role, for
# HealthClient to answer the signals probe with and for the parser's tests
# to read. Written by hand from what each source prints rather than captured
# from a node, which is why the plan's live run exists. The worker's k3s
# agent is mid-restart, because 'activating' is a state a real one is seen
# in and the report has to carry it as it is.
SERVER_SIGNALS_OUTPUT = (
    'boot_id=3f0c3c4e-5b8e-4f43-9d1c-0d6a8f2b7e11\n'
    'booted_at=1759712345\n'
    'oom_kills=2\n'
    'memory_total_kb=4022148\n'
    'memory_available_kb=2876544\n'
    'NRestarts=3\n'
    'LoadState=loaded\n'
    'ActiveState=active\n'
    'etcd_bytes=68321280\n'
    'etcd_snapshot_bytes=41943040\n')

WORKER_SIGNALS_OUTPUT = (
    'boot_id=9a1d7c22-0e4b-4c5f-a0b3-77c1e2d4f6a8\n'
    'booted_at=1759712399\n'
    'oom_kills=0\n'
    'memory_total_kb=2010264\n'
    'memory_available_kb=1102336\n'
    'NRestarts=0\n'
    'LoadState=loaded\n'
    'ActiveState=activating\n')

# What K3S_KUBERNETES_PROBE_COMMAND prints for a three node cluster on which
# one container has been killed for running out of memory: a 'node' line per
# node, then an 'oom' line for the container, in the shape kubectl rendered
# the templates in when they were written. The node names are the ones the
# health tests give their instances, lowercased as kubelet registers them,
# so that HealthClient's default answer is a cluster whose every node is
# registered and Ready, with one OOM kill on a worker, which leaves it
# healthy. Unix seconds for each time are given beside it, worked out with
# 'date -u' rather than with the code under test, so that the parser's
# tests and health()'s are reading the same output.
KUBERNETES_PROBE_OUTPUT = (
    'node\tk3s-banana-node-001\tTrue\tFalse\tFalse\tFalse\t'
    '2026-10-05T08:12:25Z\n'
    'node\tk3s-banana-node-002\tTrue\tFalse\tFalse\tFalse\t'
    '2026-10-05T08:13:02Z\n'
    'node\tk3s-banana-node-003\tTrue\tFalse\tFalse\tFalse\t'
    '2026-10-05T08:13:09Z\n'
    'oom\tk3s-banana-node-002\tdefault\tmemory-hog-7d9f8b6c5-x2x7k\thog\t2\t'
    '2026-10-06T21:40:11Z\n')
KUBERNETES_PROBE_READINGS = {
    'nodes': {
        'k3s-banana-node-001': {
            'ready': 'True',
            'ready_since': 1791187945,
            'memory_pressure': 'False',
            'disk_pressure': 'False',
            'pid_pressure': 'False',
        },
        'k3s-banana-node-002': {
            'ready': 'True',
            'ready_since': 1791187982,
            'memory_pressure': 'False',
            'disk_pressure': 'False',
            'pid_pressure': 'False',
        },
        'k3s-banana-node-003': {
            'ready': 'True',
            'ready_since': 1791187989,
            'memory_pressure': 'False',
            'disk_pressure': 'False',
            'pid_pressure': 'False',
        },
    },
    'oom_killed': {
        'k3s-banana-node-002': [
            {
                'namespace': 'default',
                'pod': 'memory-hog-7d9f8b6c5-x2x7k',
                'container': 'hog',
                'restarts': 2,
                'finished_at': 1791322811,
            },
        ],
    },
}


class HealthClient(FakeClusterClient):
    """A scripted client which can be made unwell in each of the ways health() reports.

    The stock fake cannot express any of them: every instance it knows
    about is created with its agent ready and every agent command it is
    given completes with a return code of zero. A health check whose entire
    purpose is reporting bad news needs a client which can deliver some.

    health() runs three kinds of command through the agent, and this answers
    them separately, routed by command line: K3S_API_PROBE_COMMAND is the
    k3s API probe and answers from the ``probe_*`` attributes, exactly as it
    did when it was the only command there was;
    K3S_KUBERNETES_PROBE_COMMAND is the Kubernetes probe and answers from
    the ``kubernetes_*`` attributes; and anything else is a node's signals
    probe and answers from the ``signals_*`` attributes for the instance it
    was sent to. The first two are matched exactly, against the module's
    constants rather than literals, so a test cannot pass against a command
    health() no longer sends: a changed constant is a command this answers
    as signals, whose output reads as no API answer and no Kubernetes node.
    Both are kubectl, which is why routing on a 'kubectl ' prefix, as this
    once did, would answer one with the other's output. Each operation
    keeps its own kind and instance, so a pending probe of one kind and a
    complete one of another can be in flight together, which is the shape
    of the cases worth testing.

    Submission and reading are modelled as the real server does them for
    this plugin, whose client is built with ASYNC_CONTINUE: instance_execute()
    returns the operation as submitted, 'queued' with no results, and only
    get_agent_operation() reports what the command did. So the scripted
    attributes -- probe_state, signals_state and the rest -- say what the
    operation is when it is read, never what instance_execute() hands back.
    This fake used to return the scripted ending from instance_execute()
    itself, which let a probe that was never read report a result it had not
    looked for, and so hid a wait that judged probes on their submission
    state.
    """

    def __init__(self):
        super(HealthClient, self).__init__()

        # Every mutation the client was asked to make. health() must make
        # none of them, and the calls have to be recorded rather than their
        # effect inspected: a metadata write which writes back the same
        # dictionary the cache is already holding leaves the stored
        # document comparing equal to what it was, so only the call itself
        # shows that the write happened at all.
        self.metadata_writes = []
        self.metadata_deletes = []
        self.deleted_instances = []

        # What the API probe does. Between them these cover the three
        # ways it can fail: the command runs and exits non-zero, the agent
        # operation itself errors, and the API refuses to accept the
        # command at all. probe_state is the state the operation is in when
        # it is read; at submission it is always 'queued' (see the class
        # docstring).
        self.probe_return_code = 0
        self.probe_stdout = (
            'NAME                  STATUS   ROLES\n'
            'k3s-banana-node-001   Ready    control-plane\n')
        self.probe_stderr = ''
        self.probe_state = 'complete'
        self.probe_raises = None

        # What the Kubernetes probe does, in the same terms. Its default
        # output is KUBERNETES_PROBE_OUTPUT above: every node the health
        # tests name registered and Ready, and one OOM kill.
        self.kubernetes_return_code = 0
        self.kubernetes_stdout = KUBERNETES_PROBE_OUTPUT
        self.kubernetes_stderr = ''
        self.kubernetes_state = 'complete'
        self.kubernetes_raises = None

        # What each node's signals probe does, keyed by instance uuid, so
        # one node can be made to fail while the others answer. The same
        # three ways to fail as the API probe, plus what the command
        # prints. An instance with no entry answers as a healthy node would
        # when read: state 'complete', exit 0, and the realistic output above
        # for the role the command was built for.
        self.signals_stdout = {}
        self.signals_stderr = {}
        self.signals_return_code = {}
        self.signals_state = {}
        self.signals_raises = {}

        # Every call to instance_execute() and get_agent_operation(), in the
        # order made, as ('execute', instance uuid, command line) and
        # ('read', operation uuid). health() must submit every probe before
        # it waits for any, and that is a claim about the order of two kinds
        # of call which neither's own record can show.
        self.calls = []

        # Which probe and which instance each operation this fake handed out
        # belongs to, so that a re-read answers for that operation rather
        # than for whichever command was submitted last.
        self.operations = {}

        # How many times one operation may be re-read before this fake
        # decides the wait is not going to end. A wait which does not end
        # is the bug health() had, so a test for it must fail rather than
        # hang: a hung suite names no test, and nobody reads a run which
        # did not finish. The correct code reads a pending operation once
        # per second up to its timeout, which is well inside this. Counted
        # per operation rather than in total, so the limit scales with the
        # number of operations in flight -- one per probed node and the two
        # kubectl ones -- rather than assuming there is only one.
        self.agent_operation_reads = 0
        self.agent_operation_reads_by_uuid = {}
        self.max_agent_operation_reads = 60

    def set_namespace_metadata_item(self, namespace, key, value):
        self.metadata_writes.append(key)
        return super(HealthClient, self).set_namespace_metadata_item(
            namespace, key, value)

    def delete_namespace_metadata_item(self, namespace, key):
        self.metadata_deletes.append(key)
        return super(HealthClient, self).delete_namespace_metadata_item(
            namespace, key)

    def delete_instance(self, instance_ref):
        self.deleted_instances.append(instance_ref)
        return super(HealthClient, self).delete_instance(instance_ref)

    def _answer(self, kind, instance_ref, commandline):
        """Return (state, results) for an operation of kind on instance_ref.

        Called only when an operation is read, never at submission, and
        read afresh on every call rather than captured, as the probe_*
        answer always was, so a test may change an attribute between
        submission and the wait, or between one read and the next.
        """
        if kind == 'api':
            return self.probe_state, {
                '0': {'return-code': self.probe_return_code,
                      'stdout': self.probe_stdout,
                      'stderr': self.probe_stderr}}
        if kind == 'kubernetes':
            return self.kubernetes_state, {
                '0': {'return-code': self.kubernetes_return_code,
                      'stdout': self.kubernetes_stdout,
                      'stderr': self.kubernetes_stderr}}

        # The default output follows the unit the command asked about,
        # which is how a real node's output follows its role: it prints
        # what it was asked for.
        if 'systemctl show k3s-agent ' in (commandline or ''):
            default_stdout = WORKER_SIGNALS_OUTPUT
        else:
            default_stdout = SERVER_SIGNALS_OUTPUT
        return self.signals_state.get(instance_ref, 'complete'), {
            '0': {'return-code': self.signals_return_code.get(instance_ref, 0),
                  'stdout': self.signals_stdout.get(instance_ref,
                                                    default_stdout),
                  'stderr': self.signals_stderr.get(instance_ref, '')}}

    def get_agent_operation(self, operation_uuid):
        # A probe which is still pending stays pending: this is the node
        # whose agent is not connected, where the operation is accepted and
        # then never runs. A test which wants the wait to end sets the
        # state to a terminal one. An operation this fake did not hand out
        # answers as the API probe, which is what every operation was
        # before there was more than one kind.
        self.calls.append(('read', operation_uuid))
        self.agent_operation_reads += 1
        reads = self.agent_operation_reads_by_uuid.get(operation_uuid, 0) + 1
        self.agent_operation_reads_by_uuid[operation_uuid] = reads

        kind, instance_ref, commandline = self.operations.get(
            operation_uuid, ('api', None, None))
        state, results = self._answer(kind, instance_ref, commandline)
        # The command the operation was submitted with, as the server
        # reports it on every read, so that a description of the operation
        # built from what was read names it.
        commands = []
        if commandline is not None:
            commands = [{'command': 'execute', 'commandline': commandline}]
        if reads > self.max_agent_operation_reads:
            raise AssertionError(
                'the wait re-read the agent operation %s %d times without '
                'ending. The operation is %r, which is not a state it can '
                'leave, so whatever is waiting on it is waiting forever.'
                % (operation_uuid, reads, state))
        return {
            'uuid': operation_uuid,
            'instance_uuid': instance_ref,
            'state': state,
            'commands': commands,
            'results': results
        }

    def instance_execute(self, instance_ref, commandline):
        self.executed.append((instance_ref, commandline))
        self.calls.append(('execute', instance_ref, commandline))

        if commandline == cluster_module.K3S_API_PROBE_COMMAND:
            kind, raises = 'api', self.probe_raises
        elif commandline == cluster_module.K3S_KUBERNETES_PROBE_COMMAND:
            kind, raises = 'kubernetes', self.kubernetes_raises
        else:
            kind, raises = 'signals', self.signals_raises.get(instance_ref)
        if raises:
            raise raises

        # The operation as the server returns it on submission: queued,
        # with nothing run and so no results, whatever the scripted ending
        # is. A caller which wants to know how the command went has to read
        # the operation, which is the path health() takes on a real cluster
        # and so the one its tests must exercise.
        self.aop_serial += 1
        operation_uuid = 'aop-%03d' % self.aop_serial
        self.operations[operation_uuid] = (kind, instance_ref, commandline)
        return {
            'uuid': operation_uuid,
            'instance_uuid': instance_ref,
            'state': 'queued',
            'commands': [{'command': 'execute', 'commandline': commandline}],
            'results': {}
        }
