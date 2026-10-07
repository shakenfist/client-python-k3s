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
import json
import os
import subprocess
import tempfile

import mock
from shakenfist_client import apiclient


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


# What Cluster.delete() runs to learn which kubeconfig entries are present,
# after the 'kubectl --kubeconfig FILE' which starts every cleanup call.
KUBECTL_CONFIG_VIEW_JSON = ['config', 'view', '-o', 'json']


def cleanup_kubectl(main_config_path, args):
    """The argument list delete()'s cleanup runs for args, against main_config_path."""
    return ['kubectl', '--kubeconfig', main_config_path] + list(args)


def kubectl_subcommand(argv):
    """argv without 'kubectl' and a leading '--kubeconfig FILE': what the call does, not to which file.

    For matchers and for tests whose subject is the names a call carries.
    Which file a cleanup call acts on is pinned separately, by
    OptionalKubeconfigTestCase in test_library_api.
    """
    argv = list(argv[1:])
    if argv[:1] == ['--kubeconfig']:
        argv = argv[2:]
    return argv


def home_with_kubeconfig(testcase):
    """Point HOME at a temporary directory holding a ~/.kube/config, and return its path.

    delete()'s kubeconfig cleanup acts on ~/.kube/config and runs no
    kubectl when there is no such file, so a test of the cleanup needs one
    -- and needs it somewhere other than the operator's own home.
    """
    home = tempfile.TemporaryDirectory()
    testcase.addCleanup(home.cleanup)
    patcher = mock.patch.dict('os.environ', {'HOME': home.name})
    patcher.start()
    testcase.addCleanup(patcher.stop)

    path = os.path.join(home.name, '.kube', 'config')
    os.makedirs(os.path.dirname(path))
    with open(path, 'w', encoding='utf-8') as f:
        f.write(KUBECONFIG)
    return path


class FakeKubectl:
    """The read half of a local kubectl, as a subprocess.run() side effect.

    delete() reads the entry names present with ``kubectl config view -o
    json`` and then runs a ``delete-*`` command only for those, so a fake
    which answered every command with the same empty bytes would make the
    cleanup do nothing and every test of it pass for the wrong reason. This
    answers the read with the document kubectl prints for a kubeconfig
    holding a user, context and cluster for each of ``names`` -- including
    kubectl's null, rather than an empty list, for an empty section -- and
    returns ``mock.DEFAULT`` for every other command, so the patched mock's
    own ``return_value`` still decides how the delete commands and create's
    merge behave.

    It does not remove names as they are deleted: a test which wants the
    entries gone says so by setting ``names``.
    """

    def __init__(self, names=()):
        self.names = list(names)
        self.view_returncode = 0
        self.view_stdout = None
        self.view_stderr = b''

    def view_json(self):
        def section(kind):
            return [{'name': name, kind: {}} for name in self.names] or None

        return json.dumps({
            'kind': 'Config', 'apiVersion': 'v1',
            'clusters': section('cluster'),
            'users': section('user'),
            'contexts': section('context'),
            'current-context': self.names[0] if self.names else '',
        }).encode('utf-8')

    def __call__(self, args, **kwargs):
        if list(args[:1]) != ['kubectl'] or kubectl_subcommand(args) != KUBECTL_CONFIG_VIEW_JSON:
            return mock.DEFAULT
        stdout = self.view_stdout
        if stdout is None:
            stdout = self.view_json()
        return subprocess.CompletedProcess(
            args, self.view_returncode, stdout, self.view_stderr)


class FakeClusterClient:
    """Enough of the sf-client API surface for a whole cluster lifecycle.

    Instances boot instantly, every agent operation completes successfully
    at submission, file fetches return canned content, and deleting an
    instance moves it straight to the deleted state so delete's wait loop
    terminates.
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
        return self._complete_aop(
            instance_ref,
            [{'command': 'execute', 'commandline': commandline}],
            {'0': {'return-code': 0, 'stdout': '', 'stderr': ''}})

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


class HealthClient(FakeClusterClient):
    """A scripted client which can be made unwell in each of the ways health() reports.

    The stock fake cannot express any of them: every instance it knows
    about is created with its agent ready and every agent command it is
    given completes with a return code of zero. A health check whose entire
    purpose is reporting bad news needs a client which can deliver some.

    health() runs two kinds of command through the agent, and this answers
    them separately, routed by command line: one starting 'kubectl ' is the
    k3s API probe and answers from the ``probe_*`` attributes, exactly as it
    did when it was the only command there was, and anything else is a
    node's signals probe and answers from the ``signals_*`` attributes for
    the instance it was sent to. Each operation keeps its own kind and
    instance, so a pending kubectl probe and a complete signals probe -- or
    the other way around -- can be in flight together, which is the shape
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

        # What the kubectl probe does. Between them these cover the three
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

        # What each node's signals probe does, keyed by instance uuid, so
        # one node can be made to fail while the others answer. The same
        # three ways to fail as the kubectl probe, plus what the command
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
        # number of operations in flight -- one per probed node and the
        # kubectl one -- rather than assuming there is only one.
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
        if kind == 'kubectl':
            return self.probe_state, {
                '0': {'return-code': self.probe_return_code,
                      'stdout': self.probe_stdout,
                      'stderr': self.probe_stderr}}

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
        # answers as the kubectl probe, which is what every operation was
        # before there was more than one kind.
        self.calls.append(('read', operation_uuid))
        self.agent_operation_reads += 1
        reads = self.agent_operation_reads_by_uuid.get(operation_uuid, 0) + 1
        self.agent_operation_reads_by_uuid[operation_uuid] = reads

        kind, instance_ref, commandline = self.operations.get(
            operation_uuid, ('kubectl', None, None))
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

        kind = 'kubectl' if commandline.startswith('kubectl ') else 'signals'
        raises = (self.probe_raises if kind == 'kubectl'
                  else self.signals_raises.get(instance_ref))
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
