import copy
import io
import os
import tempfile
import time

from click.testing import CliRunner

# The PyPI mock backport is used rather than unittest.mock for consistency
# with test_primitives.py, as the project supports Python >= 3.7.
import mock
import testtools

import shakenfist_client_k3s
from shakenfist_client_k3s import cluster as cluster_module
from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress
from shakenfist_client_k3s.tests import fakes


class NamespaceDefaultingTestCase(testtools.TestCase):
    """Commands must default --namespace to the client's own namespace.

    Every command resolves the --namespace option onto the Cluster it
    builds, whose namespace the primitives pass directly to namespace
    metadata API calls. If the option is not given the value must come
    from the client (and never remain None, which the API client rejects
    with a TypeError).
    """

    def setUp(self):
        super(NamespaceDefaultingTestCase, self).setUp()
        self.client = mock.MagicMock()
        self.client.namespace = 'clientns'
        self.runner = CliRunner()

    def _invoke(self, args):
        return self.runner.invoke(
            shakenfist_client_k3s.k3s, args, obj={'VERBOSE': False, 'CLIENT': self.client})

    def test_show_defaults_namespace_from_client(self):
        md_key = cluster_module.METADATA_KEY % 'banana'
        self.client.get_namespace_metadata.return_value = {
            md_key: {'name': 'banana', 'state': 'created'}}

        result = self._invoke(['show', 'banana'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('clientns')

    def test_show_explicit_namespace_wins(self):
        md_key = cluster_module.METADATA_KEY % 'banana'
        self.client.get_namespace_metadata.return_value = {
            md_key: {'name': 'banana', 'state': 'created'}}

        result = self._invoke(['show', 'banana', '--namespace', 'otherns'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('otherns')

    def test_list_defaults_namespace_from_client(self):
        self.client.get_namespace_metadata.return_value = {}

        result = self._invoke(['list'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('clientns')

    def test_query_k3s_version_defaults_namespace_from_client(self):
        self.client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'releases': {'stable': 'v1.33.4+k3s1'}
            }
        }

        result = self._invoke(['query-k3s-version', 'stable'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('clientns')
        self.assertIn('v1.33.4+k3s1', result.output)


class CreateNamespaceNoticeTestCase(testtools.TestCase):
    """create's 'Created namespace %s' notice now goes through a reporter.

    Step 1d moves this off a bare print() and onto the reporter that is
    also handed to the Cluster, so this pins the printed text exactly:
    routing it must not have changed a single character.
    """

    def setUp(self):
        super(CreateNamespaceNoticeTestCase, self).setUp()
        self.client = mock.MagicMock()
        self.client.namespace = 'clientns'
        self.client.get_namespace.return_value = None
        self.client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'releases': {'stable': 'v1.33.4+k3s1'}
            },
            # The name is already registered, so create fails fast right
            # after the namespace-created notice, before touching instances.
            primitives.CLUSTER_LIST: ['banana'],
        }
        self.runner = CliRunner()

    def test_namespace_created_notice_text_is_unchanged(self):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s,
            ['create', 'banana', '--namespace', 'newns'],
            obj={'VERBOSE': False, 'CLIENT': self.client})

        self.client.create_namespace.assert_called_once_with('newns')
        self.assertIn('Created namespace newns\n', result.output)
        self.assertEqual(1, result.exit_code)


class ListOutputTestCase(testtools.TestCase):
    """list prints the namespace's cluster names, one per line.

    The golden --help fixture pins the command's interface and
    NamespaceDefaultingTestCase pins its namespace resolution against empty
    metadata, so between them nothing asserts what the command actually
    prints. The body is a loop around print() in the Click layer now that
    primitives.list_clusters() returns the list, and that formatting is
    user-visible output this phase must not have changed.
    """

    def setUp(self):
        super(ListOutputTestCase, self).setUp()
        self.client = mock.MagicMock()
        self.client.namespace = 'clientns'
        self.runner = CliRunner()

    def test_list_prints_one_cluster_per_line(self):
        self.client.get_namespace_metadata.return_value = {
            primitives.CLUSTER_LIST: ['banana', 'apple']}

        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['list'], obj={'VERBOSE': False, 'CLIENT': self.client})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual('banana\napple\n', result.output)

    def test_list_prints_nothing_when_the_namespace_has_no_clusters(self):
        self.client.get_namespace_metadata.return_value = {}

        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['list'], obj={'VERBOSE': False, 'CLIENT': self.client})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual('', result.output)


class CreateNodeSizingOptionsTestCase(testtools.TestCase):
    """create's sizing options must reach Cluster.create() as keywords."""

    def setUp(self):
        super(CreateNodeSizingOptionsTestCase, self).setUp()
        self.client = mock.MagicMock()
        self.client.namespace = 'clientns'
        self.runner = CliRunner()

        patcher = mock.patch.object(cluster_module.Cluster, 'create')
        self.create = patcher.start()
        self.addCleanup(patcher.stop)

    def _invoke(self, *args):
        return self.runner.invoke(
            shakenfist_client_k3s.k3s, ['create', 'banana'] + list(args),
            obj={'VERBOSE': False, 'CLIENT': self.client})

    def test_the_options_are_forwarded_as_keywords(self):
        result = self._invoke(
            '--control-plane-cpus', '4', '--control-plane-memory', '8192',
            '--control-plane-disk', '100', '--worker-cpus', '6',
            '--worker-memory', '16384', '--worker-disk', '200')

        self.assertEqual(0, result.exit_code, result.output)
        self.create.assert_called_once()
        kwargs = self.create.call_args[1]
        self.assertEqual(
            {'control_plane_cpus': 4, 'control_plane_memory': 8192,
             'control_plane_disk': 100, 'worker_cpus': 6,
             'worker_memory': 16384, 'worker_disk': 200},
            {k: v for k, v in kwargs.items() if k.startswith(('control_plane_', 'worker_'))})

    def test_the_defaults_are_the_default_node_size(self):
        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        kwargs = self.create.call_args[1]
        size = cluster_module.DEFAULT_NODE_SIZE
        self.assertEqual(size['cpus'], kwargs['control_plane_cpus'])
        self.assertEqual(size['memory'], kwargs['control_plane_memory'])
        self.assertEqual(size['disk'], kwargs['control_plane_disk'])
        self.assertEqual(size['cpus'], kwargs['worker_cpus'])
        self.assertEqual(size['memory'], kwargs['worker_memory'])
        self.assertEqual(size['disk'], kwargs['worker_disk'])

    def test_a_zero_size_is_refused_before_create_is_called(self):
        # By the library's validate_create_arguments(), not by click: the
        # option is a plain click.INT, so this is the library's message and
        # the group handler's exit code 1. ArgumentRefusalTestCase in
        # test_cli_errors.py asserts the streams separately.
        result = self._invoke('--worker-memory', '0')

        self.assertEqual(1, result.exit_code)
        self.assertIn('worker memory must be a positive integer, not 0',
                      result.output)
        self.create.assert_not_called()

    def _write_config(self, text):
        f = tempfile.NamedTemporaryFile('w', encoding='utf-8', suffix='.yaml', delete=False)
        self.addCleanup(os.unlink, f.name)
        f.write(text)
        f.close()
        return f.name

    def test_the_config_files_are_forwarded_as_mappings(self):
        server = self._write_config('disable: [traefik]\nnode-label: [a=b]\n')
        agent = self._write_config('node-label: [c=d]\n')

        result = self._invoke('--server-config', server, '--agent-config', agent)

        self.assertEqual(0, result.exit_code, result.output)
        kwargs = self.create.call_args[1]
        self.assertEqual({'disable': ['traefik'], 'node-label': ['a=b']}, kwargs['server_config'])
        self.assertEqual({'node-label': ['c=d']}, kwargs['agent_config'])

    def test_omitting_the_config_files_passes_none(self):
        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        kwargs = self.create.call_args[1]
        self.assertIsNone(kwargs['server_config'])
        self.assertIsNone(kwargs['agent_config'])

    def test_a_refused_key_exits_before_create_is_called(self):
        # --namespace naming one which does not exist yet, because that is
        # the only case in which binding the context creates a namespace,
        # and so the only case in which binding before the file is read
        # would leave one behind.
        self.client.get_namespace.return_value = None
        server = self._write_config('token: x\n')

        result = self._invoke('--server-config', server, '--namespace', 'newns')

        self.assertNotEqual(0, result.exit_code)
        self.assertIn('token', result.output)
        self.assertIn(str(exceptions.K3sConfigError.owned_key('server', 'token')), result.output)
        self.create.assert_not_called()
        self.client.create_namespace.assert_not_called()


# A cluster which has finished being created, in the shape the three
# expansion commands read it in: one control plane node, one worker, one
# routed address, and the tokens and versions install_k3s_component wants.
EXISTING_MD = {
    'name': 'banana',
    'namespace': 'testns',
    'state': 'created',
    'node_serial': 2,
    'node_network': 'net-1',
    'control_plane_nodes': ['inst-cp1'],
    'worker_nodes': ['inst-w1'],
    'routed_addresses': ['192.168.10.1'],
    'node_token': 'node-token',
    'server_token': 'server-token',
    'k3s_version': 'v1.33',
    'api_address_inner': '10.0.0.4',
}


class CommandWiringTestCase(testtools.TestCase):
    """The one-line command bodies forward the right argument.

    expand-workers, expand-addresses and update-os stopped being command
    bodies in this phase and became argument parsing plus a single Cluster
    call. Nothing else pins that wiring: the golden --help fixtures assert
    the interface, not the forwarding, and would still pass if
    expand-addresses called expand_addresses(worker_count) or if a count
    were dropped on the floor. So each of these drives the real method
    against a scripted client and counts what it did.

    delete is here for the same reason and one more: its
    update_kubeconfig defaults to False in the library and the command line
    passes True, so the forwarding is the only thing standing between
    ``k3s delete`` and a silent change to what the command does.
    """

    def setUp(self):
        super(CommandWiringTestCase, self).setUp()
        self.client = fakes.FakeClusterClient()
        # Deep, not shallow: expand-workers appends to worker_nodes and
        # expand-addresses to routed_addresses, so a shallow copy would
        # leave those lists shared with the module level constant and let
        # each test grow the fixture for the tests which follow it.
        self.client.metadata[cluster_module.METADATA_KEY % 'banana'] = (
            copy.deepcopy(EXISTING_MD))
        self.client.metadata[primitives.CLUSTER_LIST] = ['banana']

        # The two nodes the seeded metadata claims already exist. The wait
        # loops call get_instance() on them, which the fake answers from
        # this dict.
        for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-000'),
                                    ('inst-w1', 'k3s-banana-node-001')]:
            self.client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        # Nothing on these paths should reach the network, a local kubectl
        # or the operator's own ~/.kube/config, and a test which is wrong
        # about that must fail rather than do it.
        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        patcher = mock.patch.dict('os.environ', {'HOME': home.name})
        patcher.start()
        self.addCleanup(patcher.stop)

        self.subprocess_run = mock.MagicMock()
        self.subprocess_run.return_value.returncode = 0
        # delete reads the kubeconfig before it removes anything from it.
        self.subprocess_run.side_effect = fakes.FakeKubectl(['banana.testns'])
        patcher = mock.patch('subprocess.run', self.subprocess_run)
        patcher.start()
        self.addCleanup(patcher.stop)

        for target in ['get_k3s_release', 'get_longhorn_release']:
            patcher = mock.patch(
                'shakenfist_client_k3s.primitives.%s' % target,
                side_effect=AssertionError('%s must not be called here' % target))
            patcher.start()
            self.addCleanup(patcher.stop)

        self.runner = CliRunner()

    def _md(self):
        return self.client.metadata[cluster_module.METADATA_KEY % 'banana']

    def test_expand_workers_forwards_the_worker_count(self):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s,
            ['expand-workers', 'banana', '--worker-count', '3'],
            obj={'VERBOSE': False, 'CLIENT': self.client})

        self.assertEqual(0, result.exit_code, result.output)
        # Three new instances, and each recorded against the cluster
        # alongside the worker it already had.
        self.assertEqual(3, self.client.instance_serial)
        self.assertEqual(4, len(self._md()['worker_nodes']))
        self.assertEqual(0, self.client.routed_serial)
        self.assertIn('Added 3 workers to cluster banana', result.output)

    def test_expand_addresses_forwards_the_address_count(self):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s,
            ['expand-addresses', 'banana', '--address-count', '3'],
            obj={'VERBOSE': False, 'CLIENT': self.client})

        self.assertEqual(0, result.exit_code, result.output)
        # Three addresses routed, on top of the one the cluster had, and
        # no instances created: this is the command which would look
        # identical if it were handed the wrong count.
        self.assertEqual(3, self.client.routed_serial)
        self.assertEqual(4, len(self._md()['routed_addresses']))
        self.assertEqual(0, self.client.instance_serial)
        self.assertIn('Added 3 metallb addresses to cluster banana',
                      result.output)

    def test_delete_updates_the_local_kubeconfig(self):
        # A file to clean, or the cleanup runs no kubectl at all.
        path = fakes.home_with_kubeconfig(self)
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['delete', 'banana'],
            obj={'VERBOSE': False, 'CLIENT': self.client})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            [fakes.cleanup_kubectl(path, fakes.KUBECTL_CONFIG_VIEW_JSON),
             fakes.cleanup_kubectl(path, ['config', 'delete-context', 'banana.testns']),
             fakes.cleanup_kubectl(path, ['config', 'delete-user', 'banana.testns']),
             fakes.cleanup_kubectl(path, ['config', 'delete-cluster', 'banana.testns'])],
            [call[0][0] for call in self.subprocess_run.call_args_list])

    def test_delete_with_no_kubeconfig_leaves_it_alone(self):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['delete', 'banana', '--no-kubeconfig'],
            obj={'VERBOSE': False, 'CLIENT': self.client})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual([], self.subprocess_run.call_args_list)

        # The cluster still went away: --no-kubeconfig declines one local
        # side effect, not the delete.
        self.assertNotIn(cluster_module.METADATA_KEY % 'banana',
                         self.client.metadata)

    def test_update_os_updates_every_node(self):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['update-os', 'banana'],
            obj={'VERBOSE': False, 'CLIENT': self.client})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            [('inst-cp1', 'apt-get update'),
             ('inst-w1', 'apt-get update'),
             ('inst-cp1', 'apt-get dist-upgrade -y'),
             ('inst-w1', 'apt-get dist-upgrade -y')],
            self.client.executed)
        self.assertIn('Updated the OS on all nodes in cluster banana',
                      result.output)


class DeleteRecordingClient(fakes.FakeClusterClient):
    """A scripted client which also records the instances it was asked to delete."""

    def __init__(self):
        super(DeleteRecordingClient, self).__init__()
        self.deleted_instances = []

    def delete_instance(self, instance_ref):
        self.deleted_instances.append(instance_ref)
        return super(DeleteRecordingClient, self).delete_instance(instance_ref)


class RemoveWorkerCommandTestCase(testtools.TestCase):
    """remove-worker's option parsing, and what it does with what it parsed.

    --worker is repeatable and required, and the library method takes a
    list of uuids, so there are three ways for this wiring to be wrong
    which the golden --help fixture cannot see: the tuple Click builds
    reaching the method unconverted, only the first or last --worker being
    forwarded, and the command name resolving to some other verb entirely.
    The error path is here too, because turning a raised
    WorkerNotFoundError back into an exit code is the Click layer's job.
    """

    def setUp(self):
        super(RemoveWorkerCommandTestCase, self).setUp()
        self.client = DeleteRecordingClient()

        md = copy.deepcopy(EXISTING_MD)
        md['worker_nodes'] = ['inst-w1', 'inst-w2', 'inst-w3']
        self.client.metadata[cluster_module.METADATA_KEY % 'banana'] = md
        self.client.metadata[primitives.CLUSTER_LIST] = ['banana']

        for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-001'),
                                    ('inst-w1', 'k3s-banana-node-002'),
                                    ('inst-w2', 'k3s-banana-node-003'),
                                    ('inst-w3', 'k3s-banana-node-004')]:
            self.client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.runner = CliRunner()

    def _invoke(self, args):
        return self.runner.invoke(
            shakenfist_client_k3s.k3s, args,
            obj={'VERBOSE': False, 'CLIENT': self.client})

    def _md(self):
        return self.client.metadata[cluster_module.METADATA_KEY % 'banana']

    def test_one_worker_is_removed(self):
        result = self._invoke(['remove-worker', 'banana', '--worker', 'inst-w2'])

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(['inst-w2'], self.client.deleted_instances)
        self.assertEqual(['inst-w1', 'inst-w3'], self._md()['worker_nodes'])
        self.assertIn('Removed 1 worker from cluster banana', result.output)

    def test_the_option_may_be_repeated(self):
        result = self._invoke(['remove-worker', 'banana',
                               '--worker', 'inst-w1', '--worker', 'inst-w3'])

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(['inst-w1', 'inst-w3'], self.client.deleted_instances)
        self.assertEqual(['inst-w2'], self._md()['worker_nodes'])
        self.assertIn('Removed 2 workers from cluster banana', result.output)

    def test_the_option_is_required(self):
        result = self._invoke(['remove-worker', 'banana'])

        self.assertEqual(2, result.exit_code, result.output)
        self.assertIn("Missing option '--worker'", result.output)
        self.assertEqual([], self.client.deleted_instances)

    def test_an_unknown_worker_fails_the_command_and_deletes_nothing(self):
        result = self._invoke(['remove-worker', 'banana',
                               '--worker', 'inst-w1', '--worker', 'inst-w9'])

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn('inst-w9', result.output)
        self.assertEqual([], self.client.deleted_instances)
        self.assertEqual(['inst-w1', 'inst-w2', 'inst-w3'],
                         self._md()['worker_nodes'])


class HealthCommandTestCase(testtools.TestCase):
    """health renders the report, and renders an unhealthy cluster without failing.

    The library method is covered in test_cluster.py; what is here is the
    rendering, which is the half of decision 7 the library tests cannot
    see. Three things can go wrong in it and nothing else pins them: the
    report could be rendered with a bare print() rather than through the
    reporter, an unhealthy cluster could be turned into a non-zero exit by
    default (health is a report, not a judgement, and the verb which exits
    1 cannot be used to find out whether it should), and a node the
    metadata names which no longer exists has no name to interpolate, so
    rendering it is the one line most likely to raise a TypeError in front
    of the operator who most needed the report.

    --strict is the supported way to get the judgement, and it changes the
    exit code and nothing else: the report is rendered identically either
    way, so a caller which wants both the text and the branch gets them
    from one run.
    """

    def setUp(self):
        super(HealthCommandTestCase, self).setUp()
        self.client = fakes.HealthClient()

        md = copy.deepcopy(EXISTING_MD)
        md['worker_nodes'] = ['inst-w1', 'inst-w2']
        self.client.metadata[cluster_module.METADATA_KEY % 'banana'] = md
        self.client.metadata[primitives.CLUSTER_LIST] = ['banana']
        self.md = md

        for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-001'),
                                    ('inst-w1', 'k3s-banana-node-002'),
                                    ('inst-w2', 'k3s-banana-node-003')]:
            self.client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.runner = CliRunner()

    def _invoke(self, *args):
        return self.runner.invoke(
            shakenfist_client_k3s.k3s, ['health', 'banana'] + list(args),
            obj={'VERBOSE': False, 'CLIENT': self.client})

    def _make_unhealthy(self):
        self.client.instances['inst-w1']['state'] = 'error'
        self.client.instances['inst-w1']['agent_state'] = None
        self.client.probe_return_code = 1
        self.client.probe_stdout = ''
        self.client.probe_stderr = (
            'The connection to the server 127.0.0.1:6443 was refused\n')

    def test_a_healthy_cluster_renders_every_node_and_the_api(self):
        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('Cluster banana in namespace testns is healthy',
                      result.output)
        self.assertIn('state: created', result.output)
        self.assertIn('[ok] k3s-banana-node-001 (inst-cp1, control plane): '
                      'instance created, agent ready', result.output)
        self.assertIn('[ok] k3s-banana-node-002 (inst-w1, worker): '
                      'instance created, agent ready', result.output)
        self.assertIn('[ok] k3s-banana-node-003 (inst-w2, worker): '
                      'instance created, agent ready', result.output)
        self.assertIn('k3s API: answered on inst-cp1', result.output)

        # kubectl's own output is the most useful thing in the report, so it
        # is rendered rather than discarded.
        self.assertIn('k3s-banana-node-001   Ready    control-plane',
                      result.output)

    def test_a_healthy_cluster_renders_each_nodes_signals_under_its_line(self):
        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)

        # etcd is only read on the control plane, so only its line has the
        # etcd readings.
        self.assertIn(
            '(inst-cp1, control plane): instance created, agent ready\n'
            '        booted 2025-10-06T00:59:05Z, k3s active, 3 restarts, '
            '2 OOM kills, 2809 of 3927 MiB available, '
            'etcd 65 MiB, snapshots 40 MiB\n', result.output)
        self.assertIn(
            '(inst-w1, worker): instance created, agent ready\n'
            '        booted 2025-10-06T00:59:59Z, k3s-agent activating, '
            '0 restarts, 0 OOM kills, 1076 of 1963 MiB available\n',
            result.output)
        self.assertEqual(1, result.output.count('etcd '))
        self.assertNotIn('None', result.output)

    def test_a_node_whose_signals_were_skipped_says_so(self):
        del self.client.instances['inst-w2']

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn(
            '[!!] inst-w2 (worker): this instance no longer exists\n'
            '        signals: not read (', result.output)
        self.assertNotIn('None', result.output)

    def test_a_node_with_missing_readings_renders_unknown(self):
        # No memory or etcd lines at all, so those readings are None in
        # the report and the line must keep its shape.
        self.client.signals_stdout['inst-cp1'] = (
            'boot_id=3f0c3c4e-5b8e-4f43-9d1c-0d6a8f2b7e11\nbooted_at=1759712345\n'
            'oom_kills=2\n'
            'NRestarts=3\nLoadState=loaded\nActiveState=active\n')

        result = self._invoke()

        self.assertIn(
            '        booted 2025-10-06T00:59:05Z, k3s active, 3 restarts, '
            '2 OOM kills, unknown of unknown MiB available, '
            'etcd unknown MiB, snapshots unknown MiB\n', result.output)
        self.assertNotIn('None', result.output)

    def test_a_probed_node_with_an_error_appends_it(self):
        self.client.signals_return_code['inst-w1'] = 1
        self.client.signals_stderr['inst-w1'] = 'du: cannot read\n'

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertRegex(
            result.output,
            r'1076 of 1963 MiB available \(.+\)\n')
        self.assertNotIn('None', result.output)

    def test_an_unhealthy_cluster_renders_and_still_exits_zero(self):
        self._make_unhealthy()

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('Cluster banana in namespace testns is NOT healthy',
                      result.output)
        self.assertIn('[!!] k3s-banana-node-002 (inst-w1, worker): '
                      'instance error, agent not yet contactable',
                      result.output)
        self.assertIn('k3s API: did not answer', result.output)
        self.assertIn('The connection to the server 127.0.0.1:6443 was refused',
                      result.output)

    def test_a_node_which_no_longer_exists_renders(self):
        del self.client.instances['inst-w2']

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('[!!] inst-w2 (worker): this instance no longer exists',
                      result.output)

    def test_an_interrupted_cluster_renders_its_state(self):
        self.md['state'] = 'initial'
        self.md['control_plane_nodes'] = []
        self.md['worker_nodes'] = []

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('state: initial (interrupted: this cluster never '
                      'finished being built)', result.output)
        self.assertIn('this cluster has no nodes', result.output)
        self.assertIn('no control plane node', result.output)

    def test_strict_exits_one_for_an_unhealthy_cluster(self):
        self._make_unhealthy()

        result = self._invoke('--strict')

        self.assertEqual(1, result.exit_code, result.output)

    def test_strict_renders_the_same_report_it_would_have_anyway(self):
        # The exit code is the only difference. A --strict which printed
        # something extra, or which exited before rendering, would make
        # "run it once and both read and branch on the answer" impossible,
        # which is the entire reason for the flag.
        self._make_unhealthy()

        plain = self._invoke()
        strict = self._invoke('--strict')

        self.assertEqual(0, plain.exit_code)
        self.assertEqual(1, strict.exit_code)
        self.assertEqual(plain.output, strict.output)

    def test_strict_exits_zero_for_a_healthy_cluster(self):
        result = self._invoke('--strict')

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('Cluster banana in namespace testns is healthy',
                      result.output)

    def test_no_strict_is_the_default(self):
        self._make_unhealthy()

        result = self._invoke('--no-strict')

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(self._invoke().output, result.output)

    def test_a_healthy_cluster_renders_each_nodes_kubernetes_readings(self):
        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)

        # The kubernetes line follows the signals line it sits beside, and
        # an OOM kill is a further indented line under the node it ran on.
        self.assertIn(
            'booted 2025-10-06T00:59:05Z, k3s active, 3 restarts, '
            '2 OOM kills, 2809 of 3927 MiB available, '
            'etcd 65 MiB, snapshots 40 MiB\n'
            '        kubernetes: Ready since 2026-10-05T08:12:25Z, no pressure\n'
            '    [ok] k3s-banana-node-002', result.output)
        self.assertIn(
            '        kubernetes: Ready since 2026-10-05T08:13:02Z, no pressure\n'
            '            OOM killed: default/memory-hog-7d9f8b6c5-x2x7k hog '
            'at 2026-10-06T21:40:11Z, 2 restarts\n'
            '    [ok] k3s-banana-node-003', result.output)
        self.assertEqual(1, result.output.count('OOM killed:'))
        self.assertNotIn('unmatched', result.output)
        self.assertNotIn('Kubernetes probe', result.output)
        self.assertNotIn('None', result.output)

    def test_a_cluster_whose_kubernetes_probe_did_not_answer_says_so(self):
        self.client.kubernetes_return_code = 1
        self.client.kubernetes_stdout = ''
        self.client.kubernetes_stderr = 'error: the server doesn\'t have a resource\n'

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('Cluster banana in namespace testns is NOT healthy',
                      result.output)
        self.assertEqual(3, result.output.count('kubernetes: not read ('))
        self.assertIn('  Kubernetes probe: did not answer (', result.output)
        self.assertNotIn('OOM killed', result.output)
        self.assertNotIn('None', result.output)

    def test_a_node_which_is_not_ready_is_reported_and_not_hidden(self):
        self.client.kubernetes_stdout = (
            'node\tk3s-banana-node-001\tTrue\tFalse\tFalse\tFalse\t'
            '2026-10-05T08:12:25Z\n'
            'node\tk3s-banana-node-002\tFalse\tTrue\tFalse\tTrue\t'
            '2026-10-06T08:13:02Z\n'
            'node\tk3s-banana-node-003\tUnknown\tFalse\tFalse\tFalse\t'
            '2026-10-06T09:13:09Z\n')

        result = self._invoke('--strict')

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn(
            '        kubernetes: NotReady (False) since 2026-10-06T08:13:02Z, '
            'MemoryPressure, PIDPressure\n', result.output)
        self.assertIn(
            '        kubernetes: NotReady (Unknown) since 2026-10-06T09:13:09Z, '
            'no pressure\n', result.output)
        self.assertNotIn('OOM killed', result.output)

    def test_a_node_kubernetes_does_not_know_and_a_stale_node_render(self):
        # k3s-banana-node-003 never registered, and a node object with no
        # instance is left behind.
        self.client.kubernetes_stdout = (
            'node\tk3s-banana-node-001\tTrue\tFalse\tFalse\tFalse\t'
            '2026-10-05T08:12:25Z\n'
            'node\tk3s-banana-node-002\tTrue\tFalse\tFalse\tFalse\t'
            '2026-10-05T08:13:02Z\n'
            'node\tzombie-b\tTrue\tFalse\tFalse\tFalse\t'
            '2026-10-05T08:13:09Z\n'
            'node\tzombie-a\tTrue\tFalse\tFalse\tFalse\t'
            '2026-10-05T08:13:09Z\n')

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('        kubernetes: not registered\n', result.output)
        self.assertIn('  Kubernetes: unmatched nodes zombie-a, zombie-b\n',
                      result.output)

    def test_a_node_unread_for_its_own_reason_is_not_blamed_on_the_probe(self):
        # An instance which is gone has no name to match, and the probe
        # answered, so the line must not read as though it had not.
        del self.client.instances['inst-w2']

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn(
            'this instance no longer exists\n'
            '        signals: not read (', result.output)
        self.assertIn(
            '        kubernetes: not read (no instance name to match)\n',
            result.output)
        self.assertNotIn('Kubernetes probe: did not answer', result.output)

    def test_nodes_sharing_a_name_are_unread_and_say_why(self):
        self.client.instances['inst-w2']['name'] = 'K3S-BANANA-NODE-002'

        result = self._invoke()

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            2, result.output.count(
                'kubernetes: not read (another node has the same name)'))

        # The Kubernetes node the two share is not one no instance accounts
        # for. node-003's was renamed away from, so it is the only stale one.
        self.assertIn('  Kubernetes: unmatched nodes k3s-banana-node-003\n',
                      result.output)

    def test_kubernetes_readings_never_change_the_exit_code_without_strict(self):
        self.client.kubernetes_return_code = 1

        self.assertEqual(0, self._invoke().exit_code)
        self.assertEqual(1, self._invoke('--strict').exit_code)

    def test_an_unknown_cluster_fails_the_command(self):
        client = fakes.HealthClient()
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['health', 'banana'],
            obj={'VERBOSE': False, 'CLIENT': client})

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn('does not appear to exist', result.output)


class HealthRenderingReporterTestCase(testtools.TestCase):
    """The health rendering writes to the reporter it is handed, not to stdout.

    HealthCommandTestCase cannot see the difference: Click's CliRunner
    replaces sys.stdout, and the default Reporter writes to sys.stdout, so
    a bare print() in the renderer would land in result.output and pass
    every assertion there. This drives the renderer directly with a
    collecting reporter, which is the only arrangement in which the two
    destinations are distinguishable -- and it is the arrangement phase 5's
    Ansible module will be in, where stdout carries a JSON result and
    anything else written there corrupts it.
    """

    # A minimal report, in the shape Cluster.health() returns and with one
    # of everything the renderer has a branch for.
    REPORT = {
        'name': 'banana',
        'namespace': 'testns',
        'state': 'created',
        'interrupted': False,
        'nodes': [
            {'uuid': 'inst-cp1', 'role': 'control_plane',
             'name': 'k3s-banana-node-001', 'exists': True,
             'state': 'created', 'agent_state': 'ready', 'healthy': True,
             'signals': {
                 'probed': True, 'error': None, 'boot_id': 'abc',
                 'booted_at': 1759712345, 'k3s_unit': 'k3s',
                 'k3s_state': 'active', 'k3s_restarts': 1, 'oom_kills': 0,
                 'memory_total_bytes': 4 * 1048576 * 1000,
                 'memory_available_bytes': 3 * 1048576 * 1000,
                 'etcd_bytes': 64 * 1048576,
                 'etcd_snapshot_bytes': 32 * 1048576}},
            {'uuid': 'inst-w1', 'role': 'worker', 'name': None,
             'exists': False, 'state': None, 'agent_state': None,
             'healthy': False,
             'signals': {
                 'probed': False, 'error': 'instance is gone',
                 'boot_id': None, 'booted_at': None, 'k3s_unit': 'k3s-agent',
                 'k3s_state': None, 'k3s_restarts': None, 'oom_kills': None,
                 'memory_total_bytes': None, 'memory_available_bytes': None,
                 'etcd_bytes': None, 'etcd_snapshot_bytes': None}}
        ],
        'api': {
            'probed': True, 'answered': True, 'instance_uuid': 'inst-cp1',
            'command': 'kubectl get nodes', 'return_code': 0,
            'stdout': 'NAME   STATUS\nk3s-banana-node-001   Ready\n',
            'stderr': '', 'error': None
        },
        'healthy': False
    }

    def test_an_existing_instance_with_no_name_is_not_rendered_as_None(self):
        # _node_health() reads every field with .get(), so an instance which
        # exists and has no name is a shape the report can carry. The
        # existing-but-unnamed case has its own line here because the
        # renderer only special-cases the instance which is gone, and the
        # literal string None in a health report is worse than useless --
        # remove_worker() refuses the same shape outright.
        report = copy.deepcopy(self.REPORT)
        report['nodes'][1].update({'name': None, 'exists': True,
                                   'state': 'created', 'agent_state': 'ready'})
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, report)

        self.assertIn('[!!] (unnamed) (inst-w1, worker): instance created',
                      reporter.getvalue())
        self.assertNotIn('None', reporter.getvalue())

    def test_signals_render_under_each_node_line(self):
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, self.REPORT)

        self.assertIn(
            '(inst-cp1, control plane): instance created, agent ready\n'
            '        booted 2025-10-06T00:59:05Z, k3s active, 1 restart, '
            '0 OOM kills, 3000 of 4000 MiB available, '
            'etcd 64 MiB, snapshots 32 MiB\n', reporter.getvalue())
        self.assertIn(
            'this instance no longer exists\n'
            '        signals: not read (instance is gone)\n',
            reporter.getvalue())
        self.assertNotIn('None', reporter.getvalue())

    def test_unread_values_render_unknown_and_never_None(self):
        report = copy.deepcopy(self.REPORT)
        report['nodes'][0]['signals'].update({
            'k3s_state': None, 'k3s_restarts': None, 'booted_at': None,
            'memory_available_bytes': None, 'etcd_bytes': None})
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, report)

        self.assertIn(
            '        booted unknown, k3s unknown, unknown restarts, '
            '0 OOM kills, unknown of 4000 MiB available, '
            'etcd unknown MiB, snapshots 32 MiB\n', reporter.getvalue())
        self.assertNotIn('None', reporter.getvalue())

    def test_a_boot_time_datetime_cannot_hold_renders_unknown(self):
        # The parser accepts up to twenty digits as a btime, and datetime
        # takes far fewer: twenty nines of seconds is far past the year
        # 9999, and fromtimestamp() raises for it (OverflowError, OSError or
        # ValueError, depending on the platform and where it overflows).
        # That must cost the one reading, not the report: the rest of the
        # line, and the rest of the nodes, are rendered as usual.
        report = copy.deepcopy(self.REPORT)
        report['nodes'][0]['signals']['booted_at'] = int('9' * 20)
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, report)

        self.assertIn(
            '        booted unknown, k3s active, 1 restart, '
            '0 OOM kills, 3000 of 4000 MiB available, '
            'etcd 64 MiB, snapshots 32 MiB\n', reporter.getvalue())
        self.assertIn('signals: not read (instance is gone)',
                      reporter.getvalue())
        self.assertIn('k3s API: answered on inst-cp1', reporter.getvalue())

    # Every int reading, and the line they render to when none of them is an
    # int. The unit and its state are strings and stay as they are.
    INT_READINGS = ('booted_at', 'k3s_restarts', 'oom_kills',
                    'memory_total_bytes', 'memory_available_bytes',
                    'etcd_bytes', 'etcd_snapshot_bytes')
    ALL_UNKNOWN = (
        '        booted unknown, k3s active, unknown restarts, '
        'unknown OOM kills, unknown of unknown MiB available, '
        'etcd unknown MiB, snapshots unknown MiB\n')

    def _render_with(self, readings):
        report = copy.deepcopy(self.REPORT)
        report['nodes'][0]['signals'].update(readings)
        reporter = progress.CollectingReporter()
        shakenfist_client_k3s._render_health(reporter, report)
        return reporter.getvalue()

    def test_string_readings_render_unknown(self):
        # A caller which built the report by hand, or read it back from
        # somewhere that stores everything as text. '1759712345' // 1048576
        # and fromtimestamp('1759712345') both raise TypeError, which used
        # to lose the whole report; '3' restarts rendered as if it had been
        # read from systemd.
        rendered = self._render_with(
            {key: '1759712345' for key in self.INT_READINGS})

        self.assertIn(self.ALL_UNKNOWN, rendered)
        self.assertIn('k3s API: answered on inst-cp1', rendered)

    def test_float_readings_render_unknown(self):
        # The shape a JSON round trip through something which does not keep
        # ints apart from floats leaves. A float size used to render as a
        # fractional MiB, and a float restart count as '3.0 restarts'; a
        # float boot time happened to render, but is no more a reading this
        # package took than the others are.
        rendered = self._render_with(
            {key: 1759712345.0 for key in self.INT_READINGS})

        self.assertIn(self.ALL_UNKNOWN, rendered)
        self.assertNotIn('.0', rendered)
        self.assertNotIn('2025-', rendered)

    def test_bool_readings_render_unknown(self):
        # bool is a subclass of int, so isinstance(True, int) alone would
        # let True through as a restart count of 1 and a boot time of one
        # second past the epoch. It is not a reading of either.
        for flag in (True, False):
            rendered = self._render_with(
                {key: flag for key in self.INT_READINGS})

            self.assertIn(self.ALL_UNKNOWN, rendered, flag)
            self.assertNotIn('True', rendered, flag)
            self.assertNotIn('False', rendered, flag)
            self.assertNotIn('1970-', rendered, flag)

    def test_a_unit_state_which_is_not_a_string_renders_unknown(self):
        # The same rule for the two string readings: a state of 7 is not
        # something systemd said.
        rendered = self._render_with({'k3s_unit': 7, 'k3s_state': True})

        self.assertIn('        booted 2025-10-06T00:59:05Z, unknown unknown, '
                      '1 restart, ', rendered)

    def test_the_epoch_is_a_boot_time_and_not_unknown(self):
        # The negative of the above: zero is a timestamp datetime can hold,
        # and a falsy one, so it is rendered rather than caught or mistaken
        # for a reading which was not taken.
        report = copy.deepcopy(self.REPORT)
        report['nodes'][0]['signals']['booted_at'] = 0
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, report)

        self.assertIn('        booted 1970-01-01T00:00:00Z, k3s active, ',
                      reporter.getvalue())

    def test_hand_built_sizes_at_str_s_digit_limit_still_render(self):
        # The other conversions on an int, swept for the same failure:
        # Python 3.11 and later refuse str() of an int past 4300 digits. The
        # parser caps a reading at twenty digits, so nothing it produces
        # comes near that, but the renderer also takes reports a caller
        # built, and must not raise on those either. A size at the limit,
        # scaled by 1024 as memory is, renders because mib() divides before
        # str() sees it.
        biggest = int('9' * 4300)
        report = copy.deepcopy(self.REPORT)
        report['nodes'][0]['signals'].update({
            'memory_total_bytes': biggest * 1024,
            'memory_available_bytes': biggest * 1024,
            'k3s_restarts': biggest, 'oom_kills': biggest,
            'etcd_bytes': biggest, 'etcd_snapshot_bytes': biggest})
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, report)

        self.assertIn('k3s API: answered on inst-cp1', reporter.getvalue())
        self.assertIn(' MiB available, etcd ', reporter.getvalue())

    def test_a_count_of_one_is_singular(self):
        # And only one: zero and every other count are plural, as is a
        # count which could not be read ('unknown restarts').
        for restarts, oom_kills, expected in (
                (1, 1, '1 restart, 1 OOM kill, '),
                (0, 2, '0 restarts, 2 OOM kills, '),
                (None, 1, 'unknown restarts, 1 OOM kill, '),
                (11, None, '11 restarts, unknown OOM kills, ')):
            rendered = self._render_with(
                {'k3s_restarts': restarts, 'oom_kills': oom_kills})

            self.assertIn('k3s active, %s3000 of 4000 MiB available' % expected,
                          rendered)

    def test_a_worker_line_has_no_etcd(self):
        report = copy.deepcopy(self.REPORT)
        report['nodes'][1].update({'exists': True, 'name': 'w', 'state': 'created',
                                   'agent_state': 'ready'})
        report['nodes'][1]['signals'].update({
            'probed': True, 'error': None, 'booted_at': 0,
            'k3s_state': 'active', 'k3s_restarts': 0, 'oom_kills': 0,
            'memory_total_bytes': 1048576, 'memory_available_bytes': 1048576})
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, report)

        self.assertIn(
            '        booted 1970-01-01T00:00:00Z, k3s-agent active, '
            '0 restarts, 0 OOM kills, 1 of 1 MiB available\n',
            reporter.getvalue())

    def test_a_probed_node_with_an_error_appends_it(self):
        report = copy.deepcopy(self.REPORT)
        report['nodes'][0]['signals']['error'] = 'du timed out'
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, report)

        self.assertIn('snapshots 32 MiB (du timed out)\n', reporter.getvalue())

    def test_a_node_with_no_signals_at_all_is_skipped(self):
        # A report from an older library, or built by hand.
        report = copy.deepcopy(self.REPORT)
        for node in report['nodes']:
            del node['signals']
        reporter = progress.CollectingReporter()

        shakenfist_client_k3s._render_health(reporter, report)

        self.assertNotIn('booted', reporter.getvalue())
        self.assertNotIn('signals:', reporter.getvalue())

    # A report with a Kubernetes reading for each node of REPORT, and the
    # probe's own outcome, in the shape Cluster.health() returns.
    KUBERNETES_REPORT = {
        'probed': True, 'answered': True, 'error': None,
        'unmatched_nodes': []}

    def _kubernetes_report(self, first=None, second=None, probe=None):
        report = copy.deepcopy(self.REPORT)
        report['nodes'][0]['kubernetes'] = {
            'registered': True, 'ready': 'True', 'ready_since': 1791187945,
            'memory_pressure': 'False', 'disk_pressure': 'False',
            'pid_pressure': 'False', 'oom_killed': []}
        report['nodes'][1]['kubernetes'] = {
            'registered': None, 'ready': None, 'ready_since': None,
            'memory_pressure': None, 'disk_pressure': None,
            'pid_pressure': None, 'oom_killed': None}
        report['kubernetes'] = dict(self.KUBERNETES_REPORT)
        report['nodes'][0]['kubernetes'].update(first or {})
        report['nodes'][1]['kubernetes'].update(second or {})
        report['kubernetes'].update(probe or {})
        return report

    def _render_kubernetes(self, report):
        reporter = progress.CollectingReporter()
        shakenfist_client_k3s._render_health(reporter, report)
        return reporter.getvalue()

    def test_kubernetes_renders_under_the_signals_of_each_node(self):
        rendered = self._render_kubernetes(self._kubernetes_report())

        self.assertIn(
            'etcd 64 MiB, snapshots 32 MiB\n'
            '        kubernetes: Ready since 2026-10-05T08:12:25Z, no pressure\n'
            '    [!!] inst-w1 (worker): this instance no longer exists\n'
            '        signals: not read (instance is gone)\n'
            '        kubernetes: not read (no instance name to match)\n',
            rendered)
        self.assertNotIn('None', rendered)

    def test_each_ready_status_and_pressure_renders(self):
        for ready, expected in (('True', 'Ready since'),
                                ('False', 'NotReady (False) since'),
                                ('Unknown', 'NotReady (Unknown) since')):
            rendered = self._render_kubernetes(self._kubernetes_report(
                first={'ready': ready, 'disk_pressure': 'True'}))

            self.assertIn('        kubernetes: %s 2026-10-05T08:12:25Z, '
                          'DiskPressure\n' % expected, rendered)
            self.assertNotIn('no pressure', rendered)

        rendered = self._render_kubernetes(self._kubernetes_report(
            first={'memory_pressure': 'True', 'disk_pressure': 'True',
                   'pid_pressure': 'True'}))
        self.assertIn(', MemoryPressure, DiskPressure, PIDPressure\n', rendered)

    def test_a_pressure_condition_which_was_not_read_is_not_no_pressure(self):
        # 'Unknown' is the node controller having lost the kubelet, and a
        # condition the node did not report is None. Neither is evidence
        # of there being no pressure.
        rendered = self._render_kubernetes(self._kubernetes_report(
            first={'memory_pressure': 'Unknown', 'disk_pressure': None}))

        self.assertIn(', MemoryPressure unknown, DiskPressure unknown\n', rendered)
        self.assertNotIn('no pressure', rendered)
        self.assertNotIn('None', rendered)

    def test_a_ready_node_with_no_time_has_no_since(self):
        rendered = self._render_kubernetes(self._kubernetes_report(
            first={'ready_since': None}))

        self.assertIn('        kubernetes: Ready, no pressure\n', rendered)

    def test_a_ready_time_which_cannot_be_shown_is_unknown(self):
        for since in ('1759712345', 1759712345.0, True, int('9' * 20)):
            rendered = self._render_kubernetes(self._kubernetes_report(
                first={'ready_since': since}))

            self.assertIn('        kubernetes: Ready since unknown, no pressure\n',
                          rendered, since)

    def test_a_ready_status_which_is_not_a_status_is_unknown(self):
        for ready in (None, 7, True, 'Banana'):
            rendered = self._render_kubernetes(self._kubernetes_report(
                first={'ready': ready}))

            self.assertIn('        kubernetes: readiness unknown since ',
                          rendered, ready)
            self.assertNotIn('None', rendered)

    def test_an_unregistered_node_says_so(self):
        rendered = self._render_kubernetes(self._kubernetes_report(
            first={'registered': False, 'ready': None, 'ready_since': None,
                   'memory_pressure': None, 'disk_pressure': None,
                   'pid_pressure': None, 'oom_killed': []}))

        self.assertIn('        kubernetes: not registered\n', rendered)
        self.assertNotIn('since', rendered)
        self.assertNotIn('pressure', rendered)
        self.assertNotIn('None', rendered)

    OOM_KILLS = [
        {'namespace': 'default', 'pod': 'hog-1', 'container': 'hog',
         'restarts': 1, 'finished_at': 1791322811},
        {'namespace': 'kube-system', 'pod': 'dns-0', 'container': 'dns',
         'restarts': 0, 'finished_at': 1791322900}]

    def test_each_oom_kill_is_a_line_under_its_node(self):
        rendered = self._render_kubernetes(self._kubernetes_report(
            first={'oom_killed': self.OOM_KILLS}))

        self.assertIn(
            'no pressure\n'
            '            OOM killed: default/hog-1 hog at 2026-10-06T21:40:11Z, '
            '1 restart\n'
            '            OOM killed: kube-system/dns-0 dns at '
            '2026-10-06T21:41:40Z, 0 restarts\n', rendered)

    def test_oom_kills_on_an_unregistered_node_are_still_shown(self):
        rendered = self._render_kubernetes(self._kubernetes_report(
            first={'registered': False, 'oom_killed': self.OOM_KILLS[:1]}))

        self.assertIn('kubernetes: not registered\n'
                      '            OOM killed: default/hog-1 hog at ', rendered)

    def test_oom_kill_values_of_the_wrong_type_render_unknown(self):
        rendered = self._render_kubernetes(self._kubernetes_report(
            first={'oom_killed': [
                {'namespace': 7, 'pod': None, 'container': True,
                 'restarts': '3', 'finished_at': 1.5},
                {'namespace': 'a', 'pod': 'b', 'container': 'c',
                 'restarts': True, 'finished_at': int('9' * 20)},
                'not a dict', {}]}))

        self.assertIn('OOM killed: unknown/unknown unknown at unknown, '
                      'unknown restarts\n', rendered)
        self.assertIn('OOM killed: a/b c at unknown, unknown restarts\n',
                      rendered)
        self.assertEqual(4, rendered.count('OOM killed:'))
        self.assertNotIn('None', rendered)
        self.assertNotIn('True', rendered)
        self.assertIn('k3s API: answered on inst-cp1', rendered)

    def test_a_named_node_unread_under_an_answering_probe_shares_its_name(self):
        report = self._kubernetes_report()
        report['nodes'][1].update({'name': 'k3s-banana-node-001', 'exists': True,
                                   'state': 'created', 'agent_state': 'ready'})

        rendered = self._render_kubernetes(report)

        self.assertIn(
            '        kubernetes: not read (another node has the same name)\n',
            rendered)
        self.assertNotIn('no instance name', rendered)

    def test_a_probe_which_did_not_answer_is_the_reason_nodes_are_unread(self):
        unread = {'registered': None, 'ready': None, 'ready_since': None,
                  'memory_pressure': None, 'disk_pressure': None,
                  'pid_pressure': None, 'oom_killed': None}
        rendered = self._render_kubernetes(self._kubernetes_report(
            first=unread,
            probe={'probed': True, 'answered': False,
                   'error': 'the command exited 1', 'unmatched_nodes': None}))

        self.assertEqual(2, rendered.count(
            'kubernetes: not read (the command exited 1)\n'))
        self.assertIn('  Kubernetes probe: did not answer (the command exited 1)\n',
                      rendered)
        self.assertNotIn('unmatched', rendered)
        self.assertNotIn('None', rendered)

    def test_a_probe_error_which_is_missing_renders_unknown(self):
        rendered = self._render_kubernetes(self._kubernetes_report(
            probe={'answered': False, 'error': None}, first={'registered': None}))

        self.assertIn('kubernetes: not read (unknown)\n', rendered)
        self.assertIn('  Kubernetes probe: did not answer (unknown)\n', rendered)

    def test_unmatched_nodes_are_listed_after_the_api_lines(self):
        rendered = self._render_kubernetes(self._kubernetes_report(
            probe={'unmatched_nodes': ['old-a', 'old-b']}))

        self.assertIn('k3s-banana-node-001   Ready\n'
                      '  Kubernetes: unmatched nodes old-a, old-b\n', rendered)

    def test_no_unmatched_nodes_says_nothing(self):
        rendered = self._render_kubernetes(self._kubernetes_report())

        self.assertNotIn('unmatched', rendered)
        self.assertNotIn('Kubernetes probe', rendered)

    def test_a_report_with_no_kubernetes_at_all_is_skipped(self):
        # From an older library, or built by hand.
        rendered = self._render_kubernetes(copy.deepcopy(self.REPORT))

        self.assertNotIn('kubernetes:', rendered)
        self.assertNotIn('Kubernetes', rendered)

    def test_the_report_goes_to_the_reporter_and_not_to_stdout(self):
        reporter = progress.CollectingReporter()
        stdout = io.StringIO()

        patcher = mock.patch('sys.stdout', stdout)
        patcher.start()
        self.addCleanup(patcher.stop)

        shakenfist_client_k3s._render_health(reporter, self.REPORT)

        self.assertIn('Cluster banana in namespace testns is NOT healthy',
                      reporter.getvalue())
        self.assertIn('[ok] k3s-banana-node-001 (inst-cp1, control plane)',
                      reporter.getvalue())
        self.assertIn('[!!] inst-w1 (worker): this instance no longer exists',
                      reporter.getvalue())
        self.assertIn('k3s API: answered on inst-cp1', reporter.getvalue())
        self.assertIn('k3s-banana-node-001   Ready', reporter.getvalue())

        self.assertEqual(
            '', stdout.getvalue(),
            'the health rendering wrote to the process stdout rather than to '
            'the reporter it was handed. It wrote:\n%s' % stdout.getvalue())
