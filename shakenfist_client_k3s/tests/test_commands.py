import copy
import tempfile
import time

from click.testing import CliRunner

# The PyPI mock backport is used rather than unittest.mock for consistency
# with test_primitives.py, as the project supports Python >= 3.7.
import mock
import testtools

import shakenfist_client_k3s
from shakenfist_client_k3s import cluster as cluster_module
from shakenfist_client_k3s import primitives
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
        patcher = mock.patch(
            'shakenfist_client_k3s.apiclient.Client', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.runner = CliRunner()

    def _invoke(self, args):
        return self.runner.invoke(
            shakenfist_client_k3s.k3s, args, obj={'VERBOSE': False})

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
        patcher = mock.patch(
            'shakenfist_client_k3s.apiclient.Client', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.runner = CliRunner()

    def test_namespace_created_notice_text_is_unchanged(self):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s,
            ['create', 'banana', '--namespace', 'newns'],
            obj={'VERBOSE': False})

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
        patcher = mock.patch(
            'shakenfist_client_k3s.apiclient.Client', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.runner = CliRunner()

    def test_list_prints_one_cluster_per_line(self):
        self.client.get_namespace_metadata.return_value = {
            primitives.CLUSTER_LIST: ['banana', 'apple']}

        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['list'], obj={'VERBOSE': False})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual('banana\napple\n', result.output)

    def test_list_prints_nothing_when_the_namespace_has_no_clusters(self):
        self.client.get_namespace_metadata.return_value = {}

        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['list'], obj={'VERBOSE': False})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual('', result.output)


class RecordingClient(fakes.FakeClusterClient):
    """A scripted client which also records the commands it was asked to run."""

    def __init__(self):
        super(RecordingClient, self).__init__()
        self.executed = []

    def instance_execute(self, instance_ref, commandline):
        self.executed.append((instance_ref, commandline))
        return super(RecordingClient, self).instance_execute(
            instance_ref, commandline)


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
    """The three one-line command bodies forward the right argument.

    expand-workers, expand-addresses and update-os stopped being command
    bodies in this phase and became argument parsing plus a single Cluster
    call. Nothing else pins that wiring: the golden --help fixtures assert
    the interface, not the forwarding, and would still pass if
    expand-addresses called expand_addresses(worker_count) or if a count
    were dropped on the floor. So each of these drives the real method
    against a scripted client and counts what it did.
    """

    def setUp(self):
        super(CommandWiringTestCase, self).setUp()
        self.client = RecordingClient()
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

        patcher = mock.patch(
            'shakenfist_client_k3s.apiclient.Client', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)

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
            obj={'VERBOSE': False})

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
            obj={'VERBOSE': False})

        self.assertEqual(0, result.exit_code, result.output)
        # Three addresses routed, on top of the one the cluster had, and
        # no instances created: this is the command which would look
        # identical if it were handed the wrong count.
        self.assertEqual(3, self.client.routed_serial)
        self.assertEqual(4, len(self._md()['routed_addresses']))
        self.assertEqual(0, self.client.instance_serial)
        self.assertIn('Added 3 metallb addresses to cluster banana',
                      result.output)

    def test_update_os_updates_every_node(self):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['update-os', 'banana'],
            obj={'VERBOSE': False})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            [('inst-cp1', 'apt-get update'),
             ('inst-w1', 'apt-get update'),
             ('inst-cp1', 'apt-get dist-upgrade -y'),
             ('inst-w1', 'apt-get dist-upgrade -y')],
            self.client.executed)
        self.assertIn('Updated the OS on all nodes in cluster banana',
                      result.output)
