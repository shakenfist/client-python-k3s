import copy
import io
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
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, ['delete', 'banana'],
            obj={'VERBOSE': False, 'CLIENT': self.client})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            [['kubectl', 'config', 'unset', 'users.banana.testns'],
             ['kubectl', 'config', 'unset', 'contexts.banana.testns'],
             ['kubectl', 'config', 'unset', 'clusters.banana.testns']],
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
             'state': 'created', 'agent_state': 'ready', 'healthy': True},
            {'uuid': 'inst-w1', 'role': 'worker', 'name': None,
             'exists': False, 'state': None, 'agent_state': None,
             'healthy': False}
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
