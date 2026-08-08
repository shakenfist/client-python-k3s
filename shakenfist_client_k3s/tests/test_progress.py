import io
import os
import re
import tempfile

from click.testing import CliRunner
# The PyPI mock backport is used for consistency with the other tests in
# this package, which support Python >= 3.7.
import mock
import testtools

import shakenfist_client_k3s
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress


class FakeClock:
    """A controllable stand-in for time.time()."""

    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FakeTty(io.StringIO):
    def isatty(self):
        return True


class FormatElapsedTestCase(testtools.TestCase):
    def test_formats(self):
        self.assertEqual('0s', progress.format_elapsed(0))
        self.assertEqual('45s', progress.format_elapsed(45.9))
        self.assertEqual('1m00s', progress.format_elapsed(60))
        self.assertEqual('2m05s', progress.format_elapsed(125))
        self.assertEqual('1h03m', progress.format_elapsed(3600 + 3 * 60 + 20))


class CountStrTestCase(testtools.TestCase):
    def test_pluralisation(self):
        self.assertEqual('1 instance', progress.count_str(1, 'instance'))
        self.assertEqual('2 instances', progress.count_str(2, 'instance'))
        self.assertEqual('0 operations', progress.count_str(0, 'operation'))
        self.assertEqual('3 routed addresses', progress.count_str(3, 'routed address'))


class ProgressLineModeTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        patcher = mock.patch('shakenfist_client_k3s.progress.time.time', self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_phase_numbering_with_total(self):
        stream = io.StringIO()
        p = progress.Progress(total_phases=3, stream=stream)
        p.phase('Doing the first thing')
        p.phase('Doing the second thing')
        self.assertEqual(
            '[1/3] Doing the first thing\n[2/3] Doing the second thing\n',
            stream.getvalue())

    def test_phase_numbering_without_total(self):
        stream = io.StringIO()
        p = progress.Progress(stream=stream)
        p.phase('Doing a thing')
        self.assertEqual('[1] Doing a thing\n', stream.getvalue())

    def test_update_prints_only_on_change(self):
        stream = io.StringIO()
        p = progress.Progress(stream=stream)
        p.phase('Booting')

        for _ in range(5):
            p.update('node-001', 'state initial')
            self.clock.advance(5)
        p.update('node-001', 'state created')

        # The elapsed time is per status, so it resets when the status
        # changes.
        self.assertEqual(
            '[1] Booting\n'
            '  node-001: state initial (0s)\n'
            '  node-001: state created (0s)\n',
            stream.getvalue())

    def test_unchanged_status_heartbeats(self):
        stream = io.StringIO()
        p = progress.Progress(stream=stream)
        p.phase('Booting')

        for _ in range(30):
            p.update('node-001', 'state initial')
            self.clock.advance(5)

        # 150 seconds of no change should yield the initial line plus two
        # heartbeats, not thirty lines.
        self.assertEqual(
            '[1] Booting\n'
            '  node-001: state initial (0s)\n'
            '  node-001: state initial (1m00s)\n'
            '  node-001: state initial (2m00s)\n',
            stream.getvalue())

    def test_multiple_items_tracked_independently(self):
        stream = io.StringIO()
        p = progress.Progress(stream=stream)
        p.phase('Booting')

        p.update('node-002', 'state initial')
        p.update('node-003', 'state initial')
        self.clock.advance(5)
        p.update('node-002', 'state created')
        p.update('node-003', 'state initial')

        self.assertEqual(
            '[1] Booting\n'
            '  node-002: state initial (0s)\n'
            '  node-003: state initial (0s)\n'
            '  node-002: state created (0s)\n',
            stream.getvalue())

    def test_wait_done_resets_change_detection(self):
        stream = io.StringIO()
        p = progress.Progress(stream=stream)
        p.phase('Booting')
        p.update('node-001', 'idle')
        p.wait_done()
        p.update('node-001', 'idle')

        # The same status is reprinted in a new wait block.
        self.assertEqual(2, stream.getvalue().count('node-001: idle'))

    def test_note_mid_wait_preserves_elapsed(self):
        stream = io.StringIO()
        p = progress.Progress(stream=stream)
        p.phase('Waiting')
        p.update('node-001', 'running')
        self.clock.advance(30)
        p.note('node-001 may be stalled')
        self.clock.advance(35)
        p.update('node-001', 'running')

        # The note must not reset the per-status timer: the second update
        # is a heartbeat of the same status, 65 seconds after the status
        # first appeared. A reset here would restart the elapsed counter at
        # exactly the moment the stall note has flagged it as interesting.
        self.assertEqual(
            '[1] Waiting\n'
            '  node-001: running (0s)\n'
            '  node-001 may be stalled\n'
            '  node-001: running (1m05s)\n',
            stream.getvalue())

    def test_finish_reports_total_elapsed(self):
        stream = io.StringIO()
        p = progress.Progress(stream=stream)
        self.clock.advance(125)
        p.finish('Cluster banana is ready')
        self.assertEqual('Cluster banana is ready (2m05s total)\n', stream.getvalue())

    def test_verbose_forces_line_mode(self):
        p = progress.Progress(stream=FakeTty(), verbose=True)
        self.assertFalse(p.interactive)


class ProgressInteractiveModeTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        for target, replacement in [
                ('shakenfist_client_k3s.progress.time.time', self.clock),
                ('shakenfist_client_k3s.progress.shutil.get_terminal_size',
                 lambda: os.terminal_size((80, 24)))]:
            patcher = mock.patch(target, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_updates_rewrite_in_place(self):
        stream = FakeTty()
        p = progress.Progress(stream=stream)
        p.phase('Booting')

        p.update('node-001', 'state initial')
        self.clock.advance(5)
        p.update('node-001', 'state created')

        # The first update just draws a line; the second moves the cursor
        # back up one line and redraws it. The elapsed time is per status,
        # so it resets when the status changes.
        self.assertEqual(
            '[1] Booting\n'
            '\x1b[K  node-001: state initial (0s)\n'
            '\x1b[1F\x1b[K  node-001: state created (0s)\n',
            stream.getvalue())

    def test_unchanged_status_age_grows(self):
        stream = FakeTty()
        p = progress.Progress(stream=stream)
        p.phase('Booting')

        p.update('node-001', 'state initial')
        self.clock.advance(5)
        p.update('node-001', 'state initial')

        self.assertEqual(
            '[1] Booting\n'
            '\x1b[K  node-001: state initial (0s)\n'
            '\x1b[1F\x1b[K  node-001: state initial (5s)\n',
            stream.getvalue())

    def test_note_mid_wait_redraws_block_without_duplicates(self):
        stream = FakeTty()
        p = progress.Progress(stream=stream)
        p.phase('Waiting')
        p.update('node-001', 'running')
        self.clock.advance(5)
        p.note('node-001 may be stalled')
        p.update('node-001', 'running')

        # The note is printed where the status block started and the block
        # is redrawn immediately below it with its elapsed times intact, so
        # no stale copy of the block is left on screen and the next update
        # rewrites the redrawn block in place.
        self.assertEqual(
            '[1] Waiting\n'
            '\x1b[K  node-001: running (0s)\n'
            '\x1b[1F\x1b[K  node-001 may be stalled\n'
            '\x1b[K  node-001: running (5s)\n'
            '\x1b[1F\x1b[K  node-001: running (5s)\n',
            stream.getvalue())

    def test_lines_truncated_to_terminal_width(self):
        stream = FakeTty()
        p = progress.Progress(stream=stream)
        p.update('node-001', 'x' * 200)

        drawn = stream.getvalue().split('\x1b[K')[-1].rstrip('\n')
        self.assertEqual(79, len(drawn))


class DescribeAgentOpTestCase(testtools.TestCase):
    def test_execute_sequence(self):
        aop = {
            'commands': [
                {'command': 'execute', 'commandline': 'apt-get update'},
                {'command': 'execute', 'commandline': 'apt-get dist-upgrade -y'}
            ],
            'results': {}
        }
        self.assertEqual('apt-get update', primitives._describe_agent_op(aop))

        aop['results'] = {'0': {'return-code': 0}}
        self.assertEqual('apt-get dist-upgrade -y', primitives._describe_agent_op(aop))

    def test_non_execute_command_uses_path(self):
        aop = {
            'commands': [{'command': 'get-file', 'path': '/etc/rancher/k3s/k3s.yaml'}],
            'results': {}
        }
        self.assertEqual('get-file /etc/rancher/k3s/k3s.yaml',
                         primitives._describe_agent_op(aop))

    def test_long_commands_truncated(self):
        aop = {
            'commands': [{'command': 'execute', 'commandline': 'x' * 100}],
            'results': {}
        }
        desc = primitives._describe_agent_op(aop)
        self.assertEqual(60, len(desc))
        self.assertTrue(desc.endswith('...'))

    def test_multiline_command_described_by_first_line(self):
        aop = {
            'commands': [{
                'command': 'execute',
                'commandline': 'cat - > /etc/sf/thing.yaml << EOF\nline: two\nEOF\n'
            }],
            'results': {}
        }
        self.assertEqual('cat - > /etc/sf/thing.yaml << EOF ...',
                         primitives._describe_agent_op(aop))

    def test_no_commands(self):
        self.assertIsNone(primitives._describe_agent_op({'commands': [], 'results': {}}))


class FakeContext:
    def __init__(self, obj):
        self.obj = obj


class InstallK3sComponentTestCase(testtools.TestCase):
    def _install_commands(self, md):
        ctx = FakeContext({
            'name': 'banana',
            'namespace': 'testns',
            'CLIENT': mock.MagicMock(),
            'VERBOSE': False,
            primitives.METADATA_KEY % 'banana': md
        })
        with mock.patch('shakenfist_client_k3s.primitives.execute_and_await') as ea:
            primitives.install_k3s_component(ctx, ['uuid-001'], 'token', 'agent')
            return '\n'.join(ea.call_args[0][2])

    def test_join_uses_join_address(self):
        cmds = self._install_commands({
            'k3s_version': 'stable',
            'join_address': '10.0.0.5',
            'api_address_inner': '10.0.0.4'
        })
        self.assertIn('K3S_URL=https://10.0.0.5:6443', cmds)

    def test_join_falls_back_to_api_address_inner(self):
        # Clusters created before join_address existed only carry the
        # older api_address_inner key in their metadata.
        cmds = self._install_commands({
            'k3s_version': 'stable',
            'api_address_inner': '10.0.0.4'
        })
        self.assertIn('K3S_URL=https://10.0.0.4:6443', cmds)


class WaitLoopTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        for target, replacement in [
                ('shakenfist_client_k3s.progress.time.time', self.clock),
                ('shakenfist_client_k3s.primitives.time.sleep',
                 lambda seconds: self.clock.advance(seconds))]:
            patcher = mock.patch(target, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _make_context(self, client, stream):
        return FakeContext({
            'namespace': 'testns',
            'CLIENT': client,
            'VERBOSE': False,
            'PROGRESS': progress.Progress(stream=stream)
        })

    def test_await_boot_does_not_run_os_update(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {
            'name': 'node-001', 'state': 'created', 'agent_state': 'ready'}
        ctx = self._make_context(client, io.StringIO())

        primitives.await_boot(ctx, ['uuid-001'])

        # The OS update was previously triggered implicitly from within
        # await_boot(), which made the subsequent idle wait impossible to
        # label. It is now the caller's responsibility.
        client.instance_execute.assert_not_called()

    def test_await_idle_describes_running_command(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'node-001'}
        client.get_instance_agentoperations.side_effect = [
            [],
            [{
                'uuid': 'aop-001',
                'state': 'executing',
                'commands': [{'command': 'execute', 'commandline': 'apt-get update'}],
                'results': {}
            }],
            []
        ]
        stream = io.StringIO()
        ctx = self._make_context(client, stream)

        primitives.await_idle(ctx, ['uuid-001'])

        self.assertEqual(
            "  node-001: running 'apt-get update' (1 operation remaining) (0s)\n"
            '  node-001: idle (0s)\n',
            stream.getvalue())

    def test_await_idle_aborts_on_errored_operation(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'node-001'}
        client.get_instance_agentoperations.side_effect = [
            [],
            [{
                'uuid': 'aop-002',
                'instance_uuid': 'uuid-001',
                'state': 'error',
                'commands': [{'command': 'execute', 'commandline': 'helm install banana'}],
                'results': {}
            }]
        ]
        ctx = self._make_context(client, io.StringIO())

        captured = io.StringIO()
        with mock.patch('sys.stdout', captured):
            e = self.assertRaises(SystemExit, primitives.await_idle, ctx, ['uuid-001'])

        self.assertEqual(1, e.code)
        self.assertIn('operation: aop-002', captured.getvalue())
        self.assertIn('helm install banana', captured.getvalue())
        self.assertIn('failed to start', captured.getvalue())

    def test_await_idle_ignores_preexisting_errors(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'node-001'}
        old_error = {
            'uuid': 'aop-old',
            'instance_uuid': 'uuid-001',
            'state': 'error',
            'commands': [{'command': 'execute', 'commandline': 'helm install banana'}],
            'results': {}
        }
        client.get_instance_agentoperations.side_effect = [[old_error], [old_error]]
        stream = io.StringIO()
        ctx = self._make_context(client, stream)

        # An operation which had already failed before the wait started must
        # neither wedge the wait nor abort it.
        primitives.await_idle(ctx, ['uuid-001'])
        self.assertIn('node-001: idle', stream.getvalue())

    def test_await_idle_notes_stalled_command(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'node-001'}
        running = {
            'uuid': 'aop-001',
            'instance_uuid': 'uuid-001',
            'state': 'executing',
            'commands': [{'command': 'execute', 'commandline': 'helm install banana'}],
            'results': {}
        }

        # The loop polls every five seconds, so this is enough polls to pass
        # the stall warning threshold with some slack to show the warning is
        # only emitted once.
        polls = primitives.STALL_WARNING_SECONDS // 5 + 10
        client.get_instance_agentoperations.side_effect = [[]] + [[running]] * polls + [[]]
        stream = io.StringIO()
        ctx = self._make_context(client, stream)

        primitives.await_idle(ctx, ['uuid-001'])

        self.assertIn('may be stalled', stream.getvalue())
        self.assertIn('aop-001', stream.getvalue())
        self.assertEqual(1, stream.getvalue().count('may be stalled'))

    def test_await_fetch_reports_errors_without_results(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'node-001'}
        aop = {
            'uuid': 'aop-003',
            'instance_uuid': 'uuid-001',
            'state': 'error',
            'commands': [{'command': 'get-file', 'path': '/missing'}],
            'results': {}
        }
        ctx = self._make_context(client, io.StringIO())

        captured = io.StringIO()
        with mock.patch('sys.stdout', captured):
            e = self.assertRaises(SystemExit, primitives.await_fetch, ctx, aop)

        self.assertEqual(1, e.code)
        self.assertIn('get-file /missing', captured.getvalue())

    def test_reap_execute_aborts_on_errored_operation(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'node-001'}
        aop = {
            'uuid': 'aop-004',
            'instance_uuid': 'uuid-001',
            'state': 'error',
            'commands': [{'command': 'execute', 'commandline': 'apt-get update'}],
            'results': {}
        }
        ctx = self._make_context(client, io.StringIO())

        captured = io.StringIO()
        with mock.patch('sys.stdout', captured):
            e = self.assertRaises(SystemExit, primitives.reap_execute, ctx, aop)

        self.assertEqual(1, e.code)
        self.assertIn('operation: aop-004', captured.getvalue())

    def test_reap_execute_formats_multiline_stderr(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'node-001'}
        aop = {
            'uuid': 'aop-005',
            'instance_uuid': 'uuid-001',
            'state': 'complete',
            'commands': [{'command': 'execute', 'commandline': 'kubectl wait pods'}],
            'results': {'0': {'return-code': 1, 'stdout': '',
                              'stderr': 'timed out on pod one\ntimed out on pod two'}}
        }
        ctx = self._make_context(client, io.StringIO())

        captured = io.StringIO()
        with mock.patch('sys.stdout', captured):
            e = self.assertRaises(SystemExit, primitives.reap_execute, ctx, aop)

        self.assertEqual(1, e.code)
        self.assertIn('   stderr: timed out on pod one\n'
                      '   stderr: timed out on pod two', captured.getvalue())


class AllocateMetallbAddressesTestCase(testtools.TestCase):
    def _allocate(self, route_results, count):
        stream = io.StringIO()
        client = mock.MagicMock()
        client.get_network.return_value = {'uuid': 'net-1'}
        client.route_network_address.side_effect = route_results
        md = {'name': 'banana', 'node_network': 'net-1',
              'routed_addresses': ['192.168.10.1']}
        ctx = FakeContext({
            'name': 'banana',
            'namespace': 'testns',
            'CLIENT': client,
            'VERBOSE': False,
            'PROGRESS': progress.Progress(stream=stream),
            primitives.METADATA_KEY % 'banana': md
        })
        primitives.allocate_metallb_addresses(ctx, count)
        return stream.getvalue()

    def test_allocation_reports_new_addresses_and_cluster_total(self):
        out = self._allocate(['192.168.10.2', '192.168.10.3'], 2)
        self.assertIn('allocated 2 routed addresses: 192.168.10.2, 192.168.10.3', out)
        self.assertIn('the cluster now has 3', out)

    def test_partial_allocation_notes_shortfall(self):
        out = self._allocate(['192.168.10.2', None, None], 3)
        self.assertIn('allocated 1 routed address: 192.168.10.2', out)
        self.assertIn('(requested 3)', out)
        self.assertIn('the cluster now has 2', out)

    def test_empty_allocation_reported_without_dangling_list(self):
        out = self._allocate([None, None], 2)
        self.assertIn('no routed addresses were available (requested 2)', out)
        self.assertNotIn('allocated', out)


# A minimal kubeconfig in the shape k3s writes, pointing at the loopback
# address the way the real file does before k3s_create rewrites it.
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


class FakeCreateClient:
    """Enough of the sf-client API surface for k3s create to run end to end.

    Instances boot instantly, every agent operation completes successfully
    at submission, and file fetches return canned content.
    """

    def __init__(self):
        self.namespace = 'testns'
        self.metadata = {}
        self.instances = {}
        self.instance_serial = 0
        self.aop_serial = 0
        self.routed_serial = 0

    def get_namespace_metadata(self, namespace):
        return dict(self.metadata)

    def set_namespace_metadata_item(self, namespace, key, value):
        self.metadata[key] = value

    def delete_namespace_metadata_item(self, namespace, key):
        self.metadata.pop(key, None)

    def allocate_network(self, netblock, provide_dhcp, provide_nat, name,
                         namespace=None):
        return {'uuid': 'net-1', 'name': name, 'state': 'created'}

    def get_network(self, network_ref):
        return {'uuid': 'net-1', 'name': 'k3s-banana-node', 'state': 'created'}

    def create_instance(self, name, cpus, memory, networks, disks, sshkey,
                        userdata, side_channels=None, namespace=None):
        self.instance_serial += 1
        instance_uuid = 'inst-%03d' % self.instance_serial
        self.instances[instance_uuid] = {
            'uuid': instance_uuid, 'name': name, 'state': 'created',
            'agent_state': 'ready'}
        return self.instances[instance_uuid]

    def get_instance(self, instance_ref):
        return self.instances[instance_ref]

    def get_instance_interfaces(self, instance_ref):
        return [{'ipv4': '10.0.0.4', 'floating': '192.168.10.100'}]

    def get_instance_agentoperations(self, instance_ref, all=False):
        return []

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


class K3sCreateSmokeTestCase(testtools.TestCase):
    """Drive k3s create end to end against a fake client.

    This is command level wiring coverage: it catches crashes in the
    create flow itself (for example the Progress reporter being shadowed
    by a subprocess result), and it pins the phase total computed in
    k3s_create() to the number of phase headers the primitives actually
    emit, which nothing else keeps in sync.
    """

    def setUp(self):
        super().setUp()
        self.client = FakeCreateClient()

        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        self.home = home.name

        for target, kwargs in [
                ('shakenfist_client_k3s.apiclient.Client',
                 {'return_value': self.client}),
                ('shakenfist_client_k3s.primitives.get_k3s_release',
                 {'return_value': 'stable'}),
                ('shakenfist_client_k3s.primitives.get_longhorn_release',
                 {'return_value': '1.6.0'}),
                ('time.sleep', {'new': lambda seconds: None})]:
            patcher = mock.patch(target, **kwargs)
            patcher.start()
            self.addCleanup(patcher.stop)

        patcher = mock.patch.dict('os.environ', {'HOME': self.home})
        patcher.start()
        self.addCleanup(patcher.stop)

    def _create(self, args):
        runner = CliRunner()
        result = runner.invoke(
            shakenfist_client_k3s.k3s, ['create'] + args, obj={'VERBOSE': False})
        self.assertEqual(
            0, result.exit_code, '%s\n%s' % (result.output, result.exception))
        return result.output

    def _assert_phases_consistent(self, output):
        # Every phase header must carry the same total, the phases must be
        # numbered consecutively from one, and the total must equal the
        # number of headers emitted.
        headers = re.findall(r'^\[(\d+)/(\d+)\]', output, re.MULTILINE)
        self.assertNotEqual([], headers)
        totals = {int(total) for _, total in headers}
        self.assertEqual(1, len(totals), output)
        total = totals.pop()
        self.assertEqual(
            list(range(1, total + 1)), [int(index) for index, _ in headers],
            output)

    def test_create(self):
        output = self._create(['banana'])
        self._assert_phases_consistent(output)
        self.assertIn('Cluster banana is ready', output)

        # With no pre-existing local configuration the kubeconfig is
        # written directly, pointing at the cluster's floating address.
        with open(os.path.join(self.home, '.kube', 'config')) as f:
            kubeconfig = f.read()
        self.assertIn('192.168.10.100', kubeconfig)
        self.assertNotIn('127.0.0.1', kubeconfig)

    def test_create_with_extra_control_planes(self):
        output = self._create(['banana', '--control-plane-count', '2'])
        self._assert_phases_consistent(output)
        self.assertIn('additional control plane nodes', output)

    def test_create_with_existing_network(self):
        output = self._create(['banana', '--network', 'net-1'])
        self._assert_phases_consistent(output)
        self.assertNotIn('Creating node network', output)
