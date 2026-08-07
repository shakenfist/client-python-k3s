import io
import os

# The PyPI mock backport is used for consistency with the other tests in
# this package, which support Python >= 3.7.
import mock
import testtools

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

        self.assertEqual(
            '[1] Booting\n'
            '  node-001: state initial (0s)\n'
            '  node-001: state created (25s)\n',
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
            '  node-002: state created (5s)\n',
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
        # back up one line and redraws it.
        self.assertEqual(
            '[1] Booting\n'
            '\x1b[K  node-001: state initial (0s)\n'
            '\x1b[1F\x1b[K  node-001: state created (5s)\n',
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

    def test_no_commands(self):
        self.assertIsNone(primitives._describe_agent_op({'commands': [], 'results': {}}))


class FakeContext:
    def __init__(self, obj):
        self.obj = obj


class WaitLoopTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()
        patcher = mock.patch('shakenfist_client_k3s.primitives.time.sleep')
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
            [{
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
