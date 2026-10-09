"""The judgements tools/ci_health_signals.py makes about health() reports.

The tool provokes each health signal on a real cluster in the merge tier
and asserts that Cluster.health() reports it. What it provokes, and what
systemd, the kernel and the kubelet then do, can only be checked by
running it against a cluster: step 3b of
docs/plans/PLAN-cumulative-health-signals-phase-03-live-validation.md
does that, and nothing here guesses at it. What can be checked here is
that each check_*() function fails when the reading it is about is wrong,
and says which reading -- because a check which cannot fail turns the
merge tier's provocations into a slow way of printing numbers.

The reports are built by hand in the shape health()'s docstring and
docs/library-api.md give, rather than produced by health() against the
fakes in test_cluster.py, because the tool is a consumer of that shape and
these tests are about the consumer.

Driven by importing the script rather than running it, as
test_build_collection.py does, because the judgements are pure and
everything which touches a cluster is in different functions.
"""
import copy
import importlib.util
import os
import shlex
import subprocess

import testtools


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
_SCRIPT = os.path.join(_REPO_ROOT, 'tools', 'ci_health_signals.py')

CONTROL_PLANE_BOOT_ID = '0f8c4b2e-3a5d-4c7e-9b1a-2d3e4f5a6b7c'
WORKER_BOOT_ID = '7a6b5c4d-3e2f-4a1b-8c9d-0e1f2a3b4c5d'
POD = 'ci-oom-1a2b3c4d'


def _load():
    """Import the tool by path.

    It is a script rather than a module in the package, so there is no
    import path to it; and it is not installed, so an installed copy of
    this package cannot run these tests. Returning None lets them skip the
    way test_build_collection.py skips.
    """
    if not os.path.exists(_SCRIPT):
        return None
    spec = importlib.util.spec_from_file_location('ci_health_signals', _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _node(role, name, uuid, boot_id, unit, etcd_bytes, etcd_snapshot_bytes):
    return {
        'uuid': uuid,
        'role': role,
        'name': name,
        'exists': True,
        'state': 'created',
        'agent_state': 'ready',
        'healthy': True,
        'signals': {
            'probed': True,
            'error': None,
            'boot_id': boot_id,
            'booted_at': 1791270000,
            'k3s_unit': unit,
            'k3s_state': 'active',
            'k3s_restarts': 0,
            'oom_kills': 0,
            'memory_total_bytes': 2061496320,
            'memory_available_bytes': 847249408,
            'etcd_bytes': etcd_bytes,
            'etcd_snapshot_bytes': etcd_snapshot_bytes,
        },
        'kubernetes': {
            'registered': True,
            'ready': 'True',
            'ready_since': 1791270100,
            'memory_pressure': 'False',
            'disk_pressure': 'False',
            'pid_pressure': 'False',
            'oom_killed': [],
        },
    }


def healthy_report():
    """A fresh minimal cluster, as decision 4a expects to find it: one control plane node and one worker."""
    return {
        'name': 'ciMinimal',
        'namespace': 'ci',
        'state': 'created',
        'interrupted': False,
        'nodes': [
            _node('control_plane', 'k3s-ciMinimal-node-001', 'cp-uuid', CONTROL_PLANE_BOOT_ID, 'k3s',
                  144703488, 4096),
            _node('worker', 'k3s-ciMinimal-node-002', 'worker-uuid', WORKER_BOOT_ID, 'k3s-agent',
                  None, None),
        ],
        'api': {
            'probed': True,
            'answered': True,
            'instance_uuid': 'cp-uuid',
            'command': 'kubectl get nodes',
            'return_code': 0,
            'stdout': 'NAME STATUS\nk3s-ciminimal-node-001 Ready\n',
            'stderr': '',
            'error': None,
        },
        'kubernetes': {
            'probed': True,
            'answered': True,
            'error': None,
            'unmatched_nodes': [],
        },
        'healthy': True,
    }


def node_of(report, role):
    return [node for node in report['nodes'] if node['role'] == role][0]


def oom_killed_report(**worker_signals):
    """A report taken after decision 4b's kill: the container is listed and the counter has moved."""
    report = healthy_report()
    worker = node_of(report, 'worker')
    worker['kubernetes']['oom_killed'] = [
        {'namespace': 'kube-system', 'pod': 'other', 'container': 'hog', 'restarts': 0,
         'finished_at': 1791270200},
        {'namespace': 'default', 'pod': POD, 'container': 'hog', 'restarts': 0, 'finished_at': 1791270300},
    ]
    worker['signals']['oom_kills'] = 1
    worker['signals'].update(worker_signals)
    return report


class _ToolTestCase(testtools.TestCase):

    def setUp(self):
        super().setUp()
        self.tool = _load()
        if self.tool is None:
            self.skipTest('tools/ci_health_signals.py is not in this tree')

    def assertFails(self, message, *fragments):
        """Assert a check returned a failure, and that it names each fragment."""
        self.assertIsNotNone(message)
        for fragment in fragments:
            self.assertIn(fragment, message)


class FindNodeTestCase(_ToolTestCase):

    def test_finds_each_role(self):
        report = healthy_report()
        self.assertEqual('cp-uuid', self.tool.find_node(report, 'control_plane')['uuid'])
        self.assertEqual('worker-uuid', self.tool.find_node(report, 'worker')['uuid'])

    def test_finds_by_role_not_by_name(self):
        """A worker named like a control plane node is still the worker."""
        report = healthy_report()
        node_of(report, 'worker')['name'] = 'control-plane'
        self.assertEqual('worker-uuid', self.tool.find_node(report, 'worker')['uuid'])

    def test_refuses_a_missing_role(self):
        report = healthy_report()
        report['nodes'] = [node_of(report, 'control_plane')]
        e = self.assertRaises(self.tool.Failure, self.tool.find_node, report, 'worker')
        self.assertIn('exactly one worker node', str(e))
        self.assertIn('found 0', str(e))

    def test_refuses_two_of_a_role(self):
        """Decision 4 means the one worker; with two, which one it means is ambiguous."""
        report = healthy_report()
        second = copy.deepcopy(node_of(report, 'worker'))
        second['name'] = 'k3s-ciMinimal-node-003'
        report['nodes'].append(second)
        e = self.assertRaises(self.tool.Failure, self.tool.find_node, report, 'worker')
        self.assertIn('found 2', str(e))
        self.assertIn('k3s-ciMinimal-node-003', str(e))


class CheckBaselineTestCase(_ToolTestCase):

    def test_a_fresh_cluster_passes(self):
        self.assertIsNone(self.tool.check_baseline(healthy_report()))

    def test_unhealthy(self):
        report = healthy_report()
        report['healthy'] = False
        self.assertFails(self.tool.check_baseline(report), 'healthy is False')

    def test_a_signals_probe_error(self):
        report = healthy_report()
        node_of(report, 'worker')['signals']['error'] = 'the command exited 1'
        self.assertFails(self.tool.check_baseline(report), 'worker signals.error', 'exited 1')

    def test_boot_id_not_a_uuid(self):
        for boot_id in (None, 'not-a-uuid', CONTROL_PLANE_BOOT_ID.upper()):
            report = healthy_report()
            node_of(report, 'control_plane')['signals']['boot_id'] = boot_id
            self.assertFails(self.tool.check_baseline(report), 'control plane signals.boot_id', 'not a UUID')

    def test_k3s_not_active(self):
        report = healthy_report()
        node_of(report, 'worker')['signals']['k3s_state'] = 'activating'
        self.assertFails(self.tool.check_baseline(report), 'worker signals.k3s_state', "'activating'")

    def test_the_wrong_unit_for_the_role(self):
        report = healthy_report()
        node_of(report, 'control_plane')['signals']['k3s_unit'] = 'k3s-agent'
        self.assertFails(self.tool.check_baseline(report), 'control plane signals.k3s_unit', "not 'k3s'")
        report = healthy_report()
        node_of(report, 'worker')['signals']['k3s_unit'] = 'k3s'
        self.assertFails(self.tool.check_baseline(report), 'worker signals.k3s_unit', "not 'k3s-agent'")

    def test_counters_must_be_non_negative_integers(self):
        for key in ('oom_kills', 'k3s_restarts'):
            for value in (None, -1, '0', True):
                report = healthy_report()
                node_of(report, 'worker')['signals'][key] = value
                self.assertFails(self.tool.check_baseline(report), 'worker signals.%s' % key)

    def test_counters_above_zero_pass(self):
        """Decision 4a asks for integers of 0 or more, not 0: a cluster may already have restarted."""
        report = healthy_report()
        node_of(report, 'worker')['signals']['k3s_restarts'] = 2
        node_of(report, 'control_plane')['signals']['oom_kills'] = 3
        self.assertIsNone(self.tool.check_baseline(report))

    def test_no_memory_available(self):
        for value in (0, None):
            report = healthy_report()
            node_of(report, 'worker')['signals']['memory_available_bytes'] = value
            self.assertFails(self.tool.check_baseline(report), 'worker signals.memory_available_bytes',
                             'not more than 0')

    def test_more_memory_available_than_there_is(self):
        report = healthy_report()
        signals = node_of(report, 'control_plane')['signals']
        signals['memory_available_bytes'] = signals['memory_total_bytes'] + 1
        self.assertFails(self.tool.check_baseline(report), 'control plane signals.memory_available_bytes',
                         'more than memory_total_bytes')

    def test_all_memory_available_passes(self):
        report = healthy_report()
        signals = node_of(report, 'worker')['signals']
        signals['memory_available_bytes'] = signals['memory_total_bytes']
        self.assertIsNone(self.tool.check_baseline(report))

    def test_no_memory_total(self):
        for value in (0, None):
            report = healthy_report()
            node_of(report, 'worker')['signals']['memory_total_bytes'] = value
            self.assertFails(self.tool.check_baseline(report), 'worker signals.memory_total_bytes',
                             'not more than 0')

    def test_no_etcd_on_the_control_plane(self):
        for value in (0, None):
            report = healthy_report()
            node_of(report, 'control_plane')['signals']['etcd_bytes'] = value
            self.assertFails(self.tool.check_baseline(report), 'control plane signals.etcd_bytes')

    def test_etcd_on_the_worker(self):
        report = healthy_report()
        node_of(report, 'worker')['signals']['etcd_bytes'] = 4096
        self.assertFails(self.tool.check_baseline(report), 'worker signals.etcd_bytes', 'not None')

    def test_not_ready(self):
        for value in ('False', 'Unknown', None):
            report = healthy_report()
            node_of(report, 'worker')['kubernetes']['ready'] = value
            self.assertFails(self.tool.check_baseline(report), 'worker kubernetes.ready')

    def test_disk_pressure(self):
        for value in ('True', None):
            report = healthy_report()
            node_of(report, 'control_plane')['kubernetes']['disk_pressure'] = value
            self.assertFails(self.tool.check_baseline(report), 'control plane kubernetes.disk_pressure')

    def test_every_problem_is_named(self):
        """One message for the whole report, so one CI run says everything that was wrong."""
        report = healthy_report()
        node_of(report, 'worker')['signals']['k3s_state'] = 'failed'
        node_of(report, 'control_plane')['kubernetes']['ready'] = 'Unknown'
        self.assertFails(self.tool.check_baseline(report), 'worker signals.k3s_state',
                         'control plane kubernetes.ready')


class OomPodTestCase(_ToolTestCase):

    def pod(self, phase, state, reason=None):
        status = {'phase': phase, 'containerStatuses': [
            {'name': 'pause-not-this-one', 'state': {'terminated': {'reason': 'OOMKilled'}}},
            {'name': 'hog', 'state': state},
        ]}
        if reason:
            status['reason'] = reason
        return {'status': status}

    def test_the_manifest_pins_the_pod_to_the_node(self):
        manifest = self.tool.oom_pod_manifest(POD, 'k3s-ciminimal-node-002')
        self.assertEqual(POD, manifest['metadata']['name'])
        self.assertEqual('default', manifest['metadata']['namespace'])
        self.assertEqual('k3s-ciminimal-node-002', manifest['spec']['nodeName'])
        self.assertEqual('Never', manifest['spec']['restartPolicy'])
        [container] = manifest['spec']['containers']
        self.assertEqual('hog', container['name'])
        self.assertEqual('32Mi', container['resources']['limits']['memory'])
        self.assertEqual(['tail', '/dev/zero'], container['command'])
        self.assertTrue(container['image'].startswith('registry.k8s.io/'))

    def test_oom_killed_passes(self):
        pod = self.pod('Failed', {'terminated': {'reason': 'OOMKilled', 'exitCode': 137}})
        self.assertIsNone(self.tool.check_pod_oom_killed(pod))
        self.assertIsNone(self.tool.pod_finished_otherwise(pod))

    def test_still_running_waits(self):
        pod = self.pod('Running', {'running': {'startedAt': '2026-10-08T00:00:00Z'}})
        self.assertFails(self.tool.check_pod_oom_killed(pod), "'Running'", 'running')
        self.assertIsNone(self.tool.pod_finished_otherwise(pod))

    def test_not_yet_started_waits(self):
        """Before the kubelet has reported, there is no container status at all."""
        self.assertFails(self.tool.check_pod_oom_killed({'status': {'phase': 'Pending'}}), "'Pending'")
        self.assertIsNone(self.tool.pod_finished_otherwise({'status': {'phase': 'Pending'}}))
        self.assertIsNone(self.tool.pod_finished_otherwise({}))

    def test_an_image_pull_failure_waits(self):
        """It may yet pull; the poll's bound is what gives up on it, printing the reason."""
        pod = self.pod('Pending', {'waiting': {'reason': 'ImagePullBackOff'}})
        self.assertFails(self.tool.check_pod_oom_killed(pod), 'ImagePullBackOff')
        self.assertIsNone(self.tool.pod_finished_otherwise(pod))

    def test_terminated_for_another_reason_ends_the_wait(self):
        pod = self.pod('Failed', {'terminated': {'reason': 'Error', 'exitCode': 1}})
        self.assertIsNotNone(self.tool.check_pod_oom_killed(pod))
        self.assertFails(self.tool.pod_finished_otherwise(pod), "'Error'", 'not OOMKilled')

    def test_evicted_ends_the_wait(self):
        pod = self.pod('Failed', {'waiting': {'reason': 'ContainerCreating'}}, reason='Evicted')
        self.assertFails(self.tool.pod_finished_otherwise(pod), 'Failed', "'Evicted'")


class CheckOomKilledListedTestCase(_ToolTestCase):

    def test_listed_passes(self):
        self.assertIsNone(self.tool.check_oom_killed_listed(oom_killed_report(), POD))

    def test_not_listed(self):
        self.assertFails(self.tool.check_oom_killed_listed(healthy_report(), POD),
                         'worker kubernetes.oom_killed', 'default/%s/hog' % POD)

    def test_unread_is_not_listed(self):
        report = healthy_report()
        node_of(report, 'worker')['kubernetes']['oom_killed'] = None
        self.assertFails(self.tool.check_oom_killed_listed(report, POD), 'worker kubernetes.oom_killed')

    def test_namespace_pod_and_container_must_all_match(self):
        for key, value in (('namespace', 'kube-system'), ('pod', 'ci-oom-other'), ('container', 'sidecar')):
            report = oom_killed_report()
            node_of(report, 'worker')['kubernetes']['oom_killed'][1][key] = value
            self.assertFails(self.tool.check_oom_killed_listed(report, POD), 'worker kubernetes.oom_killed')

    def test_listed_on_the_control_plane_is_not_the_worker(self):
        report = healthy_report()
        node_of(report, 'control_plane')['kubernetes']['oom_killed'] = \
            node_of(oom_killed_report(), 'worker')['kubernetes']['oom_killed']
        self.assertFails(self.tool.check_oom_killed_listed(report, POD), 'worker kubernetes.oom_killed')


class CheckOomKillTestCase(_ToolTestCase):

    def test_a_counted_listed_kill_passes(self):
        self.assertIsNone(self.tool.check_oom_kill(healthy_report(), oom_killed_report(), POD))

    def test_more_than_one_kill_passes(self):
        """The cgroup may kill more than one process; at least one more is the claim."""
        self.assertIsNone(self.tool.check_oom_kill(healthy_report(), oom_killed_report(oom_kills=3), POD))

    def test_the_kill_was_not_counted(self):
        """This is the cgroup kill claim: phase 1 read it from kernel source and never saw it."""
        self.assertFails(self.tool.check_oom_kill(healthy_report(), oom_killed_report(oom_kills=0), POD),
                         'worker signals.oom_kills', 'is 0', 'baseline 0 plus 1')

    def test_the_count_is_compared_with_the_baseline(self):
        baseline = healthy_report()
        node_of(baseline, 'worker')['signals']['oom_kills'] = 4
        self.assertFails(self.tool.check_oom_kill(baseline, oom_killed_report(oom_kills=4), POD),
                         'worker signals.oom_kills', 'baseline 4 plus 1')
        self.assertIsNone(self.tool.check_oom_kill(baseline, oom_killed_report(oom_kills=5), POD))

    def test_an_unreadable_count(self):
        self.assertFails(self.tool.check_oom_kill(healthy_report(), oom_killed_report(oom_kills=None), POD),
                         'worker signals.oom_kills', 'is None')

    def test_the_node_rebooted(self):
        report = oom_killed_report(boot_id='11111111-2222-4333-8444-555555555555')
        self.assertFails(self.tool.check_oom_kill(healthy_report(), report, POD),
                         'worker signals.boot_id', 'rebooted')

    def test_not_listed(self):
        report = oom_killed_report()
        node_of(report, 'worker')['kubernetes']['oom_killed'] = []
        self.assertFails(self.tool.check_oom_kill(healthy_report(), report, POD), 'worker kubernetes.oom_killed')

    def test_finished_at_is_not_an_integer(self):
        for value in (None, '2026-10-08T00:00:00Z'):
            report = oom_killed_report()
            node_of(report, 'worker')['kubernetes']['oom_killed'][1]['finished_at'] = value
            self.assertFails(self.tool.check_oom_kill(healthy_report(), report, POD), 'finished_at')

    def test_a_kill_must_not_make_the_cluster_unhealthy(self):
        report = oom_killed_report()
        report['healthy'] = False
        self.assertFails(self.tool.check_oom_kill(healthy_report(), report, POD), 'healthy is False')


class CheckAutomaticRestartTestCase(_ToolTestCase):

    def restarted(self, **worker_signals):
        report = healthy_report()
        node_of(report, 'worker')['signals']['k3s_restarts'] = 1
        node_of(report, 'worker')['signals'].update(worker_signals)
        return report

    def test_one_restart_passes(self):
        self.assertIsNone(self.tool.check_automatic_restart(healthy_report(), self.restarted()))

    def test_counted_from_the_report_before_the_kill(self):
        before = healthy_report()
        node_of(before, 'worker')['signals']['k3s_restarts'] = 2
        self.assertIsNone(self.tool.check_automatic_restart(before, self.restarted(k3s_restarts=3)))
        self.assertFails(self.tool.check_automatic_restart(before, self.restarted(k3s_restarts=1)),
                         'worker signals.k3s_restarts', 'the 2 before the kill plus 1')

    def test_not_yet_restarted(self):
        self.assertFails(self.tool.check_automatic_restart(healthy_report(), self.restarted(k3s_restarts=0)),
                         'worker signals.k3s_restarts', 'is 0')

    def test_restarted_twice(self):
        """Exactly one: a second restart is a crash loop, not the kill."""
        self.assertFails(self.tool.check_automatic_restart(healthy_report(), self.restarted(k3s_restarts=2)),
                         'worker signals.k3s_restarts', 'is 2')

    def test_not_active_yet(self):
        for state in ('activating', 'failed', None):
            self.assertFails(self.tool.check_automatic_restart(healthy_report(), self.restarted(k3s_state=state)),
                             'worker signals.k3s_state')

    def test_the_node_rebooted(self):
        report = self.restarted(boot_id='11111111-2222-4333-8444-555555555555')
        self.assertFails(self.tool.check_automatic_restart(healthy_report(), report),
                         'worker signals.boot_id', WORKER_BOOT_ID, 'rebooted')

    def test_the_control_plane_is_not_the_one_judged(self):
        report = self.restarted()
        node_of(report, 'control_plane')['signals']['k3s_restarts'] = 7
        self.assertIsNone(self.tool.check_automatic_restart(healthy_report(), report))


def not_ready_report(ready='Unknown'):
    """A report taken with the worker's k3s-agent stopped, as decision 4d expects it."""
    report = healthy_report()
    worker = node_of(report, 'worker')
    worker['kubernetes']['ready'] = ready
    worker['signals']['k3s_state'] = 'inactive'
    report['healthy'] = False
    return report


class CheckKubeletSilentTestCase(_ToolTestCase):

    def test_either_word_for_not_ready_passes(self):
        for ready in ('Unknown', 'False'):
            self.assertIsNone(self.tool.check_kubelet_silent(not_ready_report(ready)))

    def test_still_ready(self):
        self.assertFails(self.tool.check_kubelet_silent(healthy_report()), 'worker kubernetes.ready', "'True'")

    def test_unread_is_not_an_answer(self):
        """None means the probe could not read readiness, which says nothing about the kubelet."""
        self.assertFails(self.tool.check_kubelet_silent(not_ready_report(None)), 'worker kubernetes.ready',
                         'None')


class CheckNotReadyTestCase(_ToolTestCase):

    def test_a_stopped_kubelet_passes(self):
        self.assertIsNone(self.tool.check_not_ready(not_ready_report()))

    def test_the_cluster_must_be_unhealthy(self):
        report = not_ready_report()
        report['healthy'] = True
        self.assertFails(self.tool.check_not_ready(report), 'healthy is True', 'not False')

    def test_the_node_must_stay_healthy(self):
        report = not_ready_report()
        node_of(report, 'worker')['healthy'] = False
        self.assertFails(self.tool.check_not_ready(report), "worker's node level healthy is False")

    def test_k3s_must_be_inactive(self):
        for state in ('active', 'failed', None):
            report = not_ready_report()
            node_of(report, 'worker')['signals']['k3s_state'] = state
            self.assertFails(self.tool.check_not_ready(report), 'worker signals.k3s_state', 'inactive')

    def test_the_worker_must_not_be_ready(self):
        report = not_ready_report()
        node_of(report, 'worker')['kubernetes']['ready'] = 'True'
        self.assertFails(self.tool.check_not_ready(report), 'worker kubernetes.ready')


class CheckHealthExitCodesTestCase(_ToolTestCase):

    def test_expected_codes_pass(self):
        self.assertIsNone(self.tool.check_health_exit_codes(0, 1, 1))
        self.assertIsNone(self.tool.check_health_exit_codes(0, 0, 0))

    def test_strict_did_not_fail_an_unhealthy_cluster(self):
        """#101: --strict had only ever been run in the direction that passes."""
        self.assertFails(self.tool.check_health_exit_codes(0, 0, 1), '--strict exited 0, not 1')

    def test_strict_failed_a_healthy_cluster(self):
        self.assertFails(self.tool.check_health_exit_codes(0, 1, 0), '--strict exited 1, not 0')

    def test_without_strict_the_exit_code_is_zero(self):
        self.assertFails(self.tool.check_health_exit_codes(1, 1, 1), 'health exited 1, not 0')


class CheckReadyAgainTestCase(_ToolTestCase):

    def test_ready_again_passes(self):
        self.assertIsNone(self.tool.check_ready_again(healthy_report()))

    def test_still_not_ready(self):
        report = healthy_report()
        node_of(report, 'worker')['kubernetes']['ready'] = 'Unknown'
        self.assertFails(self.tool.check_ready_again(report), 'worker kubernetes.ready')

    def test_still_unhealthy(self):
        report = healthy_report()
        report['healthy'] = False
        self.assertFails(self.tool.check_ready_again(report), 'healthy is False')

    def test_k3s_not_active(self):
        report = healthy_report()
        node_of(report, 'worker')['signals']['k3s_state'] = 'activating'
        self.assertFails(self.tool.check_ready_again(report), 'worker signals.k3s_state')


class CheckRestartsResetTestCase(_ToolTestCase):

    def test_zero_passes(self):
        self.assertIsNone(self.tool.check_restarts_reset(healthy_report()))

    def test_not_reset(self):
        report = healthy_report()
        node_of(report, 'worker')['signals']['k3s_restarts'] = 1
        self.assertFails(self.tool.check_restarts_reset(report), 'worker signals.k3s_restarts', 'is 1')

    def test_unreadable(self):
        report = healthy_report()
        node_of(report, 'worker')['signals']['k3s_restarts'] = None
        self.assertFails(self.tool.check_restarts_reset(report), 'worker signals.k3s_restarts', 'is None')


class CheckSnapshotSavedTestCase(_ToolTestCase):

    def snapshot(self, value):
        report = healthy_report()
        node_of(report, 'control_plane')['signals']['etcd_snapshot_bytes'] = value
        return report

    def test_growth_passes(self):
        self.assertIsNone(self.tool.check_snapshot_saved(self.snapshot(4096), self.snapshot(9000000)))

    def test_a_first_snapshot_into_a_new_directory_passes(self):
        """k3s creates the directory with the first snapshot, and a missing one reads None."""
        self.assertIsNone(self.tool.check_snapshot_saved(self.snapshot(None), self.snapshot(9000000)))

    def test_no_growth(self):
        self.assertFails(self.tool.check_snapshot_saved(self.snapshot(4096), self.snapshot(4096)),
                         'control plane signals.etcd_snapshot_bytes', '4096')

    def test_unreadable_after(self):
        self.assertFails(self.tool.check_snapshot_saved(self.snapshot(4096), self.snapshot(None)),
                         'control plane signals.etcd_snapshot_bytes', 'is None')
        self.assertFails(self.tool.check_snapshot_saved(self.snapshot(None), self.snapshot(0)),
                         'control plane signals.etcd_snapshot_bytes')


def disk_pressure_report():
    """A report taken with the worker's disk full, as decision 4f expects it."""
    report = healthy_report()
    node_of(report, 'worker')['kubernetes']['disk_pressure'] = 'True'
    return report


class CheckDiskPressureTestCase(_ToolTestCase):

    def test_reported_and_not_judged_passes(self):
        self.assertIsNone(self.tool.check_disk_pressure_reported(disk_pressure_report()))
        self.assertIsNone(self.tool.check_disk_pressure(disk_pressure_report()))

    def test_not_reported(self):
        self.assertFails(self.tool.check_disk_pressure_reported(healthy_report()),
                         'worker kubernetes.disk_pressure', "'False'")
        self.assertFails(self.tool.check_disk_pressure(healthy_report()), 'worker kubernetes.disk_pressure')

    def test_pressure_must_not_make_the_cluster_unhealthy(self):
        report = disk_pressure_report()
        report['healthy'] = False
        self.assertFails(self.tool.check_disk_pressure(report), 'healthy is False', 'not judged')

    def test_the_worker_must_stay_ready(self):
        report = disk_pressure_report()
        node_of(report, 'worker')['kubernetes']['ready'] = 'False'
        self.assertFails(self.tool.check_disk_pressure(report), 'worker kubernetes.ready')

    def test_only_the_disk_is_under_pressure(self):
        for key in ('memory_pressure', 'pid_pressure'):
            for value in ('True', None):
                report = disk_pressure_report()
                node_of(report, 'worker')['kubernetes'][key] = value
                self.assertFails(self.tool.check_disk_pressure(report), 'worker kubernetes.%s' % key)


class FakeClock:
    """A monotonic clock which only moves when something sleeps."""

    def __init__(self):
        self.now = 5000.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class PollTestCase(_ToolTestCase):

    def reader(self, reports):
        """Return a fake health() which hands out reports in turn, repeating the last."""
        calls = []

        def read():
            calls.append(len(calls))
            return reports[min(len(calls), len(reports)) - 1]
        return read, calls

    def test_returns_the_report_which_satisfied_it(self):
        clock = FakeClock()
        satisfied = not_ready_report()
        read, calls = self.reader([healthy_report(), healthy_report(), satisfied])
        report, elapsed = self.tool.poll(read, self.tool.check_kubelet_silent, 180, 'not ready',
                                         clock=clock, sleep=clock.sleep)
        self.assertIs(satisfied, report)
        self.assertEqual(3, len(calls))
        self.assertEqual([5, 5], clock.sleeps)
        self.assertEqual(10, elapsed)

    def test_satisfied_at_once_does_not_sleep(self):
        clock = FakeClock()
        read, calls = self.reader([not_ready_report()])
        self.tool.poll(read, self.tool.check_kubelet_silent, 180, 'not ready', clock=clock, sleep=clock.sleep)
        self.assertEqual(1, len(calls))
        self.assertEqual([], clock.sleeps)

    def test_gives_up_at_its_bound_with_the_readings(self):
        clock = FakeClock()
        report = healthy_report()
        node_of(report, 'worker')['signals']['oom_kills'] = 17
        read, calls = self.reader([report])
        e = self.assertRaises(self.tool.Failure, self.tool.poll, read, self.tool.check_kubelet_silent, 60,
                              'Kubernetes to stop calling the worker Ready', clock=clock, sleep=clock.sleep)
        message = str(e)

        # Every five seconds from 0 to 60 inclusive, and no sleep after the
        # read which found the bound passed.
        self.assertEqual(13, len(calls))
        self.assertEqual(60, sum(clock.sleeps))

        self.assertIn('gave up after 60s (bound 60s)', message)
        self.assertIn('Kubernetes to stop calling the worker Ready', message)
        # What the predicate last said.
        self.assertIn("worker kubernetes.ready is 'True'", message)
        # The worker's signals and kubernetes entries, and the cluster's verdict.
        self.assertIn('"oom_kills": 17', message)
        self.assertIn(WORKER_BOOT_ID, message)
        self.assertIn('"disk_pressure": "False"', message)
        self.assertIn('healthy: True', message)
        self.assertIn('api: {', message)
        self.assertIn('"answered": true', message)
        self.assertIn('kubernetes: {', message)
        self.assertIn('"unmatched_nodes": []', message)
        # The worker's, not the control plane's.
        self.assertNotIn(CONTROL_PLANE_BOOT_ID, message)

    def test_names_the_role_it_was_asked_about(self):
        clock = FakeClock()
        read, _ = self.reader([healthy_report()])
        e = self.assertRaises(self.tool.Failure, self.tool.poll, read, lambda r: 'never', 10, 'something',
                              role='control_plane', clock=clock, sleep=clock.sleep)
        self.assertIn(CONTROL_PLANE_BOOT_ID, str(e))
        self.assertNotIn(WORKER_BOOT_ID, str(e))

    def test_a_report_without_the_node_still_gives_up_cleanly(self):
        """The give-up message must not replace the failure it explains with a KeyError."""
        clock = FakeClock()
        report = healthy_report()
        report['nodes'] = []
        read, _ = self.reader([report])
        e = self.assertRaises(self.tool.Failure, self.tool.poll, read, lambda r: 'never', 10, 'something',
                              clock=clock, sleep=clock.sleep)
        self.assertIn('no worker node in the report', str(e))


class NodeCommandsTestCase(_ToolTestCase):

    def test_every_node_command_starts_with_a_word_on_the_agents_path(self):
        """The agent refuses a command line whose first word is not an executable on its PATH."""
        for command in (self.tool.KILL_COMMAND, self.tool.STOP_COMMAND, self.tool.START_COMMAND,
                        self.tool.SNAPSHOT_COMMAND, self.tool.disk_fill_command()):
            self.assertIn(shlex.split(command)[0], ('systemctl', 'k3s', 'sh'))

    def test_the_kill_reaches_k3s_alone(self):
        """--kill-who, not --kill-whom: the nodes' Debian 12 systemd 252 predates the newer spelling."""
        self.assertIn('--kill-who=main', self.tool.KILL_COMMAND.split())
        self.assertIn('--signal=KILL', self.tool.KILL_COMMAND.split())

    def test_the_disk_fill_is_one_valid_sh_script(self):
        argv = shlex.split(self.tool.disk_fill_command())
        self.assertEqual(['sh', '-c'], argv[:2])
        self.assertEqual(3, len(argv))
        proc = subprocess.run(['sh', '-n', '-c', argv[2]], capture_output=True, text=True)
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn('fallocate', argv[2])
        self.assertIn('* 3 / 100', argv[2])
