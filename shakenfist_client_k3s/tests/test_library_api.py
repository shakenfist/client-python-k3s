"""The orchestration driven as a library, with no Click anywhere.

Every test here calls Cluster methods (and the namespace scoped module
level functions) directly. That is the whole point of this phase: the
command bodies moved out of the Click layer, so a caller which is not a
terminal -- the Ansible module this work exists for -- can drive a cluster
and keep stdout for its own output.
"""

import copy
import io
import os
import re
import stat
import tempfile

# The PyPI mock backport is used for consistency with the other tests in
# this package, which support Python >= 3.7.
import mock
import testtools
import yaml

from shakenfist_client_k3s import cluster as cluster_module
from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress
from shakenfist_client_k3s.cluster import Cluster
from shakenfist_client_k3s.tests import fakes


MD_KEY = cluster_module.METADATA_KEY % 'banana'


class RecordingClient(fakes.FakeClusterClient):
    """A fake client which records the order of the calls that build a cluster.

    The order in which create writes metadata relative to creating
    instances is load bearing rather than incidental: a create which dies
    part way through leaves behind whatever it had already recorded, and
    that record is what a later crash recovery has to work from.
    """

    def __init__(self):
        super(RecordingClient, self).__init__()
        self.calls = []

    def set_namespace_metadata_item(self, namespace, key, value):
        self.calls.append('set_namespace_metadata_item:%s' % key)
        return super(RecordingClient, self).set_namespace_metadata_item(
            namespace, key, value)

    def create_instance(self, *args, **kwargs):
        self.calls.append('create_instance')
        return super(RecordingClient, self).create_instance(*args, **kwargs)


class LibraryTestCase(testtools.TestCase):
    """Shared setup for driving a Cluster with no terminal attached.

    ~/.kube/config is redirected into a temporary directory and kubectl is
    replaced. Since step 3e a library caller only touches either when it
    asks to, with create(write_kubeconfig=True) or
    delete(update_kubeconfig=True), but the redirection stays
    unconditionally: the tests which do ask for it are here, and a test
    which grew the side effect back by accident would otherwise edit the
    operator's own configuration to tell us so.
    """

    def setUp(self):
        super(LibraryTestCase, self).setUp()
        self.client = RecordingClient()
        self.reporter = progress.CollectingReporter()

        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        self.home = home.name

        patcher = mock.patch.dict('os.environ', {'HOME': self.home})
        patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        # The release lookups are namespace scoped functions with their own
        # tests, and left real they would both reach the internet and write
        # a version cache into the namespace metadata these tests assert on.
        # A real release rather than a channel name, because create()
        # refuses one it cannot parse (check_k3s_release()).
        for target, release in [('get_k3s_release', 'v1.33.4+k3s1'),
                                ('get_longhorn_release', '1.6.0')]:
            patcher = mock.patch(
                'shakenfist_client_k3s.primitives.%s' % target,
                return_value=release)
            patcher.start()
            self.addCleanup(patcher.stop)

        self.subprocess_run = mock.MagicMock()
        self.subprocess_run.return_value.returncode = 0
        # Bytes, as subprocess.run(capture_output=True) returns, and empty
        # rather than a MagicMock: both call sites decode what they are
        # given, and a MagicMock standing in for output is a mock which
        # cannot be wrong in the way the real thing can.
        self.subprocess_run.return_value.stdout = b''
        self.subprocess_run.return_value.stderr = b''
        patcher = mock.patch('subprocess.run', self.subprocess_run)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _cluster(self, client=None):
        return Cluster(client if client is not None else self.client,
                       'banana', 'testns', reporter=self.reporter)

    def _kubeconfig_path(self):
        return os.path.join(self.home, '.kube', 'config')

    def _write_existing_kubeconfig(self):
        """Put an unrelated cluster in ~/.kube/config, and return its path.

        The presence of a file here is what sends create() down its merge
        path rather than its write-a-new-file path, so both the merge tests
        and the tests which assert the merge did not happen need one.
        """
        kube_dir = os.path.join(self.home, '.kube')
        os.makedirs(kube_dir)
        with open(self._kubeconfig_path(), 'w') as f:
            f.write(yaml.dump({
                'apiVersion': 'v1',
                'kind': 'Config',
                'clusters': [{'name': 'other',
                              'cluster': {'server': 'https://192.168.10.1:6443'}}],
                'contexts': [{'name': 'other',
                              'context': {'cluster': 'other', 'user': 'other'}}],
                'users': [{'name': 'other', 'user': {'token': 'x'}}],
                'current-context': 'other'}))
        return self._kubeconfig_path()

    def _kubectl_calls(self, predicate):
        """Every subprocess.run() call whose command matches predicate."""
        return [c for c in self.subprocess_run.call_args_list
                if c.args and predicate(c.args[0])]


class ClusterLifecycleTestCase(LibraryTestCase):
    def test_create_through_delete_writes_nothing_to_stdout(self):
        # The phase's actual deliverable, expressed as a test: a cluster is
        # built, inspected, expanded, updated and destroyed entirely from
        # Python, and not one byte reaches the process's stdout. A caller
        # which is emitting JSON there cannot afford otherwise.
        #
        # The client is a scripted fake rather than a bare mock.MagicMock
        # because the wait loops compare dict values against literals such
        # as 'created' and 'ready'; a MagicMock equals none of them and
        # every wait would spin forever. See tests/fakes.py.
        #
        # Both kubeconfig side effects are asked for explicitly, because
        # they are the paths which shell out to kubectl and this assertion
        # is about what a child process writes as much as about what Python
        # prints. sys.stdout is not the process's file descriptor 1, so
        # until step 3e delete's kubectl calls escaped this test entirely;
        # they now capture their output, which is what lets the assertion
        # cover the whole lifecycle rather than most of it.
        c = self._cluster()

        captured = io.StringIO()
        with mock.patch('sys.stdout', captured):
            c.create(1, 1, 1, write_kubeconfig=True)

            md = c.show()
            self.assertEqual('created', md['state'])
            self.assertEqual('banana', md['name'])

            kubeconfig = c.get_kubeconfig()
            self.assertIn('banana.testns', kubeconfig)

            c.expand_workers(1)
            c.expand_addresses(1)
            c.update_os()
            c.delete(update_kubeconfig=True)

        self.assertEqual('', captured.getvalue())

        # The output went to the reporter instead, which is where a caller
        # collects it from.
        self.assertIn('Cluster banana is ready', self.reporter.getvalue())

        # And the cluster really is gone: both its own metadata document
        # and its entry in the namespace's cluster list.
        self.assertNotIn(MD_KEY, self.client.metadata)
        self.assertNotIn(primitives.CLUSTER_LIST, self.client.metadata)

    def test_create_registers_the_name_before_building_anything(self):
        # The name goes into the namespace cluster list, and then the
        # cluster's own metadata document, before the first instance
        # exists. Reversing that would leave a create which died early with
        # instances nothing knows how to find.
        c = self._cluster()
        c.create(1, 1, 1)

        first_instance = self.client.calls.index('create_instance')
        self.assertEqual(
            ['set_namespace_metadata_item:%s' % primitives.CLUSTER_LIST,
             'set_namespace_metadata_item:%s' % MD_KEY],
            self.client.calls[:first_instance])

    def test_create_records_the_plugin_version(self):
        # plugin_version comes from importlib.metadata, whose import guard
        # moved to cluster.py with this body. If that import had been left
        # behind, this is where it would show up.
        c = self._cluster()
        c.create(1, 1, 1)
        self.assertTrue(c.show()['plugin_version'])

    # Writing ~/.kube/config in create and unsetting it in delete used to
    # be pinned here as mandatory behaviour. Step 3e made both optional, so
    # they moved to OptionalKubeconfigTestCase, which asserts each of them
    # in both directions rather than only the one this class could.

    def test_delete_does_not_run_the_unsets_through_a_shell(self):
        # A cluster name is a bare click.STRING on the CLI, and an Ansible
        # variable or an API request field to the library callers this phase
        # exists for, with no validation on any of those paths. Built into a
        # shell command line, a name like 'foo; rm -rf ~' would execute. The
        # calls must therefore stay a list of arguments with shell unset.
        c = Cluster(self.client, 'foo; touch /tmp/pwned', 'testns',
                    reporter=self.reporter)
        self.client.metadata[cluster_module.METADATA_KEY
                             % 'foo; touch /tmp/pwned'] = copy.deepcopy(DELETABLE_MD)
        self.client.metadata[primitives.CLUSTER_LIST] = [
            'foo; touch /tmp/pwned']
        # update_kubeconfig, or the loop under test does not run at all and
        # the assertions below iterate over nothing.
        c.delete(update_kubeconfig=True)

        for call in self.subprocess_run.call_args_list:
            self.assertIsInstance(call[0][0], list)
            self.assertNotIn('shell', call[1])
        self.assertEqual(
            'clusters.foo; touch /tmp/pwned.testns',
            self.subprocess_run.call_args_list[-1][0][0][-1])

    def test_delete_raises_when_kubectl_unset_fails(self):
        c = self._cluster()
        c.create(1, 1, 1)
        self.subprocess_run.return_value.returncode = 1

        e = self.assertRaises(exceptions.KubeconfigError, c.delete,
                              update_kubeconfig=True)
        self.assertEqual('users.banana.testns', e.config_elem)

    def test_delete_destroys_a_network_it_was_given(self):
        # Known bug, tracked as shakenfist/client-python-k3s#41: delete
        # removes md['node_network'] whether or not create allocated it, so
        # a network handed to create --network is destroyed with the
        # cluster which borrowed it. Pinned here as current behaviour
        # because this step moves code without changing what it does; the
        # fix belongs in its own change, and this test is what will fail
        # loudly when it happens.
        c = self._cluster()
        c.create(1, 1, 1, network='net-1')
        c.delete()

        self.assertEqual(['net-1'], self.client.deleted_networks)


class CreatePhaseCountTestCase(LibraryTestCase):
    """The phase total create computes from its own arguments.

    Progress prints '[n/total]' headers, so a total which does not match
    the number of phases actually run is visible in every line of a
    create's output. The arithmetic depends on five of create's arguments
    -- network, control_plane_count, install_metallb, install_longhorn and
    write_kubeconfig -- and moving it out of the Click layer is exactly
    where it could have been lost.

    The totals below are one lower than they were before step 3e, because
    write_kubeconfig defaults to False: the default library create does not
    update the local kubeconfig, so it does not count that phase either.
    The command line, which passes True, is covered by
    K3sCreateSmokeTestCase in tests/test_progress.py.
    """

    def _phase_totals(self, *args, **kwargs):
        c = self._cluster()
        c.create(*args, **kwargs)
        headers = re.findall(
            r'^\[(\d+)/(\d+)\]', self.reporter.getvalue(), re.MULTILINE)
        self.assertNotEqual([], headers)
        return [int(index) for index, _ in headers], {
            int(total) for _, total in headers}

    def _assert_phases(self, expected_total, *args, **kwargs):
        indexes, totals = self._phase_totals(*args, **kwargs)
        self.assertEqual({expected_total}, totals)
        self.assertEqual(list(range(1, expected_total + 1)), indexes)

    def test_default_shape_allocates_a_network(self):
        # Seven phases, plus one for creating the node network.
        self._assert_phases(8, 1, 1, 1)

    def test_supplied_network_removes_a_phase(self):
        self._assert_phases(7, 1, 1, 1, network='net-1')

    def test_extra_control_plane_nodes_add_a_phase(self):
        self._assert_phases(9, 2, 1, 1)

    def test_both_adjustments_together(self):
        self._assert_phases(8, 2, 1, 1, network='net-1')

    def test_no_metallb_removes_a_phase(self):
        self._assert_phases(7, 1, 1, 1, install_metallb=False)

    def test_no_longhorn_removes_a_phase(self):
        self._assert_phases(7, 1, 1, 1, install_longhorn=False)

    def test_no_metallb_and_no_longhorn_removes_two_phases(self):
        self._assert_phases(
            6, 1, 1, 1, install_metallb=False, install_longhorn=False)

    def test_write_kubeconfig_adds_a_phase(self):
        # The command line's shape: every phase there is.
        self._assert_phases(9, 1, 1, 1, write_kubeconfig=True)

    def test_write_kubeconfig_composes_with_the_other_adjustments(self):
        # The arithmetic is four independent adjustments to a base of eight,
        # so the one added here has to survive the others moving.
        self._assert_phases(
            8, 2, 1, 1, network='net-1', install_longhorn=False,
            write_kubeconfig=True)

    def test_manifests_do_not_add_a_phase(self):
        # Staging a manifest is extra commands inside the phase which
        # installs k3s on the first control plane node rather than a phase
        # of its own, so the total does not move with this argument. The
        # write itself is covered by ManifestStagingTestCase below.
        path = os.path.join(self.home, 'payload.yaml')
        with open(path, 'w') as f:
            f.write('kind: One\n')
        self._assert_phases(8, 1, 1, 1, manifests=[path])


class OptionalMetallbLonghornTestCase(LibraryTestCase):
    """install_metallb and install_longhorn each gate exactly their own setup call.

    CreatePhaseCountTestCase pins the phase arithmetic; this class pins the
    behaviour the arithmetic is standing in for -- that turning one flag
    off skips only that one setup() call, that the other one still runs,
    and that create() still reaches state 'created' either way.
    """

    def test_no_metallb_skips_only_metallb(self):
        c = self._cluster()
        with mock.patch.object(Cluster, 'setup_metallb') as metallb, \
                mock.patch.object(Cluster, 'setup_longhorn') as longhorn:
            c.create(1, 1, 1, install_metallb=False)
        metallb.assert_not_called()
        longhorn.assert_called_once()
        self.assertEqual('created', c.show()['state'])

    def test_no_longhorn_skips_only_longhorn(self):
        c = self._cluster()
        with mock.patch.object(Cluster, 'setup_metallb') as metallb, \
                mock.patch.object(Cluster, 'setup_longhorn') as longhorn:
            c.create(1, 1, 1, install_longhorn=False)
        metallb.assert_called_once()
        longhorn.assert_not_called()
        self.assertEqual('created', c.show()['state'])

    def test_neither_flag_off_runs_both(self):
        c = self._cluster()
        with mock.patch.object(Cluster, 'setup_metallb') as metallb, \
                mock.patch.object(Cluster, 'setup_longhorn') as longhorn:
            c.create(1, 1, 1)
        metallb.assert_called_once()
        longhorn.assert_called_once()

    def test_no_metallb_leaves_no_routed_addresses(self):
        # allocate_metallb_addresses(), which writes routed_addresses, is
        # inside setup_metallb(). Skipping the phase header is not enough
        # on its own -- this checks the work behind it was skipped too.
        c = self._cluster()
        c.create(1, 1, 3, install_metallb=False)
        self.assertEqual([], c.show()['routed_addresses'])

    def test_what_was_installed_is_recorded_in_the_metadata(self):
        # The flags outlive the call which set them: expand_addresses()
        # reads metallb_installed to refuse a cluster which has no metallb
        # before it routes any address, and nothing else in the cluster
        # state says whether the component is there.
        c = self._cluster()
        with mock.patch.object(Cluster, 'setup_metallb'), \
                mock.patch.object(Cluster, 'setup_longhorn'):
            c.create(1, 1, 1, install_metallb=False, install_longhorn=True)

        md = c.show()
        self.assertEqual(False, md['metallb_installed'])
        self.assertEqual(True, md['longhorn_installed'])

    def test_a_default_create_records_both_components(self):
        c = self._cluster()
        with mock.patch.object(Cluster, 'setup_metallb'), \
                mock.patch.object(Cluster, 'setup_longhorn'):
            c.create(1, 1, 1)

        md = c.show()
        self.assertEqual(True, md['metallb_installed'])
        self.assertEqual(True, md['longhorn_installed'])

    def test_the_flags_are_recorded_before_anything_is_built(self):
        # A create which fails part way through still has to describe what
        # it was building, because delete() is the only way out of one and
        # the health report reads the same document.
        c = self._cluster()
        with mock.patch.object(Cluster, 'install_control_plane',
                               side_effect=RuntimeError('boom')):
            self.assertRaises(RuntimeError, c.create, 1, 1, 1,
                              install_longhorn=False)

        md = c.show()
        self.assertEqual(True, md['metallb_installed'])
        self.assertEqual(False, md['longhorn_installed'])

    def test_metal_address_count_is_ignored_without_metallb(self):
        # Decision recorded in create()'s docstring and in --metal-address-
        # count's --help: the combination is accepted, not an error, and
        # the count is simply unused when metallb is not being installed.
        c = self._cluster()
        c.create(1, 1, 999999, install_metallb=False)
        self.assertEqual('created', c.show()['state'])


class NodeSizingTestCase(LibraryTestCase):
    """create() sizes each role as asked, records it, and refuses a size first.

    The six sizing arguments reach the API per role, and are recorded in
    the metadata as node_sizes so that expand_workers() builds the same.
    The recording is asserted against the stored document rather than
    through show(), because show() fills node_sizes in when it is absent
    and would make a create which forgot to record it look as though it
    had.

    The refusal is the claim worth reading closely rather than trusting
    (risk 2 of the phase plan): validate_node_sizes() has to run before
    the name is registered, or a typo becomes a claimed name and a
    metadata document stuck in 'initial'.
    """

    CONTROL_PLANE = {'cpus': 4, 'memory': 8192, 'disk': 100}
    WORKER = {'cpus': 2, 'memory': 4096, 'disk': 60}

    def _sized_kwargs(self):
        kwargs = {}
        for role, size in (('control_plane', self.CONTROL_PLANE),
                           ('worker', self.WORKER)):
            for field, value in size.items():
                kwargs['%s_%s' % (role, field)] = value
        return kwargs

    def _stored(self):
        return self.client.metadata[MD_KEY]

    def _assert_refused_before_anything_is_built(self, **kwargs):
        e = self.assertRaises(
            exceptions.NodeSizeError, self._cluster().create, 1, 1, 1,
            **kwargs)

        self.assertEqual(
            [], self.client.calls,
            'the cluster was registered or an instance created before the '
            'node sizes were validated')
        # Neither the cluster list nor this cluster's own document.
        self.assertEqual({}, self.client.metadata)
        self.assertEqual([], self.client.allocated_networks)
        self.assertEqual({}, self.client.instances)
        self.assertEqual([], self.client.executed)
        return e

    def test_an_invalid_size_registers_nothing(self):
        e = self._assert_refused_before_anything_is_built(worker_memory=0)
        self.assertEqual('worker', e.role)
        self.assertEqual('memory', e.field)
        self.assertEqual(
            'worker memory must be a positive integer, not 0', str(e))

    def test_a_bool_size_registers_nothing(self):
        # The one an int check alone would let through, as a one vCPU node.
        e = self._assert_refused_before_anything_is_built(
            control_plane_cpus=True)
        self.assertEqual('control_plane', e.role)
        self.assertEqual('cpus', e.field)

    def test_the_name_is_free_after_a_refusal(self):
        # The consequence the ordering exists for: the caller fixes the
        # typo and runs the same create again, and it is not told the name
        # is taken or that an earlier attempt was interrupted.
        self.assertRaises(exceptions.NodeSizeError, self._cluster().create,
                          1, 1, 1, control_plane_disk=-1)
        self._cluster().create(1, 1, 1)
        self.assertEqual('created', self._stored()['state'])

    def test_each_role_is_built_at_its_own_size(self):
        self._cluster().create(1, 2, 1, **self._sized_kwargs())

        cp, w = self.CONTROL_PLANE, self.WORKER
        self.assertEqual(
            [('k3s-banana-node-001', cp['cpus'], cp['memory'], cp['disk']),
             ('k3s-banana-node-002', w['cpus'], w['memory'], w['disk']),
             ('k3s-banana-node-003', w['cpus'], w['memory'], w['disk'])],
            self.client.instance_sizes)

    def test_the_sizes_are_recorded_in_the_metadata(self):
        self._cluster().create(1, 1, 1, **self._sized_kwargs())

        self.assertEqual(
            {'control_plane': self.CONTROL_PLANE, 'worker': self.WORKER},
            self._stored()['node_sizes'])

    def test_a_default_create_builds_every_node_at_the_default(self):
        # Literals rather than DEFAULT_NODE_SIZE: this is the claim that an
        # existing invocation builds exactly what it built before these
        # arguments existed, and that is 2 vCPUs, 2048 MB and 50 GB
        # whatever the constant says.
        self._cluster().create(3, 2, 1)

        self.assertEqual(5, len(self.client.instance_sizes))
        self.assertEqual(
            {(2, 2048, 50)},
            {sizes[1:] for sizes in self.client.instance_sizes})

        # And it is recorded, not left to the fallback: a default create
        # is a cluster created after node_sizes existed like any other.
        self.assertEqual(
            {'control_plane': {'cpus': 2, 'memory': 2048, 'disk': 50},
             'worker': {'cpus': 2, 'memory': 2048, 'disk': 50}},
            self._stored()['node_sizes'])

    def test_the_sizes_are_recorded_before_anything_is_built(self):
        # As metallb_installed is: a create which dies before its first
        # instance still describes what it was building.
        with mock.patch.object(Cluster, 'create_and_await_instances',
                               side_effect=RuntimeError('boom')):
            self.assertRaises(RuntimeError, self._cluster().create, 1, 1, 1,
                              **self._sized_kwargs())

        self.assertEqual('initial', self._stored()['state'])
        self.assertEqual(
            {'control_plane': self.CONTROL_PLANE, 'worker': self.WORKER},
            self._stored()['node_sizes'])

    def test_expand_workers_builds_at_the_size_create_recorded(self):
        # The reason node_sizes is recorded at all, end to end through the
        # library: the new worker is the size the create asked for, not
        # the default and not the control plane's.
        c = self._cluster()
        c.create(1, 1, 1, **self._sized_kwargs())
        c.expand_workers(1)

        w = self.WORKER
        self.assertEqual(
            ('k3s-banana-node-003', w['cpus'], w['memory'], w['disk']),
            self.client.instance_sizes[-1])


class K3sConfigTestCase(LibraryTestCase):
    """create() records both k3s configurations, and refuses a bad one or an old release first.

    The refusals are NodeSizingTestCase's claim again, for two more checks:
    a configuration validate_k3s_config() refuses, and a release older
    than K3S_RELEASE_FLOOR, must each raise before the name is registered,
    or the caller is left with a claimed name and a metadata document
    stuck in 'initial'. The release check is the one worth reading
    closely, because it cannot sit with the others at the top of create():
    it needs the resolved release, so it has to sit between the lookup
    and the registration.

    As in NodeSizingTestCase, the recording is asserted against the stored
    document rather than through show(), which fills both keys in when they
    are absent.
    """

    SERVER = {'disable': ['traefik'],
              'node-label': ['openstack-control-plane=enabled']}
    AGENT = {'node-label': ['openstack-compute-node=enabled',
                            'openvswitch=enabled']}

    def _stored(self):
        return self.client.metadata[MD_KEY]

    def _assert_refused_before_anything_is_built(self, exception, **kwargs):
        e = self.assertRaises(
            exception, self._cluster().create, 1, 1, 1, **kwargs)

        self.assertEqual(
            [], self.client.calls,
            'the cluster was registered or an instance created before the '
            'refusal')
        self.assertEqual({}, self.client.metadata)
        self.assertEqual([], self.client.allocated_networks)
        self.assertEqual({}, self.client.instances)
        self.assertEqual([], self.client.executed)
        return e

    def test_an_owned_server_key_registers_nothing(self):
        e = self._assert_refused_before_anything_is_built(
            exceptions.K3sConfigError,
            server_config={'disable': ['traefik'], 'cluster-init': False})
        self.assertEqual('owned_key', e.reason)
        self.assertEqual('server', e.role)
        self.assertEqual('cluster-init', e.key)

    def test_an_owned_agent_key_registers_nothing(self):
        e = self._assert_refused_before_anything_is_built(
            exceptions.K3sConfigError,
            server_config=self.SERVER, agent_config={'node-name': 'w'})
        self.assertEqual('owned_key', e.reason)
        self.assertEqual('agent', e.role)
        self.assertEqual('node-name', e.key)

    def test_a_release_below_the_floor_registers_nothing(self):
        with mock.patch('shakenfist_client_k3s.primitives.get_k3s_release',
                        return_value='v1.20.15+k3s1'):
            e = self._assert_refused_before_anything_is_built(
                exceptions.UnsupportedReleaseError, release_channel='v1.20')
        self.assertEqual('too_old', e.reason)
        self.assertEqual('v1.20.15+k3s1', e.release)
        self.assertEqual('v1.20', e.channel)

    def test_a_failed_longhorn_lookup_registers_nothing(self):
        # #119: the Longhorn lookup used to run at the Longhorn phase,
        # after every instance existed, so a failure there left a cluster
        # create could not resume. It now fails with nothing built.
        with mock.patch(
                'shakenfist_client_k3s.primitives.get_longhorn_release',
                side_effect=exceptions.ReleaseLookupError.
                no_compatible_longhorn_release('v1.33.4+k3s1')):
            e = self._assert_refused_before_anything_is_built(
                exceptions.ReleaseLookupError)
        self.assertEqual('no_compatible_longhorn_release', e.reason)

    def test_the_longhorn_lookup_is_for_the_resolved_release(self):
        # #118 and #119 together: the chart is chosen for the k3s release
        # this create resolved, once, at the start, with the refresh flag
        # the caller passed; and setup_longhorn() installs what was
        # recorded rather than looking again.
        with mock.patch(
                'shakenfist_client_k3s.primitives.get_longhorn_release',
                return_value='1.12.1') as lookup:
            self._cluster().create(1, 1, 1, refresh_version_cache=True)

        lookup.assert_called_once()
        self.assertEqual('v1.33.4+k3s1', lookup.call_args.kwargs['k3s_version'])
        self.assertTrue(lookup.call_args.kwargs['force_cache_update'])
        self.assertEqual('1.12.1', self._stored()['longhorn_version'])
        self.assertTrue(any('--version 1.12.1' in command
                            for _, command in self.client.executed),
                        self.client.executed)

    def test_no_longhorn_means_no_longhorn_lookup(self):
        with mock.patch(
                'shakenfist_client_k3s.primitives.get_longhorn_release',
                side_effect=AssertionError('looked up')):
            self._cluster().create(1, 1, 1, install_longhorn=False)

        self.assertEqual('created', self._stored()['state'])
        self.assertIsNone(self._stored()['longhorn_version'])

    def test_both_configs_are_recorded(self):
        self._cluster().create(1, 1, 1, server_config=self.SERVER,
                               agent_config=self.AGENT)

        self.assertEqual('created', self._stored()['state'])
        self.assertEqual(self.SERVER, self._stored()['server_config'])
        self.assertEqual(self.AGENT, self._stored()['agent_config'])

    def test_a_default_create_records_empty_configs(self):
        # Recorded, not left to show()'s fill: a default create is a
        # cluster created after these keys existed like any other.
        self._cluster().create(1, 1, 1)

        self.assertEqual({}, self._stored()['server_config'])
        self.assertEqual({}, self._stored()['agent_config'])

    def test_the_configs_are_recorded_before_anything_is_built(self):
        # As node_sizes is: a create which dies before its first instance
        # still describes what it was building.
        with mock.patch.object(Cluster, 'create_and_await_instances',
                               side_effect=RuntimeError('boom')):
            self.assertRaises(RuntimeError, self._cluster().create, 1, 1, 1,
                              server_config=self.SERVER,
                              agent_config=self.AGENT)

        self.assertEqual('initial', self._stored()['state'])
        self.assertEqual(self.SERVER, self._stored()['server_config'])
        self.assertEqual(self.AGENT, self._stored()['agent_config'])


class BindAddressClient(RecordingClient):
    """A client whose node wrote its kubeconfig for a configured bind-address.

    k3s puts https://127.0.0.1:6443 in /etc/rancher/k3s/k3s.yaml only when
    no bind-address is set. With one, the server URL names that address.
    """

    def get_blob_data(self, blob_uuid):
        if blob_uuid.endswith('k3s.yaml'):
            yield fakes.KUBECONFIG.replace(
                '127.0.0.1', '10.0.0.4').encode('utf-8')
        else:
            yield from super(BindAddressClient, self).get_blob_data(blob_uuid)


class KubeconfigServerTestCase(LibraryTestCase):
    """The recorded kubeconfig names the floating API address, however k3s wrote it."""

    def _server(self):
        kc = yaml.safe_load(self.client.metadata[MD_KEY]['kubeconfig'])
        return kc['clusters'][0]['cluster']['server']

    def test_the_loopback_address_is_replaced(self):
        self._cluster().create(1, 1, 1)
        self.assertEqual('https://192.168.10.100:6443', self._server())

    def test_a_bind_address_is_replaced_too(self):
        self.client = BindAddressClient()
        self._cluster().create(
            1, 1, 1, server_config={'bind-address': '10.0.0.4'})
        self.assertEqual('https://192.168.10.100:6443', self._server())


class ClusterAccessorTestCase(testtools.TestCase):
    """show() and get_kubeconfig(), which return rather than print."""

    def _cluster(self, namespace_md):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = namespace_md
        return Cluster(client, 'banana', 'testns',
                       reporter=progress.CollectingReporter())

    def test_show_returns_the_metadata(self):
        # node_sizes, server_config and agent_config are stored here
        # because they are the keys show() fills in when they are absent,
        # which is not what this test is about; the fills are
        # ShowReportsNodeSizesTestCase's and ShowReportsK3sConfigTestCase's,
        # in tests/test_cluster.py.
        md = {'name': 'banana', 'state': 'created',
              'node_sizes': {
                  'control_plane': {'cpus': 2, 'memory': 2048, 'disk': 50},
                  'worker': {'cpus': 2, 'memory': 2048, 'disk': 50}},
              'server_config': {}, 'agent_config': {}}
        self.assertEqual(md, self._cluster({MD_KEY: md}).show())

    def test_show_raises_for_an_unknown_cluster(self):
        c = self._cluster({})
        e = self.assertRaises(exceptions.ClusterNotFoundError, c.show)
        self.assertEqual('banana', e.name)

    def test_get_kubeconfig_returns_the_config(self):
        c = self._cluster({MD_KEY: {'kubeconfig': fakes.KUBECONFIG}})
        self.assertEqual(fakes.KUBECONFIG, c.get_kubeconfig())

    def test_get_kubeconfig_raises_for_an_unknown_cluster(self):
        c = self._cluster({})
        self.assertRaises(exceptions.ClusterNotFoundError, c.get_kubeconfig)

    def test_get_kubeconfig_raises_for_an_unfinished_cluster(self):
        # A cluster which exists but never finished being built is a
        # different failure to one which does not exist at all.
        c = self._cluster({MD_KEY: {'name': 'banana', 'kubeconfig': None}})
        self.assertRaises(exceptions.ClusterIncompleteError, c.get_kubeconfig)


class MissingClusterTestCase(testtools.TestCase):
    """The three expansion verbs all refuse to work on a cluster which is absent."""

    def _cluster(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {}
        return Cluster(client, 'banana', 'testns',
                       reporter=progress.CollectingReporter())

    def test_expand_workers(self):
        self.assertRaises(exceptions.ClusterNotFoundError,
                          self._cluster().expand_workers, 2)

    def test_expand_addresses(self):
        self.assertRaises(exceptions.ClusterNotFoundError,
                          self._cluster().expand_addresses, 2)

    def test_update_os(self):
        self.assertRaises(exceptions.ClusterNotFoundError,
                          self._cluster().update_os)

    def test_delete(self):
        self.assertRaises(exceptions.ClusterNotFoundError,
                          self._cluster().delete)


class ListClustersTestCase(testtools.TestCase):
    """list is namespace scoped, so it is a function rather than a method.

    There is no cluster to build here -- the command names none -- which is
    why this lives in primitives alongside the two release lookups, and why
    the constant it reads does too.
    """

    def test_returns_the_names(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.CLUSTER_LIST: ['banana', 'carrot']}
        self.assertEqual(['banana', 'carrot'],
                         primitives.list_clusters(client, 'testns'))
        client.get_namespace_metadata.assert_called_once_with('testns')

    def test_empty_when_the_namespace_has_no_clusters(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {}
        self.assertEqual([], primitives.list_clusters(client, 'testns'))


# Enough cluster metadata for delete to run straight to the local kubectl
# cleanup: no nodes to wait for and no node network to unroute addresses
# from.
DELETABLE_MD = {
    'name': 'banana', 'namespace': 'testns', 'state': 'created',
    'control_plane_nodes': [], 'worker_nodes': [], 'routed_addresses': [],
    'node_network': None,
}


class LocalKubeconfigFailureTestCase(LibraryTestCase):
    """create's two local kubeconfig failures, raised where they happen.

    Both only occur when there is an existing ~/.kube/config to merge
    into, so each test writes one first, and both are downstream of
    write_kubeconfig, which each test therefore has to ask for. They are
    the last two exception constructors in the hierarchy without a test
    which drives the code that raises them, as opposed to constructing them
    directly.
    """

    def test_create_without_a_local_kubectl(self):
        main_config_path = self._write_existing_kubeconfig()
        c = self._cluster()

        captured = io.StringIO()
        with mock.patch('shutil.which', return_value=None):
            with mock.patch('sys.stdout', captured):
                e = self.assertRaises(
                    exceptions.KubeconfigError, c.create, 1, 1, 1,
                    write_kubeconfig=True)

        self.assertEqual('', captured.getvalue())
        self.assertEqual('missing_kubectl', e.reason)
        self.assertEqual(main_config_path, e.main_config_path)
        self.assertEqual('banana', e.name)
        self.assertIn("'sf-client k3s getconfig banana'", str(e))

    def test_create_when_the_kubectl_merge_fails(self):
        main_config_path = self._write_existing_kubeconfig()
        # kubectl's stderr arrives as bytes and is decoded at the raise, so
        # the exception has to render the text rather than a bytes repr.
        self.subprocess_run.return_value.returncode = 1
        self.subprocess_run.return_value.stderr = b'error: no such context\n'
        c = self._cluster()

        captured = io.StringIO()
        with mock.patch('shutil.which', return_value='/usr/bin/kubectl'):
            with mock.patch('sys.stdout', captured):
                e = self.assertRaises(
                    exceptions.KubeconfigError, c.create, 1, 1, 1,
                    write_kubeconfig=True)

        self.assertEqual('', captured.getvalue())
        self.assertEqual('merge_failed', e.reason)
        self.assertEqual(main_config_path, e.main_config_path)
        self.assertEqual(1, e.returncode)
        self.assertEqual('error: no such context\n', e.stderr)
        self.assertEqual(
            'Failed to update %s, return code 1\nerror: no such context\n'
            % main_config_path, str(e))

        # The pre-existing configuration must be left exactly as it was: a
        # failed merge must not half-write the file it was merging into.
        with open(main_config_path) as f:
            self.assertEqual('other', yaml.safe_load(f)['current-context'])


def _is_kubectl_unset(command):
    """Is this subprocess.run() first argument a 'kubectl config unset'?

    The argument is a list rather than a shell string, so a prefix match on
    a string does not work here. Matching on the list's leading elements
    keeps this working whichever way a future change spells the call.
    """
    if isinstance(command, str):
        return command.startswith('kubectl config unset')
    return list(command[:3]) == ['kubectl', 'config', 'unset']


def _is_kubectl_config_view(command):
    """Is this subprocess.run() first argument create's merge command?

    An argument list, like the unset calls. The string form is still
    recognised so that this helper describes the call rather than the
    spelling of the day.
    """
    if isinstance(command, str):
        return command.startswith('kubectl config view')
    return list(command[:3]) == ['kubectl', 'config', 'view']


class OptionalKubeconfigTestCase(LibraryTestCase):
    """Neither kubeconfig side effect happens unless the caller asks for it.

    These are the only two things either verb does to the machine it is
    running on rather than to the cluster, and step 3e made them the one
    place where the library's default is not the command line's:
    create(write_kubeconfig=False) and delete(update_kubeconfig=False), with
    the Click commands passing True. Decision 6 of the phase 3 plan argues
    it; what is pinned here is that "off" means no file and no kubectl, not
    merely a missing phase header.

    The tests which assert nothing happened are the ones which can pass for
    the wrong reason, so each of them has a partner which turns the flag on
    and sees the same side effect occur.
    """

    def test_create_writes_no_kubeconfig_by_default(self):
        c = self._cluster()
        c.create(1, 1, 1)

        self.assertFalse(os.path.exists(self._kubeconfig_path()))
        self.assertEqual([], self._kubectl_calls(_is_kubectl_config_view))
        self.assertNotIn('Updating local kubeconfig', self.reporter.getvalue())

        # The fetch is deliberately not gated: get_kubeconfig() serves what
        # create() recorded, so declining the local write must not cost the
        # caller the credentials.
        self.assertIn('banana.testns', c.get_kubeconfig())

    def test_create_writes_the_kubeconfig_when_asked(self):
        c = self._cluster()
        c.create(1, 1, 1, write_kubeconfig=True)

        with open(self._kubeconfig_path()) as f:
            written = yaml.safe_load(f)
        self.assertEqual('banana.testns', written['current-context'])
        self.assertIn('Updating local kubeconfig', self.reporter.getvalue())

    def test_a_new_kubeconfig_and_its_directory_are_private(self):
        """A k3s kubeconfig is a cluster-admin credential, so 0600 and 0700.

        Both were created at the process umask, which on a default umask
        022 host is 0755 and 0644 -- readable by every local user on the
        machine that ran create. That is the normal situation on a shared
        jump box or a CI runner, and the file embeds
        client-certificate-data and client-key-data for a cluster-admin
        identity, so it is full control of the cluster rather than read
        access to it.
        """
        # Not inherited from the test runner's: a umask which already
        # happened to be 077 would make this pass without the fix.
        previous = os.umask(0o022)
        self.addCleanup(os.umask, previous)

        self._cluster().create(1, 1, 1, write_kubeconfig=True)

        path = self._kubeconfig_path()
        self.assertEqual(
            0o600, stat.S_IMODE(os.stat(path).st_mode),
            'a new kubeconfig is readable by other local users')
        self.assertEqual(
            0o700, stat.S_IMODE(os.stat(os.path.dirname(path)).st_mode),
            '~/.kube is readable by other local users')

    def test_an_existing_kube_directory_keeps_its_own_mode(self):
        """exist_ok, so a user who tightened or loosened theirs decides."""
        kube_dir = os.path.join(self.home, '.kube')
        os.makedirs(kube_dir, mode=0o750)

        self._cluster().create(1, 1, 1, write_kubeconfig=True)

        self.assertEqual(0o750, stat.S_IMODE(os.stat(kube_dir).st_mode))

    def test_create_leaves_an_existing_kubeconfig_alone_by_default(self):
        # The sharper version of the first test: with a file already there,
        # create's other path is the one which shells out to kubectl, and
        # neither the merge nor the write-back may happen.
        main_config_path = self._write_existing_kubeconfig()
        with open(main_config_path) as f:
            before = f.read()

        # A working kubectl and a merge which would succeed, deliberately,
        # for a path this test says must not be taken. Without them the
        # assertions below would be doing their work by accident: a create
        # which wrongly took the merge path would die on a missing kubectl
        # or on unparsable output, and this test would pass on a traceback
        # rather than on the claim it is making.
        self.subprocess_run.return_value.stdout = before.encode('utf-8')

        with mock.patch('shutil.which', return_value='/usr/bin/kubectl'):
            self._cluster().create(1, 1, 1)

        self.assertEqual([], self._kubectl_calls(_is_kubectl_config_view))
        with open(main_config_path) as f:
            self.assertEqual(before, f.read())

    def test_create_merges_into_an_existing_kubeconfig_when_asked(self):
        main_config_path = self._write_existing_kubeconfig()
        self.subprocess_run.return_value.stdout = yaml.dump({
            'apiVersion': 'v1', 'kind': 'Config',
            'clusters': [{'name': 'other', 'cluster': {}},
                         {'name': 'banana.testns', 'cluster': {}}],
            'contexts': [], 'users': [],
            'current-context': 'other'}).encode('utf-8')

        with mock.patch('shutil.which', return_value='/usr/bin/kubectl'):
            self._cluster().create(1, 1, 1, write_kubeconfig=True)

        self.assertEqual(
            1, len(self._kubectl_calls(_is_kubectl_config_view)))
        with open(main_config_path) as f:
            merged = yaml.safe_load(f)
        self.assertEqual(['other', 'banana.testns'],
                         [c['name'] for c in merged['clusters']])
        self.assertEqual('banana.testns', merged['current-context'])

    def test_delete_unsets_nothing_by_default(self):
        c = self._cluster()
        c.create(1, 1, 1, write_kubeconfig=True)
        c.delete()

        self.assertEqual([], self._kubectl_calls(_is_kubectl_unset))

        # And the file create() wrote is still there, which is the cost of
        # the asymmetry rather than a bug: a caller which asked for the
        # write asks for the cleanup too.
        self.assertTrue(os.path.exists(self._kubeconfig_path()))

    def test_delete_unsets_all_three_elements_when_asked(self):
        c = self._cluster()
        c.create(1, 1, 1)
        c.delete(update_kubeconfig=True)

        self.assertEqual(
            [['kubectl', 'config', 'unset', 'users.banana.testns'],
             ['kubectl', 'config', 'unset', 'contexts.banana.testns'],
             ['kubectl', 'config', 'unset', 'clusters.banana.testns']],
            [call.args[0] for call in self._kubectl_calls(_is_kubectl_unset)])

        # Argument lists rather than shell strings, because the cluster name
        # interpolated into each element is caller supplied and unvalidated;
        # and nothing else shelled out, since this create declined the write.
        self.assertEqual(3, len(self.subprocess_run.call_args_list))

    def test_the_unset_calls_capture_their_output(self):
        # What KubectlUnsetLeakTestCase used to pin, inverted. sys.stdout is
        # a Python object and these calls are child processes, so without
        # capture_output kubectl's three 'Property "..." unset.' lines went
        # to the process's file descriptor 1 whatever the reporter was doing
        # -- which an Ansible module emitting JSON there cannot afford.
        c = self._cluster()
        c.create(1, 1, 1)
        c.delete(update_kubeconfig=True)

        calls = self._kubectl_calls(_is_kubectl_unset)
        self.assertEqual(3, len(calls))
        for call in calls:
            self.assertEqual(
                True, call.kwargs.get('capture_output'),
                'This kubectl config unset inherits file descriptor 1, so '
                "its 'Property \"...\" unset.' line bypasses the reporter "
                'and lands on the caller\'s stdout: %r' % (call,))

    def test_the_captured_stdout_reaches_the_reporter(self):
        # Captured is not discarded: the lines go where every other line
        # delete() emits goes, at debug level.
        c = self._cluster()
        c.create(1, 1, 1)
        self.subprocess_run.return_value.stdout = (
            b'Property "users.banana.testns" unset.\n')

        verbose = progress.CollectingReporter(verbose=True)
        Cluster(self.client, 'banana', 'testns',
                reporter=verbose).delete(update_kubeconfig=True)

        self.assertIn('Property "users.banana.testns" unset.',
                      verbose.getvalue())

    def test_the_captured_stderr_is_carried_by_the_failure(self):
        # The other half of capturing the output: kubectl's account of why
        # it failed used to reach the terminal by itself.
        c = self._cluster()
        c.create(1, 1, 1)
        self.subprocess_run.return_value.returncode = 1
        self.subprocess_run.return_value.stderr = (
            b'error: unable to parse /home/u/.kube/config\n')

        e = self.assertRaises(exceptions.KubeconfigError, c.delete,
                              update_kubeconfig=True)

        self.assertEqual('unset_failed', e.reason)
        self.assertEqual('users.banana.testns', e.config_elem)
        self.assertEqual('error: unable to parse /home/u/.kube/config\n',
                         e.stderr)
        self.assertEqual(
            'Could not unset kubectl config element users.banana.testns\n'
            'error: unable to parse /home/u/.kube/config\n', str(e))


class ManifestStagingTestCase(LibraryTestCase):
    """create(manifests=...) stages files on the node before k3s is installed.

    The ordering is the whole mechanism rather than a detail: k3s applies
    everything in its manifests directory when the server first starts, so
    a manifest written after the installer has run is a manifest which is
    applied on the next restart, whenever that turns out to be. That is
    also the kind of claim a test can agree with while checking nothing, so
    the assertions here are on positions in one ordered log of what the
    client was asked to run, not on the presence of two commands somewhere.

    Nothing is templated (decision 8 of the phase 3 plan), which makes
    "written verbatim" a testable property and one worth testing: the
    content goes to the node inside a shell heredoc, and an unquoted
    delimiter would have the node's shell expand the $ and the backticks a
    real manifest is full of.
    """

    def setUp(self):
        super(ManifestStagingTestCase, self).setUp()
        manifests = tempfile.TemporaryDirectory()
        self.addCleanup(manifests.cleanup)
        self.manifest_dir = manifests.name

    def _manifest(self, name, content, subdir=None):
        directory = self.manifest_dir
        if subdir:
            directory = os.path.join(self.manifest_dir, subdir)
            os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, name)
        with open(path, 'w') as f:
            f.write(content)
        return path

    def _commands(self):
        return [commandline for _, commandline in self.client.executed]

    def _index_of(self, predicate, description):
        for i, commandline in enumerate(self._commands()):
            if predicate(commandline):
                return i
        self.fail('%s never ran. The node was asked to run:\n    %s'
                  % (description,
                     '\n    '.join(repr(c) for c in self._commands())
                     or '(nothing at all)'))

    def _write_index(self, basename):
        return self._index_of(
            lambda c: c.startswith(
                'cat - > %s/%s ' % (cluster_module.K3S_MANIFEST_DIR, basename)),
            'the write of %s' % basename)

    def _server_install_index(self):
        # The worker install uses the same installer with 'agent', so the
        # role is what identifies the control plane node's install. The
        # installer itself is recognised by the environment variable it
        # is driven with rather than by the URL it is fetched from:
        # CodeQL reads a hostname substring test as an incomplete URL
        # sanitization (py/incomplete-url-substring-sanitization) and
        # fails the Analyze job over it, which is a fair complaint about
        # the shape even though nothing here is sanitizing anything.
        return self._index_of(
            lambda c: ('INSTALL_K3S_CHANNEL=' in c
                       and c.endswith('sh -s - server')),
            'the k3s server install')

    def test_two_manifests_are_written_before_k3s_is_installed(self):
        first = self._manifest('first.yaml', 'kind: One\n')
        second = self._manifest('second.yaml', 'kind: Two\n')

        self._cluster().create(1, 1, 1, manifests=[first, second])

        install = self._server_install_index()
        for basename in ['first.yaml', 'second.yaml']:
            write = self._write_index(basename)
            self.assertTrue(
                write < install,
                '%s was written at position %d, after the k3s install at '
                'position %d. k3s applies its manifests directory when the '
                'server starts, so this manifest would not be applied until '
                'something restarted it. The node was asked to run:\n    %s'
                % (basename, write, install,
                   '\n    '.join(repr(c) for c in self._commands())))

    def test_the_manifests_are_written_on_the_first_control_plane_node(self):
        path = self._manifest('only.yaml', 'kind: One\n')

        cluster = self._cluster()
        cluster.create(1, 1, 1, manifests=[path])

        md = cluster.show()
        wrote_on = [instance_uuid for instance_uuid, commandline
                    in self.client.executed
                    if cluster_module.K3S_MANIFEST_DIR in commandline]
        self.assertNotEqual([], wrote_on)
        self.assertEqual([md['control_plane_nodes'][0]], list(set(wrote_on)))

    def test_the_directory_is_created_before_the_manifests_are_written(self):
        # k3s creates this directory when the server starts, which has not
        # happened yet, so the write would fail without this.
        path = self._manifest('only.yaml', 'kind: One\n')

        self._cluster().create(1, 1, 1, manifests=[path])

        mkdir = self._index_of(
            lambda c: c.startswith('mkdir -p') and c.endswith(
                cluster_module.K3S_MANIFEST_DIR),
            'the creation of %s' % cluster_module.K3S_MANIFEST_DIR)
        self.assertTrue(mkdir < self._write_index('only.yaml'))

        # And it is created with the mode k3s would have used. Go's
        # MkdirAll does not tighten a directory which already exists, so a
        # directory left at mkdir's default here would permanently loosen
        # the directory k3s is about to put the cluster's tokens and TLS
        # keys in. -m applies only to the directories named as operands,
        # which is why the parents are named.
        command = self._commands()[mkdir]
        self.assertIn('-m 0700', command)
        for directory in ['/var/lib/rancher', '/var/lib/rancher/k3s',
                          '/var/lib/rancher/k3s/server']:
            self.assertIn(directory + ' ', command)

    def test_the_content_is_written_verbatim(self):
        # A manifest full of the things a shell would otherwise eat: $, a
        # command substitution, and both kinds of quote. The heredoc
        # delimiter is quoted, which is what turns all of that off, and
        # this asserts on the whole command so that a change to the
        # quoting cannot pass.
        content = (
            'apiVersion: v1\n'
            'kind: ConfigMap\n'
            'metadata:\n'
            '  name: shell-hazards\n'
            'data:\n'
            '  entrypoint.sh: |\n'
            '    echo "$HOME is $(hostname) or `hostname`"\n'
            "    echo '${NOT_EXPANDED}' > /tmp/manifest-test\n"
        )
        path = self._manifest('hazards.yaml', content)

        self._cluster().create(1, 1, 1, manifests=[path])

        self.assertEqual(
            "cat - > %s/hazards.yaml << '%s'\n%s%s\n"
            % (cluster_module.K3S_MANIFEST_DIR,
               cluster_module.K3S_MANIFEST_DELIMITER, content,
               cluster_module.K3S_MANIFEST_DELIMITER),
            self._commands()[self._write_index('hazards.yaml')])

    def test_a_manifest_with_no_trailing_newline_gains_one(self):
        # The heredoc's closing delimiter has to be on a line of its own,
        # so a file which does not end in a newline needs one added. YAML
        # does not care, and the alternative is a command which never
        # terminates its heredoc.
        path = self._manifest('terse.yaml', 'kind: One')

        self._cluster().create(1, 1, 1, manifests=[path])

        self.assertEqual(
            "cat - > %s/terse.yaml << '%s'\nkind: One\n%s\n"
            % (cluster_module.K3S_MANIFEST_DIR,
               cluster_module.K3S_MANIFEST_DELIMITER,
               cluster_module.K3S_MANIFEST_DELIMITER),
            self._commands()[self._write_index('terse.yaml')])

    def test_no_manifests_means_no_write_at_all(self):
        # The default, and every caller which existed before this argument
        # did: the manifests directory is never mentioned, so neither the
        # mkdir nor a write happens.
        self._cluster().create(1, 1, 1)

        self.assertEqual(
            [], [c for c in self._commands()
                 if cluster_module.K3S_MANIFEST_DIR in c])

    def test_a_duplicate_basename_raises_before_anything_is_built(self):
        # The basename is the destination filename, so the second of these
        # would silently replace the first on the node.
        first = self._manifest('same.yaml', 'kind: One\n', subdir='one')
        second = self._manifest('same.yaml', 'kind: Two\n', subdir='two')

        e = self.assertRaises(
            exceptions.ManifestError, self._cluster().create,
            1, 1, 1, manifests=[first, second])
        self.assertEqual('duplicate_basename', e.reason)

        self._assert_nothing_was_built()

    def test_an_absent_manifest_raises_before_anything_is_built(self):
        # The library boundary: --manifest is a click.Path(exists=True), so
        # the command line refuses this before create() is called at all,
        # but a library caller's paths have been checked by nobody. Failing
        # at the point of use instead would leave a cluster of instances
        # which have to be deleted before the name can be used again.
        e = self.assertRaises(
            exceptions.ManifestError, self._cluster().create,
            1, 1, 1, manifests=[os.path.join(self.manifest_dir, 'absent.yaml')])
        self.assertEqual('unreadable', e.reason)

        self._assert_nothing_was_built()

    def _assert_nothing_was_built(self):
        self.assertEqual(
            [], self.client.calls,
            'the cluster was registered or an instance created before the '
            'manifests were read')
        self.assertEqual({}, self.client.instances)
        self.assertEqual({}, self.client.metadata)
        self.assertEqual([], self.client.executed)

    def test_the_staging_is_reported(self):
        first = self._manifest('first.yaml', 'kind: One\n')
        second = self._manifest('second.yaml', 'kind: Two\n')

        self._cluster().create(1, 1, 1, manifests=[first, second])

        output = self.reporter.getvalue()
        self.assertIn('staging 2 manifests', output)
        self.assertIn('first.yaml, second.yaml', output)
