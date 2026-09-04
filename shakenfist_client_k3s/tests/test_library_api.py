"""The orchestration driven as a library, with no Click anywhere.

Every test here calls Cluster methods (and the namespace scoped module
level functions) directly. That is the whole point of this phase: the
command bodies moved out of the Click layer, so a caller which is not a
terminal -- the Ansible module this work exists for -- can drive a cluster
and keep stdout for its own output.
"""

import io
import os
import re
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
    replaced, because writing the local kubeconfig in create and unsetting
    it in delete are unconditional. Making them optional is phase 3; this
    phase only has to preserve them.
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
        for target, release in [('get_k3s_release', 'stable'),
                                ('get_longhorn_release', '1.6.0')]:
            patcher = mock.patch(
                'shakenfist_client_k3s.primitives.%s' % target,
                return_value=release)
            patcher.start()
            self.addCleanup(patcher.stop)

        self.subprocess_run = mock.MagicMock()
        self.subprocess_run.return_value.returncode = 0
        patcher = mock.patch('subprocess.run', self.subprocess_run)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _cluster(self, client=None):
        return Cluster(client if client is not None else self.client,
                       'banana', 'testns', reporter=self.reporter)


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
        c = self._cluster()

        captured = io.StringIO()
        with mock.patch('sys.stdout', captured):
            c.create(1, 1, 1)

            md = c.show()
            self.assertEqual('created', md['state'])
            self.assertEqual('banana', md['name'])

            kubeconfig = c.get_kubeconfig()
            self.assertIn('banana.testns', kubeconfig)

            c.expand_workers(1)
            c.expand_addresses(1)
            c.update_os()
            c.delete()

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

    def test_create_writes_the_local_kubeconfig(self):
        # Writing ~/.kube/config is mandatory today, and this step is a
        # move rather than a redesign, so it stays mandatory. Phase 3 makes
        # it optional.
        c = self._cluster()
        c.create(1, 1, 1)

        with open(os.path.join(self.home, '.kube', 'config')) as f:
            kc = yaml.safe_load(f)
        self.assertEqual('banana.testns', kc['current-context'])

    def test_delete_unsets_the_local_kubeconfig_entries(self):
        # Likewise the three kubectl config unset calls in delete.
        c = self._cluster()
        c.create(1, 1, 1)
        self.subprocess_run.reset_mock()
        c.delete()

        self.assertEqual(
            ['kubectl config unset users.banana.testns',
             'kubectl config unset contexts.banana.testns',
             'kubectl config unset clusters.banana.testns'],
            [call[0][0] for call in self.subprocess_run.call_args_list])

    def test_delete_raises_when_kubectl_unset_fails(self):
        c = self._cluster()
        c.create(1, 1, 1)
        self.subprocess_run.return_value.returncode = 1

        e = self.assertRaises(exceptions.KubeconfigError, c.delete)
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
    create's output. The arithmetic depends on two of create's arguments,
    and moving it out of the Click layer is exactly where it could have
    been lost.
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
        # Eight phases, plus one for creating the node network.
        self._assert_phases(9, 1, 1, 1)

    def test_supplied_network_removes_a_phase(self):
        self._assert_phases(8, 1, 1, 1, network='net-1')

    def test_extra_control_plane_nodes_add_a_phase(self):
        self._assert_phases(10, 2, 1, 1)

    def test_both_adjustments_together(self):
        self._assert_phases(9, 2, 1, 1, network='net-1')


class ClusterAccessorTestCase(testtools.TestCase):
    """show() and get_kubeconfig(), which return rather than print."""

    def _cluster(self, namespace_md):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = namespace_md
        return Cluster(client, 'banana', 'testns',
                       reporter=progress.CollectingReporter())

    def test_show_returns_the_metadata(self):
        md = {'name': 'banana', 'state': 'created'}
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
