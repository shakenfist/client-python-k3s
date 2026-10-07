import ast
import collections
import copy
import datetime
import io
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time

# The PyPI mock backport is used for consistency with the other tests in
# this package, which support Python >= 3.7.
import mock
from shakenfist_client import apiclient
import testtools
import yaml

from shakenfist_client_k3s import cluster as cluster_module
from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress
from shakenfist_client_k3s.cluster import Cluster
from shakenfist_client_k3s.tests import fakes


MD_KEY = cluster_module.METADATA_KEY % 'banana'


def _make_cluster(client):
    return Cluster(client, 'banana', 'testns',
                   reporter=progress.CollectingReporter())


class ClusterMetadataTestCase(testtools.TestCase):
    """The cluster metadata cache, whose semantics are load bearing.

    Cluster metadata lives in a single namespace metadata document which
    conductor also writes, so how many times an operation reads it is
    behaviour rather than an implementation detail: every read is a chance
    to pick up a stale copy and write back over someone else's update.
    """

    def test_metadata_is_fetched_once(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: {'name': 'banana'}}
        cluster = _make_cluster(client)

        self.assertEqual({'name': 'banana'}, cluster.get_metadata())
        self.assertEqual({'name': 'banana'}, cluster.get_metadata())

        client.get_namespace_metadata.assert_called_once_with('testns')

    def test_absent_metadata_is_cached_as_none(self):
        # A cluster which does not exist must cache the miss. Create asks
        # before it writes, and re-reading here would both cost an extra
        # API call and widen the window in which conductor's copy of the
        # namespace metadata can diverge from ours.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {}
        cluster = _make_cluster(client)

        self.assertIsNone(cluster.get_metadata())
        self.assertIsNone(cluster.get_metadata())

        self.assertEqual(1, client.get_namespace_metadata.call_count)

    def test_set_metadata_writes_through(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {}
        cluster = _make_cluster(client)

        cluster.set_metadata({'name': 'banana', 'state': 'created'})

        client.set_namespace_metadata_item.assert_called_once_with(
            'testns', MD_KEY, {'name': 'banana', 'state': 'created'})

        # The write populates the cache, so a subsequent read is free and
        # returns what we just wrote rather than the API's older copy.
        self.assertEqual({'name': 'banana', 'state': 'created'}, cluster.get_metadata())
        client.get_namespace_metadata.assert_not_called()

    def test_delete_metadata_removes_from_cache_and_api(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: {'name': 'banana'}}
        cluster = _make_cluster(client)

        cluster.get_metadata()
        cluster.delete_metadata()

        client.delete_namespace_metadata_item.assert_called_once_with('testns', MD_KEY)

        # The cache entry is gone too, so the next read goes back to the API.
        client.get_namespace_metadata.return_value = {}
        self.assertIsNone(cluster.get_metadata())
        self.assertEqual(2, client.get_namespace_metadata.call_count)

    def test_delete_metadata_with_cold_cache_raises(self):
        # Deleting metadata which was never read is a caller bug: delete
        # reads and updates the metadata before removing it, so a cold
        # cache here means the caller is confused. This raised KeyError
        # when the cache lived in ctx.obj, and must continue to.
        client = mock.MagicMock()
        cluster = _make_cluster(client)

        self.assertRaises(KeyError, cluster.delete_metadata)
        client.delete_namespace_metadata_item.assert_not_called()


class ClusterProgressTestCase(testtools.TestCase):
    def test_default_reporter_is_stdout_backed(self):
        cluster = Cluster(mock.MagicMock(), 'banana', 'testns')
        self.assertIsInstance(cluster.reporter, progress.Reporter)
        self.assertFalse(cluster.reporter.verbose)

    def test_progress_is_created_on_demand_and_reused(self):
        reporter = progress.CollectingReporter()
        cluster = Cluster(mock.MagicMock(), 'banana', 'testns', reporter=reporter)

        p = cluster.get_progress()
        self.assertIs(reporter, p.stream)
        self.assertFalse(p.interactive)
        self.assertIs(p, cluster.get_progress())
        self.assertIs(p, cluster.progress)

    def test_assigned_progress_is_used(self):
        cluster = Cluster(mock.MagicMock(), 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        p = progress.Progress(total_phases=3, stream=cluster.reporter)
        cluster.progress = p
        self.assertIs(p, cluster.get_progress())

    def test_lazy_default_carries_a_real_total_phases(self):
        # A library caller invoking a mid-level method directly (rather
        # than going through an entry point like create() or
        # expand_workers(), which build their own Progress with the real
        # count) gets this lazy default. 1 is the honest count for it: see
        # get_progress()'s docstring for which callers this serves and why
        # each of them opens exactly one phase.
        reporter = progress.CollectingReporter()
        cluster = Cluster(mock.MagicMock(), 'banana', 'testns', reporter=reporter)

        p = cluster.get_progress()
        self.assertEqual(1, p.total_phases)

        # The number is not just stored but used: the phase header it
        # produces is numbered "[1/1]", not the un-numbered "[1]" a caller
        # got before total_phases existed here.
        p.phase('Doing a thing')
        self.assertEqual(['[1/1] Doing a thing'], reporter.lines)

    def test_reporter_verbosity_selects_the_output_mode(self):
        # A verbose reporter's debug lines would interleave badly with in
        # place cursor updates, which is why Progress refuses interactive
        # mode when verbose. Driven from a terminal, the Cluster must pass
        # both the verbosity and the terminal through, because between
        # them they choose the CLI's entire output format.
        with mock.patch('sys.stdout', fakes.FakeTty()):
            quiet = Cluster(mock.MagicMock(), 'banana', 'testns',
                            reporter=progress.Reporter(verbose=False))
            self.assertTrue(quiet.get_progress().interactive)

            noisy = Cluster(mock.MagicMock(), 'banana', 'testns',
                            reporter=progress.Reporter(verbose=True))
            self.assertFalse(noisy.get_progress().interactive)


class InstallK3sComponentTestCase(testtools.TestCase):
    def _install_commands(self, md):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: md}
        cluster = Cluster(client, 'banana', 'testns')
        with mock.patch.object(Cluster, 'execute_and_await') as ea:
            cluster.install_k3s_component(['uuid-001'], 'token', 'agent')
            return '\n'.join(ea.call_args[0][1])

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


class AllocateMetallbAddressesTestCase(testtools.TestCase):
    def _allocate(self, route_results, count):
        stream = io.StringIO()
        client = mock.MagicMock()
        client.get_network.return_value = {'uuid': 'net-1'}
        client.route_network_address.side_effect = route_results
        md = {'name': 'banana', 'node_network': 'net-1',
              'routed_addresses': ['192.168.10.1']}
        client.get_namespace_metadata.return_value = {MD_KEY: md}
        cluster = Cluster(client, 'banana', 'testns')
        cluster.progress = progress.Progress(stream=stream)
        cluster.allocate_metallb_addresses(count)
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

    def test_a_non_address_from_the_api_is_refused_before_it_is_recorded(self):
        """Rule 1 puts the API outside this package's trust boundary.

        Not paranoia about our own server so much as the rule applied
        where it happens to point at it: whatever reaches
        ``routed_addresses`` is interpolated into a YAML body written on
        a node, and the metadata document is the one place a bad value
        would persist and be used again by a later run. Refusing it here
        is the only point at which it can be stopped from entering the
        document at all.
        """
        stream = io.StringIO()
        client = mock.MagicMock()
        client.get_network.return_value = {'uuid': 'net-1'}
        client.route_network_address.side_effect = [
            '192.168.10.2', '10.0.0.1\nEOF\ntouch /pwned']
        client.get_namespace_metadata.return_value = {MD_KEY: {
            'name': 'banana', 'node_network': 'net-1',
            'routed_addresses': ['192.168.10.1']}}
        cluster = Cluster(client, 'banana', 'testns')
        cluster.progress = progress.Progress(stream=stream)

        e = self.assertRaises(exceptions.ClusterMetadataError,
                              cluster.allocate_metallb_addresses, 2)

        self.assertEqual('not_an_address', e.reason)
        self.assertEqual('routed_addresses', e.key)
        self.assertEqual('banana', e.name)
        self.assertIn('is not an IP address', str(e))
        # Nothing was written, so a later run does not find the value
        # waiting for it.
        self.assertEqual([], client.set_namespace_metadata_item.mock_calls)


class ExpandWorkersTestCase(testtools.TestCase):
    """Expanding a cluster installs k3s on the new workers and no others.

    The k3s agent installer is not idempotent in a way that is safe to
    rely on: re-running it on a worker which is already carrying
    workloads restarts the agent on a live node. An expand which hands
    install_workers() the whole of md['worker_nodes'] therefore damages
    every node it did not create, which is why the argument exists.

    It also builds the new workers at the size the cluster recorded for
    them, rather than at whatever the default is today, and at exactly the
    default for a cluster created before sizes were recorded -- which is
    the size every one of that cluster's nodes was built at. And it writes
    the cluster's recorded agent_config onto them, or no drop-in at all for
    a cluster which has none.
    """

    def _expand(self, existing_workers, worker_count, new_instances,
                node_sizes=None):
        md = {
            'name': 'banana',
            'namespace': 'testns',
            'state': 'created',
            'node_serial': len(existing_workers) + 1,
            'node_network': 'net-1',
            'node_token': 'node-token',
            'control_plane_nodes': ['uuid-cp-001'],
            'worker_nodes': list(existing_workers)
        }
        if node_sizes is not None:
            md['node_sizes'] = node_sizes
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: md}
        client.create_instance.side_effect = list(new_instances)
        cluster = _make_cluster(client)

        with mock.patch.object(Cluster, 'await_boot'), \
                mock.patch.object(Cluster, 'instance_os_update'), \
                mock.patch.object(Cluster, 'install_k3s_component') as ikc:
            cluster.expand_workers(worker_count)

        return cluster, ikc

    def test_only_the_new_worker_has_k3s_installed(self):
        cluster, ikc = self._expand(
            ['uuid-w-001', 'uuid-w-002', 'uuid-w-003'], 1,
            [{'uuid': 'uuid-w-004', 'name': 'k3s-banana-node-004'}])

        # Exactly the one instance we just created, not all four.
        ikc.assert_called_once_with(['uuid-w-004'], 'node-token', 'agent')

        # The cluster still knows about all four of its workers though.
        self.assertEqual(
            ['uuid-w-001', 'uuid-w-002', 'uuid-w-003', 'uuid-w-004'],
            cluster.get_metadata()['worker_nodes'])

    def test_every_new_worker_has_k3s_installed(self):
        _, ikc = self._expand(
            ['uuid-w-001'], 2,
            [{'uuid': 'uuid-w-002', 'name': 'k3s-banana-node-002'},
             {'uuid': 'uuid-w-003', 'name': 'k3s-banana-node-003'}])

        ikc.assert_called_once_with(
            ['uuid-w-002', 'uuid-w-003'], 'node-token', 'agent')

    def _built_sizes(self, cluster):
        """(cpus, memory, disk) for every create_instance() call, in order."""
        return [(c.args[1], c.args[2], c.args[4][0]['size'])
                for c in cluster.client.create_instance.call_args_list]

    def test_new_workers_are_built_at_the_recorded_worker_size(self):
        # The control plane size is deliberately different, so that a
        # lookup of the wrong role fails here rather than passing by
        # coincidence.
        cluster, _ = self._expand(
            ['uuid-w-001'], 2,
            [{'uuid': 'uuid-w-002', 'name': 'k3s-banana-node-002'},
             {'uuid': 'uuid-w-003', 'name': 'k3s-banana-node-003'}],
            node_sizes={
                'control_plane': {'cpus': 8, 'memory': 16384, 'disk': 200},
                'worker': {'cpus': 4, 'memory': 6144, 'disk': 80}})

        self.assertEqual([(4, 6144, 80), (4, 6144, 80)],
                         self._built_sizes(cluster))

    def test_a_cluster_without_recorded_sizes_builds_at_the_default(self):
        # Every cluster created before node_sizes existed. The fallback is
        # what those clusters were built at, not a guess: there was no way
        # to build a node at any other size. The literals are spelled out
        # rather than read from DEFAULT_NODE_SIZE, so that changing the
        # default is a decision this test makes somebody notice.
        cluster, _ = self._expand(
            ['uuid-w-001'], 1,
            [{'uuid': 'uuid-w-002', 'name': 'k3s-banana-node-002'}])

        self.assertNotIn('node_sizes', cluster.get_metadata())
        self.assertEqual([(2, 2048, 50)], self._built_sizes(cluster))

    def test_a_partial_record_falls_back_per_field(self):
        # The plugin never writes this shape -- create() records every
        # field for both roles -- but namespace metadata can be edited by
        # anything with the namespace's credentials. A recorded field is
        # used, a missing one comes from the default, and a missing role
        # is the default whole, rather than a KeyError mid-expand.
        cluster, _ = self._expand(
            ['uuid-w-001'], 1,
            [{'uuid': 'uuid-w-002', 'name': 'k3s-banana-node-002'}],
            node_sizes={'worker': {'cpus': 4}})

        self.assertEqual([(4, 2048, 50)], self._built_sizes(cluster))
        self.assertEqual(
            {'cpus': 2, 'memory': 2048, 'disk': 50},
            cluster._node_size(cluster.get_metadata(), 'control_plane'))

    def _expand_for_real(self, md_extra):
        """Expand by one worker with install_k3s_component() left in; return the new worker's commands.

        The tests above patch install_k3s_component() out, because their
        question is which instances it is handed. These ask what those
        instances are sent, so they keep it and drive a scripted fake.
        """
        client = fakes.FakeClusterClient()
        client.instance_serial = 2
        md = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 3, 'node_network': 'net-1',
            'node_token': 'node-token', 'server_token': 'server-token',
            'k3s_version': 'v1.33', 'api_address_floating': '192.168.10.100',
            'api_address_inner': '10.0.0.4', 'join_address': '10.0.0.4',
            'control_plane_nodes': ['inst-001'], 'worker_nodes': ['inst-002'],
            'routed_addresses': []
        }
        md.update(md_extra)
        client.metadata[MD_KEY] = md
        for instance_uuid in md['control_plane_nodes'] + md['worker_nodes']:
            client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': 'k3s-banana-' + instance_uuid,
                'state': 'created', 'agent_state': 'ready'}

        with mock.patch('time.sleep', lambda seconds: None):
            _make_cluster(client).expand_workers(1)

        self.assertEqual(['inst-001', 'inst-002', 'inst-003'],
                         sorted(client.instances))
        return [commandline for instance_uuid, commandline in client.executed
                if instance_uuid == 'inst-003']

    def test_new_workers_are_given_the_recorded_agent_config(self):
        # expand_workers() is not changed to make this happen: it reaches
        # install_k3s_component() through install_workers(), which reads
        # the recorded agent_config by role. The server_config is recorded
        # too, and must not reach a worker.
        agent_config = {'node-label': ['openvswitch=enabled']}
        files = _k3s_config_files(self._expand_for_real({
            'agent_config': agent_config,
            'server_config': {'disable': ['traefik']}}))
        self.assertEqual(agent_config,
                         yaml.safe_load(files[K3S_CALLER_DROP_IN]))
        self.assertEqual(K3S_AGENT_CONFIG_BODY, files[K3S_CONFIG])
        self.assertNotIn(K3S_ENFORCED_DROP_IN, files)

    def test_a_cluster_without_an_agent_config_gives_new_workers_no_drop_in(self):
        # Every cluster created before agent_config was recorded. Such a
        # cluster could not have been given one, so no drop-in is exact,
        # and the new worker still gets the plugin's config.yaml.
        files = _k3s_config_files(self._expand_for_real({}))
        self.assertEqual([K3S_CONFIG], list(files))


class CreateInstallsWorkersTestCase(testtools.TestCase):
    """Creating a cluster installs k3s on every worker it just created.

    create() hands install_workers() md['worker_nodes'], and when this test
    was written in step 3a that was the right list only by aliasing:
    get_metadata() caches the metadata dictionary and set_metadata() stores
    that same object, so the list create() was holding was the one
    create_and_await_instances() had appended the new workers to. Nothing
    at the call site said so, and making the cache copy on read would have
    had create() install k3s on no workers at all and still report the
    cluster ready.

    Step 3c closed that: create() now picks the metadata up again after
    each method which writes to it, so this assertion no longer rests on
    the cache's identity semantics and holds whether the cache aliases or
    copies. What is pinned here is the outcome rather than the mechanism --
    the workers create() built are the workers it installs k3s on -- which
    is the claim worth keeping either way.

    A scripted fake rather than a MagicMock because create() drives wait
    loops which compare dictionary values against literals; see
    tests/fakes.py.
    """

    def setUp(self):
        super(CreateInstallsWorkersTestCase, self).setUp()
        self.client = fakes.FakeClusterClient()

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        # create() only writes ~/.kube/config when asked, which these tests
        # do not do, but a test which grew that side effect back must not
        # write the operator's own.
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

        # The release lookups reach the internet, and have their own tests.
        # A real release rather than a channel name, because create()
        # refuses one it cannot parse (check_k3s_release()).
        for target, release in [('get_k3s_release', 'v1.33.4+k3s1'),
                                ('get_longhorn_release', '1.6.0')]:
            patcher = mock.patch(
                'shakenfist_client_k3s.primitives.%s' % target,
                return_value=release)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _create(self, control_plane_count, worker_count):
        cluster = _make_cluster(self.client)
        with mock.patch.object(Cluster, 'install_k3s_component') as ikc:
            cluster.create(control_plane_count, worker_count, 1)

        # Which instances ought to have had an agent installed on them is
        # worked out from the fake, which numbers instances in creation
        # order, and from create()'s own ordering: control plane nodes
        # first, then workers. Reading it out of the metadata instead would
        # ask the aliasing under test to vouch for itself.
        created = list(self.client.instances)
        control_plane = created[:control_plane_count]
        workers = created[control_plane_count:control_plane_count + worker_count]

        # install_k3s_component installs the control plane as well as the
        # workers, so the role argument is what separates them.
        agent_calls = [call for call in ikc.call_args_list
                       if call[0][2] == 'agent']
        return control_plane, workers, agent_calls

    def _assert_installed_on(self, expected_workers, agent_calls):
        self.assertEqual(
            1, len(agent_calls),
            'create() must install the k3s agent exactly once, for the %d '
            'workers it created' % len(expected_workers))
        installed_on = list(agent_calls[0][0][0])
        self.assertEqual(
            expected_workers, installed_on,
            'create() installed the k3s agent on %s, but the workers it '
            'created were %s' % (installed_on or 'no instances at all',
                                 expected_workers))
        return installed_on

    def test_k3s_is_installed_on_the_workers_which_were_created(self):
        control_plane, workers, agent_calls = self._create(1, 2)
        installed_on = self._assert_installed_on(workers, agent_calls)

        for instance_uuid in control_plane:
            self.assertNotIn(
                instance_uuid, installed_on,
                'control plane node %s must not have a k3s agent installed '
                'on it' % instance_uuid)

    def test_a_single_worker_cluster_still_installs_its_worker(self):
        _, workers, agent_calls = self._create(1, 1)
        self._assert_installed_on(workers, agent_calls)


class ActionLogClient(fakes.FakeClusterClient):
    """A scripted client which records executes and deletes in one ordered log.

    The assertion this class exists for -- that a worker is drained and
    removed from k3s before its instance is destroyed -- cannot be made
    from two separate call lists, because neither of them knows where in
    the other its own calls fell. One log, in the order the client was
    asked, is the only shape which can answer "before".

    Named for what it does rather than "RecordingClient", which
    test_library_api.py already uses for a differently shaped fake in the
    same test package: two classes sharing a name is one grep away from
    being read as one class.
    """

    def __init__(self):
        super(ActionLogClient, self).__init__()
        self.actions = []

    def instance_execute(self, instance_ref, commandline):
        self.actions.append(('execute', instance_ref, commandline))
        return super(ActionLogClient, self).instance_execute(
            instance_ref, commandline)

    def delete_instance(self, instance_ref):
        self.actions.append(('delete_instance', instance_ref, None))
        return super(ActionLogClient, self).delete_instance(instance_ref)


class DeleteClearsTheKeysCreateWroteTestCase(testtools.TestCase):
    """delete() clears the address keys create() actually writes.

    It cleared api_floating_address and api_inner_address, while create()
    writes api_address_floating and api_address_inner and the two install
    methods read those -- the words transposed. So the write in the middle
    of delete() invented two keys nothing in the package has ever used and
    carried the two real ones through unchanged.

    The effect was cosmetic, because the document is deleted a few lines
    later, and it stayed invisible for the same reason: nothing reads the
    intermediate write, so nothing noticed. These assertions read it.
    """

    def _delete_and_capture(self):
        """Run a delete, returning every metadata document it wrote."""
        written = []

        class CapturingClient(ActionLogClient):
            def set_namespace_metadata_item(self, namespace, key, value):
                if key == MD_KEY:
                    written.append(copy.deepcopy(value))
                return super(CapturingClient, self).\
                    set_namespace_metadata_item(namespace, key, value)

        client = CapturingClient()
        client.metadata[primitives.CLUSTER_LIST] = ['banana']
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 2, 'node_network': 'net-1',
            'node_token': 'node-token', 'k3s_version': 'v1.33',
            'api_address_inner': '10.0.0.4',
            'api_address_floating': '192.168.10.100',
            'kubeconfig': 'apiVersion: v1\n',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
            'routed_addresses': []}
        client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        with mock.patch('time.sleep', lambda seconds: None):
            _make_cluster(client).delete()

        self.assertNotEqual([], written)
        return written

    def test_the_address_keys_are_the_ones_create_writes(self):
        for md in self._delete_and_capture():
            self.assertNotIn('api_floating_address', md)
            self.assertNotIn('api_inner_address', md)

        # And the real ones were actually cleared, which is the half the
        # transposition was costing.
        self.assertIsNone(self._delete_and_capture()[-1]['api_address_inner'])
        self.assertIsNone(
            self._delete_and_capture()[-1]['api_address_floating'])

    def test_the_network_key_is_cleared_to_none_not_a_list(self):
        """Everywhere else this key is a network uuid string."""
        for md in self._delete_and_capture():
            self.assertNotEqual([], md.get('node_network'))
        self.assertIsNone(self._delete_and_capture()[-1]['node_network'])


class DeleteNodeNetworkOwnershipTestCase(testtools.TestCase):
    """delete() destroys the node network only if create() allocated it.

    A network handed to create --network is borrowed, and destroying it
    with the cluster took down whatever else was on it
    (shakenfist/client-python-k3s#41). create() now records which it was as
    node_network_created; a cluster built before that has no record, and
    is classified by the network's name, because create() has always named
    the network it allocates k3s-<cluster>-node. The end to end halves,
    through create(), are in test_library_api.
    """

    def _delete(self, network=None, recorded=None):
        """Delete a seeded cluster, returning the client it was deleted with.

        network is what get_network() answers for the node network, or None
        for a network the API no longer has. recorded is the
        node_network_created value, or None for a cluster created before
        the key existed.
        """
        class NetworkClient(ActionLogClient):
            def get_network(self, network_ref):
                self.network_lookups.append(network_ref)
                if network is None:
                    raise fakes.not_found(network_ref)
                return network

        client = NetworkClient()
        client.network_lookups = []
        client.metadata[primitives.CLUSTER_LIST] = ['banana']
        md = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 1, 'node_network': 'net-1', 'node_token': None,
            'control_plane_nodes': [], 'worker_nodes': [],
            'routed_addresses': ['192.168.10.1']}
        if recorded is not None:
            md['node_network_created'] = recorded
        client.metadata[MD_KEY] = md

        with mock.patch('time.sleep', lambda seconds: None):
            _make_cluster(client).delete()

        # Whichever way the network went, the cluster's addresses are given
        # back and the cluster itself is gone.
        self.assertEqual([('net-1', '192.168.10.1')],
                         client.unrouted_addresses)
        self.assertNotIn(MD_KEY, client.metadata)
        return client

    def test_a_recorded_created_network_is_deleted_without_a_lookup(self):
        client = self._delete(recorded=True)
        self.assertEqual(['net-1'], client.deleted_networks)
        self.assertEqual([], client.network_lookups)

    def test_a_recorded_borrowed_network_is_kept_whatever_its_name(self):
        # Named exactly as create would have named its own: the record is
        # what decides, not the name.
        client = self._delete(
            network={'uuid': 'net-1', 'name': 'k3s-banana-node'},
            recorded=False)
        self.assertEqual([], client.deleted_networks)
        self.assertEqual([], client.network_lookups)

    def test_an_older_cluster_with_create_s_network_name_is_deleted(self):
        client = self._delete(
            network={'uuid': 'net-1', 'name': 'k3s-banana-node'})
        self.assertEqual(['net-1'], client.deleted_networks)

    def test_an_older_cluster_on_any_other_network_keeps_it(self):
        for name in ('shared', 'k3s-apple-node', None):
            client = self._delete(network={'uuid': 'net-1', 'name': name})
            self.assertEqual([], client.deleted_networks, name)

    def test_an_older_cluster_whose_network_is_gone_still_deletes(self):
        client = self._delete(network=None)
        self.assertEqual([], client.deleted_networks)


class RemoveWorkerTestCase(testtools.TestCase):
    """remove-worker drains a worker out of k3s before it destroys it.

    Deleting the Shaken Fist instance first leaves a NotReady node object
    in the cluster forever and strands the pods which were running on it
    until the node controller's eviction timeout expires, so the order of
    those two operations is the verb's entire safety argument (decision 3
    of the phase 3 plan). The other half is that a run which is going to
    fail fails before it has destroyed anything.
    """

    def setUp(self):
        super(RemoveWorkerTestCase, self).setUp()
        self.client = ActionLogClient()

        self.md = {
            'name': 'banana',
            'namespace': 'testns',
            'state': 'created',
            'node_serial': 5,
            'node_network': 'net-1',
            'node_token': 'node-token',
            'k3s_version': 'v1.33',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'],
            'worker_nodes': ['inst-w1', 'inst-w2', 'inst-w3'],
            'routed_addresses': []
        }
        self.client.metadata[MD_KEY] = self.md

        # The nodes the seeded metadata claims exist. The instance names
        # matter: the k3s node name is the instance's hostname, which
        # Shaken Fist derives from the instance name, so these are the
        # names the drain has to use.
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

        self.cluster = _make_cluster(self.client)

    def _index_of(self, predicate, description):
        for i, action in enumerate(self.client.actions):
            if predicate(action):
                return i
        self.fail('%s never happened. The client was asked to:\n    %s'
                  % (description,
                     '\n    '.join(repr(a) for a in self.client.actions)
                     or '(nothing at all)'))

    def test_a_worker_is_drained_and_removed_before_it_is_deleted(self):
        self.cluster.remove_worker(['inst-w2'])

        drain = self._index_of(
            lambda a: a[0] == 'execute' and a[2].startswith('kubectl drain'),
            'the drain of k3s-banana-node-003')
        delete_node = self._index_of(
            lambda a: a[0] == 'execute' and a[2].startswith('kubectl delete node'),
            'the k3s node deletion of k3s-banana-node-003')
        delete_instance = self._index_of(
            lambda a: a[0] == 'delete_instance',
            'the deletion of instance inst-w2')

        self.assertTrue(
            drain < delete_instance,
            'the instance was deleted before the node was drained, which '
            'strands its pods until the eviction timeout expires. The client '
            'was asked to:\n    %s'
            % '\n    '.join(repr(a) for a in self.client.actions))
        self.assertTrue(
            delete_node < delete_instance,
            'the instance was deleted before the node was removed from k3s, '
            'which leaves a NotReady node object behind forever. The client '
            'was asked to:\n    %s'
            % '\n    '.join(repr(a) for a in self.client.actions))

    def test_the_commands_name_the_node_and_run_on_the_control_plane(self):
        self.cluster.remove_worker(['inst-w2'])

        self.assertEqual(
            [('execute', 'inst-cp1',
              'kubectl drain k3s-banana-node-003 --ignore-daemonsets '
              '--delete-emptydir-data --timeout=300s '
              '--kubeconfig /etc/rancher/k3s/k3s.yaml'),
             ('execute', 'inst-cp1',
              'kubectl delete node k3s-banana-node-003 '
              '--kubeconfig /etc/rancher/k3s/k3s.yaml'),
             ('delete_instance', 'inst-w2', None)],
            self.client.actions)

    def test_the_survivors_keep_their_original_order(self):
        self.cluster.remove_worker(['inst-w2'])

        self.assertEqual(['inst-w1', 'inst-w3'],
                         self.client.metadata[MD_KEY]['worker_nodes'])

    def test_several_workers_are_removed_in_the_order_they_were_asked_for(self):
        self.cluster.remove_worker(['inst-w3', 'inst-w1'])

        self.assertEqual(
            [('delete_instance', 'inst-w3', None),
             ('delete_instance', 'inst-w1', None)],
            [a for a in self.client.actions if a[0] == 'delete_instance'])
        self.assertEqual(['inst-w2'],
                         self.client.metadata[MD_KEY]['worker_nodes'])

    def test_a_repeated_uuid_removes_that_worker_once(self):
        self.cluster.remove_worker(['inst-w2', 'inst-w2'])

        self.assertEqual(
            [('delete_instance', 'inst-w2', None)],
            [a for a in self.client.actions if a[0] == 'delete_instance'])
        self.assertEqual(['inst-w1', 'inst-w3'],
                         self.client.metadata[MD_KEY]['worker_nodes'])

    def test_an_unknown_uuid_raises_before_anything_is_destroyed(self):
        # The typo is deliberately last: the point of validating the whole
        # list up front is that the two good uuids in front of it are
        # untouched when it fails.
        self.assertRaises(
            exceptions.WorkerNotFoundError, self.cluster.remove_worker,
            ['inst-w1', 'inst-w2', 'inst-w9'])

        self.assertEqual(
            [], self.client.actions,
            'remove_worker() drained or deleted something despite being '
            'given a worker uuid which is not in this cluster. It was asked '
            'to:\n    %s'
            % '\n    '.join(repr(a) for a in self.client.actions))
        self.assertEqual(['inst-w1', 'inst-w2', 'inst-w3'],
                         self.client.metadata[MD_KEY]['worker_nodes'])

    def test_the_error_names_every_uuid_which_did_not_match(self):
        e = self.assertRaises(
            exceptions.WorkerNotFoundError, self.cluster.remove_worker,
            ['inst-w9', 'inst-w8'])

        self.assertEqual(['inst-w9', 'inst-w8'], e.instance_uuids)
        self.assertIn('inst-w9', str(e))
        self.assertIn('inst-w8', str(e))
        self.assertIn('banana', str(e))

    def test_a_control_plane_node_is_not_a_worker(self):
        # Removing a control plane node is shrinking the control plane,
        # which the master plan puts out of scope. Asking for one by uuid
        # must not quietly work because the instance happens to exist.
        self.assertRaises(
            exceptions.WorkerNotFoundError, self.cluster.remove_worker,
            ['inst-cp1'])
        self.assertEqual([], self.client.actions)

    def test_the_last_worker_may_be_removed(self):
        # A k3s server node is schedulable, so a cluster with no workers is
        # still a working cluster, and conductor's ephemeral CI runners
        # legitimately go to zero. This is deliberately allowed.
        self.cluster.remove_worker(['inst-w1', 'inst-w2', 'inst-w3'])

        self.assertEqual([], self.client.metadata[MD_KEY]['worker_nodes'])

    def test_an_unknown_cluster_raises(self):
        client = ActionLogClient()
        cluster = _make_cluster(client)

        self.assertRaises(
            exceptions.ClusterNotFoundError, cluster.remove_worker,
            ['inst-w1'])


# A cluster which create() started and never finished: the metadata
# document exists and says 'initial', the name is in the namespace cluster
# list, and none of the things the later phases of a create record -- the
# node token, the api addresses, the kubeconfig -- are present at all. This
# is the exact shape create() writes at cluster.py's "Initialise the
# metadata", which is why the keys it does not write are absent here rather
# than present and None.
def _interrupted_md(state='initial', control_plane_nodes=None,
                    worker_nodes=None):
    md = {
        'name': 'banana',
        'namespace': 'testns',
        'type': 'k3s',
        'k3s_version': 'stable',
        'k3s_version_history': ['stable'],
        'plugin_version': '0.0.1',
        'state': state,
        'node_serial': 1,
        'node_network': 'net-1',
        'node_token': None,
        'control_plane_nodes': list(control_plane_nodes or []),
        'worker_nodes': list(worker_nodes or []),
        'routed_addresses': [],
        'ssh_key': None
    }
    if state is None:
        del md['state']
    return md


class InterruptedStateTestCase(testtools.TestCase):
    """Which recorded states mean "this cluster was never finished".

    md['state'] has been written since this package's first commit and read
    nowhere until now, so this is the first code which has an opinion about
    what its values mean. Everything else in this file depends on that
    opinion being the one written down in the phase 3 plan's decision 5.
    """

    def _state_of(self, md):
        return _make_cluster(mock.MagicMock())._interrupted_state(md)

    def test_a_finished_cluster_is_not_interrupted(self):
        self.assertIsNone(self._state_of(_interrupted_md(state='created')))

    def test_initial_is_interrupted(self):
        self.assertEqual('initial', self._state_of(_interrupted_md()))

    def test_deleted_is_interrupted(self):
        # delete() writes 'deleted' and then removes the metadata document,
        # so metadata which still says 'deleted' is a delete which died in
        # between. Rerunning delete is the way out of that too.
        self.assertEqual('deleted', self._state_of(_interrupted_md(state='deleted')))

    def test_metadata_with_no_state_is_unknown_rather_than_finished(self):
        # This package has always written the key, so a document without one
        # came from something else. Guessing "finished" would guess in the
        # direction which drives k3s installs at half built clusters.
        self.assertEqual('unknown', self._state_of(_interrupted_md(state=None)))


class CreateOverInterruptedClusterTestCase(testtools.TestCase):
    """create() tells an interrupted cluster apart from a finished one.

    Both used to be ClusterExistsError: "Sorry, that cluster name is
    already taken", which is true but useless, because the two have
    opposite answers. A finished cluster of that name is someone's working
    cluster and the caller should pick another name. An unfinished one is
    the wreckage of an earlier create, and the caller can delete it and try
    again -- which, per decision 5 of the phase 3 plan, is the only
    recovery there is, so the error has to say so.
    """

    def setUp(self):
        super(CreateOverInterruptedClusterTestCase, self).setUp()
        self.client = fakes.FakeClusterClient()
        # An interrupted create leaves the name in the namespace cluster
        # list as well as in its own metadata document: create() registers
        # the name before it writes anything else, which is what makes the
        # order of create()'s two guards matter.
        self.client.metadata[primitives.CLUSTER_LIST] = ['banana']

        # create() looks up the k3s release before it looks at the name,
        # and checks it against K3S_RELEASE_FLOOR, so this is a release
        # rather than a channel name.
        patcher = mock.patch(
            'shakenfist_client_k3s.primitives.get_k3s_release',
            return_value='v1.33.4+k3s1')
        patcher.start()
        self.addCleanup(patcher.stop)

        self.cluster = _make_cluster(self.client)

    def _create(self, md):
        self.client.metadata[MD_KEY] = md
        return self.cluster.create(1, 1, 1)

    def test_an_interrupted_cluster_raises_and_names_the_delete_command(self):
        e = self.assertRaises(
            exceptions.ClusterInterruptedError, self._create, _interrupted_md())

        self.assertEqual('mid_create', e.reason)
        self.assertEqual('banana', e.name)
        self.assertEqual('initial', e.state)

        message = str(e)
        self.assertIn("'sf-client k3s delete banana'", message)
        self.assertIn("'initial'", message)

    def test_nothing_is_built_over_the_wreckage(self):
        self.assertRaises(
            exceptions.ClusterInterruptedError, self._create, _interrupted_md())

        self.assertEqual({}, self.client.instances)
        self.assertEqual(_interrupted_md(), self.client.metadata[MD_KEY])

    def test_a_half_deleted_cluster_is_also_interrupted(self):
        e = self.assertRaises(
            exceptions.ClusterInterruptedError, self._create,
            _interrupted_md(state='deleted'))
        self.assertEqual('deleted', e.state)

    def test_a_finished_cluster_still_reports_that_the_name_is_taken(self):
        # The old error, unchanged, for the case it was always right about.
        e = self.assertRaises(
            exceptions.ClusterExistsError, self._create,
            _interrupted_md(state='created'))
        self.assertEqual('Sorry, that cluster name is already taken', str(e))

    def test_a_name_in_the_cluster_list_with_no_metadata_is_still_taken(self):
        # The second of create()'s two original checks. There is no
        # metadata to read a state out of, so there is nothing more useful
        # to say than that the name is taken.
        self.assertRaises(exceptions.ClusterExistsError, self.cluster.create,
                          1, 1, 1)


class DeleteInterruptedClusterTestCase(testtools.TestCase):
    """delete() is the way out of an interrupted create, so it has to work.

    create() now refuses a name whose metadata says the cluster was never
    finished and points at delete, which makes delete the only exit from
    that state (decision 5 of the phase 3 plan: detection and teardown, not
    resume). If delete failed on the same clusters create refuses, the name
    would be unusable forever.

    Reading the code found that it already does work, and that the three
    things which looked like they would break do not: md['kubeconfig'] is
    only ever written by delete, never read; an empty control_plane_nodes
    makes the instance loop a no-op rather than an IndexError; and
    'kubectl config unset' on an entry which was never written exits zero.
    These tests exist so that stays true, because it is true by accident
    rather than by design.
    """

    def setUp(self):
        super(DeleteInterruptedClusterTestCase, self).setUp()
        self.client = ActionLogClient()
        self.client.metadata[primitives.CLUSTER_LIST] = ['banana']

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        # delete() only runs its three 'kubectl config unset' calls when
        # asked, which these tests do not do; the mock stays so that a
        # regression there fails rather than edits the operator's own
        # ~/.kube/config.
        self.subprocess_run = mock.MagicMock()
        self.subprocess_run.return_value.returncode = 0
        patcher = mock.patch('subprocess.run', self.subprocess_run)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.reporter = progress.CollectingReporter()
        self.cluster = Cluster(self.client, 'banana', 'testns',
                               reporter=self.reporter)

    def _seed(self, md, instances=()):
        self.client.metadata[MD_KEY] = md
        for instance_uuid, name in instances:
            self.client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}

    def test_a_cluster_with_one_node_and_no_kubeconfig_is_deleted(self):
        # The shape an interrupted create most often leaves: the control
        # plane node was made, and nothing after it happened, so there is no
        # node token, no api address and no 'kubeconfig' key at all.
        md = _interrupted_md(control_plane_nodes=['inst-001'])
        self.assertNotIn('kubeconfig', md)
        self._seed(md, [('inst-001', 'k3s-banana-node-001')])

        self.cluster.delete()

        self.assertEqual([('delete_instance', 'inst-001', None)],
                         self.client.actions)
        self.assertEqual(['net-1'], self.client.deleted_networks)

        # Both halves of the cluster's record are gone, which is what makes
        # the name usable again.
        self.assertNotIn(MD_KEY, self.client.metadata)
        self.assertNotIn(primitives.CLUSTER_LIST, self.client.metadata)

    def test_a_cluster_with_no_nodes_at_all_is_deleted(self):
        # Interrupted between writing the metadata and creating the first
        # instance, so both node lists are empty.
        self._seed(_interrupted_md())

        self.cluster.delete()

        self.assertEqual([], self.client.actions)
        self.assertNotIn(MD_KEY, self.client.metadata)
        self.assertNotIn(primitives.CLUSTER_LIST, self.client.metadata)

    def test_the_operator_is_told_the_cluster_never_finished(self):
        self._seed(_interrupted_md(control_plane_nodes=['inst-001']),
                   [('inst-001', 'k3s-banana-node-001')])

        self.cluster.delete()

        out = self.reporter.getvalue()
        self.assertIn("state 'initial'", out)
        self.assertIn('never finished being built', out)

    def test_a_finished_cluster_is_deleted_without_the_note(self):
        # The normal case must be unchanged: nothing new is printed for a
        # cluster which reached 'created'.
        self._seed(_interrupted_md(state='created',
                                   control_plane_nodes=['inst-001']),
                   [('inst-001', 'k3s-banana-node-001')])

        self.cluster.delete()

        self.assertEqual('', self.reporter.getvalue())
        self.assertNotIn(MD_KEY, self.client.metadata)


class ShowReportsStateTestCase(testtools.TestCase):
    """show() reports the cluster state, and says what an unusable one means.

    The state has always been one of the keys show() returns, because show
    returns the whole metadata document. What it did not do was read it:
    'state = initial' sat in a screenful of key/value pairs with nothing to
    say that it was the line which mattered, or that the answer to it is a
    delete.
    """

    def _show(self, md):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: md}
        reporter = progress.CollectingReporter()
        cluster = Cluster(client, 'banana', 'testns', reporter=reporter)
        return cluster.show(), reporter.getvalue()

    def test_the_state_is_in_the_returned_metadata(self):
        md, _ = self._show(_interrupted_md())
        self.assertEqual('initial', md['state'])

    def test_an_interrupted_cluster_is_called_out_and_the_way_out_named(self):
        _, out = self._show(_interrupted_md())
        self.assertIn("state 'initial'", out)
        self.assertIn("'sf-client k3s delete banana'", out)

    def test_a_finished_cluster_says_nothing_new(self):
        md, out = self._show(_interrupted_md(state='created'))
        self.assertEqual('created', md['state'])
        self.assertEqual('', out)

    def test_show_does_not_refuse_an_interrupted_cluster(self):
        # show is the verb for looking at a cluster which is not working.
        # Raising here would take away the only tool which can say why.
        md, _ = self._show(_interrupted_md(state='deleted'))
        self.assertEqual('banana', md['name'])


class ShowReportsNodeSizesTestCase(testtools.TestCase):
    """show() reports node sizes for every cluster, including ones which never recorded them.

    A cluster created before node_sizes existed has no such key, and show()
    fills it in from DEFAULT_NODE_SIZE. That, and the matching fill for
    server_config and agent_config (ShowReportsK3sConfigTestCase), are the
    only places show reports something other than what is stored, and they
    are allowed to because the filled in value is a fact: before the key
    existed, every node was built at the default. What it must not do is
    make that fact true by writing it: show is read only, and a show which
    rewrote the document would be a metadata write racing conductor's on
    every look at a cluster.
    """

    def _show(self, md):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: md}
        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        return cluster.show(), client

    def test_a_cluster_without_sizes_reports_the_defaults(self):
        # Literals rather than DEFAULT_NODE_SIZE, as in ExpandWorkersTestCase:
        # what an old cluster was built at does not change if the default
        # does, and this is the test which should notice.
        shown, _ = self._show(_interrupted_md(state='created'))
        self.assertEqual(
            {'control_plane': {'cpus': 2, 'memory': 2048, 'disk': 50},
             'worker': {'cpus': 2, 'memory': 2048, 'disk': 50}},
            shown['node_sizes'])

    def test_the_fallback_is_not_written_back(self):
        stored = _interrupted_md(state='created')
        shown, client = self._show(stored)

        self.assertIn('node_sizes', shown)
        client.set_namespace_metadata_item.assert_not_called()

        # Nor is the stored document changed in place: the fill is on a
        # copy, so the dictionary get_metadata() cached -- and which a
        # later set_metadata() would write -- still has no such key.
        self.assertNotIn('node_sizes', stored)
        self.assertEqual(_interrupted_md(state='created'), stored)

    def test_the_fallback_is_not_shared_with_the_default(self):
        # A caller which edits what show() handed it must not be editing
        # the module's default, which every later create and expand reads.
        shown, _ = self._show(_interrupted_md(state='created'))
        shown['node_sizes']['worker']['memory'] = 1
        self.assertEqual(2048, cluster_module.DEFAULT_NODE_SIZE['memory'])

    def test_recorded_sizes_are_returned_as_stored(self):
        # The two k3s configuration keys are stored too, as every cluster
        # created since they existed has them, because show() fills those
        # as well and this test is about a document with nothing to fill.
        stored = _interrupted_md(state='created')
        stored['node_sizes'] = {
            'control_plane': {'cpus': 4, 'memory': 8192, 'disk': 100},
            'worker': {'cpus': 2, 'memory': 4096, 'disk': 60}}
        stored['server_config'] = {}
        stored['agent_config'] = {}
        expected = copy.deepcopy(stored)

        shown, client = self._show(stored)

        self.assertEqual(expected, shown)
        client.set_namespace_metadata_item.assert_not_called()

    def test_a_partial_record_is_reported_as_expand_would_build_it(self):
        # Not a shape the plugin writes; see _node_size(). show() reports
        # what expand-workers would build, and still writes nothing.
        stored = _interrupted_md(state='created')
        stored['node_sizes'] = {'worker': {'cpus': 4}}
        expected_stored = copy.deepcopy(stored)

        shown, client = self._show(stored)

        self.assertEqual(
            {'control_plane': {'cpus': 2, 'memory': 2048, 'disk': 50},
             'worker': {'cpus': 4, 'memory': 2048, 'disk': 50}},
            shown['node_sizes'])
        self.assertEqual(expected_stored, stored)
        client.set_namespace_metadata_item.assert_not_called()


class ShowReportsK3sConfigTestCase(testtools.TestCase):
    """show() reports both k3s configurations, as {} for clusters which never recorded them.

    The same exact fill as node_sizes: a cluster created before the keys
    existed had no way to be given a configuration, so an empty one is a
    fact about it rather than a guess. And the same rule: nothing is
    written back, and the cached document is not changed in place.
    """

    def _show(self, md):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: md}
        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        return cluster.show(), client

    def test_a_cluster_without_configs_reports_empty_ones(self):
        stored = _interrupted_md(state='created')
        shown, client = self._show(stored)

        self.assertEqual({}, shown['server_config'])
        self.assertEqual({}, shown['agent_config'])
        # node_sizes is filled on the same copy.
        self.assertIn('node_sizes', shown)

        client.set_namespace_metadata_item.assert_not_called()
        self.assertNotIn('server_config', stored)
        self.assertNotIn('agent_config', stored)
        self.assertEqual(_interrupted_md(state='created'), stored)

    def test_recorded_configs_are_returned_as_stored(self):
        stored = _interrupted_md(state='created')
        stored['node_sizes'] = {
            'control_plane': {'cpus': 2, 'memory': 2048, 'disk': 50},
            'worker': {'cpus': 2, 'memory': 2048, 'disk': 50}}
        stored['server_config'] = {'disable': ['traefik']}
        stored['agent_config'] = {'node-label': ['a=b']}
        expected = copy.deepcopy(stored)

        shown, client = self._show(stored)

        self.assertEqual(expected, shown)
        # Nothing to fill, so no copy either: the cached dictionary itself.
        self.assertIs(stored, shown)
        client.set_namespace_metadata_item.assert_not_called()

    def test_one_missing_config_is_filled_alone(self):
        stored = _interrupted_md(state='created')
        stored['server_config'] = {'disable': ['traefik']}

        shown, _ = self._show(stored)

        self.assertEqual({'disable': ['traefik']}, shown['server_config'])
        self.assertEqual({}, shown['agent_config'])
        self.assertNotIn('agent_config', stored)


class InterruptedClusterVerbsTestCase(testtools.TestCase):
    """The verbs which need a built cluster refuse an interrupted one.

    Each of these reaches for something only a finished create records:
    md['control_plane_nodes'][0] to run kubectl on, or md['node_token'] to
    join a new worker with. On a cluster interrupted before those existed
    they fail with an IndexError, or -- worse, because it is silent --
    build instances which run 'sh -s - agent' with a K3S_TOKEN of None and
    can never join anything. Now that create() refuses to build over an
    interrupted cluster, these are the remaining ways to reach one.
    """

    def _cluster(self, md):
        client = ActionLogClient()
        client.metadata[primitives.CLUSTER_LIST] = ['banana']
        client.metadata[MD_KEY] = md
        client.instances['inst-w1'] = {
            'uuid': 'inst-w1', 'name': 'k3s-banana-node-002',
            'state': 'created', 'agent_state': 'ready'}
        return Cluster(client, 'banana', 'testns',
                       reporter=progress.CollectingReporter()), client

    def _assert_refused(self, verb, call, *args):
        cluster, client = self._cluster(
            _interrupted_md(control_plane_nodes=[], worker_nodes=['inst-w1']))

        e = self.assertRaises(exceptions.ClusterInterruptedError,
                              getattr(cluster, call), *args)

        self.assertEqual('not_usable', e.reason)
        self.assertEqual('initial', e.state)
        self.assertEqual(verb, e.verb)
        self.assertIn(verb, str(e))
        self.assertIn("'sf-client k3s delete banana'", str(e))

        self.assertEqual(
            [], client.actions,
            '%s acted on a cluster which was never finished being built. '
            'The client was asked to:\n    %s'
            % (verb, '\n    '.join(repr(a) for a in client.actions)))
        # And the metadata is exactly as it was found: a refusal must not
        # be a partial run.
        self.assertEqual(
            _interrupted_md(control_plane_nodes=[], worker_nodes=['inst-w1']),
            client.metadata[MD_KEY])
        return e

    def test_expand_workers_is_refused(self):
        self._assert_refused('expand-workers', 'expand_workers', 1)

    def test_remove_worker_is_refused(self):
        # The uuid asked for is genuinely one of this cluster's workers, so
        # it is the cluster's state which refuses this and not the worker
        # lookup which 3b added.
        self._assert_refused('remove-worker', 'remove_worker', ['inst-w1'])

    def test_expand_addresses_is_refused(self):
        self._assert_refused('expand-addresses', 'expand_addresses', 1)

    def test_update_os_is_allowed(self):
        # update-os talks to the instances in the metadata and nothing
        # else, so on an interrupted cluster it truthfully updates whatever
        # nodes exist. Refusing it would be a rule for its own sake.
        cluster, client = self._cluster(
            _interrupted_md(control_plane_nodes=[], worker_nodes=['inst-w1']))
        cluster.update_os()

        self.assertEqual(
            [('execute', 'inst-w1', 'apt-get update'),
             ('execute', 'inst-w1', 'apt-get dist-upgrade -y')],
            client.actions)

    def test_a_finished_cluster_is_not_refused(self):
        # The guard must not fire on the clusters these verbs exist for.
        cluster, client = self._cluster(
            _interrupted_md(state='created', control_plane_nodes=['inst-cp1'],
                            worker_nodes=['inst-w1']))
        client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        cluster.remove_worker(['inst-w1'])

        self.assertEqual([], client.metadata[MD_KEY]['worker_nodes'])


class HealthTestCase(testtools.TestCase):
    """health() reports an unhealthy cluster rather than failing on one.

    That is the whole verb: decision 7 of the phase 3 plan has it return
    structured data and repair nothing, and phase 5's Ansible module will
    branch on the dict rather than parse text. So the assertions here are
    on the content of the report, and in particular on the cases where the
    orchestration's normal behaviour is to raise -- a command which exits
    non-zero, an agent operation in the error state, an instance the
    metadata names which no longer exists -- each of which must arrive as a
    finding instead.
    """

    def setUp(self):
        super(HealthTestCase, self).setUp()
        self.client = fakes.HealthClient()

        self.md = {
            'name': 'banana',
            'namespace': 'testns',
            'state': 'created',
            'node_serial': 4,
            'node_network': 'net-1',
            'node_token': 'node-token',
            'k3s_version': 'v1.33',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'],
            'worker_nodes': ['inst-w1', 'inst-w2'],
            'routed_addresses': []
        }
        self.client.metadata[MD_KEY] = self.md

        for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-001'),
                                    ('inst-w1', 'k3s-banana-node-002'),
                                    ('inst-w2', 'k3s-banana-node-003')]:
            self.client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.cluster = _make_cluster(self.client)

    def test_a_healthy_cluster_reports_every_node(self):
        report = self.cluster.health()

        # signals is mock.ANY here because its content has tests of its own
        # in HealthSignalsTestCase; this one is about the node keys, and
        # still pins that there are no others.
        self.assertEqual(
            [{'uuid': 'inst-cp1', 'role': 'control_plane',
              'name': 'k3s-banana-node-001', 'exists': True,
              'state': 'created', 'agent_state': 'ready', 'healthy': True,
              'signals': mock.ANY},
             {'uuid': 'inst-w1', 'role': 'worker',
              'name': 'k3s-banana-node-002', 'exists': True,
              'state': 'created', 'agent_state': 'ready', 'healthy': True,
              'signals': mock.ANY},
             {'uuid': 'inst-w2', 'role': 'worker',
              'name': 'k3s-banana-node-003', 'exists': True,
              'state': 'created', 'agent_state': 'ready', 'healthy': True,
              'signals': mock.ANY}],
            report['nodes'])
        self.assertEqual('created', report['state'])
        self.assertFalse(report['interrupted'])
        self.assertTrue(report['healthy'])
        self.assertEqual('banana', report['name'])
        self.assertEqual('testns', report['namespace'])

    def test_the_api_is_probed_on_the_first_control_plane_node(self):
        report = self.cluster.health()

        # Every node's signals are read through the agent too, so the
        # kubectl probe is no longer the only command: it is the first, and
        # the only kubectl.
        self.assertEqual(
            ('inst-cp1',
             'kubectl get nodes --kubeconfig /etc/rancher/k3s/k3s.yaml'),
            self.client.executed[0])
        self.assertEqual(
            [('inst-cp1',
              'kubectl get nodes --kubeconfig /etc/rancher/k3s/k3s.yaml')],
            [(instance_uuid, command)
             for instance_uuid, command in self.client.executed
             if command.startswith('kubectl')])
        self.assertTrue(report['api']['probed'])
        self.assertTrue(report['api']['answered'])
        self.assertEqual('inst-cp1', report['api']['instance_uuid'])
        self.assertEqual(0, report['api']['return_code'])
        self.assertIn('k3s-banana-node-001   Ready', report['api']['stdout'])
        self.assertIsNone(report['api']['error'])

    def test_nothing_is_repaired(self):
        # health() must not be the verb which quietly fixes things, so the
        # only things it is allowed to ask the cluster to do are the read
        # only probes: no metadata write, no instance created or destroyed, no
        # network touched. The write is asserted as a call rather than as a
        # changed document because set_metadata() stores the very dictionary
        # the cache is already holding, so a write back leaves the stored
        # metadata comparing equal to what it was and an assertion on the
        # content cannot see it.
        before = copy.deepcopy(self.client.metadata)

        self.cluster.health()

        self.assertEqual(before, self.client.metadata)
        self.assertEqual([], self.client.metadata_writes)
        self.assertEqual([], self.client.metadata_deletes)
        self.assertEqual([], self.client.deleted_instances)
        self.assertEqual([], self.client.deleted_networks)
        self.assertEqual([], self.client.unrouted_addresses)
        self.assertEqual(0, self.client.instance_serial)
        self.assertEqual(
            ['kubectl get nodes --kubeconfig /etc/rancher/k3s/k3s.yaml',
             cluster_module.node_signals_command('control_plane'),
             cluster_module.node_signals_command('worker'),
             cluster_module.node_signals_command('worker')],
            [command for _, command in self.client.executed])

    def test_an_instance_in_the_error_state_is_reported_rather_than_raised(self):
        self.client.instances['inst-w1']['state'] = 'error'
        self.client.instances['inst-w1']['agent_state'] = None

        report = self.cluster.health()

        worker = [n for n in report['nodes'] if n['uuid'] == 'inst-w1'][0]
        self.assertEqual('error', worker['state'])
        self.assertIsNone(worker['agent_state'])
        self.assertFalse(worker['healthy'])
        self.assertFalse(report['healthy'])

        # And the rest of the report is still complete: a broken node must
        # not truncate the answer.
        self.assertEqual(3, len(report['nodes']))
        self.assertTrue(
            [n for n in report['nodes'] if n['uuid'] == 'inst-w2'][0]['healthy'])
        self.assertTrue(report['api']['answered'])

    def test_an_instance_whose_agent_is_not_ready_is_unhealthy(self):
        # A booted instance whose in-guest agent has never answered is a
        # node k3s cannot be running on, and is the state await_boot()
        # waits out rather than the one it accepts.
        self.client.instances['inst-w2']['agent_state'] = 'not ready'

        report = self.cluster.health()

        worker = [n for n in report['nodes'] if n['uuid'] == 'inst-w2'][0]
        self.assertEqual('created', worker['state'])
        self.assertEqual('not ready', worker['agent_state'])
        self.assertFalse(worker['healthy'])
        self.assertFalse(report['healthy'])

    def test_an_instance_which_no_longer_exists_is_reported_rather_than_raised(self):
        # Cluster metadata can name an instance somebody has since deleted
        # out from under it, which is why delete() catches this per
        # instance. health() hits the same case, and is the verb which is
        # supposed to tell you about it.
        del self.client.instances['inst-w1']

        report = self.cluster.health()

        self.assertEqual(
            {'uuid': 'inst-w1', 'role': 'worker', 'name': None,
             'exists': False, 'state': None, 'agent_state': None,
             'healthy': False,
             'signals': {
                 'probed': False, 'error': 'this instance no longer exists',
                 'boot_id': None, 'booted_at': None,
                 'k3s_unit': 'k3s-agent', 'k3s_state': None,
                 'k3s_restarts': None, 'oom_kills': None,
                 'memory_total_bytes': None, 'memory_available_bytes': None,
                 'etcd_bytes': None, 'etcd_snapshot_bytes': None}},
            [n for n in report['nodes'] if n['uuid'] == 'inst-w1'][0])
        self.assertFalse(report['healthy'])
        self.assertEqual(3, len(report['nodes']))

    def test_a_kubectl_which_exits_non_zero_is_reported_rather_than_raised(self):
        # execute_and_await() would raise CommandFailedError here, which is
        # exactly what the verb cannot do: an API which does not answer is
        # the single most important thing health() has to be able to say.
        self.client.probe_return_code = 1
        self.client.probe_stdout = ''
        self.client.probe_stderr = (
            'The connection to the server 127.0.0.1:6443 was refused\n')

        report = self.cluster.health()

        self.assertTrue(report['api']['probed'])
        self.assertFalse(report['api']['answered'])
        self.assertEqual(1, report['api']['return_code'])
        self.assertIn('connection to the server', report['api']['stderr'])
        self.assertIn('exited 1', report['api']['error'])
        self.assertFalse(report['healthy'])

        # ...and every node is still reported, which is the other half of
        # not raising.
        self.assertEqual(3, len(report['nodes']))
        self.assertTrue(all(n['healthy'] for n in report['nodes']))

    def test_an_errored_agent_operation_is_reported_rather_than_raised(self):
        # await_idle() raises AgentOperationError for this, which is why the
        # probe does not go through execute_and_await().
        self.client.probe_state = 'error'

        report = self.cluster.health()

        self.assertFalse(report['api']['answered'])
        self.assertIn('error state', report['api']['error'])
        self.assertIn('kubectl get nodes', report['api']['error'])
        self.assertFalse(report['healthy'])
        self.assertEqual(3, len(report['nodes']))

    def test_an_api_refusal_to_run_the_probe_is_reported_rather_than_raised(self):
        # The control plane instance is gone, so the command cannot even be
        # submitted. The nodes are still reported.
        self.client.probe_raises = fakes.not_found('inst-cp1')

        report = self.cluster.health()

        self.assertFalse(report['api']['probed'])
        self.assertFalse(report['api']['answered'])
        self.assertIn('ResourceNotFoundException', report['api']['error'])
        self.assertIn('inst-cp1', report['api']['error'])
        self.assertFalse(report['healthy'])
        self.assertEqual(3, len(report['nodes']))

    def test_an_interrupted_cluster_is_reported_rather_than_refused(self):
        # The three verbs which change a built cluster call
        # _require_usable() and refuse this. health() must not: describing a
        # cluster which never finished being built is what it is for.
        self.md['state'] = 'initial'
        self.md['control_plane_nodes'] = []
        self.md['worker_nodes'] = []

        report = self.cluster.health()

        self.assertEqual('initial', report['state'])
        self.assertTrue(report['interrupted'])
        self.assertEqual([], report['nodes'])
        self.assertFalse(report['healthy'])

        # There was no node to ask, so the probe was never run rather than
        # failing on md['control_plane_nodes'][0].
        self.assertFalse(report['api']['probed'])
        self.assertIsNone(report['api']['instance_uuid'])
        self.assertIn('no control plane node', report['api']['error'])
        self.assertEqual([], self.client.executed)

    def test_metadata_with_no_state_at_all_is_interrupted(self):
        # _interrupted_state() answers 'unknown' rather than None for a
        # document this package did not write, and the report says so.
        del self.md['state']

        report = self.cluster.health()

        self.assertEqual('unknown', report['state'])
        self.assertTrue(report['interrupted'])
        self.assertFalse(report['healthy'])

    def test_a_cluster_with_no_nodes_is_not_healthy(self):
        # The state here is 'created' and there is nothing unhealthy in the
        # (empty) node list, over which all() answers True. What makes this
        # unhealthy is that there was no control plane node to ask, so the
        # k3s API never answered -- which is why health() carries no
        # separate "has at least one node" term. This pins that the empty
        # cluster still comes out unhealthy, and for that reason.
        self.md['control_plane_nodes'] = []
        self.md['worker_nodes'] = []

        report = self.cluster.health()

        self.assertEqual([], report['nodes'])
        self.assertTrue(all(node['healthy'] for node in report['nodes']))
        self.assertFalse(report['api']['answered'])
        self.assertFalse(report['healthy'])

    def test_a_missing_cluster_raises(self):
        client = fakes.HealthClient()
        cluster = _make_cluster(client)

        self.assertRaises(
            exceptions.ClusterNotFoundError, cluster.health)


class ReadManifestsTestCase(testtools.TestCase):
    """read_manifests() reads what it can stage, and refuses what it cannot.

    Everything it refuses, it refuses before create() has built anything,
    which is the whole reason it is a separate function called at the top of
    create() rather than a loop inside install_control_plane(). Six of the
    seven refusals are each a way a manifest would otherwise be lost
    silently -- overwritten by another manifest, copied to a filename k3s
    never looks at, truncated by its own content, rejected on the node by a
    parser nobody is watching -- or, in the case of an unreadable file, the
    check which stands in for click.Path(exists=True) for a caller with no
    click. The seventh is about the name rather than the content: the
    basename is interpolated into a shell command line which runs as root
    on the control plane node.

    Which parser the content is checked with is decided the way k3s decides
    it, on the content and not the suffix, so the tests below check a JSON
    manifest as JSON wherever k3s would.
    """

    def setUp(self):
        super(ReadManifestsTestCase, self).setUp()
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.tempdir = tempdir.name

    def _write(self, name, content, subdir=None):
        directory = self.tempdir
        if subdir:
            directory = os.path.join(self.tempdir, subdir)
            os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, name)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return path

    def _write_bytes(self, name, content):
        path = os.path.join(self.tempdir, name)
        with open(path, 'wb') as f:
            f.write(content)
        return path

    def test_nothing_to_read(self):
        # None and an empty list are both "no manifests", which is what
        # every existing caller of install_control_plane() passes.
        self.assertEqual([], cluster_module.read_manifests(None))
        self.assertEqual([], cluster_module.read_manifests([]))

    def test_the_basename_and_the_content_are_returned_in_order(self):
        first = self._write('first.yaml', 'kind: One\n')
        second = self._write('second.yml', 'kind: Two\n')
        third = self._write('third.json', '{"kind": "Three"}\n')

        self.assertEqual(
            [('first.yaml', 'kind: One\n'),
             ('second.yml', 'kind: Two\n'),
             ('third.json', '{"kind": "Three"}\n')],
            cluster_module.read_manifests([first, second, third]))

    def test_the_directory_the_file_came_from_is_not_carried_along(self):
        # The destination is the basename, so a manifest read from a deep
        # local path lands in the manifests directory itself.
        path = self._write('deep.yaml', 'kind: Deep\n', subdir='a/b/c')
        self.assertEqual([('deep.yaml', 'kind: Deep\n')],
                         cluster_module.read_manifests([path]))

    def test_an_upper_case_extension_is_still_a_manifest(self):
        # k3s matches its three suffixes case insensitively, so this file
        # will be applied and must not be refused.
        path = self._write('SHOUTY.YAML', 'kind: Loud\n')
        self.assertEqual([('SHOUTY.YAML', 'kind: Loud\n')],
                         cluster_module.read_manifests([path]))

    def test_a_basename_with_shell_metacharacters_is_refused(self):
        # os.path.basename('/tmp/a;touch pwned.yaml') is
        # 'a;touch pwned.yaml', which as the destination of the staging
        # write would run 'touch pwned' as root on the control plane node.
        # click.Path(exists=True) does not help here: the file exists.
        path = self._write('a;touch pwned.yaml', 'kind: One\n')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertEqual('unsafe_basename', e.reason)
        self.assertEqual('a;touch pwned.yaml', e.basename)
        self.assertEqual(path, e.path)

    def test_a_basename_with_a_newline_is_refused(self):
        # Quoting makes this safe rather than dangerous, but a heredoc
        # whose destination filename spans two lines is unreadable in the
        # agent operation log and is not a name anybody meant to use.
        path = self._write('two\nlines.yaml', 'kind: One\n')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertEqual('unsafe_basename', e.reason)

    def test_a_basename_starting_with_a_dot_or_a_hyphen_is_refused(self):
        for name in ['.hidden.yaml', '-dash.yaml']:
            path = self._write(name, 'kind: One\n')
            e = self.assertRaises(
                exceptions.ManifestError, cluster_module.read_manifests,
                [path])
            self.assertEqual('unsafe_basename', e.reason, name)

    def test_the_ordinary_filename_shapes_are_still_accepted(self):
        # The charset is only worth having if it does not refuse the names
        # a real manifest has. Underscores, hyphens, dots and digits are
        # all ordinary in a Kubernetes manifest filename.
        for name in ['plain.yaml', 'with-hyphens.yaml', 'with_underscores.yml',
                     '00-ordered.yaml', 'dotted.name.json', 'CamelCase.yaml']:
            path = self._write(name, 'kind: One\n')
            self.assertEqual([(name, 'kind: One\n')],
                             cluster_module.read_manifests([path]))

    def test_a_duplicate_basename_raises_and_names_both_paths(self):
        first = self._write('same.yaml', 'kind: One\n', subdir='one')
        second = self._write('same.yaml', 'kind: Two\n', subdir='two')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests,
            [first, second])

        self.assertEqual('duplicate_basename', e.reason)
        self.assertEqual('same.yaml', e.basename)
        self.assertIn(first, str(e))
        self.assertIn(second, str(e))

    def test_a_file_which_is_not_there_raises_a_manifest_error(self):
        # The library boundary: --manifest is a click.Path(exists=True), so
        # the command line never gets here, but a library caller passes
        # paths nothing has checked. It must get this hierarchy's exception
        # rather than a bare FileNotFoundError, because the Ansible module
        # phase 5 builds catches K3sClusterException.
        path = os.path.join(self.tempdir, 'absent.yaml')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual('unreadable', e.reason)
        self.assertEqual(path, e.path)
        self.assertIn(path, str(e))

    def test_a_file_which_cannot_be_read_raises_a_manifest_error(self):
        # Existing but unopenable, which click.Path(exists=True) does not
        # catch either: it is a directory, so open() raises IsADirectoryError.
        path = os.path.join(self.tempdir, 'directory.yaml')
        os.makedirs(path)

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertEqual('unreadable', e.reason)
        self.assertIn(path, str(e))

    def test_a_file_which_is_not_yaml_raises(self):
        path = self._write('broken.yaml', 'kind: [unclosed\n')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertEqual('invalid_yaml', e.reason)
        self.assertEqual(path, e.path)
        self.assertIn(path, str(e))

    def test_a_file_which_is_not_text_at_all_is_unreadable(self):
        # UnicodeDecodeError is a ValueError, not an OSError, so a read which
        # catches only OSError lets it out of the K3sClusterException
        # hierarchy entirely -- which is the one thing unreadable() exists to
        # stop for a library caller.
        path = self._write_bytes('binary.yaml', b'kind: One\n# \xff\xfe\n')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertEqual('unreadable', e.reason)
        self.assertEqual(path, e.path)

    def test_a_utf_8_manifest_reads_the_same_under_an_ascii_locale(self):
        # The read states its encoding rather than inheriting the locale's.
        # Without that, the same manifest is a different string depending on
        # where this runs: a UnicodeDecodeError under LC_ALL=C, or worse, a
        # successful mis-decode under a latin-1 locale which stages bytes the
        # caller never supplied. A subprocess because tox sets LC_ALL to a
        # UTF-8 locale for the suite, so this is the only way to vary it.
        path = self._write('accented.yaml', 'kind: One\nname: caf\u00e9\n')
        package_root = os.path.dirname(
            os.path.dirname(os.path.dirname(cluster_module.__file__)))
        script = (
            'import locale, sys\n'
            'if "utf" in locale.getpreferredencoding(False).lower():\n'
            '    print("SKIP"); sys.exit(0)\n'
            'from shakenfist_client_k3s import cluster\n'
            'print(cluster.read_manifests([%r])[0][1].encode("unicode_escape")'
            '.decode())\n' % path)
        env = {'PATH': os.environ.get('PATH', ''), 'LC_ALL': 'C',
               'LANG': 'C', 'PYTHONCOERCECLOCALE': '0', 'PYTHONUTF8': '0',
               'PYTHONPATH': package_root}

        proc = subprocess.run([sys.executable, '-c', script], env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.assertEqual(0, proc.returncode, proc.stderr)
        out = proc.stdout.decode('utf-8').strip()
        if out == 'SKIP':
            self.skipTest('this interpreter will not leave UTF-8 mode')
        self.assertEqual('kind: One\\nname: caf\\xe9\\n', out)

    def test_a_tab_indented_json_manifest_is_accepted(self):
        # PyYAML implements YAML 1.1, which forbids tabs where JSON permits
        # them, so a YAML-only gate refuses this. k3s does not: its deploy
        # controller hands the document to apimachinery's ToJSON(), which
        # returns a '{'-prefixed buffer untouched.
        content = json.dumps({'kind': 'ConfigMap', 'metadata': {'name': 'x'}},
                             indent='\t') + '\n'
        self.assertIn('\n\t', content)
        path = self._write('tabbed.json', content)

        self.assertEqual([('tabbed.json', content)],
                         cluster_module.read_manifests([path]))

    def test_a_json_manifest_which_does_not_parse_is_refused_as_json(self):
        path = self._write('broken.json', '{"kind": "ConfigMap",\n')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertEqual('invalid_json', e.reason)
        self.assertEqual(path, e.path)
        self.assertIn('not valid JSON', str(e))

    def test_json_content_in_a_yaml_file_is_checked_as_json(self):
        # The suffix is not the deciding fact, because it is not the fact k3s
        # decides on: IsJSONBuffer() looks at the content, so a .yaml file
        # full of tab indented JSON is applied as JSON and must be accepted
        # here as JSON too.
        content = json.dumps({'kind': 'ConfigMap'}, indent='\t') + '\n'
        path = self._write('really-json.yaml', content)

        self.assertEqual([('really-json.yaml', content)],
                         cluster_module.read_manifests([path]))

    def test_a_json_file_which_is_not_object_shaped_is_checked_as_yaml(self):
        # IsJSONBuffer() tests for '{' and nothing else, so a document
        # starting with '[' goes through k3s's YAML path however it is named,
        # and a tab in it is refused there rather than here.
        path = self._write('list.json', '[{"kind": "ConfigMap"}]\n')
        self.assertEqual([('list.json', '[{"kind": "ConfigMap"}]\n')],
                         cluster_module.read_manifests([path]))

        tabbed = self._write('tabbed-list.json', '[\n\t{"kind": "ConfigMap"}\n]\n')
        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [tabbed])
        self.assertEqual('invalid_yaml', e.reason)

    def test_leading_whitespace_does_not_hide_the_json(self):
        # apimachinery trims leading whitespace before it looks for the
        # brace, so this is JSON to k3s and has to be JSON here.
        content = '\n  ' + json.dumps({'kind': 'ConfigMap'}, indent='\t') + '\n'
        path = self._write('indented.json', content)

        self.assertEqual([('indented.json', content)],
                         cluster_module.read_manifests([path]))

    def test_several_documents_in_one_file_are_still_yaml(self):
        # A manifest is routinely a multi document stream, which
        # safe_load() alone would refuse.
        content = 'kind: One\n---\nkind: Two\n'
        path = self._write('two-documents.yaml', content)
        self.assertEqual([('two-documents.yaml', content)],
                         cluster_module.read_manifests([path]))

    def test_an_extension_k3s_ignores_raises(self):
        path = self._write('payload.txt', 'kind: Ignored\n')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertEqual('not_a_manifest', e.reason)
        for suffix in cluster_module.K3S_MANIFEST_SUFFIXES:
            self.assertIn(suffix, str(e))

    def test_content_which_would_end_the_heredoc_early_raises(self):
        # Valid YAML -- the second document is a plain scalar -- so what is
        # refused here is the transport rather than the payload.
        path = self._write(
            'collides.yaml',
            'kind: One\n---\n%s\n' % cluster_module.K3S_MANIFEST_DELIMITER)

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests, [path])

        self.assertEqual('delimiter_collision', e.reason)
        self.assertEqual(cluster_module.K3S_MANIFEST_DELIMITER, e.delimiter)

    def test_the_delimiter_inside_a_line_is_not_a_collision(self):
        # Only a line which is exactly the delimiter ends the heredoc, so
        # refusing one which merely mentions it would be a false refusal.
        content = 'kind: One\nvalue: not-%s-really\n' % (
            cluster_module.K3S_MANIFEST_DELIMITER)
        path = self._write('mentions.yaml', content)
        self.assertEqual([('mentions.yaml', content)],
                         cluster_module.read_manifests([path]))

    def test_the_first_bad_manifest_is_the_one_reported(self):
        # Unlike WorkerNotFoundError this does not aggregate, because
        # nothing has been changed when it raises. What matters is that the
        # good manifest which follows does not paper over the bad one.
        bad = self._write('bad.txt', 'kind: Ignored\n')
        good = self._write('good.yaml', 'kind: Fine\n')

        e = self.assertRaises(
            exceptions.ManifestError, cluster_module.read_manifests,
            [bad, good])

        self.assertEqual(bad, e.path)


class ValidateNodeSizesTestCase(testtools.TestCase):
    """validate_node_sizes() accepts positive integers and nothing else.

    It is read_manifests()'s sibling: a pure function create() calls before
    it registers the name, so that a size which cannot be built costs the
    caller an error rather than a claimed name and a metadata document
    stuck in 'initial'. The command line's click.IntRange(min=1) refuses
    most of these first, but a library caller -- an Ansible variable, a
    YAML document -- has no click, so each refusal is pinned here against
    the function itself.
    """

    def _sizes(self, role=None, field=None, value=None):
        sizes = {
            'control_plane': dict(cluster_module.DEFAULT_NODE_SIZE),
            'worker': dict(cluster_module.DEFAULT_NODE_SIZE),
        }
        if role:
            sizes[role][field] = value
        return sizes

    def test_the_defaults_are_accepted(self):
        self.assertIsNone(cluster_module.validate_node_sizes(self._sizes()))

    def test_distinct_positive_sizes_are_accepted(self):
        # One is the smallest positive integer, and is accepted on purpose:
        # a floor above it would be a guess about the workload.
        sizes = {'control_plane': {'cpus': 4, 'memory': 8192, 'disk': 100},
                 'worker': {'cpus': 1, 'memory': 1, 'disk': 1}}
        self.assertIsNone(cluster_module.validate_node_sizes(sizes))

    def _assert_refused(self, role, field, value):
        e = self.assertRaises(
            exceptions.NodeSizeError, cluster_module.validate_node_sizes,
            self._sizes(role, field, value))
        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual(role, e.role)
        self.assertEqual(field, e.field)
        self.assertIs(value, e.value)
        self.assertEqual(
            '%s %s must be a positive integer, not %r'
            % (role.replace('_', ' '), field, value), str(e))
        return e

    def test_zero_is_refused(self):
        e = self._assert_refused('worker', 'memory', 0)
        self.assertEqual(
            'worker memory must be a positive integer, not 0', str(e))

    def test_a_negative_size_is_refused(self):
        self._assert_refused('control_plane', 'disk', -1)

    def test_true_is_refused(self):
        # True is an int in Python and is >= 1, so without an explicit bool
        # check this would build a one vCPU node and say nothing. YAML's
        # 'yes' arrives as exactly this.
        e = self._assert_refused('control_plane', 'cpus', True)
        self.assertEqual(
            'control plane cpus must be a positive integer, not True', str(e))

    def test_a_float_is_refused(self):
        # Refused rather than truncated: 2.0 happens to be whole, but the
        # same check has to refuse 2.5, and accepting one float and not the
        # other is a rule about values where the caller's mistake is a type.
        self._assert_refused('worker', 'cpus', 2.0)

    def test_a_string_is_refused(self):
        # And told apart from the integer it looks like: the message uses
        # repr(), so '2' is not rendered as 2.
        e = self._assert_refused('worker', 'disk', '2')
        self.assertIn("not '2'", str(e))

    def test_none_is_refused(self):
        self._assert_refused('control_plane', 'memory', None)


# The keys each role refuses, written out rather than read from the
# frozensets, so that a key leaving or joining a set is a change this file
# has to make on purpose.
SERVER_OWNED_KEYS = (
    'cluster-init', 'data-dir', 'https-listen-port', 'node-name', 'server',
    'tls-san', 'token', 'token-file', 'with-node-id', 'write-kubeconfig',
    'write-kubeconfig-mode',
    # k3s's aliases for data-dir, server, token and write-kubeconfig.
    'd', 's', 't', 'o')
AGENT_OWNED_KEYS = (
    'data-dir', 'node-name', 'server', 'token', 'token-file', 'with-node-id',
    # k3s's aliases for data-dir, server and token.
    'd', 's', 't')


class ValidateK3sConfigTestCase(testtools.TestCase):
    """validate_k3s_config() accepts configuration it can write, and refuses the rest.

    Another of create()'s checks before the name is registered, so each
    refusal here is one a caller hears about before anything exists to
    clean up. What it refuses is what the plugin cannot live with -- a key
    it owns, a value the JSON metadata cannot record, text that would end
    its own heredoc -- and nothing about whether k3s knows the key.
    """

    REALISTIC = {
        'disable': ['traefik'],
        'node-label': ['openstack-control-plane=enabled'],
        'tls-san+': ['k3s.example.com'],
        'node-taint': [],
        'kubelet-arg': ['max-pods=250'],
    }

    def _assert_refused(self, reason, config, role='server'):
        e = self.assertRaises(
            exceptions.K3sConfigError, cluster_module.validate_k3s_config,
            config, role)
        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual(reason, e.reason)
        self.assertEqual(role, e.role)
        return e

    def test_the_owned_key_sets_are_exactly_the_plans(self):
        self.assertEqual(frozenset(SERVER_OWNED_KEYS),
                         cluster_module.K3S_SERVER_OWNED_KEYS)
        self.assertEqual(frozenset(AGENT_OWNED_KEYS),
                         cluster_module.K3S_AGENT_OWNED_KEYS)

    def test_an_empty_mapping_is_no_text(self):
        for role in ('server', 'agent'):
            self.assertEqual(
                '', cluster_module.validate_k3s_config({}, role))

    def test_none_is_an_empty_mapping(self):
        # What an empty file and a library caller's default both produce.
        for role in ('server', 'agent'):
            self.assertEqual(
                '', cluster_module.validate_k3s_config(None, role))

    def test_a_realistic_mapping_is_returned_as_sorted_block_yaml(self):
        text = cluster_module.validate_k3s_config(self.REALISTIC, 'server')

        self.assertEqual(self.REALISTIC, yaml.safe_load(text))
        self.assertEqual(
            yaml.safe_dump(self.REALISTIC, default_flow_style=False),
            text)
        # Block style and sorted, so the file on the node reads the same
        # whatever order the caller's mapping happened to be in.
        top_level = [line.split(':')[0] for line in text.split('\n')
                     if line and not line.startswith(('-', ' '))]
        self.assertEqual(sorted(self.REALISTIC), top_level)

    def test_a_dict_subclass_is_written_as_plain_yaml(self):
        # yaml.safe_dump() refuses to represent an OrderedDict. The text is
        # dumped from the JSON round trip, which is plain dicts, so a
        # library caller's mapping type does not decide whether this works.
        config = collections.OrderedDict([('node-label', ['a=b'])])
        self.assertEqual(
            'node-label:\n- a=b\n',
            cluster_module.validate_k3s_config(config, 'agent'))

    def test_a_list_is_refused(self):
        e = self._assert_refused('not_a_mapping', ['disable', 'traefik'])
        self.assertIn('not list', str(e))

    def test_a_string_is_refused(self):
        # What a file holding one bare line of text parses to.
        e = self._assert_refused('not_a_mapping', 'disable traefik', 'agent')
        self.assertIn('not str', str(e))

    def test_an_integer_key_is_refused(self):
        e = self._assert_refused('non_string_key', {1: 'x'})
        self.assertEqual(1, e.key)

    def test_a_date_value_is_refused(self):
        # yaml.safe_load reads an unquoted 2026-10-05 as a datetime.date,
        # which json.dumps() cannot serialise at all, so set_metadata()
        # would fail with the name already registered.
        value = datetime.date(2026, 10, 5)
        e = self._assert_refused(
            'not_representable', {'node-label': ['ok=yes'], 'kubelet-arg': value})
        self.assertEqual('kubelet-arg', e.key)
        self.assertIs(value, e.value)
        self.assertIn('JSON', str(e))

    def test_a_value_json_changes_is_refused(self):
        # Serialisable, but not unchanged: JSON turns the integer key into
        # '1', so what the metadata recorded would not be what was written.
        e = self._assert_refused(
            'not_representable', {'node-label': {1: 'a'}}, 'agent')
        self.assertEqual('node-label', e.key)

    def test_nan_and_infinities_are_refused(self):
        # YAML reads .nan, .inf and -.inf as floats, and json.dumps() writes
        # them as NaN, Infinity and -Infinity, which are not JSON. An
        # infinity compares equal after that round trip, so only refusing
        # them outright keeps them out of the metadata.
        for text in ('.nan', '.inf', '-.inf'):
            value = yaml.safe_load('x: %s\n' % text)['x']
            e = self._assert_refused(
                'not_representable', {'kubelet-arg': [value]}, 'agent')
            self.assertEqual('kubelet-arg', e.key)

    def test_every_server_owned_key_is_refused_with_and_without_plus(self):
        for key in SERVER_OWNED_KEYS:
            for spelling in (key, key + '+'):
                if spelling == 'tls-san+':
                    continue
                e = self._assert_refused('owned_key', {spelling: 'x'})
                self.assertEqual(spelling, e.key)
                self.assertIn(spelling, str(e))

    def test_every_agent_owned_key_is_refused_with_and_without_plus(self):
        for key in AGENT_OWNED_KEYS:
            for spelling in (key, key + '+'):
                e = self._assert_refused(
                    'owned_key', {spelling: 'x'}, 'agent')
                self.assertEqual(spelling, e.key)

    def test_bare_tls_san_is_refused_and_the_message_says_what_to_write(self):
        e = self._assert_refused('owned_key', {'tls-san': ['k3s.example.com']})
        self.assertIn('tls-san+', str(e))

    def test_tls_san_plus_is_allowed_on_a_server(self):
        # The one '+' spelling of an owned key which is allowed: it is how
        # a caller adds SANs to the floating address the plugin sets.
        text = cluster_module.validate_k3s_config(
            {'tls-san+': ['k3s.example.com']}, 'server')
        self.assertEqual({'tls-san+': ['k3s.example.com']},
                         yaml.safe_load(text))

    def test_k3s_aliases_of_owned_keys_are_refused(self):
        # k3s accepts a flag's one-letter alias in a configuration file as
        # readily as its long name, so t: is token by another spelling.
        for key, role in (('t', 'agent'), ('s', 'agent'), ('d', 'server'),
                          ('o', 'server'), ('t+', 'server')):
            e = self._assert_refused('owned_key', {key: 'x'}, role)
            self.assertEqual(key, e.key)

        # o is write-kubeconfig on a server only; an agent has no such flag.
        self.assertNotEqual(
            '', cluster_module.validate_k3s_config({'o': 'x'}, 'agent'))

    def test_a_key_containing_equals_is_refused(self):
        # k3s would pass token=abc: def on as --token=abc=def, which sets
        # token. Refused whatever precedes the '=', since no flag has one.
        for key in ('token=abc', 'node-label=a', '=x'):
            for role in ('server', 'agent'):
                e = self._assert_refused('key_contains_equals', {key: 'x'}, role)
                self.assertEqual(key, e.key)
                self.assertIn(repr(key), str(e))

    def test_a_callers_own_plus_key_is_accepted(self):
        # docs/usage.md tells a caller to write node-taint+ to add a taint
        # to the plugin's. The '+' is stripped only to find an owned key
        # behind it; a '+' on any other key is the caller's to use.
        config = {'node-taint+': ['dedicated=infra:NoSchedule'],
                  'node-label+': ['a=b'],
                  'disable+': ['local-storage']}
        for role in ('server', 'agent'):
            text = cluster_module.validate_k3s_config(config, role)
            self.assertEqual(config, yaml.safe_load(text))

    def test_a_doubled_plus_is_still_an_owned_key(self):
        # Only exactly tls-san+ is excused; anything else which strips to
        # an owned key is that key.
        self._assert_refused('owned_key', {'tls-san++': ['x']})
        self._assert_refused('owned_key', {'token++': 'x'}, 'agent')

    def test_ownership_is_per_role(self):
        # write-kubeconfig-mode is a server's concern only: an agent writes
        # no kubeconfig, and k3s ignores the key there itself.
        self.assertNotEqual(
            '', cluster_module.validate_k3s_config(
                {'write-kubeconfig-mode': '0600'}, 'agent'))

    def test_a_value_holding_the_delimiter_is_written_indented(self):
        # The check is on the text that will be written, not on the input.
        # PyYAML indents every continuation line of a value in a mapping,
        # so the delimiter on a line of its own inside a value never
        # reaches column zero, and refusing it would refuse configuration
        # which can be written perfectly well.
        config = {'node-label': 'a\n%s\nb' % cluster_module.K3S_CONFIG_DELIMITER,
                  cluster_module.K3S_CONFIG_DELIMITER: ['x']}
        text = cluster_module.validate_k3s_config(config, 'server')
        self.assertNotIn(cluster_module.K3S_CONFIG_DELIMITER, text.split('\n'))
        self.assertEqual(config, yaml.safe_load(text))

    def test_text_with_a_delimiter_line_is_refused(self):
        # No mapping makes today's PyYAML emit a line which is exactly
        # SFK3SCONFIG (see the test above), so the check is exercised by
        # pointing it at a line this dump does emit. That still runs the
        # real dumper and the real comparison; only the marker differs.
        with mock.patch.object(cluster_module, 'K3S_CONFIG_DELIMITER',
                               '- traefik'):
            e = self._assert_refused('delimiter_collision',
                                     {'disable': ['traefik']})
        self.assertEqual('- traefik', e.delimiter)

    def test_an_unknown_role_is_a_programming_error(self):
        self.assertRaises(ValueError, cluster_module.validate_k3s_config,
                          {}, 'worker')


class ReadK3sConfigTestCase(testtools.TestCase):
    """read_k3s_config() reads one YAML mapping, and keeps every failure in the hierarchy."""

    def setUp(self):
        super(ReadK3sConfigTestCase, self).setUp()
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.tempdir = tempdir.name

    def _write_bytes(self, content, name='config.yaml'):
        path = os.path.join(self.tempdir, name)
        with open(path, 'wb') as f:
            f.write(content)
        return path

    def _assert_unreadable(self, path):
        e = self.assertRaises(
            exceptions.K3sConfigError, cluster_module.read_k3s_config,
            path, 'server')
        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual('unreadable', e.reason)
        self.assertEqual(path, e.path)
        self.assertIn(path, str(e))
        return e

    def test_the_mapping_is_returned_not_the_text(self):
        path = self._write_bytes(
            b'disable:\n- traefik\nnode-label:\n- a=b\n')
        self.assertEqual({'disable': ['traefik'], 'node-label': ['a=b']},
                         cluster_module.read_k3s_config(path, 'server'))

    def test_an_empty_file_is_an_empty_mapping(self):
        path = self._write_bytes(b'')
        self.assertEqual({}, cluster_module.read_k3s_config(path, 'agent'))

    def test_a_missing_file_is_refused(self):
        self._assert_unreadable(os.path.join(self.tempdir, 'missing.yaml'))

    def test_a_file_which_is_not_utf8_is_refused(self):
        # A UnicodeDecodeError is a ValueError, not an OSError, and has to
        # be named to stay inside the hierarchy.
        self._assert_unreadable(self._write_bytes(b'node-label:\n- \xff\xfe\n'))

    def test_invalid_yaml_is_refused(self):
        self._assert_unreadable(self._write_bytes(b'disable: [traefik\n'))

    def test_two_documents_are_refused(self):
        # yaml.safe_load raises for a stream holding more than one
        # document, which is a YAMLError like any other parse failure.
        self._assert_unreadable(
            self._write_bytes(b'disable:\n- traefik\n---\ntoken: x\n'))

    def test_an_alias_is_refused(self):
        e = self._assert_unreadable(self._write_bytes(
            b'node-label: &labels [a=b]\nnode-label+: *labels\n'))
        self.assertIn('alias', str(e))

    def test_a_merge_key_is_refused(self):
        # <<: needs an alias to name what it merges.
        self._assert_unreadable(self._write_bytes(
            b'base: &base {a: b}\nkubelet-arg:\n  <<: *base\n'))

    def test_nested_aliases_are_refused_before_they_are_expanded(self):
        # Ten references per level, so each level multiplies the expanded
        # text by ten; nine levels in about 350 bytes would be gigabytes.
        # Two levels here, so that if the refusal were lost this would
        # fail by returning a mapping rather than hang expanding one.
        lines = [b'l0: &l0 [x]']
        for level in range(1, 3):
            lines.append(b'l%d: &l%d [%s]' % (
                level, level, b', '.join([b'*l%d' % (level - 1)] * 10)))
        self._assert_unreadable(self._write_bytes(b'\n'.join(lines) + b'\n'))

    def test_an_anchor_with_no_alias_is_accepted(self):
        path = self._write_bytes(b'node-label: &labels [a=b]\n')
        self.assertEqual({'node-label': ['a=b']},
                         cluster_module.read_k3s_config(path, 'agent'))

    def test_a_mapping_passed_directly_may_share_a_value(self):
        # Only parsing refuses aliases. A library caller's own mapping, in
        # which two keys refer to one list, is validated as it always was.
        labels = ['a=b']
        config = {'node-label': labels, 'node-label+': labels}
        self.assertEqual(
            {'node-label': ['a=b'], 'node-label+': ['a=b']},
            yaml.safe_load(cluster_module.validate_k3s_config(config, 'agent')))

    def test_what_is_read_is_validated(self):
        path = self._write_bytes(b'token: x\n')
        e = self.assertRaises(
            exceptions.K3sConfigError, cluster_module.read_k3s_config,
            path, 'agent')
        self.assertEqual('owned_key', e.reason)
        self.assertEqual('agent', e.role)

    def test_a_file_holding_a_list_is_refused_as_not_a_mapping(self):
        path = self._write_bytes(b'- disable\n- traefik\n')
        e = self.assertRaises(
            exceptions.K3sConfigError, cluster_module.read_k3s_config,
            path, 'server')
        self.assertEqual('not_a_mapping', e.reason)


class CheckK3sReleaseTestCase(testtools.TestCase):
    """check_k3s_release() refuses anything older than v1.21.1, and anything it cannot read.

    v1.21.0 is the boundary worth pinning: it reads drop-in files but not
    the '+' suffix, so a cluster built on it would take tls-san+ and
    disable+ as keys with other names and say nothing.
    """

    def test_the_floor_is_v1_21_1(self):
        self.assertEqual((1, 21, 1), cluster_module.K3S_RELEASE_FLOOR)

    def test_supported_releases_are_accepted(self):
        for release in ('v1.21.1+k3s1', 'v1.36.5+k3s1', 'v2.0.0+k3s1',
                        'v1.33.4', 'v1.34.0-rc1+k3s1'):
            self.assertIsNone(
                cluster_module.check_k3s_release(release, 'stable'))

    def _assert_too_old(self, release, channel):
        e = self.assertRaises(
            exceptions.UnsupportedReleaseError,
            cluster_module.check_k3s_release, release, channel)
        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual('too_old', e.reason)
        self.assertEqual(release, e.release)
        self.assertEqual(channel, e.channel)
        self.assertEqual((1, 21, 1), e.floor)
        self.assertIn(release, str(e))
        self.assertIn(channel, str(e))
        self.assertIn('v1.21.1', str(e))
        return e

    def test_v1_21_0_is_refused(self):
        self._assert_too_old('v1.21.0+k3s1', 'v1.21')

    def test_the_last_v1_20_is_refused(self):
        self._assert_too_old('v1.20.15+k3s1', 'v1.20')

    def test_a_release_candidate_is_compared_by_its_numbers(self):
        # What the testing channel has resolved to.
        self._assert_too_old('v1.18.2-rc3+k3s1', 'testing')

    def test_a_channel_name_is_unparseable(self):
        e = self.assertRaises(
            exceptions.UnsupportedReleaseError,
            cluster_module.check_k3s_release, 'stable', 'stable')
        self.assertEqual('unparseable', e.reason)
        self.assertEqual('stable', e.release)
        self.assertIsNone(e.floor)
        self.assertIn("'stable'", str(e))

    def test_none_is_unparseable(self):
        e = self.assertRaises(
            exceptions.UnsupportedReleaseError,
            cluster_module.check_k3s_release, None, 'stable')
        self.assertEqual('unparseable', e.reason)

    def test_a_release_which_only_starts_well_is_unparseable(self):
        # The release is third-party text. A good prefix followed by a
        # terminal escape or a newline would reach too_old()'s message as
        # it stands; unparseable() renders it with repr().
        for release in ('v1.20.0\x1b[2J+k3s1', 'v1.33.4\nINJECT',
                        'v1.33.4 +k3s1', 'v1.33.4k3s1'):
            e = self.assertRaises(
                exceptions.UnsupportedReleaseError,
                cluster_module.check_k3s_release, release, 'stable')
            self.assertEqual('unparseable', e.reason)
            self.assertIn(repr(release), str(e))

    def test_a_component_must_be_a_few_ascii_digits(self):
        # \d would take digits from other scripts, which int() reads, and
        # thousands of digits make int() itself raise on newer Pythons.
        for release in ('v\uff11.\uff12\uff11.\uff11', 'v1.21.' + '1' * 5000,
                        'v1.21.1234567890'):
            e = self.assertRaises(
                exceptions.UnsupportedReleaseError,
                cluster_module.check_k3s_release, release, 'stable')
            self.assertEqual('unparseable', e.reason)


class ManifestHeredocTestCase(testtools.TestCase):
    """The command a manifest is staged with, run through a real shell.

    Asserting on the text of the command (test_library_api.py's
    ManifestStagingTestCase does) pins what this package generates. It
    cannot tell whether what it generates means what we think it does: the
    write is a shell heredoc, so a manifest arriving intact is a fact about
    shell quoting rather than about Python string formatting. This runs the
    command and compares the file which lands with the file which went in.

    Only the write is run, with its destination rewritten into a temporary
    directory. The mkdir alongside it names absolute paths under
    /var/lib/rancher and is asserted on rather than executed, because a test
    which creates those on the developer's own machine is a test which has
    misunderstood its job.
    """

    # A manifest carrying every hazard the transport has to survive: a
    # variable, both kinds of command substitution, both kinds of quote,
    # trailing whitespace, and a second document which is nothing but the
    # delimiter the config file write above it uses -- a line which would
    # have truncated the manifest had this reused that delimiter.
    HAZARDS = (
        'apiVersion: v1\n'
        'kind: ConfigMap\n'
        'metadata:\n'
        '  name: shell-hazards\n'
        'data:\n'
        '  entrypoint.sh: |\n'
        '    echo "$HOME is $(hostname) or `hostname`"\n'
        "    echo '${NOT_EXPANDED}' | tee /tmp/x\n"
        '    printf "%s\\n" "trailing   "\n'
        '---\n'
        'EOF\n'
    )

    def setUp(self):
        super(ManifestHeredocTestCase, self).setUp()
        if not os.path.exists('/bin/sh'):
            self.skipTest('this test runs the staging command through /bin/sh')

        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.tempdir = tempdir.name

        self.client = fakes.FakeClusterClient()
        self.client.metadata[MD_KEY] = {
            'name': 'banana',
            'namespace': 'testns',
            'state': 'initial',
            'node_serial': 2,
            'node_network': 'net-1',
            'k3s_version': 'v1.33',
            'api_address_floating': '192.168.10.100',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'],
            'worker_nodes': []
        }
        self.client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _stage(self, name, content):
        path = os.path.join(self.tempdir, name)
        with open(path, 'w') as f:
            f.write(content)

        _make_cluster(self.client).install_control_plane(manifests=[path])

        writes = [commandline for _, commandline in self.client.executed
                  if commandline.startswith(
                      'cat - > %s/%s ' % (cluster_module.K3S_MANIFEST_DIR, name))]
        self.assertEqual(1, len(writes), self.client.executed)

        # Run the write where it can do no harm. Only the destination
        # directory moves; the rest of the command, quoting included, is
        # what would have run on the node.
        destination = os.path.join(self.tempdir, 'manifests')
        os.makedirs(destination, exist_ok=True)
        command = writes[0].replace(cluster_module.K3S_MANIFEST_DIR, destination)
        run = subprocess.run(command, shell=True, cwd=self.tempdir,
                             capture_output=True)
        self.assertEqual(
            0, run.returncode,
            'the staging command failed: %s' % run.stderr.decode(
                'utf-8', errors='replace'))

        with open(os.path.join(destination, name)) as f:
            return f.read()

    def test_a_hazardous_manifest_arrives_unchanged(self):
        self.assertEqual(self.HAZARDS, self._stage('hazards.yaml', self.HAZARDS))

    def test_the_manifest_which_arrives_is_still_the_yaml_which_went_in(self):
        # The point of the previous test, expressed as the thing k3s will
        # do with the file: an expansion which ate a $ or a backtick could
        # leave a file which still parses but says something else.
        arrived = self._stage('hazards.yaml', self.HAZARDS)
        self.assertEqual(
            list(yaml.safe_load_all(self.HAZARDS)),
            list(yaml.safe_load_all(arrived)))

    def test_a_manifest_with_no_trailing_newline_arrives_with_one(self):
        self.assertEqual('kind: One\n', self._stage('terse.yaml', 'kind: One'))

    def test_a_manifest_which_already_ends_in_a_newline_gains_nothing(self):
        self.assertEqual('kind: One\n', self._stage('tidy.yaml', 'kind: One\n'))


class RemoveWorkerNodeNameTestCase(testtools.TestCase):
    """The name remove-worker drains is the name k3s knows, not the instance's.

    Shaken Fist accepts a capital letter in an instance name and Kubernetes
    does not accept one in a node name, so on a cluster whose name has one
    the two spellings differ. Everything else in this package addresses
    nodes by instance uuid; remove-worker is the only verb which addresses
    one by name, so it is the only verb the difference reaches.
    """

    def setUp(self):
        super(RemoveWorkerNodeNameTestCase, self).setUp()
        self.client = ActionLogClient()

        # 'MixedCase' is the cluster name a user typed. create() builds
        # instance names from it verbatim -- create_instance() does
        # 'k3s-%s-node-%03d' % (md['name'], md['node_serial']) -- and the
        # Shaken Fist API accepts that, because its instance name guard
        # permits A-Z.
        key = cluster_module.METADATA_KEY % 'MixedCase'
        self.client.metadata[key] = {
            'name': 'MixedCase',
            'namespace': 'testns',
            'state': 'created',
            'node_serial': 3,
            'node_network': 'net-1',
            'node_token': 'node-token',
            'k3s_version': 'v1.33',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'],
            'worker_nodes': ['inst-w1'],
            'routed_addresses': []
        }
        for instance_uuid, name in [('inst-cp1', 'k3s-MixedCase-node-001'),
                                    ('inst-w1', 'k3s-MixedCase-node-002')]:
            self.client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.cluster = Cluster(self.client, 'MixedCase', 'testns',
                               reporter=progress.CollectingReporter())

    def test_the_node_name_is_lowercased(self):
        self.cluster.remove_worker(['inst-w1'])

        self.assertEqual(
            [('execute', 'inst-cp1',
              'kubectl drain k3s-mixedcase-node-002 --ignore-daemonsets '
              '--delete-emptydir-data --timeout=300s '
              '--kubeconfig /etc/rancher/k3s/k3s.yaml'),
             ('execute', 'inst-cp1',
              'kubectl delete node k3s-mixedcase-node-002 '
              '--kubeconfig /etc/rancher/k3s/k3s.yaml'),
             ('delete_instance', 'inst-w1', None)],
            self.client.actions)

    def test_the_instance_name_is_never_sent_as_typed(self):
        # The failure this pins is not "the name is wrong" but "the drain
        # is aimed at a node which does not exist", which kubectl answers
        # with a non-zero exit and remove_worker turns into a
        # CommandFailedError. Asserting the absence separately means a
        # future rewrite which sends both spellings still fails here.
        self.cluster.remove_worker(['inst-w1'])

        for _, _, commandline in self.client.actions:
            if commandline:
                self.assertNotIn('k3s-MixedCase-node-002', commandline)


class ShellQuotingTestCase(testtools.TestCase):
    """Values interpolated into a command line cannot become a command.

    Rule 1 at the top of cluster.py: anything interpolated into a shell
    command line this module builds goes through shlex.quote() unless it
    is one of this module's own literals. These commands run as root on a
    cluster node, so a value which the remote shell reads as a command
    separator is a root shell on that node.

    The values here are hostile in a way the real ones cannot be today --
    the Shaken Fist API refuses an instance name containing a semicolon,
    and a k3s channel comes from a release lookup -- which is the point:
    this pins the quoting rather than the current reachability of a value
    which bypasses it.
    """

    HOSTILE = 'v1.33; touch /pwned #'

    def _install_commands(self, md):
        # mock.patch of execute_and_await rather than a scripted client,
        # matching InstallK3sComponentTestCase above: the question here is
        # what command line was built, and nothing after that matters.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: md}
        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        with mock.patch.object(Cluster, 'execute_and_await') as ea:
            cluster.install_k3s_component(['inst-w1'], 'token', 'agent')
            return list(ea.call_args[0][1])

    def test_the_k3s_channel_is_quoted(self):
        commands = self._install_commands({
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'k3s_version': self.HOSTILE, 'join_address': '10.0.0.4',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': ['inst-w1']})

        installs = [c for c in commands if 'INSTALL_K3S_CHANNEL' in c]
        self.assertEqual(1, len(installs), commands)
        self.assertIn("INSTALL_K3S_CHANNEL='%s'" % self.HOSTILE, installs[0])

    def test_the_drained_node_name_is_quoted(self):
        # The Shaken Fist API would not return an instance named this --
        # its name guard allows only letters, digits and hyphens -- which
        # is the point: a remote API's input validation is not this
        # package's trust boundary, and without the quoting this command
        # line carries a second command to run as root on the control
        # plane node.
        client = ActionLogClient()
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 2, 'node_network': 'net-1', 'node_token': 'tok',
            'k3s_version': 'v1.33', 'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': ['inst-w1'],
            'routed_addresses': []
        }
        client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        client.instances['inst-w1'] = {
            'uuid': 'inst-w1', 'name': 'node;touch /pwned',
            'state': 'created', 'agent_state': 'ready'}

        with mock.patch('time.sleep', lambda seconds: None):
            _make_cluster(client).remove_worker(['inst-w1'])

        drains = [a[2] for a in client.actions
                  if a[0] == 'execute' and a[2].startswith('kubectl drain')]
        self.assertEqual(1, len(drains), client.actions)
        self.assertIn("kubectl drain 'node;touch /pwned' ", drains[0])

        # And through a shell, which is what actually decides how many
        # commands that line is.
        with tempfile.TemporaryDirectory() as tempdir:
            probe = drains[0].replace('kubectl', 'true', 1)
            probe = probe.replace('/pwned', 'pwned')
            subprocess.run(probe, shell=True, cwd=tempdir, capture_output=True)
            self.assertEqual([], os.listdir(tempdir),
                             'the shell read the node name as a second '
                             'command: %s' % probe)

    def test_the_longhorn_version_is_quoted(self):
        # This one comes from a GitHub release lookup rather than from
        # anything this module wrote, which is the same trust position as
        # the k3s channel above.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': []}}
        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())

        with mock.patch.object(cluster_module.primitives,
                               'get_longhorn_release',
                               return_value='v1.9.0; touch /pwned'), \
                mock.patch.object(Cluster, 'execute_and_await') as ea:
            cluster.setup_longhorn()

        installs = [c for c in ea.call_args[0][1] if 'longhorn/longhorn' in c]
        self.assertEqual(1, len(installs), ea.call_args[0][1])
        self.assertIn("--version 'v1.9.0; touch /pwned'", installs[0])

    def test_the_quoted_install_runs_no_second_command(self):
        # Asserting the quoting through a shell rather than against a
        # string, the way ManifestHeredocTestCase does: the question is
        # what /bin/sh does with this line, and only /bin/sh answers it.
        commands = self._install_commands({
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'k3s_version': 'v1.33; touch pwned #', 'join_address': '10.0.0.4',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': ['inst-w1']})
        install = [c for c in commands if 'INSTALL_K3S_CHANNEL' in c][0]

        with tempfile.TemporaryDirectory() as tempdir:
            # curl is not going to be reached; the point is what the shell
            # decides the line is made of before anything runs. 'env' as a
            # harmless stand-in for the pipeline keeps this off the
            # network while leaving the word splitting intact.
            probe = install.replace('curl -sfL https://get.k3s.io | ', 'env ')
            probe = probe.replace(' sh -s - agent', ' true')
            subprocess.run(probe, shell=True, cwd=tempdir, capture_output=True)
            self.assertEqual([], os.listdir(tempdir),
                             'the shell ran something the quoting should '
                             'have made into a word: %s' % probe)


def _control_plane_and_metallb_commands():
    """Every commandline a control plane install, a k3s join and a metallb reconfigure send.

    Shared by the test cases below which assert a property over the
    generated commands rather than at each call site. They want the same
    paths driven, and a second copy of this setup is a second thing to
    forget to update.

    install_k3s_component() is driven for both roles, with a non-empty
    configuration recorded for each, because it writes k3s configuration
    files through heredocs of its own, and a property asserted over every
    heredoc is only as good as the set of commands it is asserted over.
    """
    client = fakes.FakeClusterClient()
    client.metadata[MD_KEY] = {
        'name': 'banana', 'namespace': 'testns', 'state': 'created',
        'node_serial': 1, 'node_network': 'net-1', 'node_token': None,
        'server_token': None, 'k3s_version': 'v1.33',
        'api_address_floating': '192.168.10.100',
        'api_address_inner': '10.0.0.4',
        'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
        'routed_addresses': ['192.168.10.101', '192.168.10.102'],
        'server_config': {'disable': ['traefik']},
        'agent_config': {'node-label': ['openvswitch=enabled']}
    }
    for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-001'),
                                ('inst-cp2', 'k3s-banana-node-002'),
                                ('inst-w1', 'k3s-banana-node-003')]:
        client.instances[instance_uuid] = {
            'uuid': instance_uuid, 'name': name,
            'state': 'created', 'agent_state': 'ready'}

    with tempfile.TemporaryDirectory() as tempdir:
        manifest = os.path.join(tempdir, 'staged.yaml')
        with open(manifest, 'w') as f:
            f.write('kind: One\n')

        cluster = _make_cluster(client)
        cluster.install_control_plane(manifests=[manifest])
        _make_cluster(client).install_k3s_component(
            ['inst-cp2'], 'server-token', 'server')
        _make_cluster(client).install_k3s_component(
            ['inst-w1'], 'node-token', 'agent')
        _make_cluster(client).configure_metallb_addresses()

    return [commandline for _, commandline in client.executed]


class SecretRedactionTestCase(testtools.TestCase):
    """The node token must not survive into an error message.

    install_k3s_component() has to put K3S_TOKEN= on the command line,
    because that is how the k3s installer is told which cluster to join.
    The Shaken Fist API echoes the submitted command line back in
    commands[0]['commandline'], so a worker install which exits non-zero
    hands the cluster's node token to CommandFailedError -- and from there
    to stderr, to an Ansible play's registered variables, and to a public
    CI job log. A transient apt failure is enough to trigger it.

    The command line these tests feed to reap_execute() is the real one,
    built by the real install_k3s_component(), so that a future change to
    the installer template which named the credential differently would
    fail here rather than silently stop being redacted.
    """

    TOKEN = 'K10SECRETNODETOKEN::server:ffffffff'

    def _install_commandline(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {MD_KEY: {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'k3s_version': 'v1.33', 'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': ['inst-w1']}}
        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        with mock.patch.object(Cluster, 'execute_and_await') as ea:
            cluster.install_k3s_component(['inst-w1'], self.TOKEN, 'agent')
        commands = list(ea.call_args[0][1])

        installs = [c for c in commands if 'K3S_TOKEN=' in c]
        self.assertEqual(1, len(installs), commands)
        # The premise: the real command line does carry the token. Without
        # this the rest of the test would pass against a template which
        # had stopped including it, proving nothing.
        self.assertIn(self.TOKEN, installs[0])
        return installs[0]

    def _failed_op(self, commandline, state='complete', return_code=1):
        return {
            'uuid': 'aop-100',
            'instance_uuid': 'inst-w1',
            'state': state,
            'commands': [{'command': 'execute', 'commandline': commandline}],
            'results': {'0': {'return-code': return_code,
                              'stdout': 'running %s\n' % commandline,
                              'stderr': 'E: Unable to fetch some archives'}}
        }

    def test_a_failed_install_does_not_report_the_node_token(self):
        commandline = self._install_commandline()
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'k3s-banana-node-002'}
        cluster = _make_cluster(client)

        e = self.assertRaises(exceptions.CommandFailedError,
                              cluster.reap_execute,
                              self._failed_op(commandline))

        self.assertNotIn(self.TOKEN, str(e))
        self.assertIn('K3S_TOKEN=%s' % progress.REDACTED, str(e))
        # The rest of the command line is what makes the message useful,
        # so redaction must not have eaten it.
        self.assertIn('INSTALL_K3S_CHANNEL=', str(e))
        self.assertIn('E: Unable to fetch some archives', str(e))

    def test_the_stored_attributes_carry_no_token_either(self):
        """Not only __str__, because the attributes are the public surface."""
        commandline = self._install_commandline()
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'k3s-banana-node-002'}
        cluster = _make_cluster(client)

        e = self.assertRaises(exceptions.CommandFailedError,
                              cluster.reap_execute,
                              self._failed_op(commandline))

        self.assertNotIn(self.TOKEN, e.commandline)
        self.assertNotIn(self.TOKEN, e.stdout)
        self.assertNotIn(self.TOKEN, e.stderr)

    def test_an_agent_operation_error_does_not_report_the_node_token(self):
        """The other rendering site: a state which did not run the command."""
        commandline = self._install_commandline()
        client = mock.MagicMock()
        client.get_instance.return_value = {'name': 'k3s-banana-node-002'}
        cluster = _make_cluster(client)

        e = self.assertRaises(
            exceptions.AgentOperationError, cluster.reap_execute,
            self._failed_op(commandline, state='expired', return_code=0))

        self.assertNotIn(self.TOKEN, str(e))
        self.assertNotIn(self.TOKEN, e.command_description)
        self.assertNotIn(self.TOKEN, json.dumps(e.results))
        self.assertIn('K3S_TOKEN=%s' % progress.REDACTED,
                      e.command_description)

    def test_delete_does_not_debug_log_the_cluster_secrets(self):
        """delete() dumps the metadata document, so it has to redact it.

        sf_k3s_cluster.py leaves its reporter non-verbose and calls that a
        security property because of this loop, which makes the property
        hold only for as long as one caller remembers a flag. -v is also
        exactly the flag somebody adds when a delete is failing, which is
        when the output gets pasted into a bug report.
        """
        md = _interrupted_md(state='created',
                             control_plane_nodes=['inst-001'])
        md['node_token'] = 'SECRET-NODE-TOKEN'
        md['server_token'] = 'SECRET-SERVER-TOKEN'
        md['kubeconfig'] = 'apiVersion: v1\nSECRET-KUBECONFIG\n'
        md['ssh_key'] = 'ssh-rsa SECRET-SSH-KEY'

        client = ActionLogClient()
        client.metadata[primitives.CLUSTER_LIST] = ['banana']
        client.metadata[MD_KEY] = md
        client.instances['inst-001'] = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        reporter = progress.CollectingReporter(verbose=True)

        with mock.patch('time.sleep', lambda seconds: None):
            Cluster(client, 'banana', 'testns', reporter=reporter).delete()

        out = reporter.getvalue()
        for secret in ('SECRET-NODE-TOKEN', 'SECRET-SERVER-TOKEN',
                       'SECRET-KUBECONFIG', 'SECRET-SSH-KEY'):
            self.assertNotIn(secret, out)
        for key in cluster_module.SECRET_METADATA_KEYS:
            self.assertIn('%s = %s' % (key, progress.REDACTED), out)

        # The point of a debug dump is the rest of the document, which
        # must still be there for it to be worth having.
        self.assertIn('node_network = net-1', out)
        self.assertIn('state = created', out)

    def test_delete_does_not_debug_log_the_callers_k3s_configuration(self):
        # k3s takes credentials inline as configuration keys, and nothing
        # stops a caller putting one in either file.
        md = _interrupted_md(state='created',
                             control_plane_nodes=['inst-001'])
        md['server_config'] = {'etcd-s3-secret-key': 'SECRET-S3-KEY'}
        md['agent_config'] = {}

        client = ActionLogClient()
        client.metadata[primitives.CLUSTER_LIST] = ['banana']
        client.metadata[MD_KEY] = md
        client.instances['inst-001'] = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        reporter = progress.CollectingReporter(verbose=True)

        with mock.patch('time.sleep', lambda seconds: None):
            Cluster(client, 'banana', 'testns', reporter=reporter).delete()

        out = reporter.getvalue()
        self.assertNotIn('SECRET-S3-KEY', out)
        self.assertIn('server_config = %s' % progress.REDACTED, out)
        # An empty configuration is not a secret being hidden.
        self.assertIn('agent_config = {}', out)

    def test_an_absent_secret_is_not_reported_as_redacted(self):
        """A None token is not a secret being hidden, and saying so misleads."""
        client = ActionLogClient()
        client.metadata[primitives.CLUSTER_LIST] = ['banana']
        client.metadata[MD_KEY] = _interrupted_md()
        reporter = progress.CollectingReporter(verbose=True)

        with mock.patch('time.sleep', lambda seconds: None):
            Cluster(client, 'banana', 'testns', reporter=reporter).delete()

        self.assertIn('node_token = None', reporter.getvalue())

    def test_progress_output_during_the_install_carries_no_token(self):
        """await_idle() describes what it is waiting on, from the same text."""
        commandline = self._install_commandline()
        aop = self._failed_op(commandline, return_code=0)
        aop['results'] = {}

        self.assertNotIn(self.TOKEN,
                         progress.describe_agent_op(aop, max_len=None))


class HeredocDelimiterTestCase(testtools.TestCase):
    """Every heredoc this module generates has a quoted delimiter, and no
    body which ends it early.

    Rule 2 at the top of cluster.py, both halves. An unquoted delimiter
    lets the remote shell expand $, backticks and $( ) inside the body, and
    every heredoc here carries a value Python already substituted, so there
    is nothing for the shell to be expanding. A quoted delimiter is
    necessary and not sufficient: it does not stop an interpolated value
    from ending the heredoc, and whatever follows the delimiter line is
    then read by the shell as commands, running as root on the node. Both
    are asserted over the generated commands rather than per site, because
    the failure mode is a new heredoc written the old way rather than one
    of these changing back.
    """

    def test_no_generated_heredoc_is_unquoted(self):
        heredocs = []
        for commandline in _control_plane_and_metallb_commands():
            for line in commandline.split('\n'):
                if '<<' in line:
                    heredocs.append(line)

        self.assertNotEqual([], heredocs, 'no heredoc was generated at all')
        for line in heredocs:
            introducer = line.split('<<', 1)[1].strip()
            self.assertTrue(
                introducer.startswith("'") and introducer.endswith("'"),
                'this heredoc delimiter is not quoted, so the remote shell '
                'expands the body: %s' % line)

    def test_a_metadata_address_cannot_end_the_heredoc(self):
        """The namespace metadata document is third party writable.

        Anything holding the namespace's credentials can write this
        document, and conductor writes it too, so a value containing a
        newline is not something the API's own validation rules out on
        this package's behalf -- rule 1 says so in as many words. Without
        a defence, these bodies would carry 'kubectl ...' or anything else
        the writer chose, as root on the first control plane node.

        The two sinks are defended differently, which is why both are
        checked here rather than one standing in for the other.

        The k3s configuration files are serialised by yaml.safe_dump(),
        which emits a value containing newlines as a quoted scalar whose
        continuation lines are indented. No line of the result can equal
        the delimiter, so the body cannot end its own heredoc and
        heredoc() has nothing to refuse -- the attack is answered by
        construction rather than by a check. Asserted on the generated
        command rather than trusting the serialiser, and the round trip is
        asserted too, because escaping which corrupted the value would be
        a different bug wearing this one's clothes.

        MetalLB's address list is interpolated into its body as text, so
        there the delimiter refusal in heredoc() is the whole defence and
        the call raises.
        """
        hostile = '10.0.0.1"\nEOF\ntouch /pwned\ncat - > /dev/null << \'EOF\'\nx'

        client = ActionLogClient()
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'k3s_version': 'v1.33', 'api_address_floating': hostile,
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
            'routed_addresses': [hostile]}
        client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        _make_cluster(client).install_control_plane()

        wrote = [c for _, c in client.executed
                 if cluster_module.K3S_CONFIG_DELIMITER in c]
        self.assertNotEqual([], wrote)
        for command in wrote:
            body = command.split('\n', 1)[1]
            lines = body.split('\n')
            self.assertNotIn(cluster_module.K3S_CONFIG_DELIMITER,
                             lines[:-2], command)
            self.assertNotIn('EOF', lines, command)

        main = [c for _, c in client.executed
                if '/etc/rancher/k3s/config.yaml' in c
                and 'config.yaml.d' not in c]
        self.assertEqual(1, len(main), client.executed)
        body = main[0].split('\n', 1)[1]
        body = body[:body.rindex(cluster_module.K3S_CONFIG_DELIMITER)]
        self.assertEqual([hostile], yaml.safe_load(body)['tls-san'])

        e = self.assertRaises(
            exceptions.GuestFileError,
            _make_cluster(client).configure_metallb_addresses)
        self.assertEqual('delimiter_collision', e.reason)
        self.assertEqual('/etc/sf/metallb-range-allocation.yaml', e.path)

    def test_heredoc_builds_what_it_used_to_build(self):
        """The builder is a refactor of three identical string literals."""
        self.assertEqual(
            "cat - > /etc/sf/thing.yaml << 'EOF'\nkey: value\nEOF\n",
            cluster_module.heredoc('/etc/sf/thing.yaml', 'key: value\n'))

    def test_heredoc_adds_exactly_one_trailing_newline(self):
        self.assertEqual(
            "cat - > /etc/sf/thing.yaml << 'EOF'\nkey: value\nEOF\n",
            cluster_module.heredoc('/etc/sf/thing.yaml', 'key: value'))

    def test_heredoc_quotes_the_destination(self):
        self.assertIn(
            "cat - > '/etc/sf/a b.yaml'",
            cluster_module.heredoc('/etc/sf/a b.yaml', 'x\n'))

    def test_heredoc_honours_a_custom_delimiter(self):
        e = self.assertRaises(
            exceptions.GuestFileError, cluster_module.heredoc,
            '/etc/sf/thing.yaml', 'SFK3SMANIFEST\n',
            cluster_module.K3S_MANIFEST_DELIMITER)
        self.assertEqual(cluster_module.K3S_MANIFEST_DELIMITER, e.delimiter)

    def test_the_k3s_configuration_heredocs_are_among_those_checked(self):
        # The check above is only as wide as the commands it is given.
        # install_k3s_component() generated no heredoc until it started
        # writing k3s configuration, and nothing failed when the shared
        # helper did not drive it; this is what fails if it stops doing so.
        # One config.yaml for each of the three nodes the helper installs,
        # and each drop-in at least once.
        destinations = [line.split(' << ', 1)[0][len('cat - > '):]
                        for commandline in _control_plane_and_metallb_commands()
                        for line in commandline.split('\n')
                        if line.startswith('cat - > /etc/rancher/k3s/')]
        self.assertEqual(3, destinations.count('/etc/rancher/k3s/config.yaml'),
                         destinations)
        for drop_in in ('50-sf-client-k3s.yaml',
                        '90-sf-client-k3s-enforced.yaml'):
            self.assertIn('/etc/rancher/k3s/config.yaml.d/' + drop_in,
                          destinations)


class NodeSignalsCommandTestCase(testtools.TestCase):
    """The command health() will run on each node to take its signals.

    Decisions 5 and 6 of the cumulative health signals phase 1 plan: the
    k3s unit is chosen by role, because a worker's is k3s-agent and asking
    it about k3s reports a unit which does not exist as never having
    restarted; and etcd is sized on control plane nodes only, at the
    caller's etcd-snapshot-dir when there is one. That directory is the
    one value in the command which is not a literal of cluster.py, so it
    is the one rule 1 applies to.
    """

    def test_a_control_plane_node_reads_k3s_and_etcd(self):
        command = cluster_module.node_signals_command('control_plane')

        self.assertIn('systemctl show k3s -p LoadState -p ActiveState '
                      '-p NRestarts', command)
        self.assertNotIn('k3s-agent', command)
        self.assertIn('etcd_bytes=', command)
        self.assertIn('etcd_snapshot_bytes=', command)
        self.assertIn('du -sb -- %s ' % cluster_module.K3S_ETCD_DIR, command)
        self.assertIn('du -sb -- %s ' % cluster_module.K3S_ETCD_SNAPSHOT_DIR,
                      command)

    def test_a_worker_reads_k3s_agent_and_no_etcd(self):
        command = cluster_module.node_signals_command('worker')

        self.assertIn('systemctl show k3s-agent -p LoadState -p ActiveState '
                      '-p NRestarts', command)
        self.assertNotIn('systemctl show k3s ', command)
        self.assertNotIn('etcd', command)
        self.assertNotIn('du ', command)

    def test_every_role_reads_the_same_proc_signals(self):
        for role in ('control_plane', 'worker'):
            command = cluster_module.node_signals_command(role)
            for key in ('boot_id', 'booted_at', 'oom_kills',
                        'memory_total_kb', 'memory_available_kb'):
                self.assertIn("printf '%s=%%s\\n'" % key, command)
            self.assertIn('/proc/sys/kernel/random/boot_id', command)
            self.assertIn('"btime"', command)
            self.assertIn('"oom_kill"', command)
            self.assertIn('"MemTotal:"', command)
            self.assertIn('"MemAvailable:"', command)

    def test_the_command_is_one_line(self):
        # The agent takes one command line. The '\n' in each printf format
        # is a backslash and an n, for printf to interpret on the node.
        for role in ('control_plane', 'worker'):
            self.assertNotIn(
                '\n', cluster_module.node_signals_command(role, '/x'))

    def test_a_failing_systemctl_does_not_fail_the_command(self):
        # On a worker the systemctl call is the last command, so its exit
        # status would be the command line's.
        for role in ('control_plane', 'worker'):
            self.assertIn(
                'NRestarts || true',
                cluster_module.node_signals_command(role))

    def test_the_snapshot_directory_is_quoted(self):
        snapshot_dir = '/srv/etcd snaps/$HOME; touch /pwned'
        command = cluster_module.node_signals_command(
            'control_plane', snapshot_dir)

        self.assertIn('du -sb -- %s 2>/dev/null' % shlex.quote(snapshot_dir),
                      command)
        self.assertNotIn(snapshot_dir,
                         command.replace(shlex.quote(snapshot_dir), ''))
        self.assertNotIn(cluster_module.K3S_ETCD_SNAPSHOT_DIR, command)

    def test_an_empty_snapshot_directory_is_the_default(self):
        # As it is to k3s, which uses its default for an empty
        # etcd-snapshot-dir.
        self.assertEqual(
            cluster_module.node_signals_command('control_plane'),
            cluster_module.node_signals_command('control_plane', ''))

    def test_a_relative_snapshot_directory_is_not_sized(self):
        # du would resolve it against the agent's working directory, which
        # need not be what k3s resolved it against, and a size of the wrong
        # directory is worse than no size. The key is still printed, empty,
        # so it parses to None like any reading which could not be taken.
        for snapshot_dir in ('snapshots', './snapshots', '../var/snaps',
                             'srv/etcd snaps/$HOME'):
            command = cluster_module.node_signals_command(
                'control_plane', snapshot_dir)

            # The command ends with the empty reading, and neither the
            # caller's directory nor the default one is sized instead: the
            # one du left is the etcd member's.
            self.assertTrue(
                command.endswith("; printf 'etcd_snapshot_bytes=\\n'"),
                command)
            self.assertNotIn(snapshot_dir, command)
            self.assertNotIn(shlex.quote(snapshot_dir), command)
            self.assertEqual(1, command.count('du -sb'), snapshot_dir)
            self.assertIn('du -sb -- %s ' % cluster_module.K3S_ETCD_DIR,
                          command)
            self.assertNotIn(cluster_module.K3S_ETCD_SNAPSHOT_DIR, command,
                             snapshot_dir)

    def test_an_absolute_snapshot_directory_is_still_sized(self):
        # The negative of the above: only the leading slash decides.
        command = cluster_module.node_signals_command(
            'control_plane', '/srv/snapshots')

        self.assertEqual(2, command.count('du -sb'))
        self.assertIn('du -sb -- /srv/snapshots 2>/dev/null', command)

    def test_a_worker_ignores_the_snapshot_directory(self):
        self.assertEqual(
            cluster_module.node_signals_command('worker'),
            cluster_module.node_signals_command('worker', '/srv/snaps'))

    def test_an_unknown_role_is_refused(self):
        # k3s's own role names are the likeliest mistake, because
        # install_k3s_component() is handed those.
        for role in ('server', 'agent', 'controlplane', '', None,
                     ['worker']):
            self.assertRaises(ValueError,
                              cluster_module.node_signals_command, role)
            self.assertRaises(ValueError,
                              cluster_module.parse_node_signals, '', role)


# Defined beside HealthClient, which answers the signals probe with them, so
# that what the parser is tested on and what health() is tested with are
# the same output.
SERVER_SIGNALS_OUTPUT = fakes.SERVER_SIGNALS_OUTPUT
WORKER_SIGNALS_OUTPUT = fakes.WORKER_SIGNALS_OUTPUT


class ParseNodeSignalsTestCase(testtools.TestCase):
    """What a node's signals output becomes in health()'s report.

    Decision 1 of the cumulative health signals phase 1 plan: always the
    same keys, a reading which could not be taken is None on its own, and
    a zero is only reported where it was measured. The output comes from a
    node which may be unwell, so nothing a node prints may make this raise.
    """

    def test_a_realistic_server_output(self):
        self.assertEqual(
            {
                'boot_id': '3f0c3c4e-5b8e-4f43-9d1c-0d6a8f2b7e11',
                'booted_at': 1759712345,
                'k3s_unit': 'k3s',
                'k3s_state': 'active',
                'k3s_restarts': 3,
                'oom_kills': 2,
                'memory_total_bytes': 4022148 * 1024,
                'memory_available_bytes': 2876544 * 1024,
                'etcd_bytes': 68321280,
                'etcd_snapshot_bytes': 41943040,
            },
            cluster_module.parse_node_signals(
                SERVER_SIGNALS_OUTPUT, 'control_plane'))

    def test_a_realistic_worker_output(self):
        self.assertEqual(
            {
                'boot_id': '9a1d7c22-0e4b-4c5f-a0b3-77c1e2d4f6a8',
                'booted_at': 1759712399,
                'k3s_unit': 'k3s-agent',
                'k3s_state': 'activating',
                'k3s_restarts': 0,
                'oom_kills': 0,
                'memory_total_bytes': 2010264 * 1024,
                'memory_available_bytes': 1102336 * 1024,
                'etcd_bytes': None,
                'etcd_snapshot_bytes': None,
            },
            cluster_module.parse_node_signals(
                WORKER_SIGNALS_OUTPUT, 'worker'))

    def test_the_key_set_is_always_the_same(self):
        expected = set(cluster_module.NODE_SIGNAL_KEYS)
        self.assertEqual(10, len(expected))
        for stdout, role in ((SERVER_SIGNALS_OUTPUT, 'control_plane'),
                             (WORKER_SIGNALS_OUTPUT, 'worker'),
                             ('', 'control_plane'), (None, 'worker'),
                             ('rubbish', 'worker')):
            self.assertEqual(
                expected,
                set(cluster_module.parse_node_signals(stdout, role)))

    def test_a_unit_which_is_not_loaded_has_no_state_or_restarts(self):
        # What systemctl prints for a unit which does not exist: a zero
        # restart count which would otherwise read as a measurement.
        stdout = SERVER_SIGNALS_OUTPUT.replace(
            'LoadState=loaded', 'LoadState=not-found').replace(
            'NRestarts=3', 'NRestarts=0').replace(
            'ActiveState=active', 'ActiveState=inactive')
        signals = cluster_module.parse_node_signals(stdout, 'control_plane')

        self.assertEqual('k3s', signals['k3s_unit'])
        self.assertIsNone(signals['k3s_state'])
        self.assertIsNone(signals['k3s_restarts'])
        # The other readings are not voided by it.
        self.assertEqual(2, signals['oom_kills'])
        self.assertEqual(68321280, signals['etcd_bytes'])

    def test_a_missing_load_state_is_not_loaded(self):
        stdout = SERVER_SIGNALS_OUTPUT.replace('LoadState=loaded\n', '')
        signals = cluster_module.parse_node_signals(stdout, 'control_plane')
        self.assertIsNone(signals['k3s_state'])
        self.assertIsNone(signals['k3s_restarts'])

    def test_empty_output_is_all_none_but_the_unit(self):
        for stdout in ('', None, '\n\n'):
            for role, unit in (('control_plane', 'k3s'),
                               ('worker', 'k3s-agent')):
                expected = dict.fromkeys(cluster_module.NODE_SIGNAL_KEYS)
                expected['k3s_unit'] = unit
                self.assertEqual(
                    expected,
                    cluster_module.parse_node_signals(stdout, role))

    def test_readings_which_could_not_be_taken_are_none_on_their_own(self):
        # What the command prints when a source is unreadable: the key,
        # with nothing after it.
        stdout = SERVER_SIGNALS_OUTPUT.replace(
            'oom_kills=2', 'oom_kills=').replace(
            'boot_id=3f0c3c4e-5b8e-4f43-9d1c-0d6a8f2b7e11', 'boot_id=').replace(
            'etcd_snapshot_bytes=41943040', 'etcd_snapshot_bytes=')
        signals = cluster_module.parse_node_signals(stdout, 'control_plane')

        self.assertIsNone(signals['oom_kills'])
        self.assertIsNone(signals['boot_id'])
        self.assertIsNone(signals['etcd_snapshot_bytes'])
        self.assertEqual(1759712345, signals['booted_at'])
        self.assertEqual(68321280, signals['etcd_bytes'])
        self.assertEqual(3, signals['k3s_restarts'])

    def test_garbage_values_are_none(self):
        # Each is something int() would either refuse or, worse, accept:
        # a sign, a digit separator, non-ASCII digits, and a string longer
        # than Python 3.11 will convert at all.
        stdout = (
            'boot_id=   \n'
            'booted_at=yesterday\n'
            'oom_kills=-1\n'
            'memory_total_kb=12.5\n'
            'memory_available_kb=١٢٣\n'
            'LoadState=loaded\n'
            'ActiveState=\n'
            'NRestarts=+3\n'
            'etcd_bytes=1_000\n'
            'etcd_snapshot_bytes=%s\n' % ('9' * 5000))
        signals = cluster_module.parse_node_signals(stdout, 'control_plane')

        expected = dict.fromkeys(cluster_module.NODE_SIGNAL_KEYS)
        expected['k3s_unit'] = 'k3s'
        self.assertEqual(expected, signals)

    def test_a_reading_is_at_most_twenty_digits_and_always_serialises(self):
        # Twenty digits is 2**64 - 1's length, the most any reading here
        # can hold, and is accepted; twenty-one is not a reading. The cap
        # is what keeps the report serialisable: memory is scaled by 1024
        # after parsing, and json.dumps() refuses an int of more than 4300
        # digits on Python 3.11 and later, which would break the Ansible
        # module's result over one garbage line.
        twenty = '9' * 20
        stdout = ''.join('%s=%s\n' % (key, twenty) for key in (
            'booted_at', 'oom_kills', 'memory_total_kb',
            'memory_available_kb', 'NRestarts', 'etcd_bytes',
            'etcd_snapshot_bytes'))
        signals = cluster_module.parse_node_signals(
            'LoadState=loaded\n' + stdout, 'control_plane')

        self.assertEqual(int(twenty), signals['oom_kills'])
        self.assertEqual(int(twenty) * 1024, signals['memory_total_bytes'])
        json.dumps(signals)

        signals = cluster_module.parse_node_signals(
            'oom_kills=%s\n' % ('9' * 21), 'control_plane')
        self.assertIsNone(signals['oom_kills'])

        signals = cluster_module.parse_node_signals(
            'memory_total_kb=%s\n' % ('9' * 4300), 'control_plane')
        self.assertIsNone(signals['memory_total_bytes'])
        json.dumps(signals)

    def test_trailing_whitespace_and_crlf_are_stripped(self):
        stdout = SERVER_SIGNALS_OUTPUT.replace('\n', ' \r\n').replace(
            'oom_kills=2', 'oom_kills=\t2')
        self.assertEqual(
            cluster_module.parse_node_signals(
                SERVER_SIGNALS_OUTPUT, 'control_plane'),
            cluster_module.parse_node_signals(stdout, 'control_plane'))

    def test_a_worker_has_no_etcd_whatever_it_prints(self):
        stdout = WORKER_SIGNALS_OUTPUT + (
            'etcd_bytes=68321280\n'
            'etcd_snapshot_bytes=41943040\n')
        signals = cluster_module.parse_node_signals(stdout, 'worker')
        self.assertIsNone(signals['etcd_bytes'])
        self.assertIsNone(signals['etcd_snapshot_bytes'])

    def test_lines_and_keys_which_are_not_readings_are_ignored(self):
        stdout = ('sh: 1: something: not found\n'
                  'Unit=k3s\n'
                  'k3s_unit=k3s-agent\n'
                  '=7\n' + SERVER_SIGNALS_OUTPUT)
        self.assertEqual(
            cluster_module.parse_node_signals(
                SERVER_SIGNALS_OUTPUT, 'control_plane'),
            cluster_module.parse_node_signals(stdout, 'control_plane'))

    def test_a_value_is_split_from_its_key_at_the_first_equals(self):
        # And kept whole, so a value with a second '=' in it is not a
        # reading -- no reading contains one -- rather than being cut down
        # to the part which looks like one. The line still claims its key,
        # so the first-occurrence rule applies to it as to any other.
        stdout = SERVER_SIGNALS_OUTPUT.replace(
            'ActiveState=active', 'ActiveState=active=x').replace(
            'oom_kills=2', 'oom_kills=2=3')
        signals = cluster_module.parse_node_signals(stdout, 'control_plane')

        self.assertIsNone(signals['k3s_state'])
        self.assertIsNone(signals['oom_kills'])
        self.assertEqual(3, signals['k3s_restarts'])

    def test_a_boot_id_is_a_uuid_or_none(self):
        # boot_id is what a caller compares to decide whether the node
        # rebooted, so anything which is not one is None rather than a
        # string which differs from the baseline and reads as a reboot.
        uuid = '3f0c3c4e-5b8e-4f43-9d1c-0d6a8f2b7e11'
        for value, expected in (
                (uuid, uuid),
                # Either case is accepted and reported in lowercase, so one
                # boot is always one string.
                (uuid.upper(), uuid),
                ('', None),
                ('abc', None),
                ('evil', None),
                (uuid[:-1], None),
                (uuid + '0', None),
                (uuid + '-' + uuid, None),
                (uuid.replace('-', ''), None),
                ('{%s}' % uuid, None),
                (uuid.replace('f', 'g'), None),
                # Non-ASCII digits, which int() would take and a \d would
                # match.
                (uuid.replace('3', '\u0663'), None),
                ('x' * 5000, None)):
            stdout = SERVER_SIGNALS_OUTPUT.replace(
                'boot_id=' + uuid, 'boot_id=' + value)
            signals = cluster_module.parse_node_signals(
                stdout, 'control_plane')
            self.assertEqual(expected, signals['boot_id'], value)

    def test_a_k3s_state_is_an_active_state_or_none(self):
        for value in ('active', 'inactive', 'activating', 'deactivating',
                      'failed', 'reloading', 'maintenance', 'refreshing',
                      'some-future-state', 'a' * 32):
            stdout = SERVER_SIGNALS_OUTPUT.replace(
                'ActiveState=active', 'ActiveState=' + value)
            self.assertEqual(
                value,
                cluster_module.parse_node_signals(
                    stdout, 'control_plane')['k3s_state'])

        for value in ('', 'Active', 'ACTIVE', 'active (running)',
                      'active;rm -rf /', 'active\x00', 'a' * 33, 'x' * 5000,
                      'act1ve', 'active_state', '\u00e4ctive'):
            stdout = SERVER_SIGNALS_OUTPUT.replace(
                'ActiveState=active', 'ActiveState=' + value)
            signals = cluster_module.parse_node_signals(
                stdout, 'control_plane')
            self.assertIsNone(signals['k3s_state'], value)
            # It voids that reading alone.
            self.assertEqual(3, signals['k3s_restarts'], value)

    def test_the_first_occurrence_of_a_key_wins(self):
        # The snapshot size is printed last and is the one value derived
        # from caller data, so a directory name holding a newline and a
        # line of its own cannot replace a reading printed before it.
        stdout = SERVER_SIGNALS_OUTPUT + (
            'boot_id=0b5e7d3a-2c41-4f6e-8a90-1d2c3b4a5f60\n'
            'ActiveState=failed\nNRestarts=0\n')
        signals = cluster_module.parse_node_signals(stdout, 'control_plane')
        # Valid readings every one, so that it is the order which keeps
        # them out and not the shape checks.
        self.assertEqual('3f0c3c4e-5b8e-4f43-9d1c-0d6a8f2b7e11',
                         signals['boot_id'])
        self.assertEqual('active', signals['k3s_state'])
        self.assertEqual(3, signals['k3s_restarts'])

    def test_no_string_makes_it_raise(self):
        for stdout in ('=', '==', '\x00=\x00', 'booted_at=\udcff',
                       '\n'.join('%s=%s' % (k, k)
                                 for k in cluster_module.NODE_SIGNAL_KEYS),
                       'LoadState=loaded\nNRestarts=²'):
            for role in ('control_plane', 'worker'):
                cluster_module.parse_node_signals(stdout, role)


class NodeSignalsCommandRunsTestCase(testtools.TestCase):
    """The command parses as shell and survives its own quoting.

    The other tests compare the command with strings, which cannot tell
    whether the awk programs' quotes nest correctly inside the command
    substitutions, or whether a quoted snapshot directory reaches du
    intact. This runs it with the local /bin/sh. systemctl is replaced by
    a script on PATH, so that the unit's readings are this test's rather
    than the host's. The /proc readings are the host's, so the only
    assertions about them are that they are there and are numbers; it is
    skipped where there is no /proc to read.
    """

    def setUp(self):
        super().setUp()
        if not sys.platform.startswith('linux'):
            self.skipTest('the command reads Linux /proc files')
        for path in ('/bin/sh', '/proc/stat', '/proc/meminfo'):
            if not os.path.exists(path):
                self.skipTest('%s does not exist' % path)

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.bin = os.path.join(self.tmp, 'bin')
        os.mkdir(self.bin)

    def _systemctl(self, script):
        path = os.path.join(self.bin, 'systemctl')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('#!/bin/sh\n' + script)
        os.chmod(path, 0o755)

    def _run(self, role, snapshot_dir=None, cwd=None):
        env = dict(os.environ)
        env['PATH'] = '%s:%s' % (self.bin, env.get('PATH', '/usr/bin:/bin'))
        return subprocess.run(
            ['/bin/sh', '-c',
             cluster_module.node_signals_command(role, snapshot_dir)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, env=env, cwd=cwd)

    def test_a_control_plane_node(self):
        self._systemctl(
            'printf "NRestarts=4\\nLoadState=loaded\\nActiveState=active\\n"\n')
        # Every character rule 1 exists for, in the one value it covers.
        snapshot_dir = os.path.join(
            self.tmp, 'snaps $HOME \'q\' "dq" ) `id`; x')
        os.mkdir(snapshot_dir)
        with open(os.path.join(snapshot_dir, 'snapshot'), 'wb') as f:
            f.write(b'x' * 5000)

        result = self._run('control_plane', snapshot_dir)
        self.assertEqual(0, result.returncode, result.stderr)

        signals = cluster_module.parse_node_signals(
            result.stdout, 'control_plane')
        self.assertIsInstance(signals['booted_at'], int, result.stdout)
        self.assertIsInstance(signals['memory_total_bytes'], int,
                              result.stdout)
        self.assertEqual('active', signals['k3s_state'])
        self.assertEqual(4, signals['k3s_restarts'])
        # du -sb counts the directory as well as the file.
        self.assertGreaterEqual(signals['etcd_snapshot_bytes'], 5000,
                                result.stdout)

    def test_a_worker_whose_systemctl_fails(self):
        # The last command on a worker, and the one whose exit status
        # '|| true' keeps from becoming the probe's.
        self._systemctl(
            'echo "System has not been booted with systemd" >&2\nexit 1\n')

        result = self._run('worker')
        self.assertEqual(0, result.returncode, result.stderr)

        signals = cluster_module.parse_node_signals(result.stdout, 'worker')
        self.assertIsInstance(signals['booted_at'], int, result.stdout)
        self.assertIsNone(signals['k3s_state'])
        self.assertIsNone(signals['k3s_restarts'])

    def test_a_missing_directory_is_none_not_zero(self):
        self._systemctl('exit 0\n')
        result = self._run('control_plane',
                           os.path.join(self.tmp, 'does-not-exist'))
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn('etcd_snapshot_bytes=\n', result.stdout)
        self.assertIsNone(cluster_module.parse_node_signals(
            result.stdout, 'control_plane')['etcd_snapshot_bytes'])

    def test_a_relative_directory_is_none_even_where_it_resolves(self):
        # The command is run from a working directory in which the relative
        # name does exist and has something in it, which is the case a du
        # would have answered with a confident size for a directory nobody
        # knows is the one k3s writes to.
        self._systemctl('exit 0\n')
        os.mkdir(os.path.join(self.tmp, 'snaps'))
        with open(os.path.join(self.tmp, 'snaps', 'snapshot'), 'wb') as f:
            f.write(b'x' * 5000)

        result = self._run('control_plane', 'snaps', cwd=self.tmp)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn('etcd_snapshot_bytes=\n', result.stdout)
        self.assertIsNone(cluster_module.parse_node_signals(
            result.stdout, 'control_plane')['etcd_snapshot_bytes'])


KUBECONFIG_PATH = '/etc/rancher/k3s/k3s.yaml'

# A go-template action: everything between '{{' and '}}'. Neither template
# holds a '}}' inside a string literal, so a non-greedy match finds each.
GO_TEMPLATE_ACTION_RE = re.compile(r'{{(.*?)}}')
GO_TEMPLATE_STRING_RE = re.compile(r'"[^"]*"')


def _go_template_path(token):
    """(root, [fields]) if token reads a field path, else None.

    root is '' for a path from the dot, or the variable it starts from.
    """
    if token.startswith('.') and token != '.':
        return '', token[1:].split('.')
    if token.startswith('$') and '.' in token:
        root, rest = token.split('.', 1)
        return root, rest.split('.')
    return None


def _go_template_unguarded_reads(template):
    """List every read in template which a missing field could break.

    Three rules, each one kubectl's template engine needs for the probe to
    survive an object which lacks a field (see _node_condition_template()
    in cluster.py):

    - a path two or more fields deep sits inside an 'if' on its parent,
      in the same dot, so the parent is known to exist;
    - 'eq' compares only a value an enclosing 'if' has tested, because
      the oldest supported kubectl's eq fails on a missing one;
    - a field printed on its own sits inside an 'if' on itself, or
      kubectl's 'exists' for it, so a missing one prints an empty field
      rather than '<no value>'.

    Also reports blocks which do not balance. This is a check of the
    template's shape, written because no Go template engine can be relied
    on where the unit tests run; it is not a template engine, and step 2e
    of the cumulative health signals phase 2 plan runs the real one.
    """
    problems = []
    # (keyword, its argument as written), innermost last.
    stack = []

    def guarded(root, condition, also=None):
        for keyword, argument in reversed(stack):
            if keyword == 'if' and argument in (condition, also):
                return True
            # range and with move the dot, so an 'if' outside them tested
            # some other object's field. A variable keeps its value.
            if keyword in ('range', 'with') and root == '':
                return False
        return False

    for match in GO_TEMPLATE_ACTION_RE.finditer(template):
        action = match.group(1).strip()
        tokens = GO_TEMPLATE_STRING_RE.sub('""', action).split()
        keyword = tokens[0] if tokens[0] in ('if', 'range', 'with', 'end',
                                             'else') else None

        if keyword == 'end':
            if not stack:
                problems.append('an {{end}} closes nothing')
            else:
                stack.pop()
            continue
        if keyword == 'else':
            problems.append('{{%s}} is not checked by this test' % action)
            continue

        for token in tokens:
            path = _go_template_path(token)
            if path and len(path[1]) > 1:
                root, fields = path
                parent = root + '.' + '.'.join(fields[:-1])
                if not guarded(root, parent):
                    problems.append('%s is read without testing %s'
                                    % (token, parent))

        if 'eq' in tokens:
            operand = tokens[tokens.index('eq') + 1]
            path = _go_template_path(operand)
            if not path or not guarded(path[0], operand):
                problems.append('eq compares %s without testing it'
                                % operand)

        path = _go_template_path(tokens[0])
        if keyword is None and len(tokens) == 1 and path:
            exists = None
            if path[0] == '' and len(path[1]) == 1:
                exists = 'exists . "%s"' % path[1][0]
            if not guarded(path[0], tokens[0], exists):
                problems.append('%s is printed without testing it'
                                % tokens[0])

        if keyword is not None:
            stack.append((keyword, action[len(keyword):].strip()))

    if stack:
        problems.append('%d blocks are never closed' % len(stack))
    return problems


class KubernetesProbeCommandTestCase(testtools.TestCase):
    """The command health() will run to read Kubernetes about every node.

    Decision 9 of the cumulative health signals phase 2 plan: two kubectl
    reads, each rendered by a go-template into one short line per fact,
    joined so that either failing fails the command. No Go template engine
    is available where these tests run, so the templates are checked for
    the shape kubectl needs rather than rendered; they were rendered by
    kubectl v1.21.1+k3s1 and v1.31.4+k3s1 when written, and step 2e of that
    plan renders them on a real cluster.
    """

    def test_the_command_reads_nodes_then_pods(self):
        self.assertEqual(
            ['kubectl', 'get', 'nodes', '--kubeconfig', KUBECONFIG_PATH,
             '-o', 'go-template=' + cluster_module.KUBERNETES_NODES_TEMPLATE,
             '&&',
             'kubectl', 'get', 'pods', '-A', '--kubeconfig', KUBECONFIG_PATH,
             '-o', 'go-template=' + cluster_module.KUBERNETES_PODS_TEMPLATE],
            shlex.split(cluster_module.K3S_KUBERNETES_PROBE_COMMAND))

    def test_the_command_is_one_line(self):
        # The agent takes one command line. The separators are Go string
        # escapes, a backslash and a letter, for the template engine.
        for character in ('\n', '\t', '\r'):
            self.assertNotIn(
                character, cluster_module.K3S_KUBERNETES_PROBE_COMMAND)

    def test_each_template_is_one_single_quoted_word(self):
        # Single quotes, because the templates are full of $ and double
        # quotes; and no single quote inside either, so that shlex.quote()
        # has nothing to escape and the operation log shows each template
        # as it is.
        for template in (cluster_module.KUBERNETES_NODES_TEMPLATE,
                         cluster_module.KUBERNETES_PODS_TEMPLATE):
            self.assertNotIn("'", template)
            self.assertIn(" -o go-template='%s'" % template,
                          cluster_module.K3S_KUBERNETES_PROBE_COMMAND)

    def test_no_field_a_missing_one_could_break_is_read_unguarded(self):
        for template in (cluster_module.KUBERNETES_NODES_TEMPLATE,
                         cluster_module.KUBERNETES_PODS_TEMPLATE):
            self.assertEqual([], _go_template_unguarded_reads(template))

    def test_the_guard_check_finds_unguarded_reads(self):
        # The check above passing is only worth something if it can fail.
        for template in (
                '{{.a.b}}',
                '{{if .a}}{{end}}{{.a.b}}',
                '{{if .a}}{{range .items}}{{.a.b}}{{end}}{{end}}',
                '{{if eq .type "Ready"}}{{end}}',
                '{{range .items}}{{.name}}{{end}}',
                '{{if $pod}}{{$pod.spec.nodeName}}{{end}}',
                '{{if .a}}',
                '{{end}}'):
            self.assertNotEqual(
                [], _go_template_unguarded_reads(template), template)
        # And a variable keeps its value inside a range.
        self.assertEqual([], _go_template_unguarded_reads(
            '{{if $pod.spec}}{{range .items}}{{if $pod.spec.nodeName}}'
            '{{$pod.spec.nodeName}}{{end}}{{end}}{{end}}'))

    def test_the_only_text_is_the_record_types(self):
        # Everything else -- every separator and every value -- comes from
        # an action, so the only literal text is each line's type.
        self.assertEqual('node', GO_TEMPLATE_ACTION_RE.sub(
            '', cluster_module.KUBERNETES_NODES_TEMPLATE))
        # Two kinds of status, and two places a kill is reported in each.
        self.assertEqual('oom' * 4, GO_TEMPLATE_ACTION_RE.sub(
            '', cluster_module.KUBERNETES_PODS_TEMPLATE))

    def test_each_line_has_its_records_fields(self):
        # One tab fewer than the parser's field count for the type, the
        # type itself being a field, and one newline.
        nodes = cluster_module.KUBERNETES_NODES_TEMPLATE
        self.assertEqual(6, nodes.count('{{"\\t"}}'))
        self.assertEqual(1, nodes.count('{{"\\n"}}'))
        pods = cluster_module.KUBERNETES_PODS_TEMPLATE
        self.assertEqual(4 * 6, pods.count('{{"\\t"}}'))
        self.assertEqual(4, pods.count('{{"\\n"}}'))

    def test_every_condition_is_read(self):
        nodes = cluster_module.KUBERNETES_NODES_TEMPLATE
        for condition, count in (('Ready', 2), ('MemoryPressure', 1),
                                 ('DiskPressure', 1), ('PIDPressure', 1)):
            self.assertEqual(
                count, nodes.count('{{if eq .type "%s"}}' % condition))
        self.assertIn('{{.lastTransitionTime}}', nodes)

    def test_both_states_of_every_container_are_read(self):
        # Init containers too: one killed for OOM holds its pod in
        # Init:CrashLoopBackOff, which is worth seeing.
        pods = cluster_module.KUBERNETES_PODS_TEMPLATE
        self.assertEqual(1, pods.count('{{range .status.containerStatuses}}'))
        self.assertEqual(
            1, pods.count('{{range .status.initContainerStatuses}}'))
        for state in ('state', 'lastState'):
            self.assertEqual(
                2,
                pods.count('{{if eq .%s.terminated.reason "OOMKilled"}}'
                           % state))
            self.assertEqual(
                2, pods.count('{{.%s.terminated.finishedAt}}' % state))

    def test_a_restart_count_of_zero_is_printed(self):
        # 'if' is false for 0, which is the count of a container killed for
        # the first time and not yet restarted, so the count is tested with
        # kubectl's exists rather than for truth.
        pods = cluster_module.KUBERNETES_PODS_TEMPLATE
        self.assertEqual(
            4, pods.count('{{if exists . "restartCount"}}'
                          '{{.restartCount}}{{end}}'))
        self.assertNotIn('{{if .restartCount}}', pods)

    def test_a_container_killed_twice_is_reported_once_from_its_state(self):
        # The second test runs only if the first did not print, so the
        # newer of two kills is the one reported.
        pods = cluster_module.KUBERNETES_PODS_TEMPLATE
        state = ('{{if eq .state.terminated.reason "OOMKilled"}}'
                 '{{$reported = true}}')
        last = '{{if not $reported}}{{if .lastState}}'
        for container in ('{{range .status.containerStatuses}}',
                          '{{range .status.initContainerStatuses}}'):
            body = pods[pods.index(container):]
            self.assertIn(state, body)
            self.assertIn(last, body)
            self.assertLess(body.index(state), body.index(last))


# What K3S_KUBERNETES_PROBE_COMMAND prints for a two node cluster on which
# one container has been killed for running out of memory: a 'node' line per
# node, then an 'oom' line for the container, in the shape kubectl rendered
# the templates in when they were written. Unix seconds for each time are
# given beside it, worked out with 'date -u' rather than with the code
# under test.
KUBERNETES_PROBE_OUTPUT = (
    'node\tk3s-banana-node-001\tTrue\tFalse\tFalse\tFalse\t'
    '2026-10-05T08:12:25Z\n'
    'node\tk3s-banana-node-002\tTrue\tFalse\tFalse\tFalse\t'
    '2026-10-05T08:13:02Z\n'
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
NODE_LINE = KUBERNETES_PROBE_OUTPUT.splitlines()[0]
OOM_LINE = KUBERNETES_PROBE_OUTPUT.splitlines()[2]


def _with_field(line, index, value):
    """line with its tab separated field index replaced by value."""
    fields = line.split('\t')
    fields[index] = value
    return '\t'.join(fields)


class KubernetesProbeCommandRunsTestCase(testtools.TestCase):
    """The command parses as shell, and its templates reach kubectl intact.

    The tests above compare the command with strings, which cannot tell
    whether the shell hands each template to kubectl byte for byte, or
    what '&&' does when a read fails. This runs it with the local /bin/sh
    and a kubectl on PATH which records its arguments, prints what the
    test gives it, and exits as the test says.
    """

    def setUp(self):
        super().setUp()
        if not os.path.exists('/bin/sh'):
            self.skipTest('/bin/sh does not exist')

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.log = os.path.join(tmp.name, 'calls')
        bin_dir = os.path.join(tmp.name, 'bin')
        os.mkdir(bin_dir)
        kubectl = os.path.join(bin_dir, 'kubectl')
        with open(kubectl, 'w', encoding='utf-8') as f:
            f.write(
                '#!%s\n'
                'import json, os, sys\n'
                'resource = sys.argv[2]\n'
                "with open(os.environ['FAKE_KUBECTL_LOG'], 'a') as f:\n"
                '    f.write(json.dumps(sys.argv[1:]) + "\\n")\n'
                "sys.stdout.write(os.environ.get('FAKE_STDOUT_' + resource, ''))\n"
                "sys.exit(int(os.environ.get('FAKE_EXIT_' + resource, '0')))\n"
                % sys.executable)
        os.chmod(kubectl, 0o755)

        self.env = dict(os.environ)
        self.env['PATH'] = '%s:%s' % (bin_dir,
                                      self.env.get('PATH', '/usr/bin:/bin'))
        self.env['FAKE_KUBECTL_LOG'] = self.log

    def _run(self, **env):
        self.env.update(env)
        return subprocess.run(
            ['/bin/sh', '-c', cluster_module.K3S_KUBERNETES_PROBE_COMMAND],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, env=self.env)

    def _calls(self):
        if not os.path.exists(self.log):
            return []
        with open(self.log, encoding='utf-8') as f:
            return [json.loads(line) for line in f]

    def test_both_reads_run_and_their_output_is_parsed(self):
        nodes, pods = KUBERNETES_PROBE_OUTPUT.split('oom\t', 1)
        result = self._run(FAKE_STDOUT_nodes=nodes,
                           FAKE_STDOUT_pods='oom\t' + pods)

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(
            [['get', 'nodes', '--kubeconfig', KUBECONFIG_PATH, '-o',
              'go-template=' + cluster_module.KUBERNETES_NODES_TEMPLATE],
             ['get', 'pods', '-A', '--kubeconfig', KUBECONFIG_PATH, '-o',
              'go-template=' + cluster_module.KUBERNETES_PODS_TEMPLATE]],
            self._calls())
        self.assertEqual(
            cluster_module.parse_kubernetes_readings(KUBERNETES_PROBE_OUTPUT),
            cluster_module.parse_kubernetes_readings(result.stdout))

    def test_a_failed_node_read_fails_the_command_and_reads_no_pods(self):
        # A node list with no pod list would read as no OOM kills, and the
        # other way round is no nodes at all, so neither half alone is an
        # answer.
        result = self._run(FAKE_EXIT_nodes='1')

        self.assertNotEqual(0, result.returncode)
        self.assertEqual(['nodes'], [call[1] for call in self._calls()])

    def test_a_failed_pod_read_fails_the_command(self):
        result = self._run(FAKE_EXIT_pods='1')

        self.assertNotEqual(0, result.returncode)
        self.assertEqual(['nodes', 'pods'],
                         [call[1] for call in self._calls()])


class ParseKubernetesReadingsTestCase(testtools.TestCase):
    """What the Kubernetes probe's output becomes in health()'s report.

    Decision 9 of the cumulative health signals phase 2 plan: every field
    is validated, a record with any field which fails is dropped whole,
    the first record for a node wins, and nothing the output holds may
    make this raise.
    """

    def parse(self, stdout):
        return cluster_module.parse_kubernetes_readings(stdout)

    def assertReadsNothing(self, line):
        self.assertEqual({'nodes': {}, 'oom_killed': {}}, self.parse(line),
                         line)

    def test_a_realistic_two_node_cluster(self):
        self.assertEqual(KUBERNETES_PROBE_READINGS,
                         self.parse(KUBERNETES_PROBE_OUTPUT))

    def test_what_kubectl_printed(self):
        # Lines kubectl v1.21.1+k3s1 printed with these templates, for a
        # real node, a node object with no status, and a node object whose
        # status was written to report Ready Unknown, MemoryPressure and no
        # PIDPressure condition at all; and containers killed in each of
        # the places a kill is reported, one with no finishedAt.
        stdout = (
            'node\t353896f35a22\tTrue\tFalse\tFalse\tFalse\t'
            '2026-10-07T08:01:13Z\n'
            'node\tempty-node\t\t\t\t\t\n'
            'node\tghost-node.example\tUnknown\tTrue\tFalse\t\t'
            '2026-10-05T08:12:25Z\n'
            'oom\tghost-node.example\tdefault\toom-state\tc\t0\t'
            '2026-10-05T08:12:25Z\n'
            'oom\tghost-node.example\tkube-system\toom-last\tc\t5\t'
            '2026-10-05T08:00:00Z\n'
            'oom\tghost-node.example\tdefault\toom-init\tsidecar\t1000000\t'
            '2026-10-05T07:45:00Z\n'
            'oom\tghost-node.example\tdefault\toom-no-finish\tc\t1\t\n')
        readings = self.parse(stdout)

        self.assertEqual(
            {
                '353896f35a22': {
                    'ready': 'True', 'ready_since': 1791360073,
                    'memory_pressure': 'False', 'disk_pressure': 'False',
                    'pid_pressure': 'False'},
                'empty-node': {
                    'ready': None, 'ready_since': None,
                    'memory_pressure': None, 'disk_pressure': None,
                    'pid_pressure': None},
                'ghost-node.example': {
                    'ready': 'Unknown', 'ready_since': 1791187945,
                    'memory_pressure': 'True', 'disk_pressure': 'False',
                    'pid_pressure': None},
            },
            readings['nodes'])
        # The kill with no finishedAt is dropped: finished_at is how a
        # caller tells a new kill from one it has seen, and decision 5
        # promises it is always an int.
        self.assertEqual(
            [('default', 'oom-state', 'c', 0, 1791187945),
             ('kube-system', 'oom-last', 'c', 5, 1791187200),
             ('default', 'oom-init', 'sidecar', 1000000, 1791186300)],
            [(entry['namespace'], entry['pod'], entry['container'],
              entry['restarts'], entry['finished_at'])
             for entry in readings['oom_killed']['ghost-node.example']])

    def test_a_missing_condition_is_none_on_its_own(self):
        for index, key in ((2, 'ready'), (3, 'memory_pressure'),
                           (4, 'disk_pressure'), (5, 'pid_pressure'),
                           (6, 'ready_since')):
            node = self.parse(_with_field(NODE_LINE, index, ''))[
                'nodes']['k3s-banana-node-001']
            expected = dict(
                KUBERNETES_PROBE_READINGS['nodes']['k3s-banana-node-001'])
            expected[key] = None
            self.assertEqual(expected, node, key)

    def test_unknown_is_a_status(self):
        # What the node controller reports when it has stopped hearing
        # from the kubelet. A third value, not a False.
        for index, key in ((2, 'ready'), (3, 'memory_pressure'),
                           (4, 'disk_pressure'), (5, 'pid_pressure')):
            node = self.parse(_with_field(NODE_LINE, index, 'Unknown'))[
                'nodes']['k3s-banana-node-001']
            self.assertEqual('Unknown', node[key])

    def test_a_status_which_is_not_one_drops_the_node(self):
        for value in ('true', 'TRUE', 'false', 'Maybe', 'True ish', '1',
                      '<no value>', 'TrueTrue', 'Unknown\x00'):
            for index in (2, 3, 4, 5):
                self.assertReadsNothing(_with_field(NODE_LINE, index, value))

    def test_an_invalid_name_drops_its_record(self):
        for value in ('K3S-BANANA-NODE-001', '-node', 'node-', 'node_001',
                      'node..one', '.node', 'node.', 'nöde', 'node 001',
                      'node;rm -rf /', '<no value>', '\u0661'):
            self.assertReadsNothing(_with_field(NODE_LINE, 1, value))
            self.assertReadsNothing(_with_field(OOM_LINE, 1, value))
            self.assertReadsNothing(_with_field(OOM_LINE, 3, value))
        # A namespace or a container is one label, so a dot is not allowed
        # there as it is in a node or pod name.
        for index in (2, 4):
            self.assertReadsNothing(_with_field(OOM_LINE, index, 'a.b'))
            self.assertReadsNothing(_with_field(OOM_LINE, index, ''))
        self.assertReadsNothing(_with_field(NODE_LINE, 1, ''))

    def test_a_name_is_capped_at_its_kubernetes_length(self):
        # 253 characters for a node or pod name, which may be several
        # labels; 63 for a namespace or container, which is one.
        subdomain = '.'.join(['a' * 63] * 4)
        self.assertEqual(255, len(subdomain))
        for name, kept in ((subdomain[:253], True), (subdomain[:254], False),
                           ('a' * 253, True), ('a' * 254, False),
                           ('a' * 5000, False)):
            readings = self.parse(_with_field(NODE_LINE, 1, name))
            self.assertEqual(kept, name in readings['nodes'], len(name))
            readings = self.parse(_with_field(OOM_LINE, 3, name))
            self.assertEqual(kept, bool(readings['oom_killed']), len(name))

        for index in (2, 4):
            for name, kept in (('a' * 63, True), ('a' * 64, False),
                               ('a' * 62 + '-b', False)):
                readings = self.parse(_with_field(OOM_LINE, index, name))
                self.assertEqual(kept, bool(readings['oom_killed']),
                                 (index, name))

    def test_an_invalid_time_drops_its_record(self):
        for value in ('2026-10-05 08:12:25Z', '2026-10-05T08:12:25',
                      '2026-10-05T08:12:25.123Z', '2026-10-05T08:12:25+00:00',
                      '2026-10-05t08:12:25z', '2026-02-30T08:12:25Z',
                      '2026-13-05T08:12:25Z', '2026-10-05T24:00:00Z',
                      '2026-10-05T08:12:60Z', '0000-10-05T08:12:25Z',
                      '1791187945', '<no value>', '٢٠٢٦-10-05T08:12:25Z',
                      '2026-10-05T08:12:25Z2026-10-05T08:12:25Z'):
            self.assertReadsNothing(_with_field(NODE_LINE, 6, value))
            self.assertReadsNothing(_with_field(OOM_LINE, 6, value))
        # An OOM kill must say when, because that is how a caller tells a
        # new one from one it has already seen.
        self.assertReadsNothing(_with_field(OOM_LINE, 6, ''))

    def test_a_time_is_unix_seconds_in_utc_whatever_the_local_zone(self):
        # The API's times are UTC. Converting them as local time would be
        # right only where the local zone is UTC, which is where tests
        # usually run, so this test runs somewhere it is not: ten hours
        # east, spelled as a POSIX rule rather than a zone name so that it
        # needs no timezone database to mean that.
        if not hasattr(time, 'tzset'):
            self.skipTest('time.tzset() is not available here')
        self.addCleanup(time.tzset)
        patcher = mock.patch.dict(os.environ, {'TZ': 'AEST-10'})
        patcher.start()
        self.addCleanup(patcher.stop)
        time.tzset()

        for value, seconds in (('1970-01-01T00:00:00Z', 0),
                               ('2024-02-29T23:59:59Z', 1709251199),
                               ('2026-10-05T08:12:25Z', 1791187945)):
            readings = self.parse(_with_field(NODE_LINE, 6, value))
            self.assertEqual(
                seconds,
                readings['nodes']['k3s-banana-node-001']['ready_since'])
            readings = self.parse(_with_field(OOM_LINE, 6, value))
            self.assertEqual(
                seconds,
                readings['oom_killed']['k3s-banana-node-002'][0][
                    'finished_at'])

    def test_an_invalid_restart_count_drops_its_record(self):
        # Each is something int() would refuse or, worse, accept: a sign,
        # a float's spelling, a digit separator, non-ASCII digits, a value
        # too long to serialise, and kubectl's placeholder for a field it
        # could not find.
        for value in ('', '-1', '+2', '2.0', '1e+06', '1_000', '\u0663',
                      '9' * 21, '9' * 5000, '<no value>', 'two'):
            self.assertReadsNothing(_with_field(OOM_LINE, 5, value))
        readings = self.parse(_with_field(OOM_LINE, 5, '9' * 20))
        self.assertEqual(
            int('9' * 20),
            readings['oom_killed']['k3s-banana-node-002'][0]['restarts'])

    def test_one_bad_record_leaves_the_others(self):
        stdout = KUBERNETES_PROBE_OUTPUT.replace(
            '\tk3s-banana-node-001\tTrue', '\tk3s-banana-node-001\tMaybe')
        readings = self.parse(stdout)
        self.assertEqual(['k3s-banana-node-002'], list(readings['nodes']))
        self.assertEqual(KUBERNETES_PROBE_READINGS['oom_killed'],
                         readings['oom_killed'])

    def test_the_first_record_for_a_node_wins(self):
        # The API does not let two nodes share a name, so a second line is
        # not the API's. Valid, so that it is the order which keeps it out
        # and not the validation.
        stdout = KUBERNETES_PROBE_OUTPUT + (
            'node\tk3s-banana-node-001\tFalse\tTrue\tTrue\tTrue\t'
            '2026-10-06T21:40:11Z\n')
        self.assertEqual(KUBERNETES_PROBE_READINGS, self.parse(stdout))

    def test_a_node_is_not_matched_by_a_dropped_record(self):
        # A first record which was dropped claims nothing, so a later valid
        # one for the same name is read.
        stdout = (_with_field(NODE_LINE, 2, 'Maybe') + '\n'
                  + _with_field(NODE_LINE, 2, 'False') + '\n')
        self.assertEqual(
            'False',
            self.parse(stdout)['nodes']['k3s-banana-node-001']['ready'])

    def test_every_oom_kill_on_a_node_is_kept_in_order(self):
        stdout = KUBERNETES_PROBE_OUTPUT + (
            'oom\tk3s-banana-node-002\tkube-system\tcoredns-5d78c9869d-abcde\t'
            'coredns\t0\t2026-10-05T08:12:25Z\n')
        entries = self.parse(stdout)['oom_killed']['k3s-banana-node-002']
        self.assertEqual(['hog', 'coredns'],
                         [entry['container'] for entry in entries])

    def test_an_oom_kill_on_a_node_with_no_node_line_is_kept(self):
        # Which node an entry belongs to is health()'s question, not the
        # parser's: it is filed under the name it gave, and no node
        # reading is invented for it.
        stdout = (KUBERNETES_PROBE_OUTPUT
                  + _with_field(OOM_LINE, 1, 'k3s-banana-node-099') + '\n')
        readings = self.parse(stdout)
        self.assertEqual(
            ['k3s-banana-node-001', 'k3s-banana-node-002'],
            sorted(readings['nodes']))
        self.assertEqual(
            [{'namespace': 'default', 'pod': 'memory-hog-7d9f8b6c5-x2x7k',
              'container': 'hog', 'restarts': 2,
              'finished_at': 1791322811}],
            readings['oom_killed']['k3s-banana-node-099'])

    def test_empty_output_reads_nothing(self):
        for stdout in ('', None, '\n\n', '\t\t\n'):
            self.assertEqual({'nodes': {}, 'oom_killed': {}},
                             self.parse(stdout))

    def test_trailing_whitespace_and_crlf_are_stripped(self):
        stdout = KUBERNETES_PROBE_OUTPUT.replace('\n', ' \r\n').replace(
            '\tTrue\t', '\t True \t')
        self.assertEqual(KUBERNETES_PROBE_READINGS, self.parse(stdout))

    def test_a_line_with_the_wrong_number_of_fields_is_dropped(self):
        for line in (NODE_LINE, OOM_LINE):
            self.assertReadsNothing(line + '\t')
            self.assertReadsNothing(line + '\textra')
            self.assertReadsNothing(line.rsplit('\t', 1)[0])
            self.assertReadsNothing(line.replace('\t', ' '))

    def test_lines_which_are_not_records_are_ignored(self):
        stdout = ('error: the server does not have a resource type "nodez"\n'
                  'pod\tdefault\tsomething\n'
                  'NODE' + NODE_LINE[4:] + '\n'
                  'nodes' + NODE_LINE[4:] + '\n'
                  '\toom\n'
                  + KUBERNETES_PROBE_OUTPUT)
        self.assertEqual(KUBERNETES_PROBE_READINGS, self.parse(stdout))

    def test_the_readings_serialise(self):
        json.dumps(self.parse(KUBERNETES_PROBE_OUTPUT))

    def test_no_string_makes_it_raise(self):
        for stdout in ('\t', '\t' * 7, 'node', 'oom', 'node\t' * 7,
                       'oom\t' * 7, '\x00\t\x00', 'node\t\udcff\tTrue',
                       'node\tn\tTrue\tTrue\tTrue\tTrue\t\udcff',
                       'oom\tn\tn\tn\tn\t\u00b2\t2026-10-05T08:12:25Z',
                       'node\tn\tTrue\tTrue\tTrue\tTrue\t9999-12-31T23:59:59Z',
                       '\u2028node\t\x85', 'x' * 100000,
                       ('node\t' + 'a-' * 50000 + '\t\t\t\t\t')):
            self.parse(stdout)


# The framing every k3s configuration file is written with: a quoted
# heredoc, its body, and the delimiter on a line of its own. A body which
# did not end in a newline would put the delimiter on the body's last line
# and fail this match, which is the point of matching the whole command.
K3S_CONFIG_WRITE_RE = re.compile(
    r"\Acat - > (/etc/rancher/k3s/\S+) << '%s'\n(.*\n)%s\n\Z"
    % (re.escape(cluster_module.K3S_CONFIG_DELIMITER),
       re.escape(cluster_module.K3S_CONFIG_DELIMITER)),
    re.DOTALL)

K3S_CONFIG = '/etc/rancher/k3s/config.yaml'
K3S_CALLER_DROP_IN = '/etc/rancher/k3s/config.yaml.d/50-sf-client-k3s.yaml'
K3S_ENFORCED_DROP_IN = (
    '/etc/rancher/k3s/config.yaml.d/90-sf-client-k3s-enforced.yaml')
K3S_CONTROL_PLANE_TAINT = 'node-role.kubernetes.io/control-plane:NoSchedule'
K3S_AGENT_CONFIG_BODY = (
    '# Written by shakenfist_client_k3s; caller configuration is in '
    'config.yaml.d/.\n')


def _k3s_config_files(commands):
    """{path: body} for every k3s configuration file commands write, in order.

    Fails rather than skipping a write whose framing it does not recognise,
    so that a malformed heredoc is a failure here and not a missing file.
    """
    files = collections.OrderedDict()
    for command in commands:
        if not command.startswith('cat - > /etc/rancher/k3s/'):
            continue
        match = K3S_CONFIG_WRITE_RE.match(command)
        if not match:
            raise AssertionError(
                'a k3s configuration write is not framed as a quoted '
                'heredoc ending in its delimiter: %r' % command)
        files[match.group(1)] = match.group(2)
    return files


class K3sConfigCommandsTestCase(testtools.TestCase):
    """The k3s configuration files every node is given before its installer runs.

    Decisions 4, 5 and 7 of
    docs/plans/PLAN-node-customisation-phase-02-k3s-config.md. Each node
    gets config.yaml with the plugin's own settings, the caller's
    configuration for its role as a drop-in read after it, and on servers
    of a cluster with MetalLB an enforced drop-in read last which appends
    servicelb to disable.

    Driven through install_control_plane(), install_extra_control_plane()
    and install_workers() rather than by calling the helper, so that what
    is pinned is what each kind of node is sent, first_server and role
    included. These assert on the commands. Whether k3s merges the files
    the way the comment above _k3s_config_commands() says is phase 3's
    live check, not something a unit test can answer.
    """

    SERVER_CONFIG = {'disable': ['traefik'],
                     'node-label': ['openstack-control-plane=enabled']}
    AGENT_CONFIG = {'node-label': ['openstack-compute-node=enabled',
                                   'openvswitch=enabled']}

    def setUp(self):
        super(K3sConfigCommandsTestCase, self).setUp()
        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _install(self, workers=True, **md_extra):
        """Install a two server cluster, and its worker if any; return each node's commands."""
        client = fakes.FakeClusterClient()
        worker_nodes = ['inst-w1'] if workers else []
        md = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 4, 'node_network': 'net-1',
            'node_token': None, 'server_token': None, 'k3s_version': 'v1.33',
            'api_address_floating': '192.168.10.100',
            'api_address_inner': '10.0.0.4', 'join_address': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1', 'inst-cp2'],
            'worker_nodes': worker_nodes, 'routed_addresses': []
        }
        md.update(md_extra)
        client.metadata[MD_KEY] = md
        for i, instance_uuid in enumerate(['inst-cp1', 'inst-cp2']
                                          + worker_nodes):
            client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': 'k3s-banana-node-%03d' % (i + 1),
                'state': 'created', 'agent_state': 'ready'}

        cluster = _make_cluster(client)
        cluster.install_control_plane()
        if workers:
            cluster.install_workers(worker_nodes)

        commands = collections.defaultdict(list)
        for instance_uuid, commandline in client.executed:
            commands[instance_uuid].append(commandline)
        return commands

    def _files(self, commands):
        return _k3s_config_files(commands)

    def _expected_server_config(self, workers, first_server):
        expected = {'write-kubeconfig-mode': '0644',
                    'tls-san': ['192.168.10.100']}
        if first_server:
            expected['cluster-init'] = True
        if workers:
            expected['node-taint'] = [K3S_CONTROL_PLANE_TAINT]
        return expected

    def test_the_first_server_config_with_workers(self):
        files = self._files(self._install(workers=True)['inst-cp1'])
        self.assertEqual(
            self._expected_server_config(workers=True, first_server=True),
            yaml.safe_load(files[K3S_CONFIG]))

    def test_the_first_server_config_without_workers_has_no_taint(self):
        # MetalLB's controller does not tolerate the taint, and create()
        # waits for it to roll out, so a zero-worker cluster tainting its
        # only node would fail every create (survey finding 7).
        files = self._files(self._install(workers=False)['inst-cp1'])
        self.assertEqual(
            self._expected_server_config(workers=False, first_server=True),
            yaml.safe_load(files[K3S_CONFIG]))

    def test_the_kubeconfig_mode_is_a_string(self):
        # Unquoted, YAML reads 0644 as the integer 644 (or 420, as an
        # octal), and k3s would be given a mode it did not mean.
        files = self._files(self._install()['inst-cp1'])
        self.assertIn("write-kubeconfig-mode: '0644'\n", files[K3S_CONFIG])

    def test_an_extra_server_config_has_no_cluster_init(self):
        for workers in (True, False):
            files = self._files(self._install(workers=workers)['inst-cp2'])
            self.assertEqual(
                self._expected_server_config(workers=workers,
                                             first_server=False),
                yaml.safe_load(files[K3S_CONFIG]),
                'with%s workers' % ('' if workers else 'out'))

    def test_an_agent_config_is_the_comment_line_only(self):
        files = self._files(self._install()['inst-w1'])
        self.assertEqual(K3S_AGENT_CONFIG_BODY, files[K3S_CONFIG])
        self.assertIsNone(yaml.safe_load(files[K3S_CONFIG]))

    def test_the_caller_drop_in_round_trips_for_each_role(self):
        commands = self._install(server_config=self.SERVER_CONFIG,
                                 agent_config=self.AGENT_CONFIG)
        for instance_uuid, expected in [('inst-cp1', self.SERVER_CONFIG),
                                        ('inst-cp2', self.SERVER_CONFIG),
                                        ('inst-w1', self.AGENT_CONFIG)]:
            files = self._files(commands[instance_uuid])
            self.assertEqual(expected,
                             yaml.safe_load(files[K3S_CALLER_DROP_IN]),
                             instance_uuid)

    def test_the_caller_drop_in_is_written_only_for_a_non_empty_config(self):
        # Each role's file is decided by that role's configuration alone:
        # a server_config does not give the workers a drop-in, nor an
        # agent_config the servers. Missing and empty both mean none.
        cases = [
            ({}, {'inst-cp1': False, 'inst-cp2': False, 'inst-w1': False}),
            ({'server_config': {}, 'agent_config': {}},
             {'inst-cp1': False, 'inst-cp2': False, 'inst-w1': False}),
            ({'server_config': self.SERVER_CONFIG},
             {'inst-cp1': True, 'inst-cp2': True, 'inst-w1': False}),
            ({'agent_config': self.AGENT_CONFIG},
             {'inst-cp1': False, 'inst-cp2': False, 'inst-w1': True}),
        ]
        for md_extra, expected in cases:
            commands = self._install(**md_extra)
            written = {instance_uuid: K3S_CALLER_DROP_IN in self._files(
                           commands[instance_uuid])
                       for instance_uuid in expected}
            self.assertEqual(expected, written, md_extra)

    def test_the_enforced_drop_in_is_on_servers_with_metallb_only(self):
        # Missing means True: a cluster built before metallb_installed was
        # recorded has MetalLB.
        for md_extra, on_servers in [({}, True),
                                     ({'metallb_installed': True}, True),
                                     ({'metallb_installed': False}, False)]:
            commands = self._install(**md_extra)
            for instance_uuid in ('inst-cp1', 'inst-cp2'):
                files = self._files(commands[instance_uuid])
                if on_servers:
                    self.assertEqual(
                        {'disable+': ['servicelb']},
                        yaml.safe_load(files[K3S_ENFORCED_DROP_IN]),
                        (instance_uuid, md_extra))
                else:
                    self.assertNotIn(K3S_ENFORCED_DROP_IN, files,
                                     (instance_uuid, md_extra))
            self.assertNotIn(K3S_ENFORCED_DROP_IN,
                             self._files(commands['inst-w1']), md_extra)

    def test_servicelb_is_not_in_config_yaml(self):
        # A caller's disable in the 50 file would replace a disable in
        # config.yaml and bring servicelb back (survey finding 4).
        files = self._files(self._install(
            server_config={'disable': ['traefik']})['inst-cp1'])
        self.assertNotIn('disable', yaml.safe_load(files[K3S_CONFIG]))

    def test_the_drop_ins_are_written_in_the_order_k3s_reads_them(self):
        files = self._files(self._install(
            server_config=self.SERVER_CONFIG)['inst-cp1'])
        self.assertEqual(
            [K3S_CONFIG, K3S_CALLER_DROP_IN, K3S_ENFORCED_DROP_IN],
            list(files))

    def test_node_taint_opt_out_replaces_the_default(self):
        # The default stays in config.yaml; the opt-out is the caller's
        # file, read after it, replacing the list with an empty one.
        files = self._files(self._install(
            server_config={'node-taint': []})['inst-cp1'])
        self.assertEqual([K3S_CONTROL_PLANE_TAINT],
                         yaml.safe_load(files[K3S_CONFIG])['node-taint'])
        self.assertEqual([], yaml.safe_load(
            files[K3S_CALLER_DROP_IN])['node-taint'])

    def test_every_config_command_precedes_the_installer(self):
        commands = self._install(server_config=self.SERVER_CONFIG,
                                 agent_config=self.AGENT_CONFIG)
        # The directory, config.yaml and the caller's drop-in on every
        # node, and the enforced drop-in on the servers.
        for instance_uuid, expected_count in [('inst-cp1', 4), ('inst-cp2', 4),
                                              ('inst-w1', 3)]:
            node_commands = commands[instance_uuid]
            # startswith() rather than a hostname substring test, for the
            # CodeQL reason test_library_api.py's _server_install_index()
            # gives.
            installs = [i for i, c in enumerate(node_commands)
                        if c.startswith('curl -sfL https://get.k3s.io | ')]
            self.assertEqual(1, len(installs), node_commands)
            config = [i for i, c in enumerate(node_commands)
                      if '/etc/rancher/k3s/config.yaml' in c]
            self.assertEqual(expected_count, len(config), node_commands)
            for i in config:
                self.assertLess(
                    i, installs[0],
                    '%s: %r runs after the k3s installer, which has already '
                    'started the service by then' % (instance_uuid,
                                                     node_commands[i]))
            self.assertTrue(node_commands[config[0]].startswith('mkdir -p '),
                            node_commands)

    def test_the_files_which_land_are_the_files_which_were_sent(self):
        # Through a real shell, as ManifestHeredocTestCase does for
        # manifests: whether the heredoc framing delivers the body intact
        # is a fact about /bin/sh rather than about string formatting.
        if not os.path.exists('/bin/sh'):
            self.skipTest('this test runs the write commands through /bin/sh')
        config = {'node-label': ['shell=$HOME `hostname` $(id)'],
                  'kubelet-arg': ["eviction-hard=memory.available<'5%'"]}
        commands = self._install(server_config=config)['inst-cp1']
        sent = self._files(commands)

        with tempfile.TemporaryDirectory() as tempdir:
            for command in commands:
                if '/etc/rancher/k3s' not in command:
                    continue
                command = command.replace('/etc/rancher/k3s', tempdir)
                run = subprocess.run(command, shell=True, cwd=tempdir,
                                     capture_output=True)
                self.assertEqual(0, run.returncode, run.stderr)
            for path, body in sent.items():
                with open(path.replace('/etc/rancher/k3s', tempdir),
                          encoding='utf-8') as f:
                    self.assertEqual(body, f.read(), path)
        self.assertEqual(config, yaml.safe_load(sent[K3S_CALLER_DROP_IN]))

    def test_metadata_configuration_is_validated_again_at_write_time(self):
        # A library caller can reach the install methods without create(),
        # with metadata nothing has checked. A plugin-owned key there is
        # refused before anything is run on the node.
        client = fakes.FakeClusterClient()
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 2, 'node_network': 'net-1', 'k3s_version': 'v1.33',
            'api_address_floating': '192.168.10.100',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
            'routed_addresses': [], 'server_config': {'token': 'x'}
        }
        client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        self.assertRaises(exceptions.K3sConfigError,
                          _make_cluster(client).install_control_plane)
        self.assertEqual([], client.executed)


class ReadinessWaitsOnWorkloadsTestCase(testtools.TestCase):
    """No readiness wait this module generates waits on a pod selector.

    A pod wait resolves its label selector once and then spends a single
    timeout budget across everything it matched, so one pod which can never
    report Ready costs the whole timeout and then fails naming the healthy
    pods the wait never reached. remove_worker() produces exactly such a
    pod: drain leaves DaemonSet pods alone by design, so deleting the node
    orphans the metallb speaker pod that node was running until the pod
    garbage collector catches up, and an expand-addresses in that window
    used to fail blaming three speakers which had been ready for minutes.
    A workload wait reads the counts the controller keeps, and the node's
    deletion corrects those.

    Asserted over the generated commands rather than at the one call site,
    because the failure mode is the next readiness wait written the old way
    rather than this one changing back.
    """

    def test_metallb_readiness_is_waited_on_per_workload(self):
        rollouts = [line
                    for commandline in _control_plane_and_metallb_commands()
                    for line in commandline.split('\n')
                    if line.startswith('kubectl rollout status')]

        self.assertNotEqual(
            [], rollouts, 'no workload readiness wait was generated at all')
        for workload in ('deployment/metallb-controller',
                         'daemonset/metallb-speaker'):
            self.assertTrue(
                any(workload in line for line in rollouts),
                'nothing waits for %s to roll out: %s' % (workload, rollouts))

    def test_nothing_waits_on_a_pod(self):
        for commandline in _control_plane_and_metallb_commands():
            for line in commandline.split('\n'):
                if not line.startswith('kubectl wait'):
                    continue
                self.assertIsNone(
                    re.search(r'(?<![-\w])pods?(?![-\w])', line),
                    'this waits on a snapshot of pods, so one pod which '
                    'cannot become ready spends the whole timeout and the '
                    'failure names the pods it never reached: %s' % line)


class DeleteReleasesTheNameBeforeTheMetadataTestCase(testtools.TestCase):
    """delete() unlists the cluster name before it deletes the document.

    The two writes are not atomic, and which order they happen in decides
    what an interrupted delete leaves behind. Unlisting first leaves a
    metadata document in state 'deleted' whose name is free of the cluster
    list, which delete() itself runs to completion over, so the recovery
    is to run the delete again. The other order leaves the name listed
    with no document behind it: delete() raises ClusterNotFoundError
    because there is no metadata and create() raises ClusterExistsError
    because the name is listed, so the name is unusable forever.
    """

    def setUp(self):
        super(DeleteReleasesTheNameBeforeTheMetadataTestCase, self).setUp()
        self.client = fakes.FakeClusterClient()
        self.client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 1, 'node_network': None, 'node_token': None,
            'k3s_version': 'v1.33', 'control_plane_nodes': [],
            'worker_nodes': [], 'routed_addresses': []
        }
        self.client.metadata[primitives.CLUSTER_LIST] = ['banana', 'other']

        self.writes = []
        original_set = self.client.set_namespace_metadata_item
        original_delete = self.client.delete_namespace_metadata_item

        def record_set(namespace, key, value):
            self.writes.append(('set', key, copy.deepcopy(value)))
            return original_set(namespace, key, value)

        def record_delete(namespace, key):
            self.writes.append(('delete', key, None))
            return original_delete(namespace, key)

        self.client.set_namespace_metadata_item = record_set
        self.client.delete_namespace_metadata_item = record_delete

    def test_the_name_is_unlisted_before_the_document_is_removed(self):
        _make_cluster(self.client).delete()

        unlist = [i for i, w in enumerate(self.writes)
                  if w[0] == 'set' and w[1] == primitives.CLUSTER_LIST]
        document = [i for i, w in enumerate(self.writes)
                    if w[0] == 'delete' and w[1] == MD_KEY]
        self.assertEqual(1, len(unlist), self.writes)
        self.assertEqual(1, len(document), self.writes)
        self.assertTrue(
            unlist[0] < document[0],
            'the metadata document was removed while the name was still '
            'listed, which strands the name forever. The writes were:\n    %s'
            % '\n    '.join(repr(w) for w in self.writes))

    def test_the_survivors_keep_their_place_in_the_list(self):
        _make_cluster(self.client).delete()
        self.assertEqual(['other'],
                         self.client.metadata[primitives.CLUSTER_LIST])

    def test_a_name_already_absent_from_the_list_is_not_an_error(self):
        # Which is the state a delete interrupted between the two writes
        # above leaves, and the state two concurrent deletes reach.
        # list.remove() raises a bare ValueError, which is not a
        # K3sClusterException, so GroupCatchClusterExceptions does not
        # catch it and the operator sees a traceback instead of a cluster
        # which finished being deleted.
        self.client.metadata[primitives.CLUSTER_LIST] = ['other']

        _make_cluster(self.client).delete()

        self.assertEqual(['other'],
                         self.client.metadata[primitives.CLUSTER_LIST])
        self.assertNotIn(MD_KEY, self.client.metadata)

    def test_the_list_is_removed_when_this_was_the_last_cluster(self):
        self.client.metadata[primitives.CLUSTER_LIST] = ['banana']

        _make_cluster(self.client).delete()

        self.assertNotIn(primitives.CLUSTER_LIST, self.client.metadata)


class ExpandAddressesWithoutMetallbTestCase(testtools.TestCase):
    """expand-addresses refuses a cluster which was built without metallb.

    Without this the verb routes the addresses, commits them to the
    metadata, and then fails looking for metallb workloads in a namespace
    which does not exist -- leaving the caller with routed addresses
    nothing can hand out. The refusal has to come before the allocation,
    which is what these assert.
    """

    def _cluster(self, md_extra):
        client = fakes.FakeClusterClient()
        md = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 1, 'node_network': 'net-1', 'node_token': 'tok',
            'k3s_version': 'v1.33', 'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
            'routed_addresses': []
        }
        md.update(md_extra)
        client.metadata[MD_KEY] = md
        client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        return client, _make_cluster(client)

    def test_a_cluster_built_without_metallb_is_refused(self):
        client, cluster = self._cluster({'metallb_installed': False})

        e = self.assertRaises(exceptions.ComponentNotInstalledError,
                              cluster.expand_addresses, 2)
        self.assertEqual('metallb', e.component)
        self.assertEqual('expand-addresses', e.verb)
        self.assertIn('metallb', str(e))

    def test_no_address_is_routed_by_the_refusal(self):
        client, cluster = self._cluster({'metallb_installed': False})

        self.assertRaises(exceptions.ComponentNotInstalledError,
                          cluster.expand_addresses, 2)

        self.assertEqual([], client.metadata[MD_KEY]['routed_addresses'])
        self.assertEqual([], client.executed)

    def test_a_cluster_with_metallb_is_expanded(self):
        client, cluster = self._cluster({'metallb_installed': True})

        cluster.expand_addresses(2)

        self.assertEqual(
            2, len(client.metadata[MD_KEY]['routed_addresses']))

    def test_metadata_written_before_the_key_existed_is_expanded(self):
        # Every cluster built before create() recorded this has both
        # components, so an absent key must read as True rather than as
        # False or as an error.
        client, cluster = self._cluster({})
        self.assertNotIn('metallb_installed', client.metadata[MD_KEY])

        cluster.expand_addresses(1)

        self.assertEqual(
            1, len(client.metadata[MD_KEY]['routed_addresses']))


class InstallControlPlanePhaseCountTestCase(testtools.TestCase):
    """install_control_plane() counts the phases it is actually going to open.

    It opens one for the first control plane node, and a second through
    install_extra_control_plane() when there is more than one. A library
    caller who invokes it directly on an HA cluster got "[2/1]" for the
    second of those, which is worse than the un-numbered header the
    lazy default replaced.
    """

    def _headers(self, control_plane_nodes):
        client = fakes.FakeClusterClient()
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': len(control_plane_nodes), 'node_network': 'net-1',
            'node_token': None, 'server_token': None, 'k3s_version': 'v1.33',
            'api_address_floating': '192.168.10.100',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': list(control_plane_nodes),
            'worker_nodes': [], 'routed_addresses': []
        }
        for i, instance_uuid in enumerate(control_plane_nodes):
            client.instances[instance_uuid] = {
                'uuid': instance_uuid,
                'name': 'k3s-banana-node-%03d' % (i + 1),
                'state': 'created', 'agent_state': 'ready'}

        reporter = progress.CollectingReporter()
        cluster = Cluster(client, 'banana', 'testns', reporter=reporter)
        cluster.install_control_plane()
        return [line for line in reporter.lines if line.startswith('[')]

    def test_a_single_control_plane_node_opens_one_phase(self):
        self.assertEqual(
            ['[1/1] Installing k3s on the first control plane node'],
            self._headers(['inst-cp1']))

    def test_extra_control_plane_nodes_open_a_second_phase(self):
        self.assertEqual(
            ['[1/2] Installing k3s on the first control plane node',
             '[2/2] Installing k3s on the additional control plane nodes'],
            self._headers(['inst-cp1', 'inst-cp2', 'inst-cp3']))


def _pending_aop(state='queued'):
    return {'uuid': 'aop-001', 'instance_uuid': 'inst-001', 'state': state,
            'commands': [], 'results': {}}


class AgentOperationEndingsTestCase(testtools.TestCase):
    """A wait ends when the operation can no longer progress, not only on two states.

    Shaken Fist gives every agent operation a wall clock budget and moves
    one which overruns it to 'expired', which the server documents as
    deliberately distinct from 'error'. 'deleted' is reachable from every
    state. Each wait loop here used to test for 'complete' and 'error' by
    name, so either of those left it spinning for as long as the process
    was running.

    The client answers a bounded number of times and then runs out, which
    is what turns that spinning into a failure rather than a hang. A
    regression here would otherwise wedge the test run, and a wedged run
    says nothing: nobody reads a suite which did not finish, and CI would
    report a timeout with no failing test named. The correct code reads the
    operation once, so the budget is generous.
    """

    ANSWERS = 20

    def _cluster(self, aop_state):
        client = mock.MagicMock()
        client.get_agent_operation.side_effect = [
            _pending_aop(aop_state) for _ in range(self.ANSWERS)]
        client.get_instance.return_value = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001'}
        return client, Cluster(client, 'banana', 'testns',
                               reporter=progress.CollectingReporter())

    def test_await_execute_returns_an_expired_operation(self):
        client, cluster = self._cluster('expired')
        with mock.patch('time.sleep', lambda seconds: None):
            aop = cluster.await_execute(_pending_aop())
        self.assertEqual('expired', aop['state'])

    def test_reap_execute_raises_for_an_expired_operation(self):
        client, cluster = self._cluster('expired')
        with mock.patch('time.sleep', lambda seconds: None):
            e = self.assertRaises(exceptions.AgentOperationError,
                                  cluster.reap_execute, _pending_aop())
        self.assertEqual('expired', e.state)
        self.assertIn('state: expired', str(e))

    def test_await_fetch_raises_for_an_expired_operation(self):
        client, cluster = self._cluster('expired')
        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(exceptions.AgentOperationError,
                              cluster.await_fetch, _pending_aop())

    def test_await_fetch_raises_for_a_deleted_operation(self):
        client, cluster = self._cluster('deleted')
        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(exceptions.AgentOperationError,
                              cluster.await_fetch, _pending_aop())

    def test_an_error_still_renders_exactly_the_text_it_did(self):
        # The state line is only added when the state is not 'error', so
        # the message for the ending which has always raised this is
        # unchanged.
        client, cluster = self._cluster('error')
        with mock.patch('time.sleep', lambda seconds: None):
            e = self.assertRaises(exceptions.AgentOperationError,
                                  cluster.reap_execute, _pending_aop())
        self.assertNotIn('state:', str(e))

    def _await_idle(self, aop_states, own=True):
        client = mock.MagicMock()
        client.get_instance.return_value = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        aops = [{'uuid': 'aop-%03d' % (i + 1), 'instance_uuid': 'inst-001',
                 'state': state, 'commands': [], 'results': {}}
                for i, state in enumerate(aop_states)]

        client.get_instance_agentoperations.side_effect = [aops] * 50
        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        own_operations = [aop['uuid'] for aop in aops] if own else ['aop-ours']
        with mock.patch('time.sleep', lambda seconds: None):
            return cluster.await_idle(['inst-001'], own_operations)

    def test_await_idle_raises_for_an_expired_operation(self):
        self.assertRaises(exceptions.AgentOperationError,
                          self._await_idle, ['expired'])

    def test_await_idle_does_not_wait_for_a_deleted_operation(self):
        # A deleted operation is finished. Counting it as incomplete waits
        # for a transition which cannot happen.
        self._await_idle(['deleted'])

    def test_await_idle_still_waits_for_a_running_operation(self):
        client = mock.MagicMock()
        client.get_instance.return_value = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        running = [{'uuid': 'aop-001', 'instance_uuid': 'inst-001',
                    'state': 'executing', 'commands': [], 'results': {}}]
        done = [{'uuid': 'aop-001', 'instance_uuid': 'inst-001',
                 'state': 'complete', 'commands': [], 'results': {}}]
        client.get_instance_agentoperations.side_effect = [running, done]

        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        with mock.patch('time.sleep', lambda seconds: None):
            cluster.await_idle(['inst-001'], ['aop-001'])

        self.assertEqual(2, client.get_instance_agentoperations.call_count)

    def _queued_then_expired(self, own_operations):
        client = mock.MagicMock()
        client.get_instance.return_value = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        def aops(state):
            return [{'uuid': 'aop-orphan', 'instance_uuid': 'inst-001',
                     'state': state,
                     'commands': [{'command': 'execute',
                                   'commandline': 'kubectl get nodes'}],
                     'results': {}}]

        # Queued when the wait starts, expired while it runs. Bounded, for
        # the reason the class docstring gives.
        client.get_instance_agentoperations.side_effect = (
            [aops('queued')] * 3 + [aops('expired')] * 20)

        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())

        # The patch has to be inside the callable, not around the return:
        # the wait happens when the caller runs it, and a returned lambda
        # would do its sleeping for real.
        def run():
            with mock.patch('time.sleep', lambda seconds: None):
                return cluster.await_idle(['inst-001'], own_operations)

        return cluster, run

    def test_an_orphan_which_expires_mid_wait_does_not_abort_the_command(self):
        # The shape health()'s abandoned probe creates, and the one the
        # snapshot of already-failed operations could not cover: the
        # 'kubectl get nodes' it left queued is still queued when a later
        # expand-workers starts, and the server's deadline moves it to
        # 'expired' while that wait is running. Aborting an unrelated
        # command over it, naming a command the operator never ran, is what
        # this asserts does not happen.
        _, run = self._queued_then_expired(['aop-ours'])

        run()

    def test_our_own_operation_expiring_mid_wait_still_aborts(self):
        # The other half of the same distinction: an operation this wait
        # submitted, which the server then took the deadline away from, is
        # a failure of the command in hand.
        _, run = self._queued_then_expired(['aop-orphan'])

        e = self.assertRaises(exceptions.AgentOperationError, run)

        self.assertEqual('expired', e.state)
        self.assertEqual('aop-orphan', e.operation_uuid)

    def test_execute_and_await_names_the_operations_it_submitted(self):
        # The thread between the two: execute_and_await() submits the
        # commands and hands their uuids to await_idle(), which is the only
        # reason await_idle() can tell its own failures from a bystander's.
        # A version which passed nothing would wait out the whole install
        # and then report the failure from reap_execute() instead, and one
        # which passed the wrong thing would not report it at all.
        client = mock.MagicMock()
        client.get_instance.return_value = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        submitted = {'uuid': 'aop-mine', 'instance_uuid': 'inst-001',
                     'state': 'queued',
                     'commands': [{'command': 'execute',
                                   'commandline': 'apt-get update'}],
                     'results': {}}
        client.instance_execute.return_value = submitted

        def errored(state):
            return [dict(submitted, state=state)]

        client.get_instance_agentoperations.side_effect = (
            [errored('queued')] + [errored('error')] * 20)

        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        with mock.patch('time.sleep', lambda seconds: None):
            e = self.assertRaises(
                exceptions.AgentOperationError,
                cluster.execute_and_await, ['inst-001'], ['apt-get update'])

        self.assertEqual('aop-mine', e.operation_uuid)

    def test_await_idle_waits_for_a_state_it_does_not_recognise(self):
        # The asymmetry this covers: await_fetch() raises for anything but
        # 'complete', which is fail-safe, while await_idle() used to treat
        # anything not in AGENT_OP_PENDING_STATES as finished, which is
        # fail-open. A state Shaken Fist adds later is far more likely to be
        # a new way of being in flight than a new ending, and declaring the
        # instance idle would run the next install step over a command still
        # executing on the node.
        client = mock.MagicMock()
        client.get_instance.return_value = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        def aops(state):
            return [{'uuid': 'aop-001', 'instance_uuid': 'inst-001',
                     'state': state, 'commands': [], 'results': {}}]

        # Bounded, for the reason the class docstring gives: a regression
        # which waits forever must fail rather than hang.
        client.get_instance_agentoperations.side_effect = (
            [aops('reticulating')] * 3 + [aops('complete')])

        reporter = progress.CollectingReporter()
        cluster = Cluster(client, 'banana', 'testns', reporter=reporter)
        with mock.patch('time.sleep', lambda seconds: None):
            cluster.await_idle(['inst-001'], ['aop-001'])

        # It kept waiting rather than declaring the instance idle at the
        # first sight of the state.
        self.assertEqual(4, client.get_instance_agentoperations.call_count)

        # And said so, once, naming the state: a wait which silently treats
        # a new state as "still running" is indistinguishable from a hang.
        written = '\n'.join(reporter.lines)
        self.assertIn('reticulating', written)
        self.assertIn('not one this version of the k3s plugin knows about',
                      written)
        self.assertEqual(1, written.count('reticulating is not one'))

    def test_a_preexisting_expired_operation_does_not_wedge_the_wait(self):
        # Someone else's expired operation must neither wedge this wait nor
        # abort it. It used to wedge it, because the snapshot which exempted
        # historical failures covered 'error' only; then it aborted the
        # command instead, because the snapshot could not see an operation
        # which was still queued when the wait began. Now it is simply not
        # one of ours.
        client = mock.MagicMock()
        client.get_instance.return_value = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        old = [{'uuid': 'aop-old', 'instance_uuid': 'inst-001',
                'state': 'expired', 'commands': [], 'results': {}}]
        # Bounded, for the reason the class docstring gives.
        client.get_instance_agentoperations.side_effect = [old] * 20

        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        with mock.patch('time.sleep', lambda seconds: None):
            cluster.await_idle(['inst-001'], ['aop-ours'])


class AwaitExecuteTimeoutTestCase(testtools.TestCase):
    """await_execute's timeout, which is health()'s probe and nothing else.

    A fake clock, because the point is how long it waits: real time would
    make the assertion either slow or untrue.
    """

    def _cluster(self, states):
        client = mock.MagicMock()
        client.get_agent_operation.side_effect = [
            _pending_aop(state) for state in states]
        return client, Cluster(client, 'banana', 'testns',
                               reporter=progress.CollectingReporter())

    def _with_clock(self, fn):
        clock = [1000.0]

        def sleep(seconds):
            clock[0] += seconds

        with mock.patch('time.monotonic', lambda: clock[0]), \
                mock.patch('time.sleep', sleep):
            return fn(), clock[0] - 1000.0

    def test_a_pending_operation_is_abandoned_at_the_deadline(self):
        client, cluster = self._cluster(['queued'] * 100)

        aop, elapsed = self._with_clock(
            lambda: cluster.await_execute(_pending_aop(), timeout=5))

        self.assertEqual('queued', aop['state'])
        self.assertEqual(5, elapsed)

    def test_an_operation_which_finishes_first_is_returned(self):
        client, cluster = self._cluster(['queued', 'executing', 'complete'])

        aop, elapsed = self._with_clock(
            lambda: cluster.await_execute(_pending_aop(), timeout=30))

        self.assertEqual('complete', aop['state'])
        # Read at 0 (queued), sleep, read at 1 (executing), sleep, read at 2
        # (complete), return: three reads and two seconds. It was three
        # seconds when the wait slept before each read, the first sleep
        # being for an operation nobody had yet looked at.
        self.assertEqual(2, elapsed)
        self.assertEqual(3, client.get_agent_operation.call_count)

    def test_an_operation_finished_on_its_first_read_costs_no_sleep(self):
        # The property health()'s run time rests on. Its probes are
        # collected one after another, but all of them are running on the
        # server while the first is waited for, so the later ones have
        # usually finished by their turn. Each costs one read and nothing
        # else; a sleep before that read was a second per node on a
        # cluster with nothing wrong with it.
        client, cluster = self._cluster(['complete'])
        sleep = mock.MagicMock()

        with mock.patch('time.sleep', sleep):
            aop = cluster.await_execute(_pending_aop(), timeout=30)

        self.assertEqual('complete', aop['state'])
        self.assertEqual(1, client.get_agent_operation.call_count)
        sleep.assert_not_called()

    def test_an_operation_handed_in_finished_is_not_read(self):
        # Nothing can move a finished operation on, so there is nothing a
        # read could tell the caller. With a timeout or without, and with
        # the timeout already spent: the at-least-once read is for an
        # operation handed in pending, not for every operation.
        for timeout in (None, 30, 0):
            client, cluster = self._cluster([])
            given = _pending_aop('complete')

            aop, elapsed = self._with_clock(
                lambda: cluster.await_execute(given, timeout=timeout))

            self.assertIs(given, aop, timeout)
            self.assertEqual(0, elapsed, timeout)
            client.get_agent_operation.assert_not_called()

    def test_a_wall_clock_step_does_not_move_the_deadline(self):
        # time.time() is not the clock for measuring how long something has
        # been going: an ntp correction or somebody setting the date mid-wait
        # would otherwise cut the probe short or extend it well past its
        # bound. Here the wall clock is stuck at the epoch, which would make
        # every deadline computed from it already past.
        client, cluster = self._cluster(['queued'] * 100)

        with mock.patch('time.time', lambda: 0.0):
            aop, elapsed = self._with_clock(
                lambda: cluster.await_execute(_pending_aop(), timeout=5))

        self.assertEqual('queued', aop['state'])
        self.assertEqual(5, elapsed)

    def test_an_operation_past_its_deadline_is_read_once(self):
        # health() collects every probe against one shared deadline, so all
        # but the first may be collected with none of it left, and each is
        # handed the operation instance_execute() returned: queued, because
        # the plugin's client does not wait for anything it submits.
        # Returning that as given would report a command which finished
        # long ago as abandoned without anybody having looked.
        client, cluster = self._cluster(['complete'])

        aop, elapsed = self._with_clock(
            lambda: cluster.await_execute(_pending_aop(), timeout=0))

        self.assertEqual('complete', aop['state'])
        self.assertEqual(0, elapsed)
        self.assertEqual(1, client.get_agent_operation.call_count)

    def test_an_operation_still_pending_past_its_deadline_is_read_only_once(self):
        # Looking once is not waiting: one read, no sleep, and the pending
        # state that read found is returned for the caller to report.
        client, cluster = self._cluster(['queued'] * 100)

        aop, elapsed = self._with_clock(
            lambda: cluster.await_execute(_pending_aop(), timeout=0))

        self.assertEqual('queued', aop['state'])
        self.assertEqual(0, elapsed)
        self.assertEqual(1, client.get_agent_operation.call_count)

    def test_a_wait_reads_at_each_second_up_to_and_including_the_deadline(self):
        # Read first, then sleep, so a budget of five seconds is reads at
        # 0, 1, 2, 3, 4 and 5: six, the last landing on the deadline. That
        # read finds the deadline passed and is the last thing the wait
        # does, so there is no sixth sleep -- elapsed is 5, not 6 -- and no
        # seventh read of an operation whose state was read a moment ago.
        client, cluster = self._cluster(['queued'] * 100)

        aop, elapsed = self._with_clock(
            lambda: cluster.await_execute(_pending_aop(), timeout=5))

        self.assertEqual('queued', aop['state'])
        self.assertEqual(5, elapsed)
        self.assertEqual(6, client.get_agent_operation.call_count)

    def test_a_deadline_which_passes_mid_sleep_still_gets_its_read(self):
        # A budget which is not a whole number of seconds: the read at 2
        # finds 0.5 seconds left, so the wait sleeps a whole second past
        # the deadline and reads once more at 3. That read is the one at
        # least once the deadline has passed, and finds the operation done.
        client, cluster = self._cluster(['queued', 'queued', 'queued',
                                         'complete'])

        aop, elapsed = self._with_clock(
            lambda: cluster.await_execute(_pending_aop(), timeout=2.5))

        self.assertEqual('complete', aop['state'])
        self.assertEqual(3, elapsed)
        self.assertEqual(4, client.get_agent_operation.call_count)

    def test_an_api_error_from_the_look_past_the_deadline_propagates(self):
        # As one from the wait's own reads does: _collect_probe() catches
        # apiclient.APIException around await_execute() and reports it, and
        # this must not swallow it into a pending state on its way there.
        client, cluster = self._cluster([])
        client.get_agent_operation.side_effect = apiclient.APIException(
            'the server is too busy to answer', 'GET',
            'http://sf-1:13000/agentoperations/aop-001', 503, 'busy')

        self.assertRaises(
            apiclient.APIException,
            self._with_clock,
            lambda: cluster.await_execute(_pending_aop(), timeout=0))

    def test_no_timeout_keeps_waiting(self):
        # Which is every caller but the probe. An install which takes
        # eleven minutes is a slow install, and abandoning it would leave
        # the caller believing a command it can still see running did not
        # happen.
        client, cluster = self._cluster(['queued'] * 600 + ['complete'])

        aop, elapsed = self._with_clock(
            lambda: cluster.await_execute(_pending_aop()))

        self.assertEqual('complete', aop['state'])
        # 600 pending reads at 0 to 599, each followed by a sleep, and the
        # 601st at 600 finds it complete. It was 601 seconds when the wait
        # slept before its first read as well as between them.
        self.assertEqual(600, elapsed)
        self.assertEqual(601, client.get_agent_operation.call_count)


class HealthProbeIsSkippedTestCase(testtools.TestCase):
    """health() does not ask a node which it can already see cannot answer.

    This is the bug the verb existed to avoid and had: an agent operation
    queued against an instance whose agent is not connected is accepted by
    the API and then never runs, so the probe waited on exactly the cluster
    health() is for. The node entry has already read the state and
    agent_state which say so, a few lines earlier in the same method.

    Every node is also asked for its signals under the same rule, so where
    a test here says a node is not asked, the one healthy node beside it
    still is, and only for its signals.
    """

    KUBECTL = 'kubectl get nodes --kubeconfig /etc/rancher/k3s/k3s.yaml'
    WORKER_ONLY = [('inst-w1', cluster_module.node_signals_command('worker'))]

    def setUp(self):
        super(HealthProbeIsSkippedTestCase, self).setUp()
        self.client = fakes.HealthClient()
        self.client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 2, 'node_network': 'net-1', 'node_token': 'tok',
            'k3s_version': 'v1.33',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': ['inst-w1'],
            'routed_addresses': []
        }
        for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-001'),
                                    ('inst-w1', 'k3s-banana-node-002')]:
            self.client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}

        # HealthClient hands every operation out queued, as the server does,
        # so every probe is read at least once, and one scripted to stay
        # pending is waited on through sleeps. Real seconds would make the
        # tests which do that slow for nothing.
        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.cluster = _make_cluster(self.client)

    def test_an_agentless_control_plane_node_is_not_asked(self):
        self.client.instances['inst-cp1']['agent_state'] = None

        report = self.cluster.health()

        self.assertEqual(self.WORKER_ONLY, self.client.executed)
        self.assertFalse(report['api']['probed'])
        self.assertFalse(report['api']['answered'])
        self.assertFalse(report['healthy'])
        self.assertIn('not in a state which can answer', report['api']['error'])
        self.assertIn('agent not contactable', report['api']['error'])

    def test_an_errored_control_plane_node_is_not_asked(self):
        self.client.instances['inst-cp1']['state'] = 'error'

        report = self.cluster.health()

        self.assertEqual(self.WORKER_ONLY, self.client.executed)
        self.assertIn('instance error', report['api']['error'])

    def test_a_vanished_control_plane_node_is_not_asked(self):
        del self.client.instances['inst-cp1']

        report = self.cluster.health()

        self.assertEqual(self.WORKER_ONLY, self.client.executed)
        self.assertFalse(report['api']['probed'])
        self.assertEqual('inst-cp1', report['api']['instance_uuid'])
        self.assertIn('instance gone', report['api']['error'])

    def test_an_unhealthy_worker_does_not_stop_the_probe(self):
        # Only the node the probe runs on decides whether to run it. A
        # worker in the error state is a finding about that worker, and the
        # k3s API still has something to say about it.
        self.client.instances['inst-w1']['state'] = 'error'

        report = self.cluster.health()

        self.assertEqual(
            [('inst-cp1', self.KUBECTL),
             ('inst-cp1', cluster_module.node_signals_command('control_plane'))],
            self.client.executed)
        self.assertTrue(report['api']['probed'])
        self.assertTrue(report['api']['answered'])
        self.assertFalse(report['healthy'])

    def test_a_cluster_with_no_control_plane_reports_the_same_shape(self):
        # Both unprobed answers carry the same keys, so a caller never has
        # to tell them apart by which are present.
        self.client.metadata[MD_KEY]['control_plane_nodes'] = []
        self.cluster = _make_cluster(self.client)

        absent = self.cluster.health()['api']

        self.client.metadata[MD_KEY]['control_plane_nodes'] = ['inst-cp1']
        self.client.instances['inst-cp1']['agent_state'] = None
        down = _make_cluster(self.client).health()['api']

        self.assertEqual(sorted(absent), sorted(down))
        self.assertFalse(absent['probed'])
        self.assertFalse(down['probed'])

    def test_a_probe_which_never_finishes_is_abandoned(self):
        # The node looks well and the command still does not run. Bounded,
        # and reported as a finding rather than waited on.
        self.client.probe_state = 'queued'

        with mock.patch.object(cluster_module, 'HEALTH_PROBE_TIMEOUT_SECONDS', 0):
            report = self.cluster.health()

        self.assertEqual(
            [('inst-cp1', self.KUBECTL),
             ('inst-cp1', cluster_module.node_signals_command('control_plane')),
             ('inst-w1', cluster_module.node_signals_command('worker'))],
            self.client.executed)
        self.assertFalse(report['api']['probed'])
        self.assertIn('had not finished after 0 seconds',
                      report['api']['error'])
        self.assertIn('still queued', report['api']['error'])

        # The uuid of the operation left queued against the node, because
        # this is the one outcome which leaves something behind: an
        # await_idle() in a later verb waits for it until the server's
        # deadline ends it, and a polling caller can only account for that
        # if it is told which operation.
        self.assertIn('agent operation aop-001 is still queued',
                      report['api']['error'])

    def test_an_unrecognised_ending_is_named_rather_than_mislabelled(self):
        # await_execute() returns as soon as the state leaves the pending
        # set, so a terminal state this version has never heard of reaches
        # _probe_k3s_api(). It used to fall through to the result lookup and
        # be reported as "completed but recorded no result", which is the one
        # message in the report that could not be true -- on the verb whose
        # job is describing unusual states accurately.
        self.client.probe_state = 'reticulated'

        report = self.cluster.health()

        self.assertFalse(report['api']['answered'])
        self.assertIn('reticulated', report['api']['error'])
        self.assertIn('does not recognise', report['api']['error'])
        self.assertNotIn('recorded no result', report['api']['error'])

    def test_an_expired_probe_is_a_finding_and_names_the_state(self):
        self.client.probe_state = 'expired'

        report = self.cluster.health()

        self.assertTrue(report['api']['probed'])
        self.assertFalse(report['api']['answered'])
        self.assertIn('expired state', report['api']['error'])


# Decision 1 of the cumulative health signals phase 1 plan, written out
# rather than taken from NODE_SIGNAL_KEYS, so that a key added to or dropped
# from the constant is a change to a documented return shape that a test
# notices rather than one it follows.
SIGNALS_KEYS = {
    'probed', 'error', 'boot_id', 'booted_at', 'k3s_unit', 'k3s_state',
    'k3s_restarts', 'oom_kills', 'memory_total_bytes',
    'memory_available_bytes', 'etcd_bytes', 'etcd_snapshot_bytes'}


class HealthSignalsTestCase(testtools.TestCase):
    """health() reads every node's signals, and they change nothing else.

    Decisions 1, 3, 7 and 8 of the cumulative health signals phase 1 plan:
    each node carries a ``signals`` dict with the same twelve keys whatever
    happened; only a node able to answer is asked; every probe is
    submitted before any is waited for and all share one deadline; and no
    reading, and no failure to take one, moves ``healthy``. The last is the
    one with consequences outside this package, because the Ansible
    module's documented gate and ``--strict`` branch on ``healthy``.
    """

    def setUp(self):
        super(HealthSignalsTestCase, self).setUp()
        self.client = fakes.HealthClient()

        self.md = {
            'name': 'banana',
            'namespace': 'testns',
            'state': 'created',
            'node_serial': 4,
            'node_network': 'net-1',
            'node_token': 'node-token',
            'k3s_version': 'v1.33',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'],
            'worker_nodes': ['inst-w1', 'inst-w2'],
            'routed_addresses': []
        }
        self.client.metadata[MD_KEY] = self.md

        for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-001'),
                                    ('inst-w1', 'k3s-banana-node-002'),
                                    ('inst-w2', 'k3s-banana-node-003')]:
            self.client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.cluster = _make_cluster(self.client)

    def _node(self, report, instance_uuid):
        return [n for n in report['nodes'] if n['uuid'] == instance_uuid][0]

    def _assert_every_node_has_the_signal_keys(self, report):
        self.assertEqual(12, len(SIGNALS_KEYS))
        self.assertNotEqual([], report['nodes'])
        for node in report['nodes']:
            self.assertEqual(SIGNALS_KEYS, set(node['signals']), node)

    def _with_clock(self, fn):
        # A fake clock, as AwaitExecuteTimeoutTestCase uses, because the
        # point is how long health() waits: real time would make the
        # assertion either slow or untrue. Every sleep is recorded as well
        # as added to the clock, so a test can say there were none.
        clock = [1000.0]
        self.sleeps = []

        def sleep(seconds):
            self.sleeps.append(seconds)
            clock[0] += seconds

        with mock.patch('time.monotonic', lambda: clock[0]), \
                mock.patch('time.sleep', sleep):
            return fn(), clock[0] - 1000.0

    def _seven_nodes(self):
        # One control plane node and six workers, all able to answer: eight
        # probes, the kubectl one and seven signals.
        self.md['worker_nodes'] = ['inst-w%d' % n for n in range(1, 7)]
        for n in range(3, 7):
            self.client.instances['inst-w%d' % n] = {
                'uuid': 'inst-w%d' % n, 'name': 'k3s-banana-node-%03d' % (n + 1),
                'state': 'created', 'agent_state': 'ready'}

    def _reads_by_operation(self):
        # In submission order: aop-001 is the kubectl probe, and then each
        # node's signals in node order.
        return [self.client.agent_operation_reads_by_uuid.get(uuid, 0)
                for uuid in sorted(self.client.operations)]

    def _everything_pending(self):
        self.client.probe_state = 'queued'
        for instance_uuid in self.client.instances:
            self.client.signals_state[instance_uuid] = 'queued'

    # The shape, in every outcome.

    def test_a_healthy_cluster_has_the_signal_keys_on_every_node(self):
        self._assert_every_node_has_the_signal_keys(self.cluster.health())

    def test_a_gone_node_has_the_signal_keys(self):
        del self.client.instances['inst-w1']

        self._assert_every_node_has_the_signal_keys(self.cluster.health())

    def test_an_unready_node_has_the_signal_keys(self):
        self.client.instances['inst-w2']['agent_state'] = 'not ready'
        self.client.instances['inst-cp1']['state'] = 'error'

        self._assert_every_node_has_the_signal_keys(self.cluster.health())

    def test_a_failed_probe_has_the_signal_keys(self):
        self.client.signals_state['inst-cp1'] = 'error'
        self.client.signals_return_code['inst-w1'] = 1
        self.client.signals_raises['inst-w2'] = fakes.not_found('inst-w2')

        self._assert_every_node_has_the_signal_keys(self.cluster.health())

    # What is read, and what is not.

    def test_the_readings_reach_the_report(self):
        report = self.cluster.health()

        self.assertEqual(
            {'probed': True, 'error': None,
             'boot_id': '3f0c3c4e-5b8e-4f43-9d1c-0d6a8f2b7e11',
             'booted_at': 1759712345, 'k3s_unit': 'k3s',
             'k3s_state': 'active', 'k3s_restarts': 3, 'oom_kills': 2,
             'memory_total_bytes': 4022148 * 1024,
             'memory_available_bytes': 2876544 * 1024,
             'etcd_bytes': 68321280, 'etcd_snapshot_bytes': 41943040},
            self._node(report, 'inst-cp1')['signals'])

        worker = self._node(report, 'inst-w1')['signals']
        self.assertTrue(worker['probed'])
        self.assertEqual('k3s-agent', worker['k3s_unit'])
        self.assertEqual('activating', worker['k3s_state'])
        self.assertEqual(0, worker['k3s_restarts'])
        self.assertEqual(2010264 * 1024, worker['memory_total_bytes'])
        self.assertIsNone(worker['etcd_bytes'])

    def test_each_node_is_read_on_its_own(self):
        # Answers are per operation, so one node's output is not another's.
        self.client.signals_stdout['inst-w2'] = (
            fakes.WORKER_SIGNALS_OUTPUT.replace('NRestarts=0', 'NRestarts=7'))

        report = self.cluster.health()

        self.assertEqual(0, self._node(report, 'inst-w1')['signals']['k3s_restarts'])
        self.assertEqual(7, self._node(report, 'inst-w2')['signals']['k3s_restarts'])

    def test_a_gone_node_is_not_asked_and_says_why(self):
        del self.client.instances['inst-w1']

        report = self.cluster.health()

        self.assertNotIn('inst-w1', [i for i, _ in self.client.executed])
        signals = self._node(report, 'inst-w1')['signals']
        self.assertFalse(signals['probed'])
        self.assertEqual('this instance no longer exists', signals['error'])
        self.assertEqual('k3s-agent', signals['k3s_unit'])
        self.assertEqual(
            set(), {k for k, v in signals.items()
                    if v is not None and k not in ('probed', 'error', 'k3s_unit')})

    def test_an_unready_node_is_not_asked_and_says_why(self):
        self.client.instances['inst-w2']['agent_state'] = 'not ready'

        report = self.cluster.health()

        self.assertNotIn('inst-w2', [i for i, _ in self.client.executed])
        signals = self._node(report, 'inst-w2')['signals']
        self.assertFalse(signals['probed'])
        self.assertEqual(
            'this node is not in a state which can answer: instance '
            'created, agent not ready', signals['error'])
        self.assertIsNone(signals['boot_id'])
        self.assertIsNone(signals['k3s_restarts'])

    def test_a_skipped_node_says_why_in_the_api_probe_s_words(self):
        # Both probes are skipped for the same reason on the same node, and
        # a caller matching on one message must have matched on both.
        self.client.instances['inst-cp1']['agent_state'] = None

        report = self.cluster.health()

        self.assertEqual([], [i for i, _ in self.client.executed
                              if i == 'inst-cp1'])
        reason = ('is not in a state which can answer: instance created, '
                  'agent not contactable')
        self.assertEqual('the first control plane node ' + reason,
                         report['api']['error'])
        self.assertEqual('this node ' + reason,
                         self._node(report, 'inst-cp1')['signals']['error'])
        self.assertEqual('k3s',
                         self._node(report, 'inst-cp1')['signals']['k3s_unit'])

    # Submission order and the one budget.

    def test_the_kubectl_probe_is_submitted_first(self):
        self._everything_pending()

        self._with_clock(self.cluster.health)

        self.assertEqual(
            ('inst-cp1', cluster_module.K3S_API_PROBE_COMMAND),
            self.client.executed[0])
        self.assertEqual(4, len(self.client.executed))

    def test_every_probe_is_submitted_before_any_is_waited_for(self):
        self._everything_pending()

        self._with_clock(self.cluster.health)

        kinds = [call[0] for call in self.client.calls]
        self.assertEqual(4, kinds.count('execute'))
        self.assertIn('read', kinds)
        self.assertNotIn('execute', kinds[kinds.index('read'):], kinds)

    def test_every_probe_pending_costs_one_budget_not_one_per_node(self):
        # Seven nodes, none of which ever answers. Waited for one after
        # another this is eight budgets; submitted together against one
        # deadline it is one.
        self._seven_nodes()
        self._everything_pending()

        report, elapsed = self._with_clock(self.cluster.health)

        self.assertEqual(8, len(self.client.executed))
        self.assertEqual(cluster_module.HEALTH_PROBE_TIMEOUT_SECONDS, elapsed)
        # One read a second across one budget, and then one read each of
        # the operations left. The kubectl probe is collected first, with
        # the whole budget: a read and then a sleep each second, so reads at
        # 0, 1, ..., 30 -- 31 of them, the last landing exactly on the
        # deadline, after which the wait returns without sleeping. Each of
        # the seven signals probes is collected after that with no time
        # left, and is read once without sleeping, because the state it was
        # handed is the one it was submitted in. 31 + 7 = 38, and it is
        # exact rather than a range: fewer would mean a probe judged without
        # being read, and more a wait which slept or read twice after the
        # deadline.
        self.assertEqual(cluster_module.HEALTH_PROBE_TIMEOUT_SECONDS + 1 + 7,
                         self.client.agent_operation_reads)
        self.assertEqual(
            [cluster_module.HEALTH_PROBE_TIMEOUT_SECONDS + 1] + [1] * 7,
            self._reads_by_operation())

        self.assertFalse(report['api']['probed'])
        for node in report['nodes']:
            self.assertFalse(node['signals']['probed'])
            self.assertIn('the node signals command had not finished after '
                          '30 seconds', node['signals']['error'])
            self.assertIn('is still queued', node['signals']['error'])
            # The node itself is as healthy as it was.
            self.assertTrue(node['healthy'])

    def test_a_healthy_cluster_of_seven_nodes_costs_no_sleep(self):
        # The other end of the budget, and the case it is spent on almost
        # every time: nothing wrong, and every operation finished by the
        # time it is first read. Collection is one probe after another, so a
        # wait which slept before its first read cost a second a probe --
        # eight seconds here, and the whole budget on a cluster of thirty --
        # for a cluster with nothing to report. Read first, each probe is
        # one read and no sleep, so the call takes no time on the fake clock
        # at all.
        self._seven_nodes()

        report, elapsed = self._with_clock(self.cluster.health)

        self.assertEqual(0, elapsed)
        self.assertEqual([], self.sleeps)
        self.assertEqual([1] * 8, self._reads_by_operation())
        self.assertTrue(report['healthy'])
        self.assertTrue(report['api']['answered'])
        self.assertEqual(7, len(report['nodes']))
        for node in report['nodes']:
            self.assertTrue(node['signals']['probed'], node['uuid'])
            self.assertIsNone(node['signals']['error'], node['uuid'])

    def test_a_slow_first_probe_costs_its_own_time_not_one_second_per_node(self):
        # The kubectl probe is still running when it is first read, and
        # done on its second, one sleep later. The signals probes ran
        # alongside it on their nodes, so each has finished by the time it
        # is collected and is read once without sleeping. The call takes
        # the slowest probe's second, not that second plus one per node.
        self._seven_nodes()
        self.client.probe_state = 'executing'
        original = self.client.get_agent_operation

        def read(operation_uuid):
            aop = original(operation_uuid)
            # Whatever was read, the kubectl command finishes straight
            # after: the first read, of aop-001, finds it executing, and
            # every later one finds it complete.
            self.client.probe_state = 'complete'
            return aop

        self.client.get_agent_operation = read

        report, elapsed = self._with_clock(self.cluster.health)

        self.assertEqual(1, elapsed)
        self.assertEqual([1], self.sleeps)
        self.assertEqual([2] + [1] * 7, self._reads_by_operation())
        self.assertTrue(report['healthy'])
        self.assertTrue(all(n['signals']['probed'] for n in report['nodes']))

    def test_a_pending_kubectl_probe_does_not_hold_up_the_signals(self):
        # Each operation answers for itself: the API probe is abandoned and
        # the signals, which completed, are still read.
        self.client.probe_state = 'queued'

        report, elapsed = self._with_clock(self.cluster.health)

        self.assertFalse(report['api']['probed'])
        self.assertIn('had not finished', report['api']['error'])
        self.assertTrue(all(n['signals']['probed'] for n in report['nodes']))
        self.assertEqual(3, self._node(report, 'inst-cp1')['signals']['k3s_restarts'])

    def test_a_pending_signals_probe_does_not_void_the_api_answer(self):
        self.client.signals_state['inst-w2'] = 'queued'

        report, elapsed = self._with_clock(self.cluster.health)

        self.assertTrue(report['api']['answered'])
        self.assertTrue(self._node(report, 'inst-w1')['signals']['probed'])
        self.assertFalse(self._node(report, 'inst-w2')['signals']['probed'])
        self.assertTrue(report['healthy'])

    def _signals_operation(self, instance_uuid):
        return [uuid for uuid, (kind, inst, _) in self.client.operations.items()
                if kind == 'signals' and inst == instance_uuid][0]

    def test_signals_collected_after_the_deadline_are_read_not_judged_as_submitted(self):
        # The kubectl probe never leaves 'queued' and so uses up the whole
        # budget; it is collected first. Every signals operation is
        # 'queued' as submitted, as the server returns every operation to
        # an ASYNC_CONTINUE client, and 'complete' the first time anybody
        # reads it. Collected after the deadline, each must still be read
        # once and reported as the finished probe it is. The bug was that
        # with no time left the wait returned the operation it was handed
        # without reading it, so every node's signals were reported
        # abandoned, still 'queued', on a cluster where every one of them
        # had answered.
        self.client.probe_state = 'queued'

        report, elapsed = self._with_clock(self.cluster.health)

        # One budget, spent on the kubectl probe, which is abandoned.
        self.assertEqual(cluster_module.HEALTH_PROBE_TIMEOUT_SECONDS, elapsed)
        self.assertFalse(report['api']['probed'])
        self.assertIn('had not finished after 30 seconds',
                      report['api']['error'])
        self.assertIn('agent operation aop-001 is still queued',
                      report['api']['error'])

        # And every node's signals, read once each after the deadline.
        for node in report['nodes']:
            self.assertTrue(node['healthy'], node)
            signals = node['signals']
            self.assertTrue(signals['probed'], signals)
            self.assertIsNone(signals['error'], signals)
            self.assertEqual(
                1, self.client.agent_operation_reads_by_uuid.get(
                    self._signals_operation(node['uuid']), 0), node['uuid'])

        self.assertEqual(
            {'probed': True, 'error': None,
             'boot_id': '3f0c3c4e-5b8e-4f43-9d1c-0d6a8f2b7e11',
             'booted_at': 1759712345, 'k3s_unit': 'k3s',
             'k3s_state': 'active', 'k3s_restarts': 3, 'oom_kills': 2,
             'memory_total_bytes': 4022148 * 1024,
             'memory_available_bytes': 2876544 * 1024,
             'etcd_bytes': 68321280, 'etcd_snapshot_bytes': 41943040},
            self._node(report, 'inst-cp1')['signals'])
        for worker in ('inst-w1', 'inst-w2'):
            signals = self._node(report, worker)['signals']
            self.assertEqual('9a1d7c22-0e4b-4c5f-a0b3-77c1e2d4f6a8',
                             signals['boot_id'])
            self.assertEqual('k3s-agent', signals['k3s_unit'])
            self.assertEqual('activating', signals['k3s_state'])
            self.assertEqual(1102336 * 1024, signals['memory_available_bytes'])

    def test_a_signals_probe_still_pending_after_the_deadline_is_abandoned(self):
        # The other half of looking once: the one read after the deadline
        # finds inst-w2's operation still queued, and that is reported as
        # abandoned, naming the operation, without a second read or any
        # further wait. The nodes beside it are read and answer.
        self.client.probe_state = 'queued'
        self.client.signals_state['inst-w2'] = 'queued'

        report, elapsed = self._with_clock(self.cluster.health)

        self.assertEqual(cluster_module.HEALTH_PROBE_TIMEOUT_SECONDS, elapsed)
        operation = self._signals_operation('inst-w2')
        self.assertEqual(
            1, self.client.agent_operation_reads_by_uuid.get(operation, 0))

        signals = self._node(report, 'inst-w2')['signals']
        self.assertFalse(signals['probed'])
        self.assertEqual(
            'the node signals command had not finished after 30 seconds '
            '(agent operation %s is still queued), so the wait was '
            'abandoned' % operation, signals['error'])
        self.assertIsNone(signals['boot_id'])
        self.assertTrue(self._node(report, 'inst-w2')['healthy'])

        for other in ('inst-cp1', 'inst-w1'):
            self.assertTrue(self._node(report, other)['signals']['probed'],
                            other)

    # A signals probe never moves healthy.

    def _assert_still_healthy(self, report):
        self.assertTrue(all(n['healthy'] for n in report['nodes']))
        self.assertTrue(report['api']['answered'])
        self.assertTrue(report['healthy'])

    def test_an_errored_signals_probe_leaves_the_cluster_healthy(self):
        self.client.signals_state['inst-w1'] = 'error'

        report = self.cluster.health()

        self._assert_still_healthy(report)
        signals = self._node(report, 'inst-w1')['signals']
        self.assertTrue(signals['probed'])
        self.assertEqual('the agent operation for the node signals command '
                         'entered the error state', signals['error'])
        self.assertIsNone(signals['boot_id'])

    def test_an_abandoned_signals_probe_leaves_the_cluster_healthy(self):
        self.client.signals_state['inst-cp1'] = 'queued'

        with mock.patch.object(cluster_module, 'HEALTH_PROBE_TIMEOUT_SECONDS', 0):
            report = self.cluster.health()

        self._assert_still_healthy(report)
        signals = self._node(report, 'inst-cp1')['signals']
        self.assertFalse(signals['probed'])
        # The uuid of the operation left queued, as for the API probe: it
        # is the thing a later await_idle() will wait on. aop-001 is the
        # kubectl probe, submitted first; aop-002 is this node's signals.
        self.assertEqual(
            'the node signals command had not finished after 0 seconds '
            '(agent operation aop-002 is still queued), so the wait was '
            'abandoned', signals['error'])

    def test_a_refused_signals_probe_leaves_the_cluster_healthy(self):
        self.client.signals_raises['inst-w2'] = apiclient.APIException(
            'the server is too busy to answer', 'POST',
            'http://sf-1:13000/instances/inst-w2/agent/execute', 503, 'busy')

        report = self.cluster.health()

        self._assert_still_healthy(report)
        signals = self._node(report, 'inst-w2')['signals']
        self.assertFalse(signals['probed'])
        self.assertIn('APIException', signals['error'])
        self.assertIn('inst-w2', signals['error'])

    def test_a_signals_command_which_exits_non_zero_leaves_the_cluster_healthy(self):
        # And what it did print is still read: each reading stands alone.
        self.client.signals_return_code['inst-w1'] = 2
        self.client.signals_stdout['inst-w1'] = (
            'boot_id=0b5e7d3a-2c41-4f6e-8a90-1d2c3b4a5f60\noom_kills=5\n')

        report = self.cluster.health()

        self._assert_still_healthy(report)
        signals = self._node(report, 'inst-w1')['signals']
        self.assertTrue(signals['probed'])
        self.assertEqual('the node signals command exited 2', signals['error'])
        self.assertEqual('0b5e7d3a-2c41-4f6e-8a90-1d2c3b4a5f60',
                         signals['boot_id'])
        self.assertEqual(5, signals['oom_kills'])
        self.assertIsNone(signals['memory_total_bytes'])

    def test_no_raw_command_output_reaches_the_report(self):
        # The stderr of a failed read and the command line itself are raw
        # material; only the twelve keys are reported.
        self.client.signals_return_code['inst-w1'] = 1
        self.client.signals_stderr['inst-w1'] = 'awk: cannot open /proc/vmstat'

        report = self.cluster.health()

        rendered = json.dumps([n['signals'] for n in report['nodes']])
        self.assertNotIn('awk', rendered)
        self.assertNotIn('printf', rendered)
        self.assertNotIn('systemctl', rendered)
        self.assertNotIn('cannot open', rendered)

    # Read only, and the one caller value.

    def test_no_metadata_is_written(self):
        self.md['server_config'] = {'etcd-snapshot-dir': '/srv/snapshots'}
        before = copy.deepcopy(self.client.metadata)

        self.cluster.health()

        self.assertEqual(before, self.client.metadata)
        self.assertEqual([], self.client.metadata_writes)
        self.assertEqual([], self.client.metadata_deletes)
        self.assertEqual([], self.client.deleted_instances)

    def test_a_recorded_snapshot_directory_reaches_the_control_plane_command_quoted(self):
        snapshot_dir = '/srv/etcd snaps/$HOME'
        self.md['server_config'] = {'etcd-snapshot-dir': snapshot_dir,
                                    'node-label': ['a=b']}

        self.cluster.health()

        commands = dict(self.client.executed[1:])
        self.assertEqual(
            cluster_module.node_signals_command('control_plane', snapshot_dir),
            commands['inst-cp1'])
        self.assertIn(shlex.quote(snapshot_dir), commands['inst-cp1'])
        for worker in ('inst-w1', 'inst-w2'):
            self.assertEqual(cluster_module.node_signals_command('worker'),
                             commands[worker])
            self.assertNotIn('snaps', commands[worker])

    def test_without_a_recorded_snapshot_directory_the_default_is_read(self):
        # Metadata written before server_config was recorded has none, and
        # a value which is not a string is not a directory k3s was given.
        for server_config in (None, {}, {'etcd-snapshot-dir': ['/srv/x']},
                              {'etcd-snapshot-dir': 7}, ['not', 'a', 'dict']):
            self.md['server_config'] = server_config
            self.client.executed = []

            _make_cluster(self.client).health()

            self.assertEqual(
                cluster_module.node_signals_command('control_plane'),
                dict(self.client.executed[1:])['inst-cp1'], server_config)


class ActionLogFailingDrainClient(ActionLogClient):
    """An action log client where one nominated kubectl exits non-zero.

    Parameterised by which command fails rather than hard wired to the
    drain, because the property under test is that every command which can
    leave the node cordoned routes through the uncordon, and 'kubectl
    delete node' is the second of those.
    """

    def __init__(self, uncordon_fails=False, failing_prefix='kubectl drain'):
        super(ActionLogFailingDrainClient, self).__init__()
        self.uncordon_fails = uncordon_fails
        self.failing_prefix = failing_prefix

    def instance_execute(self, instance_ref, commandline):
        self.actions.append(('execute', instance_ref, commandline))
        failing = (commandline.startswith(self.failing_prefix)
                   or (self.uncordon_fails
                       and commandline.startswith('kubectl uncordon')))
        self.aop_serial += 1
        return {
            'uuid': 'aop-%03d' % self.aop_serial,
            'instance_uuid': instance_ref,
            'state': 'complete',
            'commands': [{'command': 'execute', 'commandline': commandline}],
            'results': {'0': {
                'return-code': 1 if failing else 0,
                'stdout': '',
                'stderr': ('error when evicting pod "web-1": Cannot evict pod '
                           'as it would violate the disruption budget\n')
                          if failing else ''}}
        }


class DrainFailureTestCase(testtools.TestCase):
    """A refused drain leaves the cluster as it found it.

    kubectl drain cordons the node as its first act, so every way the
    eviction can fail -- a PodDisruptionBudget, an unmanaged pod, a pod with
    nowhere to go -- leaves the node unschedulable. Nothing else in this
    package would put it back, so a remove-worker which refuses has to.
    """

    def _cluster(self, uncordon_fails=False,
                 failing_prefix='kubectl drain'):
        client = ActionLogFailingDrainClient(uncordon_fails=uncordon_fails,
                                             failing_prefix=failing_prefix)
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 3, 'node_network': 'net-1', 'node_token': 'tok',
            'k3s_version': 'v1.33', 'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'],
            'worker_nodes': ['inst-w1', 'inst-w2'], 'routed_addresses': []
        }
        for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-001'),
                                    ('inst-w1', 'k3s-banana-node-002'),
                                    ('inst-w2', 'k3s-banana-node-003')]:
            client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}
        reporter = progress.CollectingReporter()
        return client, reporter, Cluster(client, 'banana', 'testns',
                                         reporter=reporter)

    def test_the_drain_carries_a_timeout(self):
        client, _, cluster = self._cluster()
        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(exceptions.CommandFailedError,
                              cluster.remove_worker, ['inst-w1'])

        drain = [a[2] for a in client.actions
                 if a[2] and a[2].startswith('kubectl drain')][0]
        self.assertIn('--timeout=%s' % cluster_module.KUBECTL_DRAIN_TIMEOUT,
                      drain)

    def test_the_node_is_uncordoned_before_the_failure_is_raised(self):
        client, _, cluster = self._cluster()
        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(exceptions.CommandFailedError,
                              cluster.remove_worker, ['inst-w1'])

        self.assertEqual(
            ['kubectl drain', 'kubectl uncordon'],
            [' '.join(a[2].split()[:2]) for a in client.actions if a[2]])

    def test_nothing_is_destroyed_by_a_refused_drain(self):
        client, _, cluster = self._cluster()
        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(exceptions.CommandFailedError,
                              cluster.remove_worker, ['inst-w1'])

        self.assertEqual([], [a for a in client.actions
                              if a[0] == 'delete_instance'])
        self.assertEqual(['inst-w1', 'inst-w2'],
                         client.metadata[MD_KEY]['worker_nodes'])

    def test_an_api_failure_during_the_drain_also_uncordons(self):
        # apiclient's exceptions do not descend from K3sClusterException,
        # and the question the handler is answering is whether the drain
        # might have cordoned the node, not which hierarchy the failure
        # came from.
        client, _, cluster = self._cluster()
        boom = apiclient.APIException(
            'nope', 'POST', '/instances/inst-cp1/agent/execute', 500, 'nope')
        calls = []

        real = client.instance_execute

        def execute(instance_ref, commandline):
            calls.append(commandline)
            if commandline.startswith('kubectl drain'):
                raise boom
            return real(instance_ref, commandline)

        client.instance_execute = execute
        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(apiclient.APIException,
                              cluster.remove_worker, ['inst-w1'])

        self.assertEqual(['kubectl drain', 'kubectl uncordon'],
                         [' '.join(c.split()[:2]) for c in calls])

    def test_a_failing_delete_node_also_uncordons(self):
        # The drain succeeded, so the node is not merely cordoned but empty.
        # Leaving it there costs the cluster a node's worth of schedulable
        # capacity, which is exactly what remove_worker() promises a refused
        # removal will not do.
        client, _, cluster = self._cluster(
            failing_prefix='kubectl delete node')
        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(exceptions.CommandFailedError,
                              cluster.remove_worker, ['inst-w1'])

        self.assertEqual(
            ['kubectl drain', 'kubectl delete', 'kubectl uncordon'],
            [' '.join(a[2].split()[:2]) for a in client.actions if a[2]])
        self.assertEqual([], [a for a in client.actions
                              if a[0] == 'delete_instance'])
        self.assertEqual(['inst-w1', 'inst-w2'],
                         client.metadata[MD_KEY]['worker_nodes'])

    def test_a_failing_instance_delete_keeps_the_worker_in_the_metadata(self):
        # Past the point an uncordon helps: the node object is gone. The
        # metadata entry stays so that 'k3s delete' still destroys the
        # instance and health() still reports it -- dropping it would trade
        # a visible stale entry for an instance nothing here can see -- and
        # the recovery is said rather than attempted.
        client, reporter, cluster = self._cluster(failing_prefix='never')
        boom = apiclient.APIException(
            'nope', 'DELETE', '/instances/inst-w1', 500, 'nope')

        def delete_instance(instance_ref):
            raise boom

        client.delete_instance = delete_instance
        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(apiclient.APIException,
                              cluster.remove_worker, ['inst-w1'])

        self.assertEqual(['inst-w1', 'inst-w2'],
                         client.metadata[MD_KEY]['worker_nodes'])
        written = '\n'.join(reporter.lines)
        self.assertIn('still lists it as a worker', written)
        self.assertIn('inst-w1', written)
        # No uncordon: there is no node object left to put back in service.
        self.assertNotIn('kubectl uncordon',
                         ' '.join(a[2] or '' for a in client.actions))

    def test_a_failed_uncordon_does_not_replace_the_reason(self):
        # Reporting "the uncordon failed" instead of "the disruption budget
        # refused the eviction" loses the only thing the operator can act
        # on, so the original failure is still what is raised.
        client, reporter, cluster = self._cluster(uncordon_fails=True)
        with mock.patch('time.sleep', lambda seconds: None):
            e = self.assertRaises(exceptions.CommandFailedError,
                                  cluster.remove_worker, ['inst-w1'])

        self.assertIn('disruption budget', str(e))
        written = '\n'.join(reporter.lines)
        self.assertIn('still unschedulable', written)
        self.assertIn("kubectl uncordon k3s-banana-node-002", written)


class RemoveWorkerEdgeCaseTestCase(testtools.TestCase):
    """The two shapes a programmatic caller reaches and the command line cannot."""

    def _cluster(self):
        client = ActionLogClient()
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 3, 'node_network': 'net-1', 'node_token': 'tok',
            'k3s_version': 'v1.33', 'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'],
            'worker_nodes': ['inst-w1', 'inst-w2'], 'routed_addresses': []
        }
        for instance_uuid, name in [('inst-cp1', 'k3s-banana-node-001'),
                                    ('inst-w1', 'k3s-banana-node-002'),
                                    ('inst-w2', 'k3s-banana-node-003')]:
            client.instances[instance_uuid] = {
                'uuid': instance_uuid, 'name': name, 'state': 'created',
                'agent_state': 'ready'}
        reporter = progress.CollectingReporter()
        return client, reporter, Cluster(client, 'banana', 'testns',
                                         reporter=reporter)

    def test_an_empty_list_does_nothing_and_says_nothing(self):
        client, reporter, cluster = self._cluster()

        cluster.remove_worker([])

        self.assertEqual([], client.actions)
        self.assertEqual([], reporter.lines)
        self.assertEqual(['inst-w1', 'inst-w2'],
                         client.metadata[MD_KEY]['worker_nodes'])

    def test_an_instance_which_no_longer_exists_is_still_removed(self):
        # The state health() reports as a finding and delete() tolerates.
        # remove-worker is the only verb which can clear the metadata entry,
        # so refusing it would make a stale entry unfixable short of
        # deleting the cluster.
        client, reporter, cluster = self._cluster()
        del client.instances['inst-w1']

        with mock.patch('time.sleep', lambda seconds: None):
            cluster.remove_worker(['inst-w1'])

        self.assertEqual([], client.actions)
        self.assertEqual(['inst-w2'], client.metadata[MD_KEY]['worker_nodes'])
        self.assertIn('no longer exists', '\n'.join(reporter.lines))

    def test_an_instance_with_no_name_is_refused(self):
        # k3s knows a node by the hostname Shaken Fist built from the
        # instance's name, so an instance with no name is one whose node
        # cannot be identified. Draining a guess would evict somebody
        # else's pods; skipping the drain would delete a node object with
        # workloads still on it.
        client, _, cluster = self._cluster()
        del client.instances['inst-w1']['name']

        with mock.patch('time.sleep', lambda seconds: None):
            e = self.assertRaises(exceptions.WorkerUnnamedError,
                                  cluster.remove_worker, ['inst-w1'])

        self.assertIn('inst-w1', str(e))
        self.assertEqual([], client.actions)
        self.assertEqual(['inst-w1', 'inst-w2'],
                         client.metadata[MD_KEY]['worker_nodes'])

    def test_a_null_name_is_refused_the_same_way(self):
        client, _, cluster = self._cluster()
        client.instances['inst-w1']['name'] = None

        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(exceptions.WorkerUnnamedError,
                              cluster.remove_worker, ['inst-w1'])

        self.assertEqual([], client.actions)

    def test_an_unnamed_worker_is_caught_before_the_first_one_is_drained(self):
        # The same property the uuid check has: a run which discovers a
        # problem on its second worker has already destroyed the first, so
        # every name is resolved before anything is touched.
        client, _, cluster = self._cluster()
        del client.instances['inst-w2']['name']

        with mock.patch('time.sleep', lambda seconds: None):
            self.assertRaises(exceptions.WorkerUnnamedError,
                              cluster.remove_worker, ['inst-w1', 'inst-w2'])

        self.assertEqual([], [a for a in client.actions
                              if a[2] and a[2].startswith('kubectl')])
        self.assertEqual([], [a for a in client.actions
                              if a[0] == 'delete_instance'])
        self.assertEqual(['inst-w1', 'inst-w2'],
                         client.metadata[MD_KEY]['worker_nodes'])

    def test_a_gone_instance_alongside_a_live_one(self):
        client, reporter, cluster = self._cluster()
        del client.instances['inst-w1']

        with mock.patch('time.sleep', lambda seconds: None):
            cluster.remove_worker(['inst-w1', 'inst-w2'])

        self.assertEqual([], client.metadata[MD_KEY]['worker_nodes'])
        drains = [a for a in client.actions
                  if a[2] and a[2].startswith('kubectl drain')]
        self.assertEqual(1, len(drains), client.actions)
        self.assertIn('k3s-banana-node-003', drains[0][2])


class ExpandAddressesChecksTheDocumentFirstTestCase(testtools.TestCase):
    """A document which cannot produce a configuration is refused before spending.

    ``heredoc()`` refuses a body an interpolated address could end, and
    that refusal is correct and stays. What it cannot do is arrive in
    time: ``expand_addresses()`` routes new floating addresses -- which
    are charged for -- and commits them to the metadata, and only then
    writes the configuration the old and new addresses share. So a value
    which was already bad costs the caller an allocation before it is
    told.

    ``expand_addresses()`` makes exactly this argument in its own
    docstring for a cluster without metallb, which it checks up front
    for that reason. These tests are that argument applied to the
    document's contents as well as its flags.
    """

    def _cluster(self, routed_addresses):
        client = ActionLogClient()
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'k3s_version': 'v1.33', 'node_network': 'net-1',
            'node_token': 'a-token',
            'api_address_floating': '10.0.0.1',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
            'routed_addresses': routed_addresses}
        return client, _make_cluster(client)

    def test_a_hostile_recorded_address_is_refused_before_anything_is_routed(self):
        client, cluster = self._cluster(
            ['192.168.10.1', '10.0.0.1\nEOF\ntouch /pwned'])

        e = self.assertRaises(exceptions.ClusterMetadataError,
                              cluster.expand_addresses, 2)

        self.assertEqual('not_an_address', e.reason)
        self.assertEqual('routed_addresses', e.key)
        # The point of the whole change: nothing was allocated, so the
        # refusal costs the caller an error and nothing else.
        self.assertEqual(0, client.routed_serial)
        self.assertEqual([], client.executed)

    def test_a_merely_malformed_address_is_refused_too(self):
        # The delimiter collision is the dramatic case; the check is for
        # addresses, so a value which is harmless and still not an
        # address is refused on the same ground rather than written into
        # metallb's configuration for it to reject later.
        client, cluster = self._cluster(['192.168.10.300'])

        e = self.assertRaises(exceptions.ClusterMetadataError,
                              cluster.expand_addresses, 1)

        self.assertEqual('192.168.10.300', e.value)
        self.assertEqual(0, client.routed_serial)

    def _expand_past_the_check(self, routed_addresses):
        """Run expand_addresses as far as the configuration write, and no further.

        What these cases assert is that the new check does not refuse a
        document it should accept, which is the whole risk of adding a
        validator. Writing metallb's configuration is a separate
        concern with its own tests, and running it here would need the
        agent's rollout commands scripted for no gain, so it is
        replaced and asserted to have been reached.
        """
        client, cluster = self._cluster(routed_addresses)
        with mock.patch.object(cluster, 'configure_metallb_addresses') as cfg:
            cluster.expand_addresses(1)
        cfg.assert_called_once_with()
        return client

    def test_an_ipv6_address_is_accepted(self):
        # ip_address() rather than a regexp precisely so that this is not
        # a new restriction: the check is "is this an address", not "does
        # it look like the addresses we have seen so far".
        client = self._expand_past_the_check(['fd00::1'])

        self.assertEqual(1, client.routed_serial)

    def test_an_empty_list_is_not_an_error(self):
        # A cluster with metallb and no addresses yet is the ordinary
        # case for the first expand-addresses.
        client = self._expand_past_the_check([])

        self.assertEqual(1, client.routed_serial)

    def test_an_integer_is_not_an_address(self):
        # ipaddress.ip_address(1) is 0.0.0.1, so a bare int passes the
        # address parse and would then reach str.join() and raise
        # TypeError from a place that cannot name the key. The metadata
        # document is JSON, so an int in this list is a thing a writer can
        # put there.
        client, cluster = self._cluster(['192.168.10.1', 1])

        e = self.assertRaises(exceptions.ClusterMetadataError,
                              cluster.expand_addresses, 1)

        self.assertEqual(1, e.value)
        self.assertEqual(0, client.routed_serial)

    def test_a_boolean_is_not_an_address_either(self):
        # Same reason: ip_address(True) is 0.0.0.1, and JSON has booleans.
        client, cluster = self._cluster([True])

        self.assertRaises(exceptions.ClusterMetadataError,
                          cluster.expand_addresses, 1)

        self.assertEqual(0, client.routed_serial)

    def test_an_absent_key_is_not_an_error(self):
        # Older clusters predate the key, and _require_addresses() is
        # reached before anything reads it for real.
        client, cluster = self._cluster([])
        del client.metadata[MD_KEY]['routed_addresses']

        with mock.patch.object(cluster, 'configure_metallb_addresses'):
            cluster.expand_addresses(1)

        self.assertEqual(1, client.routed_serial)


class StagedManifestsAreWhatWasValidatedTestCase(testtools.TestCase):
    """create() stages the manifests it read, not the files as they are later.

    The read at the top of create() exists so that a bad manifest costs an
    error rather than a half built cluster. Ten to twenty minutes of network
    allocation, instance creation, boot and OS update sit between that read
    and the write, so re-reading the paths there would make the early check
    a check of something else -- and a file which changed in the window
    would raise from inside install_control_plane(), with the name claimed
    and the metadata document stuck in 'initial'.
    """

    def setUp(self):
        super(StagedManifestsAreWhatWasValidatedTestCase, self).setUp()
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.tempdir = tempdir.name
        self.path = os.path.join(self.tempdir, 'staged.yaml')
        with open(self.path, 'w') as f:
            f.write('kind: Original\n')

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client = fakes.FakeClusterClient()

    def _writes(self):
        return [commandline for _, commandline in self.client.executed
                if commandline.startswith(
                    'cat - > %s/staged.yaml ' % cluster_module.K3S_MANIFEST_DIR)]

    def test_the_content_read_up_front_is_what_is_staged(self):
        original = 'kind: Original\n'

        def rewrite(*args, **kwargs):
            # Stand in for the twenty minutes: the file changes after
            # create() has validated it and before the write happens.
            with open(self.path, 'w') as f:
                f.write('kind: Replaced\n')
            return fakes.FakeClusterClient.get_instance_interfaces(
                self.client, *args, **kwargs)

        with mock.patch.object(self.client, 'get_instance_interfaces',
                               side_effect=rewrite):
            _make_cluster(self.client).create(1, 1, 1, manifests=[self.path])

        writes = self._writes()
        self.assertEqual(1, len(writes), self.client.executed)
        self.assertIn(original, writes[0])
        self.assertNotIn('kind: Replaced', writes[0])

    def test_a_manifest_deleted_after_validation_still_reaches_the_node(self):
        def remove(*args, **kwargs):
            os.unlink(self.path)
            return fakes.FakeClusterClient.get_instance_interfaces(
                self.client, *args, **kwargs)

        with mock.patch.object(self.client, 'get_instance_interfaces',
                               side_effect=remove):
            _make_cluster(self.client).create(1, 1, 1, manifests=[self.path])

        self.assertEqual(1, len(self._writes()))
        self.assertEqual('created',
                         self.client.metadata[MD_KEY]['state'])

    def test_install_control_plane_still_reads_paths_for_a_direct_caller(self):
        # staged is create()'s optimisation, not a new requirement: a
        # library caller invoking install_control_plane() on its own has had
        # nothing check its arguments, so the paths are still read there.
        self.client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 1, 'node_network': 'net-1', 'k3s_version': 'v1.33',
            'api_address_floating': '192.168.10.100',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
            'routed_addresses': []
        }
        self.client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        _make_cluster(self.client).install_control_plane(manifests=[self.path])

        writes = self._writes()
        self.assertEqual(1, len(writes), self.client.executed)
        self.assertIn('kind: Original\n', writes[0])

    def test_a_direct_caller_with_no_manifests_stages_nothing(self):
        self.client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 1, 'node_network': 'net-1', 'k3s_version': 'v1.33',
            'api_address_floating': '192.168.10.100',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
            'routed_addresses': []
        }
        self.client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        _make_cluster(self.client).install_control_plane()

        self.assertEqual([], self._writes())


class SshKeyIsReadThroughTheHierarchyTestCase(testtools.TestCase):
    """The other local path a caller hands create() is refused the same way.

    The CLI has click.Path(exists=True) on --sshkey; a library caller has
    nothing, and phase 5's Ansible module catches K3sClusterException. An
    OSError out of create() is therefore a traceback in somebody's playbook
    rather than an error message, which is the same defect ManifestError's
    unreadable() exists to prevent for manifests.
    """

    def setUp(self):
        super(SshKeyIsReadThroughTheHierarchyTestCase, self).setUp()
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.tempdir = tempdir.name

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client = fakes.FakeClusterClient()

    def test_a_key_which_is_not_there_is_refused_in_the_hierarchy(self):
        missing = os.path.join(self.tempdir, 'no-such-key.pub')

        e = self.assertRaises(
            exceptions.SshKeyError,
            _make_cluster(self.client).create, 1, 1, 1, sshkey=missing)

        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual(missing, e.path)
        self.assertIn(missing, str(e))

    def test_a_key_which_cannot_be_decoded_is_refused_the_same_way(self):
        path = os.path.join(self.tempdir, 'binary.pub')
        with open(path, 'wb') as f:
            f.write(b'ssh-rsa AAAA\xff\xfe comment\n')

        self.assertRaises(
            exceptions.SshKeyError,
            _make_cluster(self.client).create, 1, 1, 1, sshkey=path)

    def test_a_key_with_a_non_ascii_comment_is_read_and_passed_on(self):
        path = os.path.join(self.tempdir, 'accented.pub')
        content = 'ssh-rsa AAAAB3Nz josé@example.com\n'
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)

        _make_cluster(self.client).create(1, 1, 1, sshkey=path)

        self.assertEqual([content], sorted(set(self.client.instance_sshkeys)))


def _repository_python_files():
    """Every .py file this repository ships, not only the package's.

    The scan used to be os.listdir() over the package directory alone,
    which is how an unencoded pathlib call in tools/build-collection.py
    reached the review of #90: the lint existed, and the file it needed
    to read was outside the only directory it looked in. collection/
    has the same exposure -- its module has no open() today and nothing
    stops one being added.

    Directories which are absent are skipped rather than failed, the
    way ModuleTestCase skips: an installed copy of this package has the
    tests but neither tools/ nor collection/.

    tests/ is deliberately out of scope, and that is a boundary rather
    than an oversight: the original scan was a non-recursive listdir of
    the package directory, so it never covered this directory, and
    twenty call sites in five test files have grown up unencoded behind
    that. Fixing them is a mechanical change to files which have
    nothing to do with the collection, so they are tracked in
    shakenfist/client-python-k3s#93 rather than folded into a review
    round. The hazard there is also
    the milder one -- a fixture written in the locale encoding can make
    a test pass or fail by machine, which is a bad day for whoever is
    debugging it, but it is not bytes shipped to a cluster.
    """
    package_dir = os.path.dirname(cluster_module.__file__)
    roots = [package_dir]

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(
        cluster_module.__file__)))
    for extra in (os.path.join(repo_root, 'tools'),
                  os.path.join(repo_root, 'collection')):
        if os.path.isdir(extra):
            roots.append(extra)

    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            # ansible-lint's working tree is a copy of the collection,
            # so scanning it would report every offender twice under a
            # path nobody edits. tests/ is excluded for the reason in
            # the docstring.
            dirnames[:] = [d for d in sorted(dirnames)
                           if d not in ('.ansible', '__pycache__',
                                        'tests')]
            for name in sorted(filenames):
                if name.endswith('.py'):
                    yield os.path.join(dirpath, name)


class NoShellInvocationTestCase(testtools.TestCase):
    """Nothing in this package asks subprocess for a shell.

    Rule 3 at the top of cluster.py, as a property of the tree rather
    than of one call site. Every local command this package runs has an
    argument list available, so none needs a shell to parse a string --
    and the one site which asked for one passed a constant, which is how
    a reader comparing it with the argument lists beside it was left to
    work out for themselves that the difference did not matter. The
    useful property is that there is no next one, written with an
    interpolation in it.
    """

    def test_no_subprocess_call_asks_for_a_shell(self):
        """Rule 3 at the top of cluster.py, as a property of the tree.

        Every local command this package runs has an argument list
        available, so none of them needs a shell to parse a string -- and
        the one which asked for one was a constant, which is how a reader
        comparing it with the unset calls beside it was left to work out
        for themselves that the difference did not matter. The useful
        property is that there is no next one, written with an
        interpolation in it.
        """
        offenders = []

        for path in _repository_python_files():
            name = os.path.relpath(path, os.path.dirname(os.path.dirname(
                os.path.abspath(cluster_module.__file__))))
            with open(path, encoding='utf-8') as f:
                tree = ast.parse(f.read(), filename=path)

            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                for kw in node.keywords:
                    if (kw.arg == 'shell'
                            and isinstance(kw.value, ast.Constant)
                            and kw.value.value):
                        offenders.append('%s:%s' % (name, node.lineno))

        self.assertEqual([], offenders)


class ProgressIsStartedInOnePlaceTestCase(testtools.TestCase):
    """progress.Progress is constructed in exactly one place.

    What is repetitive about building one is the wiring -- the reporter
    is both the stream written to and the source of the verbose flag --
    and five entry points wrote it out by hand, three of them added by
    later phases, so the sixth was going to as well.
    Cluster.start_progress() is now that place. Asserted over the parsed
    source rather than by counting phase headers, because the defect is
    a new entry point written the old way, which no behavioural test
    would notice.
    """

    def test_only_start_progress_constructs_a_progress(self):
        path = cluster_module.__file__
        with open(path, encoding='utf-8') as f:
            tree = ast.parse(f.read(), filename=path)

        constructing = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            for inner in ast.walk(node):
                if (isinstance(inner, ast.Call)
                        and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr == 'Progress'):
                    constructing.append(node.name)

        self.assertEqual(['start_progress'], sorted(set(constructing)))


class FileEncodingIsStatedTestCase(testtools.TestCase):
    """Every text file this package opens names the encoding it is in.

    A general check rather than one assertion per call site, for the reason
    HeredocDelimiterTestCase is general: the defect the review found was one
    of five open() calls, and the useful property is that there is no sixth.
    Without an encoding, open() uses locale.getpreferredencoding(), so the
    same manifest, ssh key or kubeconfig is a different sequence of bytes
    depending on where the plugin runs -- and that is the quiet failure, not
    the UnicodeDecodeError.

    Checked against the parsed source, because the alternative is exercising
    every path under every locale. A new open() without an encoding fails
    here and names its own line number.
    """

    def test_no_open_call_leaves_the_encoding_to_the_locale(self):
        offenders = []

        for path in _repository_python_files():
            name = os.path.relpath(path, os.path.dirname(os.path.dirname(
                os.path.abspath(cluster_module.__file__))))
            with open(path, encoding='utf-8') as f:
                tree = ast.parse(f.read(), filename=path)

            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                # Both spellings: a bare open() and an io.open()/builtins.open()
                # have the same defect, and a check which only knew the first
                # would report no offenders for a module using the second.
                if isinstance(node.func, ast.Name):
                    if node.func.id != 'open':
                        continue
                elif isinstance(node.func, ast.Attribute):
                    if node.func.attr != 'open':
                        continue
                    # os.open() is the file descriptor call, not the text
                    # one: it returns an int, takes a mode rather than an
                    # encoding, and raises TypeError if given one. It is
                    # how a file is created with an explicit permission
                    # mode, which the kubeconfig write needs; the open()
                    # wrapped around the descriptor it returns is a
                    # separate call and is still checked here.
                    if (isinstance(node.func.value, ast.Name)
                            and node.func.value.id == 'os'):
                        continue
                else:
                    continue
                # A binary mode open has no encoding to state.
                modes = [a.value for a in node.args[1:2]
                         if isinstance(a, ast.Constant)]
                if modes and 'b' in modes[0]:
                    continue
                if not any(kw.arg == 'encoding' for kw in node.keywords):
                    offenders.append('%s:%s' % (name, node.lineno))

        self.assertEqual([], offenders)

    def test_no_pathlib_text_call_leaves_the_encoding_to_the_locale(self):
        """The same defect through Path.read_text() and Path.write_text().

        Both default to locale.getpreferredencoding() exactly as open()
        does, and neither is an open() call, so the check above cannot see
        them. This is the spelling the review of #90 actually found, in
        tools/build-collection.py's round trip of galaxy.yml -- which is
        ASCII, so the bug was latent rather than live, which is precisely
        the kind that survives a review nobody asks for.
        """
        offenders = []

        for path in _repository_python_files():
            name = os.path.relpath(path, os.path.dirname(os.path.dirname(
                os.path.abspath(cluster_module.__file__))))
            with open(path, encoding='utf-8') as f:
                tree = ast.parse(f.read(), filename=path)

            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if not isinstance(node.func, ast.Attribute):
                    continue
                if node.func.attr not in ('read_text', 'write_text'):
                    continue
                if not any(kw.arg == 'encoding' for kw in node.keywords):
                    offenders.append('%s:%s' % (name, node.lineno))

        self.assertEqual([], offenders)
