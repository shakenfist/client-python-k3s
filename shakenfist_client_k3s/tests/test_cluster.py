import io
import tempfile

# The PyPI mock backport is used for consistency with the other tests in
# this package, which support Python >= 3.7.
import mock
import testtools

from shakenfist_client_k3s import cluster as cluster_module
from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import progress
from shakenfist_client_k3s.cluster import Cluster
from shakenfist_client_k3s.tests import fakes


MD_KEY = cluster_module.METADATA_KEY % 'banana'


class FakeTty(io.StringIO):
    def isatty(self):
        return True


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

    def test_reporter_verbosity_selects_the_output_mode(self):
        # A verbose reporter's debug lines would interleave badly with in
        # place cursor updates, which is why Progress refuses interactive
        # mode when verbose. Driven from a terminal, the Cluster must pass
        # both the verbosity and the terminal through, because between
        # them they choose the CLI's entire output format.
        with mock.patch('sys.stdout', FakeTty()):
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


class ExpandWorkersTestCase(testtools.TestCase):
    """Expanding a cluster installs k3s on the new workers and no others.

    The k3s agent installer is not idempotent in a way that is safe to
    rely on: re-running it on a worker which is already carrying
    workloads restarts the agent on a live node. An expand which hands
    install_workers() the whole of md['worker_nodes'] therefore damages
    every node it did not create, which is why the argument exists.
    """

    def _expand(self, existing_workers, worker_count, new_instances):
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


class CreateInstallsWorkersTestCase(testtools.TestCase):
    """Creating a cluster installs k3s on every worker it just created.

    create() hands install_workers() md['worker_nodes'], and that is the
    right list only by aliasing: get_metadata() caches the metadata
    dictionary and set_metadata() stores that same object, so the list
    create() is holding is the one create_and_await_instances() appended
    the new workers to. Nothing at the call site says so. Make the cache
    copy on read and create() would pass the empty list it initialised the
    metadata with, install k3s on no workers at all, and still report a
    cluster as ready. This is the test which notices.

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

        # create() writes ~/.kube/config unconditionally, and must not be
        # allowed to write the operator's own.
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
        for target, release in [('get_k3s_release', 'stable'),
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


class RecordingClient(fakes.FakeClusterClient):
    """A scripted client which records executes and deletes in one ordered log.

    The assertion this class exists for -- that a worker is drained and
    removed from k3s before its instance is destroyed -- cannot be made
    from two separate call lists, because neither of them knows where in
    the other its own calls fell. One log, in the order the client was
    asked, is the only shape which can answer "before".
    """

    def __init__(self):
        super(RecordingClient, self).__init__()
        self.actions = []

    def instance_execute(self, instance_ref, commandline):
        self.actions.append(('execute', instance_ref, commandline))
        return super(RecordingClient, self).instance_execute(
            instance_ref, commandline)

    def delete_instance(self, instance_ref):
        self.actions.append(('delete_instance', instance_ref, None))
        return super(RecordingClient, self).delete_instance(instance_ref)


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
        self.client = RecordingClient()

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
              '--delete-emptydir-data --kubeconfig /etc/rancher/k3s/k3s.yaml'),
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
        client = RecordingClient()
        cluster = _make_cluster(client)

        self.assertRaises(
            exceptions.ClusterNotFoundError, cluster.remove_worker,
            ['inst-w1'])
