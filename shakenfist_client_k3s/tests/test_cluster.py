import io
import tempfile

# The PyPI mock backport is used for consistency with the other tests in
# this package, which support Python >= 3.7.
import mock
import testtools

from shakenfist_client_k3s import cluster as cluster_module
from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
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
        return _make_cluster(mock.MagicMock()).interrupted_state(md)

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

        # create() looks up the k3s release before it looks at the name.
        patcher = mock.patch(
            'shakenfist_client_k3s.primitives.get_k3s_release',
            return_value='stable')
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
        self.client = RecordingClient()
        self.client.metadata[primitives.CLUSTER_LIST] = ['banana']

        patcher = mock.patch('time.sleep', lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

        # Unmocked, delete's three 'kubectl config unset' calls would edit
        # the operator's own ~/.kube/config.
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
        client = RecordingClient()
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
