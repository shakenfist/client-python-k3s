import copy
import io
import os
import subprocess
import tempfile

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

        self.assertEqual(
            [{'uuid': 'inst-cp1', 'role': 'control_plane',
              'name': 'k3s-banana-node-001', 'exists': True,
              'state': 'created', 'agent_state': 'ready', 'healthy': True},
             {'uuid': 'inst-w1', 'role': 'worker',
              'name': 'k3s-banana-node-002', 'exists': True,
              'state': 'created', 'agent_state': 'ready', 'healthy': True},
             {'uuid': 'inst-w2', 'role': 'worker',
              'name': 'k3s-banana-node-003', 'exists': True,
              'state': 'created', 'agent_state': 'ready', 'healthy': True}],
            report['nodes'])
        self.assertEqual('created', report['state'])
        self.assertFalse(report['interrupted'])
        self.assertTrue(report['healthy'])
        self.assertEqual('banana', report['name'])
        self.assertEqual('testns', report['namespace'])

    def test_the_api_is_probed_on_the_first_control_plane_node(self):
        report = self.cluster.health()

        self.assertEqual(
            [('inst-cp1',
              'kubectl get nodes --kubeconfig /etc/rancher/k3s/k3s.yaml')],
            self.client.executed)
        self.assertTrue(report['api']['probed'])
        self.assertTrue(report['api']['answered'])
        self.assertEqual('inst-cp1', report['api']['instance_uuid'])
        self.assertEqual(0, report['api']['return_code'])
        self.assertIn('k3s-banana-node-001   Ready', report['api']['stdout'])
        self.assertIsNone(report['api']['error'])

    def test_nothing_is_repaired(self):
        # health() must not be the verb which quietly fixes things, so the
        # only thing it is allowed to ask the cluster to do is the read only
        # probe: no metadata write, no instance created or destroyed, no
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
            ['kubectl get nodes --kubeconfig /etc/rancher/k3s/k3s.yaml'],
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
             'healthy': False},
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
    create() rather than a loop inside install_control_plane(). Five of the
    six refusals are each a way a manifest would otherwise be lost silently
    -- overwritten by another manifest, copied to a filename k3s never
    looks at, truncated by its own content, rejected on the node by a
    parser nobody is watching -- or, in the case of an unreadable file, the
    check which stands in for click.Path(exists=True) for a caller with no
    click. The sixth is about the name rather than the content: the
    basename is interpolated into a shell command line which runs as root
    on the control plane node.
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
        with open(path, 'w') as f:
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


class HeredocDelimiterTestCase(testtools.TestCase):
    """Every heredoc this module generates has a quoted delimiter.

    Rule 2 at the top of cluster.py. An unquoted delimiter lets the remote
    shell expand $, backticks and $( ) inside the body, and every heredoc
    here carries a value Python already substituted, so there is nothing
    for the shell to be expanding. This asserts the rule over the
    generated commands rather than per site, because the failure mode is a
    new heredoc written the old way rather than one of these two changing
    back.
    """

    def _commands(self):
        client = fakes.FakeClusterClient()
        client.metadata[MD_KEY] = {
            'name': 'banana', 'namespace': 'testns', 'state': 'created',
            'node_serial': 1, 'node_network': 'net-1', 'node_token': None,
            'server_token': None, 'k3s_version': 'v1.33',
            'api_address_floating': '192.168.10.100',
            'api_address_inner': '10.0.0.4',
            'control_plane_nodes': ['inst-cp1'], 'worker_nodes': [],
            'routed_addresses': ['192.168.10.101', '192.168.10.102']
        }
        client.instances['inst-cp1'] = {
            'uuid': 'inst-cp1', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}

        with tempfile.TemporaryDirectory() as tempdir:
            manifest = os.path.join(tempdir, 'staged.yaml')
            with open(manifest, 'w') as f:
                f.write('kind: One\n')

            cluster = _make_cluster(client)
            cluster.install_control_plane(manifests=[manifest])
            _make_cluster(client).configure_metallb_addresses()

        return [commandline for _, commandline in client.executed]

    def test_no_generated_heredoc_is_unquoted(self):
        heredocs = []
        for commandline in self._commands():
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
    metadata, and then waits five minutes for a metallb pod in a namespace
    which does not exist before failing -- leaving the caller with routed
    addresses nothing can hand out. The refusal has to come before the
    allocation, which is what these assert.
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

    def _await_idle(self, aop_states):
        client = mock.MagicMock()
        client.get_instance.return_value = {
            'uuid': 'inst-001', 'name': 'k3s-banana-node-001',
            'state': 'created', 'agent_state': 'ready'}
        aops = [{'uuid': 'aop-%03d' % (i + 1), 'instance_uuid': 'inst-001',
                 'state': state, 'commands': [], 'results': {}}
                for i, state in enumerate(aop_states)]

        # An empty first answer, so nothing is snapshotted as a
        # pre-existing failure and the states below are the ones the wait
        # actually sees.
        client.get_instance_agentoperations.side_effect = [[]] + [aops] * 50
        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        with mock.patch('time.sleep', lambda seconds: None):
            return cluster.await_idle(['inst-001'])

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
        client.get_instance_agentoperations.side_effect = [[], running, done]

        cluster = Cluster(client, 'banana', 'testns',
                          reporter=progress.CollectingReporter())
        with mock.patch('time.sleep', lambda seconds: None):
            cluster.await_idle(['inst-001'])

        self.assertEqual(3, client.get_instance_agentoperations.call_count)

    def test_a_preexisting_expired_operation_does_not_wedge_the_wait(self):
        # The snapshot at the top of await_idle() exists so a historical
        # failure neither wedges the wait nor aborts it. It covered
        # 'error' only, so a historical expired operation did both.
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
            cluster.await_idle(['inst-001'])


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

        with mock.patch('time.time', lambda: clock[0]), \
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
        self.assertEqual(3, elapsed)

    def test_no_timeout_keeps_waiting(self):
        # Which is every caller but the probe. An install which takes
        # eleven minutes is a slow install, and abandoning it would leave
        # the caller believing a command it can still see running did not
        # happen.
        client, cluster = self._cluster(['queued'] * 600 + ['complete'])

        aop, elapsed = self._with_clock(
            lambda: cluster.await_execute(_pending_aop()))

        self.assertEqual('complete', aop['state'])
        self.assertEqual(601, elapsed)


class HealthProbeIsSkippedTestCase(testtools.TestCase):
    """health() does not ask a node which it can already see cannot answer.

    This is the bug the verb existed to avoid and had: an agent operation
    queued against an instance whose agent is not connected is accepted by
    the API and then never runs, so the probe waited on exactly the cluster
    health() is for. The node entry has already read the state and
    agent_state which say so, a few lines earlier in the same method.
    """

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
        self.cluster = _make_cluster(self.client)

    def test_an_agentless_control_plane_node_is_not_asked(self):
        self.client.instances['inst-cp1']['agent_state'] = None

        report = self.cluster.health()

        self.assertEqual([], self.client.executed)
        self.assertFalse(report['api']['probed'])
        self.assertFalse(report['api']['answered'])
        self.assertFalse(report['healthy'])
        self.assertIn('not in a state which can answer', report['api']['error'])
        self.assertIn('agent not contactable', report['api']['error'])

    def test_an_errored_control_plane_node_is_not_asked(self):
        self.client.instances['inst-cp1']['state'] = 'error'

        report = self.cluster.health()

        self.assertEqual([], self.client.executed)
        self.assertIn('instance error', report['api']['error'])

    def test_a_vanished_control_plane_node_is_not_asked(self):
        del self.client.instances['inst-cp1']

        report = self.cluster.health()

        self.assertEqual([], self.client.executed)
        self.assertFalse(report['api']['probed'])
        self.assertEqual('inst-cp1', report['api']['instance_uuid'])
        self.assertIn('instance gone', report['api']['error'])

    def test_an_unhealthy_worker_does_not_stop_the_probe(self):
        # Only the node the probe runs on decides whether to run it. A
        # worker in the error state is a finding about that worker, and the
        # k3s API still has something to say about it.
        self.client.instances['inst-w1']['state'] = 'error'

        report = self.cluster.health()

        self.assertEqual(1, len(self.client.executed))
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

        self.assertEqual(1, len(self.client.executed))
        self.assertFalse(report['api']['probed'])
        self.assertIn('had not finished after 0 seconds',
                      report['api']['error'])
        self.assertIn('still queued', report['api']['error'])

    def test_an_expired_probe_is_a_finding_and_names_the_state(self):
        self.client.probe_state = 'expired'

        report = self.cluster.health()

        self.assertTrue(report['api']['probed'])
        self.assertFalse(report['api']['answered'])
        self.assertIn('expired state', report['api']['error'])


class ActionLogFailingDrainClient(ActionLogClient):
    """An action log client whose kubectl drain (and optionally uncordon) fails."""

    def __init__(self, uncordon_fails=False):
        super(ActionLogFailingDrainClient, self).__init__()
        self.uncordon_fails = uncordon_fails

    def instance_execute(self, instance_ref, commandline):
        self.actions.append(('execute', instance_ref, commandline))
        failing = (commandline.startswith('kubectl drain')
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

    def _cluster(self, uncordon_fails=False):
        client = ActionLogFailingDrainClient(uncordon_fails=uncordon_fails)
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
