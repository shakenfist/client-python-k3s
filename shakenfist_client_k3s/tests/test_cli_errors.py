"""Which exception each failure path raises, and what the CLI does with it.

The orchestration raises instead of calling sys.exit(), so there are two
things to hold still. The first is that each site raises the right
exception, carrying the right structured fields, and prints nothing on the
way -- that is what makes the library usable from an Ansible module, which
owns stdout for its own JSON result. The second is that the command line
behaves exactly as it did before: the same text, on stdout, with exit code
1. The tests below assert the first by invoking the subcommand object
directly (which bypasses the group, so the exception escapes to the test)
and the second by invoking through the group, where the handler catches it.
"""

import copy
import time

import click
from click.testing import CliRunner

# The PyPI mock backport is used rather than unittest.mock for consistency
# with the other tests here, as the project supports Python >= 3.7.
import mock
from shakenfist_client import apiclient
import testtools

import shakenfist_client_k3s
from shakenfist_client_k3s import cluster as cluster_module
from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives


MD_KEY = cluster_module.METADATA_KEY % 'banana'
CLUSTER_LIST = 'orchestrated_k3s_clusters'

# Enough cluster metadata for delete to run to completion: no nodes to wait
# for and no node network to unroute addresses from, so it reaches the
# local kubectl cleanup, which is the failure under test.
DELETABLE_MD = {
    'name': 'banana', 'namespace': 'clientns', 'state': 'created',
    'control_plane_nodes': [], 'worker_nodes': [], 'routed_addresses': [],
    'node_network': None,
}

K3S_CHANNELS = {'data': [{'name': 'stable', 'latest': 'v1.33.4+k3s1'}]}


def _version_cache():
    """A k3s version cache fresh enough that create does not fetch releases."""
    return {
        primitives.K3S_VERSION_CACHE_KEY: {
            'updated': time.time(), 'releases': {'stable': 'v1.33.4+k3s1'}}
    }


def _response(payload, status_code=200, text='the server said no'):
    resp = mock.MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.json.return_value = payload
    return resp


class ClientTestCase(testtools.TestCase):
    """Shared plumbing: a mocked API client and a Click runner."""

    def setUp(self):
        super(ClientTestCase, self).setUp()
        self.client = mock.MagicMock()
        self.client.namespace = 'clientns'
        patcher = mock.patch(
            'shakenfist_client_k3s.apiclient.Client', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.runner = CliRunner()

    def _invoke(self, target, args, namespace_metadata=None):
        if namespace_metadata is not None:
            self.client.get_namespace_metadata.return_value = namespace_metadata
        return self.runner.invoke(
            target, args, obj={'VERBOSE': False}, terminal_width=80)


class CommandExceptionTestCase(ClientTestCase):
    """Each command's failure path raises its exception, and prints nothing.

    These invoke the subcommand object rather than the group, so the
    exception is not caught by the group handler and the test can see its
    type and fields. Nothing may be written to stdout on the way: the text
    now belongs to the exception, and a library caller must be able to keep
    stdout to itself.
    """

    def _assert_raises(self, target, args, expected_class,
                       namespace_metadata=None):
        result = self._invoke(target, args, namespace_metadata)
        self.assertIsInstance(result.exception, expected_class)
        self.assertEqual('', result.output)
        return result.exception

    def test_create_name_in_cluster_list(self):
        md = dict(_version_cache())
        md[CLUSTER_LIST] = ['banana']
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_create, ['banana'],
            exceptions.ClusterExistsError, md)
        self.assertEqual('banana', e.name)

    def test_create_name_has_metadata(self):
        md = dict(_version_cache())
        md[MD_KEY] = {'name': 'banana', 'state': 'created'}
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_create, ['banana'],
            exceptions.ClusterExistsError, md)
        self.assertEqual('banana', e.name)

    def test_create_network_missing(self):
        self.client.get_network.return_value = None
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_create, ['banana', '--network', 'nosuch'],
            exceptions.NetworkNotFoundError, dict(_version_cache()))
        self.assertEqual('nosuch', e.network)

    def test_getconfig_unknown_cluster(self):
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_getconfig, ['banana'],
            exceptions.ClusterNotFoundError, {})
        self.assertEqual('unknown_cluster', e.reason)
        self.assertEqual('banana', e.name)

    def test_getconfig_without_kubeconfig(self):
        # The cluster exists but is half built, which is a different
        # failure to it not existing, and consumers need the difference.
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_getconfig, ['banana'],
            exceptions.ClusterIncompleteError,
            {MD_KEY: {'name': 'banana', 'kubeconfig': None}})
        self.assertNotIsInstance(e, exceptions.ClusterNotFoundError)
        self.assertEqual('banana', e.name)

    def test_show_missing_cluster(self):
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_show, ['banana'],
            exceptions.ClusterNotFoundError, {})
        self.assertEqual('does_not_exist', e.reason)

    def test_delete_missing_cluster(self):
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_delete, ['banana'],
            exceptions.ClusterNotFoundError, {})
        self.assertEqual('does_not_exist', e.reason)

    def test_expand_workers_missing_cluster(self):
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_expand_workers, ['banana'],
            exceptions.ClusterNotFoundError, {})
        self.assertEqual('not_found', e.reason)

    def test_expand_addresses_missing_cluster(self):
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_expand_addresses, ['banana'],
            exceptions.ClusterNotFoundError, {})
        self.assertEqual('not_found', e.reason)

    def test_update_os_missing_cluster(self):
        e = self._assert_raises(
            shakenfist_client_k3s.k3s_update_os, ['banana'],
            exceptions.ClusterNotFoundError, {})
        self.assertEqual('not_found', e.reason)

    def test_delete_kubectl_unset_failure(self):
        completed = mock.MagicMock()
        completed.returncode = 1
        with mock.patch('subprocess.run',
                        return_value=completed):
            e = self._assert_raises(
                shakenfist_client_k3s.k3s_delete, ['banana'],
                exceptions.KubeconfigError,
                {CLUSTER_LIST: ['banana'], MD_KEY: copy.deepcopy(DELETABLE_MD)})

        self.assertEqual('unset_failed', e.reason)
        self.assertEqual('users.banana.clientns', e.config_elem)

    def test_query_k3s_version_http_error(self):
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_response(None, status_code=500)):
            e = self._assert_raises(
                shakenfist_client_k3s.k3s_query_k3s_version,
                ['stable', '--refresh-version-cache'],
                exceptions.ReleaseLookupError, {})

        self.assertEqual('http_status', e.reason)
        self.assertEqual('k3s', e.product)

    def test_query_k3s_version_unknown_channel(self):
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_response(K3S_CHANNELS)):
            e = self._assert_raises(
                shakenfist_client_k3s.k3s_query_k3s_version,
                ['banana', '--refresh-version-cache'],
                exceptions.ReleaseLookupError, {})

        self.assertEqual('unknown_channel', e.reason)

    def test_query_longhorn_version_http_error(self):
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_response(None, status_code=500)):
            e = self._assert_raises(
                shakenfist_client_k3s.k3s_query_longhorn_version,
                ['--refresh-version-cache'],
                exceptions.ReleaseLookupError, {})

        self.assertEqual('Longhorn', e.product)


class GroupHandlerTestCase(ClientTestCase):
    """The group handler reproduces the pre-refactor CLI failure behaviour.

    Every one of these printed its message and called sys.exit(1) before
    this change, and the text and exit code are the contract. The output
    asserted here is the whole of result.output, so an extra blank line or
    a message which moved to stderr fails the test.
    """

    def _assert_cli_failure(self, args, expected_output, namespace_metadata=None):
        result = self._invoke(
            shakenfist_client_k3s.k3s, args, namespace_metadata)
        self.assertEqual(1, result.exit_code, result.output)
        self.assertEqual(expected_output, result.output)

    def test_getconfig_unknown_cluster(self):
        self._assert_cli_failure(
            ['getconfig', 'banana'], 'Unknown cluster\n', {})

    def test_getconfig_without_kubeconfig(self):
        self._assert_cli_failure(
            ['getconfig', 'banana'],
            'No kubeconfig for this cluster. Is it fully installed?\n',
            {MD_KEY: {'name': 'banana', 'kubeconfig': None}})

    def test_show_missing_cluster(self):
        self._assert_cli_failure(
            ['show', 'banana'],
            'Sorry, that cluster name does not appear to exist\n', {})

    def test_delete_missing_cluster(self):
        self._assert_cli_failure(
            ['delete', 'banana'],
            'Sorry, that cluster name does not appear to exist\n', {})

    def test_expand_workers_missing_cluster(self):
        self._assert_cli_failure(
            ['expand-workers', 'banana'], 'Cluster not found!\n', {})

    def test_expand_addresses_missing_cluster(self):
        self._assert_cli_failure(
            ['expand-addresses', 'banana'], 'Cluster not found!\n', {})

    def test_update_os_missing_cluster(self):
        self._assert_cli_failure(
            ['update-os', 'banana'], 'Cluster not found!\n', {})

    def test_create_name_taken(self):
        md = dict(_version_cache())
        md[CLUSTER_LIST] = ['banana']
        self._assert_cli_failure(
            ['create', 'banana'],
            'Sorry, that cluster name is already taken\n', md)

    def test_create_network_missing(self):
        self.client.get_network.return_value = None
        self._assert_cli_failure(
            ['create', 'banana', '--network', 'nosuch'],
            'Specified network does not exist\n', dict(_version_cache()))

    def test_delete_kubectl_unset_failure(self):
        completed = mock.MagicMock()
        completed.returncode = 1
        with mock.patch('subprocess.run',
                        return_value=completed):
            self._assert_cli_failure(
                ['delete', 'banana'],
                'Could not unset kubectl config element users.banana.clientns\n',
                {CLUSTER_LIST: ['banana'], MD_KEY: copy.deepcopy(DELETABLE_MD)})

    def test_query_k3s_version_http_error(self):
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_response(None, status_code=500)):
            self._assert_cli_failure(
                ['query-k3s-version', 'stable', '--refresh-version-cache'],
                'Unable to determine latest k3s release version\n'
                '    GET https://update.k3s.io/v1-release/channels\n'
                '    returned HTTP status code 500 with text:\n'
                '    the server said no\n', {})

    def test_query_longhorn_version_no_parsable_release(self):
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_response([])):
            self._assert_cli_failure(
                ['query-longhorn-version', '--refresh-version-cache'],
                'Unable to determine the latest Longhorn release\n', {})


class GroupHandlerScopeTestCase(testtools.TestCase):
    """What the group handler catches, and just as importantly what it does not."""

    def _group(self, error):
        @click.group(cls=shakenfist_client_k3s.GroupCatchClusterExceptions)
        def fake_group():
            ...

        @fake_group.command(name='boom')
        def boom():
            raise error

        return fake_group

    def test_cluster_exception_becomes_text_and_exit_one(self):
        group = self._group(exceptions.ClusterNotFoundError.not_found('banana'))

        result = CliRunner().invoke(group, ['boom'], terminal_width=80)

        self.assertEqual(1, result.exit_code)
        self.assertEqual('Cluster not found!\n', result.output)
        self.assertIsInstance(result.exception, SystemExit)

    def test_multiline_message_is_printed_verbatim(self):
        # The two agent failures carry the most fields and render the
        # longest text, and they are the ones the pre-refactor code printed
        # a line at a time. The handler prints str(e) once, so the whole
        # block has to arrive unwrapped and unindented, with exactly one
        # trailing newline, on stdout.
        error = exceptions.CommandFailedError(
            'node-001', 'uuid-001', 'kubectl wait pods', 1,
            'still waiting', 'timed out on pod one\ntimed out on pod two')
        group = self._group(error)

        result = CliRunner().invoke(group, ['boom'], terminal_width=80)

        self.assertEqual(1, result.exit_code)
        self.assertEqual(
            'Command failed!\n'
            '  instance: node-001 (UUID uuid-001)\n'
            '  command: kubectl wait pods\n'
            'exit code: 1\n'
            '   stdout: still waiting\n'
            '   stderr: timed out on pod one\n'
            '   stderr: timed out on pod two\n',
            result.output)

    def test_apiclient_exceptions_are_left_alone(self):
        # The parent CLI's GroupCatchExceptions maps every apiclient
        # exception to its own error line and exit code. Catching one here
        # would silently take that over, so the handler must let it past
        # untouched.
        error = apiclient.UnauthorizedException(
            'nope', 'GET', 'http://sf/instances', 401, 'not for you')
        group = self._group(error)

        result = CliRunner().invoke(group, ['boom'], terminal_width=80)

        self.assertIs(error, result.exception)
        self.assertEqual('', result.output)
