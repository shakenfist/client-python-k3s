import time

from click.testing import CliRunner

# The PyPI mock backport is used rather than unittest.mock for consistency
# with test_primitives.py, as the project supports Python >= 3.7.
import mock
import testtools

import shakenfist_client_k3s
from shakenfist_client_k3s import cluster as cluster_module
from shakenfist_client_k3s import primitives


class NamespaceDefaultingTestCase(testtools.TestCase):
    """Commands must default --namespace to the client's own namespace.

    Every command resolves the --namespace option onto the Cluster it
    builds, whose namespace the primitives pass directly to namespace
    metadata API calls. If the option is not given the value must come
    from the client (and never remain None, which the API client rejects
    with a TypeError).
    """

    def setUp(self):
        super(NamespaceDefaultingTestCase, self).setUp()
        self.client = mock.MagicMock()
        self.client.namespace = 'clientns'
        patcher = mock.patch(
            'shakenfist_client_k3s.apiclient.Client', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.runner = CliRunner()

    def _invoke(self, args):
        return self.runner.invoke(
            shakenfist_client_k3s.k3s, args, obj={'VERBOSE': False})

    def test_show_defaults_namespace_from_client(self):
        md_key = cluster_module.METADATA_KEY % 'banana'
        self.client.get_namespace_metadata.return_value = {
            md_key: {'name': 'banana', 'state': 'created'}}

        result = self._invoke(['show', 'banana'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('clientns')

    def test_show_explicit_namespace_wins(self):
        md_key = cluster_module.METADATA_KEY % 'banana'
        self.client.get_namespace_metadata.return_value = {
            md_key: {'name': 'banana', 'state': 'created'}}

        result = self._invoke(['show', 'banana', '--namespace', 'otherns'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('otherns')

    def test_list_defaults_namespace_from_client(self):
        self.client.get_namespace_metadata.return_value = {}

        result = self._invoke(['list'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('clientns')

    def test_query_k3s_version_defaults_namespace_from_client(self):
        self.client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'releases': {'stable': 'v1.33.4+k3s1'}
            }
        }

        result = self._invoke(['query-k3s-version', 'stable'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('clientns')
        self.assertIn('v1.33.4+k3s1', result.output)


class CreateNamespaceNoticeTestCase(testtools.TestCase):
    """create's 'Created namespace %s' notice now goes through a reporter.

    Step 1d moves this off a bare print() and onto the reporter that is
    also handed to the Cluster, so this pins the printed text exactly:
    routing it must not have changed a single character.
    """

    def setUp(self):
        super(CreateNamespaceNoticeTestCase, self).setUp()
        self.client = mock.MagicMock()
        self.client.namespace = 'clientns'
        self.client.get_namespace.return_value = None
        self.client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'releases': {'stable': 'v1.33.4+k3s1'}
            },
            # The name is already registered, so create fails fast right
            # after the namespace-created notice, before touching instances.
            primitives.CLUSTER_LIST: ['banana'],
        }
        patcher = mock.patch(
            'shakenfist_client_k3s.apiclient.Client', return_value=self.client)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.runner = CliRunner()

    def test_namespace_created_notice_text_is_unchanged(self):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s,
            ['create', 'banana', '--namespace', 'newns'],
            obj={'VERBOSE': False})

        self.client.create_namespace.assert_called_once_with('newns')
        self.assertIn('Created namespace newns\n', result.output)
        self.assertEqual(1, result.exit_code)
