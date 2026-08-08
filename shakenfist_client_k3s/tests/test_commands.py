import time

from click.testing import CliRunner

# The PyPI mock backport is used rather than unittest.mock for consistency
# with test_primitives.py, as the project supports Python >= 3.7.
import mock
import testtools

import shakenfist_client_k3s
from shakenfist_client_k3s import primitives


class NamespaceDefaultingTestCase(testtools.TestCase):
    """Commands must default --namespace to the client's own namespace.

    Every command stores the --namespace option in ctx.obj['namespace'],
    which the primitives pass directly to namespace metadata API calls. If
    the option is not given the value must come from the client (and never
    remain None, which the API client rejects with a TypeError).
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
        md_key = primitives.METADATA_KEY % 'banana'
        self.client.get_namespace_metadata.return_value = {
            md_key: {'name': 'banana', 'state': 'created'}}

        result = self._invoke(['show', 'banana'])

        self.assertEqual(0, result.exit_code, result.output)
        self.client.get_namespace_metadata.assert_called_once_with('clientns')

    def test_show_explicit_namespace_wins(self):
        md_key = primitives.METADATA_KEY % 'banana'
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
