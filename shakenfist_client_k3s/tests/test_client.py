"""make_client() builds the client a library caller needs.

The CLI does not use this factory -- it is handed a client by sf-client --
so nothing else in the suite covers it. The rule it has to get right is
all three of api_url, namespace and key used verbatim, none of them
meaning discovery, and anything in between a ValueError: falling back to
discovery for a partial set would hand back a client pointed at whatever
cloud the environment names, with nothing said about it. That departs
deliberately from the Shaken Fist Ansible collection's _make_client(),
which falls back; client.py's docstring says why.
"""

# The PyPI mock backport is used rather than unittest.mock for consistency
# with the other tests here, as the project supports Python >= 3.7.
import mock
from shakenfist_client import apiclient
import testtools

from shakenfist_client_k3s import client as client_module


SUPPLIED = ('base_url', 'namespace', 'key', 'suppress_configuration_lookup')


class MakeClientTestCase(testtools.TestCase):
    def setUp(self):
        super(MakeClientTestCase, self).setUp()
        patcher = mock.patch('shakenfist_client_k3s.client.apiclient.Client')
        self.mock_client = patcher.start()
        self.addCleanup(patcher.stop)

    def _kwargs(self, **call):
        result = client_module.make_client(**call)
        self.assertIs(self.mock_client.return_value, result)
        return self.mock_client.call_args[1]

    def test_all_three_are_used_verbatim(self):
        kwargs = self._kwargs(
            api_url='https://api.example.com', namespace='ns', key='k')

        self.assertEqual('https://api.example.com', kwargs['base_url'])
        self.assertEqual('ns', kwargs['namespace'])
        self.assertEqual('k', kwargs['key'])
        self.assertTrue(kwargs['suppress_configuration_lookup'])

    def test_nothing_supplied_auto_discovers(self):
        kwargs = self._kwargs()

        for name in SUPPLIED:
            self.assertNotIn(name, kwargs)

    def test_a_partial_set_is_an_error(self):
        # Each of the three omitted in turn. Falling back to discovery here
        # would hand back a client pointed at whatever cloud the
        # environment names, with nothing said about it. The last case
        # records that an empty string counts as absent, not as supplied.
        for call in [{'namespace': 'ns', 'key': 'k'},
                     {'api_url': 'https://api.example.com', 'key': 'k'},
                     {'api_url': 'https://api.example.com', 'namespace': 'ns'},
                     {'api_url': 'https://api.example.com', 'namespace': 'ns',
                      'key': ''}]:
            self.mock_client.reset_mock()

            error = self.assertRaises(
                ValueError, client_module.make_client, **call)

            # The message names exactly what was passed, so the caller can
            # see which of the three it forgot. The check is against the
            # variable clause only: the fixed prefix names all three.
            detail = str(error).split('Got only ', 1)[1]
            for name in ('api_url', 'namespace', 'key'):
                if call.get(name):
                    self.assertIn(name, detail, call)
                else:
                    self.assertNotIn(name, detail, call)
            self.mock_client.assert_not_called()

    def test_the_strategy_is_continue_either_way(self):
        for call in [{}, {'api_url': 'https://api.example.com',
                          'namespace': 'ns', 'key': 'k'}]:
            self.mock_client.reset_mock()
            kwargs = self._kwargs(**call)

            self.assertEqual(
                apiclient.ASYNC_CONTINUE, kwargs['async_strategy'], call)
            self.assertFalse(kwargs['verbose'], call)

    def test_sync_request_timeout_is_left_alone(self):
        # The collection widens this to 1800 because it blocks. With
        # ASYNC_CONTINUE no single call waits for orchestration, so the
        # default stands and setting it would be cargo culting.
        self.assertNotIn('sync_request_timeout', self._kwargs())

    def test_unconfigured_propagates(self):
        error = apiclient.UnconfiguredException('no configuration found')
        self.mock_client.side_effect = error

        raised = self.assertRaises(
            apiclient.UnconfiguredException, client_module.make_client)

        self.assertIs(error, raised)
