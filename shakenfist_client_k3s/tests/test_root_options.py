"""The root sf-client options must reach the client k3s commands use.

This plugin used to build its own apiclient.Client, which discarded the
one sf-client's root callback had already built from --apiurl, --key and
--namespace. Nothing failed when it did: apiclient's own configuration
lookup reads SHAKENFIST_API_URL, SHAKENFIST_NAMESPACE and SHAKENFIST_KEY,
so an operator who configures by environment variable got the right
credentials by accident, and only one who passed the flags was silently
pointed at another cloud. That is invisible to every test which invokes
the k3s group directly, because the group never sees the root options at
all, so the two tests here are deliberately the other shape: one asserts
the construction is gone, and one drives the real root group end to end.

The second test reaches the k3s group through the shakenfist_client.plugin
entry point, which means it only works when this package is installed.
"tox -epy3" installs it and CI runs tox, so that is the supported way to
run these; a bare stestr run in a tree where the package is not installed
fails on a missing subcommand rather than on the behaviour being pinned.
"""

from click.testing import CliRunner

# The PyPI mock backport is used rather than unittest.mock for consistency
# with the other tests here, as the project supports Python >= 3.7.
import mock
from shakenfist_client import apiclient
from shakenfist_client import main
import testtools

import shakenfist_client_k3s


class NoClientConstructionTestCase(testtools.TestCase):
    """A k3s command must never construct an API client of its own.

    The six command tests which used to patch apiclient.Client now hand
    their fake in through the Click object instead, and a fake handed in
    that way does not by itself prove no real client was built. This does,
    and it is a stronger statement than those patches ever made: they
    would have quietly returned a fake to a second construction rather
    than objecting to it.
    """

    def test_client_is_never_constructed(self):
        client = mock.MagicMock()
        client.namespace = 'clientns'
        client.get_namespace_metadata.return_value = {}

        with mock.patch('shakenfist_client_k3s.apiclient.Client') as mock_client:
            mock_client.side_effect = AssertionError(
                'a k3s command constructed its own API client')
            result = CliRunner().invoke(
                shakenfist_client_k3s.k3s, ['list'],
                obj={'VERBOSE': False, 'CLIENT': client})

        # Asserted before the exit code so that a failure names the cause
        # rather than the symptom: the side effect above makes a second
        # construction fail the command, and "exit code 1" says nothing
        # about why.
        mock_client.assert_not_called()
        self.assertEqual(0, result.exit_code, result.output)


class RootOptionsTestCase(testtools.TestCase):
    """The client built from --apiurl, --key and --namespace is the one used.

    This drives shakenfist_client.main.cli, not the k3s group, because the
    root callback is where those options are parsed and where the client is
    built. Asserting on the identity of the object the command used is the
    point: a plugin which built its own client would satisfy every other
    assertion here while talking to a different cloud.
    """

    def test_root_options_reach_the_k3s_command(self):
        constructed = []

        def _client(*args, **kwargs):
            # A distinct mock per construction, because a single
            # return_value would hand the same object to a plugin which
            # built its own and every assertion below would pass anyway.
            client = mock.MagicMock()
            client.namespace = kwargs.get('namespace') or 'ns'
            client.get_namespace_metadata.return_value = {}
            constructed.append((kwargs, client))
            return client

        with mock.patch('shakenfist_client.main.apiclient.Client',
                        side_effect=_client):
            result = CliRunner().invoke(
                main.cli,
                ['--apiurl', 'https://api.example.com', '--key', 'k',
                 '--namespace', 'ns', 'k3s', 'list'])

        self.assertEqual(0, result.exit_code, result.output)

        # One client exists in the whole process. This is the assertion
        # the old code failed: it built a second one, from the
        # configuration lookup rather than from these options.
        self.assertEqual(
            1, len(constructed),
            'expected one client, got %d' % len(constructed))

        # The root built it from the options as passed. Index 1 is the
        # kwargs dict rather than call_args.kwargs, which needs 3.8.
        kwargs, client = constructed[0]
        self.assertEqual('https://api.example.com', kwargs['base_url'])
        self.assertEqual('k', kwargs['key'])
        self.assertEqual('ns', kwargs['namespace'])

        # And the k3s command used that client, not one of its own.
        client.get_namespace_metadata.assert_called_once_with('ns')

        # The strategy is forced on it, because the orchestration runs its
        # own wait loops; the root's default is "pause", which would sit
        # for up to a minute per call before the reporter ever ran.
        self.assertEqual(apiclient.ASYNC_CONTINUE, client.async_strategy)
