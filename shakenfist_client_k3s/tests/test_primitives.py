import contextlib
import io
import time

# The PyPI mock backport is used rather than unittest.mock because these
# tests use call_args.args, which the stdlib version only gained in
# Python 3.8, and the project supports Python >= 3.7.
import mock
import testtools

from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress


# The release lookups are namespace scoped rather than cluster scoped:
# their caches live in namespace metadata and the commands which use them
# name no cluster, so they take a client, a namespace and a reporter
# rather than a Cluster.
NAMESPACE = 'testns'


def _reporter():
    return progress.Reporter()


def _fake_response(payload, status_code=200):
    resp = mock.MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload
    return resp


# A cut down version of real data from https://update.k3s.io/v1-release/channels.
# Note that some channels (v1.16-testing here) have no 'latest' key, just a
# 'latestRegexp', because no matching release is retained upstream.
K3S_CHANNELS = {
    'data': [
        {'name': 'stable', 'latest': 'v1.33.4+k3s1'},
        {'name': 'latest', 'latest': 'v1.34.1+k3s1'},
        {'name': 'v1.16-testing', 'latestRegexp': 'v1\\.16\\..*'},
        {'name': 'v1.33', 'latest': 'v1.33.4+k3s1'},
        # Synthetic: an entry with no 'name' must be skipped, not stored
        # under a None key.
        {'latest': 'v1.99.0+k3s1'},
    ]
}


class GetK3sReleaseTestCase(testtools.TestCase):
    def test_channels_without_latest_are_skipped(self):
        client = mock.MagicMock()

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)):
            release = primitives.get_k3s_release(
                client, NAMESPACE, _reporter(), force_cache_update=True,
                release_channel='stable')

        self.assertEqual('v1.33.4+k3s1', release)

        # The cache we write back should contain the resolvable channels and
        # silently omit the ones without a latest release.
        client.set_namespace_metadata_item.assert_called_once_with(
            'testns', primitives.K3S_VERSION_CACHE_KEY, mock.ANY)
        cache = client.set_namespace_metadata_item.call_args.args[2]
        self.assertEqual(
            {'stable': 'v1.33.4+k3s1', 'latest': 'v1.34.1+k3s1', 'v1.33': 'v1.33.4+k3s1'},
            cache['releases'])

    def test_unresolvable_channel_raises(self):
        client = mock.MagicMock()

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_k3s_release,
                client, NAMESPACE, _reporter(), force_cache_update=True,
                release_channel='v1.16-testing')

        self.assertEqual('unknown_channel', e.reason)
        self.assertEqual('v1.16-testing', e.release_channel)
        self.assertEqual('Release channel v1.16-testing not found', str(e))

    def test_unknown_channel_raises(self):
        client = mock.MagicMock()

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_k3s_release,
                client, NAMESPACE, _reporter(), force_cache_update=True,
                release_channel='banana')

        self.assertEqual('unknown_channel', e.reason)
        self.assertEqual('Release channel banana not found', str(e))

    def test_fresh_cache_avoids_fetch(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'releases': {'stable': 'v1.30.0+k3s1'}
            }
        }

        with mock.patch('shakenfist_client_k3s.primitives.requests.request') as mock_request:
            release = primitives.get_k3s_release(
                client, NAMESPACE, _reporter(), release_channel='stable')

        self.assertEqual('v1.30.0+k3s1', release)
        mock_request.assert_not_called()

    def test_invalid_cache_is_clobbered(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: 'this is not a dict'
        }

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)) as mock_request:
            release = primitives.get_k3s_release(
                client, NAMESPACE, _reporter(), release_channel='stable')

        self.assertEqual('v1.33.4+k3s1', release)
        mock_request.assert_called_once()

    def test_response_missing_data_raises(self):
        # An error envelope or schema change with no 'data' key must fail
        # tidily rather than raise KeyError, and must not persist an empty
        # releases dict, which would poison the shared namespace cache
        # until it next expires.
        client = mock.MagicMock()

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response({'error': 'nope'})):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_k3s_release,
                client, NAMESPACE, _reporter(), force_cache_update=True,
                release_channel='stable')

        self.assertEqual('no_usable_k3s_channels', e.reason)
        self.assertIn('No usable k3s release channels found', str(e))
        client.set_namespace_metadata_item.assert_not_called()

    def test_cache_missing_releases_is_clobbered(self):
        # A cache dict with a fresh timestamp but no 'releases' key must be
        # treated as invalid, not returned as-is.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {'updated': time.time()}
        }

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)) as mock_request:
            release = primitives.get_k3s_release(
                client, NAMESPACE, _reporter(), release_channel='stable')

        self.assertEqual('v1.33.4+k3s1', release)
        mock_request.assert_called_once()

    def test_http_error_raises(self):
        client = mock.MagicMock()

        # The failure is carried by the exception now, so nothing at all
        # should reach stdout: the Click layer does the printing.
        stdout = io.StringIO()
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(None, status_code=500)):
            with contextlib.redirect_stdout(stdout):
                e = self.assertRaises(
                    exceptions.ReleaseLookupError, primitives.get_k3s_release,
                    client, NAMESPACE, _reporter(), force_cache_update=True,
                    release_channel='stable')

        self.assertEqual('', stdout.getvalue())
        self.assertEqual('http_status', e.reason)
        self.assertEqual('k3s', e.product)
        self.assertEqual(500, e.status_code)

        # The error must name the URL we actually fetched, not a literal
        # '{url}' from a missing f-string prefix.
        self.assertIn('GET https://update.k3s.io/v1-release/channels', str(e))


# A cut down version of real data from the GitHub releases API for
# longhorn/longhorn. Prereleases and tags which are not valid PEP 440
# versions should both be handled gracefully.
LONGHORN_RELEASES = [
    {'prerelease': False, 'tag_name': 'v1.5.1',
     'tarball_url': 'https://example.com/tarball/v1.5.1'},
    {'prerelease': True, 'tag_name': 'v1.7.0-rc1',
     'tarball_url': 'https://example.com/tarball/v1.7.0-rc1'},
    {'prerelease': False, 'tag_name': 'v1.4.0-hotfix1',
     'tarball_url': 'https://example.com/tarball/v1.4.0-hotfix1'},
    {'prerelease': False, 'tag_name': 'v1.6.0',
     'tarball_url': 'https://example.com/tarball/v1.6.0'},
]


class GetLonghornReleaseTestCase(testtools.TestCase):
    def test_prereleases_and_unparsable_tags_are_skipped(self):
        client = mock.MagicMock()

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(LONGHORN_RELEASES)):
            release = primitives.get_longhorn_release(
                client, NAMESPACE, _reporter(), force_cache_update=True)

        self.assertEqual('1.6.0', release)

    def test_no_valid_releases_raises(self):
        client = mock.MagicMock()

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response([])):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_longhorn_release,
                client, NAMESPACE, _reporter(), force_cache_update=True)

        self.assertEqual('no_parsable_longhorn_release', e.reason)
        self.assertEqual('Unable to determine the latest Longhorn release', str(e))

    def test_cache_missing_latest_is_refreshed(self):
        # Caches written before the 'latest' key existed have a fresh
        # timestamp and a 'releases' dict but no 'latest'. They must be
        # refreshed rather than raising KeyError.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.LONGHORN_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'releases': {'1.5.1': 'https://example.com/tarball/v1.5.1'}
            }
        }

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(LONGHORN_RELEASES)) as mock_request:
            release = primitives.get_longhorn_release(
                client, NAMESPACE, _reporter())

        self.assertEqual('1.6.0', release)
        mock_request.assert_called()

    def test_http_error_raises(self):
        client = mock.MagicMock()

        stdout = io.StringIO()
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(None, status_code=500)):
            with contextlib.redirect_stdout(stdout):
                e = self.assertRaises(
                    exceptions.ReleaseLookupError, primitives.get_longhorn_release,
                    client, NAMESPACE, _reporter(), force_cache_update=True)

        self.assertEqual('', stdout.getvalue())
        self.assertEqual('Longhorn', e.product)

        # The error must blame Longhorn, not k3s, and name the fetched URL.
        self.assertIn('Unable to determine latest Longhorn release version', str(e))
        self.assertIn('GET https://api.github.com/repos/longhorn/longhorn/releases',
                      str(e))


class DebugRoutingTestCase(testtools.TestCase):
    """Debug output goes through the reporter, not a bare print().

    This is the behaviour step 1d buys: a caller with a CollectingReporter
    gets debug lines back through the reporter instead of them landing on
    the process's own stdout, which is what happens when _emit_debug()
    calls print() directly rather than reporter.debug().
    """

    def test_verbose_debug_reaches_collector_not_stdout(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'releases': {'stable': 'v1.30.0+k3s1'}
            }
        }
        reporter = progress.CollectingReporter(verbose=True)

        stdout = io.StringIO()
        with mock.patch('shakenfist_client_k3s.primitives.requests.request') as mock_request:
            with contextlib.redirect_stdout(stdout):
                release = primitives.get_k3s_release(
                    client, NAMESPACE, reporter, release_channel='stable')

        self.assertEqual('v1.30.0+k3s1', release)
        mock_request.assert_not_called()

        # Nothing reached the real stdout...
        self.assertEqual('', stdout.getvalue())

        # ...it all went to the collector instead.
        self.assertIn('Cached version information from', reporter.getvalue())
        self.assertIn('Selected kubernetes version: v1.30.0+k3s1', reporter.getvalue())
