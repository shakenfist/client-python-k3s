import contextlib
import io
import time

# The PyPI mock backport is used rather than unittest.mock because these
# tests use call_args.args, which the stdlib version only gained in
# Python 3.8, and the project supports Python >= 3.7.
import mock
import testtools

from shakenfist_client_k3s import primitives


class FakeContext:
    """A minimal stand-in for a click context, which primitives only uses as a holder for the obj dict."""

    def __init__(self, obj):
        self.obj = obj


def _make_context(client):
    return FakeContext({
        'namespace': 'testns',
        'CLIENT': client,
        'VERBOSE': False
    })


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
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)):
            release = primitives.get_k3s_release(
                ctx, force_cache_update=True, release_channel='stable')

        self.assertEqual('v1.33.4+k3s1', release)

        # The cache we write back should contain the resolvable channels and
        # silently omit the ones without a latest release.
        client.set_namespace_metadata_item.assert_called_once_with(
            'testns', primitives.K3S_VERSION_CACHE_KEY, mock.ANY)
        cache = client.set_namespace_metadata_item.call_args.args[2]
        self.assertEqual(
            {'stable': 'v1.33.4+k3s1', 'latest': 'v1.34.1+k3s1', 'v1.33': 'v1.33.4+k3s1'},
            cache['releases'])

    def test_unresolvable_channel_exits(self):
        client = mock.MagicMock()
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)):
            self.assertRaises(
                SystemExit, primitives.get_k3s_release,
                ctx, force_cache_update=True, release_channel='v1.16-testing')

    def test_unknown_channel_exits(self):
        client = mock.MagicMock()
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)):
            self.assertRaises(
                SystemExit, primitives.get_k3s_release,
                ctx, force_cache_update=True, release_channel='banana')

    def test_fresh_cache_avoids_fetch(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'releases': {'stable': 'v1.30.0+k3s1'}
            }
        }
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request') as mock_request:
            release = primitives.get_k3s_release(ctx, release_channel='stable')

        self.assertEqual('v1.30.0+k3s1', release)
        mock_request.assert_not_called()

    def test_invalid_cache_is_clobbered(self):
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: 'this is not a dict'
        }
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)) as mock_request:
            release = primitives.get_k3s_release(ctx, release_channel='stable')

        self.assertEqual('v1.33.4+k3s1', release)
        mock_request.assert_called_once()

    def test_response_missing_data_exits(self):
        # An error envelope or schema change with no 'data' key must take
        # the tidy 'Release channel not found' exit, not raise KeyError.
        client = mock.MagicMock()
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response({'error': 'nope'})):
            self.assertRaises(
                SystemExit, primitives.get_k3s_release,
                ctx, force_cache_update=True, release_channel='stable')

    def test_cache_missing_releases_is_clobbered(self):
        # A cache dict with a fresh timestamp but no 'releases' key must be
        # treated as invalid, not returned as-is.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {'updated': time.time()}
        }
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)) as mock_request:
            release = primitives.get_k3s_release(ctx, release_channel='stable')

        self.assertEqual('v1.33.4+k3s1', release)
        mock_request.assert_called_once()

    def test_http_error_exits(self):
        client = mock.MagicMock()
        ctx = _make_context(client)

        stdout = io.StringIO()
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(None, status_code=500)):
            with contextlib.redirect_stdout(stdout):
                self.assertRaises(
                    SystemExit, primitives.get_k3s_release,
                    ctx, force_cache_update=True, release_channel='stable')

        # The error must name the URL we actually fetched, not a literal
        # '{url}' from a missing f-string prefix.
        self.assertIn('GET https://update.k3s.io/v1-release/channels',
                      stdout.getvalue())


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
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(LONGHORN_RELEASES)):
            release = primitives.get_longhorn_release(ctx, force_cache_update=True)

        self.assertEqual('1.6.0', release)

    def test_no_valid_releases_exits(self):
        client = mock.MagicMock()
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response([])):
            self.assertRaises(
                SystemExit, primitives.get_longhorn_release,
                ctx, force_cache_update=True)

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
        ctx = _make_context(client)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(LONGHORN_RELEASES)) as mock_request:
            release = primitives.get_longhorn_release(ctx)

        self.assertEqual('1.6.0', release)
        mock_request.assert_called()

    def test_http_error_exits(self):
        client = mock.MagicMock()
        ctx = _make_context(client)

        stdout = io.StringIO()
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(None, status_code=500)):
            with contextlib.redirect_stdout(stdout):
                self.assertRaises(
                    SystemExit, primitives.get_longhorn_release,
                    ctx, force_cache_update=True)

        # The error must blame Longhorn, not k3s, and name the fetched URL.
        self.assertIn('Unable to determine latest Longhorn release version',
                      stdout.getvalue())
        self.assertIn('GET https://api.github.com/repos/longhorn/longhorn/releases',
                      stdout.getvalue())
