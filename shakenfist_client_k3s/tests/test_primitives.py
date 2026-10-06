import contextlib
import io
import time

# The PyPI mock backport is used rather than unittest.mock because these
# tests use call_args.args, which the stdlib version only gained in
# Python 3.8, and the project supports Python >= 3.7.
import mock
import requests
import testtools
import yaml

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


def _fake_response(payload, status_code=200, text=None):
    resp = mock.MagicMock()
    resp.status_code = status_code
    if isinstance(payload, Exception):
        resp.json.side_effect = payload
    else:
        resp.json.return_value = payload
    # A real string, not the MagicMock attribute, whenever a test cares:
    # slicing a MagicMock yields another MagicMock, so a truncation
    # assertion against the default would pass without any truncation.
    if text is not None:
        resp.text = text
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

    def test_a_body_which_is_not_json_raises(self):
        # A 200 carrying HTML -- a captive portal, a proxy error page --
        # is a lookup failure, not a JSONDecodeError traceback.
        client = mock.MagicMock()
        body = '<html>' + 'z' * (primitives.RESPONSE_SNIPPET_BYTES * 3)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(
                            ValueError('Expecting value'), text=body)):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_k3s_release,
                client, NAMESPACE, _reporter(), force_cache_update=True,
                release_channel='stable')

        self.assertEqual('unreadable_response', e.reason)
        self.assertEqual('k3s', e.product)
        self.assertEqual(primitives.K3S_CHANNELS_URL, e.url)
        self.assertEqual(primitives.RESPONSE_SNIPPET_BYTES,
                         len(e.response_snippet))
        client.set_namespace_metadata_item.assert_not_called()

    def test_documents_of_the_wrong_shape_raise(self):
        # Neither a top level list nor a channel which is not a dict may
        # reach a subscript or a .get().
        client = mock.MagicMock()

        for payload in [['stable'], {'data': 'stable'},
                        {'data': ['name latest']}]:
            with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                            return_value=_fake_response(payload)):
                e = self.assertRaises(
                    exceptions.ReleaseLookupError, primitives.get_k3s_release,
                    client, NAMESPACE, _reporter(), force_cache_update=True,
                    release_channel='stable')
            self.assertEqual('no_usable_k3s_channels', e.reason, payload)

        client.set_namespace_metadata_item.assert_not_called()

    def test_the_quoted_response_body_is_bounded(self):
        """Whoever serves the error does not get to choose its length.

        update.k3s.io, or a proxy holding a certificate this client
        trusts, otherwise decides how many bytes -- and which bytes --
        reach the operator's terminal and an Ansible msg. The practical
        harms are an unbounded error line in a log and terminal escape
        sequences rendered by an emulator, not execution.
        """
        client = mock.MagicMock()
        body = 'x' * (primitives.RESPONSE_SNIPPET_BYTES * 3)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(
                            None, status_code=502, text=body)):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_k3s_release,
                client, NAMESPACE, _reporter(), force_cache_update=True,
                release_channel='stable')

        self.assertEqual(primitives.RESPONSE_SNIPPET_BYTES,
                         len(e.response_text))
        self.assertNotIn(body, str(e))


def _chart(version, **extra):
    chart = {'apiVersion': 'v1', 'name': 'longhorn', 'version': version,
             'appVersion': 'v%s' % version,
             'urls': ['https://example.com/longhorn-%s.tgz' % version]}
    chart.update(extra)
    return chart


# A cut down version of real data from https://charts.longhorn.io/index.yaml,
# plus the entries which must be skipped: a prerelease, a version PEP 440
# cannot parse, a deprecated chart, and a version YAML reads as a float.
LONGHORN_INDEX = {
    'apiVersion': 'v1',
    'entries': {
        'longhorn': [
            _chart('1.7.0-rc1'),
            _chart('1.6.0'),
            _chart('1.4.0-hotfix1'),
            _chart('1.9.0', deprecated=True),
            _chart(1.8),
            _chart('1.5.1'),
        ],
    },
    'generated': '2026-09-29T09:48:33Z',
}


def _index_response(index=LONGHORN_INDEX, **kwargs):
    return _fake_response(None, text=yaml.safe_dump(index), **kwargs)


class GetLonghornReleaseTestCase(testtools.TestCase):
    def test_the_newest_final_chart_version_is_chosen(self):
        client = mock.MagicMock()

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_index_response()) as mock_request:
            release = primitives.get_longhorn_release(
                client, NAMESPACE, _reporter(), force_cache_update=True)

        self.assertEqual('1.6.0', release)

        # One request, to the chart index, never the GitHub API (#97).
        mock_request.assert_called_once()
        self.assertEqual(('GET', primitives.LONGHORN_CHART_INDEX_URL),
                         mock_request.call_args.args)
        self.assertEqual(primitives.RELEASE_LOOKUP_TIMEOUT,
                         mock_request.call_args.kwargs['timeout'])

        cache = client.set_namespace_metadata_item.call_args.args[2]
        self.assertEqual('1.6.0', cache['latest'])
        self.assertEqual(['1.6.0', '1.5.1'], cache['releases'])

    def test_no_valid_releases_raises(self):
        client = mock.MagicMock()
        index = {'apiVersion': 'v1',
                 'entries': {'longhorn': [_chart('1.7.0-rc1')]}}

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_index_response(index)):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_longhorn_release,
                client, NAMESPACE, _reporter(), force_cache_update=True)

        self.assertEqual('no_parsable_longhorn_release', e.reason)
        self.assertEqual('Unable to determine the latest Longhorn release', str(e))
        client.set_namespace_metadata_item.assert_not_called()

    def test_documents_of_the_wrong_shape_raise(self):
        # Each of these parses as YAML, so none is unreadable: they are
        # indexes with nothing usable in them, and must neither raise a
        # TypeError nor be cached.
        client = mock.MagicMock()

        for index in ['an error page which is a YAML string',
                      ['entries'],
                      {'entries': ['longhorn']},
                      {'entries': {'longhorn': 'all of them'}},
                      {'entries': {'longhorn': ['1.6.0']}},
                      {'entries': {'not-longhorn': [_chart('1.6.0')]}}]:
            with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                            return_value=_index_response(index)):
                e = self.assertRaises(
                    exceptions.ReleaseLookupError,
                    primitives.get_longhorn_release,
                    client, NAMESPACE, _reporter(), force_cache_update=True)
            self.assertEqual('no_parsable_longhorn_release', e.reason, index)

        client.set_namespace_metadata_item.assert_not_called()

    def test_a_body_which_is_not_yaml_raises(self):
        client = mock.MagicMock()
        body = 'key: [unterminated\n' + 'w' * (primitives.RESPONSE_SNIPPET_BYTES * 3)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(None, text=body)):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_longhorn_release,
                client, NAMESPACE, _reporter(), force_cache_update=True)

        self.assertEqual('unreadable_response', e.reason)
        self.assertEqual('Longhorn', e.product)
        self.assertEqual(primitives.LONGHORN_CHART_INDEX_URL, e.url)
        self.assertEqual(primitives.RESPONSE_SNIPPET_BYTES,
                         len(e.response_snippet))

    def test_the_index_cannot_construct_objects(self):
        # safe_load, not load: a python/object tag is a parse failure.
        client = mock.MagicMock()
        body = '!!python/object/apply:os.system ["true"]\n'

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(None, text=body)):
            e = self.assertRaises(
                exceptions.ReleaseLookupError, primitives.get_longhorn_release,
                client, NAMESPACE, _reporter(), force_cache_update=True)

        self.assertEqual('unreadable_response', e.reason)

    def test_fresh_cache_avoids_fetch(self):
        # Includes a cache written by the old GitHub releases lookup, whose
        # 'releases' was a dict of tarball URLs: 'latest' is still a chart
        # version, so it stays good until it expires.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.LONGHORN_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'latest': '1.5.1',
                'releases': {'1.5.1': 'https://example.com/tarball/v1.5.1'}
            }
        }

        with mock.patch('shakenfist_client_k3s.primitives.requests.request') as mock_request:
            release = primitives.get_longhorn_release(
                client, NAMESPACE, _reporter())

        self.assertEqual('1.5.1', release)
        mock_request.assert_not_called()

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
                        return_value=_index_response()) as mock_request:
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
        self.assertIn('GET https://charts.longhorn.io/index.yaml', str(e))

    def test_the_quoted_response_body_is_bounded(self):
        client = mock.MagicMock()
        body = 'y' * (primitives.RESPONSE_SNIPPET_BYTES * 3)

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(
                            None, status_code=502, text=body)):
            e = self.assertRaises(
                exceptions.ReleaseLookupError,
                primitives.get_longhorn_release,
                client, NAMESPACE, _reporter(), force_cache_update=True)

        self.assertEqual(primitives.RESPONSE_SNIPPET_BYTES,
                         len(e.response_text))
        self.assertNotIn(body, str(e))


class FetchFailureTestCase(testtools.TestCase):
    """A fetch which fails outright is a ReleaseLookupError, not a traceback.

    Both lookups share the fetch, so both are driven here: a timeout is
    the case RELEASE_LOOKUP_TIMEOUT exists to produce, and a connection
    failure is the commoner one.
    """

    def _lookups(self, client):
        return [
            ('k3s', lambda: primitives.get_k3s_release(
                client, NAMESPACE, _reporter(), force_cache_update=True,
                release_channel='stable')),
            ('Longhorn', lambda: primitives.get_longhorn_release(
                client, NAMESPACE, _reporter(), force_cache_update=True)),
        ]

    def test_a_failed_request_raises(self):
        client = mock.MagicMock()

        for failure in [requests.Timeout('Read timed out. (read timeout=30)'),
                        requests.ConnectionError('Name or service not known')]:
            for product, lookup in self._lookups(client):
                with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                                side_effect=failure) as mock_request:
                    e = self.assertRaises(exceptions.ReleaseLookupError, lookup)

                self.assertEqual('request_failed', e.reason)
                self.assertEqual(product, e.product)
                self.assertEqual(str(failure), e.error)
                self.assertIn('Unable to determine latest %s release version'
                              % product, str(e))
                self.assertEqual(primitives.RELEASE_LOOKUP_TIMEOUT,
                                 mock_request.call_args.kwargs['timeout'])

        client.set_namespace_metadata_item.assert_not_called()


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
