import contextlib
import copy
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
from shakenfist_client_k3s.tests import fakes


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

    def test_cache_whose_releases_is_not_a_dict_is_clobbered(self):
        # Namespace metadata is writable by any client with access to the
        # namespace. A releases value which is not a dict must be fetched
        # past, not read as one by the channel lookup or the stale copy.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.K3S_VERSION_CACHE_KEY: {
                'updated': time.time(), 'releases': ['stable']}
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


def _chart(version, kube_version='>=1.21.0-0', **extra):
    chart = {'apiVersion': 'v1', 'name': 'longhorn', 'version': version,
             'appVersion': 'v%s' % version, 'kubeVersion': kube_version,
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
        self.assertEqual({'1.6.0': '>=1.21.0-0', '1.5.1': '>=1.21.0-0'},
                         cache['charts'])

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
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.LONGHORN_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'charts': {'1.5.1': '>=1.21.0-0'}
            }
        }

        with mock.patch('shakenfist_client_k3s.primitives.requests.request') as mock_request:
            release = primitives.get_longhorn_release(
                client, NAMESPACE, _reporter(), k3s_version='v1.33.4+k3s1')

        self.assertEqual('1.5.1', release)
        mock_request.assert_not_called()

    def test_caches_which_cannot_answer_compatibility_are_refreshed(self):
        # The first two are the formats earlier lookups wrote: GitHub's
        # tarball URLs with no 'latest', then with one. Neither records a
        # chart's kubeVersion, so neither can answer for a k3s release.
        client = mock.MagicMock()

        for cache in [{'updated': time.time(),
                       'releases': {'1.5.1': 'https://example.com/tarball/v1.5.1'}},
                      {'updated': time.time(), 'latest': '1.5.1',
                       'releases': ['1.5.1']},
                      {'updated': time.time(), 'charts': ['1.5.1']},
                      'not a dict']:
            client.get_namespace_metadata.return_value = {
                primitives.LONGHORN_VERSION_CACHE_KEY: cache}
            with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                            return_value=_index_response()) as mock_request:
                release = primitives.get_longhorn_release(
                    client, NAMESPACE, _reporter())

            self.assertEqual('1.6.0', release, cache)
            mock_request.assert_called_once()

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


class LonghornCompatibilityTestCase(testtools.TestCase):
    """The Longhorn chart chosen is one the cluster's k3s can run (#118).

    The index below is the shape of the real one in October 2026: the
    newest chart narrowed its kubeVersion, so the newest chart is the
    wrong answer for any k3s older than 1.34.
    """

    INDEX = {
        'apiVersion': 'v1',
        'entries': {
            'longhorn': [
                _chart('1.13.0', '>=1.34.0-0'),
                _chart('1.12.1', '>=1.25.0-0'),
                _chart('1.4.0', '>=1.21.0-0'),
                _chart('1.3.3', '>=1.18.0-0 <1.25.0-0'),
                _chart('1.1.3', '>= v1.16.0-0, < v1.22.0-0'),
            ],
        },
    }

    def _lookup(self, k3s_version, index=None, reporter=None):
        client = mock.MagicMock()
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_index_response(index or self.INDEX)):
            return primitives.get_longhorn_release(
                client, NAMESPACE, reporter or _reporter(),
                force_cache_update=True, k3s_version=k3s_version)

    def test_the_newest_chart_the_release_admits_is_chosen(self):
        for k3s_version, expected in [
                ('v1.36.5+k3s1', '1.13.0'),
                ('v1.34.0+k3s1', '1.13.0'),
                ('v1.33.4+k3s1', '1.12.1'),
                ('v1.25.0+k3s1', '1.12.1'),
                ('v1.24.17+k3s1', '1.4.0'),
                ('v1.20.15+k3s1', '1.3.3'),
                ('v1.17.0+k3s1', '1.1.3'),
                ('v1.33.0-rc1+k3s1', '1.12.1')]:
            self.assertEqual(expected, self._lookup(k3s_version), k3s_version)

    def test_without_a_k3s_version_the_newest_chart_is_chosen(self):
        self.assertEqual('1.13.0', self._lookup(None))

    def test_no_compatible_chart_raises(self):
        e = self.assertRaises(exceptions.ReleaseLookupError,
                              self._lookup, 'v1.15.0+k3s1')
        self.assertEqual('no_compatible_longhorn_release', e.reason)
        self.assertEqual('v1.15.0+k3s1', e.k3s_version)

    def test_a_chart_with_no_kube_version_fits_anything(self):
        index = {'entries': {'longhorn': [
            _chart('1.2.0', '>=1.30.0'),
            {'version': '1.1.0'}]}}
        self.assertEqual('1.1.0', self._lookup('v1.15.0+k3s1', index))

    def test_a_constraint_which_cannot_be_read_is_skipped_not_guessed(self):
        # A newer chart with a range this does not parse loses to an older
        # one it can prove fits, and the skip is said in debug output.
        index = {'entries': {'longhorn': [
            _chart('1.9.0', '~1.30'),
            _chart('1.8.0', ['>=1.25']),
            _chart('1.7.0', '>=1.25.0-0')]}}
        reporter = progress.CollectingReporter(verbose=True)
        self.assertEqual('1.7.0', self._lookup('v1.30.2+k3s1', index, reporter))
        self.assertIn("Skipping Longhorn 1.9.0: cannot read its kubeVersion '~1.30'",
                      reporter.getvalue())

    def test_a_tampered_cache_is_revalidated(self):
        # The cache lives in namespace metadata, which anything holding the
        # namespace's credentials can write. Junk in it is skipped, not
        # trusted and not a traceback.
        client = mock.MagicMock()
        client.get_namespace_metadata.return_value = {
            primitives.LONGHORN_VERSION_CACHE_KEY: {
                'updated': time.time(),
                'charts': {'9.9.9-rc1': '', 'banana': '', '2.0.0': 7,
                           '1.5.1': '>=1.21.0-0'}
            }
        }

        with mock.patch('shakenfist_client_k3s.primitives.requests.request') as mock_request:
            release = primitives.get_longhorn_release(
                client, NAMESPACE, _reporter(), k3s_version='v1.33.4+k3s1')

        self.assertEqual('1.5.1', release)
        mock_request.assert_not_called()


class KubeVersionTestCase(testtools.TestCase):
    def test_parse_kube_version(self):
        for text, expected in [
                ('v1.36.5+k3s1', (1, 36, 5)),
                ('1.25.0-0', (1, 25, 0)),
                ('v1.16.0-r0', (1, 16, 0)),
                ('1.25', (1, 25, 0)),
                ('1', (1, 0, 0)),
                ('v1.18.2-rc3+k3s1', (1, 18, 2)),
                ('', None),
                ('1.2.3.4', None),
                ('1.2.3; touch /pwned', None),
                ('\u0661.2.3', None),
                ('1' * 10 + '.0.0', None),
                (None, None),
                (1.25, None)]:
            self.assertEqual(expected, primitives.parse_kube_version(text), text)

    def test_kube_version_satisfies(self):
        for constraint, version, expected in [
                ('>=1.34.0-0', (1, 34, 0), True),
                ('>=1.34.0-0', (1, 33, 9), False),
                ('>=1.18.0-0 <1.25.0-0', (1, 24, 17), True),
                ('>=1.18.0-0 <1.25.0-0', (1, 25, 0), False),
                ('>= v1.16.0-0, < v1.22.0-0', (1, 21, 0), True),
                ('>= v1.16.0-0, < v1.22.0-0', (1, 22, 0), False),
                ('>=v1.16.0-r0', (1, 16, 0), True),
                ('<=1.30', (1, 30, 0), True),
                ('>1.30', (1, 30, 0), False),
                ('1.30.1', (1, 30, 1), True),
                ('=1.30.1', (1, 30, 2), False),
                ('!=1.30.1', (1, 30, 2), True),
                ('<1.20 || >=1.30', (1, 31, 0), True),
                ('<1.20 || >=1.30', (1, 25, 0), False),
                ('~1.30', (1, 30, 0), None),
                ('^1.30', (1, 30, 0), None),
                ('1.30.x', (1, 30, 0), None),
                ('1.20 - 1.30', (1, 25, 0), None),
                ('>=1.20 ||', (1, 25, 0), None),
                ('', (1, 25, 0), None),
                (None, (1, 25, 0), None)]:
            self.assertEqual(
                expected,
                primitives.kube_version_satisfies(constraint, version),
                (constraint, version))


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


# When the expired caches below were written: three days before NOW, which
# is what time.time() answers while a StaleCopyTestCase test runs. Fixed
# rather than relative so that the warning's date and age can be asserted
# whole.
NOW = 1791676800.0                  # 2026-10-11 00:00 UTC
FETCHED = NOW - 3 * 24 * 3600 - 90  # 2026-10-07 23:58 UTC, 72h01m earlier

STALE_K3S = {'updated': FETCHED,
             'releases': {'stable': 'v1.32.1+k3s1', 'v1.20': 'v1.20.15+k3s1'}}
STALE_LONGHORN = {'updated': FETCHED,
                  'charts': {'1.5.1': '>=1.25.0-0', '1.5.0': '>=1.21.0-0'}}


class StaleCopyTestCase(testtools.TestCase):
    """An expired cache stands in for an upstream which cannot answer.

    update.k3s.io served an unverifiable certificate from 2026-10-10 (#132)
    and was unreachable for eight hours the next day. Every create in that
    window failed at the release lookup, with yesterday's answer sitting
    in the namespace metadata. These tests pin which failures may use it,
    that it is used as it is -- never written back, so the next lookup asks
    upstream again -- and that the cases which have always failed still do.

    The client is the shared fake rather than a MagicMock, because the
    claim "nothing was written" is about the stored metadata, and a fake
    which keeps it can be asserted on directly. Its get_namespace_metadata()
    shares the cache dict with what it stores, so a lookup which so much
    as touched the copy's 'updated' in place would show here too.
    """

    def setUp(self):
        super(StaleCopyTestCase, self).setUp()
        self.client = fakes.FakeClusterClient()
        self.client.metadata = {
            primitives.K3S_VERSION_CACHE_KEY: copy.deepcopy(STALE_K3S),
            primitives.LONGHORN_VERSION_CACHE_KEY: copy.deepcopy(STALE_LONGHORN),
        }
        self.reporter = progress.CollectingReporter()

        patcher = mock.patch('time.time', return_value=NOW)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _k3s(self, channel='stable', **kwargs):
        return primitives.get_k3s_release(
            self.client, NAMESPACE, self.reporter, release_channel=channel,
            **kwargs)

    def _longhorn(self, **kwargs):
        return primitives.get_longhorn_release(
            self.client, NAMESPACE, self.reporter, **kwargs)

    def _assert_untouched(self):
        self.assertEqual(
            {primitives.K3S_VERSION_CACHE_KEY: STALE_K3S,
             primitives.LONGHORN_VERSION_CACHE_KEY: STALE_LONGHORN},
            self.client.metadata)

    def _failures(self, product):
        """Each way of failing which means the upstream could not answer."""
        unparsable = ('<html>a captive portal</html>' if product == 'k3s'
                      else 'key: [unterminated')
        failures = [
            ('a timeout', {'side_effect': requests.Timeout('Read timed out.')}),
            ('a refused connection',
             {'side_effect': requests.ConnectionError('Connection refused')}),
            ('a certificate which does not verify (#132)',
             {'side_effect': requests.exceptions.SSLError(
                 'certificate verify failed: self-signed certificate')}),
            ('an unreadable 200',
             {'return_value': _fake_response(
                 ValueError('Expecting value'), text=unparsable)}),
            # The empty parses: an error page which happens to parse, which
            # for the Longhorn index is most HTML pages.
            ('a document with nothing usable in it',
             {'return_value': _fake_response(
                 {'error': 'nope'}, text='<html>a captive portal</html>')}),
        ]
        for status in [500, 502, 503, 408, 429]:
            failures.append(('HTTP %d' % status,
                             {'return_value': _fake_response(
                                 None, status_code=status, text='busy')}))
        return failures

    def test_an_unavailable_upstream_falls_back_to_the_expired_copy(self):
        for product, lookup, expected in [('k3s', self._k3s, 'v1.32.1+k3s1'),
                                          ('Longhorn', self._longhorn, '1.5.1')]:
            for what, behaviour in self._failures(product):
                self.reporter = progress.CollectingReporter()
                with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                                **behaviour) as mock_request:
                    release = lookup()

                self.assertEqual(expected, release, (product, what))
                mock_request.assert_called_once()
                self._assert_untouched()
                self.assertIn('The %s release cache could not be refreshed'
                              % product, self.reporter.getvalue(), (product, what))

    def test_the_warning_names_the_upstream_the_age_and_the_failure(self):
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        side_effect=requests.ConnectionError('Connection refused')):
            self._k3s()

        self.assertEqual(
            'The k3s release cache could not be refreshed from '
            'https://update.k3s.io/v1-release/channels, so the copy fetched '
            'at 2026-10-07 23:58 UTC (72h01m ago) is being used instead. The '
            'next lookup will try again. The refresh failed with:\n'
            'Unable to determine latest k3s release version\n'
            '    GET https://update.k3s.io/v1-release/channels\n'
            '    failed: Connection refused\n',
            self.reporter.getvalue())

    def test_the_longhorn_warning_names_its_own_upstream(self):
        # Its empty-parse error names no URL, so the warning must.
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(
                            None, text='an error page which is a YAML string')):
            self._longhorn()

        self.assertIn('The Longhorn release cache could not be refreshed from '
                      'https://charts.longhorn.io/index.yaml, so',
                      self.reporter.getvalue())
        self.assertIn('Unable to determine the latest Longhorn release',
                      self.reporter.getvalue())

    def test_the_next_lookup_asks_upstream_again(self):
        # The copy's 'updated' is left alone, so it is still expired: the
        # first lookup which reaches upstream refreshes and stores it.
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        side_effect=requests.ConnectionError('down')):
            self._k3s()
            self._longhorn()
        self._assert_untouched()

        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)):
            self.assertEqual('v1.33.4+k3s1', self._k3s())
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_index_response()):
            self.assertEqual('1.6.0', self._longhorn())

        self.assertEqual(
            NOW, self.client.metadata[primitives.K3S_VERSION_CACHE_KEY]['updated'])
        self.assertEqual(
            NOW, self.client.metadata[primitives.LONGHORN_VERSION_CACHE_KEY]['updated'])

    def test_verification_is_never_turned_off_to_get_an_answer(self):
        # The #132 case: the copy is used, and the one request made was a
        # verifying one. A fallback which retried without verification
        # would be reading exactly the data the failure says not to trust.
        for lookup in [self._k3s, self._longhorn]:
            with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                            side_effect=requests.exceptions.SSLError(
                                'certificate verify failed')) as mock_request:
                lookup()

            mock_request.assert_called_once()
            self.assertIs(True, mock_request.call_args.kwargs.get('verify', True))
        self._assert_untouched()

    def test_a_4xx_which_is_an_answer_does_not_fall_back(self):
        # A URL which has gone, or a proxy which refuses or wants
        # credentials, will not change by waiting. A copy would hide that
        # for as long as nobody read the warning.
        for status in [400, 401, 403, 404, 407, 410]:
            for product, lookup in [('k3s', self._k3s),
                                    ('Longhorn', self._longhorn)]:
                with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                                return_value=_fake_response(
                                    None, status_code=status, text='no')):
                    e = self.assertRaises(exceptions.ReleaseLookupError, lookup)

                self.assertEqual('http_status', e.reason)
                self.assertEqual(status, e.status_code)
                self.assertEqual(product, e.product)
        self.assertEqual('', self.reporter.getvalue())

    def test_a_forced_refresh_does_not_fall_back(self):
        # Somebody who asked for upstream's answer gets it or the error.
        for product, lookup in [('k3s', self._k3s), ('Longhorn', self._longhorn)]:
            with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                            side_effect=requests.ConnectionError('down')):
                e = self.assertRaises(exceptions.ReleaseLookupError, lookup,
                                      force_cache_update=True)

            self.assertEqual('request_failed', e.reason)
            self.assertEqual(product, e.product)
        self.assertEqual('', self.reporter.getvalue())
        self._assert_untouched()

    def test_with_no_copy_a_failure_raises_as_before(self):
        for cache in [None, 'not a dict', {'updated': FETCHED}]:
            self.client.metadata = {}
            if cache is not None:
                self.client.metadata = {
                    primitives.K3S_VERSION_CACHE_KEY: cache,
                    primitives.LONGHORN_VERSION_CACHE_KEY: cache}
            for product, lookup in [('k3s', self._k3s),
                                    ('Longhorn', self._longhorn)]:
                with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                                side_effect=requests.ConnectionError('down')):
                    e = self.assertRaises(exceptions.ReleaseLookupError, lookup)

                self.assertEqual('request_failed', e.reason, (product, cache))
                self.assertEqual(product, e.product)
        self.assertEqual('', self.reporter.getvalue())

    def test_a_copy_without_the_channel_raises_the_fetch_failure(self):
        # Not unknown_channel: the channel may well exist upstream, and
        # "no such channel" would send the operator to the wrong option.
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        side_effect=requests.ConnectionError('down')):
            e = self.assertRaises(exceptions.ReleaseLookupError, self._k3s,
                                  channel='v1.35')

        self.assertEqual('request_failed', e.reason)
        self.assertEqual('', self.reporter.getvalue())
        self._assert_untouched()

    def test_a_stale_longhorn_copy_still_chooses_for_the_release(self):
        # The copy is a chart map, not an answer: the chart is chosen from
        # it for the k3s release asked about, as a fresh one would be.
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        side_effect=requests.ConnectionError('down')):
            self.assertEqual('1.5.0', self._longhorn(k3s_version='v1.24.17+k3s1'))
            self.assertEqual('1.5.1', self._longhorn(k3s_version='v1.33.4+k3s1'))

        self._assert_untouched()

    def test_a_copy_with_no_chart_for_the_release_raises_the_fetch_failure(self):
        # Not no_compatible_longhorn_release, for the reason the k3s case
        # above gives: upstream may well list a chart this release can run.
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        side_effect=requests.ConnectionError('down')):
            e = self.assertRaises(exceptions.ReleaseLookupError, self._longhorn,
                                  k3s_version='v1.20.15+k3s1')

        self.assertEqual('request_failed', e.reason)
        self.assertEqual('Longhorn', e.product)
        self.assertEqual('', self.reporter.getvalue())
        self._assert_untouched()

    def test_an_unknown_channel_from_upstream_is_still_unknown(self):
        # The fallback is for upstream failing, not for upstream answering.
        with mock.patch('shakenfist_client_k3s.primitives.requests.request',
                        return_value=_fake_response(K3S_CHANNELS)):
            e = self.assertRaises(exceptions.ReleaseLookupError, self._k3s,
                                  channel='v1.20')

        self.assertEqual('unknown_channel', e.reason)


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
