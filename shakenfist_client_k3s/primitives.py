"""Namespace scoped lookups and stateless helpers.

What is left here is deliberately not cluster scoped. The two release
lookups cache their results in *namespace* metadata rather than in any
one cluster's metadata, and the commands which use them
(``query-k3s-version`` and ``query-longhorn-version``) have no cluster at
all, so they take a client, a namespace and a reporter rather than a
Cluster. Everything which reads or writes cluster metadata, or drives a
cluster's nodes, is a method on ``cluster.Cluster``.

This module must never import ``cluster``: the dependency runs the other
way, so that a Cluster can call these lookups.
"""

import json
from packaging.version import InvalidVersion, Version
import requests
from shakenfist_client import apiclient
import time
import yaml

from shakenfist_client_k3s import exceptions


# The namespace metadata key the list of managed clusters is stored under.
# It lives here rather than in the package __init__ because it is namespace
# scoped state, alongside the two version caches below, and because both
# cluster.py and the CLI need to reach it without importing each other.
CLUSTER_LIST = 'orchestrated_k3s_clusters'

K3S_VERSION_CACHE_KEY = 'orchestrated_k3s_cluster_k3s_version_cache'
LONGHORN_VERSION_CACHE_KEY = 'orchestrated_k3s_cluster_longhorn_version_cache'

# How much of a third-party HTTP response body is quoted back in an error
# message. Enough to recognise a proxy error page or a rate limit notice,
# and bounded because the party choosing those bytes is not this one: an
# unbounded body is an unbounded error message in somebody's log, and a
# vehicle for terminal escape sequences in somebody's terminal.
RESPONSE_SNIPPET_BYTES = 512

K3S_CHANNELS_URL = 'https://update.k3s.io/v1-release/channels'

# The index of the Helm repository the Longhorn install itself uses (see
# Cluster.setup_longhorn()), so the version found here is by construction
# one the install can fetch. It is a static file with no API rate limit.
# This lookup used to page through the GitHub releases API instead, which
# allows 60 anonymous requests an hour per source address: behind a shared
# NAT that failed creates at the Longhorn phase, after every instance had
# been built (#97).
LONGHORN_CHART_INDEX_URL = 'https://charts.longhorn.io/index.yaml'

# How long a release lookup waits on its upstream, in seconds, to connect
# and then between bytes of the response. Without it an upstream which
# accepts the connection and never answers hangs the command, silently.
RELEASE_LOOKUP_TIMEOUT = 30


def list_clusters(client, namespace):
    """Return the names of the managed k3s clusters in a namespace.

    Namespace scoped rather than cluster scoped: there is no cluster to
    build here, only the list in namespace metadata which create appends to
    and delete removes from. This takes no reporter because it emits
    nothing -- the caller decides what to do with the list.
    """
    namespace_md = client.get_namespace_metadata(namespace)
    return namespace_md.get(CLUSTER_LIST, [])


def _fetch_release_data(product, url, accept, reporter):
    """GET one release lookup's upstream document.

    Every way the fetch can fail becomes a ReleaseLookupError naming the
    product and the URL, rather than a requests traceback. Parsing the
    body is left to the caller, which knows what format to expect.
    """
    reporter.debug(f'Fetching {url}')
    try:
        r = requests.request(
            'GET', url,
            headers={
                'Accept': accept,
                'User-Agent': apiclient.get_user_agent()
            },
            timeout=RELEASE_LOOKUP_TIMEOUT)
    except requests.RequestException as e:
        raise exceptions.ReleaseLookupError.request_failed(product, url, str(e))

    if r.status_code not in [200, 201, 204]:
        # Truncated: whoever controls this response -- the upstream, or a
        # proxy holding a certificate the client trusts -- otherwise
        # decides how many bytes reach the user's terminal and an Ansible
        # msg.
        raise exceptions.ReleaseLookupError.http_status(
            product, url, r.status_code, r.text[:RESPONSE_SNIPPET_BYTES])
    return r


def get_k3s_release(client, namespace, reporter, force_cache_update=False,
                    release_channel=None):
    if force_cache_update:
        version_cache = {'updated': 0}
        reporter.debug('Forcing cache update')
    else:
        namespace_md = client.get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            K3S_VERSION_CACHE_KEY, {'updated': 0, 'releases': {}})
        if not isinstance(version_cache, dict) or 'releases' not in version_cache:
            reporter.debug('Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    reporter.debug(f'Cached version information from {updated}: '
                   f'{version_cache.get("releases", {})}')

    if time.time() - updated > 24 * 3600:
        reporter.debug('Updating release version cache')

        url = K3S_CHANNELS_URL
        r = _fetch_release_data('k3s', url, 'application/json', reporter)
        try:
            d = r.json()
        except ValueError:
            # A 200 carrying HTML -- a captive portal, a proxy error page.
            raise exceptions.ReleaseLookupError.unreadable_response(
                'k3s', url, r.text[:RESPONSE_SNIPPET_BYTES])

        releases = {}
        reporter.debug('Fetched release data:')
        reporter.debug(json.dumps(d, indent=4, sort_keys=True))
        # A document of the wrong shape yields no channels, and so the
        # no_usable_k3s_channels error below rather than a traceback.
        channels = d.get('data', []) if isinstance(d, dict) else []
        for reldata in channels if isinstance(channels, list) else []:
            if not isinstance(reldata, dict):
                continue
            # Some channels (for example v1.16-testing) have no released
            # version and therefore no 'latest' key.
            if 'name' not in reldata or 'latest' not in reldata:
                reporter.debug(f'Channel {reldata.get("name")} has no latest release, '
                               'skipping')
                continue
            releases[reldata['name']] = reldata['latest']

        # Don't persist an empty parse result: a transient upstream error
        # would otherwise poison the shared namespace cache until it next
        # expires. This mirrors the 'latest is None' guard in
        # get_longhorn_release().
        if not releases:
            raise exceptions.ReleaseLookupError.no_usable_k3s_channels(
                url, json.dumps(d)[:RESPONSE_SNIPPET_BYTES])

        version_cache['releases'] = releases
        version_cache['updated'] = time.time()
        client.set_namespace_metadata_item(
            namespace, K3S_VERSION_CACHE_KEY, version_cache)

    most_recent = version_cache['releases'].get(release_channel, None)
    if not most_recent:
        raise exceptions.ReleaseLookupError.unknown_channel(release_channel)

    reporter.debug(f'Selected kubernetes version: {most_recent}')
    return most_recent


def get_longhorn_release(client, namespace, reporter, force_cache_update=False):
    if force_cache_update:
        version_cache = {'updated': 0}
        reporter.debug('Forcing cache update')
    else:
        namespace_md = client.get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            LONGHORN_VERSION_CACHE_KEY, {'updated': 0, 'releases': {}})
        if not isinstance(version_cache, dict) or 'latest' not in version_cache:
            reporter.debug('Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    reporter.debug(f'Cached version information from {updated}: '
                   f'{version_cache.get("releases", {})}')

    if time.time() - updated > 24 * 3600:
        reporter.debug('Updating release version cache')

        r = _fetch_release_data(
            'Longhorn', LONGHORN_CHART_INDEX_URL, 'application/yaml, */*',
            reporter)
        try:
            # safe_load, because this is third-party data. It is no less
            # trusted than the chart the install then runs from the same
            # host, but it should not be able to construct objects here.
            index = yaml.safe_load(r.text)
        except yaml.YAMLError:
            raise exceptions.ReleaseLookupError.unreadable_response(
                'Longhorn', LONGHORN_CHART_INDEX_URL,
                r.text[:RESPONSE_SNIPPET_BYTES])

        # A Helm repository index maps each chart name under 'entries' to
        # a list of that chart's versions. A document of any other shape
        # -- an error page which happens to parse as YAML, a reshaped
        # index -- yields no releases, and so the no_parsable_longhorn_release
        # error below rather than a traceback.
        entries = index.get('entries') if isinstance(index, dict) else None
        charts = entries.get('longhorn') if isinstance(entries, dict) else None

        releases = []
        latest = None
        for chart in charts if isinstance(charts, list) else []:
            if not isinstance(chart, dict) or chart.get('deprecated'):
                continue

            # The chart version, not appVersion, because it is what helm
            # install --version takes. Longhorn has kept the two equal so
            # far, apart from appVersion's leading 'v', which is also why a
            # cache written by the old GitHub lookup is still a valid
            # answer until it expires. A version YAML
            # read as something other than a string (1.10 is the float
            # 1.1) is not one to trust, so it is skipped.
            chart_version = chart.get('version')
            if not isinstance(chart_version, str):
                continue

            # Helm chart versions are SemVer, which PEP 440 mostly
            # accepts: 1.7.0-rc1 parses as a prerelease and is skipped as
            # one. Longhorn has published versions which are not valid at
            # all (v1.4.0-hotfix1 was a release tag), so skip those too.
            try:
                parsed_version = Version(chart_version)
            except InvalidVersion:
                reporter.debug(f'Skipping unparsable chart version {chart_version}')
                continue
            if parsed_version.is_prerelease:
                continue

            releases.append(chart_version)
            if latest is None or parsed_version > latest:
                latest = parsed_version
                latest_version = chart_version

        if latest is None:
            raise exceptions.ReleaseLookupError.no_parsable_longhorn_release()

        version_cache['releases'] = releases
        version_cache['latest'] = latest_version
        version_cache['updated'] = time.time()
        client.set_namespace_metadata_item(
            namespace, LONGHORN_VERSION_CACHE_KEY, version_cache)

    return version_cache['latest']
