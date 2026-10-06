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
import re
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


# One Kubernetes version as Helm writes them in a chart's kubeVersion,
# and as k3s names its releases: an optional 'v', one to three numeric
# components, then an optional SemVer pre-release and build. Only the
# numbers are kept. Components are ASCII digits and bounded in length, for
# the reasons check_k3s_release() in cluster.py gives.
_KUBE_VERSION_RE = re.compile(
    r'v?([0-9]{1,9})(?:\.([0-9]{1,9}))?(?:\.([0-9]{1,9}))?'
    r'(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?\Z')

_KUBE_CONSTRAINT_RE = re.compile(r'(>=|<=|!=|==|=|>|<)?(.+)\Z')

_KUBE_COMPARISONS = {
    '>=': lambda a, b: a >= b,
    '<=': lambda a, b: a <= b,
    '>': lambda a, b: a > b,
    '<': lambda a, b: a < b,
    '!=': lambda a, b: a != b,
    '==': lambda a, b: a == b,
    '=': lambda a, b: a == b,
    None: lambda a, b: a == b,
}


def parse_kube_version(text):
    """Return a Kubernetes or k3s version as a (major, minor, patch) tuple.

    Missing components are zero, so '1.25' is (1, 25, 0). A pre-release
    or build suffix -- the '+k3s1' on every k3s release, the '-0' Helm
    charts put on a constraint's bound -- is accepted and ignored.
    Returns None for anything else.
    """
    if not isinstance(text, str):
        return None
    match = _KUBE_VERSION_RE.match(text)
    if not match:
        return None
    return tuple(int(part or 0) for part in match.groups())


def kube_version_satisfies(constraint, version):
    """Whether a (major, minor, patch) tuple satisfies a chart's kubeVersion.

    Returns True or False, or None when the constraint is not one this can
    read. That is a subset of Helm's syntax: '||' between alternatives,
    and within one alternative comparisons (>=, <=, >, <, =, !=, or none
    for equality) separated by commas or spaces, with optional space after
    the operator. That covers every kubeVersion Longhorn's chart index has
    used: '>=1.25.0-0', '>=1.18.0-0 <1.25.0-0', '>=v1.18.0' and
    '>= v1.16.0-0, < v1.22.0-0'. Tilde and caret ranges, wildcards and
    hyphen ranges are not read, and give None rather than a guess.

    Pre-releases are compared as their release, on both sides. Helm's
    '-0' on a bound exists to let pre-releases of that version through,
    and k3s channels resolve to releases, so for every version this is
    asked about the answer is the one Helm gives.
    """
    if not isinstance(constraint, str) or not constraint.strip():
        return None

    # Parsed whole before any of it is evaluated, so that a constraint
    # with an unreadable part is unreadable whether or not an earlier
    # alternative would already have matched.
    alternatives = []
    for alternative in constraint.split('||'):
        alternative = re.sub(r'(>=|<=|!=|==|=|>|<)\s+', r'\1', alternative)
        tokens = [t for t in re.split(r'[\s,]+', alternative) if t]
        if not tokens:
            return None

        comparisons = []
        for token in tokens:
            match = _KUBE_CONSTRAINT_RE.match(token)
            bound = parse_kube_version(match.group(2)) if match else None
            if bound is None:
                return None
            comparisons.append((_KUBE_COMPARISONS[match.group(1)], bound))
        alternatives.append(comparisons)

    return any(all(compare(version, bound) for compare, bound in comparisons)
               for comparisons in alternatives)


def _select_longhorn_chart(charts, k3s_version, reporter):
    """Return the newest chart version in charts which k3s_version can run.

    charts maps a chart version to its kubeVersion constraint, as the
    cache records it: '' for a chart which states none, which Helm installs
    anywhere, and None for one whose constraint was not a string. The map
    is re-validated here rather than trusted, because it may have come
    back from namespace metadata, which anybody holding the namespace's
    credentials can write.

    With k3s_version None, compatibility is not considered and the newest
    chart is returned.
    """
    candidates = []
    for chart_version in charts:
        try:
            parsed = Version(chart_version)
        except (InvalidVersion, TypeError):
            continue
        if not parsed.is_prerelease:
            candidates.append((parsed, chart_version))
    if not candidates:
        raise exceptions.ReleaseLookupError.no_parsable_longhorn_release()
    candidates.sort(reverse=True)

    if k3s_version is None:
        return candidates[0][1]

    kube = parse_kube_version(k3s_version)
    if kube is not None:
        for _, chart_version in candidates:
            constraint = charts[chart_version]
            if constraint == '':
                return chart_version
            fits = kube_version_satisfies(constraint, kube)
            if fits:
                return chart_version
            if fits is None:
                reporter.debug(f'Skipping Longhorn {chart_version}: cannot read '
                               f'its kubeVersion {constraint!r}')
            else:
                reporter.debug(f'Skipping Longhorn {chart_version}: it needs '
                               f'Kubernetes {constraint}, not {k3s_version}')
    raise exceptions.ReleaseLookupError.no_compatible_longhorn_release(
        k3s_version)


def get_longhorn_release(client, namespace, reporter, force_cache_update=False,
                         k3s_version=None):
    """Return the newest Longhorn chart version a cluster can install.

    k3s_version is the k3s release the cluster runs or will run, such as
    'v1.36.5+k3s1'. Each chart in the index states the Kubernetes versions
    it supports, and a chart outside them is one helm install refuses, so
    the newest chart whose kubeVersion admits this release is returned.
    Left as None, compatibility is not considered and the newest chart is
    returned; create() always passes one (#118).
    """
    if force_cache_update:
        version_cache = {'updated': 0}
        reporter.debug('Forcing cache update')
    else:
        namespace_md = client.get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            LONGHORN_VERSION_CACHE_KEY, {'updated': 0})
        # A cache without 'charts' -- one written before the lookup
        # recorded each chart's kubeVersion, or anything else -- cannot
        # answer a compatibility question, so it is refetched.
        if (not isinstance(version_cache, dict)
                or not isinstance(version_cache.get('charts'), dict)):
            reporter.debug('Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    reporter.debug(f'Cached version information from {updated}: '
                   f'{version_cache.get("charts", {})}')

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
        listed = entries.get('longhorn') if isinstance(entries, dict) else None

        charts = {}
        for chart in listed if isinstance(listed, list) else []:
            if not isinstance(chart, dict) or chart.get('deprecated'):
                continue

            # The chart version, not appVersion, because it is what helm
            # install --version takes. Longhorn has kept the two equal so
            # far, apart from appVersion's leading 'v'. A version YAML
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

            # '' for a chart which states no kubeVersion, which Helm will
            # install on anything; None for one which states something
            # other than a string, which is not a constraint to guess at.
            kube_version = chart.get('kubeVersion', '')
            charts[chart_version] = (
                kube_version if isinstance(kube_version, str) else None)

        # Don't persist an empty parse result, for the reason
        # get_k3s_release() gives.
        if not charts:
            raise exceptions.ReleaseLookupError.no_parsable_longhorn_release()

        version_cache = {'charts': charts, 'updated': time.time()}
        client.set_namespace_metadata_item(
            namespace, LONGHORN_VERSION_CACHE_KEY, version_cache)

    selected = _select_longhorn_chart(
        version_cache['charts'], k3s_version, reporter)
    reporter.debug(f'Selected Longhorn version: {selected}')
    return selected
