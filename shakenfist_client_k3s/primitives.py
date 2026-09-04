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

from shakenfist_client_k3s import exceptions


K3S_VERSION_CACHE_KEY = 'orchestrated_k3s_cluster_k3s_version_cache'
LONGHORN_VERSION_CACHE_KEY = 'orchestrated_k3s_cluster_longhorn_version_cache'


def _emit_debug(reporter, m):
    if reporter.verbose:
        print(m)


def get_k3s_release(client, namespace, reporter, force_cache_update=False,
                    release_channel=None):
    if force_cache_update:
        version_cache = {'updated': 0}
        _emit_debug(reporter, 'Forcing cache update')
    else:
        namespace_md = client.get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            K3S_VERSION_CACHE_KEY, {'updated': 0, 'releases': {}})
        if not isinstance(version_cache, dict) or 'releases' not in version_cache:
            _emit_debug(reporter, 'Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    _emit_debug(reporter, (f'Cached version information from {updated}: '
                           f'{version_cache.get("releases", {})}'))

    if time.time() - updated > 24 * 3600:
        _emit_debug(reporter, 'Updating release version cache')

        url = 'https://update.k3s.io/v1-release/channels'
        _emit_debug(reporter, f'Fetching {url}')
        r = requests.request(
            'GET', url,
            headers={
                'Accept': 'application/json',
                'User-Agent': apiclient.get_user_agent()
            })
        if r.status_code not in [200, 201, 204]:
            raise exceptions.ReleaseLookupError.http_status(
                'k3s', url, r.status_code, r.text)

        d = r.json()
        releases = {}
        _emit_debug(reporter, 'Fetched release data:')
        _emit_debug(reporter, json.dumps(d, indent=4, sort_keys=True))
        for reldata in d.get('data', []):
            # Some channels (for example v1.16-testing) have no released
            # version and therefore no 'latest' key.
            if 'name' not in reldata or 'latest' not in reldata:
                _emit_debug(reporter, (f'Channel {reldata.get("name")} has no latest release, '
                                       'skipping'))
                continue
            releases[reldata['name']] = reldata['latest']

        # Don't persist an empty parse result: a transient upstream error
        # would otherwise poison the shared namespace cache until it next
        # expires. This mirrors the 'latest is None' guard in
        # get_longhorn_release().
        if not releases:
            raise exceptions.ReleaseLookupError.no_usable_k3s_channels(
                url, json.dumps(d)[:512])

        version_cache['releases'] = releases
        version_cache['updated'] = time.time()
        client.set_namespace_metadata_item(
            namespace, K3S_VERSION_CACHE_KEY, version_cache)

    most_recent = version_cache['releases'].get(release_channel, None)
    if not most_recent:
        raise exceptions.ReleaseLookupError.unknown_channel(release_channel)

    _emit_debug(reporter, f'Selected kubernetes version: {most_recent}')
    return most_recent


def get_longhorn_release(client, namespace, reporter, force_cache_update=False):
    if force_cache_update:
        version_cache = {'updated': 0}
        _emit_debug(reporter, 'Forcing cache update')
    else:
        namespace_md = client.get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            LONGHORN_VERSION_CACHE_KEY, {'updated': 0, 'releases': {}})
        if not isinstance(version_cache, dict) or 'latest' not in version_cache:
            _emit_debug(reporter, 'Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    _emit_debug(reporter, (f'Cached version information from {updated}: '
                           f'{version_cache.get("releases", {})}'))

    if time.time() - updated > 24 * 3600:
        _emit_debug(reporter, 'Updating release version cache')

        releases = {}
        for page in range(5):
            url = f'https://api.github.com/repos/longhorn/longhorn/releases?page={page}'
            _emit_debug(reporter, f'Fetching {url}')
            r = requests.request(
                'GET', url,
                headers={
                    'Accept': 'application/vnd.github+json',
                    'User-Agent': apiclient.get_user_agent()
                })

            if r.status_code not in [200, 201, 204]:
                raise exceptions.ReleaseLookupError.http_status(
                    'Longhorn', url, r.status_code, r.text)

            d = r.json()
            _emit_debug(reporter, 'Fetched release data:')
            _emit_debug(reporter, json.dumps(d, indent=4, sort_keys=True))
            for reldata in d:
                if reldata['prerelease']:
                    continue
                tagname = reldata['tag_name'].lstrip('v')
                releases[tagname] = reldata['tarball_url']

        # Find the most recent version. Longhorn has occasionally
        # published tags which are not valid PEP 440 versions (for
        # example v1.4.0-hotfix1), so skip anything unparsable.
        latest = None
        for tagname in list(releases.keys()):
            try:
                parsed_version = Version(tagname)
            except InvalidVersion:
                _emit_debug(reporter, f'Skipping unparsable tag {tagname}')
                continue
            if not latest:
                latest = parsed_version
            elif parsed_version > latest:
                latest = parsed_version

        if latest is None:
            raise exceptions.ReleaseLookupError.no_parsable_longhorn_release()

        version_cache['releases'] = releases
        version_cache['latest'] = str(latest)
        version_cache['updated'] = time.time()
        client.set_namespace_metadata_item(
            namespace, LONGHORN_VERSION_CACHE_KEY, version_cache)

    return version_cache['latest']


def _describe_agent_op(aop, max_len=60):
    """Return a short human readable description of the command an agent operation is up to."""
    commands = aop.get('commands', [])
    results = aop.get('results', {}) or {}

    # Results are recorded per command index as they complete, so the number
    # of results is the index of the currently executing command.
    idx = min(len(results), len(commands) - 1)
    if idx < 0:
        return None

    c = commands[idx]
    desc = c.get('commandline')
    if not desc:
        desc = c.get('command', 'unknown')
        if c.get('path'):
            desc += ' %s' % c['path']

    # Multi-line commands (for example heredocs) would break the one line
    # per item status display, so describe them by their first line.
    if '\n' in desc:
        desc = desc.split('\n', 1)[0] + ' ...'

    if max_len and len(desc) > max_len:
        desc = desc[:max_len - 3] + '...'
    return desc
