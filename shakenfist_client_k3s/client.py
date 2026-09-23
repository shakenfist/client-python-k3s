"""Build an API client for a caller which does not have one already.

The CLI never calls this. sf-client's root callback builds a client from
--apiurl, --key and --namespace and leaves it in the Click context, and
__init__.py takes it from there; a construction path of the CLI's own is
how those options came to be ignored once already. This module is for
the callers which have no Click context at all: the sf_k3s_cluster
Ansible module, conductor, and anyone else importing Cluster as a
library.

It mirrors _make_client() in the Shaken Fist server repository at
deploy/collection/plugins/modules/sf_namespace.py, so that a caller who
supplies nothing gets the same environment, ~/.shakenfist and
/etc/sf/shakenfist.json discovery the CLI gets, and one who supplies all
three connection parameters gets them used verbatim.
"""

from shakenfist_client import apiclient


def make_client(api_url=None, namespace=None, key=None):
    """Return an API client suitable for driving this package's orchestration.

    Supply all three of api_url, namespace and key to use them verbatim, or
    none of them to auto-discover configuration the way the CLI does.
    Supplying only some is treated as supplying none, which is the rule the
    Shaken Fist Ansible collection's own modules follow.

    apiclient.UnconfiguredException propagates when discovery finds nothing.
    """
    kwargs = {
        'verbose': False,
        # ASYNC_CONTINUE rather than the collection's ASYNC_BLOCK: this
        # package's orchestration runs its own wait loops and reports
        # progress as it goes, and a blocking client does that waiting
        # internally, so the reporter sees nothing at all.
        #
        # sync_request_timeout is left at apiclient's own default rather
        # than widened to 1800 as the collection widens it. The collection
        # needs the headroom because it blocks; with ASYNC_CONTINUE no
        # single HTTP call waits for orchestration to finish.
        'async_strategy': apiclient.ASYNC_CONTINUE,
    }
    if api_url and namespace and key:
        kwargs.update({
            'base_url': api_url,
            'namespace': namespace,
            'key': key,
            'suppress_configuration_lookup': True,
        })

    # UnconfiguredException is deliberately not caught. The collection
    # translates it into fail_json() because it is an Ansible module;
    # turning it into an exit code or a message here would take that
    # choice away from a library caller who wants the exception.
    return apiclient.Client(**kwargs)
