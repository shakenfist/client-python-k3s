"""The state a k3s orchestration call needs, as an object.

This replaces the click context the orchestration primitives used to be
handed. The primitives remain module level functions (see decision 2 in
``docs/plans/library-api-and-collection-phase-01-library-api.md``); they
simply take a ``Cluster`` as their first argument now, so that a library
caller does not have to fabricate a click context to reach them.

Because ``shakenfist_client_k3s`` is imported unconditionally by the
``sf-client`` plugin loader, this module imports nothing beyond the
standard library and modules this package already imports.
"""

from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress


class Cluster:
    """One k3s cluster, and everything the orchestration primitives need to reach it.

    ``name`` may be None. Three of the commands (``list``,
    ``query-k3s-version`` and ``query-longhorn-version``) are namespace
    scoped rather than cluster scoped and bind that way today, and the
    release lookups they use only need a client and a namespace. Asking
    such a Cluster for cluster metadata is a programming error rather than
    a user error, so it raises ValueError rather than one of the
    exceptions in ``exceptions.py``: there is no message a user could act
    on, and it must not be caught by the click layer's handler and
    reported as a cluster failure.
    """

    def __init__(self, client, name, namespace, reporter=None):
        self.client = client
        self.name = name
        self.namespace = namespace
        self.reporter = reporter if reporter is not None else progress.Reporter()

        # The Progress reporter for the operation in flight, if one has
        # been built. get_progress() makes a default on demand, which is
        # what the primitives rely on when a caller has not made one.
        self.progress = None

        # This cluster's namespace metadata, cached for the life of the
        # object. Namespace metadata is a single document which conductor
        # also writes, so every read is an opportunity to lose someone
        # else's update; the cache exists to keep the number of reads (and
        # therefore the width of that window) the same as it was when this
        # state lived in ctx.obj.
        self._metadata = {}

    def _metadata_key(self):
        """Return the namespace metadata key this cluster's state is stored under."""
        if not self.name:
            raise ValueError(
                'this Cluster has no name, so it has no cluster metadata; '
                'namespace scoped operations must not ask for it')
        return primitives.METADATA_KEY % self.name

    def get_metadata(self):
        """Return this cluster's metadata, fetching it once and then caching it.

        A miss is cached as well as a hit: a cluster which does not exist
        stores None, so asking twice does not fetch twice. Create asks
        before it writes, and the number of namespace metadata reads a
        create performs is behaviour worth preserving exactly.
        """
        md_key = self._metadata_key()
        if md_key not in self._metadata:
            namespace_md = self.client.get_namespace_metadata(self.namespace)
            self._metadata[md_key] = namespace_md.get(md_key)
        return self._metadata[md_key]

    def set_metadata(self, md):
        """Write this cluster's metadata through to the API, updating the cache."""
        md_key = self._metadata_key()
        self._metadata[md_key] = md
        self.client.set_namespace_metadata_item(self.namespace, md_key, md)

    def delete_metadata(self):
        """Remove this cluster's metadata from both the cache and the API.

        Deleting metadata which was never read raises KeyError, as it
        always has: the only caller deletes a cluster it has just read and
        updated, so a cold cache here means the caller is confused rather
        than that there is nothing to do.
        """
        md_key = self._metadata_key()
        del self._metadata[md_key]
        self.client.delete_namespace_metadata_item(self.namespace, md_key)

    def get_progress(self):
        """Return the Progress reporter for this operation, making a default if needed.

        Commands which know how many phases they have build their own and
        assign it; everything else gets one lazily, so a primitive called
        directly by a library caller still reports progress somewhere
        sensible.
        """
        if not self.progress:
            self.progress = progress.Progress(
                verbose=self.reporter.verbose, stream=self.reporter)
        return self.progress
