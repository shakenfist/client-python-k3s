"""The exception hierarchy raised by the k3s library API.

These are raised where ``primitives.py`` and ``cluster.py`` used to
print a message and exit the process -- the command bodies which did that
in ``__init__.py`` are ``Cluster`` methods now. Each exception stores the
values the failing code used to interpolate into its ``print()`` calls as
plain attributes, and its ``__str__`` renders exactly the text the CLI
prints today, so the Click layer can catch the common base once and print
``str(e)`` with no per-command formatting.

``GroupCatchClusterExceptions`` in ``__init__.py`` is that catch: it
overrides ``click.Group.invoke()``, prints ``str(e)`` to stdout and exits
1, and is the one place in this package which exits the process. Because
``shakenfist_client_k3s`` is imported unconditionally by the ``sf-client``
plugin loader, this module must import nothing beyond the standard
library.

``K3sClusterException`` deliberately does not subclass anything from
``shakenfist_client.apiclient``: the parent CLI's ``GroupCatchExceptions``
(in ``client-python/shakenfist_client/main.py``) already maps every
``apiclient`` exception to its own error line and exit code, and this
hierarchy must not be caught by that machinery.
"""

import json


class K3sClusterException(Exception):
    """Base class for every exception this library raises."""


class ClusterExistsError(K3sClusterException):
    """Raised when a cluster create is attempted with a name already in use.

    Raised by ``Cluster.create()``, from either of its two identical
    checks: the name is already present in the namespace's cluster list,
    or namespace metadata already exists for it.
    Both render the same text, so this class does not need to distinguish
    which check failed.
    """

    def __init__(self, name):
        self.name = name
        super(ClusterExistsError, self).__init__(name)

    def __str__(self):
        return 'Sorry, that cluster name is already taken'


class NetworkNotFoundError(K3sClusterException):
    """Raised when ``--network`` names a network that does not exist.

    Raised by ``Cluster.create()`` when it looks up the network named by
    its ``network`` argument and finds nothing.
    """

    def __init__(self, network):
        self.network = network
        super(NetworkNotFoundError, self).__init__(network)

    def __str__(self):
        return 'Specified network does not exist'


class ClusterNotFoundError(K3sClusterException):
    """Raised when a named cluster (or a required part of its state) is missing.

    This covers six sites across five commands, which use three distinct
    message strings. None of the three interpolate the cluster name, so
    ``name`` is carried only as a structured attribute, not rendered.
    Construct via the classmethods below, one per distinct message:

    - ``unknown_cluster()``: raised by ``Cluster.get_kubeconfig()`` when
      there is no cluster metadata at all.
    - ``does_not_exist()``: raised by ``Cluster.show()`` and
      ``Cluster.delete()``.
    - ``not_found()``: raised by ``Cluster.expand_workers()``,
      ``Cluster.expand_addresses()`` and ``Cluster.update_os()``.

    The sixth original site, ``Cluster.get_kubeconfig()``'s second check
    -- metadata present but no kubeconfig recorded -- is not here: that
    cluster does exist, so it raises ``ClusterIncompleteError`` instead.
    """

    def __init__(self, name, reason, message):
        self.name = name
        self.reason = reason
        self.message = message
        super(ClusterNotFoundError, self).__init__(message)

    def __str__(self):
        return self.message

    @classmethod
    def unknown_cluster(cls, name):
        return cls(name, 'unknown_cluster', 'Unknown cluster')

    @classmethod
    def does_not_exist(cls, name):
        return cls(name, 'does_not_exist',
                   'Sorry, that cluster name does not appear to exist')

    @classmethod
    def not_found(cls, name):
        return cls(name, 'not_found', 'Cluster not found!')


class ClusterIncompleteError(K3sClusterException):
    """Raised when a cluster exists but has not finished being built.

    Raised by ``Cluster.get_kubeconfig()`` from its second check:
    cluster metadata exists (unlike ``ClusterNotFoundError``), but the
    field being asked for -- here, the kubeconfig -- has not been recorded
    yet, because create has not reached that point. This is deliberately a
    distinct class from ``ClusterNotFoundError`` rather than another of its
    classmethods: a caller (in particular the reconcile verb phase 3 adds
    for the ``state: initial`` case, and the Ansible module and conductor
    that motivate this plan) needs to tell "no such cluster, create one"
    apart from "cluster exists but is incomplete, wait or reconcile", and
    that distinction has to survive as the exception's type, not just its
    text.
    """

    def __init__(self, name):
        self.name = name
        super(ClusterIncompleteError, self).__init__(name)

    def __str__(self):
        return 'No kubeconfig for this cluster. Is it fully installed?'


class ReleaseLookupError(K3sClusterException):
    """Raised when looking up a k3s or Longhorn release fails.

    Covers five sites in ``primitives.py``, which reduce to four distinct
    message shapes. Construct via the classmethods below:

    - ``http_status(product, url, status_code, response_text)``: a
      non-2xx response fetching release data, for either product. Raised
      by ``primitives.get_k3s_release()`` on its channel fetch (with
      ``product='k3s'``) and by ``primitives.get_longhorn_release()`` on
      its release fetch (with ``product='Longhorn'``); both render
      identically apart from the product name.
    - ``no_usable_k3s_channels(url, response_snippet)``: raised by
      ``primitives.get_k3s_release()`` when the channel response parsed
      but yielded no channels at all. ``response_snippet`` is the
      caller's already-truncated ``json.dumps(d)[:512]``.
    - ``unknown_channel(release_channel)``: raised by
      ``primitives.get_k3s_release()`` when the requested channel is not
      in the (possibly cached) release map.
    - ``no_parsable_longhorn_release()``: raised by
      ``primitives.get_longhorn_release()`` when every Longhorn tag
      failed to parse as a PEP 440 version.

    Which classmethod built an instance is recorded in ``reason``, but it
    does not decide which attributes exist: every field any of them sets
    is declared below, so ``getattr`` is total and an unset field answers
    None rather than raising ``AttributeError``.
    """

    #: The union of the fields the classmethods below set. A caller which
    #: does not know which constructor ran -- ``docs/library-api.md``
    #: describes these as the failure's details, and phase 5's
    #: ``fail_json()`` will serialise them -- must be able to read any of
    #: them off any instance.
    FIELDS = ('product', 'url', 'status_code', 'response_text',
              'response_snippet', 'release_channel')

    def __init__(self, reason, message, **fields):
        self.reason = reason
        self.message = message
        for key in self.FIELDS:
            setattr(self, key, None)
        for key, value in fields.items():
            setattr(self, key, value)
        super(ReleaseLookupError, self).__init__(message)

    def __str__(self):
        return self.message

    @classmethod
    def http_status(cls, product, url, status_code, response_text):
        message = (
            'Unable to determine latest %s release version\n'
            '    GET %s\n'
            '    returned HTTP status code %s with text:\n'
            '    %s'
        ) % (product, url, status_code, response_text)
        return cls('http_status', message, product=product, url=url,
                   status_code=status_code, response_text=response_text)

    @classmethod
    def no_usable_k3s_channels(cls, url, response_snippet):
        message = (
            'No usable k3s release channels found\n'
            '    GET %s\n'
            '    returned: %s'
        ) % (url, response_snippet)
        return cls('no_usable_k3s_channels', message, url=url,
                   response_snippet=response_snippet)

    @classmethod
    def unknown_channel(cls, release_channel):
        message = 'Release channel %s not found' % release_channel
        return cls('unknown_channel', message, release_channel=release_channel)

    @classmethod
    def no_parsable_longhorn_release(cls):
        return cls('no_parsable_longhorn_release',
                   'Unable to determine the latest Longhorn release')


class AgentOperationError(K3sClusterException):
    """Raised when a Shaken Fist agent operation enters the error state.

    Built by ``Cluster._agent_op_error()`` and raised by its three
    callers: ``Cluster.await_idle()``, ``Cluster.await_fetch()`` and
    ``Cluster.reap_execute()``.
    ``command_description`` is the value ``_describe_agent_op(aop,
    max_len=None)`` returns, and is only rendered when truthy. ``results``
    is the agent operation's results dict; when it is empty (or falsy) a
    fixed "no results were recorded" line is rendered instead of a JSON
    dump.
    """

    def __init__(self, instance_name, instance_uuid, operation_uuid,
                 command_description, results):
        self.instance_name = instance_name
        self.instance_uuid = instance_uuid
        self.operation_uuid = operation_uuid
        self.command_description = command_description
        self.results = results
        super(AgentOperationError, self).__init__(instance_name)

    def __str__(self):
        lines = [
            'Agent operation failed!',
            '  instance: %s (uuid %s)' % (self.instance_name, self.instance_uuid),
            '  operation: %s' % self.operation_uuid,
        ]
        if self.command_description:
            lines.append('  command: %s' % self.command_description)
        if self.results:
            lines.append('  results: %s' % json.dumps(self.results, indent=4, sort_keys=True))
        else:
            lines.append('  no results were recorded, so the command probably failed to start')
        lines.append(
            "  the server side event log may have more detail: 'sf-client instance events %s'"
            % self.instance_name)
        return '\n'.join(lines)


class CommandFailedError(K3sClusterException):
    """Raised when an agent command completes with a non-zero return code.

    Raised by ``Cluster.reap_execute()`` from its return-code check, as
    distinct from its agent operation state check, which raises
    ``AgentOperationError``. ``stdout`` and ``stderr`` are the raw
    strings from the agent operation's results; ``__str__`` re-joins each
    on its own prefixed line exactly as the original ``print()`` calls did.
    """

    def __init__(self, instance_name, instance_uuid, commandline, return_code, stdout, stderr):
        self.instance_name = instance_name
        self.instance_uuid = instance_uuid
        self.commandline = commandline
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr
        super(CommandFailedError, self).__init__(instance_name)

    def __str__(self):
        lines = [
            'Command failed!',
            '  instance: %s (UUID %s)' % (self.instance_name, self.instance_uuid),
            '  command: %s' % self.commandline,
            'exit code: %s' % self.return_code,
            '   stdout: %s' % '\n   stdout: '.join(self.stdout.split('\n')),
            '   stderr: %s' % '\n   stderr: '.join(self.stderr.split('\n')),
        ]
        return '\n'.join(lines)


class KubeconfigError(K3sClusterException):
    """Raised when a local kubeconfig merge or cleanup fails.

    Not writes: the three ``open(main_config_path, 'w')`` calls in
    ``Cluster.create()`` are unguarded, so a write which fails raises
    ``OSError`` rather than anything from this hierarchy. Every
    classmethod here is about invoking ``kubectl``.

    - ``missing_kubectl(main_config_path, name)``: raised by
      ``Cluster.create()`` when it finds an existing ``~/.kube/config``
      to merge into but no local ``kubectl`` binary to do the merge with.
    - ``merge_failed(main_config_path, returncode, stderr)``: raised by
      ``Cluster.create()`` when its ``kubectl config view --flatten``
      merge exits non-zero; ``stderr`` is decoded from the bytes
      ``subprocess`` returns at the raise, and the second line is only
      rendered when it is non-empty, matching the original's conditional
      ``print()``.
    - ``unset_failed(config_elem)``: raised by ``Cluster.delete()`` when
      its ``kubectl config unset`` loop exits non-zero for one config
      element. This is a separate rendering from the two above -- it
      names a config element, not a config file path -- but it is still
      local-kubectl-state, so it lives on this class rather than on
      ``ClusterNotFoundError``.

    As with ``ReleaseLookupError``, the union of the fields the
    classmethods set is declared explicitly, so which attributes an
    instance answers to does not depend on which one built it.
    """

    #: The union of the fields the classmethods below set. See
    #: ``ReleaseLookupError.FIELDS`` for why this is not left implicit.
    FIELDS = ('main_config_path', 'name', 'returncode', 'stderr',
              'config_elem')

    def __init__(self, reason, message, **fields):
        self.reason = reason
        self.message = message
        for key in self.FIELDS:
            setattr(self, key, None)
        for key, value in fields.items():
            setattr(self, key, value)
        super(KubeconfigError, self).__init__(message)

    def __str__(self):
        return self.message

    @classmethod
    def missing_kubectl(cls, main_config_path, name):
        message = (
            'A local kubectl binary is required to merge the new cluster into\n'
            '%s, but none was found. The new cluster credentials are\n'
            "available from 'sf-client k3s getconfig %s'."
        ) % (main_config_path, name)
        return cls('missing_kubectl', message, main_config_path=main_config_path, name=name)

    @classmethod
    def merge_failed(cls, main_config_path, returncode, stderr=None):
        lines = ['Failed to update %s, return code %d' % (main_config_path, returncode)]
        if stderr:
            lines.append(stderr)
        message = '\n'.join(lines)
        return cls('merge_failed', message, main_config_path=main_config_path,
                   returncode=returncode, stderr=stderr)

    @classmethod
    def unset_failed(cls, config_elem):
        message = 'Could not unset kubectl config element %s' % config_elem
        return cls('unset_failed', message, config_elem=config_elem)
