"""The exception hierarchy raised by the k3s library API.

These replace the ``sys.exit(1)`` calls in ``primitives.py`` and
``__init__.py``. Each exception stores the values the failing code used to
interpolate into its ``print()`` calls as plain attributes, and its
``__str__`` renders exactly the text the CLI prints today, so the Click
layer can catch the common base once and print ``str(e)`` with no
per-command formatting.

Nothing here is raised yet -- this module is inert until a later step in
the phase 1 plan replaces the ``sys.exit(1)`` sites with raises. Because
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

    Corresponds to the two identical checks in ``k3s_create()``
    (``__init__.py:134-139``): the name is already present in the
    namespace's cluster list, or namespace metadata already exists for it.
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

    Corresponds to ``__init__.py:146-148``.
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

    - ``unknown_cluster()``: ``getconfig``, no cluster metadata at all
      (``__init__.py:339-341``).
    - ``does_not_exist()``: ``show`` (``__init__.py:361-363``) and
      ``delete`` (``__init__.py:383-386``).
    - ``not_found()``: ``expand-workers`` (``__init__.py:483-485``),
      ``expand-addresses`` (``__init__.py:509-512``) and ``update-os``
      (``__init__.py:534-537``).

    The sixth original site, ``getconfig``'s "no kubeconfig" check
    (``__init__.py:343-346``), is not here: that cluster does exist, so it
    is ``ClusterIncompleteError`` instead.
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

    Corresponds to ``getconfig``'s second check (``__init__.py:343-346``):
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
      non-2xx response fetching release data, for either product. Used
      for the k3s channel fetch (``primitives.py:86-91``, with
      ``product='k3s'``) and the Longhorn release fetch
      (``primitives.py:165-170``, with ``product='Longhorn'``); both
      render identically apart from the product name.
    - ``no_usable_k3s_channels(url, response_snippet)``: the k3s channel
      response parsed but yielded no channels at all
      (``primitives.py:110-114``). ``response_snippet`` is the caller's
      already-truncated ``json.dumps(d)[:512]``.
    - ``unknown_channel(release_channel)``: the requested channel is not
      in the (possibly cached) release map (``primitives.py:122-124``).
    - ``no_parsable_longhorn_release()``: every Longhorn tag failed to
      parse as a PEP 440 version (``primitives.py:196-198``).
    """

    def __init__(self, reason, message, **fields):
        self.reason = reason
        self.message = message
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

    Corresponds to ``_abort_agent_op_error()`` (``primitives.py:266-283``).
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

    Corresponds to the return-code check in ``reap_execute()``
    (``primitives.py:394-405``). ``stdout`` and ``stderr`` are the raw
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
    """Raised when a local kubeconfig write, merge or cleanup fails.

    Three distinct sites, all in ``__init__.py``, construct via the
    classmethods below:

    - ``missing_kubectl(main_config_path, name)``: create found an
      existing ``~/.kube/config`` to merge into but no local ``kubectl``
      binary to do the merge with (``__init__.py:249-253``).
    - ``merge_failed(main_config_path, returncode, stderr)``: create's
      ``kubectl config view --flatten`` merge exited non-zero
      (``__init__.py:264-268``); the second line is only rendered when
      ``stderr`` is non-empty, matching the original's conditional
      ``print()``.
    - ``unset_failed(config_elem)``: delete's ``kubectl config unset``
      loop exited non-zero for one config element
      (``__init__.py:461-465``). This is a separate rendering from the
      two above -- it names a config element, not a config file path --
      but it is still local-kubectl-state, so it lives on this class
      rather than on ``ClusterNotFoundError``.
    """

    def __init__(self, reason, message, **fields):
        self.reason = reason
        self.message = message
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
