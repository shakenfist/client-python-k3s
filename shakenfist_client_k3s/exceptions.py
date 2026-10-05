"""The exception hierarchy raised by the k3s library API.

These are raised where ``primitives.py`` and ``cluster.py`` used to
print a message and exit the process -- the command bodies which did that
in ``__init__.py`` are ``Cluster`` methods now. Each exception stores the
values the failing code used to interpolate into its ``print()`` calls as
plain attributes, and its ``__str__`` renders exactly the text the CLI
prints today, so the Click layer can catch the common base once and print
``str(e)`` with no per-command formatting.

``GroupCatchClusterExceptions`` in ``__init__.py`` is that catch: it
overrides ``click.Group.invoke()``, prints ``str(e)`` to stderr and exits
1, and is the one place in this package which exits the process. stderr
rather than stdout because ``get-kubeconfig`` and the ``--json`` outputs
put machine readable text on stdout, and an error printed there is text a
pipeline would parse. Because
``shakenfist_client_k3s`` is imported unconditionally by the ``sf-client``
plugin loader, this module must import nothing beyond the standard library
and siblings which do the same -- ``progress``, for the redaction two of
these classes apply to the command text they are handed.

``K3sClusterException`` deliberately does not subclass anything from
``shakenfist_client.apiclient``: the parent CLI's ``GroupCatchExceptions``
(in ``client-python/shakenfist_client/main.py``) already maps every
``apiclient`` exception to its own error line and exit code, and this
hierarchy must not be caught by that machinery.
"""

import json

from shakenfist_client_k3s import progress


class K3sClusterException(Exception):
    """Base class for every exception this library raises."""


class _ReasonedK3sException(K3sClusterException):
    """Base for the exceptions built through classmethods rather than directly.

    Eight of the classes below describe several distinct failures that read
    the same way to a caller: a manifest cannot be staged, a release
    lookup failed. Each is built through a classmethod per failure, each
    records which one ran in ``reason``, each renders a message its
    classmethod composed, and each carries the failure's details as
    attributes. That shape was written out seven times, byte for byte,
    before this base existed, and the duplication was a cross-phase one:
    two copies arrived with the exception hierarchy, two more when later
    verbs needed their own reasoned errors, a fifth with the heredoc
    refusal, and two with k3s configuration pass-through -- which landed
    on the default branch while this base class was being written, and is
    why the count in this docstring is worth keeping accurate rather than
    approximate. The eighth, ``ClusterMetadataError``, was written against
    this base from the start.
    ``UnsupportedReleaseError`` named its three fields in its own
    ``__init__`` rather than taking ``**fields``; it declares them in
    ``FIELDS`` like the others now.

    ``reason`` does not decide which attributes exist. Every field any
    classmethod of a subclass sets is declared in that subclass's
    ``FIELDS``, and all of them are initialised to None here, so
    ``getattr`` is total: a caller which does not know which constructor
    ran -- ``docs/library-api.md`` describes these attributes as the
    failure's details, and the Ansible module serialises them into
    ``fail_json()`` -- reads any field off any instance and gets None
    rather than ``AttributeError``. That is why ``FIELDS`` is declared
    rather than left implicit in what each classmethod happens to pass.

    Subclasses declare ``FIELDS`` and their classmethods, and nothing
    else. A subclass whose failures do not share a message shape --
    ``AgentOperationError``, ``CommandFailedError``, ``NodeSizeError`` --
    is not one of these and takes its own named arguments.
    """

    #: The union of the fields this class's classmethods set. Declared by
    #: each subclass; empty here so that the loop below is total for a
    #: subclass which has no details to carry.
    FIELDS = ()

    def __init__(self, reason, message, **fields):
        self.reason = reason
        self.message = message
        for key in self.FIELDS:
            setattr(self, key, None)
        for key, value in fields.items():
            setattr(self, key, value)
        super(_ReasonedK3sException, self).__init__(message)

    def __str__(self):
        return self.message


class ClusterExistsError(K3sClusterException):
    """Raised when a cluster create is attempted with a name already in use.

    Raised by ``Cluster.create()``, from either of two checks: the name is
    already present in the namespace's cluster list, or namespace metadata
    already exists for it and records a cluster which finished being built.
    The second check no longer always reaches here -- metadata whose
    ``state`` is anything but ``created`` raises
    ``ClusterInterruptedError.mid_create()`` instead, because a cluster
    which never finished is a different problem with a different answer
    (tear it down) from a name which is genuinely in use. Both paths which
    do reach here render the same text, so this class does not need to
    distinguish which check failed.
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

    This covers one site in each of eight commands, which use three
    distinct message strings. None of the three interpolate the cluster
    name, so ``name`` is carried only as a structured attribute, not
    rendered. Construct via the classmethods below, one per distinct
    message:

    - ``unknown_cluster()``: raised by ``Cluster.get_kubeconfig()`` when
      there is no cluster metadata at all.
    - ``does_not_exist()``: raised by ``Cluster.show()``,
      ``Cluster.health()`` and ``Cluster.delete()``.
    - ``not_found()``: raised by ``Cluster.expand_workers()``,
      ``Cluster.remove_worker()``, ``Cluster.expand_addresses()`` and
      ``Cluster.update_os()``.

    Which of the two "there is no such cluster" messages a verb uses is
    historical rather than meaningful, and new verbs join the group they
    read like: ``health()`` reports on a cluster the way ``show()`` does,
    so it says what ``show()`` says.

    One further original site, ``Cluster.get_kubeconfig()``'s second check
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
    classmethods: a caller (the Ansible module and conductor that motivate
    this plan) needs to tell "no such cluster, create one" apart from
    "cluster exists but is incomplete, wait or tear it down", and that
    distinction has to survive as the exception's type, not just its text.

    Phase 3 decided there is no reconcile verb: an incomplete cluster is
    torn down and rebuilt rather than resumed (decision 5). A caller which
    wants to know whether the cluster as a whole ever finished, rather than
    whether one field of it is present, reads ``md['state']`` or catches
    ``ClusterInterruptedError``.
    """

    def __init__(self, name):
        self.name = name
        super(ClusterIncompleteError, self).__init__(name)

    def __str__(self):
        return 'No kubeconfig for this cluster. Is it fully installed?'


class ClusterInterruptedError(_ReasonedK3sException):
    """Raised when a cluster's own metadata says it never finished being built.

    ``md['state']`` is written by ``Cluster.create()`` and
    ``Cluster.delete()`` and, until phase 3, was read nowhere at all (survey
    finding 2 of
    ``docs/plans/PLAN-library-api-and-collection-phase-03-missing-verbs.md``).
    Anything other than ``created`` means a create or a delete stopped part
    way through, so the cluster's nodes, tokens and kubeconfig are in an
    unknown combination of present and absent.

    Per decision 5 of that plan the answer is always teardown rather than
    resume, so every message here names ``sf-client k3s delete <name>``.
    There are two distinct messages, hence the classmethod constructors
    rather than ``WorkerNotFoundError``'s plain one:

    - ``mid_create(name, state)``: raised by ``Cluster.create()`` when the
      name it was asked for already holds the metadata of an unfinished
      cluster. This is deliberately distinguished from
      ``ClusterExistsError``, which says only that the name is taken: a
      name held by a finished cluster belongs to a working cluster, and a
      name held by an unfinished one belongs to rubbish the caller can
      remove.
    - ``not_usable(name, state, verb)``: raised by the verbs which need a
      built cluster to work on -- ``Cluster.expand_workers()``,
      ``Cluster.remove_worker()`` and ``Cluster.expand_addresses()``. All
      three reach for ``md['control_plane_nodes'][0]`` or for a node token
      which an interrupted create may never have recorded, and would
      otherwise fail with an ``IndexError`` or install k3s against a token
      of ``None``.

    This is not ``ClusterIncompleteError``, which is about one absent field
    of a cluster that may still be being built successfully right now
    (``get_kubeconfig()`` called against a create which has not reached its
    credentials phase). This one is about the recorded state of the cluster
    as a whole, and carries that state as an attribute so a caller --
    conductor, or phase 5's Ansible module -- can branch on it rather than
    on the message text.
    """

    #: The union of the fields the classmethods below set. See
    #: ``_ReasonedK3sException`` for why this is not left implicit.
    FIELDS = ('name', 'state', 'verb')

    @classmethod
    def mid_create(cls, name, state):
        message = (
            'Cluster %s was interrupted while it was being built: its state is\n'
            "'%s' rather than 'created'. Resuming a half built cluster is not\n"
            'supported, so remove what is left of it with\n'
            "'sf-client k3s delete %s' and then create it again."
        ) % (name, state, name)
        return cls('mid_create', message, name=name, state=state)

    @classmethod
    def not_usable(cls, name, state, verb):
        message = (
            'Cluster %s was interrupted while it was being built: its state is\n'
            "'%s' rather than 'created', so %s cannot run against it.\n"
            "Remove what is left of it with 'sf-client k3s delete %s'."
        ) % (name, state, verb, name)
        return cls('not_usable', message, name=name, state=state, verb=verb)


class WorkerNotFoundError(K3sClusterException):
    """Raised when a worker asked for by uuid is not one of this cluster's workers.

    Raised by ``Cluster.remove_worker()``, which validates every uuid it
    was handed against ``md['worker_nodes']`` before it drains or deletes
    anything: a typo in the third of three arguments must not leave the
    first two destroyed. It therefore carries every uuid which did not
    match, not just the first, so that one run reports every typo.

    A single message shape, so this is a plain constructor rather than the
    classmethods ``ClusterNotFoundError`` and ``KubeconfigError`` use --
    those exist to give several distinct messages one class, and there is
    only one here. The uuids are rendered, unlike the cluster names the
    older exceptions carry but do not print, because a caller removing
    three workers cannot otherwise tell which argument was wrong.
    """

    def __init__(self, name, instance_uuids):
        self.name = name
        self.instance_uuids = list(instance_uuids)
        super(WorkerNotFoundError, self).__init__(name)

    def __str__(self):
        if len(self.instance_uuids) == 1:
            return ('Cluster %s has no worker node with uuid %s'
                    % (self.name, self.instance_uuids[0]))
        return ('Cluster %s has no worker nodes with uuids %s'
                % (self.name, ', '.join(self.instance_uuids)))


class WorkerUnnamedError(K3sClusterException):
    """Raised when a worker's instance has no name to drain its node by.

    Raised by ``Cluster.remove_worker()``, which resolves every worker's
    node name from its instance before it drains anything. k3s knows a node
    by the hostname of the machine it runs on, and Shaken Fist derives that
    from the instance's name, so an instance representation with no usable
    ``name`` is one whose node cannot be identified. Draining the wrong node
    would evict somebody else's pods, and skipping the drain would delete a
    node object with workloads still on it, so this refuses instead --
    before the first worker is touched, so nothing has been destroyed when
    it fires.

    Not reachable from the Shaken Fist API as it stands: every instance
    representation it returns carries a name. It exists because
    ``remove_worker()`` is the destructive verb and ``_node_health()``
    already reads the same field defensively, and because a caller which
    catches ``K3sClusterException`` should not be handed an
    ``AttributeError`` from the middle of a multi-worker removal.
    """

    def __init__(self, name, instance_uuid):
        self.name = name
        self.instance_uuid = instance_uuid
        super(WorkerUnnamedError, self).__init__(name)

    def __str__(self):
        return ('Cluster %s has a worker, instance %s, whose instance record\n'
                'has no name, so the k3s node it became cannot be identified\n'
                'and it cannot be drained.'
                % (self.name, self.instance_uuid))


class ComponentNotInstalledError(K3sClusterException):
    """Raised when a verb needs an optional component this cluster was built without.

    ``create()`` takes ``install_metallb`` and ``install_longhorn``, and
    records which way each of them went in the cluster metadata. The verbs
    which drive one of those components ask that metadata before they do
    anything, because the alternative is not a clean failure: MetalLB's
    absence is discovered by ``kubectl wait`` timing out after five
    minutes on a namespace which does not exist, by which point
    ``expand_addresses()`` has already routed and charged for addresses
    nothing can hand out.

    ``component`` is the component's own name as an operator would type
    it, and ``verb`` is the command line spelling of what was refused,
    matching ``ClusterInterruptedError.not_usable()``.
    """

    def __init__(self, name, component, verb):
        self.name = name
        self.component = component
        self.verb = verb
        super(ComponentNotInstalledError, self).__init__(name)

    def __str__(self):
        return (
            'Cluster %s was created without %s, so %s has nothing to\n'
            'configure. Install %s on the cluster yourself if you want it.'
            % (self.name, self.component, self.verb, self.component))


class ManifestError(_ReasonedK3sException):
    """Raised when a manifest handed to ``Cluster.create()`` cannot be staged.

    ``--manifest`` (and the ``manifests`` argument behind it) names local
    files which are written into k3s's auto-apply directory on the first
    control plane node before k3s is installed there, so that k3s applies
    them itself when the server first starts. Decision 8 of the phase 3
    plan says nothing about them is templated, so this is not a validation
    of what a manifest declares. It is a refusal of the seven ways a
    file cannot be staged at all, each of which would otherwise either corrupt
    the payload or drop it silently:

    - ``not_a_manifest(path, suffixes)``: the basename does not end in
      ``.yaml``, ``.yml`` or ``.json``. k3s's deploy controller only looks
      at those three, so such a file would be copied onto the node and then
      ignored without comment.
    - ``unsafe_basename(path, basename)``: the basename is not a plain
      filename. The basename is interpolated into the shell command line
      which writes the file on the control plane node, so this is the
      check which makes ``/tmp/a;touch /pwned.yaml`` a refusal rather
      than a command run as root. The write site quotes the name as well
      -- see rule 1 at the top of ``cluster.py`` -- so neither of these
      is the only thing standing between a caller and that.
    - ``duplicate_basename(basename, first_path, second_path)``: two paths
      share a basename, and the basename is the destination filename, so
      the second write would silently replace the first.
    - ``unreadable(path, detail)``: the local file could not be opened,
      read, or decoded as UTF-8. This is the check which stands in for
      ``click.Path(exists=True)`` for a library caller, which has no click
      to check its paths for it, and the reason the read names its
      encoding and catches ``UnicodeDecodeError`` as well as ``OSError``:
      a decode error is a ``ValueError``, and letting one out would put a
      caller which catches ``K3sClusterException`` back to catching
      builtins.
    - ``invalid_yaml(path, detail)``: the file is not parsable YAML. k3s
      logs a failure to apply it and carries on, so without this the
      cluster comes up looking healthy with the payload missing.
    - ``invalid_json(path, detail)``: the same refusal for a file whose
      content marks it as JSON. Which of the two applies is decided by the
      content and not by the suffix, because that is how k3s decides: see
      the comment on the parse in ``read_manifests()``. One class rather
      than two because the caller's problem is the same either way, and
      separate ``reason`` values because the format named in the message
      has to be the one the file is written in.
    - ``delimiter_collision(path, delimiter)``: a line of the file is
      exactly the heredoc delimiter the staging write uses, which would end
      the heredoc early and truncate the manifest.

    Unlike ``WorkerNotFoundError``, which collects every uuid which did not
    match, this raises at the first unusable manifest. That class
    aggregates because it is about to destroy instances and a second run is
    not free; every check here is local, nothing has been changed when it
    fires, and re-running after a fix costs nothing.
    """

    #: The union of the fields the classmethods below set. See
    #: ``_ReasonedK3sException`` for why this is not left implicit.
    FIELDS = ('path', 'other_path', 'basename', 'suffixes', 'delimiter',
              'detail')

    @classmethod
    def not_a_manifest(cls, path, suffixes):
        message = (
            'Manifest %s will never be applied: k3s only applies files in its\n'
            'manifests directory whose names end in %s.'
        ) % (path, ', '.join(suffixes))
        return cls('not_a_manifest', message, path=path,
                   suffixes=tuple(suffixes))

    @classmethod
    def unsafe_basename(cls, path, basename):
        message = (
            'Manifest %s cannot be staged: the name it would be written under\n'
            'on the cluster, %r, is not a plain filename. Manifest names may\n'
            'contain letters, digits, dots, underscores and hyphens, and must\n'
            'start with a letter or a digit.'
        ) % (path, basename)
        return cls('unsafe_basename', message, path=path, basename=basename)

    @classmethod
    def duplicate_basename(cls, basename, first_path, second_path):
        message = (
            'Manifests %s and %s would both be written to %s on the cluster,\n'
            'so one of them would replace the other. Rename one of them.'
        ) % (first_path, second_path, basename)
        return cls('duplicate_basename', message, path=second_path,
                   other_path=first_path, basename=basename)

    @classmethod
    def unreadable(cls, path, detail):
        message = 'Could not read manifest %s: %s' % (path, detail)
        return cls('unreadable', message, path=path, detail=detail)

    @classmethod
    def invalid_yaml(cls, path, detail):
        message = (
            'Manifest %s is not valid YAML, so k3s would refuse to apply it:\n'
            '%s'
        ) % (path, detail)
        return cls('invalid_yaml', message, path=path, detail=detail)

    @classmethod
    def invalid_json(cls, path, detail):
        message = (
            'Manifest %s is not valid JSON, so k3s would refuse to apply it:\n'
            '%s'
        ) % (path, detail)
        return cls('invalid_json', message, path=path, detail=detail)

    @classmethod
    def delimiter_collision(cls, path, delimiter):
        message = (
            'Manifest %s contains a line which is exactly %s, which is the\n'
            'marker used to write it to the cluster, so it cannot be written\n'
            'without being truncated there.'
        ) % (path, delimiter)
        return cls('delimiter_collision', message, path=path,
                   delimiter=delimiter)


class GuestFileError(_ReasonedK3sException):
    """Raised when a file this library writes onto a cluster node cannot be written.

    The in-guest agent runs a shell command line, so every file this
    library puts on a node is written by a ``cat - > path << DELIMITER``
    heredoc. Rule 2 at the top of ``cluster.py`` keeps the delimiter
    quoted, which stops the remote shell expanding anything inside the
    body -- and that is not the whole of what a body can do. A body
    containing a line which is exactly the delimiter ends the heredoc
    early, and everything after it is read by the shell as commands,
    running as root on the node.

    So an interpolated value is refused rather than written:

    - ``delimiter_collision(path, delimiter)``: the body contains a line
      equal to the delimiter. The values which reach these bodies are
      addresses out of the namespace metadata document, which anything
      holding the namespace's credentials can write and which conductor
      also writes, so "the API would not return that" is not an argument
      this package makes -- rule 1 says so in as many words.

    ``ManifestError.delimiter_collision`` is the same refusal for a
    caller's own manifest file, checked earlier so that the message can
    name the local path the operator passed; this is the backstop which
    covers every heredoc, including ones added later.
    """

    #: The union of the fields the classmethods below set. See
    #: ``_ReasonedK3sException`` for why this is not left implicit.
    FIELDS = ('path', 'delimiter')

    @classmethod
    def delimiter_collision(cls, path, delimiter):
        message = (
            'Refusing to write %s on the cluster node: the content contains a\n'
            'line which is exactly %s, which is the marker used to write it,\n'
            'so the rest of the content would be run as commands instead.'
        ) % (path, delimiter)
        return cls('delimiter_collision', message, path=path,
                   delimiter=delimiter)


class ClusterMetadataError(_ReasonedK3sException):
    """Raised when a value read back out of the namespace metadata is not usable.

    Rule 1 at the top of ``cluster.py`` says the metadata document is
    outside controlled: anything holding the namespace's credentials can
    write it, and conductor also does. Everything this package reads
    from it is therefore either quoted, validated, or refused.

    ``GuestFileError.delimiter_collision`` is the refusal of last resort
    for a metadata value on its way into a heredoc, and it fires at the
    point of writing. That is the wrong place for a verb which spends
    something first. ``expand_addresses()`` routes floating addresses --
    which are charged for -- and commits them to the metadata before the
    configuration they are for is written, so a value which was already
    bad fails after the spending rather than before it. Its docstring
    makes the same argument for refusing a cluster without metallb up
    front, and this is that argument applied to the document's contents
    as well as its flags.

    - ``not_an_address(name, key, value)``: a key which must hold IP
      addresses holds something that is not one. Raised from two points:
      ``expand_addresses()`` checks what is already recorded before it
      allocates anything, and ``allocate_metallb_addresses()`` checks
      each address the API hands back before recording it. Rule 1 says
      the Shaken Fist API "is not this package's trust boundary" in as
      many words, so the second check is required by the same rule as
      the first rather than being paranoia about our own server. The
      message deliberately does not say whether anything was changed,
      because that differs between the two sites; the first has spent
      nothing and the second may have routed an address it then refused
      to record.
    """

    #: The union of the fields the classmethods below set. See
    #: ``_ReasonedK3sException`` for why this is not left implicit.
    FIELDS = ('name', 'key', 'value')

    @classmethod
    def not_an_address(cls, name, key, value):
        message = (
            'Cluster %s has an unusable value in its metadata: %s contains\n'
            '%r, which is not an IP address, so it is refused rather than\n'
            'written into the cluster configuration. That document can be\n'
            "written by anything holding this namespace's credentials, so a\n"
            'value in it is checked rather than trusted.'
        ) % (name, key, value)
        return cls('not_an_address', message, name=name, key=key, value=value)


class SshKeyError(K3sClusterException):
    """Raised when the ssh key handed to ``Cluster.create()`` cannot be read.

    ``--sshkey`` (and the ``sshkey`` argument behind it) names a local
    public key file whose content is put in the cloud-init user data of
    every node, so it is read before the first instance is created. This is
    the same refusal ``ManifestError.unreadable()`` is, for the same reason
    and at the same point: the CLI has ``click.Path(exists=True)`` to catch
    a path which is not there, a library caller has nothing, and an
    ``OSError`` or ``UnicodeDecodeError`` out of ``create()`` is outside the
    hierarchy phase 5's Ansible module catches.

    A separate class rather than another ``ManifestError`` reason, because
    the two are handed in by different arguments and a caller which wants
    to tell "your manifest is unusable" from "your ssh key is unusable"
    should be able to do it on the type.
    """

    def __init__(self, path, detail):
        self.path = path
        self.detail = detail
        super(SshKeyError, self).__init__(path)

    @classmethod
    def unreadable(cls, path, detail):
        return cls(path, detail)

    def __str__(self):
        return 'Could not read ssh key %s: %s' % (self.path, self.detail)


class NodeSizeError(K3sClusterException):
    """Raised when a node size handed to ``Cluster.create()`` is not a positive integer.

    ``create()`` takes a vCPU count, a memory size in MB and a disk size in
    GB for each of the two roles, and ``validate_node_sizes()`` in
    ``cluster.py`` checks all six before the cluster's name is registered.
    That is the point of raising early: a size discovered to be unusable
    once the name is in the namespace's cluster list leaves a claimed name
    and a metadata document stuck in ``initial``, which only a delete
    clears. The command line also refuses these with
    ``click.IntRange(min=1)``, in click's own style, but a library caller
    has no click, and an Ansible variable or a YAML document is exactly
    where ``0``, ``'2'`` or ``yes`` comes from.

    Only "not a positive integer" is refused. ``bool`` counts as not an
    integer, because ``True`` is an ``int`` in Python and would otherwise
    build a one vCPU node. A size which is positive but too small to be
    useful is accepted on purpose: a realistic floor depends on the
    workload, which this library cannot see. See
    ``validate_node_sizes()`` for the reasoning.

    ``role`` is ``'control_plane'`` or ``'worker'`` as the metadata spells
    it, ``field`` is ``'cpus'``, ``'memory'`` or ``'disk'``, and ``value``
    is what was passed, unchanged. The message spells the role with a
    space and renders the value with ``repr()``, so that ``'2'`` and ``2``
    are told apart.
    """

    def __init__(self, role, field, value):
        self.role = role
        self.field = field
        self.value = value
        super(NodeSizeError, self).__init__(role, field, value)

    @classmethod
    def not_positive_integer(cls, role, field, value):
        return cls(role, field, value)

    def __str__(self):
        return '%s %s must be a positive integer, not %r' % (
            self.role.replace('_', ' '), self.field, self.value)


class K3sConfigError(_ReasonedK3sException):
    """Raised when k3s configuration handed to ``Cluster.create()`` cannot be used.

    ``create()`` takes a ``server_config`` and an ``agent_config``: mappings
    of k3s configuration keys, written onto every control plane node and
    every worker respectively as a drop-in file in
    ``/etc/rancher/k3s/config.yaml.d/``, which k3s reads after the plugin's
    own ``config.yaml``. ``validate_k3s_config()`` in ``cluster.py`` checks
    both before the cluster's name is registered, for the reason
    ``NodeSizeError`` gives, and ``read_k3s_config()`` reads a file into
    one for the command line. Construct via the classmethods below, one
    per refusal:

    - ``not_a_mapping(role, value)``: the configuration is not a mapping
      -- a list, or a scalar. k3s's configuration file is a mapping of
      flag names to values, and nothing else means anything to it.
    - ``non_string_key(role, key)``: a key is not a string. YAML reads
      ``1: x`` as an integer key, which names no k3s flag, and which the
      JSON the metadata is stored as cannot hold unchanged either.
    - ``not_representable(role, key, value)``: a value does not survive a
      round trip through JSON unchanged. The mapping is recorded in the
      cluster's namespace metadata, which is a JSON document, so that
      ``expand-workers`` can write the same file onto workers it adds
      later. ``yaml.safe_load`` happily produces ``datetime.date``,
      ``bytes`` and integer keys inside a value, and without this refusal
      the first sign of one would be ``set_metadata()`` failing after the
      name had been registered -- or worse, succeeding with something
      other than what was written onto the nodes.
    - ``owned_key(role, key)``: the key is one the plugin sets itself, or
      depends on k3s leaving at its default. A trailing ``+`` does not
      change that, because on a string key k3s's ``+`` appends to the
      plugin's value rather than leaving it alone. The one exception is
      ``tls-san+``, which is how a caller adds SANs to the plugin's; the
      message for a bare ``tls-san`` says so. The keys and why each is
      owned are listed beside ``K3S_SERVER_OWNED_KEYS`` and
      ``K3S_AGENT_OWNED_KEYS`` in ``cluster.py``.
    - ``delimiter_collision(role, delimiter)``: a line of the YAML the
      configuration is written as is exactly the heredoc delimiter the
      write uses, which would end the heredoc early and truncate the
      file. This is ``ManifestError.delimiter_collision()`` for
      configuration rather than manifests.
    - ``unreadable(path, reason)``: the file ``read_k3s_config()`` was
      given could not be opened, decoded as UTF-8, or parsed as a single
      YAML document. As with ``ManifestError.unreadable()``, this keeps
      ``OSError``, ``UnicodeDecodeError`` and ``yaml.YAMLError`` inside
      this hierarchy, so a caller which catches ``K3sClusterException``
      does not have to catch builtins and PyYAML's errors as well.

    Keys are not checked against k3s's own flag list, deliberately: that
    would be a copy of k3s's flags which goes stale with every release,
    and k3s already logs and ignores a flag it does not recognise for the
    role (decision 2 of
    ``docs/plans/PLAN-node-customisation-phase-02-k3s-config.md``).

    ``role`` is ``'server'`` or ``'agent'``, as k3s spells the two roles,
    and is None for ``unreadable()``, which is about a file before it is
    about a role. Which classmethod built an instance is recorded in
    ``reason``; every field any of them sets is declared in ``FIELDS``, as
    ``ManifestError`` does, so an unset field answers None. ``unreadable()``
    stores its reason as ``detail``, the name ``ManifestError`` uses, since
    ``reason`` is already which refusal this is.
    """

    #: The union of the fields the classmethods below set. See
    #: ``_ReasonedK3sException`` for why this is not left implicit.
    FIELDS = ('role', 'key', 'value', 'delimiter', 'path', 'detail')

    @classmethod
    def not_a_mapping(cls, role, value):
        message = (
            'k3s %s configuration must be a mapping of configuration keys\n'
            'to values, not %s.'
        ) % (role, type(value).__name__)
        return cls('not_a_mapping', message, role=role, value=value)

    @classmethod
    def non_string_key(cls, role, key):
        message = (
            'k3s %s configuration key %r is not a string. k3s configuration\n'
            'keys are flag names, such as node-label.'
        ) % (role, key)
        return cls('non_string_key', message, role=role, key=key)

    @classmethod
    def not_representable(cls, role, key, value):
        message = (
            'k3s %s configuration key %s has a value which cannot be stored\n'
            'unchanged as JSON: %r. The configuration is recorded in the\n'
            'cluster metadata, which is JSON, so that workers added later are\n'
            'configured the same way. Quote dates, and use only strings,\n'
            'numbers, booleans, lists and mappings with string keys.'
        ) % (role, key, value)
        return cls('not_representable', message, role=role, key=key,
                   value=value)

    @classmethod
    def owned_key(cls, role, key):
        message = (
            'k3s %s configuration key %s is set by shakenfist_client_k3s\n'
            'itself, or must be left at its default for the cluster to work,\n'
            'so it cannot be supplied.'
        ) % (role, key)
        if key.rstrip('+') == 'tls-san':
            message += (
                ' To add subject alternative names to the API server\n'
                'certificate alongside the ones the plugin sets, write\n'
                'tls-san+ instead.')
        return cls('owned_key', message, role=role, key=key)

    @classmethod
    def delimiter_collision(cls, role, delimiter):
        message = (
            'k3s %s configuration, written as YAML, contains a line which is\n'
            'exactly %s, which is the marker used to write it to the\n'
            'cluster, so it cannot be written without being truncated there.'
        ) % (role, delimiter)
        return cls('delimiter_collision', message, role=role,
                   delimiter=delimiter)

    @classmethod
    def unreadable(cls, path, reason):
        message = 'Could not read k3s configuration %s: %s' % (path, reason)
        return cls('unreadable', message, path=path, detail=reason)


class UnsupportedReleaseError(_ReasonedK3sException):
    """Raised when ``Cluster.create()`` resolves a k3s release older than the plugin supports.

    The plugin writes configuration onto every node as files in
    ``/etc/rancher/k3s/config.yaml.d/``, and depends on k3s's ``+`` key
    suffix to append to a list rather than replace it. k3s gained the
    drop-in directory in v1.21.0+k3s1 and the ``+`` suffix in
    v1.21.1+k3s1, and an older k3s ignores both silently: a drop-in is
    never read, and ``disable+`` is a key with a different name. So
    ``check_k3s_release()`` in ``cluster.py`` refuses anything older than
    ``K3S_RELEASE_FLOOR`` straight after the release channel is resolved,
    which is before the cluster's name is registered. The channels which
    still resolve to such a release are ones whose Kubernetes went end of
    life in 2022, and Longhorn's chart already refuses them.

    - ``too_old(release, channel, floor)``: the release parsed, and is
      older than ``floor``.
    - ``unparseable(release, channel)``: the release does not start with
      ``vMAJOR.MINOR.PATCH``. That is refused rather than guessed at,
      because a version which cannot be read is not one the plugin can
      promise drop-ins on.

    ``release`` is the version string the channel resolved to, ``channel``
    the release channel which was asked for, and ``floor`` the oldest
    supported version as a ``(major, minor, patch)`` tuple, or None for
    ``unparseable()``. Which classmethod built an instance is recorded in
    ``reason``.
    """

    #: The union of the fields the classmethods below set. See
    #: ``_ReasonedK3sException`` for why this is not left implicit.
    FIELDS = ('release', 'channel', 'floor')

    @classmethod
    def too_old(cls, release, channel, floor):
        message = (
            'k3s release channel %s resolves to %s, which is older than\n'
            'v%s, the oldest release shakenfist_client_k3s supports: older\n'
            'releases silently ignore the configuration files the plugin\n'
            'writes onto every node. Choose a newer release channel.'
        ) % (channel, release, '.'.join(str(part) for part in floor))
        return cls('too_old', message, release=release, channel=channel,
                   floor=tuple(floor))

    @classmethod
    def unparseable(cls, release, channel):
        message = (
            'k3s release channel %s resolves to %r, which is not a version\n'
            'shakenfist_client_k3s can read, so it cannot tell whether that\n'
            'release supports the configuration files the plugin writes.'
        ) % (channel, release)
        return cls('unparseable', message, release=release,
                   channel=channel)


class ReleaseLookupError(_ReasonedK3sException):
    """Raised when looking up a k3s or Longhorn release fails.

    Covers five sites in ``primitives.py``, which reduce to four distinct
    message shapes. Construct via the classmethods below:

    - ``http_status(product, url, status_code, response_text)``: a
      non-2xx response fetching release data, for either product. Raised
      by ``primitives.get_k3s_release()`` on its channel fetch (with
      ``product='k3s'``) and by ``primitives.get_longhorn_release()`` on
      its release fetch (with ``product='Longhorn'``); both render
      identically apart from the product name. ``response_text`` is
      bounded by the caller to ``primitives.RESPONSE_SNIPPET_BYTES``,
      because it is third-party text and whoever serves it would
      otherwise choose the length of this message.
    - ``no_usable_k3s_channels(url, response_snippet)``: raised by
      ``primitives.get_k3s_release()`` when the channel response parsed
      but yielded no channels at all. ``response_snippet`` is the
      caller's already-truncated ``json.dumps(d)``, bounded by
      ``primitives.RESPONSE_SNIPPET_BYTES``.
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

    #: The union of the fields the classmethods below set. See
    #: ``_ReasonedK3sException`` for why this is not left implicit.
    FIELDS = ('product', 'url', 'status_code', 'response_text',
              'response_snippet', 'release_channel')

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
    """Raised when a Shaken Fist agent operation finished without doing its work.

    Built by ``Cluster._agent_op_error()`` and raised by its three
    callers: ``Cluster.await_idle()``, ``Cluster.await_fetch()`` and
    ``Cluster.reap_execute()``.
    ``command_description`` is the value
    ``progress.describe_agent_op(aop, max_len=None)`` returns, and is
    only rendered when truthy. ``results``
    is the agent operation's results dict; when it is empty (or falsy) a
    fixed "no results were recorded" line is rendered instead of a JSON
    dump.

    ``command_description`` and ``results`` are redacted on the way in,
    not on the way out: an agent command carries a credential when the
    command needs one, the API echoes the command line back, and the
    agent's own stdout may repeat it. Redacting in the constructor means
    no raiser has to remember and nothing dangerous is ever stored, so a
    rendering site added later is covered by construction. See
    ``progress.redact_command_line()``.

    ``state`` is the operation state which brought us here, and is
    rendered only when it is not ``error``. That keeps the message byte
    for byte what it was for the case which has always raised this, while
    saying which ending it was for the one which did not. Why the states
    are distinguished at all, and why a wait enumerates them rather than
    naming two endings, is on the constants at the top of ``cluster.py``.
    """

    def __init__(self, instance_name, instance_uuid, operation_uuid,
                 command_description, results, state='error'):
        self.instance_name = instance_name
        self.instance_uuid = instance_uuid
        self.operation_uuid = operation_uuid
        self.command_description = progress.redact_command_line(
            command_description)
        self.results = progress.redact_structure(results)
        self.state = state
        super(AgentOperationError, self).__init__(instance_name)

    def __str__(self):
        lines = [
            'Agent operation failed!',
            '  instance: %s (uuid %s)' % (self.instance_name, self.instance_uuid),
            '  operation: %s' % self.operation_uuid,
        ]
        if self.state and self.state != 'error':
            lines.append('  state: %s' % self.state)
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
    ``AgentOperationError``. ``stdout`` and ``stderr`` are the strings
    from the agent operation's results; ``__str__`` re-joins each
    on its own prefixed line exactly as the original ``print()`` calls did.

    ``commandline``, ``stdout`` and ``stderr`` are redacted on the way in,
    for the reason ``AgentOperationError`` gives: this is the exception a
    failed k3s install raises, and the command line the API echoes back
    carries the cluster's node or server token.
    """

    def __init__(self, instance_name, instance_uuid, commandline, return_code, stdout, stderr):
        self.instance_name = instance_name
        self.instance_uuid = instance_uuid
        self.commandline = progress.redact_command_line(commandline)
        self.return_code = return_code
        self.stdout = progress.redact_command_line(stdout)
        self.stderr = progress.redact_command_line(stderr)
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


class KubeconfigError(_ReasonedK3sException):
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
    - ``unset_failed(config_elem, stderr)``: raised by
      ``Cluster.delete()`` when its ``kubectl config unset`` loop exits
      non-zero for one config element. This is a separate rendering from
      the two above -- it names a config element, not a config file path
      -- but it is still local-kubectl-state, so it lives on this class
      rather than on ``ClusterNotFoundError``. ``stderr`` is carried for
      the same reason as ``merge_failed()``'s: the loop captures the
      child's output, so kubectl's own account of why it failed reaches
      nobody unless the exception renders it.

    As with ``ReleaseLookupError``, the union of the fields the
    classmethods set is declared explicitly, so which attributes an
    instance answers to does not depend on which one built it.
    """

    #: The union of the fields the classmethods below set. See
    #: ``_ReasonedK3sException`` for why this is not left implicit.
    FIELDS = ('main_config_path', 'name', 'returncode', 'stderr',
              'config_elem')

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
    def unset_failed(cls, config_elem, stderr=None):
        lines = ['Could not unset kubectl config element %s' % config_elem]
        if stderr:
            lines.append(stderr)
        message = '\n'.join(lines)
        return cls('unset_failed', message, config_elem=config_elem, stderr=stderr)
