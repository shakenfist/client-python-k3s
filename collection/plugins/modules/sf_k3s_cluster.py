# Copyright 2019 Michael Still and contributors
#
# A native Ansible module for orchestrating k3s clusters on Shaken Fist. The
# orchestration itself lives in shakenfist_client_k3s.cluster.Cluster, which
# the sf-client command line drives through the same verbs; this module is a
# thin translation between Ansible's argument spec and those verbs, and it
# deliberately contains no orchestration of its own.
#
# Two things about it are worth knowing before changing it.
#
# First, a module's stdout is its JSON result. Ansible parses file
# descriptor 1 and nothing else, so a single stray write corrupts the result
# of an otherwise successful run. Every line this module's work would print
# is routed into a progress.CollectingReporter and handed back as the `log`
# return value instead -- that reporter exists for this caller, and
# Cluster's reporter parameter exists so that it can be injected.
#
# Second, this module manages existence, not size. It has no worker count
# parameter beyond the one creation needs (`initial_workers`), and nothing
# here reconciles the shape of a cluster which already exists. Worker
# membership has another owner; two writers to one namespace metadata
# document is the race that would create.
from __future__ import annotations

import traceback

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.basic import missing_required_lib

# Guarded, because the likeliest first-run failure is a control node which
# installed the collection and not the plugin. Galaxy cannot install a
# Python package, so "ansible-galaxy collection install shakenfist.k3s"
# leaves this import unsatisfiable until someone also runs the pip install
# that collection/requirements.txt names -- and an unguarded import turns
# that into a ModuleNotFoundError traceback inside MODULE FAILURE, which
# says what Python could not find rather than what the operator should do.
# missing_required_lib() says the latter. The traceback is kept and handed
# back under "exception" so a genuinely broken install is still
# diagnosable, which is the whole reason for capturing it here rather than
# discarding it.
#
# requests is imported for the same reason it is caught below: it is
# shakenfist_client's transport, so it is present whenever that is, and
# naming it here keeps the failure for a half-installed environment in one
# place.
HAS_SF_K3S = True
SF_K3S_IMPORT_ERROR = None
try:
    import requests

    from shakenfist_client import apiclient
    from shakenfist_client_k3s import client as sf_client
    from shakenfist_client_k3s import cluster as sf_cluster
    from shakenfist_client_k3s import exceptions as sf_exceptions
    from shakenfist_client_k3s import progress as sf_progress
except ImportError:
    HAS_SF_K3S = False
    SF_K3S_IMPORT_ERROR = traceback.format_exc()


DOCUMENTATION = r"""
---
module: sf_k3s_cluster
short_description: Create and delete k3s clusters on Shaken Fist.
description:
  - Idempotently ensure a k3s cluster is present or absent in a Shaken Fist
    namespace, building it out of Shaken Fist instances and a Shaken Fist
    network.
  - Imports the C(shakenfist_client_k3s) plugin and talks to the Shaken Fist
    REST API directly, so the machine this module runs on needs
    C(pip install shakenfist_client_k3s) and nothing else -- no Shaken Fist
    server package, and no kubectl. That machine is usually the control
    node, reached with C(delegate_to) or a local connection, and O(sshkey)
    and O(manifests) are read there rather than on the cluster.
  - This module manages existence, not size. A cluster which already exists
    is reported as it is found and is never reshaped - none of the sizing
    parameters below is reconciled against a cluster that is already there,
    and in particular the module never adds or removes worker nodes. See
    O(initial_workers) for why.
  - Creating a cluster takes tens of minutes, because it boots instances,
    waits for their agents, installs k3s on each of them and then installs
    metallb and Longhorn. The task blocks for all of it. Deleting one is
    quicker but still waits for every instance to reach the deleted state.
options:
  name:
    description:
      - The name of the cluster. Cluster names are unique within
        O(namespace), and the cluster's state is stored in that namespace's
        metadata under a key derived from this name.
    required: true
    type: str
  namespace:
    description:
      - The Shaken Fist namespace the cluster lives in. The namespace must
        already exist; this module does not create it, and C(sf_namespace)
        from the C(shakenfist.shakenfist) collection is how a play usually
        does.
      - This is B(not) the identity the module authenticates as, which is
        O(auth_namespace). That differs from every module in the
        C(shakenfist.shakenfist) collection except C(sf_claim), where
        O(namespace) names the identity - so a reader arriving from those
        modules should not assume it does here. A k3s cluster is told
        separately which namespace it lives in and which credentials to use,
        because an administrator routinely builds a cluster in a namespace
        whose key they do not hold, so the two are two parameters.
    required: true
    type: str
  state:
    description:
      - Whether the cluster should be present or absent.
      - V(present) creates a cluster which does not exist and leaves one
        which does alone. V(absent) destroys the cluster, its instances and
        the node network, and works on a cluster an earlier run interrupted
        half way through building - which is the only way to clear one.
    required: false
    default: present
    choices: [present, absent]
    type: str
  initial_workers:
    description:
      - How many worker nodes to give the cluster B(when this module creates
        it). Used on creation and never again, which is what the name is
        saying.
      - An existing cluster whose worker count differs from this is not a
        change, is not reported as one, and is not resized. Worker
        membership belongs to whatever scales the cluster - a CI conductor,
        typically, adding and removing runners as load demands - and this
        module owns only "a cluster of at least this shape exists". Both
        writing to one namespace metadata document is the race that a
        reconciling worker count parameter would be.
      - The default of 0 builds a cluster with control plane nodes only,
        which is what a play that hands the cluster straight to such a
        scaler wants. It is deliberately not the command line's default of
        2.
    required: false
    default: 0
    type: int
  control_plane_count:
    description:
      - How many control plane nodes to give the cluster when this module
        creates it. Like O(initial_workers), this is a creation parameter
        and is not reconciled - there is no verb which adds a control plane
        node to a built cluster.
    required: false
    default: 1
    type: int
  metal_address_count:
    description:
      - How many floating addresses to route into the cluster's network for
        metallb to manage, when this module creates the cluster. Accepted
        and ignored when O(install_metallb) is V(false).
    required: false
    default: 5
    type: int
  network:
    description:
      - The name or UUID of an existing Shaken Fist network to attach the
        cluster's nodes to. When omitted a network is created for the
        cluster.
      - A borrowed network is destroyed along with the cluster, which is
        shakenfist/client-python-k3s#41. Until that is fixed, do not point
        two clusters at one network.
    required: false
    type: str
  release_channel:
    description:
      - The k3s release channel to install from. Common choices are
        V(stable), V(latest), and a minor version prefixed with C(v) such as
        V(v1.26).
    required: false
    default: stable
    type: str
  sshkey:
    description:
      - Path to an SSH public key to place on the cluster's instances, read
        from the machine this module runs on. No key is installed when this
        is omitted; the Shaken Fist agent, not SSH, is how the orchestration
        reaches the nodes, so a cluster without one is fully functional.
    required: false
    type: path
  install_metallb:
    description:
      - Whether to install metallb, so that Kubernetes services of type
        C(LoadBalancer) get an address. Recorded against the cluster, so
        later operations know whether it is there.
    required: false
    default: true
    type: bool
  install_longhorn:
    description:
      - Whether to install Longhorn, so that the cluster has a default
        storage class for persistent volumes.
    required: false
    default: true
    type: bool
  manifests:
    description:
      - Paths to local C(.yaml), C(.yml) or C(.json) manifests, read from
        the machine this module runs on and written verbatim into the k3s
        auto-apply directory on the first control plane node before k3s is
        installed, so that k3s applies them as it first starts.
      - Nothing in them is templated and the order they are applied in is
        k3s's business rather than this module's. A payload needing either
        belongs in a chart installed afterwards.
    required: false
    type: list
    elements: path
  api_url:
    description:
      - Base URL of the Shaken Fist API (for example
        C(http://sf-1:13000)). This, O(auth_namespace) and O(key) are all
        supplied together or not at all, and supplying only some of them is
        an error rather than a silent fall back - the values given would
        otherwise be discarded and the module pointed at whatever cloud
        discovery found. When all three are omitted the module
        auto-discovers credentials from the environment and
        C(sfrc)/C(~/.shakenfist)/C(/etc/sf/shakenfist.json) exactly like the
        C(sf-client) CLI, which is the usual way to call this.
    required: false
    type: str
  auth_namespace:
    description:
      - The namespace to authenticate as, which is not O(namespace) - see
        there. Building a cluster in someone else's namespace is an
        administrator operation, so this is normally C(system). See
        O(api_url) for the all-or-nothing rule it shares with O(api_url) and
        O(key).
    required: false
    type: str
  key:
    description: The authentication key for O(auth_namespace). See O(api_url).
    required: false
    type: str
    no_log: true
author:
  - Michael Still and contributors
"""

EXAMPLES = r"""
- name: Ensure a CI runner cluster exists, with workers left to the conductor
  shakenfist.k3s.sf_k3s_cluster:
    name: ci-runners
    namespace: ci
    state: present
    control_plane_count: 1
    initial_workers: 0
  delegate_to: localhost
  register: cluster

- name: Fail the play if the cluster is not healthy
  ansible.builtin.assert:
    that: cluster.health.healthy
    fail_msg: "{{ cluster.health }}"

- name: Create a three node cluster with no metallb, as an administrator
  shakenfist.k3s.sf_k3s_cluster:
    name: scratch
    namespace: testing
    initial_workers: 2
    install_metallb: false
    api_url: http://sf-1:13000
    auth_namespace: system
    key: "{{ sf_system_key }}"
  delegate_to: localhost

- name: Create a cluster with a manifest k3s applies as it starts
  shakenfist.k3s.sf_k3s_cluster:
    name: preloaded
    namespace: ci
    manifests:
      - files/ingress-nginx.yaml
  delegate_to: localhost

- name: Report what would change, without changing it
  shakenfist.k3s.sf_k3s_cluster:
    name: ci-runners
    namespace: ci
  check_mode: true
  delegate_to: localhost

- name: Destroy the cluster
  shakenfist.k3s.sf_k3s_cluster:
    name: ci-runners
    namespace: ci
    state: absent
  delegate_to: localhost
"""

RETURN = r"""
changed:
  description:
    - Whether the module changed the cluster. True only when a cluster was
      created or destroyed (or, in check mode, would have been).
    - A cluster which already exists is never a change, whatever its
      current size, because this module does not reshape one.
  returned: always
  type: bool
failed:
  description: Whether the module failed.
  returned: always
  type: bool
health:
  description:
    - The cluster's health report, as C(sf-client k3s health) renders it. A
      play can branch on this without parsing anything.
    - Returned when O(state=present) and the module reached a real cluster -
      after a create, and for a cluster which already existed. It is
      C(none) when there is no cluster to report on, and also for
      O(state=absent), where the module does not probe a cluster it is about
      to destroy.
    - Obtaining it submits one read-only agent operation against the first
      control plane node, to run C(kubectl get nodes) there.
    - There is deliberately no return value carrying the cluster's raw
      namespace metadata, because that document holds the cluster's
      kubeconfig, its k3s node token and any SSH key it was built with.
      Fetch credentials with C(sf-client k3s getconfig) instead, where they
      do not pass through a play's registered variables and logs.
  returned: when the cluster exists and state is present
  type: dict
  contains:
    name:
      description: The cluster's name.
      type: str
    namespace:
      description: The namespace the cluster lives in.
      type: str
    state:
      description:
        - The cluster's own state, C(created) for a cluster which finished
          being built.
      type: str
    interrupted:
      description:
        - Whether the cluster never finished being built. Always false here,
          because O(state=present) against an interrupted cluster fails
          rather than returning.
      type: bool
    nodes:
      description:
        - One entry per node the metadata names, control plane nodes first,
          each giving the instance UUID, the role, the instance and agent
          states, and whether that node is healthy.
      type: list
      elements: dict
    api:
      description:
        - Whether the k3s API answered, and the output of the
          C(kubectl get nodes) run against it. C(probed) false means the
          probe was not attempted, and C(error) says why.
      type: dict
    healthy:
      description:
        - The conjunction of everything above - the cluster finished being
          built, every node exists and is up, and the k3s API answered.
      type: bool
log:
  description:
    - The progress output the orchestration would have printed, one element
      per line, collected rather than written to stdout. For debugging a
      create which failed half way, mostly.
  returned: always
  type: list
  elements: str
"""


def _present(module, cluster, reporter):
    """Ensure the cluster exists, and report whether that took a change."""
    # get_metadata() rather than health() for the existence decision: this
    # is a single namespace metadata read, which is the cheap check decision
    # 6 of the phase 5 plan promised, and it caches its answer -- including
    # a miss -- so the health() calls below cost no second read.
    if cluster.get_metadata() is None:
        if module.check_mode:
            # Nothing above this point mutated anything, and nothing below
            # it runs: a check mode run of a play which would build a
            # cluster must not build one.
            module.exit_json(changed=True, health=None, log=reporter.lines)

        # The line below is the whole of this module's involvement with
        # worker counts: initial_workers becomes the count create() needs,
        # on the one path which creates a cluster. Do not grow a branch
        # which compares it against a cluster that already exists -- see the
        # DOCUMENTATION for initial_workers for what that would race with.
        # It is also deliberately the only occurrence of that argument's
        # name anywhere under collection/, which the phase 5 plan's done
        # criteria check for exactly this reason.
        #
        # Two of create()'s parameters are deliberately left at their
        # library defaults rather than exposed. write_kubeconfig stays False
        # because ~/.kube/config on whichever machine ran this module is not
        # part of the cluster, and a module which rewrites it has reached
        # outside its own business; refresh_version_cache stays False
        # because a cache refresh is a thing a human asks for once, not a
        # property of the cluster a play is declaring.
        cluster.create(
            control_plane_count=module.params['control_plane_count'],
            worker_count=module.params['initial_workers'],
            metal_address_count=module.params['metal_address_count'],
            network=module.params['network'],
            release_channel=module.params['release_channel'],
            sshkey=module.params['sshkey'],
            install_metallb=module.params['install_metallb'],
            install_longhorn=module.params['install_longhorn'],
            manifests=module.params['manifests'])
        module.exit_json(
            changed=True, health=cluster.health(), log=reporter.lines)

    # The cluster exists. health() is what says whether it is a cluster at
    # all: it returns structure rather than text precisely so that this
    # module can branch on it, and it is the only public way to learn that a
    # cluster never finished being built.
    report = cluster.health()
    if report['interrupted']:
        module.fail_json(
            msg=('Cluster %s is in state %s rather than created: an earlier '
                 'run was interrupted while building it, so its nodes, '
                 'tokens and kubeconfig are in an unknown combination of '
                 'present and absent and it cannot be used. It is neither '
                 'absent nor present at the shape you asked for. Remove it '
                 "with state: absent and then run this task again."
                 % (cluster.name, report['state'])),
            health=report, log=reporter.lines)

    # Present, and finished. Not a change, whatever size it is: existence is
    # the whole of what this module reconciles.
    module.exit_json(changed=False, health=report, log=reporter.lines)


def _absent(module, cluster, reporter):
    """Ensure the cluster does not exist, and report whether that took a change."""
    if cluster.get_metadata() is None:
        module.exit_json(changed=False, health=None, log=reporter.lines)

    if module.check_mode:
        module.exit_json(changed=True, health=None, log=reporter.lines)

    # update_kubeconfig is left at its default of False for the same reason
    # create()'s write_kubeconfig is: the kubectl configuration of the
    # machine which happened to run this module is not part of the cluster,
    # and editing it would also shell out to kubectl, which this module does
    # not require to be installed.
    cluster.delete()
    module.exit_json(changed=True, health=None, log=reporter.lines)


def run_module():
    argument_spec = {
        'name': {'required': True, 'type': 'str'},
        'namespace': {'required': True, 'type': 'str'},
        'state': {
            'default': 'present',
            'choices': ['present', 'absent'],
            'type': 'str',
        },

        # The shape of a cluster this module creates. Every one of these is
        # read only on the creation path; there is no reconciliation of any
        # of them, and adding one is a design change rather than a feature.
        'initial_workers': {'default': 0, 'type': 'int'},
        'control_plane_count': {'default': 1, 'type': 'int'},
        'metal_address_count': {'default': 5, 'type': 'int'},
        'network': {'required': False, 'type': 'str'},
        'release_channel': {'default': 'stable', 'type': 'str'},
        'sshkey': {'required': False, 'type': 'path'},
        'install_metallb': {'default': True, 'type': 'bool'},
        'install_longhorn': {'default': True, 'type': 'bool'},
        'manifests': {'required': False, 'type': 'list', 'elements': 'path'},

        # The connection, declared here rather than imported from a shared
        # module_utils. This collection ships one module, so a shared
        # fragment would be a copy of the shakenfist.shakenfist
        # collection's sf_connection.py -- and that file's own header
        # records shakenfist/shakenfist#4314, where per-module copies of
        # exactly this spec diverged. The all-or-nothing rule over the three
        # is enforced by make_client() below rather than restated here, so
        # there is one definition of it in one place.
        #
        # The identity is auth_namespace because namespace above already
        # names the namespace the cluster lives in, which is the same
        # collision sf_claim has and the same answer it gives.
        'api_url': {'required': False, 'type': 'str'},
        'auth_namespace': {'required': False, 'type': 'str'},
        'key': {'required': False, 'type': 'str', 'no_log': True},
    }

    module = AnsibleModule(
        argument_spec=argument_spec, supports_check_mode=True)

    # Checked immediately after AnsibleModule exists and before anything
    # else, because fail_json() is the only way to say this in a form
    # Ansible renders as a message rather than a crash, and it needs the
    # module object. Nothing above this point touches the guarded names.
    if not HAS_SF_K3S:
        module.fail_json(
            msg=missing_required_lib(
                'shakenfist_client_k3s',
                reason=('on the Ansible control node. The collection cannot '
                        'install it: Galaxy ships Ansible content and not '
                        'Python packages, so the pip install in '
                        'requirements.txt is a separate step')),
            exception=SF_K3S_IMPORT_ERROR)

    # Where the orchestration's output goes. A Reporter is file like and
    # Progress writes to it as a stream, so this one object catches
    # everything the library would otherwise have printed, and stdout -- the
    # file descriptor Ansible reads this module's JSON result from -- is
    # left to exit_json() alone.
    #
    # verbose is left False, and that is a security property rather than a
    # volume preference: delete() writes the whole cluster metadata document
    # at debug level, which includes the kubeconfig, the k3s node token and
    # any SSH key the cluster was built with. Turning verbose on here would
    # put all three into the log return value, and from there into the
    # play's registered variables and whatever logs them.
    reporter = sf_progress.CollectingReporter()

    # The client is built before anything branches, so that a connection
    # error is reported on every path -- including the check mode paths,
    # which would otherwise tell a play it would have succeeded with
    # credentials the real run refuses.
    try:
        client = sf_client.make_client(
            api_url=module.params['api_url'],
            namespace=module.params['auth_namespace'],
            key=module.params['key'])
    except ValueError as e:
        # make_client()'s message names which of the three were supplied, so
        # it is passed through rather than rewritten. It calls the identity
        # "namespace", because that is what its own parameter is called; the
        # parenthetical is the whole of the translation this module owes.
        module.fail_json(
            msg=('%s (this module spells make_client()\'s namespace '
                 'auth_namespace, because namespace names the cluster\'s '
                 'own namespace here)' % e),
            health=None, log=reporter.lines)
    except apiclient.UnconfiguredException as e:
        # make_client() deliberately does not catch this, so that a library
        # caller keeps the exception. Translating it is this module's job.
        module.fail_json(
            msg=('Could not configure the Shaken Fist client: %s. No '
                 'connection parameters were given, so credentials were '
                 'looked for in the environment and in '
                 'sfrc/~/.shakenfist//etc/sf/shakenfist.json, and none were '
                 'found.' % e),
            health=None, log=reporter.lines)
    except (apiclient.APIException,
            requests.exceptions.RequestException) as e:
        # Constructing a client is not a local operation, which is easy to
        # miss and is why these two were not caught here at first: the
        # review of #90 found the orchestration below unprotected, and
        # sweeping for the same shape found this site as well.
        # apiclient.Client.__init__ calls _collect_capabilities(), which
        # does a GET against base_url before the constructor returns. So an
        # api_url with a typo in it, a host which is down, DNS which does
        # not resolve or a TLS failure all raise here -- before any
        # orchestration, on what is very likely a first run with a
        # misconfigured inventory. That makes this the more probable of the
        # two sites, not the lesser one.
        module.fail_json(
            msg=('Could not reach the Shaken Fist API at %s: %s. The client '
                 'checks the API is there while it is being built, so this '
                 'failed before any cluster work started -- nothing has '
                 'been created.'
                 % (module.params['api_url'] or 'the discovered api_url', e)),
            health=None, log=reporter.lines)

    cluster = sf_cluster.Cluster(
        client, module.params['name'], module.params['namespace'],
        reporter=reporter)

    # Every exception the library raises for a cluster-shaped problem
    # derives from K3sClusterException and renders itself as the sentence
    # the command line prints, so one handler turns the lot into a message
    # and a log rather than a traceback Ansible reports as a module failure
    # with the collected progress thrown away. exit_json() and fail_json()
    # raise SystemExit, which is not an Exception, so they pass through this
    # untouched.
    #
    # K3sClusterException alone is not enough, which the review of #90
    # pointed out and cluster.py confirms: it catches
    # apiclient.APIException at particular call sites (:761, :2203, :2250)
    # precisely because the client raises it unwrapped, so every other call
    # through the client can hand one straight out. An UnauthorizedException
    # from the first get_namespace_metadata(), a namespace which does not
    # exist, a connection dropped twenty minutes into a create, or a
    # requests error from the GitHub release lookups in primitives.py were
    # all reaching Ansible as a traceback -- and discarding the collected
    # log, which is the one thing a half-finished create leaves behind
    # worth reading. requests.exceptions.RequestException is the transport
    # layer under apiclient; APIException does not derive from it.
    try:
        if module.params['state'] == 'present':
            _present(module, cluster, reporter)
        else:
            _absent(module, cluster, reporter)
    except sf_exceptions.K3sClusterException as e:
        module.fail_json(msg=str(e), health=None, log=reporter.lines)
    except apiclient.APIException as e:
        module.fail_json(
            msg=('The Shaken Fist API refused a request: %s. The cluster may '
                 'be partly built; "state: absent" removes whatever exists.'
                 % e),
            health=None, log=reporter.lines)
    except requests.exceptions.RequestException as e:
        module.fail_json(
            msg=('Could not reach the Shaken Fist API: %s. The cluster may '
                 'be partly built; "state: absent" removes whatever exists.'
                 % e),
            health=None, log=reporter.lines)


def main():
    run_module()


if __name__ == '__main__':
    main()
