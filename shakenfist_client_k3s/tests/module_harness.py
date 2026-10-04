"""Run collection/plugins/modules/sf_k3s_cluster.py the way Ansible runs it.

This is not a test. It is the subprocess test_ansible_module.py drives, and
it exists because the single most important property of an Ansible module
is what lands on its file descriptor 1. Asserting that needs a process
whose fd 1 belongs to the module and to nothing else, which is also how
Ansible itself executes a module -- so running it in process, with
sys.stdout swapped for a StringIO, would test something other than the
thing that breaks.

Everything on the path from AnsibleModule through make_client(), Cluster,
Progress and CollectingReporter is the real implementation. Exactly two
things are replaced:

* shakenfist_client.apiclient.Client, by a MagicMock answering the calls
  health() and delete() make. There is no Shaken Fist to talk to, and a
  fake at the HTTP client boundary is the furthest out it can go while
  leaving every line of this package's own code real.

* Cluster.create(), optionally, by a stand-in which drives the real
  Progress through the real reporter -- phases, per item statuses, notes
  and a finish line -- and then either records the metadata a create would
  have written or raises. The true create() boots instances and installs
  k3s over tens of minutes against an API, which a MagicMock cannot stand
  in for; the stand-in reproduces the part this module's behaviour depends
  on, which is that create() emits a lot of output. Tests which do not
  need create() to succeed leave the real one in place, so that a failure
  inside it is a genuine one.

Beyond the module's own JSON on fd 1 the harness reports, on stderr, which
Cluster methods were called and which methods were called on the fake
client. That is what lets a test assert a negative -- "check mode called
nothing that mutates" -- rather than assert on a result which would look
the same either way.

Usage: module_harness.py <module path> <spec json path>

The spec keys are documented on _build_spec() below. A diagnostics line
beginning with DIAGNOSTIC_MARKER is written to stderr before the process
exits; everything else on stderr is whatever the module or Ansible put
there, and a traceback appearing there is a test failure rather than
something to parse.
"""
import importlib.util
import io
import json
import sys

import mock


DIAGNOSTIC_MARKER = '[harness-diagnostics] '

# The cluster whose metadata the fake client serves. Fixed here rather
# than taken from the spec so that the metadata key and the module's
# name parameter cannot drift apart in a test which only sets one.
CLUSTER_NAME = 'ci-runners'
CLUSTER_NAMESPACE = 'ci'

# Secrets planted in the metadata the fake client serves. delete() writes
# the whole metadata document to the reporter at debug level, so these
# reach the module's log return value -- and from there a play's
# registered variables -- the moment the reporter is verbose. The tests
# look for these exact strings in the module's stdout.
SECRET_NODE_TOKEN = 'SECRET-K3S-NODE-TOKEN'
SECRET_KUBECONFIG = 'SECRET-KUBECONFIG-CONTENTS'
SECRET_SSH_KEY = 'SECRET-SSH-PUBLIC-KEY'

# Every Cluster method which changes something -- on the cloud, in the
# namespace metadata, or on the calling machine. A check mode run must
# call none of them. Listed by name rather than detected, because
# "nothing that mutates was called" is only as good as this list and a
# reader has to be able to audit it.
MUTATING_CLUSTER_METHODS = (
    'allocate_metallb_addresses',
    'configure_metallb_addresses',
    'create',
    'create_and_await_instances',
    'create_instance',
    'delete',
    'delete_metadata',
    'execute_and_await',
    'expand_addresses',
    'expand_workers',
    'install_control_plane',
    'install_extra_control_plane',
    'install_k3s_component',
    'install_workers',
    'instance_os_update',
    'remove_worker',
    'set_metadata',
    'setup_longhorn',
    'setup_metallb',
    'update_os',
)

# And the same list one layer down, for the API client. A mutation which
# reached around Cluster -- or a Cluster method this file forgot to name
# above -- still has to come through one of these to change anything.
#
# instance_execute is deliberately not here. Running a command through the
# agent is how this package reads as well as how it writes: health()'s
# "kubectl get nodes" probe is an instance_execute, and a check mode run
# which reports on an existing cluster performs one. Classing it as
# mutating would make every read look like a write, which is worse than
# not listing it -- there is no path by which the module reaches an agent
# execute that changes something without first calling one of the Cluster
# methods above.
MUTATING_CLIENT_METHODS = (
    'create_instance',
    'create_network',
    'delete_instance',
    'delete_namespace_metadata_item',
    'delete_network',
    'route_network_address',
    'set_namespace_metadata_item',
    'unroute_network_address',
)


def healthy_metadata(worker_nodes=('worker-uuid-1',)):
    """The namespace metadata document of a cluster which finished building."""
    return {
        'name': CLUSTER_NAME,
        'namespace': CLUSTER_NAMESPACE,
        'type': 'k3s',
        'state': 'created',
        'node_serial': 1 + len(worker_nodes),
        'node_network': 'network-uuid',
        'node_token': SECRET_NODE_TOKEN,
        'kubeconfig': 'apiVersion: v1\n%s\n' % SECRET_KUBECONFIG,
        'ssh_key': 'ssh-rsa %s' % SECRET_SSH_KEY,
        'control_plane_nodes': ['cp-uuid-1'],
        'worker_nodes': list(worker_nodes),
        'routed_addresses': ['10.0.0.5'],
        'metallb_installed': True,
        'longhorn_installed': True,
    }


def _build_spec(path):
    """Read the scenario description.

    Keys, all optional except params:

    params             the ANSIBLE_MODULE_ARGS dict the controller would
                       have passed.
    cluster_exists     whether the fake client serves metadata for the
                       cluster the module asks about.
    cluster_state      the metadata's own state field, 'created' for a
                       cluster which finished building.
    worker_nodes       how many worker instance uuids the metadata lists,
                       which is how a cluster whose size differs from
                       initial_workers is set up.
    instance_state     the state the fake client reports for every
                       instance, 'created' normally and 'deleted' for a
                       delete which should finish.
    unconfigured       whether constructing a client raises
                       apiclient.UnconfiguredException, which is what
                       discovery finding nothing looks like.
    fake_create        whether Cluster.create() is replaced by the
                       progress emitting stand-in. False leaves the real
                       create() in place.
    create_raises      with fake_create, raise a real SshKeyError from
                       inside the stand-in after it has emitted progress.
                       That is where create() really reads the ssh key:
                       after the name and the network are settled, so
                       there is already output to lose.
    """
    spec = json.load(io.open(path, encoding='utf-8'))
    spec.setdefault('cluster_exists', False)
    spec.setdefault('cluster_state', 'created')
    spec.setdefault('worker_nodes', 1)
    spec.setdefault('instance_state', 'created')
    spec.setdefault('unconfigured', False)
    spec.setdefault('fake_create', False)
    spec.setdefault('create_raises', False)
    return spec


def build_fake_client(spec):
    """A MagicMock which answers what health() and delete() ask it."""
    from shakenfist_client_k3s import cluster as cluster_module

    namespace_md = {}
    if spec['cluster_exists']:
        md = healthy_metadata(
            worker_nodes=['worker-uuid-%d' % n
                          for n in range(spec['worker_nodes'])])
        md['state'] = spec['cluster_state']
        namespace_md[cluster_module.METADATA_KEY % CLUSTER_NAME] = md

    client = mock.MagicMock()
    client.get_namespace_metadata.return_value = namespace_md
    client.get_instance.return_value = {
        'name': 'k3s-%s-node-001' % CLUSTER_NAME,
        'state': spec['instance_state'],
        'agent_state': 'ready',
    }
    # One completed agent operation, which is what health()'s kubectl probe
    # of the first control plane node waits for.
    operation = {
        'uuid': 'agent-op-uuid',
        'state': 'complete',
        'results': {'0': {'return-code': 0,
                          'stdout': 'NAME  STATUS\ncp-1  Ready\n',
                          'stderr': ''}},
    }
    client.instance_execute.return_value = operation
    client.get_agent_operation.return_value = operation
    return client


def install_fake_create(spec, created_kwargs):
    """Replace Cluster.create() with a stand-in which emits real progress."""
    from shakenfist_client_k3s import cluster as cluster_module
    from shakenfist_client_k3s import exceptions

    def fake_create(self, **kwargs):
        created_kwargs.update(kwargs)
        progress = self.get_progress(total_phases=2)
        progress.phase('Creating node network')
        progress.note('created k3s-%s-node (uuid network-uuid)' % self.name)
        progress.update('k3s-%s-node' % self.name, 'state creating')
        progress.phase('Installing k3s on control plane nodes')
        if spec['create_raises']:
            # A real exception object from a real factory, raised from the
            # point in create() where create() raises it: the ssh key is
            # read once the network exists, so by here a caller has already
            # been told about two phases they are about to lose.
            raise exceptions.SshKeyError.unreadable(
                '/no/such/id_rsa.pub', 'No such file or directory')
        progress.finish('Cluster %s is ready' % self.name)
        self._metadata[self._metadata_key()] = healthy_metadata()

    cluster_module.Cluster.create = fake_create


def record_mutating_methods(calls):
    """Wrap every mutating Cluster method so that calls to it are reported."""
    from shakenfist_client_k3s import cluster as cluster_module

    def wrap(name, original):
        def wrapper(self, *args, **kwargs):
            calls.append(name)
            return original(self, *args, **kwargs)
        return wrapper

    for name in MUTATING_CLUSTER_METHODS:
        original = getattr(cluster_module.Cluster, name)
        setattr(cluster_module.Cluster, name, wrap(name, original))


def main():
    module_path = sys.argv[1]
    spec = _build_spec(sys.argv[2])

    spec_module = importlib.util.spec_from_file_location(
        'sf_k3s_cluster_under_test', module_path)
    module = importlib.util.module_from_spec(spec_module)
    spec_module.loader.exec_module(module)

    created_kwargs = {}
    if spec['fake_create']:
        install_fake_create(spec, created_kwargs)
    # After install_fake_create(), so that a call to the stand-in is
    # recorded too.
    cluster_calls = []
    record_mutating_methods(cluster_calls)

    client = build_fake_client(spec)

    from shakenfist_client import apiclient

    def client_factory(**kwargs):
        if spec['unconfigured']:
            raise apiclient.UnconfiguredException(
                'no Shaken Fist configuration could be found')
        return client

    # How a controller hands a module its parameters. ansible-core reads
    # them from _ANSIBLE_ARGS when it is set and only falls back to parsing
    # argv or stdin when it is not, which is what makes it possible to
    # drive a module without writing an argument file.
    from ansible.module_utils import basic
    basic._ANSIBLE_ARGS = json.dumps(
        {'ANSIBLE_MODULE_ARGS': spec['params']}).encode('utf-8')
    if hasattr(basic, '_ANSIBLE_PROFILE'):
        basic._ANSIBLE_PROFILE = 'legacy'

    code = 0
    try:
        with mock.patch.object(apiclient, 'Client', client_factory):
            module.main()
    except SystemExit as e:
        code = e.code
    finally:
        client_calls = sorted(
            {name for name in MUTATING_CLIENT_METHODS
             if getattr(client, name).called})
        # Written to stderr, because stdout is the module's and the whole
        # point of running this out of process is that nothing else writes
        # there.
        sys.stderr.write('%s%s\n' % (DIAGNOSTIC_MARKER, json.dumps({
            'exit_code': code,
            'cluster_calls': sorted(set(cluster_calls)),
            'client_calls': client_calls,
            'create_kwargs': {k: repr(v) for k, v in created_kwargs.items()},
        })))

    sys.exit(code)


if __name__ == '__main__':
    main()
