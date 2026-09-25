"""Scripted Shaken Fist clients, good enough to drive a cluster end to end.

A bare ``mock.MagicMock`` cannot do this job. The orchestration's wait
loops compare dictionary values against literals -- ``'created'``,
``'ready'``, ``'complete'``, ``'deleted'`` -- and a MagicMock's attributes
and subscripts compare equal to none of them, so every wait spins forever.
This fake returns the shapes the orchestration actually reads instead, and
lives here rather than in one test module because both the CLI level
create smoke test and the library level lifecycle test need it. The same
goes for HealthClient below, which the library and command line tests for
the health verb both drive.
"""

from shakenfist_client import apiclient


def not_found(instance_uuid):
    """Build the exception the API client raises for an instance which is gone."""
    return apiclient.ResourceNotFoundException(
        'instance not found', 'GET', '/instances/%s' % instance_uuid, 404,
        'instance not found')


# A minimal kubeconfig in the shape k3s writes, pointing at the loopback
# address the way the real file does before create rewrites it.
KUBECONFIG = """apiVersion: v1
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
kind: Config
users:
- name: default
  user:
    token: banana
"""


class FakeClusterClient:
    """Enough of the sf-client API surface for a whole cluster lifecycle.

    Instances boot instantly, every agent operation completes successfully
    at submission, file fetches return canned content, and deleting an
    instance moves it straight to the deleted state so delete's wait loop
    terminates.
    """

    def __init__(self):
        self.namespace = 'testns'
        self.metadata = {}
        self.instances = {}
        self.instance_serial = 0
        self.instance_sshkeys = []
        self.aop_serial = 0
        self.routed_serial = 0

        # Every command the client was asked to run, in the order it was
        # asked, so a test can assert both what ran and what ran before
        # what. Ordering claims -- a file written before the installer
        # which reads it, a node drained before its instance is destroyed
        # -- cannot be made from separate per-call lists, because neither
        # knows where in the other its own calls fell.
        self.executed = []

        # What the caller asked us to destroy, so a test can assert on the
        # teardown as well as the build.
        self.deleted_networks = []
        self.unrouted_addresses = []

    def get_namespace(self, namespace):
        return {'name': namespace}

    def create_namespace(self, namespace):
        return {'name': namespace}

    def get_namespace_metadata(self, namespace):
        return dict(self.metadata)

    def set_namespace_metadata_item(self, namespace, key, value):
        self.metadata[key] = value

    def delete_namespace_metadata_item(self, namespace, key):
        self.metadata.pop(key, None)

    def allocate_network(self, netblock, provide_dhcp, provide_nat, name,
                         namespace=None):
        return {'uuid': 'net-1', 'name': name, 'state': 'created'}

    def get_network(self, network_ref):
        return {'uuid': 'net-1', 'name': 'k3s-banana-node', 'state': 'created'}

    def delete_network(self, network_ref):
        self.deleted_networks.append(network_ref)

    def create_instance(self, name, cpus, memory, networks, disks, sshkey,
                        userdata, side_channels=None, namespace=None):
        # Recorded beside the instance rather than in it, because the real
        # API does not return the key in an instance representation and a
        # fake which did would let a test assert on a field that does not
        # exist.
        self.instance_sshkeys.append(sshkey)
        self.instance_serial += 1
        instance_uuid = 'inst-%03d' % self.instance_serial
        self.instances[instance_uuid] = {
            'uuid': instance_uuid, 'name': name, 'state': 'created',
            'agent_state': 'ready'}
        return self.instances[instance_uuid]

    def get_instance(self, instance_ref):
        # The API client's exception, not a KeyError. Three places in the
        # orchestration catch ResourceNotFoundException for an instance the
        # metadata names which somebody has deleted out from under it --
        # delete(), _node_health() and remove_worker() -- and a fake which
        # raises KeyError instead cannot exercise any of them, which is how
        # a test asserting that behaviour passes for the wrong reason.
        if instance_ref not in self.instances:
            raise not_found(instance_ref)
        return self.instances[instance_ref]

    def delete_instance(self, instance_ref):
        self.instances[instance_ref]['state'] = 'deleted'

    def get_instance_interfaces(self, instance_ref):
        return [{'ipv4': '10.0.0.4', 'floating': '192.168.10.100'}]

    def get_instance_agentoperations(self, instance_ref, all=False):
        return []

    def get_agent_operation(self, operation_uuid):
        # The wait loops re-read an operation until it leaves its pending
        # states. Everything this fake hands out is already complete, so a
        # re-read is only reached by a test which built a pending operation
        # itself; such a test overrides this.
        return {'uuid': operation_uuid, 'state': 'complete', 'results': {}}

    def _complete_aop(self, instance_ref, commands, results):
        self.aop_serial += 1
        return {
            'uuid': 'aop-%03d' % self.aop_serial,
            'instance_uuid': instance_ref,
            'state': 'complete',
            'commands': commands,
            'results': results
        }

    def instance_execute(self, instance_ref, commandline):
        self.executed.append((instance_ref, commandline))
        return self._complete_aop(
            instance_ref,
            [{'command': 'execute', 'commandline': commandline}],
            {'0': {'return-code': 0, 'stdout': '', 'stderr': ''}})

    def instance_get(self, instance_ref, path):
        return self._complete_aop(
            instance_ref,
            [{'command': 'get-file', 'path': path}],
            {'0': {'content_blob': path}})

    def get_blob_data(self, blob_uuid):
        if blob_uuid.endswith('k3s.yaml'):
            yield KUBECONFIG.encode('utf-8')
        else:
            yield b'not-a-real-token\n'

    def route_network_address(self, network_uuid):
        self.routed_serial += 1
        return '192.168.10.%d' % self.routed_serial

    def unroute_network_address(self, network_uuid, address):
        self.unrouted_addresses.append((network_uuid, address))


class HealthClient(FakeClusterClient):
    """A scripted client which can be made unwell in each of the ways health() reports.

    The stock fake cannot express any of them: every instance it knows
    about is created with its agent ready and every agent command it is
    given completes with a return code of zero. A health check whose entire
    purpose is reporting bad news needs a client which can deliver some.
    """

    def __init__(self):
        super(HealthClient, self).__init__()

        # Every mutation the client was asked to make. health() must make
        # none of them, and the calls have to be recorded rather than their
        # effect inspected: a metadata write which writes back the same
        # dictionary the cache is already holding leaves the stored
        # document comparing equal to what it was, so only the call itself
        # shows that the write happened at all.
        self.metadata_writes = []
        self.metadata_deletes = []
        self.deleted_instances = []

        # What the kubectl probe does. Between them these cover the three
        # ways it can fail: the command runs and exits non-zero, the agent
        # operation itself errors, and the API refuses to accept the
        # command at all.
        self.probe_return_code = 0
        self.probe_stdout = (
            'NAME                  STATUS   ROLES\n'
            'k3s-banana-node-001   Ready    control-plane\n')
        self.probe_stderr = ''
        self.probe_state = 'complete'
        self.probe_raises = None

        # How many times the operation may be re-read before this fake
        # decides the wait is not going to end. A wait which does not end
        # is the bug health() had, so a test for it must fail rather than
        # hang: a hung suite names no test, and nobody reads a run which
        # did not finish. The correct code reads a pending operation once
        # per second up to its timeout, which is well inside this.
        self.agent_operation_reads = 0
        self.max_agent_operation_reads = 60

    def set_namespace_metadata_item(self, namespace, key, value):
        self.metadata_writes.append(key)
        return super(HealthClient, self).set_namespace_metadata_item(
            namespace, key, value)

    def delete_namespace_metadata_item(self, namespace, key):
        self.metadata_deletes.append(key)
        return super(HealthClient, self).delete_namespace_metadata_item(
            namespace, key)

    def delete_instance(self, instance_ref):
        self.deleted_instances.append(instance_ref)
        return super(HealthClient, self).delete_instance(instance_ref)

    def get_agent_operation(self, operation_uuid):
        # A probe which is still pending stays pending: this is the node
        # whose agent is not connected, where the operation is accepted and
        # then never runs. A test which wants the wait to end sets
        # probe_state to a terminal state.
        self.agent_operation_reads += 1
        if self.agent_operation_reads > self.max_agent_operation_reads:
            raise AssertionError(
                'the wait re-read the agent operation %d times without '
                'ending. The operation is %r, which is not a state it can '
                'leave, so whatever is waiting on it is waiting forever.'
                % (self.agent_operation_reads, self.probe_state))
        return {
            'uuid': operation_uuid,
            'instance_uuid': None,
            'state': self.probe_state,
            'commands': [],
            'results': {'0': {'return-code': self.probe_return_code,
                              'stdout': self.probe_stdout,
                              'stderr': self.probe_stderr}}
        }

    def instance_execute(self, instance_ref, commandline):
        self.executed.append((instance_ref, commandline))
        if self.probe_raises:
            raise self.probe_raises

        self.aop_serial += 1
        return {
            'uuid': 'aop-%03d' % self.aop_serial,
            'instance_uuid': instance_ref,
            'state': self.probe_state,
            'commands': [{'command': 'execute', 'commandline': commandline}],
            'results': {'0': {'return-code': self.probe_return_code,
                              'stdout': self.probe_stdout,
                              'stderr': self.probe_stderr}}
        }
