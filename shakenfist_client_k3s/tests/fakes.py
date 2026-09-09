"""A scripted Shaken Fist client, good enough to drive a cluster end to end.

A bare ``mock.MagicMock`` cannot do this job. The orchestration's wait
loops compare dictionary values against literals -- ``'created'``,
``'ready'``, ``'complete'``, ``'deleted'`` -- and a MagicMock's attributes
and subscripts compare equal to none of them, so every wait spins forever.
This fake returns the shapes the orchestration actually reads instead, and
lives here rather than in one test module because both the CLI level
create smoke test and the library level lifecycle test need it.
"""


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
        self.aop_serial = 0
        self.routed_serial = 0

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
        self.instance_serial += 1
        instance_uuid = 'inst-%03d' % self.instance_serial
        self.instances[instance_uuid] = {
            'uuid': instance_uuid, 'name': name, 'state': 'created',
            'agent_state': 'ready'}
        return self.instances[instance_uuid]

    def get_instance(self, instance_ref):
        return self.instances[instance_ref]

    def delete_instance(self, instance_ref):
        self.instances[instance_ref]['state'] = 'deleted'

    def get_instance_interfaces(self, instance_ref):
        return [{'ipv4': '10.0.0.4', 'floating': '192.168.10.100'}]

    def get_instance_agentoperations(self, instance_ref, all=False):
        return []

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
