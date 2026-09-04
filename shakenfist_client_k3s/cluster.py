"""One k3s cluster, and the orchestration which drives it.

This replaces the click context the orchestration primitives used to be
handed. Anything which reads or writes cluster metadata, or drives this
cluster's nodes, is a method here (see decision 2 in
``docs/plans/library-api-and-collection-phase-01-library-api.md``), so
that a library caller does not have to fabricate a click context to reach
it. What remains in ``primitives`` is namespace scoped or stateless: the
two release lookups, whose caches live in namespace metadata rather than
in any cluster's, and the pure helpers.

Imports between the two modules are deliberately one directional: this
module imports ``primitives``, and ``primitives`` must never import this
one.

Because ``shakenfist_client_k3s`` is imported unconditionally by the
``sf-client`` plugin loader, this module imports nothing beyond the
standard library and modules this package already imports.
"""

import copy
import time

from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress


# The namespace metadata key a cluster's state is stored under, one
# document per cluster.
METADATA_KEY = 'orchestrated_k3s_cluster_%s'

BASE_OS_VERSION = 'debian:12'

# How long, in seconds, a single agent command can run before the wait loop
# notes that it might be stalled.
STALL_WARNING_SECONDS = 300


class Cluster:
    """One k3s cluster, and everything the orchestration needs to reach it.

    A Cluster is always named: it is the cluster's state, and its metadata
    key is derived from the name. Work which is namespace scoped rather
    than cluster scoped -- listing clusters, and the two release lookups
    behind ``query-k3s-version`` and ``query-longhorn-version`` -- builds
    no Cluster at all and calls the module level functions in
    ``primitives`` instead.
    """

    def __init__(self, client, name, namespace, reporter=None):
        self.client = client
        self.name = name
        self.namespace = namespace
        self.reporter = reporter if reporter is not None else progress.Reporter()

        # The Progress reporter for the operation in flight, if one has
        # been built. get_progress() makes a default on demand, which is
        # what the orchestration relies on when a caller has not made one.
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
        return METADATA_KEY % self.name

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
        assign it; everything else gets one lazily, so a method called
        directly by a library caller still reports progress somewhere
        sensible.
        """
        if not self.progress:
            self.progress = progress.Progress(
                verbose=self.reporter.verbose, stream=self.reporter)
        return self.progress

    def create_instance(self):
        md = self.get_metadata()

        node_name = 'k3s-%s-node-%03d' % (md['name'], md['node_serial'])
        inst = self.client.create_instance(
            node_name, 2, 2048,
            [
                {
                    'network_uuid': md['node_network'],
                    'macaddress': None,
                    'model': 'virtio',
                    'float': True
                }
            ],
            [
                {
                    'size': 50,
                    'base': BASE_OS_VERSION,
                    'bus': None,
                    'type': 'disk'
                }
            ],
            md.get('ssh_key'), None,
            side_channels=['sf-agent2'],
            namespace=md['namespace']
        )
        return inst

    def _agent_op_error(self, aop):
        """Build the exception for an agent operation which entered the error state.

        This builds the exception rather than raising it so that the
        ``raise`` is visible at each of the three call sites. The previous
        version of this method exited the process and so never returned,
        and all three callers have code immediately after the call which is
        only correct because it is never reached: the results dict they go
        on to index is absent or unusable on an errored operation.
        Returning the exception keeps that control flow explicit rather
        than resting on a helper's promise not to come back.
        """
        inst = self.client.get_instance(aop['instance_uuid'])
        return exceptions.AgentOperationError(
            inst['name'], aop['instance_uuid'], aop['uuid'],
            primitives._describe_agent_op(aop, max_len=None),
            aop.get('results', {}) or {})

    def await_boot(self, instances):
        p = self.get_progress()
        waiting = copy.copy(instances)
        while waiting:
            for instance_uuid in copy.copy(waiting):
                inst = self.client.get_instance(instance_uuid)
                agent_state = inst['agent_state'] if inst['agent_state'] else 'not yet contactable'
                p.update(inst['name'], 'state %s, agent %s' % (inst['state'], agent_state))
                if inst['state'] == 'created' and inst['agent_state'] == 'ready':
                    waiting.remove(instance_uuid)

            if not waiting:
                break
            time.sleep(5)
        p.wait_done()

    def await_idle(self, instances):
        p = self.get_progress()
        waiting = copy.copy(instances)

        # Agent operations stay associated with an instance forever, and an
        # operation in the error state will never complete. Snapshot any which
        # had already failed before this wait started so a historical failure
        # can neither wedge this wait nor incorrectly abort it.
        preexisting_errors = {}
        for instance_uuid in waiting:
            aops = self.client.get_instance_agentoperations(instance_uuid, all=True)
            preexisting_errors[instance_uuid] = {
                aop['uuid'] for aop in aops if aop['state'] == 'error'}

        running_since = {}
        stall_warned = set()

        while waiting:
            for instance_uuid in copy.copy(waiting):
                inst = self.client.get_instance(instance_uuid)
                agent_ops = self.client.get_instance_agentoperations(
                    instance_uuid, all=True)
                agent_ops = [aop for aop in agent_ops
                             if aop['uuid'] not in preexisting_errors[instance_uuid]]

                errored = [aop for aop in agent_ops if aop['state'] == 'error']
                if errored:
                    raise self._agent_op_error(errored[0])

                incomplete = [aop for aop in agent_ops if aop['state'] != 'complete']
                if not incomplete:
                    p.update(inst['name'], 'idle')
                    waiting.remove(instance_uuid)
                else:
                    aop = incomplete[0]
                    desc = primitives._describe_agent_op(aop)
                    remaining = progress.count_str(len(incomplete), 'operation')
                    if desc:
                        p.update(inst['name'], "running '%s' (%s remaining)" % (desc, remaining))
                    else:
                        p.update(inst['name'], '%s remaining' % remaining)

                    # Note once per command if it has been running suspiciously
                    # long. The progress elapsed times show the same thing, but
                    # this note includes the operation uuid and where to look
                    # for more detail, and persists in scrollback.
                    now = time.time()
                    command_key = (aop['uuid'], len(aop.get('results', {}) or {}))
                    running_since.setdefault(command_key, now)
                    if (now - running_since[command_key] >= STALL_WARNING_SECONDS
                            and command_key not in stall_warned):
                        stall_warned.add(command_key)
                        p.note("%s has been running '%s' for %s and may be stalled; operation %s, "
                               "'sf-client instance events %s' may show why" % (
                                   inst['name'], desc or 'a command',
                                   progress.format_elapsed(now - running_since[command_key]),
                                   aop['uuid'], inst['name']))

            if not waiting:
                break
            time.sleep(5)
        p.wait_done()

    def await_fetch(self, aop):
        p = self.get_progress()
        while aop['state'] not in ['complete', 'error']:
            p.update('fetch operation', 'state %s' % aop['state'])
            time.sleep(1)
            aop = self.client.get_agent_operation(aop['uuid'])
        p.wait_done()

        if aop['state'] == 'error':
            raise self._agent_op_error(aop)

        blob_uuid = aop['results']['0']['content_blob']
        data = b''
        for chunk in self.client.get_blob_data(blob_uuid):
            data += chunk
        return data.decode('utf-8')

    def reap_execute(self, aop):
        while aop['state'] not in ('complete', 'error'):
            time.sleep(1)
            aop = self.client.get_agent_operation(aop['uuid'])

        if aop['state'] == 'error':
            raise self._agent_op_error(aop)

        if aop['results']['0']['return-code'] != 0:
            inst = self.client.get_instance(aop['instance_uuid'])
            raise exceptions.CommandFailedError(
                inst['name'], aop['instance_uuid'],
                aop['commands'][0]['commandline'],
                aop['results']['0']['return-code'],
                aop['results']['0']['stdout'],
                aop['results']['0']['stderr'])

    def create_and_await_instances(self, count, node_type):
        p = self.get_progress()
        md = self.get_metadata()

        display_type = node_type.replace('_', ' ')
        p.phase('Creating %s' % progress.count_str(count, '%s node' % display_type))

        new_nodes = []
        for i in range(count):
            inst = self.create_instance()
            new_nodes.append(inst['uuid'])
            md['node_serial'] += 1
            md[f'{node_type}_nodes'].append(inst['uuid'])
            self.set_metadata(md)
            p.note(f'created {inst["name"]} (uuid {inst["uuid"]})')

        self.await_boot(new_nodes)
        p.note('updating base OS packages')
        self.instance_os_update(new_nodes)
        self.set_metadata(md)

    def execute_and_await(self, instance_uuids, cmds):
        aops = []
        for cmd in cmds:
            for instance_uuid in instance_uuids:
                aops.append(self.client.instance_execute(
                    instance_uuid, cmd))

        # Wait for instances to be idle and check results
        self.await_idle(instance_uuids)
        for aop in aops:
            self.reap_execute(aop)

    def instance_os_update(self, instance_uuids):
        self.execute_and_await(
            instance_uuids,
            [
                'apt-get update',
                'apt-get dist-upgrade -y'
            ]
        )

    def install_control_plane(self):
        p = self.get_progress()
        md = self.get_metadata()
        cmds = []

        p.phase('Installing k3s on the first control plane node')

        # Write a configuration file with the external address to the first control
        # plane node. This is needed so that the SSL certificate includes this
        # external name.
        cmds.append('mkdir -p /etc/rancher/k3s/')
        cmds.append(
            'cat - > /etc/rancher/k3s/config.yaml << EOF\n'
            'write-kubeconfig-mode: "0644"\n'
            'tls-san:\n'
            '  - "%s"\n'
            'cluster-init: true\n'
            'EOF\n'
            % md['api_address_floating'])

        # Instruct the first control plane node to install k3s and helm
        cmds.append('curl -sfL https://get.k3s.io | '
                    'INSTALL_K3S_CHANNEL=%s sh -s - server'
                    % md['k3s_version'])
        cmds.append('sudo apt-get install -y extrepo')
        cmds.append('sudo extrepo enable helm')
        cmds.append('sudo apt-get update')
        cmds.append('sudo apt-get install -y helm')

        self.execute_and_await([md['control_plane_nodes'][0]], cmds)

        # Fetch the server and node tokens from the first control plane node
        p.note('fetching control plane registration token')
        aop = self.client.instance_get(
            md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/token')
        md['server_token'] = self.await_fetch(aop).rstrip()
        self.set_metadata(md)

        p.note('fetching node registration token')
        aop = self.client.instance_get(
            md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/node-token')
        md['node_token'] = self.await_fetch(aop).rstrip()
        self.set_metadata(md)

        # If there is more than one control plane node, then install the others
        if len(md['control_plane_nodes']) > 1:
            self.install_extra_control_plane()

    def install_k3s_component(self, instance_uuids, token, node_role):
        md = self.get_metadata()

        # Nodes must join via an address inside the node network: the network
        # node neither hairpins floating addresses nor routes in-network
        # traffic to the network's own routed addresses (see
        # shakenfist/shakenfist#3662). Clusters created before join_address
        # existed only have api_address_inner.
        join_address = md.get('join_address', md['api_address_inner'])

        self.execute_and_await(
            instance_uuids,
            [
                'sudo apt-get update',
                'sudo apt-get install -y',
                (
                    'curl -sfL https://get.k3s.io | '
                    f'INSTALL_K3S_CHANNEL={md["k3s_version"]} '
                    f'K3S_URL=https://{join_address}:6443 '
                    f'K3S_TOKEN={token} sh -s - {node_role}'
                )
            ]
        )

        self.set_metadata(md)

    def install_extra_control_plane(self):
        p = self.get_progress()
        md = self.get_metadata()
        p.phase('Installing k3s on the additional control plane nodes')
        self.install_k3s_component(
            md['control_plane_nodes'][1:], md['server_token'], 'server')

    def install_workers(self):
        p = self.get_progress()
        md = self.get_metadata()
        p.phase('Installing k3s on the worker nodes')
        self.install_k3s_component(md['worker_nodes'], md['node_token'], 'agent')

    def allocate_metallb_addresses(self, metal_address_count):
        p = self.get_progress()
        md = self.get_metadata()
        node_network = self.client.get_network(md['node_network'])

        allocated = []
        for i in range(metal_address_count):
            addr = self.client.route_network_address(node_network['uuid'])
            if addr:
                md['routed_addresses'].append(addr)
                allocated.append(addr)

        if not allocated:
            p.note('no routed addresses were available (requested %d)' % metal_address_count)
        else:
            msg = 'allocated %s: %s' % (
                progress.count_str(len(allocated), 'routed address'), ', '.join(allocated))
            if len(allocated) < metal_address_count:
                msg += ' (requested %d)' % metal_address_count
            msg += '; the cluster now has %d' % len(md['routed_addresses'])
            p.note(msg)
        self.set_metadata(md)

    def configure_metallb_addresses(self):
        md = self.get_metadata()

        # Setup metallb for traffic ingress, guided by
        # https://itnext.io/kubernetes-loadbalancer-service-for-on-premises-6b7f75187be8
        metal_lb_config = ('cat - > /etc/sf/metallb-range-allocation.yaml << EOF\n'
                           'apiVersion: metallb.io/v1beta1\n'
                           'kind: IPAddressPool\n'
                           'metadata:\n'
                           '  name: empty\n'
                           '  namespace: metallb-system\n'
                           'spec:\n'
                           '  addresses:\n'
                           '  - %s/32\n'
                           '---\n'
                           'apiVersion: metallb.io/v1beta1\n'
                           'kind: L2Advertisement\n'
                           'metadata:\n'
                           '  name: empty\n'
                           '  namespace: metallb-system\n'
                           'EOF\n'
                           % '/32\n  - '.join(md['routed_addresses']))

        self.execute_and_await(
            [md['control_plane_nodes'][0]],
            [
                ('kubectl wait --kubeconfig /etc/rancher/k3s/k3s.yaml -n metallb-system pod '
                 '--for=condition=Ready -l app.kubernetes.io/name=metallb --timeout=300s'),
                'mkdir -p /etc/sf',
                metal_lb_config,
                'kubectl apply -f /etc/sf/metallb-range-allocation.yaml'
            ]
        )

    def setup_metallb(self, metal_address_count):
        p = self.get_progress()
        md = self.get_metadata()

        p.phase('Setting up metallb')
        self.allocate_metallb_addresses(metal_address_count)
        self.execute_and_await(
            [md['control_plane_nodes'][0]],
            [
                'kubectl create ns metallb-system',
                # The official metallb chart is used here because Bitnami
                # stopped publishing versioned images to docker.io/bitnami in
                # 2025, so the bitnamicharts/metallb chart installs pods which
                # can never pull their images. Note also that we can't use the
                # KUBECONFIG=... environment variable prefix idiom: the
                # in-guest agent validates the first token of the command line
                # as an executable before running the command.
                'helm repo add metallb https://metallb.github.io/metallb',
                'helm repo update',
                ('helm --kubeconfig /etc/rancher/k3s/k3s.yaml '
                 'upgrade --install -n metallb-system metallb metallb/metallb'),
            ])

        # Let the metallb pods start
        time.sleep(5)

        # Add addresses
        self.configure_metallb_addresses()

    def setup_longhorn(self):
        p = self.get_progress()
        md = self.get_metadata()

        version = primitives.get_longhorn_release(
            self.client, self.namespace, self.reporter)
        p.phase(f'Setting up longhorn version {version}')

        self.execute_and_await(
            [md['control_plane_nodes'][0]],
            [
                'helm repo add longhorn https://charts.longhorn.io',
                'helm repo update',
                'kubectl create namespace longhorn-system || true',
                (
                    'helm --kubeconfig /etc/rancher/k3s/k3s.yaml '
                    'install longhorn longhorn/longhorn '
                    '--namespace longhorn-system '
                    f'--version {version}'
                ),
                (
                    'kubectl patch storageclass local-path -p '
                    '\'{"metadata": {"annotations":{'
                    '"storageclass.kubernetes.io/is-default-class":"false"}}}\''
                )
            ])
