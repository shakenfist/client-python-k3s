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
standard library and modules this package already imports. That includes
the ``importlib.metadata`` guard below, which moved here with
``Cluster.create()``: it is the same try/except the package __init__
carried, for the same reason (``importlib.metadata`` is only in the
standard library from Python 3.8, and this package supports 3.7).
"""

import copy
import os
from shakenfist_client import apiclient
import shutil
import subprocess
import tempfile
import time
import yaml

try:
    from importlib.metadata import version as distribution_version
except ImportError:
    from importlib_metadata import version as distribution_version

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

    # The methods below are the whole of a k3s command: each one was the
    # body of a Click command in shakenfist_client_k3s/__init__.py, and the
    # command is now argument parsing plus one call into here. They return
    # values rather than printing them, so that a library caller gets the
    # result and the CLI keeps the formatting.
    #
    # Their arguments are what the corresponding command line options carry,
    # minus the name and namespace, which are the Cluster's own. Where an
    # argument is genuinely optional it keeps the option's default, so that
    # omitting it gives the command line's behaviour; where it is not -- the
    # three counts create needs -- it is required, because None is not a
    # workable value for any of them and there is no sensible default for
    # the shape of somebody else's cluster.

    def create(self, control_plane_count, worker_count, metal_address_count,
               network=None, refresh_version_cache=False,
               release_channel='stable', sshkey=None):
        """Build this cluster, from nothing to a working k3s.

        The namespace must already exist. The command line creates it when
        --namespace named one which does not, because only the command line
        knows whether the option was passed at all; see
        _bind_new_cluster_context() in this package's __init__.

        Writing ~/.kube/config, and shelling out to kubectl to merge into
        an existing one, are unconditional here because they are
        unconditional in the command this replaces. Making them optional is
        phase 3: doing it here would put a behaviour change inside a
        refactor whose entire safety argument is that behaviour is
        unchanged.
        """
        # Phases: create control plane nodes, create workers, install control
        # plane, install workers, fetch credentials, metallb, longhorn, and
        # update the local kubeconfig. Creating a node network and installing
        # additional control plane nodes only sometimes happen.
        total_phases = 8
        if not network:
            total_phases += 1
        if control_plane_count > 1:
            total_phases += 1
        p = progress.Progress(
            total_phases=total_phases, verbose=self.reporter.verbose,
            stream=self.reporter)
        self.progress = p

        self.reporter.debug('Looking up k3s versions')
        target_release = primitives.get_k3s_release(
            self.client, self.namespace, self.reporter,
            force_cache_update=refresh_version_cache,
            release_channel=release_channel)

        # Ensure this name isn't already taken
        namespace_md = self.client.get_namespace_metadata(self.namespace)
        all_clusters = namespace_md.get(primitives.CLUSTER_LIST, [])
        md = self.get_metadata()

        if self.name in all_clusters:
            raise exceptions.ClusterExistsError(self.name)
        if md:
            raise exceptions.ClusterExistsError(self.name)
        all_clusters.append(self.name)
        self.client.set_namespace_metadata_item(
            self.namespace, primitives.CLUSTER_LIST, all_clusters)

        # Create a network for nodes
        if network:
            node_network = self.client.get_network(network)
            if not node_network:
                raise exceptions.NetworkNotFoundError(network)
        else:
            p.phase('Creating node network')
            node_network = self.client.allocate_network(
                '10.0.0.0/16', True, True, 'k3s-%s-node' % self.name,
                namespace=self.namespace)
            p.note('created %s (uuid %s)' % (node_network['name'], node_network['uuid']))
            while True:
                node_network = self.client.get_network(node_network['uuid'])
                p.update(node_network['name'], 'state %s' % node_network['state'])
                if node_network['state'] == 'created':
                    break
                time.sleep(1)
            p.wait_done()

        # Read the ssh key if any
        ssh_key_content = None
        if sshkey:
            with open(sshkey) as f:
                ssh_key_content = f.read()

        # Initialise the metadata
        self.reporter.debug('Initialize cluster metadata')
        md = {
            'name': self.name,
            'namespace': self.namespace,
            'type': 'k3s',
            'k3s_version': target_release,
            'k3s_version_history': [target_release],
            'plugin_version': distribution_version('shakenfist_client_k3s'),
            'state': 'initial',
            'node_serial': 1,
            'node_network': node_network['uuid'],
            'node_token': None,
            'control_plane_nodes': [],
            'worker_nodes': [],
            'routed_addresses': [],
            'ssh_key': ssh_key_content
        }
        self.set_metadata(md)

        # We really should do a pre-fetch on the disk image and wait for it to
        # download before starting instances. That way the point of slowness is
        # more obvious. That requires cluster operations to exist though.

        # I'd prefer to wait for these as one thing, but that's not currently a thing
        # the code supports.
        self.create_and_await_instances(control_plane_count, 'control_plane')
        self.create_and_await_instances(worker_count, 'worker')

        # Record the node network address for the first control plane node as the API
        # address
        interfaces = self.client.get_instance_interfaces(md['control_plane_nodes'][0])
        md['api_address_inner'] = interfaces[0]['ipv4']
        md['api_address_floating'] = interfaces[0]['floating']

        # The join address is the address new nodes register through, and is
        # deliberately mutable cluster state rather than "the first control
        # plane node's address": a future control plane replacement joins the
        # new server via the old address, updates join_address, and then reaps
        # the old node. k3s agents only need this address at registration time.
        md['join_address'] = interfaces[0]['ipv4']
        self.set_metadata(md)

        self.install_control_plane()
        self.install_workers()

        # Fetch kubecfg, correct IP, and include cluster name instead of "default"
        p.phase('Fetching cluster credentials')
        aop = self.client.instance_get(
            md['control_plane_nodes'][0], '/etc/rancher/k3s/k3s.yaml')
        kubeconfig = self.await_fetch(aop).replace(
            '127.0.0.1', md['api_address_floating'])

        kc = yaml.safe_load(kubeconfig)
        fqcn = '%s.%s' % (self.name, self.namespace)
        kc['clusters'][0]['name'] = fqcn
        kc['contexts'][0]['name'] = fqcn
        kc['contexts'][0]['context']['cluster'] = fqcn
        kc['contexts'][0]['context']['user'] = fqcn
        kc['users'][0]['name'] = fqcn
        kc['current-context'] = fqcn
        md['kubeconfig'] = yaml.dump(kc)
        self.set_metadata(md)

        # Install metallb and longhorn
        self.setup_metallb(metal_address_count)
        self.setup_longhorn()

        # Install the kubeconfig we fetched earlier
        p.phase('Updating local kubeconfig')
        kube_dir = os.path.join(os.path.expanduser('~'), '.kube')
        main_config_path = os.path.join(kube_dir, 'config')
        os.makedirs(kube_dir, exist_ok=True)

        if not os.path.exists(main_config_path):
            # There is no existing configuration to preserve, so no merge is
            # required and we don't need a local kubectl.
            with open(main_config_path, 'w') as f:
                f.write(yaml.dump(kc))
        else:
            if not shutil.which('kubectl'):
                raise exceptions.KubeconfigError.missing_kubectl(
                    main_config_path, self.name)

            with tempfile.TemporaryDirectory() as tempdir:
                new_config_path = os.path.join(tempdir, 'config')
                with open(new_config_path, 'w') as f:
                    f.write(yaml.dump(kc))
                merged = subprocess.run(
                    'kubectl config view --flatten', shell=True, capture_output=True,
                    env={**os.environ,
                         'KUBECONFIG': '%s:%s' % (main_config_path, new_config_path)})
                if merged.returncode != 0:
                    # kubectl's stderr arrives as bytes, and was decoded at the
                    # point it was printed; decode it here so the exception
                    # renders exactly the same text.
                    stderr = None
                    if merged.stderr:
                        stderr = merged.stderr.decode('utf-8', errors='replace')
                    raise exceptions.KubeconfigError.merge_failed(
                        main_config_path, merged.returncode, stderr)

                # kubectl's merge keeps the pre-existing file's current-context,
                # which would leave kubectl pointed at whatever cluster was
                # active before this create. Select the new cluster, matching
                # the no-merge path above.
                merged_kc = yaml.safe_load(merged.stdout)
                merged_kc['current-context'] = fqcn
                with open(main_config_path, 'w') as f:
                    f.write(yaml.dump(merged_kc))

        md['state'] = 'created'
        self.set_metadata(md)
        p.finish(f'Cluster {self.name} is ready')

    def get_kubeconfig(self):
        """Return this cluster's kubeconfig, as a string.

        This is the body of ``sf-client k3s getconfig``, which prints what
        this returns.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.unknown_cluster(self.name)

        kubeconfig = md.get('kubeconfig')
        if not kubeconfig:
            # The cluster exists, it is just not finished, which is a
            # different thing to it not existing at all.
            raise exceptions.ClusterIncompleteError(self.name)

        return kubeconfig

    def show(self):
        """Return this cluster's metadata, or raise if there is no such cluster.

        This is the body of ``sf-client k3s show``, which formats what this
        returns. It differs from get_metadata() only in insisting that the
        cluster exists, and in the error it raises when it does not.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.does_not_exist(self.name)
        return md

    def delete(self):
        """Destroy this cluster and everything created alongside it.

        This is the body of ``sf-client k3s delete``. Removing this
        cluster's entries from the local ~/.kube/config with kubectl is
        unconditional here because it is unconditional in the command this
        replaces; phase 3 makes it optional.
        """
        # Ensure this name exists
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.does_not_exist(self.name)

        self.reporter.debug('Cluster metadata:')
        for k in md:
            self.reporter.debug('    %s = %s' % (k, md[k]))

        # Delete instances
        waiting = []
        for instance_uuid in set(md['control_plane_nodes'] + md['worker_nodes']):
            try:
                inst = self.client.get_instance(instance_uuid)
                self.reporter.debug('...Deleting instance %s with uuid %s'
                                    % (inst['name'], instance_uuid))
                self.client.delete_instance(instance_uuid)
                waiting.append(instance_uuid)
            except apiclient.ResourceNotFoundException:
                pass

        while waiting:
            self.reporter.debug(
                '...Waiting for %d instances to be deleted' % len(waiting))
            for instance_uuid in copy.copy(waiting):
                try:
                    i = self.client.get_instance(instance_uuid)
                    if i['state'] == 'deleted':
                        waiting.remove(instance_uuid)
                except apiclient.ResourceNotFoundException:
                    waiting.remove(instance_uuid)

            if waiting:
                time.sleep(1)

        md['control_plane_nodes'] = []
        md['worker_nodes'] = []
        md['api_floating_address'] = None
        md['api_inner_address'] = None
        md['k3s_version'] = None
        md['kubeconfig'] = None
        md['node_token'] = None
        self.set_metadata(md)

        if md.get('node_network'):
            # Free any routed ips
            for addr in md.get('routed_addresses', []):
                try:
                    self.reporter.debug('Unrouting address %s from network %s'
                                        % (addr, md['node_network']))
                    self.client.unroute_network_address(
                        md['node_network'], addr)
                except apiclient.UnauthorizedException:
                    self.reporter.debug(
                        '...Address %s was not routed to this network' % addr)

            # Delete node network. This deletes the node network whether or
            # not create allocated it, so a network handed to
            # "create --network" is destroyed along with the cluster which
            # borrowed it. That is shakenfist/client-python-k3s#41, and it is
            # preserved here deliberately: this step moves code without
            # changing what it does, and the fix belongs in its own change.
            self.client.delete_network(md['node_network'])
            md['node_network'] = []

        md['state'] = 'deleted'
        self.set_metadata(md)

        # Then remove the metadata
        self.delete_metadata()
        namespace_md = self.client.get_namespace_metadata(self.namespace)
        all_clusters = namespace_md.get(primitives.CLUSTER_LIST, [])
        all_clusters.remove(self.name)
        if not all_clusters:
            self.client.delete_namespace_metadata_item(
                self.namespace, primitives.CLUSTER_LIST)
        else:
            self.client.set_namespace_metadata_item(
                self.namespace, primitives.CLUSTER_LIST, all_clusters)

        # And remove the local config
        fqcn = '%s.%s' % (self.name, self.namespace)
        for config_elem in ['users.%s' % fqcn,
                            'contexts.%s' % fqcn,
                            'clusters.%s' % fqcn]:
            p = subprocess.run(
                'kubectl config unset %s' % config_elem, shell=True)
            if p.returncode != 0:
                raise exceptions.KubeconfigError.unset_failed(config_elem)

    def expand_workers(self, worker_count):
        """Add worker nodes to this cluster.

        This is the body of ``sf-client k3s expand-workers``.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.not_found(self.name)

        p = progress.Progress(
            total_phases=2, verbose=self.reporter.verbose, stream=self.reporter)
        self.progress = p
        self.create_and_await_instances(worker_count, 'worker')
        self.install_workers()
        p.finish(f'Added {worker_count} workers to cluster {self.name}')

    def expand_addresses(self, address_count):
        """Route more floating addresses into this cluster for metallb to hand out.

        This is the body of ``sf-client k3s expand-addresses``.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.not_found(self.name)

        p = progress.Progress(
            total_phases=1, verbose=self.reporter.verbose, stream=self.reporter)
        self.progress = p
        p.phase('Adding metallb addresses')
        self.allocate_metallb_addresses(address_count)
        self.configure_metallb_addresses()
        p.finish(f'Added {address_count} metallb addresses to cluster {self.name}')

    def update_os(self):
        """Update the base OS packages on every node in this cluster.

        This is the body of ``sf-client k3s update-os``.
        """
        md = self.get_metadata()
        if not md:
            raise exceptions.ClusterNotFoundError.not_found(self.name)

        p = progress.Progress(
            total_phases=1, verbose=self.reporter.verbose, stream=self.reporter)
        self.progress = p
        p.phase('Updating the OS on all cluster nodes')
        self.instance_os_update(md['control_plane_nodes'] + md['worker_nodes'])
        p.finish(f'Updated the OS on all nodes in cluster {self.name}')
