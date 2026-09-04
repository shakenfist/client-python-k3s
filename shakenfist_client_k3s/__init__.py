import click
import copy
import os
from shakenfist_client import apiclient
import shutil
import subprocess
import sys
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
from shakenfist_client_k3s.cluster import Cluster


CLUSTER_LIST = 'orchestrated_k3s_clusters'


def _emit_debug(ctx, m):
    if ctx.obj['VERBOSE']:
        print(m)


def _bind_namespace_context(ctx, namespace):
    """Build what a namespace scoped command needs, and record it in ctx.obj.

    Returns the client, the resolved namespace and a reporter. The
    --namespace option is None unless the caller passed it, and the
    namespace is passed directly to API calls, so it must be defaulted to
    the client's own namespace here.

    list, query-k3s-version and query-longhorn-version are namespace
    scoped rather than cluster scoped: they name no cluster, and the
    release lookups they call cache in namespace metadata. They therefore
    build no Cluster at all.
    """
    client = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)
    ctx.obj['CLIENT'] = client
    if not namespace:
        namespace = client.namespace
    ctx.obj['namespace'] = namespace
    return client, namespace, progress.Reporter(verbose=ctx.obj.get('VERBOSE', False))


def _bind_cluster_context(ctx, name, namespace):
    """Build the Cluster this command operates on, and record it in ctx.obj.

    ctx.obj is still populated because the command bodies in this module
    read it directly. Only the orchestration has moved onto the Cluster so
    far; the bodies follow in a later step of the phase 1 plan, which is
    what retires ctx.obj.
    """
    client, namespace, reporter = _bind_namespace_context(ctx, namespace)
    ctx.obj['name'] = name
    return Cluster(client, name, namespace, reporter=reporter)


class GroupCatchClusterExceptions(click.Group):
    """Turn this plugin's exceptions back into the CLI behaviour they replaced.

    The orchestration raises K3sClusterException subclasses rather than
    exiting the process, so that an in process caller -- the Ansible
    module this plan exists for -- can fail structurally and keep stdout
    for its own JSON result. The command line still has to behave exactly
    as it did, so this is where the two meet: one handler which prints the
    message the failing command used to print, on stdout where it has
    always gone, and exits 1. Click's Group.invoke() is what resolves and
    invokes the subcommand, so catching here covers every command,
    including any added later.

    Nothing from shakenfist_client.apiclient is caught here. The parent
    CLI's GroupCatchExceptions already maps every API exception to its own
    error line and exit code, and that behaviour must survive untouched.
    """

    def invoke(self, ctx):
        try:
            return super(GroupCatchClusterExceptions, self).invoke(ctx)
        except exceptions.K3sClusterException as e:
            print(str(e))
            sys.exit(1)


@click.group(cls=GroupCatchClusterExceptions,
             help=('k3s kubernetes cluster commands (via the '
                   'shakenfist-client-k3s plugin)'))
def k3s():
    ...


@k3s.command(name='list', help='List managed k3s clusters')
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can list clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_list(ctx, namespace=None, ):
    _, namespace, _ = _bind_namespace_context(ctx, namespace)

    namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
    all_clusters = namespace_md.get(CLUSTER_LIST, [])

    for cluster in all_clusters:
        print(cluster)


k3s.add_command(k3s_list)


@k3s.command(name='create', help='Create a new k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--control-plane-count', type=click.INT,
              help='The number of control plane nodes', default=1)
@click.option('--worker-count', type=click.INT, help='The number of workers',
              default=2)
@click.option('--metal-address-count', type=click.INT,
              help=('The number of floating addresses to route into the virtual '
                    'network for metallb to manage'),
              default=5)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.option('--network', type=click.STRING,
              help=('Specify a network here to add these nodes to a pre-existing '
                    'network. Otherwise one will be created for this cluster.'))
@click.option('--refresh-version-cache/--no-refresh-version-cache', default=False,
              help=('Force a refresh of the k3s version cache.'))
@click.option('--release-channel', default='stable',
              help=('Select a k3s release channel. Common choices include stable, '
                    'latest, and specific versions pre-pended with a v such as '
                    '"v1.26".'))
@click.option('--sshkey', type=click.Path(exists=True),
              help='An optional ssh public key to place onto instances.')
@click.pass_context
def k3s_create(ctx, name=None, control_plane_count=None, worker_count=None,
               metal_address_count=None,  namespace=None, network=None,
               refresh_version_cache=False, release_channel=None,
               sshkey=None):
    ctx.obj['name'] = name
    ctx.obj['CLIENT'] = apiclient.Client(
        async_strategy=apiclient.ASYNC_CONTINUE)

    # Phases: create control plane nodes, create workers, install control
    # plane, install workers, fetch credentials, metallb, longhorn, and
    # update the local kubeconfig. Creating a node network and installing
    # additional control plane nodes only sometimes happen.
    total_phases = 8
    if not network:
        total_phases += 1
    if control_plane_count > 1:
        total_phases += 1
    p = progress.Progress(total_phases=total_phases, verbose=ctx.obj['VERBOSE'])

    # The namespace must be resolved (and exist) before anything looks up
    # namespace metadata, including the version cache.
    if namespace:
        ns = ctx.obj['CLIENT'].get_namespace(namespace)
        if not ns:
            ctx.obj['CLIENT'].create_namespace(namespace)
            print('Created namespace %s' % namespace)
    else:
        namespace = ctx.obj['CLIENT'].namespace
    ctx.obj['namespace'] = namespace

    c = Cluster(ctx.obj['CLIENT'], name, namespace,
                reporter=progress.Reporter(verbose=ctx.obj.get('VERBOSE', False)))
    c.progress = p

    _emit_debug(ctx, 'Looking up k3s versions')
    target_release = primitives.get_k3s_release(
        c.client, c.namespace, c.reporter,
        force_cache_update=refresh_version_cache,
        release_channel=release_channel)

    # Ensure this name isn't already taken
    namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
    all_clusters = namespace_md.get(CLUSTER_LIST, [])
    md = c.get_metadata()

    if name in all_clusters:
        raise exceptions.ClusterExistsError(name)
    if md:
        raise exceptions.ClusterExistsError(name)
    all_clusters.append(name)
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, CLUSTER_LIST, all_clusters)

    # Create a network for nodes
    if network:
        node_network = ctx.obj['CLIENT'].get_network(network)
        if not node_network:
            raise exceptions.NetworkNotFoundError(network)
    else:
        p.phase('Creating node network')
        node_network = ctx.obj['CLIENT'].allocate_network(
            '10.0.0.0/16', True, True, 'k3s-%s-node' % name, namespace=namespace)
        p.note('created %s (uuid %s)' % (node_network['name'], node_network['uuid']))
        while True:
            node_network = ctx.obj['CLIENT'].get_network(node_network['uuid'])
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
    _emit_debug(ctx, 'Initialize cluster metadata')
    md = {
        'name': name,
        'namespace': namespace,
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
    c.set_metadata(md)

    # We really should do a pre-fetch on the disk image and wait for it to
    # download before starting instances. That way the point of slowness is
    # more obvious. That requires cluster operations to exist though.

    # I'd prefer to wait for these as one thing, but that's not currently a thing
    # the code supports.
    c.create_and_await_instances(control_plane_count, 'control_plane')
    c.create_and_await_instances(worker_count, 'worker')

    # Record the node network address for the first control plane node as the API
    # address
    interfaces = ctx.obj['CLIENT'].get_instance_interfaces(md['control_plane_nodes'][0])
    md['api_address_inner'] = interfaces[0]['ipv4']
    md['api_address_floating'] = interfaces[0]['floating']

    # The join address is the address new nodes register through, and is
    # deliberately mutable cluster state rather than "the first control
    # plane node's address": a future control plane replacement joins the
    # new server via the old address, updates join_address, and then reaps
    # the old node. k3s agents only need this address at registration time.
    md['join_address'] = interfaces[0]['ipv4']
    c.set_metadata(md)

    c.install_control_plane()
    c.install_workers()

    # Fetch kubecfg, correct IP, and include cluster name instead of "default"
    p.phase('Fetching cluster credentials')
    aop = ctx.obj['CLIENT'].instance_get(
        md['control_plane_nodes'][0], '/etc/rancher/k3s/k3s.yaml')
    kubeconfig = c.await_fetch(aop).replace(
        '127.0.0.1', md['api_address_floating'])

    kc = yaml.safe_load(kubeconfig)
    fqcn = '%s.%s' % (name, namespace)
    kc['clusters'][0]['name'] = fqcn
    kc['contexts'][0]['name'] = fqcn
    kc['contexts'][0]['context']['cluster'] = fqcn
    kc['contexts'][0]['context']['user'] = fqcn
    kc['users'][0]['name'] = fqcn
    kc['current-context'] = fqcn
    md['kubeconfig'] = yaml.dump(kc)
    c.set_metadata(md)

    # Install metallb and longhorn
    c.setup_metallb(metal_address_count)
    c.setup_longhorn()

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
            raise exceptions.KubeconfigError.missing_kubectl(main_config_path, name)

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
    c.set_metadata(md)
    p.finish(f'Cluster {name} is ready')


k3s.add_command(k3s_create)


@k3s.command(name='query-k3s-version',
             help='Lookup the current version for a k3s release channel')
@click.argument('release_channel', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can control which namespace the '
                    'version cache is retrieved from.'))
@click.option('--refresh-version-cache/--no-refresh-version-cache', default=False,
              help=('Force a refresh of the k3s version cache.'))
@click.pass_context
def k3s_query_k3s_version(ctx, release_channel=None, namespace=None,
                          refresh_version_cache=False):
    client, namespace, reporter = _bind_namespace_context(ctx, namespace)

    target_release = primitives.get_k3s_release(
        client, namespace, reporter,
        force_cache_update=refresh_version_cache,
        release_channel=release_channel)
    print(f'Release channel {release_channel} has {target_release} as its '
          'latest version.')


k3s.add_command(k3s_query_k3s_version)


@k3s.command(name='query-longhorn-version',
             help='Lookup the current longhorn version')
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can control which namespace the '
                    'version cache is retrieved from.'))
@click.option('--refresh-version-cache/--no-refresh-version-cache', default=False,
              help=('Force a refresh of the longhorn version cache.'))
@click.pass_context
def k3s_query_longhorn_version(ctx, namespace=None, refresh_version_cache=False):
    client, namespace, reporter = _bind_namespace_context(ctx, namespace)

    target_release = primitives.get_longhorn_release(
        client, namespace, reporter,
        force_cache_update=refresh_version_cache)
    print(f'Longhorn has {target_release} as its latest version.')


k3s.add_command(k3s_query_longhorn_version)


@k3s.command(name='getconfig', help='Get kubeconfig for an existing k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can fetch the kubeconfig for a '
                    'cluster in a different namespace.'))
@click.pass_context
def k3s_getconfig(ctx, name=None, namespace=None):
    c = _bind_cluster_context(ctx, name, namespace)

    md = c.get_metadata()
    if not md:
        raise exceptions.ClusterNotFoundError.unknown_cluster(name)

    kubeconfig = md.get('kubeconfig')
    if not kubeconfig:
        # The cluster exists, it is just not finished, which is a
        # different thing to it not existing at all.
        raise exceptions.ClusterIncompleteError(name)

    print(kubeconfig)


@k3s.command(name='show', help='Show details of a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_show(ctx, name=None, namespace=None):
    c = _bind_cluster_context(ctx, name, namespace)

    md = c.get_metadata()
    if not md:
        raise exceptions.ClusterNotFoundError.does_not_exist(name)

    print('Cluster metadata:')
    for k in md:
        print('    %s = %s' % (k, md[k]))


k3s.add_command(k3s_show)


@k3s.command(name='delete', help='Destroy a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_delete(ctx, name=None, namespace=None):
    c = _bind_cluster_context(ctx, name, namespace)
    namespace = c.namespace

    # Ensure this name exists
    md = c.get_metadata()
    if not md:
        raise exceptions.ClusterNotFoundError.does_not_exist(name)

    _emit_debug(ctx, 'Cluster metadata:')
    for k in md:
        _emit_debug(ctx, '    %s = %s' % (k, md[k]))

    # Delete instances
    waiting = []
    for instance_uuid in set(md['control_plane_nodes'] + md['worker_nodes']):
        try:
            inst = ctx.obj['CLIENT'].get_instance(instance_uuid)
            _emit_debug(ctx, '...Deleting instance %s with uuid %s'
                        % (inst['name'], instance_uuid))
            ctx.obj['CLIENT'].delete_instance(instance_uuid)
            waiting.append(instance_uuid)
        except apiclient.ResourceNotFoundException:
            pass

    while waiting:
        _emit_debug(ctx, '...Waiting for %d instances to be deleted' % len(waiting))
        for instance_uuid in copy.copy(waiting):
            try:
                i = ctx.obj['CLIENT'].get_instance(instance_uuid)
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
    c.set_metadata(md)

    if md.get('node_network'):
        # Free any routed ips
        for addr in md.get('routed_addresses', []):
            try:
                _emit_debug(ctx, 'Unrouting address %s from network %s'
                            % (addr, md['node_network']))
                ctx.obj['CLIENT'].unroute_network_address(
                    md['node_network'], addr)
            except apiclient.UnauthorizedException:
                _emit_debug(
                    ctx, '...Address %s was not routed to this network' % addr)

        # Delete node network
        ctx.obj['CLIENT'].delete_network(md['node_network'])
        md['node_network'] = []

    md['state'] = 'deleted'
    c.set_metadata(md)

    # Then remove the metadata
    c.delete_metadata()
    namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
    all_clusters = namespace_md.get(CLUSTER_LIST, [])
    all_clusters.remove(name)
    if not all_clusters:
        ctx.obj['CLIENT'].delete_namespace_metadata_item(namespace, CLUSTER_LIST)
    else:
        ctx.obj['CLIENT'].set_namespace_metadata_item(
            namespace, CLUSTER_LIST, all_clusters)

    # And remove the local config
    fqcn = '%s.%s' % (name, namespace)
    for config_elem in ['users.%s' % fqcn,
                        'contexts.%s' % fqcn,
                        'clusters.%s' % fqcn]:
        p = subprocess.run(
            'kubectl config unset %s' % config_elem, shell=True)
        if p.returncode != 0:
            raise exceptions.KubeconfigError.unset_failed(config_elem)


k3s.add_command(k3s_delete)


@k3s.command(name='expand-workers', help='Add workers to a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--worker-count', type=click.INT, help='The number of workers',
              default=2)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_expand_workers(ctx, name=None, worker_count=None, namespace=None):
    c = _bind_cluster_context(ctx, name, namespace)

    md = c.get_metadata()
    if not md:
        raise exceptions.ClusterNotFoundError.not_found(name)

    p = progress.Progress(total_phases=2, verbose=ctx.obj['VERBOSE'])
    c.progress = p
    c.create_and_await_instances(worker_count, 'worker')
    c.install_workers()
    p.finish(f'Added {worker_count} workers to cluster {name}')


k3s.add_command(k3s_expand_workers)


@k3s.command(name='expand-addresses',
             help='Add floating addresses for metallb to a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--address-count', type=click.INT, help='The number of addresses to add',
              default=2)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_expand_addresses(ctx, name=None, address_count=None, namespace=None):
    c = _bind_cluster_context(ctx, name, namespace)

    md = c.get_metadata()
    if not md:
        raise exceptions.ClusterNotFoundError.not_found(name)

    p = progress.Progress(total_phases=1, verbose=ctx.obj['VERBOSE'])
    c.progress = p
    p.phase('Adding metallb addresses')
    c.allocate_metallb_addresses(address_count)
    c.configure_metallb_addresses()
    p.finish(f'Added {address_count} metallb addresses to cluster {name}')


k3s.add_command(k3s_expand_addresses)


@k3s.command(name='update-os', help='Update the OS on all nodes')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_update_os(ctx, name=None, namespace=None):
    c = _bind_cluster_context(ctx, name, namespace)

    md = c.get_metadata()
    if not md:
        raise exceptions.ClusterNotFoundError.not_found(name)

    p = progress.Progress(total_phases=1, verbose=ctx.obj['VERBOSE'])
    c.progress = p
    p.phase('Updating the OS on all cluster nodes')
    c.instance_os_update(md['control_plane_nodes'] + md['worker_nodes'])
    p.finish(f'Updated the OS on all nodes in cluster {name}')


k3s.add_command(k3s_update_os)


def load(cli):
    cli.add_command(k3s)
