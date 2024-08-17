import click
import copy
import json
import os
from pbr.version import VersionInfo
from shakenfist_client import apiclient
import subprocess
import sys
import tempfile
import time
import yaml

from shakenfist_client_k3s import primitives


CLUSTER_LIST = 'orchestrated_k3s_clusters'


def _emit_debug(ctx, m):
    if ctx.obj['VERBOSE']:
        print(m)


@click.group(help=('k3s kubernetes cluster commands (via the '
                   'shakenfist-client-k3s plugin)'))
def k3s():
    ...


@k3s.command(name='list', help='List managed k3s clusters')
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.pass_context
def k3s_list(ctx, namespace=None, ):
    ctx.obj['CLIENT'] = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)
    if not namespace:
        namespace = ctx.obj['CLIENT'].namespace

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
              help='An option ssh public key to place onto instances.')
@click.pass_context
def k3s_create(ctx, name=None, control_plane_count=None, worker_count=None,
               metal_address_count=None,  namespace=None, network=None,
               refresh_version_cache=False, release_channel=None,
               sshkey=None):
    ctx.obj['name'] = name
    ctx.obj['namespace'] = namespace
    ctx.obj['CLIENT'] = apiclient.Client(
        async_strategy=apiclient.ASYNC_CONTINUE)

    _emit_debug(ctx, 'Looking up k3s versions')
    target_release = primitives.get_k3s_release(
        ctx, namespace, force_cache_update=refresh_version_cache,
        release_channel=release_channel)

    if namespace:
        ns = ctx.obj['CLIENT'].get_namespace(namespace)
        if not ns:
            ctx.obj['CLIENT'].create_namespace(namespace)
            print('Created namespace %s' % namespace)
    else:
        namespace = ctx.obj['CLIENT'].namespace

    # Ensure this name isn't already taken
    namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
    all_clusters = namespace_md.get(CLUSTER_LIST, [])
    md = primitives.get_cluster_metadata(ctx)

    if name in all_clusters:
        print('Sorry, that cluster name is already taken')
        sys.exit(1)
    if md:
        print('Sorry, that cluster name is already taken')
        sys.exit(1)
    all_clusters.append(name)
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, CLUSTER_LIST, all_clusters)

    # Create a network for nodes
    if network:
        node_network = ctx.obj['CLIENT'].get_network(network)
        if not node_network:
            print('Specified network does not exist')
            sys.exit(1)
    else:
        node_network = ctx.obj['CLIENT'].allocate_network(
            '10.0.0.0/16', True, True, 'k3s-%s-node' % name, namespace=namespace)
        print('Created %s as the node network (uuid %s)'
              % (node_network['name'], node_network['uuid']))
        while True:
            node_network = ctx.obj['CLIENT'].get_network(node_network['uuid'])
            if node_network['state'] == 'created':
                break
            time.sleep(1)
        print('...Node network ready')

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
        'plugin_version': VersionInfo('shakenfist_client_k3s').version_string(),
        'state': 'initial',
        'node_serial': 1,
        'node_network': node_network['uuid'],
        'node_token': None,
        'control_plane_nodes': [],
        'worker_nodes': [],
        'routed_addresses': [],
        'ssh_key': ssh_key_content
    }
    primitives.set_cluster_metadata(ctx, md)

    # We really should do a pre-fetch on the disk image and wait for it to
    # download before starting instances. That way the point of slowness is
    # more obvious. That requires cluster operations to exist though.

    # I'd prefer to wait for these as one thing, but that's not currently a thing
    # the code supports.
    print(f'Creating {control_plane_count} control plane nodes and '
          f'{worker_count} worker nodes')
    primitives.create_and_await_instances(
        ctx, control_plane_count, 'control_plane')
    primitives.create_and_await_instances(ctx, worker_count, 'worker')

    # Record the node network address for the first control plane node as the API
    # address
    interfaces = ctx.obj['CLIENT'].get_instance_interfaces(md['control_plane_nodes'][0])
    md['api_address_inner'] = interfaces[0]['ipv4']
    md['api_address_floating'] = interfaces[0]['floating']
    primitives.set_cluster_metadata(ctx, md)

    print('Configuring and installing k3s control plane')
    primitives.install_control_plane(ctx)

    print('Installing workers')
    primitives.install_workers(ctx)

    # Fetch kubecfg, correct IP, and include cluster name instead of "default"
    print('Fetching kubecfg for cluster')
    aop = ctx.obj['CLIENT'].instance_get(
        md['control_plane_nodes'][0], '/etc/rancher/k3s/k3s.yaml')
    kubeconfig = primitives.await_fetch(ctx, aop).replace(
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
    primitives.set_cluster_metadata(ctx, md)

    # Setup workers
    primitives.install_workers(ctx)

    # Install metallb
    primitives.setup_metallb(ctx, metal_address_count)

    # Fetch the kubeconfig and install it
    with tempfile.TemporaryDirectory() as tempdir:
        new_config_path = os.path.join(tempdir, 'config')
        kube_dir = os.path.join(os.path.expanduser('~'), '.kube')
        main_config_path = os.path.join(kube_dir, 'config')
        os.makedirs(kube_dir, exist_ok=True)

        with open(new_config_path, 'w') as f:
            f.write(yaml.dump(kc))
        p = subprocess.run(
            'kubectl config view --flatten', shell=True, capture_output=True,
            env={
                'KUBECONFIG': ('%s/.kube/config:%s'
                               % (os.path.expanduser('~'), new_config_path))
                })
        if p.returncode != 0:
            print('Failed up update %s/.kube/config, return code %d'
                  % (os.path.expanduser('~'), p.returncode))
            sys.exit(1)
        with open(main_config_path, 'wb') as f:
            f.write(p.stdout)

    md['state'] = 'created'
    primitives.set_cluster_metadata(ctx, md)


k3s.add_command(k3s_create)


@k3s.command(name='query-version', help='Lookup the current version for a release channel')
@click.argument('release_channel', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can control which namespace the '
                    'version cache is retrieved from.'))
@click.option('--refresh-version-cache/--no-refresh-version-cache', default=False,
              help=('Force a refresh of the k3s version cache.'))
@click.pass_context
def k3s_query_version(ctx, release_channel=None, namespace=None,
                      refresh_version_cache=False):
    target_release = primitives.get_k3s_release(
        ctx, namespace, force_cache_update=refresh_version_cache,
        release_channel=release_channel)
    print(f'Release channel {release_channel} has {target_release} as its '
          'latest version.')


@k3s.command(name='getconfig', help='Get kubeconfig for an existing k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.pass_context
def k3s_getconfig(ctx, name=None, namespace=None):
    ctx.obj['name'] = name
    ctx.obj['namespace'] = namespace
    ctx.obj['CLIENT'] = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)

    if not namespace:
        namespace = ctx.obj['CLIENT'].namespace

    md = primitives.get_cluster_metadata(ctx)
    if not md:
        print('Unknown cluster')
        sys.exit(1)

    kubeconfig = md.get('kubeconfig')
    if not kubeconfig:
        print('No kubeconfig for this cluster. Is it fully installed?')
        sys.exit(1)

    print(kubeconfig)


@k3s.command(name='show', help='Show details of a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_show(ctx, name=None, namespace=None):
    ctx.obj['name'] = name
    ctx.obj['namespace'] = namespace
    ctx.obj['CLIENT'] = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)

    md = primitives.get_cluster_metadata(ctx)
    if not md:
        print('Sorry, that cluster name does not appear to exist')
        sys.exit(1)

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
    ctx.obj['name'] = name
    ctx.obj['namespace'] = namespace
    ctx.obj['CLIENT'] = apiclient.Client(async_strategy=apiclient.ASYNC_CONTINUE)

    # Ensure this name exists
    md = primitives.get_cluster_metadata(ctx)
    if not md:
        print('Sorry, that cluster name does not appear to exist')
        sys.exit(1)

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
    primitives.set_cluster_metadata(ctx, md)

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
    primitives.set_cluster_metadata(ctx, md)

    # Then remove the metadata
    primitives.delete_cluster_metadata(ctx)
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
            print('Could not unset kubectl config element %s' % config_elem)
            sys.exit(1)


k3s.add_command(k3s_delete)


@k3s.command(name='expand-workers', help='Add workers to a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--worker-count', type=click.INT, help='The number of workers',
              default=2)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.pass_context
def k3s_expand_workers(ctx, name=None, worker_count=None, namespace=None):
    ctx.obj['name'] = name
    ctx.obj['namespace'] = namespace
    ctx.obj['CLIENT'] = apiclient.Client(
        async_strategy=apiclient.ASYNC_CONTINUE)

    md = primitives.get_cluster_metadata(ctx)
    if not md:
        print('Cluster not found!')
        sys.exit(1)

    primitives.create_and_await_instances(ctx, worker_count, 'worker')
    primitives.install_workers(ctx)


k3s.add_command(k3s_expand_workers)


@k3s.command(name='expand-addresses', help='Add workers to a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--address-count', type=click.INT, help='The number of addresses to add',
              default=2)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.pass_context
def k3s_expand_addresses(ctx, name=None, address_count=None, namespace=None):
    ctx.obj['name'] = name
    ctx.obj['namespace'] = namespace
    ctx.obj['CLIENT'] = apiclient.Client(
        async_strategy=apiclient.ASYNC_CONTINUE)

    md = primitives.get_cluster_metadata(ctx)
    if not md:
        print('Cluster not found!')
        sys.exit(1)

    primitives.allocate_metallb_addresses(ctx, address_count)
    primitives.configure_metallb_addresses(ctx)


k3s.add_command(k3s_expand_addresses)


@k3s.command(name='update-os', help='Update the OS on all nodes')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this cluster in a '
                    'different namespace.'))
@click.pass_context
def k3s_update_os(ctx, name=None, namespace=None):
    ctx.obj['name'] = name
    ctx.obj['namespace'] = namespace
    ctx.obj['CLIENT'] = apiclient.Client(
        async_strategy=apiclient.ASYNC_CONTINUE)

    md = primitives.get_cluster_metadata(ctx)
    if not md:
        print('Cluster not found!')
        sys.exit(1)

    primitives.instance_os_update(
        ctx, md['control_plane_nodes'] + md['worker_nodes'])


k3s.add_command(k3s_update_os)



def load(cli):
    cli.add_command(k3s)
