import copy
import json
import requests
from shakenfist_client import apiclient
import sys
import time
from versions import parse_version


METADATA_KEY = 'orchestrated_k3s_cluster_%s'
K3S_VERSION_CACHE_KEY = 'orchestrated_k3s_cluster_k3s_version_cache'
LONGHORN_VERSION_CACHE_KEY = 'orchestrated_k3s_cluster_longhorn_version_cache'
BASE_OS_VERSION = 'debian:12'


def _emit_debug(ctx, m):
    if ctx.obj['VERBOSE']:
        print(m)


def get_cluster_metadata(ctx):
    name = ctx.obj['name']
    namespace = ctx.obj['namespace']

    md_key = METADATA_KEY % name
    if md_key not in ctx.obj:
        namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
        ctx.obj[md_key] = namespace_md.get(md_key)
    return ctx.obj[md_key]


def set_cluster_metadata(ctx, md):
    name = ctx.obj['name']
    namespace = ctx.obj['namespace']

    md_key = METADATA_KEY % name
    ctx.obj[md_key] = md
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, md_key, md)


def delete_cluster_metadata(ctx):
    name = ctx.obj['name']
    namespace = ctx.obj['namespace']

    md_key = METADATA_KEY % name
    del ctx.obj[md_key]
    ctx.obj['CLIENT'].delete_namespace_metadata_item(namespace, md_key)


def get_k3s_release(ctx, force_cache_update=False, release_channel=None):
    namespace = ctx.obj['namespace']

    if force_cache_update:
        version_cache = {'updated': 0}
        _emit_debug(ctx, 'Forcing cache update')
    else:
        namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            K3S_VERSION_CACHE_KEY, {'updated': 0, 'releases': {}})
        if not isinstance(version_cache, dict):
            _emit_debug(ctx, 'Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    _emit_debug(ctx, (f'Cached version information from {updated}: '
                      f'{version_cache.get('releases', {})}'))

    if time.time() - updated > 24 * 3600:
        _emit_debug(ctx, 'Updating release version cache')

        url = 'https://update.k3s.io/v1-release/channels'
        _emit_debug(ctx, f'Fetching {url}')
        r = requests.request(
            'GET', url,
            headers={
                'Accept': 'application/json',
                'User-Agent': apiclient.get_user_agent()
            })
        if r.status_code not in [200, 201, 204]:
            print('Unable to determine latest k3s release version')
            print('    GET {url}')
            print(f'    returned HTTP status code {r.status_code} with text:')
            print(f'    {r.text}')
            sys.exit(1)

        d = r.json()
        releases = {}
        _emit_debug(ctx, 'Fetched release data:')
        _emit_debug(ctx, json.dumps(d, indent=4, sort_keys=True))
        for reldata in d['data']:
            releases[reldata['name']] = reldata['latest']

        version_cache['releases'] = releases
        version_cache['updated'] = time.time()
        ctx.obj['CLIENT'].set_namespace_metadata_item(
            namespace, K3S_VERSION_CACHE_KEY, version_cache)

    most_recent = version_cache['releases'].get(release_channel, None)
    if not most_recent:
        print(f'Release channel {release_channel} not found')
        sys.exit(1)

    _emit_debug(ctx, f'Selected kubernetes version: {most_recent}')
    return most_recent


def get_longhorn_release(ctx, force_cache_update=False):
    namespace = ctx.obj['namespace']

    if force_cache_update:
        version_cache = {'updated': 0}
        _emit_debug(ctx, 'Forcing cache update')
    else:
        namespace_md = ctx.obj['CLIENT'].get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            LONGHORN_VERSION_CACHE_KEY, {'updated': 0, 'releases': {}})
        if not isinstance(version_cache, dict):
            _emit_debug(ctx, 'Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    _emit_debug(ctx, (f'Cached version information from {updated}: '
                      f'{version_cache.get('releases', {})}'))

    if time.time() - updated > 24 * 3600:
        _emit_debug(ctx, 'Updating release version cache')

        releases = {}
        for page in range(5):
            url = f'https://api.github.com/repos/longhorn/longhorn/releases?page={
                page}'
            _emit_debug(ctx, f'Fetching {url}')
            r = requests.request(
                'GET', url,
                headers={
                    'Accept': 'application/vnd.github+json',
                    'User-Agent': apiclient.get_user_agent()
                })

            if r.status_code not in [200, 201, 204]:
                print(
                    'Unable to determine latest k3s release version\n'
                    f'    GET {url}\n'
                    f'    returned HTTP status code {r.status_code} '
                    'with text:\n'
                    f'    {r.text}')
                sys.exit(1)

            d = r.json()
            _emit_debug(ctx, 'Fetched release data:')
            _emit_debug(ctx, json.dumps(d, indent=4, sort_keys=True))
            for reldata in d:
                if reldata['prerelease']:
                    continue
                tagname = reldata['tag_name'].lstrip('v')
                releases[tagname] = reldata['tarball_url']

        # Find the most recent version
        latest = None
        for tagname in list(releases.keys()):
            parsed_version = parse_version(tagname)
            if not latest:
                latest = parsed_version
            elif parsed_version > latest:
                latest = parsed_version

        version_cache['releases'] = releases
        version_cache['latest'] = latest.to_string()
        version_cache['updated'] = time.time()
        ctx.obj['CLIENT'].set_namespace_metadata_item(
            namespace, LONGHORN_VERSION_CACHE_KEY, version_cache)

    return version_cache['latest']


def create_instance(ctx):
    md = get_cluster_metadata(ctx)

    node_name = 'k3s-%s-node-%03d' % (md['name'], md['node_serial'])
    inst = ctx.obj['CLIENT'].create_instance(
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
        side_channels=['sf-agent'],
        namespace=md['namespace']
    )
    return inst


def await_boot(ctx, instances):
    waiting = copy.copy(instances)
    while waiting:
        print('Waiting for %d instances to boot' % len(waiting))
        for instance_uuid in copy.copy(waiting):
            inst = ctx.obj['CLIENT'].get_instance(instance_uuid)
            print('...instance %s has state %s and agent state %s'
                  % (inst['name'], inst['state'], inst['agent_state']))
            if inst['state'] == 'created' and inst['agent_state'] == 'ready':
                waiting.remove(instance_uuid)

        if not waiting:
            break
        time.sleep(5)

    instance_os_update(ctx, instances)


def await_idle(ctx, instances):
    waiting = copy.copy(instances)
    while waiting:
        print('Waiting for %d instances to be idle' % len(waiting))
        for instance_uuid in copy.copy(waiting):
            inst = ctx.obj['CLIENT'].get_instance(instance_uuid)
            agent_ops = ctx.obj['CLIENT'].get_instance_agentoperations(
                instance_uuid, all=True)

            incomplete = 0
            for aop in agent_ops:
                if aop['state'] != 'complete':
                    incomplete += 1
            print('...instance %s has %d incomplete agent operations'
                  % (inst['name'], incomplete))

            if incomplete == 0:
                waiting.remove(instance_uuid)

        if not waiting:
            break
        time.sleep(5)


def await_fetch(ctx, aop):
    while aop['state'] not in ['complete', 'error']:
        print(f'...fetch operation has state {aop['state']}')
        time.sleep(1)
        aop = ctx.obj['CLIENT'].get_agent_operation(aop['uuid'])

    if aop['state'] == 'error':
        print('File fetch failed:')
        print('  path: %s' % aop['results']['0']['path'])
        print('  message: %s' % aop['results']['0']['message'])
        sys.exit(1)

    blob_uuid = aop['results']['0']['content_blob']
    data = b''
    for chunk in ctx.obj['CLIENT'].get_blob_data(blob_uuid):
        data += chunk
    return data.decode('utf-8')


def reap_execute(ctx, aop):
    while aop['state'] != 'complete':
        time.sleep(1)
        aop = ctx.obj['CLIENT'].get_agent_operation(aop['uuid'])

    if aop['results']['0']['return-code'] != 0:
        inst = ctx.obj['CLIENT'].get_instance(aop['instance_uuid'])

        print('Command failed!')
        print('  instance: %s (UUID %s)'
              % (inst['name'], aop['instance_uuid']))
        print('  command: %s' % aop['commands'][0]['commandline'])
        print('exit code: %s' % aop['results']['0']['return-code'])
        print('   stdout: %s' % '\n   stdout: '.join(
            aop['results']['0']['stdout'].split('\n')))
        print('   stderr: %s' % '\n   stderr'.join(
            aop['results']['0']['stderr'].split('\n')))
        sys.exit(1)


def create_and_await_instances(ctx, count, node_type):
    md = get_cluster_metadata(ctx)

    new_nodes = []
    for i in range(count):
        inst = create_instance(ctx)
        new_nodes.append(inst['uuid'])
        md['node_serial'] += 1
        md[f'{node_type}_nodes'].append(inst['uuid'])
        set_cluster_metadata(ctx, md)
        print(f'Created {inst['name']} as a {node_type} node '
              f'(uuid {inst['uuid']})')

    await_boot(ctx, new_nodes)
    set_cluster_metadata(ctx, md)


def execute_and_await(ctx, instance_uuids, cmds):
    aops = []
    for cmd in cmds:
        for instance_uuid in instance_uuids:
            aops.append(ctx.obj['CLIENT'].instance_execute(
                instance_uuid, cmd))

    # Wait for instances to be idle and check results
    await_idle(ctx, instance_uuids)
    for aop in aops:
        reap_execute(ctx, aop)


def instance_os_update(ctx, instance_uuids):
    execute_and_await(
        ctx, instance_uuids,
        [
            'apt-get update',
            'apt-get dist-upgrade -y'
        ]
    )


def install_control_plane(ctx):
    md = get_cluster_metadata(ctx)
    cmds = []

    print('Setup first control plane node')

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
    cmds.append(
        'curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | '
        'sudo tee /usr/share/keyrings/helm.gpg')
    cmds.append('sudo apt-get install -y apt-transport-https')
    cmds.append(
        'echo "deb [arch=$(dpkg --print-architecture) '
        'signed-by=/usr/share/keyrings/helm.gpg] '
        'https://baltocdn.com/helm/stable/debian/ all main" | '
        'sudo tee /etc/apt/sources.list.d/helm-stable-debian.list')
    cmds.append('sudo apt-get update')
    cmds.append('sudo apt-get install -y helm')

    execute_and_await(ctx, [md['control_plane_nodes'][0]], cmds)

    # Fetch the server and node tokens from the first control plane node
    print('Fetching control plane registration token from first control plane node')
    aop = ctx.obj['CLIENT'].instance_get(
        md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/token')
    md['server_token'] = await_fetch(ctx, aop).rstrip()
    set_cluster_metadata(ctx, md)

    print('Fetching node registration token from first control plane node')
    aop = ctx.obj['CLIENT'].instance_get(
        md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/node-token')
    md['node_token'] = await_fetch(ctx, aop).rstrip()
    set_cluster_metadata(ctx, md)

    # If there is more than one control plane node, then install the others
    if len(md['control_plane_nodes']) > 1:
        print('Setup other control plane nodes')
        install_extra_control_plane(ctx)


def install_k3s_component(ctx, instance_uuids, token, node_role):
    md = get_cluster_metadata(ctx)

    execute_and_await(
        ctx, instance_uuids,
        [
            'sudo apt-get update',
            'sudo apt-get install -y',
            (
                'curl -sfL https://get.k3s.io | '
                f'INSTALL_K3S_CHANNEL={md['k3s_version']} '
                f'K3S_URL=https://{md['api_address_inner']}:6443 '
                f'K3S_TOKEN={token} sh -s - {node_role}'
            )
        ]
    )

    set_cluster_metadata(ctx, md)


def install_extra_control_plane(ctx):
    md = get_cluster_metadata(ctx)
    install_k3s_component(
        ctx, md['control_plane_nodes'][1:], md['server_token'], 'server')


def install_workers(ctx):
    md = get_cluster_metadata(ctx)
    install_k3s_component(ctx, md['worker_nodes'], md['node_token'], 'agent')


def allocate_metallb_addresses(ctx, metal_address_count):
    md = get_cluster_metadata(ctx)
    node_network = ctx.obj['CLIENT'].get_network(md['node_network'])

    for i in range(metal_address_count):
        addr = ctx.obj['CLIENT'].route_network_address(node_network['uuid'])
        if addr:
            md['routed_addresses'].append(addr)
            print('Allocated routed address %s' % addr)
    print('Allocated %d routed addresses' % len(md['routed_addresses']))
    set_cluster_metadata(ctx, md)


def configure_metallb_addresses(ctx):
    md = get_cluster_metadata(ctx)

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

    execute_and_await(
        ctx, [md['control_plane_nodes'][0]],
        [
            ('kubectl wait --kubeconfig /etc/rancher/k3s/k3s.yaml -n metallb-system pod '
             '--for=condition=Ready -l app.kubernetes.io/name=metallb --timeout=300s'),
            'mkdir -p /etc/sf',
            metal_lb_config,
            'kubectl apply -f /etc/sf/metallb-range-allocation.yaml'
        ]
    )


def setup_metallb(ctx, metal_address_count):
    md = get_cluster_metadata(ctx)

    print('Setting up metallb')
    allocate_metallb_addresses(ctx, metal_address_count)
    execute_and_await(
        ctx, [md['control_plane_nodes'][0]],
        [
            'kubectl create ns metallb-system',
            ('KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm '
             'upgrade --install -n metallb-system metallb '
             'oci://registry-1.docker.io/bitnamicharts/metallb'),
        ])

    # Let the metallb pods start
    time.sleep(5)

    # Add addresses
    configure_metallb_addresses(ctx)


def setup_longhorn(ctx):
    md = get_cluster_metadata(ctx)

    version = get_longhorn_release(ctx)
    print(f'Setting up longhorn version {version}')

    execute_and_await(
        ctx, [md['control_plane_nodes'][0]],
        [
            'helm repo add longhorn https://charts.longhorn.io',
            'helm repo update',
            'kubectl create namespace longhorn-system || true',
            (
                'KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm '
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
