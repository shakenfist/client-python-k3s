import copy
import json
from packaging.version import InvalidVersion, Version
import requests
from shakenfist_client import apiclient
import sys
import time

from shakenfist_client_k3s import progress


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
        if not isinstance(version_cache, dict) or 'releases' not in version_cache:
            _emit_debug(ctx, 'Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    _emit_debug(ctx, (f'Cached version information from {updated}: '
                      f'{version_cache.get("releases", {})}'))

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
            print(f'    GET {url}')
            print(f'    returned HTTP status code {r.status_code} with text:')
            print(f'    {r.text}')
            sys.exit(1)

        d = r.json()
        releases = {}
        _emit_debug(ctx, 'Fetched release data:')
        _emit_debug(ctx, json.dumps(d, indent=4, sort_keys=True))
        for reldata in d.get('data', []):
            # Some channels (for example v1.16-testing) have no released
            # version and therefore no 'latest' key.
            if 'name' not in reldata or 'latest' not in reldata:
                _emit_debug(ctx, (f'Channel {reldata.get("name")} has no latest release, '
                                  'skipping'))
                continue
            releases[reldata['name']] = reldata['latest']

        # Don't persist an empty parse result: a transient upstream error
        # would otherwise poison the shared namespace cache until it next
        # expires. This mirrors the 'latest is None' guard in
        # get_longhorn_release().
        if not releases:
            print('No usable k3s release channels found')
            print(f'    GET {url}')
            print(f'    returned: {json.dumps(d)[:512]}')
            sys.exit(1)

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
        if not isinstance(version_cache, dict) or 'latest' not in version_cache:
            _emit_debug(ctx, 'Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    _emit_debug(ctx, (f'Cached version information from {updated}: '
                      f'{version_cache.get("releases", {})}'))

    if time.time() - updated > 24 * 3600:
        _emit_debug(ctx, 'Updating release version cache')

        releases = {}
        for page in range(5):
            url = f'https://api.github.com/repos/longhorn/longhorn/releases?page={page}'
            _emit_debug(ctx, f'Fetching {url}')
            r = requests.request(
                'GET', url,
                headers={
                    'Accept': 'application/vnd.github+json',
                    'User-Agent': apiclient.get_user_agent()
                })

            if r.status_code not in [200, 201, 204]:
                print(
                    'Unable to determine latest Longhorn release version\n'
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

        # Find the most recent version. Longhorn has occasionally
        # published tags which are not valid PEP 440 versions (for
        # example v1.4.0-hotfix1), so skip anything unparsable.
        latest = None
        for tagname in list(releases.keys()):
            try:
                parsed_version = Version(tagname)
            except InvalidVersion:
                _emit_debug(ctx, f'Skipping unparsable tag {tagname}')
                continue
            if not latest:
                latest = parsed_version
            elif parsed_version > latest:
                latest = parsed_version

        if latest is None:
            print('Unable to determine the latest Longhorn release')
            sys.exit(1)

        version_cache['releases'] = releases
        version_cache['latest'] = str(latest)
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
        side_channels=['sf-agent2'],
        namespace=md['namespace']
    )
    return inst


def _describe_agent_op(aop):
    """Return a short human readable description of the command an agent operation is up to."""
    commands = aop.get('commands', [])
    results = aop.get('results', {}) or {}

    # Results are recorded per command index as they complete, so the number
    # of results is the index of the currently executing command.
    idx = min(len(results), len(commands) - 1)
    if idx < 0:
        return None

    c = commands[idx]
    desc = c.get('commandline')
    if not desc:
        desc = c.get('command', 'unknown')
        if c.get('path'):
            desc += ' %s' % c['path']
    if len(desc) > 60:
        desc = desc[:57] + '...'
    return desc


def await_boot(ctx, instances):
    p = progress.get_progress(ctx)
    waiting = copy.copy(instances)
    while waiting:
        for instance_uuid in copy.copy(waiting):
            inst = ctx.obj['CLIENT'].get_instance(instance_uuid)
            agent_state = inst['agent_state'] if inst['agent_state'] else 'not yet contactable'
            p.update(inst['name'], 'state %s, agent %s' % (inst['state'], agent_state))
            if inst['state'] == 'created' and inst['agent_state'] == 'ready':
                waiting.remove(instance_uuid)

        if not waiting:
            break
        time.sleep(5)
    p.wait_done()


def await_idle(ctx, instances):
    p = progress.get_progress(ctx)
    waiting = copy.copy(instances)
    while waiting:
        for instance_uuid in copy.copy(waiting):
            inst = ctx.obj['CLIENT'].get_instance(instance_uuid)
            agent_ops = ctx.obj['CLIENT'].get_instance_agentoperations(
                instance_uuid, all=True)

            incomplete = [aop for aop in agent_ops if aop['state'] != 'complete']
            if not incomplete:
                p.update(inst['name'], 'idle')
                waiting.remove(instance_uuid)
            else:
                desc = _describe_agent_op(incomplete[0])
                remaining = progress.count_str(len(incomplete), 'operation')
                if desc:
                    p.update(inst['name'], "running '%s' (%s remaining)" % (desc, remaining))
                else:
                    p.update(inst['name'], '%s remaining' % remaining)

        if not waiting:
            break
        time.sleep(5)
    p.wait_done()


def await_fetch(ctx, aop):
    p = progress.get_progress(ctx)
    while aop['state'] not in ['complete', 'error']:
        p.update('fetch operation', 'state %s' % aop['state'])
        time.sleep(1)
        aop = ctx.obj['CLIENT'].get_agent_operation(aop['uuid'])
    p.wait_done()

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
    p = progress.get_progress(ctx)
    md = get_cluster_metadata(ctx)

    display_type = node_type.replace('_', ' ')
    p.phase('Creating %s' % progress.count_str(count, '%s node' % display_type))

    new_nodes = []
    for i in range(count):
        inst = create_instance(ctx)
        new_nodes.append(inst['uuid'])
        md['node_serial'] += 1
        md[f'{node_type}_nodes'].append(inst['uuid'])
        set_cluster_metadata(ctx, md)
        p.note(f'created {inst["name"]} (uuid {inst["uuid"]})')

    await_boot(ctx, new_nodes)
    p.note('updating base OS packages')
    instance_os_update(ctx, new_nodes)
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
    p = progress.get_progress(ctx)
    md = get_cluster_metadata(ctx)
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

    execute_and_await(ctx, [md['control_plane_nodes'][0]], cmds)

    # Fetch the server and node tokens from the first control plane node
    p.note('fetching control plane registration token')
    aop = ctx.obj['CLIENT'].instance_get(
        md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/token')
    md['server_token'] = await_fetch(ctx, aop).rstrip()
    set_cluster_metadata(ctx, md)

    p.note('fetching node registration token')
    aop = ctx.obj['CLIENT'].instance_get(
        md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/node-token')
    md['node_token'] = await_fetch(ctx, aop).rstrip()
    set_cluster_metadata(ctx, md)

    # If there is more than one control plane node, then install the others
    if len(md['control_plane_nodes']) > 1:
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
                f'INSTALL_K3S_CHANNEL={md["k3s_version"]} '
                f'K3S_URL=https://{md["api_address_inner"]}:6443 '
                f'K3S_TOKEN={token} sh -s - {node_role}'
            )
        ]
    )

    set_cluster_metadata(ctx, md)


def install_extra_control_plane(ctx):
    p = progress.get_progress(ctx)
    md = get_cluster_metadata(ctx)
    p.phase('Installing k3s on the additional control plane nodes')
    install_k3s_component(
        ctx, md['control_plane_nodes'][1:], md['server_token'], 'server')


def install_workers(ctx):
    p = progress.get_progress(ctx)
    md = get_cluster_metadata(ctx)
    p.phase('Installing k3s on the worker nodes')
    install_k3s_component(ctx, md['worker_nodes'], md['node_token'], 'agent')


def allocate_metallb_addresses(ctx, metal_address_count):
    p = progress.get_progress(ctx)
    md = get_cluster_metadata(ctx)
    node_network = ctx.obj['CLIENT'].get_network(md['node_network'])

    allocated = []
    for i in range(metal_address_count):
        addr = ctx.obj['CLIENT'].route_network_address(node_network['uuid'])
        if addr:
            md['routed_addresses'].append(addr)
            allocated.append(addr)
    p.note('allocated %s: %s' % (
        progress.count_str(len(allocated), 'routed address'), ', '.join(allocated)))
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
    p = progress.get_progress(ctx)
    md = get_cluster_metadata(ctx)

    p.phase('Setting up metallb')
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
    p = progress.get_progress(ctx)
    md = get_cluster_metadata(ctx)

    version = get_longhorn_release(ctx)
    p.phase(f'Setting up longhorn version {version}')

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
