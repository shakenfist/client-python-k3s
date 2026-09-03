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

# How long, in seconds, a single agent command can run before the wait loop
# notes that it might be stalled.
STALL_WARNING_SECONDS = 300


def _emit_debug(cluster, m):
    if cluster.reporter.verbose:
        print(m)


def get_k3s_release(cluster, force_cache_update=False, release_channel=None):
    namespace = cluster.namespace

    if force_cache_update:
        version_cache = {'updated': 0}
        _emit_debug(cluster, 'Forcing cache update')
    else:
        namespace_md = cluster.client.get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            K3S_VERSION_CACHE_KEY, {'updated': 0, 'releases': {}})
        if not isinstance(version_cache, dict) or 'releases' not in version_cache:
            _emit_debug(cluster, 'Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    _emit_debug(cluster, (f'Cached version information from {updated}: '
                          f'{version_cache.get("releases", {})}'))

    if time.time() - updated > 24 * 3600:
        _emit_debug(cluster, 'Updating release version cache')

        url = 'https://update.k3s.io/v1-release/channels'
        _emit_debug(cluster, f'Fetching {url}')
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
        _emit_debug(cluster, 'Fetched release data:')
        _emit_debug(cluster, json.dumps(d, indent=4, sort_keys=True))
        for reldata in d.get('data', []):
            # Some channels (for example v1.16-testing) have no released
            # version and therefore no 'latest' key.
            if 'name' not in reldata or 'latest' not in reldata:
                _emit_debug(cluster, (f'Channel {reldata.get("name")} has no latest release, '
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
        cluster.client.set_namespace_metadata_item(
            namespace, K3S_VERSION_CACHE_KEY, version_cache)

    most_recent = version_cache['releases'].get(release_channel, None)
    if not most_recent:
        print(f'Release channel {release_channel} not found')
        sys.exit(1)

    _emit_debug(cluster, f'Selected kubernetes version: {most_recent}')
    return most_recent


def get_longhorn_release(cluster, force_cache_update=False):
    namespace = cluster.namespace

    if force_cache_update:
        version_cache = {'updated': 0}
        _emit_debug(cluster, 'Forcing cache update')
    else:
        namespace_md = cluster.client.get_namespace_metadata(namespace)
        version_cache = namespace_md.get(
            LONGHORN_VERSION_CACHE_KEY, {'updated': 0, 'releases': {}})
        if not isinstance(version_cache, dict) or 'latest' not in version_cache:
            _emit_debug(cluster, 'Version cache format invalid, clobbering')
            version_cache = {'updated': 0}

    updated = version_cache.get('updated', 0)

    _emit_debug(cluster, (f'Cached version information from {updated}: '
                          f'{version_cache.get("releases", {})}'))

    if time.time() - updated > 24 * 3600:
        _emit_debug(cluster, 'Updating release version cache')

        releases = {}
        for page in range(5):
            url = f'https://api.github.com/repos/longhorn/longhorn/releases?page={page}'
            _emit_debug(cluster, f'Fetching {url}')
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
            _emit_debug(cluster, 'Fetched release data:')
            _emit_debug(cluster, json.dumps(d, indent=4, sort_keys=True))
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
                _emit_debug(cluster, f'Skipping unparsable tag {tagname}')
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
        cluster.client.set_namespace_metadata_item(
            namespace, LONGHORN_VERSION_CACHE_KEY, version_cache)

    return version_cache['latest']


def create_instance(cluster):
    md = cluster.get_metadata()

    node_name = 'k3s-%s-node-%03d' % (md['name'], md['node_serial'])
    inst = cluster.client.create_instance(
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


def _describe_agent_op(aop, max_len=60):
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

    # Multi-line commands (for example heredocs) would break the one line
    # per item status display, so describe them by their first line.
    if '\n' in desc:
        desc = desc.split('\n', 1)[0] + ' ...'

    if max_len and len(desc) > max_len:
        desc = desc[:max_len - 3] + '...'
    return desc


def _abort_agent_op_error(cluster, aop):
    """Report an agent operation which entered the error state, then exit."""
    inst = cluster.client.get_instance(aop['instance_uuid'])

    print('Agent operation failed!')
    print('  instance: %s (uuid %s)' % (inst['name'], aop['instance_uuid']))
    print('  operation: %s' % aop['uuid'])
    desc = _describe_agent_op(aop, max_len=None)
    if desc:
        print('  command: %s' % desc)

    results = aop.get('results', {}) or {}
    if results:
        print('  results: %s' % json.dumps(results, indent=4, sort_keys=True))
    else:
        print('  no results were recorded, so the command probably failed to start')
    print("  the server side event log may have more detail: 'sf-client instance events %s'" % inst['name'])
    sys.exit(1)


def await_boot(cluster, instances):
    p = cluster.get_progress()
    waiting = copy.copy(instances)
    while waiting:
        for instance_uuid in copy.copy(waiting):
            inst = cluster.client.get_instance(instance_uuid)
            agent_state = inst['agent_state'] if inst['agent_state'] else 'not yet contactable'
            p.update(inst['name'], 'state %s, agent %s' % (inst['state'], agent_state))
            if inst['state'] == 'created' and inst['agent_state'] == 'ready':
                waiting.remove(instance_uuid)

        if not waiting:
            break
        time.sleep(5)
    p.wait_done()


def await_idle(cluster, instances):
    p = cluster.get_progress()
    waiting = copy.copy(instances)

    # Agent operations stay associated with an instance forever, and an
    # operation in the error state will never complete. Snapshot any which
    # had already failed before this wait started so a historical failure
    # can neither wedge this wait nor incorrectly abort it.
    preexisting_errors = {}
    for instance_uuid in waiting:
        aops = cluster.client.get_instance_agentoperations(instance_uuid, all=True)
        preexisting_errors[instance_uuid] = {
            aop['uuid'] for aop in aops if aop['state'] == 'error'}

    running_since = {}
    stall_warned = set()

    while waiting:
        for instance_uuid in copy.copy(waiting):
            inst = cluster.client.get_instance(instance_uuid)
            agent_ops = cluster.client.get_instance_agentoperations(
                instance_uuid, all=True)
            agent_ops = [aop for aop in agent_ops
                         if aop['uuid'] not in preexisting_errors[instance_uuid]]

            errored = [aop for aop in agent_ops if aop['state'] == 'error']
            if errored:
                _abort_agent_op_error(cluster, errored[0])

            incomplete = [aop for aop in agent_ops if aop['state'] != 'complete']
            if not incomplete:
                p.update(inst['name'], 'idle')
                waiting.remove(instance_uuid)
            else:
                aop = incomplete[0]
                desc = _describe_agent_op(aop)
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


def await_fetch(cluster, aop):
    p = cluster.get_progress()
    while aop['state'] not in ['complete', 'error']:
        p.update('fetch operation', 'state %s' % aop['state'])
        time.sleep(1)
        aop = cluster.client.get_agent_operation(aop['uuid'])
    p.wait_done()

    if aop['state'] == 'error':
        _abort_agent_op_error(cluster, aop)

    blob_uuid = aop['results']['0']['content_blob']
    data = b''
    for chunk in cluster.client.get_blob_data(blob_uuid):
        data += chunk
    return data.decode('utf-8')


def reap_execute(cluster, aop):
    while aop['state'] not in ('complete', 'error'):
        time.sleep(1)
        aop = cluster.client.get_agent_operation(aop['uuid'])

    if aop['state'] == 'error':
        _abort_agent_op_error(cluster, aop)

    if aop['results']['0']['return-code'] != 0:
        inst = cluster.client.get_instance(aop['instance_uuid'])

        print('Command failed!')
        print('  instance: %s (UUID %s)'
              % (inst['name'], aop['instance_uuid']))
        print('  command: %s' % aop['commands'][0]['commandline'])
        print('exit code: %s' % aop['results']['0']['return-code'])
        print('   stdout: %s' % '\n   stdout: '.join(
            aop['results']['0']['stdout'].split('\n')))
        print('   stderr: %s' % '\n   stderr: '.join(
            aop['results']['0']['stderr'].split('\n')))
        sys.exit(1)


def create_and_await_instances(cluster, count, node_type):
    p = cluster.get_progress()
    md = cluster.get_metadata()

    display_type = node_type.replace('_', ' ')
    p.phase('Creating %s' % progress.count_str(count, '%s node' % display_type))

    new_nodes = []
    for i in range(count):
        inst = create_instance(cluster)
        new_nodes.append(inst['uuid'])
        md['node_serial'] += 1
        md[f'{node_type}_nodes'].append(inst['uuid'])
        cluster.set_metadata(md)
        p.note(f'created {inst["name"]} (uuid {inst["uuid"]})')

    await_boot(cluster, new_nodes)
    p.note('updating base OS packages')
    instance_os_update(cluster, new_nodes)
    cluster.set_metadata(md)


def execute_and_await(cluster, instance_uuids, cmds):
    aops = []
    for cmd in cmds:
        for instance_uuid in instance_uuids:
            aops.append(cluster.client.instance_execute(
                instance_uuid, cmd))

    # Wait for instances to be idle and check results
    await_idle(cluster, instance_uuids)
    for aop in aops:
        reap_execute(cluster, aop)


def instance_os_update(cluster, instance_uuids):
    execute_and_await(
        cluster, instance_uuids,
        [
            'apt-get update',
            'apt-get dist-upgrade -y'
        ]
    )


def install_control_plane(cluster):
    p = cluster.get_progress()
    md = cluster.get_metadata()
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

    execute_and_await(cluster, [md['control_plane_nodes'][0]], cmds)

    # Fetch the server and node tokens from the first control plane node
    p.note('fetching control plane registration token')
    aop = cluster.client.instance_get(
        md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/token')
    md['server_token'] = await_fetch(cluster, aop).rstrip()
    cluster.set_metadata(md)

    p.note('fetching node registration token')
    aop = cluster.client.instance_get(
        md['control_plane_nodes'][0], '/var/lib/rancher/k3s/server/node-token')
    md['node_token'] = await_fetch(cluster, aop).rstrip()
    cluster.set_metadata(md)

    # If there is more than one control plane node, then install the others
    if len(md['control_plane_nodes']) > 1:
        install_extra_control_plane(cluster)


def install_k3s_component(cluster, instance_uuids, token, node_role):
    md = cluster.get_metadata()

    # Nodes must join via an address inside the node network: the network
    # node neither hairpins floating addresses nor routes in-network
    # traffic to the network's own routed addresses (see
    # shakenfist/shakenfist#3662). Clusters created before join_address
    # existed only have api_address_inner.
    join_address = md.get('join_address', md['api_address_inner'])

    execute_and_await(
        cluster, instance_uuids,
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

    cluster.set_metadata(md)


def install_extra_control_plane(cluster):
    p = cluster.get_progress()
    md = cluster.get_metadata()
    p.phase('Installing k3s on the additional control plane nodes')
    install_k3s_component(
        cluster, md['control_plane_nodes'][1:], md['server_token'], 'server')


def install_workers(cluster):
    p = cluster.get_progress()
    md = cluster.get_metadata()
    p.phase('Installing k3s on the worker nodes')
    install_k3s_component(cluster, md['worker_nodes'], md['node_token'], 'agent')


def allocate_metallb_addresses(cluster, metal_address_count):
    p = cluster.get_progress()
    md = cluster.get_metadata()
    node_network = cluster.client.get_network(md['node_network'])

    allocated = []
    for i in range(metal_address_count):
        addr = cluster.client.route_network_address(node_network['uuid'])
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
    cluster.set_metadata(md)


def configure_metallb_addresses(cluster):
    md = cluster.get_metadata()

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
        cluster, [md['control_plane_nodes'][0]],
        [
            ('kubectl wait --kubeconfig /etc/rancher/k3s/k3s.yaml -n metallb-system pod '
             '--for=condition=Ready -l app.kubernetes.io/name=metallb --timeout=300s'),
            'mkdir -p /etc/sf',
            metal_lb_config,
            'kubectl apply -f /etc/sf/metallb-range-allocation.yaml'
        ]
    )


def setup_metallb(cluster, metal_address_count):
    p = cluster.get_progress()
    md = cluster.get_metadata()

    p.phase('Setting up metallb')
    allocate_metallb_addresses(cluster, metal_address_count)
    execute_and_await(
        cluster, [md['control_plane_nodes'][0]],
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
    configure_metallb_addresses(cluster)


def setup_longhorn(cluster):
    p = cluster.get_progress()
    md = cluster.get_metadata()

    version = get_longhorn_release(cluster)
    p.phase(f'Setting up longhorn version {version}')

    execute_and_await(
        cluster, [md['control_plane_nodes'][0]],
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
