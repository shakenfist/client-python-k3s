import click
from shakenfist_client import apiclient
import sys

from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress
from shakenfist_client_k3s.cluster import Cluster


def _bind_namespace_context(ctx, namespace):
    """Build what a namespace scoped command needs.

    Returns the client, the resolved namespace and a reporter. The client
    is the one sf-client's root callback already built from --apiurl,
    --key and --namespace and left in the Click context. This plugin must
    not build its own: doing so discarded those options silently, leaving
    the configuration lookup to find credentials which may be for another
    cloud entirely. A KeyError on 'CLIENT' therefore means the group was
    invoked by something which is not sf-client, and naming the missing
    key is a better failure than quietly constructing a second client.

    The --namespace option is None unless the caller passed it, and the
    namespace is passed directly to API calls, so it must be defaulted to
    the client's own namespace here. That default now follows the
    operator's --namespace, because the client does.

    list, query-k3s-version and query-longhorn-version are namespace
    scoped rather than cluster scoped: they name no cluster, and the
    release lookups they call cache in namespace metadata. They therefore
    build no Cluster at all.
    """
    client = ctx.obj['CLIENT']

    # The orchestration runs its own wait loops and reports progress as
    # it goes, so this client must not block on our behalf.
    # async_strategy is read per call rather than at construction --
    # create_instance, delete_instance, allocate_network, _await_agentop
    # and _request_url all consult it -- and under sf-client's "pause"
    # default each of those would sit for up to a minute emitting
    # nothing before our reporter ever ran. Setting it here, rather than
    # building a second client, is what keeps the root's --apiurl, --key
    # and --namespace. Replace this with the per call or copy returning
    # API from shakenfist/client-python#404 once that ships, and raise
    # the shakenfist_client floor in pyproject.toml.
    client.async_strategy = apiclient.ASYNC_CONTINUE

    if not namespace:
        namespace = client.namespace
    return client, namespace, progress.Reporter(verbose=ctx.obj.get('VERBOSE', False))


def _bind_cluster_context(ctx, name, namespace):
    """Build the Cluster this command operates on.

    ctx.obj is read only now that the command bodies have moved onto
    Cluster: nothing in this module reads the client, the name or the
    namespace back out of the context, so they are no longer written into
    it either. VERBOSE is all that is left, and only to build the reporter.
    """
    client, namespace, reporter = _bind_namespace_context(ctx, namespace)
    return Cluster(client, name, namespace, reporter=reporter)


def _bind_new_cluster_context(ctx, name, namespace):
    """Build the Cluster create will build, making its namespace if needed.

    create is the only command which will create a namespace that does not
    exist yet, and it does so only when --namespace named one: with the
    option absent the namespace comes from the client and is known to
    exist, so nothing is looked up at all. That distinction is Click
    information -- whether an option was passed, rather than what it
    resolved to -- which is why it stays here rather than moving onto
    Cluster.create() with the rest of the body. A library caller creates
    the namespace itself.
    """
    client, resolved_namespace, reporter = _bind_namespace_context(ctx, namespace)
    if namespace:
        ns = client.get_namespace(resolved_namespace)
        if not ns:
            client.create_namespace(resolved_namespace)
            reporter.write('Created namespace %s\n' % resolved_namespace)
    return Cluster(client, name, resolved_namespace, reporter=reporter)


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
            # Terminal output, deliberately not routed through a reporter:
            # this is the Click layer converting a raised exception back
            # into the error line and exit code the command used to
            # produce directly, on the process's own stdout.
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
    client, namespace, _ = _bind_namespace_context(ctx, namespace)

    for cluster in primitives.list_clusters(client, namespace):
        # Terminal output: this command's job is formatting the cluster
        # list for a human. A library caller calls
        # primitives.list_clusters() and gets the list itself.
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
                    'network for metallb to manage. Ignored if --no-metallb is '
                    'passed.'),
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
@click.option('--metallb/--no-metallb', default=True,
              help=('Install metallb for load balancer addresses. --metal-address-count '
                    'is ignored when this is off.'))
@click.option('--longhorn/--no-longhorn', default=True,
              help='Install longhorn for persistent storage.')
@click.option('--kubeconfig/--no-kubeconfig', default=True,
              help=('Add the new cluster to your local ~/.kube/config, merging it '
                    'into any existing configuration. Cluster credentials remain '
                    "available from 'sf-client k3s getconfig' either way."))
@click.pass_context
def k3s_create(ctx, name=None, control_plane_count=None, worker_count=None,
               metal_address_count=None,  namespace=None, network=None,
               refresh_version_cache=False, release_channel=None,
               sshkey=None, metallb=True, longhorn=True, kubeconfig=True):
    c = _bind_new_cluster_context(ctx, name, namespace)
    # write_kubeconfig defaults to False in the library and True here: the
    # command line's behaviour is unchanged, and a library caller does not
    # have its ~/.kube/config edited unasked. Decision 6 of the phase 3 plan.
    c.create(control_plane_count, worker_count, metal_address_count,
             network=network, refresh_version_cache=refresh_version_cache,
             release_channel=release_channel, sshkey=sshkey,
             install_metallb=metallb, install_longhorn=longhorn,
             write_kubeconfig=kubeconfig)


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
    # Terminal output: presentation of the looked up value. A library
    # caller calls primitives.get_k3s_release() and gets target_release.
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
    # Terminal output: presentation of the looked up value. A library
    # caller calls primitives.get_longhorn_release() and gets
    # target_release.
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

    # Terminal output: this command's result. A library caller calls
    # Cluster.get_kubeconfig() and gets the string itself.
    print(c.get_kubeconfig())


@k3s.command(name='show', help='Show details of a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_show(ctx, name=None, namespace=None):
    c = _bind_cluster_context(ctx, name, namespace)
    md = c.show()

    # Terminal output: this command's result, formatted for a human. A
    # library caller calls Cluster.show() and gets the metadata dict.
    print('Cluster metadata:')
    for k in md:
        print('    %s = %s' % (k, md[k]))


k3s.add_command(k3s_show)


@k3s.command(name='health', help='Report the health of a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can report on a cluster in a '
                    'different namespace.'))
@click.pass_context
def k3s_health(ctx, name=None, namespace=None):
    c = _bind_cluster_context(ctx, name, namespace)
    _render_health(c.reporter, c.health())


def _render_health(out, report):
    """Render Cluster.health()'s report for a human.

    This writes to the reporter rather than to print(), which the older
    commands in this module use. Those pre-date the reporter and their
    comments say so; there is no reason to add another bare print() now,
    and a health check is the command most likely to be run by something
    which is also using the process's stdout for its own output.

    Nothing here decides anything: an unhealthy cluster is reported and the
    command still exits zero, because producing the report is what was
    asked for and it succeeded. A caller which wants to branch on the
    answer calls Cluster.health() and reads the dict, which is the whole
    point of decision 7 of the phase 3 plan.
    """
    out.write('Cluster %s in namespace %s is %s\n' % (
        report['name'], report['namespace'],
        'healthy' if report['healthy'] else 'NOT healthy'))

    if report['interrupted']:
        out.write("  state: %s (interrupted: this cluster never finished being "
                  'built)\n' % report['state'])
    else:
        out.write('  state: %s\n' % report['state'])

    out.write('  nodes:\n')
    if not report['nodes']:
        out.write('    this cluster has no nodes\n')
    for node in report['nodes']:
        # 'control_plane' is the metadata's spelling, and is what the
        # report carries; create_and_await_instances() does the same
        # substitution for the same reason.
        role = node['role'].replace('_', ' ')
        marker = 'ok' if node['healthy'] else '!!'
        if not node['exists']:
            out.write('    [%s] %s (%s): this instance no longer exists\n'
                      % (marker, node['uuid'], role))
            continue
        out.write('    [%s] %s (%s, %s): instance %s, agent %s\n' % (
            marker, node['name'], node['uuid'], role, node['state'],
            node['agent_state'] or 'not yet contactable'))

    api = report['api']
    if api['answered']:
        out.write('  k3s API: answered on %s\n' % api['instance_uuid'])
    else:
        out.write('  k3s API: did not answer (%s)\n' % api['error'])

    # kubectl's own output, which is the most useful thing in the report
    # when the API answered (it lists the k3s nodes and whether they are
    # Ready) and the explanation when it did not.
    for stream in ['stdout', 'stderr']:
        for line in (api.get(stream) or '').rstrip().split('\n'):
            if line:
                out.write('    %s\n' % line)
    out.flush()


k3s.add_command(k3s_health)


@k3s.command(name='delete', help='Destroy a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.option('--kubeconfig/--no-kubeconfig', default=True,
              help=('Remove the deleted cluster from your local ~/.kube/config. '
                    'This is the counterpart of the same flag on create.'))
@click.pass_context
def k3s_delete(ctx, name=None, namespace=None, kubeconfig=True):
    # As with create's --kubeconfig, the library default is off and the
    # command line passes True, so this command behaves as it always has.
    _bind_cluster_context(ctx, name, namespace).delete(
        update_kubeconfig=kubeconfig)


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
    _bind_cluster_context(ctx, name, namespace).expand_workers(worker_count)


k3s.add_command(k3s_expand_workers)


@k3s.command(name='remove-worker', help='Remove workers from a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--worker', 'workers', type=click.STRING, multiple=True,
              required=True,
              help=('The instance UUID of a worker to remove. Repeat the '
                    'option to remove more than one.'))
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_remove_worker(ctx, name=None, workers=None, namespace=None):
    # workers is a tuple, because the option is multiple=True. The library
    # API takes a list, so that a caller reading one back out of a result
    # and passing it straight in does the obvious thing.
    _bind_cluster_context(ctx, name, namespace).remove_worker(list(workers))


k3s.add_command(k3s_remove_worker)


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
    _bind_cluster_context(ctx, name, namespace).expand_addresses(address_count)


k3s.add_command(k3s_expand_addresses)


@k3s.command(name='update-os', help='Update the OS on all nodes')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_update_os(ctx, name=None, namespace=None):
    _bind_cluster_context(ctx, name, namespace).update_os()


k3s.add_command(k3s_update_os)


def load(cli):
    cli.add_command(k3s)
