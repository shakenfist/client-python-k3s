import click
from shakenfist_client import apiclient
import sys

from shakenfist_client_k3s import exceptions
from shakenfist_client_k3s import primitives
from shakenfist_client_k3s import progress
from shakenfist_client_k3s.cluster import Cluster


def _bind_namespace_context(ctx, namespace):
    """Build what a namespace scoped command needs.

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
    c = _bind_new_cluster_context(ctx, name, namespace)
    c.create(control_plane_count, worker_count, metal_address_count,
             network=network, refresh_version_cache=refresh_version_cache,
             release_channel=release_channel, sshkey=sshkey)


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


@k3s.command(name='delete', help='Destroy a k3s cluster')
@click.argument('name', type=click.STRING)
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can alter clusters in a '
                    'different namespace.'))
@click.pass_context
def k3s_delete(ctx, name=None, namespace=None):
    _bind_cluster_context(ctx, name, namespace).delete()


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
