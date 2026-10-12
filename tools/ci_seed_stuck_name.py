#!/usr/bin/env python3
"""List a cluster name in the namespace's cluster list, with no state behind it.

This reproduces the name a create which stopped early leaves stuck: listed
in the namespace's orchestrated_k3s_clusters metadata, with no cluster
metadata for it. tools/ci_deploy_test.sh uses it to check that delete frees
such a name, and the network a create allocated for it, against a real
Shaken Fist. A real create cannot be interrupted at the right moment on
demand, so the state it would leave is written directly.

The list is written back as a JSON list through the API client, which is
what the plugin reads. sf-client namespace set-metadata stores a string
instead, and the plugin refuses a value which is not a list.

Usage:
    python3 tools/ci_seed_stuck_name.py NAME
"""

import argparse
import sys

from shakenfist_client_k3s import primitives
from shakenfist_client_k3s.client import make_client


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('name', help='the cluster name to list')
    args = parser.parse_args()

    client = make_client()
    namespace_md = client.get_namespace_metadata(client.namespace)
    names = primitives.cluster_list(namespace_md, client.namespace)
    if args.name not in names:
        names.append(args.name)
    client.set_namespace_metadata_item(client.namespace, primitives.CLUSTER_LIST, names)
    print('Listed %s in %s: %s' % (args.name, client.namespace, names))
    return 0


if __name__ == '__main__':
    sys.exit(main())
