# Shaken Fist k3s Ansible collection (`shakenfist.k3s`)

This collection orchestrates [k3s](https://k3s.io/) clusters running on
[Shaken Fist](https://shakenfist.com/) instances. It ships a single native
Ansible module, built on the `shakenfist_client_k3s` sf-client plugin, which
talks to the Shaken Fist API directly through the `shakenfist_client` SDK.

## Installing

The collection and the Python plugin it depends on are two separate
installs, on two different machines in most deployments: the collection on
your Ansible control node, the plugin wherever its import is resolved from
(also typically the control node).

```bash
ansible-galaxy collection install shakenfist.k3s
pip install shakenfist_client_k3s
```

**The first command does not work yet.** `shakenfist.k3s` has not been
published to Galaxy, so that install fails with "Could not satisfy the
following requirements". Until it is published, build and install the
collection from a checkout:

```bash
ansible-galaxy collection build collection/ --output-path dist-collection
ansible-galaxy collection install dist-collection/shakenfist-k3s-*.tar.gz
pip install shakenfist_client_k3s
```

This note is here rather than only in `docs/collection.md` because this is
the file Galaxy renders on the collection page and the first one a reader
meets in the repository, so it is the worst place to leave a command that
cannot work. Delete it when the first version is published.

See `requirements.txt` for the minimum `shakenfist_client_k3s` version this
collection's module needs.

## Documentation

Full documentation -- the module's parameters, a worked playbook, and the
connection-parameter rules -- lives in `docs/collection.md` in the
[client-python-k3s](https://github.com/shakenfist/client-python-k3s)
repository.

## License

Apache-2.0. Copyright 2019 Michael Still and contributors.
