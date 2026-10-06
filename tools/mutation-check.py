#!/usr/bin/env python3
"""Break each defended property on purpose, and confirm a test says so.

A green suite says the tests pass. It does not say any of them would
have failed, and the difference matters most for the properties nobody
exercises by hand -- a secret that must not reach stderr, a heredoc body
that must not be able to end itself, a file that must be created 0600.
Reading the test cannot tell "this holds" from "this cannot fail".

So each entry below is a one-line edit which makes a stated property
false, together with the test that is supposed to notice. The script
applies one, runs that test, requires a failure, and puts the file back
from a copy taken before the edit. From a copy rather than
``git checkout``, because a directory-wide checkout discards whatever
else is uncommitted in that directory, which is easy to do in a script
and painful to find out about afterwards.

Not wired into CI: it rewrites the working tree, and it costs one test
run per mutation. Run it by hand when the properties change, and when
responding to a review -- the count is quoted in the pull request, so
the set visibly grows rather than being re-improvised each round.

Usage:
    python3 tools/mutation-check.py [-v] [--only SUBSTRING]
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PKG = 'shakenfist_client_k3s'

# (what the mutation falsifies, file, text to find, text to put there,
#  the test which must fail, how to run it).
#
# Keep the replacement minimal. A mutation which breaks an import, or
# which breaks half the suite, proves nothing: every test fails and none
# of them is evidence about this property.
#
# The last field is 'stestr' or 'tox'. It is not a preference. The
# Ansible module harness runs the module as a script, so sys.path[0] is
# the tests directory and the repository root is never on the path: those
# tests import the *installed* package, and a bare `stestr run` therefore
# tests whatever was last installed rather than the working tree. A
# mutation checked that way silently survives -- which is how this was
# found, and it is shakenfist/client-python-k3s#106. 'tox' reinstalls
# from the tree first, which costs a few seconds and is the only way the
# result means anything for those tests.
MUTATIONS = [
    (
        'the node token is redacted out of a failed command line',
        PKG + '/progress.py',
        "    return _SECRET_ASSIGNMENT_RE.sub(r'\\1=' + REDACTED, text)",
        '    return text',
        PKG + '.tests.test_progress.RedactCommandLineTestCase',
        'stestr',
    ),
    (
        'K3S_TOKEN is in the set of names treated as secret',
        PKG + '/progress.py',
        "SECRET_ENVIRONMENT_NAMES = ('K3S_TOKEN',)",
        'SECRET_ENVIRONMENT_NAMES = ()',
        PKG + '.tests.test_ansible_module.SecretsTestCase',
        'tox',
    ),
    (
        'a heredoc body cannot contain a line equal to its delimiter',
        PKG + '/cluster.py',
        "    if delimiter in body.split('\\n'):",
        '    if False:',
        PKG + '.tests.test_cluster.HeredocDelimiterTestCase',
        'stestr',
    ),
    (
        'expand-addresses checks recorded addresses before it spends',
        PKG + '/cluster.py',
        "        for value in md.get(key) or []:",
        '        for value in []:',
        PKG + '.tests.test_cluster.ExpandAddressesChecksTheDocumentFirstTestCase',
        'stestr',
    ),
    (
        'a routed address from the API is checked before it is recorded',
        PKG + '/cluster.py',
        """                if not _is_address(addr):
                    raise exceptions.ClusterMetadataError.not_an_address(
                        self.name, 'routed_addresses', addr)
""",
        '',
        PKG + '.tests.test_cluster.AllocateMetallbAddressesTestCase',
        'stestr',
    ),
    (
        'an address has to be a string, not just something ip_address takes',
        PKG + '/cluster.py',
        '    if not isinstance(value, str):\n        return False\n',
        '',
        PKG + '.tests.test_cluster.ExpandAddressesChecksTheDocumentFirstTestCase',
        'stestr',
    ),
    (
        'the local kubeconfig is created unreadable by other users',
        PKG + '/cluster.py',
        'os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)',
        'os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)',
        PKG + '.tests.test_library_api.OptionalKubeconfigTestCase',
        'stestr',
    ),
    (
        '~/.kube is created unreadable by other users',
        PKG + '/cluster.py',
        'os.makedirs(kube_dir, mode=0o700, exist_ok=True)',
        'os.makedirs(kube_dir, mode=0o755, exist_ok=True)',
        PKG + '.tests.test_library_api.OptionalKubeconfigTestCase',
        'stestr',
    ),
    (
        'kubectl config view runs as an argument list, not through a shell',
        PKG + '/cluster.py',
        "['kubectl', 'config', 'view', '--flatten'],",
        "'kubectl config view --flatten', shell=True,",
        PKG + '.tests.test_cluster.NoShellInvocationTestCase',
        'stestr',
    ),
    (
        'a release lookup error body is bounded',
        PKG + '/primitives.py',
        "            product, url, r.status_code, r.text[:RESPONSE_SNIPPET_BYTES])",
        "            product, url, r.status_code, r.text)",
        PKG + '.tests.test_primitives.GetK3sReleaseTestCase',
        'stestr',
    ),
    (
        'a release lookup body which does not parse is quoted bounded',
        PKG + '/primitives.py',
        "                'k3s', url, r.text[:RESPONSE_SNIPPET_BYTES])",
        "                'k3s', url, r.text)",
        PKG + '.tests.test_primitives.GetK3sReleaseTestCase',
        'stestr',
    ),
    (
        'a release lookup gives up on an upstream which never answers',
        PKG + '/primitives.py',
        '            timeout=RELEASE_LOOKUP_TIMEOUT)',
        '            )',
        PKG + '.tests.test_primitives.FetchFailureTestCase',
        'stestr',
    ),
    (
        'the Longhorn chart index cannot construct Python objects',
        PKG + '/primitives.py',
        '            index = yaml.safe_load(r.text)',
        '            index = yaml.load(r.text, Loader=yaml.UnsafeLoader)',
        PKG + '.tests.test_primitives.GetLonghornReleaseTestCase',
        'stestr',
    ),
    (
        'every reasoned exception answers all of its fields',
        PKG + '/exceptions.py',
        "    FIELDS = ('name', 'key', 'value')",
        '    FIELDS = ()',
        PKG + '.tests.test_exceptions.TotalAttributesTestCase',
        'stestr',
    ),
    (
        'a trailing + does not hide a k3s key the plugin owns',
        PKG + '/cluster.py',
        "        if key != 'tls-san+' and key.rstrip('+') in owned:",
        "        if key != 'tls-san+' and key in owned:",
        PKG + '.tests.test_cluster.ValidateK3sConfigTestCase',
        'stestr',
    ),
    (
        "k3s's one-letter alias for an owned key is owned too",
        PKG + '/cluster.py',
        "    # token install_control_plane() fetched from the first server.\n"
        "    'token',\n    # k3s's alias for token.\n    't',\n",
        "    # token install_control_plane() fetched from the first server.\n"
        "    'token',\n",
        PKG + '.tests.test_cluster.ValidateK3sConfigTestCase',
        'stestr',
    ),
    (
        "a k3s configuration key containing '=' is refused",
        PKG + '/cluster.py',
        "        if '=' in key:",
        '        if False:',
        PKG + '.tests.test_cluster.ValidateK3sConfigTestCase',
        'stestr',
    ),
    (
        'k3s configuration which would end its own heredoc is refused',
        PKG + '/cluster.py',
        "    if K3S_CONFIG_DELIMITER in text.split('\\n'):",
        '    if False:',
        PKG + '.tests.test_cluster.ValidateK3sConfigTestCase',
        'stestr',
    ),
    (
        'a YAML alias in a k3s configuration file is refused',
        PKG + '/cluster.py',
        'Loader=_NoAliasSafeLoader)',
        'Loader=yaml.SafeLoader)',
        PKG + '.tests.test_cluster.ReadK3sConfigTestCase',
        'stestr',
    ),
    (
        'k3s configuration is checked before the cluster name is registered',
        PKG + '/cluster.py',
        "        validate_k3s_config(server_config, 'server')\n",
        '',
        PKG + '.tests.test_library_api.K3sConfigTestCase',
        'stestr',
    ),
    (
        'a k3s release below the floor is refused before the name is registered',
        PKG + '/cluster.py',
        '        check_k3s_release(target_release, release_channel)\n',
        '',
        PKG + '.tests.test_library_api.K3sConfigTestCase',
        'stestr',
    ),
    (
        'only a well-formed k3s release string is read',
        PKG + '/cluster.py',
        "r'v([0-9]{1,9})\\.([0-9]{1,9})\\.([0-9]{1,9})(?:[-+][0-9A-Za-z.+-]*)?\\Z',",
        "r'^v(\\d+)\\.(\\d+)\\.(\\d+)',",
        PKG + '.tests.test_cluster.CheckK3sReleaseTestCase',
        'stestr',
    ),
    (
        'the recorded kubeconfig names the floating address, whatever k3s bound',
        PKG + '/cluster.py',
        "        kc['clusters'][0]['cluster']['server'] = (\n"
        "            'https://%s:6443' % md['api_address_floating'])",
        "        kc['clusters'][0]['cluster']['server'] = kc['clusters'][0]['cluster']['server'].replace(\n"
        "            '127.0.0.1', md['api_address_floating'])",
        PKG + '.tests.test_library_api.KubeconfigServerTestCase',
        'stestr',
    ),
    (
        "delete -v redacts the caller's k3s configuration",
        PKG + '/cluster.py',
        "                    or (k in ('server_config', 'agent_config') and md[k])):",
        '                    or False):',
        PKG + '.tests.test_cluster.SecretRedactionTestCase',
        'stestr',
    ),
    (
        'the Progress reporter is constructed in exactly one place',
        PKG + '/cluster.py',
        '        p = self.start_progress(1)\n        '
        "p.phase('Adding metallb addresses')",
        '        p = progress.Progress(stream=None)\n        '
        "p.phase('Adding metallb addresses')",
        PKG + '.tests.test_cluster.ProgressIsStartedInOnePlaceTestCase',
        'stestr',
    ),
    (
        'delete leaves a network it did not create in place',
        PKG + '/cluster.py',
        '            if self._owns_node_network(md):',
        '            if True:',
        PKG + '.tests.test_cluster.DeleteNodeNetworkOwnershipTestCase',
        'stestr',
    ),
    (
        'create records a network it was given as borrowed',
        PKG + '/cluster.py',
        "            'node_network_created': not network,",
        "            'node_network_created': True,",
        PKG + '.tests.test_library_api.ClusterLifecycleTestCase'
        '.test_delete_leaves_a_network_it_was_given',
        'stestr',
    ),
    (
        'an older cluster is classified by its network name',
        PKG + '/cluster.py',
        "        return network.get('name') == 'k3s-%s-node' % self.name",
        '        return True',
        PKG + '.tests.test_cluster.DeleteNodeNetworkOwnershipTestCase',
        'stestr',
    ),
]


def run(test_id, runner, verbose):
    """Run one test, and say whether it failed.

    See the note on the last field of MUTATIONS for why some of these
    have to go through tox rather than straight to stestr.
    """
    if runner == 'tox':
        argv = ['tox', '-epy3', '--', test_id]
    else:
        # The tox venv's stestr if there is one, so the script works from
        # a plain shell and not only from inside an activated environment.
        stestr = os.path.join(ROOT, '.tox', 'py3', 'bin', 'stestr')
        if not os.path.exists(stestr):
            stestr = shutil.which('stestr')
        if not stestr:
            print('No stestr found. Run tox -epy3 once, or activate an')
            print('environment which has it.')
            sys.exit(2)
        # --no-subunit-trace keeps the output readable when -v is on; the
        # return code is what this reads either way.
        argv = [stestr, 'run', '--no-subunit-trace', test_id]
    proc = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True)
    if verbose:
        sys.stdout.write(proc.stdout[-4000:])
        sys.stdout.write(proc.stderr[-2000:])
    return proc.returncode != 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='show each test run output')
    parser.add_argument('--only', metavar='SUBSTRING',
                        help='run only mutations whose description matches')
    args = parser.parse_args()

    dirty = subprocess.run(['git', 'status', '--porcelain', '--', PKG],
                           cwd=ROOT, capture_output=True, text=True).stdout
    if dirty.strip():
        # Not refused: verifying before committing is the normal case, and
        # each file is restored from a copy taken immediately before its
        # own edit. Worth saying out loud because if this is killed
        # between the write and the restore, `git diff` will not separate
        # the mutation from your own work.
        print('Note: uncommitted changes under %s/. Each file is restored'
              % PKG)
        print('from a copy, but do not interrupt a run.\n')

    selected = [m for m in MUTATIONS
                if not args.only or args.only in m[0]]
    if not selected:
        print('No mutation matched %r' % args.only)
        return 2

    print('Checking that the suite notices %d broken properties.\n'
          % len(selected))
    survived = []
    for i, (what, relpath, old, new, test_id, runner) in enumerate(
            selected, 1):
        path = os.path.join(ROOT, relpath)
        with open(path, encoding='utf-8') as f:
            source = f.read()

        count = source.count(old)
        if count != 1:
            print('%2d. CANNOT APPLY  %s' % (i, what))
            print('       %s matches %d times in %s, expected 1.'
                  % (old.splitlines()[0][:60], count, relpath))
            print('       The code moved. Update this script, do not skip it.')
            survived.append(what)
            continue

        backup = tempfile.NamedTemporaryFile(
            prefix='mutation-', suffix=os.path.basename(relpath),
            delete=False)
        backup.close()
        shutil.copy2(path, backup.name)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(source.replace(old, new))
            noticed = run(test_id, runner, args.verbose)
        finally:
            shutil.copy2(backup.name, path)
            os.unlink(backup.name)

        print('%2d. %-9s %s' % (i, 'caught' if noticed else 'SURVIVED', what))
        if not noticed:
            print('       %s passed with the property broken.' % test_id)
            survived.append(what)

    print()
    if survived:
        print('%d of %d mutations survived. Each is a property the suite'
              % (len(survived), len(selected)))
        print('claims to enforce and does not.')
        return 1
    print('All %d mutations were caught.' % len(selected))
    return 0


if __name__ == '__main__':
    sys.exit(main())
