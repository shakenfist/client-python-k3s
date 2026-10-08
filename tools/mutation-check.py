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
        "    validate_k3s_config(server_config, 'server')\n",
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
        "a node's signals never affect healthy",
        PKG + '/cluster.py',
        "                        and all(node['healthy'] for node in nodes)",
        "                        and all(node['healthy'] and not node['signals']['error'] for node in nodes)",
        PKG + '.tests.test_cluster.HealthSignalsTestCase',
        'stestr',
    ),
    (
        "no raw command output reaches a node's signals report",
        PKG + '/cluster.py',
        "        signals = {'probed': probe['probed'], 'error': probe['error']}",
        "        signals = {'probed': probe['probed'], 'error': probe['error'], 'stderr': probe['stderr']}",
        PKG + '.tests.test_cluster.HealthSignalsTestCase',
        'stestr',
    ),
    (
        'the etcd snapshot directory is shell quoted in the signals command',
        PKG + '/cluster.py',
        '                % shlex.quote(snapshot_dir or K3S_ETCD_SNAPSHOT_DIR)))',
        '                % (snapshot_dir or K3S_ETCD_SNAPSHOT_DIR)))',
        PKG + '.tests.test_cluster.NodeSignalsCommandTestCase',
        'stestr',
    ),
    (
        'a relative etcd-snapshot-dir is not sized',
        PKG + '/cluster.py',
        "        if snapshot_dir and not snapshot_dir.startswith('/'):",
        '        if False:',
        PKG + '.tests.test_cluster.NodeSignalsCommandTestCase',
        'stestr',
    ),
    (
        'the first occurrence of a key in the signals output wins',
        PKG + '/cluster.py',
        '        raw.setdefault(key.strip(), value.strip())',
        '        raw[key.strip()] = value.strip()',
        PKG + '.tests.test_cluster.ParseNodeSignalsTestCase',
        'stestr',
    ),
    (
        'a count or size reading is at most twenty digits',
        PKG + '/cluster.py',
        "NODE_SIGNAL_INTEGER_RE = re.compile(r'\\A[0-9]{1,20}\\Z')",
        "NODE_SIGNAL_INTEGER_RE = re.compile(r'\\A[0-9]+\\Z')",
        PKG + '.tests.test_cluster.ParseNodeSignalsTestCase',
        'stestr',
    ),
    (
        'a boot_id which is not a UUID is reported as None',
        PKG + '/cluster.py',
        "    boot_id = _signal_string(raw, 'boot_id', NODE_SIGNAL_BOOT_ID_RE)",
        "    boot_id = raw.get('boot_id')",
        PKG + '.tests.test_cluster.ParseNodeSignalsTestCase',
        'stestr',
    ),
    (
        'a boot_id is reported in lowercase, so one boot is one string',
        PKG + '/cluster.py',
        "    signals['boot_id'] = boot_id.lower() if boot_id else None",
        "    signals['boot_id'] = boot_id or None",
        PKG + '.tests.test_cluster.ParseNodeSignalsTestCase',
        'stestr',
    ),
    (
        'a k3s_state which is not an ActiveState is reported as None',
        PKG + '/cluster.py',
        "        signals['k3s_state'] = _signal_string(\n"
        "            raw, 'ActiveState', NODE_SIGNAL_STATE_RE)",
        "        signals['k3s_state'] = raw.get('ActiveState') or None",
        PKG + '.tests.test_cluster.ParseNodeSignalsTestCase',
        'stestr',
    ),
    (
        'a Kubernetes probe record with one invalid field is dropped whole',
        PKG + '/cluster.py',
        '        if value is _NOT_A_READING:\n            return None\n',
        '        if value is _NOT_A_READING:\n            value = None\n',
        PKG + '.tests.test_cluster.ParseKubernetesReadingsTestCase',
        'stestr',
    ),
    (
        'the first Kubernetes node record for a name wins',
        PKG + '/cluster.py',
        "            nodes.setdefault(values.pop('name'), values)",
        "            nodes[values.pop('name')] = values",
        PKG + '.tests.test_cluster.ParseKubernetesReadingsTestCase',
        'stestr',
    ),
    (
        'a node condition status is True, False or Unknown',
        PKG + '/cluster.py',
        '    if value not in KUBERNETES_CONDITION_STATUSES:',
        '    if False:',
        PKG + '.tests.test_cluster.ParseKubernetesReadingsTestCase',
        'stestr',
    ),
    (
        'a node or pod name is at most 253 characters',
        PKG + '/cluster.py',
        "    r'\\A(?=.{1,253}\\Z)'",
        "    r'\\A(?=.{1,}\\Z)'",
        PKG + '.tests.test_cluster.ParseKubernetesReadingsTestCase',
        'stestr',
    ),
    (
        'a namespace or container name is at most 63 characters',
        PKG + '/cluster.py',
        "KUBERNETES_LABEL_RE = re.compile(r'\\A[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?\\Z')",
        "KUBERNETES_LABEL_RE = re.compile(r'\\A[a-z0-9]([-a-z0-9]*[a-z0-9])?\\Z')",
        PKG + '.tests.test_cluster.ParseKubernetesReadingsTestCase',
        'stestr',
    ),
    (
        'a Kubernetes timestamp is converted as UTC, not local time',
        PKG + '/cluster.py',
        '    return calendar.timegm(moment.timetuple())',
        '    return int(time.mktime(moment.timetuple()))',
        PKG + '.tests.test_cluster.ParseKubernetesReadingsTestCase',
        'stestr',
    ),
    (
        'the probe templates test a condition type before eq compares it',
        PKG + '/cluster.py',
        """            '{{if .type}}{{if eq .type "%(condition)s"}}'""",
        """            '{{if true}}{{if eq .type "%(condition)s"}}'""",
        PKG + '.tests.test_cluster.KubernetesProbeCommandTestCase',
        'stestr',
    ),
    (
        'a restart count of zero is printed by the pods template',
        PKG + '/cluster.py',
        """        '{{if exists . "restartCount"}}{{.restartCount}}{{end}}' +""",
        """        '{{if .restartCount}}{{.restartCount}}{{end}}' +""",
        PKG + '.tests.test_cluster.KubernetesProbeCommandTestCase',
        'stestr',
    ),
    (
        'a container killed twice reports the newer kill, once',
        PKG + '/cluster.py',
        "    '{{if not $reported}}' +",
        "    '{{if true}}' +",
        PKG + '.tests.test_cluster.KubernetesProbeCommandTestCase',
        'stestr',
    ),
    (
        'the Kubernetes probe fails if its node read fails',
        PKG + '/cluster.py',
        "    ' && kubectl get pods -A --kubeconfig /etc/rancher/k3s/k3s.yaml'",
        "    '; kubectl get pods -A --kubeconfig /etc/rancher/k3s/k3s.yaml'",
        PKG + '.tests.test_cluster.KubernetesProbeCommandRunsTestCase',
        'stestr',
    ),
    (
        'healthy requires every node to be Ready',
        PKG + '/cluster.py',
        "                        and kubernetes['answered']\n"
        "                        and all(node['kubernetes']['ready'] == 'True' for node in nodes))",
        "                        and kubernetes['answered'])",
        PKG + '.tests.test_cluster.HealthKubernetesTestCase',
        'stestr',
    ),
    (
        'a node whose Ready is Unknown, or unread, is not Ready',
        PKG + '/cluster.py',
        "                        and all(node['kubernetes']['ready'] == 'True' for node in nodes))",
        "                        and all(node['kubernetes']['ready'] != 'False' for node in nodes))",
        PKG + '.tests.test_cluster.HealthKubernetesTestCase',
        'stestr',
    ),
    (
        # The kubernetes['answered'] term of healthy cannot be mutated on
        # its own: a probe which did not answer leaves every ready None,
        # so the readiness term already refuses it and removing the
        # answered term changes no report. What the failed probe's
        # unhealthiness rests on is this gate, so it is the mutation.
        'nothing is read from a Kubernetes probe which did not answer',
        PKG + '/cluster.py',
        "        if not probe['answered']:\n"
        "            for node in nodes:\n"
        "                node['kubernetes'] = self._unread_kubernetes()\n",
        "        if probe['stdout'] is None:\n"
        "            for node in nodes:\n"
        "                node['kubernetes'] = self._unread_kubernetes()\n",
        PKG + '.tests.test_cluster.HealthKubernetesTestCase',
        'stestr',
    ),
    (
        'the node level healthy does not take Kubernetes readiness',
        PKG + '/cluster.py',
        "            node['kubernetes']['oom_killed'] = readings['oom_killed'].get(name, [])\n",
        "            node['kubernetes']['oom_killed'] = readings['oom_killed'].get(name, [])\n"
        "            node['healthy'] = node['healthy'] and node['kubernetes']['ready'] == 'True'\n",
        PKG + '.tests.test_cluster.HealthKubernetesTestCase',
        'stestr',
    ),
    (
        'oom_killed is None, not [], when nothing was read',
        PKG + '/cluster.py',
        '        return dict.fromkeys(KUBERNETES_NODE_KEYS)\n',
        '        return dict(dict.fromkeys(KUBERNETES_NODE_KEYS), oom_killed=[])\n',
        PKG + '.tests.test_cluster.HealthKubernetesTestCase',
        'stestr',
    ),
    (
        'two nodes sharing a lowercased name are not given its readings',
        PKG + '/cluster.py',
        '            if not name or claims[name] > 1:\n',
        '            if not name:\n',
        PKG + '.tests.test_cluster.HealthKubernetesTestCase',
        'stestr',
    ),
    (
        'health() sends exactly the Kubernetes probe command the fake answers',
        PKG + '/cluster.py',
        '            kubernetes_aop, kubernetes_probe = self._submit_probe(\n'
        '                control_plane[0], K3S_KUBERNETES_PROBE_COMMAND)\n',
        '            kubernetes_aop, kubernetes_probe = self._submit_probe(\n'
        "                control_plane[0], K3S_KUBERNETES_PROBE_COMMAND + ' ')\n",
        PKG + '.tests.test_cluster.HealthKubernetesTestCase',
        'stestr',
    ),
    (
        'the API probe is submitted before the Kubernetes probe',
        PKG + '/cluster.py',
        '            api_aop, api = self._submit_probe(\n'
        '                control_plane[0], K3S_API_PROBE_COMMAND)\n'
        '            self.reporter.debug(\n'
        "                'Asking %s what Kubernetes says of each node' % control_plane[0])\n"
        '            kubernetes_aop, kubernetes_probe = self._submit_probe(\n'
        '                control_plane[0], K3S_KUBERNETES_PROBE_COMMAND)\n',
        '            kubernetes_aop, kubernetes_probe = self._submit_probe(\n'
        '                control_plane[0], K3S_KUBERNETES_PROBE_COMMAND)\n'
        '            api_aop, api = self._submit_probe(\n'
        '                control_plane[0], K3S_API_PROBE_COMMAND)\n',
        PKG + '.tests.test_cluster.HealthSignalsTestCase.'
        'test_the_api_probe_is_submitted_first_and_the_kubernetes_probe_second',
        'stestr',
    ),
    (
        'a probe collected after the deadline is read, not judged as submitted',
        PKG + '/cluster.py',
        "        while aop['state'] in AGENT_OP_PENDING_STATES:\n"
        "            aop = self.client.get_agent_operation(aop['uuid'])\n",
        "        while aop['state'] in AGENT_OP_PENDING_STATES:\n"
        "            if deadline is not None and time.monotonic() >= deadline:\n"
        "                break\n"
        "            aop = self.client.get_agent_operation(aop['uuid'])\n",
        PKG + '.tests.test_cluster.HealthSignalsTestCase.'
        'test_signals_collected_after_the_deadline_are_read_not_judged_as_submitted',
        'stestr',
    ),
    (
        'an agent operation is read before the wait sleeps',
        PKG + '/cluster.py',
        "        while aop['state'] in AGENT_OP_PENDING_STATES:\n"
        "            aop = self.client.get_agent_operation(aop['uuid'])\n",
        "        while aop['state'] in AGENT_OP_PENDING_STATES:\n"
        '            time.sleep(1)\n'
        "            aop = self.client.get_agent_operation(aop['uuid'])\n",
        PKG + '.tests.test_cluster.AwaitExecuteTimeoutTestCase',
        'stestr',
    ),
    (
        'the health renderer shows a reading which is not an int as unknown',
        PKG + '/__init__.py',
        '    return isinstance(reading, int) and not isinstance(reading, bool)',
        '    return reading is not None',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'the health renderer does not take a bool for a count',
        PKG + '/__init__.py',
        '    return isinstance(reading, int) and not isinstance(reading, bool)',
        '    return isinstance(reading, int)',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'a boot time datetime cannot hold renders as unknown',
        PKG + '/__init__.py',
        '    except (OverflowError, OSError, ValueError):',
        '    except ZeroDivisionError:',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'a count of one renders singular',
        PKG + '/__init__.py',
        "        return '%s %s%s' % (_count(reading), noun, '' if singular else 's')",
        "        return '%s %ss' % (_count(reading), noun)",
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
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
    (
        'the node names a readiness wait interpolates are quoted',
        PKG + '/cluster.py',
        '    quoted = [shlex.quote(node_name) for node_name in node_names]',
        '    quoted = list(node_names)',
        PKG + '.tests.test_cluster.ShellQuotingTestCase',
        'stestr',
    ),
    (
        'the readiness wait is one command for every node',
        PKG + '/cluster.py',
        "            [md['control_plane_nodes'][0]], [nodes_ready_command(node_names)])",
        "            [md['control_plane_nodes'][0]],\n"
        "            [nodes_ready_command([n]) for n in node_names])",
        PKG + '.tests.test_cluster.AwaitNodesReadyTestCase',
        'stestr',
    ),
    (
        'the readiness wait gives up inside the agent operation deadline',
        PKG + '/cluster.py',
        'NODE_REGISTRATION_ATTEMPTS = 24\n',
        'NODE_REGISTRATION_ATTEMPTS = 60\n',
        PKG + '.tests.test_cluster.NodesReadyCommandTestCase',
        'stestr',
    ),
    (
        'a node is waited for by the lowercased name kubelet registered',
        PKG + '/cluster.py',
        '    return name.lower()\n',
        '    return name\n',
        PKG + '.tests.test_cluster.CreateAwaitsNodesReadyTestCase',
        'stestr',
    ),
    (
        'create waits for every node to be Ready',
        PKG + '/cluster.py',
        "        self.await_nodes_ready(\n"
        "            md['control_plane_nodes'] + md['worker_nodes'])\n",
        '',
        PKG + '.tests.test_cluster.CreateAwaitsNodesReadyTestCase',
        'stestr',
    ),
    (
        'expand-workers waits for the workers it added to be Ready',
        PKG + '/cluster.py',
        '        self.await_nodes_ready(new_workers)\n',
        '',
        PKG + '.tests.test_cluster.ExpandWorkersAwaitsNodesReadyTestCase',
        'stestr',
    ),
    (
        'expand-workers waits only for the workers it added',
        PKG + '/cluster.py',
        '        self.await_nodes_ready(new_workers)\n',
        "        self.await_nodes_ready(md['worker_nodes'])\n",
        PKG + '.tests.test_cluster.ExpandWorkersTestCase',
        'stestr',
    ),
    (
        'a pressure condition which was not read is not rendered as no pressure',
        PKG + '/__init__.py',
        "            elif status != 'False':",
        '            elif False:',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'a pressure condition which is True is named',
        PKG + '/__init__.py',
        "            if status == 'True':",
        '            if False:',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'a node unread for its own reason is not blamed on the Kubernetes probe',
        PKG + '/__init__.py',
        "            if not node.get('name'):",
        '            if True:',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'a Ready with no time is rendered without since',
        PKG + '/__init__.py',
        "        if kubernetes.get('ready_since') is not None:",
        '        if True:',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'the Kubernetes probe not answering is rendered',
        PKG + '/__init__.py',
        "        if not kubernetes.get('answered'):",
        '        if False:',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'unmatched Kubernetes nodes are rendered',
        PKG + '/__init__.py',
        '            if isinstance(unmatched, list) and unmatched:',
        '            if False:',
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'an OOM kill count of one renders singular',
        PKG + '/__init__.py',
        "            'restart' if _is_count(restarts) and restarts == 1 else 'restarts'))",
        "            'restarts'))",
        PKG + '.tests.test_commands.HealthRenderingReporterTestCase',
        'stestr',
    ),
    (
        'Cluster.__init__ refuses a name whose metadata key is a release cache',
        PKG + '/cluster.py',
        '        if md_key in primitives.RESERVED_METADATA_KEYS:',
        '        if False:',
        PKG + '.tests.test_cluster.ReservedClusterNameTestCase',
        'stestr',
    ),
    (
        'create() applies the cluster name rule before anything is built',
        PKG + '/cluster.py',
        '    validate_cluster_name(name)\n'
        '    validate_create_counts(control_plane_count, worker_count,\n',
        '    validate_create_counts(control_plane_count, worker_count,\n',
        PKG + '.tests.test_library_api.NameAndCountRefusalTestCase',
        'stestr',
    ),
    (
        'a cluster name has to match the permitted characters',
        PKG + '/cluster.py',
        '    if not isinstance(name, str) or not CLUSTER_NAME_PATTERN.match(name):',
        '    if not isinstance(name, str):',
        PKG + '.tests.test_cluster.ValidateClusterNameTestCase',
        'stestr',
    ),
    (
        'create() needs at least one control plane node',
        PKG + '/cluster.py',
        '    validate_counts(1, **{control_plane_name: control_plane_count})',
        '    validate_counts(0, **{control_plane_name: control_plane_count})',
        PKG + '.tests.test_cluster.ValidateCreateArgumentsTestCase',
        'stestr',
    ),
    (
        'k3s create validates its arguments before it binds the namespace',
        PKG + '/__init__.py',
        '    validate_create_arguments(name, control_plane_count, worker_count,\n'
        '                              metal_address_count, **checked)\n',
        '',
        PKG + '.tests.test_cli_errors.ArgumentRefusalTestCase',
        'stestr',
    ),
    (
        'the Ansible module refuses a bad name when it would create the cluster',
        'collection/plugins/modules/sf_k3s_cluster.py',
        '            sf_cluster.validate_create_arguments(cluster.name, **shape)\n',
        '            pass\n',
        PKG + '.tests.test_ansible_module.NameRuleTestCase',
        'tox',
    ),
    (
        "delete -v does not log the kubeconfig read, which carries other clusters' credentials",
        PKG + '/cluster.py',
        'main_config_path, fqcn, log_stdout=False)',
        'main_config_path, fqcn, log_stdout=True)',
        PKG + '.tests.test_library_api.KubeconfigCleanupTestCase'
        '.test_the_kubeconfig_read_is_never_logged',
        'stestr',
    ),
    (
        "delete's kubeconfig cleanup acts on the file create wrote, not on KUBECONFIG",
        PKG + '/cluster.py',
        "['kubectl', '--kubeconfig', main_config_path] + list(args),",
        "['kubectl'] + list(args),",
        PKG + '.tests.test_library_api.OptionalKubeconfigTestCase'
        '.test_the_cleanup_acts_on_the_file_create_writes',
        'stestr',
    ),
    (
        'delete removes only the kubeconfig entries which are present',
        PKG + '/cluster.py',
        '            if fqcn not in present[section]:',
        '            if False:',
        PKG + '.tests.test_library_api.KubeconfigCleanupTestCase',
        'stestr',
    ),
    (
        "the collection's floor on this package covers what its module calls",
        'collection/requirements.txt',
        'shakenfist_client_k3s>=0.3.0\n',
        'shakenfist_client_k3s>=0.1.0\n',
        PKG + '.tests.test_collection_floor.CollectionFloorTestCase'
        '.test_the_floor_covers_every_referenced_symbol',
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
