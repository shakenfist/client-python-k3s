import testtools

from shakenfist_client_k3s import exceptions


class ClusterExistsErrorTestCase(testtools.TestCase):
    def test_str(self):
        e = exceptions.ClusterExistsError('banana')
        self.assertEqual('banana', e.name)
        self.assertEqual('Sorry, that cluster name is already taken', str(e))


class NetworkNotFoundErrorTestCase(testtools.TestCase):
    def test_str(self):
        e = exceptions.NetworkNotFoundError('mynet')
        self.assertEqual('mynet', e.network)
        self.assertEqual('Specified network does not exist', str(e))


class ClusterNotFoundErrorTestCase(testtools.TestCase):
    def test_unknown_cluster(self):
        e = exceptions.ClusterNotFoundError.unknown_cluster('banana')
        self.assertEqual('banana', e.name)
        self.assertEqual('unknown_cluster', e.reason)
        self.assertEqual('Unknown cluster', str(e))

    def test_does_not_exist(self):
        e = exceptions.ClusterNotFoundError.does_not_exist('banana')
        self.assertEqual('does_not_exist', e.reason)
        self.assertEqual(
            'Sorry, that cluster name does not appear to exist', str(e))

    def test_not_found(self):
        e = exceptions.ClusterNotFoundError.not_found('banana')
        self.assertEqual('not_found', e.reason)
        self.assertEqual('Cluster not found!', str(e))


class ClusterIncompleteErrorTestCase(testtools.TestCase):
    def test_str(self):
        e = exceptions.ClusterIncompleteError('banana')
        self.assertEqual('banana', e.name)
        self.assertEqual(
            'No kubeconfig for this cluster. Is it fully installed?', str(e))


class ReleaseLookupErrorTestCase(testtools.TestCase):
    def test_http_status_k3s(self):
        e = exceptions.ReleaseLookupError.http_status(
            'k3s', 'https://update.k3s.io/v1-release/channels', 503, 'server error')
        self.assertEqual('k3s', e.product)
        self.assertEqual(503, e.status_code)
        self.assertEqual(
            "Unable to determine latest k3s release version\n"
            "    GET https://update.k3s.io/v1-release/channels\n"
            "    returned HTTP status code 503 with text:\n"
            "    server error",
            str(e))

    def test_http_status_longhorn(self):
        e = exceptions.ReleaseLookupError.http_status(
            'Longhorn', 'https://api.github.com/repos/longhorn/longhorn/releases?page=0',
            404, 'not found')
        self.assertEqual(
            "Unable to determine latest Longhorn release version\n"
            "    GET https://api.github.com/repos/longhorn/longhorn/releases?page=0\n"
            "    returned HTTP status code 404 with text:\n"
            "    not found",
            str(e))

    def test_no_usable_k3s_channels(self):
        e = exceptions.ReleaseLookupError.no_usable_k3s_channels(
            'https://update.k3s.io/v1-release/channels', '{"data": []}')
        self.assertEqual(
            "No usable k3s release channels found\n"
            "    GET https://update.k3s.io/v1-release/channels\n"
            '    returned: {"data": []}',
            str(e))

    def test_unknown_channel(self):
        e = exceptions.ReleaseLookupError.unknown_channel('bogus')
        self.assertEqual('bogus', e.release_channel)
        self.assertEqual('Release channel bogus not found', str(e))

    def test_no_parsable_longhorn_release(self):
        e = exceptions.ReleaseLookupError.no_parsable_longhorn_release()
        self.assertEqual(
            'Unable to determine the latest Longhorn release', str(e))


class AgentOperationErrorTestCase(testtools.TestCase):
    def test_with_command_and_results(self):
        e = exceptions.AgentOperationError(
            'node-1', 'inst-uuid', 'op-uuid', 'apt-get install foo',
            {'0': {'return-code': 1}})
        self.assertEqual('node-1', e.instance_name)
        self.assertEqual('inst-uuid', e.instance_uuid)
        self.assertEqual('op-uuid', e.operation_uuid)
        self.assertEqual(
            "Agent operation failed!\n"
            "  instance: node-1 (uuid inst-uuid)\n"
            "  operation: op-uuid\n"
            "  command: apt-get install foo\n"
            "  results: {\n"
            '    "0": {\n'
            '        "return-code": 1\n'
            "    }\n"
            "}\n"
            "  the server side event log may have more detail: "
            "'sf-client instance events node-1'",
            str(e))

    def test_without_command_or_results(self):
        e = exceptions.AgentOperationError(
            'node-1', 'inst-uuid', 'op-uuid', None, {})
        self.assertEqual(
            "Agent operation failed!\n"
            "  instance: node-1 (uuid inst-uuid)\n"
            "  operation: op-uuid\n"
            "  no results were recorded, so the command probably failed to start\n"
            "  the server side event log may have more detail: "
            "'sf-client instance events node-1'",
            str(e))


class CommandFailedErrorTestCase(testtools.TestCase):
    def test_str(self):
        e = exceptions.CommandFailedError(
            'node-1', 'inst-uuid', 'false', 1, 'line one\nline two', 'oops\nmore oops')
        self.assertEqual('node-1', e.instance_name)
        self.assertEqual(1, e.return_code)
        self.assertEqual(
            "Command failed!\n"
            "  instance: node-1 (UUID inst-uuid)\n"
            "  command: false\n"
            "exit code: 1\n"
            "   stdout: line one\n"
            "   stdout: line two\n"
            "   stderr: oops\n"
            "   stderr: more oops",
            str(e))


class KubeconfigErrorTestCase(testtools.TestCase):
    def test_missing_kubectl(self):
        e = exceptions.KubeconfigError.missing_kubectl('/home/u/.kube/config', 'banana')
        self.assertEqual('/home/u/.kube/config', e.main_config_path)
        self.assertEqual('banana', e.name)
        self.assertEqual(
            "A local kubectl binary is required to merge the new cluster into\n"
            "/home/u/.kube/config, but none was found. The new cluster credentials are\n"
            "available from 'sf-client k3s getconfig banana'.",
            str(e))

    def test_merge_failed_with_stderr(self):
        e = exceptions.KubeconfigError.merge_failed('/home/u/.kube/config', 1, 'boom')
        self.assertEqual(1, e.returncode)
        self.assertEqual(
            "Failed to update /home/u/.kube/config, return code 1\n"
            "boom",
            str(e))

    def test_merge_failed_without_stderr(self):
        e = exceptions.KubeconfigError.merge_failed('/home/u/.kube/config', 1, '')
        self.assertEqual(
            'Failed to update /home/u/.kube/config, return code 1', str(e))

    def test_unset_failed(self):
        e = exceptions.KubeconfigError.unset_failed('users.banana.testns')
        self.assertEqual('users.banana.testns', e.config_elem)
        self.assertEqual(
            'Could not unset kubectl config element users.banana.testns', str(e))
