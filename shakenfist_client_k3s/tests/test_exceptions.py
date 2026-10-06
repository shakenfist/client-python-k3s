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


class NodeSizeErrorTestCase(testtools.TestCase):
    def test_not_positive_integer(self):
        e = exceptions.NodeSizeError.not_positive_integer(
            'control_plane', 'memory', 0)
        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual('control_plane', e.role)
        self.assertEqual('memory', e.field)
        self.assertEqual(0, e.value)
        self.assertEqual(
            'control plane memory must be a positive integer, not 0', str(e))

    def test_the_value_is_rendered_with_repr(self):
        # So that the string a YAML document or an Ansible variable handed
        # over is told apart from the integer it looks like.
        e = exceptions.NodeSizeError.not_positive_integer('worker', 'cpus', '2')
        self.assertEqual(
            "worker cpus must be a positive integer, not '2'", str(e))


class ClusterNameErrorTestCase(testtools.TestCase):
    def test_invalid_characters(self):
        e = exceptions.ClusterNameError.invalid_characters('my.cluster')
        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual('invalid_characters', e.reason)
        self.assertEqual('my.cluster', e.name)
        self.assertIsNone(e.max_length)
        self.assertIsNone(e.metadata_key)
        self.assertEqual(
            "Cluster name 'my.cluster' cannot be used. A cluster name must be made of\n"
            'letters, digits and hyphens, and must start and end with a letter\n'
            "or a digit, because it becomes part of each node's instance name,\n"
            'which Shaken Fist requires to be a DNS host name.', str(e))

    def test_an_empty_name_is_visible_in_the_message(self):
        # repr() rather than %s, or the sentence would read "Cluster name
        # cannot be used" and not say what was wrong with it.
        e = exceptions.ClusterNameError.invalid_characters('')
        self.assertIn("Cluster name '' cannot be used.", str(e))

    def test_a_name_which_is_not_a_string_renders(self):
        # A tuple is the case worth pinning: '%r' % (name) with a tuple
        # would format its elements, or raise, rather than render it.
        e = exceptions.ClusterNameError.invalid_characters(('a', 'b'))
        self.assertIn("Cluster name ('a', 'b') cannot be used.", str(e))

    def test_too_long(self):
        e = exceptions.ClusterNameError.too_long('a' * 49, 48)
        self.assertEqual('too_long', e.reason)
        self.assertEqual('a' * 49, e.name)
        self.assertEqual(48, e.max_length)
        self.assertIsNone(e.metadata_key)
        self.assertEqual(
            "Cluster name '%s' is 49 characters long, and a cluster name can be\n"
            "at most 48. Each node's instance name is k3s-<name>-node-<serial>,\n"
            'and Shaken Fist refuses an instance name longer than 63 characters.'
            % ('a' * 49), str(e))

    def test_reserved(self):
        e = exceptions.ClusterNameError.reserved(
            'k3s_version_cache', 'orchestrated_k3s_cluster_k3s_version_cache')
        self.assertEqual('reserved', e.reason)
        self.assertEqual('k3s_version_cache', e.name)
        self.assertIsNone(e.max_length)
        self.assertEqual('orchestrated_k3s_cluster_k3s_version_cache',
                         e.metadata_key)
        self.assertEqual(
            "Cluster name 'k3s_version_cache' cannot be used. A cluster of that name would be\n"
            'stored under the namespace metadata key orchestrated_k3s_cluster_k3s_version_cache,\n'
            'where shakenfist_client_k3s keeps its own data. Choose another name.', str(e))


class ShapeErrorTestCase(testtools.TestCase):
    def test_below_floor(self):
        e = exceptions.ShapeError.below_floor('control_plane_count', 0, 1)
        self.assertIsInstance(e, exceptions.K3sClusterException)
        self.assertEqual('below_floor', e.reason)
        self.assertEqual('control_plane_count', e.parameter)
        self.assertEqual(0, e.value)
        self.assertEqual(1, e.floor)
        self.assertEqual('control_plane_count must be at least 1, not 0.',
                         str(e))

    def test_not_an_integer(self):
        e = exceptions.ShapeError.not_an_integer('worker_count', True, 0)
        self.assertEqual('not_an_integer', e.reason)
        self.assertEqual('worker_count', e.parameter)
        self.assertIs(True, e.value)
        self.assertEqual(0, e.floor)
        self.assertEqual(
            'worker_count must be an integer of at least 0, not True, which '
            'is a bool.', str(e))

    def test_the_value_is_rendered_with_repr(self):
        # So that the string a YAML document or an Ansible variable handed
        # over is told apart from the integer it looks like.
        e = exceptions.ShapeError.not_an_integer('address_count', '2', 1)
        self.assertEqual(
            "address_count must be an integer of at least 1, not '2', which "
            'is a str.', str(e))

    def test_a_tuple_value_renders(self):
        # '%r' % value with a tuple would format its elements rather than
        # render it; the classmethods must pass a tuple of arguments.
        e = exceptions.ShapeError.not_an_integer('worker_count', (1, 2), 0)
        self.assertIn('not (1, 2), which is a tuple.', str(e))
        e = exceptions.ShapeError.below_floor('worker_count', -1, 0)
        self.assertEqual('worker_count must be at least 0, not -1.', str(e))


class ClusterMetadataErrorTestCase(testtools.TestCase):
    def test_not_an_address(self):
        e = exceptions.ClusterMetadataError.not_an_address(
            'banana', 'routed_addresses', '10.0.0.1\nEOF')

        self.assertEqual('not_an_address', e.reason)
        self.assertEqual('banana', e.name)
        self.assertEqual('routed_addresses', e.key)
        self.assertEqual('10.0.0.1\nEOF', e.value)

    def test_the_value_is_rendered_with_repr(self):
        # A value worth refusing usually contains a newline, and %s would
        # print it as a newline -- so the message would show the attack
        # laid out as the shell would have run it, with no indication of
        # where the value started and stopped. NodeSizeError makes the
        # same choice for the same reason.
        e = exceptions.ClusterMetadataError.not_an_address(
            'banana', 'routed_addresses', '10.0.0.1\nEOF\ntouch /pwned')

        self.assertIn("'10.0.0.1\\nEOF\\ntouch /pwned'", str(e))
        self.assertIn('routed_addresses', str(e))


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
            'Longhorn', 'https://charts.longhorn.io/index.yaml',
            404, 'not found')
        self.assertEqual(
            "Unable to determine latest Longhorn release version\n"
            "    GET https://charts.longhorn.io/index.yaml\n"
            "    returned HTTP status code 404 with text:\n"
            "    not found",
            str(e))

    def test_request_failed(self):
        e = exceptions.ReleaseLookupError.request_failed(
            'Longhorn', 'https://charts.longhorn.io/index.yaml',
            'Read timed out. (read timeout=30)')
        self.assertEqual('request_failed', e.reason)
        self.assertEqual('Read timed out. (read timeout=30)', e.error)
        self.assertEqual(
            "Unable to determine latest Longhorn release version\n"
            "    GET https://charts.longhorn.io/index.yaml\n"
            "    failed: Read timed out. (read timeout=30)",
            str(e))

    def test_unreadable_response(self):
        e = exceptions.ReleaseLookupError.unreadable_response(
            'k3s', 'https://update.k3s.io/v1-release/channels', '<html>')
        self.assertEqual('unreadable_response', e.reason)
        self.assertEqual('<html>', e.response_snippet)
        self.assertEqual(
            "Unable to determine latest k3s release version\n"
            "    GET https://update.k3s.io/v1-release/channels\n"
            "    returned a response which could not be parsed:\n"
            "    <html>",
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

    # The tail every cleanup failure ends with: the cluster has gone, so a
    # re-run of delete finds no cluster, and the operator is told which
    # entries may remain where and how to remove them by hand.
    CLEANUP_TAIL = (
        'The cluster has been deleted, but kubeconfig entries named banana.testns\n'
        'may remain in /home/u/.kube/config. Remove them with:\n'
        '    kubectl --kubeconfig /home/u/.kube/config config delete-context banana.testns\n'
        '    kubectl --kubeconfig /home/u/.kube/config config delete-user banana.testns\n'
        '    kubectl --kubeconfig /home/u/.kube/config config delete-cluster banana.testns')

    def test_delete_failed(self):
        e = exceptions.KubeconfigError.delete_failed(
            '/home/u/.kube/config', 'delete-user', 'banana.testns')
        self.assertEqual('delete_failed', e.reason)
        self.assertEqual('/home/u/.kube/config', e.main_config_path)
        self.assertEqual('delete-user', e.command)
        self.assertEqual('banana.testns', e.entry_name)
        self.assertEqual(
            "Could not remove banana.testns from /home/u/.kube/config with "
            "'kubectl config delete-user'\n" + self.CLEANUP_TAIL, str(e))

    def test_delete_failed_with_stderr(self):
        # Cluster.delete() captures the child's output, so this is the only
        # place kubectl's account of the failure can still be seen. Its
        # trailing newline is dropped so the remedy follows directly.
        e = exceptions.KubeconfigError.delete_failed(
            '/home/u/.kube/config', 'delete-user', 'banana.testns',
            'error: unable to parse config\n')
        self.assertEqual('error: unable to parse config\n', e.stderr)
        self.assertEqual(
            "Could not remove banana.testns from /home/u/.kube/config with "
            "'kubectl config delete-user'\n"
            'error: unable to parse config\n' + self.CLEANUP_TAIL, str(e))

    def test_delete_failed_with_empty_stderr(self):
        # A silent kubectl gains no blank line.
        e = exceptions.KubeconfigError.delete_failed(
            '/home/u/.kube/config', 'delete-user', 'banana.testns', '')
        self.assertEqual(
            "Could not remove banana.testns from /home/u/.kube/config with "
            "'kubectl config delete-user'\n" + self.CLEANUP_TAIL, str(e))

    def test_view_failed(self):
        e = exceptions.KubeconfigError.view_failed(
            '/home/u/.kube/config', 'banana.testns', 1, 'error: bad config\n')
        self.assertEqual('view_failed', e.reason)
        self.assertEqual('/home/u/.kube/config', e.main_config_path)
        self.assertEqual('banana.testns', e.entry_name)
        self.assertEqual(1, e.returncode)
        self.assertEqual(
            'Could not read /home/u/.kube/config, return code 1\n'
            'error: bad config\n' + self.CLEANUP_TAIL, str(e))

    def test_view_failed_without_stderr(self):
        e = exceptions.KubeconfigError.view_failed(
            '/home/u/.kube/config', 'banana.testns', 1, '')
        self.assertEqual(
            'Could not read /home/u/.kube/config, return code 1\n' + self.CLEANUP_TAIL,
            str(e))

    def test_view_unparseable(self):
        e = exceptions.KubeconfigError.view_unparseable(
            '/home/u/.kube/config', 'banana.testns', 'Expecting value')
        self.assertEqual('view_unparseable', e.reason)
        self.assertEqual('banana.testns', e.entry_name)
        self.assertEqual('Expecting value', e.detail)
        self.assertEqual(
            'Could not parse /home/u/.kube/config as kubectl reported it: '
            'Expecting value\n' + self.CLEANUP_TAIL, str(e))

    def test_missing_kubectl_on_delete(self):
        e = exceptions.KubeconfigError.missing_kubectl_on_delete(
            '/home/u/.kube/config', 'banana.testns')
        self.assertEqual('missing_kubectl_on_delete', e.reason)
        self.assertEqual('/home/u/.kube/config', e.main_config_path)
        self.assertEqual('banana.testns', e.entry_name)
        self.assertEqual(
            'A local kubectl binary is required to remove the cluster from\n'
            '/home/u/.kube/config, but none was found.\n' + self.CLEANUP_TAIL,
            str(e))

    def test_the_remedy_quotes_a_name_from_before_names_were_validated(self):
        # The commands are there to be pasted into a shell, and a cluster
        # created before create() checked names can contain anything.
        e = exceptions.KubeconfigError.view_failed(
            '/home/u/.kube/config', 'foo; touch /tmp/pwned.testns', 1)
        self.assertIn(
            "    kubectl --kubeconfig /home/u/.kube/config config delete-user "
            "'foo; touch /tmp/pwned.testns'", str(e))


class TotalAttributesTestCase(testtools.TestCase):
    """The two **fields exceptions answer every field, whoever built them.

    ReleaseLookupError and KubeconfigError take **fields and setattr them,
    so before FIELDS was declared an instance only carried the attributes
    the classmethod which built it happened to pass: e.status_code existed
    on an http_status() instance and raised AttributeError on an
    unknown_channel() one. docs/library-api.md advertises these attributes
    as the failure's details and phase 5's fail_json() will read them
    without knowing which constructor ran, so getattr has to be total.
    """

    def test_every_reasoned_exception_initialises_all_of_its_fields(self):
        """Driven by the subclass list, so a sixth one is covered the day it lands.

        The two tests below check this by hand for the two classes which
        had the defect. The shared base makes the property checkable for
        every class that has it, which matters because the way it was lost
        the first time was a new class written without FIELDS at all.
        """
        subclasses = exceptions._ReasonedK3sException.__subclasses__()
        self.assertNotEqual([], subclasses)

        for cls in subclasses:
            self.assertIsInstance(cls.FIELDS, tuple)
            self.assertNotEqual((), cls.FIELDS, cls.__name__)

            # Built directly rather than through a classmethod: the point
            # is the floor every classmethod inherits, not any one of them.
            e = cls('a_reason', 'a message')
            self.assertEqual('a_reason', e.reason)
            self.assertEqual('a message', str(e))
            for field in cls.FIELDS:
                self.assertIsNone(getattr(e, field),
                                  '%s.%s' % (cls.__name__, field))

    def test_release_lookup_fields_are_all_present(self):
        e = exceptions.ReleaseLookupError.unknown_channel('v1.26')
        self.assertEqual('v1.26', e.release_channel)

        # Fields belonging to the other constructors.
        self.assertIsNone(e.error)
        self.assertIsNone(e.status_code)
        self.assertIsNone(e.product)
        self.assertIsNone(e.url)
        self.assertIsNone(e.response_text)
        self.assertIsNone(e.response_snippet)

    def test_release_lookup_fields_from_the_other_direction(self):
        e = exceptions.ReleaseLookupError.http_status(
            'k3s', 'https://example.com', 500, 'boom')
        self.assertEqual(500, e.status_code)
        self.assertIsNone(e.release_channel)
        self.assertIsNone(e.response_snippet)

    def test_no_parsable_longhorn_release_carries_every_field(self):
        # The constructor which passes no fields at all is the one most
        # likely to strand a caller reading attributes off it.
        e = exceptions.ReleaseLookupError.no_parsable_longhorn_release()
        for field in exceptions.ReleaseLookupError.FIELDS:
            self.assertIsNone(getattr(e, field))

    def test_kubeconfig_fields_are_all_present(self):
        e = exceptions.KubeconfigError.delete_failed(
            '/home/u/.kube/config', 'delete-user', 'banana.testns')
        self.assertEqual('banana.testns', e.entry_name)

        # Fields belonging to missing_kubectl(), merge_failed(),
        # view_failed() and view_unparseable(), or left unset here.
        self.assertIsNone(e.name)
        self.assertIsNone(e.returncode)
        self.assertIsNone(e.stderr)
        self.assertIsNone(e.detail)

    def test_kubeconfig_fields_from_the_other_direction(self):
        e = exceptions.KubeconfigError.missing_kubectl(
            '/home/u/.kube/config', 'banana')
        self.assertEqual('banana', e.name)
        self.assertIsNone(e.command)
        self.assertIsNone(e.entry_name)
        self.assertIsNone(e.detail)
        self.assertIsNone(e.returncode)
