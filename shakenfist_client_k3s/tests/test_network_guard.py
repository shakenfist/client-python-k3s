"""The unit tests cannot reach the network, in either kind of process they run in.

network_guard.py is what refuses the connection. These tests are what
notice if it stops: a suite which passes with the guard gone looks exactly
like one which passes with it in place, on any machine with a route out,
and that is how the five tests behind #133 went unnoticed for months.

The addresses are TEST-NET-1 and the IPv6 documentation prefix, which
route nowhere, and the name is under .invalid, which never resolves. So
a guard which has stopped refusing gets an error or a timeout rather
than reaching anything. The assertions are on the exception type, which
only the guard raises.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile

import testtools

from shakenfist_client_k3s.tests import module_harness
from shakenfist_client_k3s.tests import network_guard


UNROUTABLE_V4 = ('192.0.2.1', 9)
UNROUTABLE_V6 = ('2001:db8::1', 9)
UNRESOLVABLE = 'k3s-update.invalid'

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
HARNESS = os.path.abspath(module_harness.__file__)
MODULE = os.path.join(_REPO_ROOT, 'collection', 'plugins', 'modules',
                      'sf_k3s_cluster.py')


class InternetSocketsAreRefusedTestCase(testtools.TestCase):

    def _socket(self, family):
        sock = socket.socket(family, socket.SOCK_STREAM)
        self.addCleanup(sock.close)
        # Bounded, so that a guard which has stopped working costs a short
        # wait on an address which never answers rather than a hang.
        sock.settimeout(1)
        return sock

    def test_connect_is_refused(self):
        sock = self._socket(socket.AF_INET)
        e = self.assertRaises(network_guard.NetworkAccessRefused,
                              sock.connect, UNROUTABLE_V4)
        self.assertIn('192.0.2.1', str(e))

    def test_connect_ex_is_refused(self):
        sock = self._socket(socket.AF_INET)
        self.assertRaises(network_guard.NetworkAccessRefused,
                          sock.connect_ex, UNROUTABLE_V4)

    def test_ipv6_connect_is_refused(self):
        if not socket.has_ipv6:
            self.skipTest('this interpreter has no IPv6 support')
        try:
            sock = self._socket(socket.AF_INET6)
        except OSError as e:
            self.skipTest('cannot create an IPv6 socket here: %s' % e)
        self.assertRaises(network_guard.NetworkAccessRefused,
                          sock.connect, UNROUTABLE_V6)


class NamesAreNotResolvedTestCase(testtools.TestCase):
    """The lookup is refused before it is made, whether or not DNS works."""

    def test_getaddrinfo_is_refused(self):
        e = self.assertRaises(network_guard.NetworkAccessRefused,
                              socket.getaddrinfo, UNRESOLVABLE, 443)
        self.assertIn(UNRESOLVABLE, str(e))

    def test_gethostbyname_is_refused(self):
        self.assertRaises(network_guard.NetworkAccessRefused,
                          socket.gethostbyname, UNRESOLVABLE)

    def test_gethostbyname_ex_is_refused(self):
        self.assertRaises(network_guard.NetworkAccessRefused,
                          socket.gethostbyname_ex, UNRESOLVABLE)

    def test_gethostbyaddr_is_refused(self):
        self.assertRaises(network_guard.NetworkAccessRefused,
                          socket.gethostbyaddr, UNROUTABLE_V4[0])

    def test_getnameinfo_is_refused(self):
        self.assertRaises(network_guard.NetworkAccessRefused,
                          socket.getnameinfo, UNROUTABLE_V4, 0)

    def test_localhost_is_refused(self):
        # Loopback connections are refused too, so letting the name resolve
        # would only move the failure to the connect.
        self.assertRaises(network_guard.NetworkAccessRefused,
                          socket.getaddrinfo, 'localhost', 443)


class NumericAddressesStillResolveTestCase(testtools.TestCase):
    """Resolving a numeric address asks nothing outside the process."""

    def test_an_ipv4_address(self):
        results = socket.getaddrinfo(UNROUTABLE_V4[0], 443,
                                     type=socket.SOCK_STREAM)
        self.assertEqual(UNROUTABLE_V4[0], results[0][4][0])

    def test_an_ipv6_address(self):
        if not socket.has_ipv6:
            self.skipTest('this interpreter has no IPv6 support')
        results = socket.getaddrinfo(UNROUTABLE_V6[0], 443,
                                     type=socket.SOCK_STREAM)
        self.assertEqual(UNROUTABLE_V6[0], results[0][4][0])

    def test_the_wildcard_address(self):
        self.assertNotEqual(
            [], socket.getaddrinfo(None, 443, flags=socket.AI_PASSIVE))

    def test_gethostbyname(self):
        self.assertEqual(UNROUTABLE_V4[0],
                         socket.gethostbyname(UNROUTABLE_V4[0]))

    def test_a_numeric_getnameinfo(self):
        self.assertEqual(
            (UNROUTABLE_V4[0], '9'),
            socket.getnameinfo(UNROUTABLE_V4,
                               socket.NI_NUMERICHOST | socket.NI_NUMERICSERV))


class UnixSocketsStillWorkTestCase(testtools.TestCase):
    """The guard is narrow: a test using a Unix domain socket is unaffected."""

    def test_a_unix_socket_connects(self):
        if not hasattr(socket, 'AF_UNIX'):
            self.skipTest('this platform has no Unix domain sockets')
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = os.path.join(tempdir.name, 'sock')

        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(server.close)
        server.bind(path)
        server.listen(1)

        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(client.close)
        client.connect(path)

        accepted, _ = server.accept()
        self.addCleanup(accepted.close)
        client.sendall(b'ping')
        self.assertEqual(b'ping', accepted.recv(4))


class TheModuleHarnessIsGuardedTestCase(testtools.TestCase):
    """The Ansible module tests run in an interpreter of their own.

    That interpreter never imports this package, so it is guarded only
    because module_harness.main() installs the guard itself. A present
    state for a cluster which does not exist, with the real create() left
    in place, is the shortest path from the module to a release lookup --
    and so to the network. The refusal has to name update.k3s.io, which
    only the lookup knows: that is what says it came before any DNS query,
    so the test gives the same answer on a machine which has none.

    Like every test which runs the harness, this one exercises the
    *installed* package and means something only under tox (#106).
    """

    def test_a_real_create_is_refused_its_release_lookup(self):
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        spec_path = os.path.join(tempdir.name, 'spec.json')
        with open(spec_path, 'w', encoding='utf-8') as f:
            json.dump({'params': {
                'name': module_harness.CLUSTER_NAME,
                'namespace': module_harness.CLUSTER_NAMESPACE,
                'state': 'present',
                'api_url': 'http://sf-1:13000',
                'auth_namespace': 'system',
                'key': 'not-a-real-key',
            }}, f)

        # Without the proxy variables, which tox passes through: with one
        # set, requests resolves the proxy's name rather than the release
        # host's, and the refusal names that instead.
        env = {k: v for k, v in os.environ.items()
               if not k.lower().endswith('_proxy')}
        process = subprocess.run(
            [sys.executable, HARNESS, MODULE, spec_path],
            stdin=subprocess.DEVNULL, capture_output=True,
            encoding='utf-8', timeout=60, env=env)

        detail = 'stdout: %s\nstderr: %s' % (process.stdout, process.stderr)
        self.assertIn('NetworkAccessRefused', process.stderr, detail)
        self.assertIn("resolve 'update.k3s.io'", process.stderr, detail)
