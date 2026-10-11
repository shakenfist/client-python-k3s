"""The unit tests do not reach the network, and this makes that a rule.

A test which calls create() without stubbing the release lookups makes a
real HTTPS request to update.k3s.io or charts.longhorn.io and passes for
as long as that site answers. Five such tests passed for months and then
failed every merge group at once when update.k3s.io began serving a self
signed certificate (#133), which is an outage the test was never about.
Refusing the connection here turns that latent dependency into a failure
the first time the test runs, naming the address it tried to reach.

Name resolution is refused as well as connection. The lookup happens
before the connect, so without this a test which forgot a stub would fail
one way where DNS works and another where it does not, and its result
would again depend on the machine it ran on. Numeric addresses still
resolve, because resolving one asks nothing outside the process, and so
does a reverse lookup which is told to stay numeric.

Internet sockets are refused whatever they connect to, loopback included,
and that is deliberate: a service listening on this machine is as much
outside the test as update.k3s.io is. A test which talks to a developer's
local Shaken Fist API on 127.0.0.1:13000 passes on that machine and
nowhere else. A test which needs a server of its own should listen on a
Unix domain socket. Those stay usable, and so does anything which never
connects, such as a socket a test binds to read back its own port.
Connectionless sends (sendto, and sendmsg given an address) are not
intercepted, because nothing here uses UDP.

install() is called from two places, because the tests run in two kinds
of process. The package's __init__ calls it, which covers everything the
stestr worker imports. module_harness.py calls it as well, because the
Ansible module tests run the harness as a separate interpreter which never
imports the test package. The harness imports this file by its bare name:
the installed package excludes tests/, and a script's sys.path[0] is its
own directory, which is this one.
"""

import ipaddress
import socket


class NetworkAccessRefused(Exception):
    pass


_INTERNET_FAMILIES = (socket.AF_INET, socket.AF_INET6)
_real_connect = socket.socket.connect
_real_connect_ex = socket.socket.connect_ex
_real_getaddrinfo = socket.getaddrinfo
_real_gethostbyname = socket.gethostbyname
_real_gethostbyname_ex = socket.gethostbyname_ex
_real_getnameinfo = socket.getnameinfo


def _refuse(sock, address):
    if sock.family in _INTERNET_FAMILIES:
        raise NetworkAccessRefused(
            'a unit test tried to connect to %r; stub the call which '
            'reaches the network instead' % (address,))


def _is_numeric(host):
    # None is how a caller asks for the local wildcard address.
    if host is None:
        return True
    if isinstance(host, bytes):
        host = host.decode('ascii', 'replace')
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


def _refuse_lookup(host):
    if not _is_numeric(host):
        raise NetworkAccessRefused(
            'a unit test tried to resolve %r; stub the call which '
            'reaches the network instead' % (host,))


def _refuse_reverse_lookup(address):
    raise NetworkAccessRefused(
        'a unit test tried a reverse lookup of %r; stub the call which '
        'reaches the network instead' % (address,))


def _guarded_connect(self, address):
    _refuse(self, address)
    return _real_connect(self, address)


def _guarded_connect_ex(self, address):
    _refuse(self, address)
    return _real_connect_ex(self, address)


def _guarded_getaddrinfo(host, *args, **kwargs):
    _refuse_lookup(host)
    return _real_getaddrinfo(host, *args, **kwargs)


def _guarded_gethostbyname(host):
    _refuse_lookup(host)
    return _real_gethostbyname(host)


def _guarded_gethostbyname_ex(host):
    _refuse_lookup(host)
    return _real_gethostbyname_ex(host)


def _guarded_gethostbyaddr(address):
    # A reverse lookup asks a resolver even when it is given a numeric
    # address, so there is nothing to let through.
    _refuse_reverse_lookup(address)


def _guarded_getnameinfo(sockaddr, flags):
    if not flags & socket.NI_NUMERICHOST:
        _refuse_reverse_lookup(sockaddr)
    return _real_getnameinfo(sockaddr, flags)


def install():
    socket.socket.connect = _guarded_connect
    socket.socket.connect_ex = _guarded_connect_ex
    socket.getaddrinfo = _guarded_getaddrinfo
    socket.gethostbyname = _guarded_gethostbyname
    socket.gethostbyname_ex = _guarded_gethostbyname_ex
    socket.gethostbyaddr = _guarded_gethostbyaddr
    socket.getnameinfo = _guarded_getnameinfo
