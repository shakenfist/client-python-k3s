"""The unit tests do not reach the network, and this makes that a rule.

A test which calls create() without stubbing the release lookups makes a
real HTTPS request to update.k3s.io or charts.longhorn.io and passes for
as long as that site answers. Five such tests passed for months and then
failed every merge group at once when update.k3s.io began serving a self
signed certificate (#133), which is an outage the test was never about.
Refusing the connection here turns that latent dependency into a failure
the first time the test runs, naming the address it tried to reach.

Only internet sockets are refused. Unix domain sockets stay usable, and so
does anything which never connects, such as a socket a test binds to read
back its own port.

install() is called from two places, because the tests run in two kinds
of process. The package's __init__ calls it, which covers everything the
stestr worker imports. module_harness.py calls it as well, because the
Ansible module tests run the harness as a separate interpreter which never
imports the test package. The harness imports this file by its bare name:
the installed package excludes tests/, and a script's sys.path[0] is its
own directory, which is this one.
"""

import socket


class NetworkAccessRefused(Exception):
    pass


_INTERNET_FAMILIES = (socket.AF_INET, socket.AF_INET6)
_real_connect = socket.socket.connect
_real_connect_ex = socket.socket.connect_ex


def _refuse(sock, address):
    if sock.family in _INTERNET_FAMILIES:
        raise NetworkAccessRefused(
            'a unit test tried to connect to %r; stub the call which '
            'reaches the network instead' % (address,))


def _guarded_connect(self, address):
    _refuse(self, address)
    return _real_connect(self, address)


def _guarded_connect_ex(self, address):
    _refuse(self, address)
    return _real_connect_ex(self, address)


def install():
    socket.socket.connect = _guarded_connect
    socket.socket.connect_ex = _guarded_connect_ex
