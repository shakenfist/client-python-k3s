# Importing the test package refuses every internet connection for the
# rest of the process; network_guard.py says why.
from shakenfist_client_k3s.tests import network_guard

network_guard.install()
