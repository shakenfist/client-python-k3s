"""Tests for the sf_k3s_cluster Ansible module.

The module lives under collection/ rather than in this package, because
that is the tree ansible-galaxy builds. Its tests live here because this
is where this repository's test suite is, and because what they exercise
is almost entirely this package: the module is a translation layer, and
make_client(), Cluster, Progress and CollectingReporter are all real on
every path below.

Every test drives the module through tests/module_harness.py in a
subprocess, which is both how Ansible runs a module and the only way to
look at the thing most likely to be wrong. A module's result *is* its
stdout: Ansible parses file descriptor 1 and nothing else, so a single
stray print() in any library this imports turns a successful run into a
module failure in a playbook while every in process assertion about the
returned dictionary still passes. The phase 5 plan's risk table names
that as the likeliest way to ship something which passes its tests, and
phase 3 fixed a real instance of it in this package (kubectl config unset
writing past the reporter). So the assertion is on bytes which came off a
real fd 1, and test_the_module_emits_one_json_document_on_stdout runs
every scenario in the file past it.

Running the module out of process has a second consequence worth
knowing: it is why these tests cost tenths of a second each rather than
microseconds. A Python interpreter start plus an ansible.module_utils
import is most of that. The alternative was faster and would have tested
something else.
"""
import json
import os
import subprocess
import sys
import tempfile

import testtools

from shakenfist_client_k3s import client as sf_client
from shakenfist_client_k3s import exceptions as sf_exceptions
from shakenfist_client_k3s import progress
from shakenfist_client_k3s.tests import module_harness


_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODULE = os.path.join(_REPO_ROOT, 'collection', 'plugins', 'modules',
                      'sf_k3s_cluster.py')
HARNESS = os.path.abspath(module_harness.__file__)

# The connection parameters of a run which supplies all three, and a key
# whose value must never appear in a result. no_log on the key is the only
# thing between it and the invocation dictionary Ansible returns.
SECRET_AUTH_KEY = 'SECRET-SHAKENFIST-API-KEY'

# How long one run of the module is allowed to take. Three tenths of a
# second is typical, so this is not a performance budget -- it is a bound
# on a hang. Several of the scenarios below leave the real create() in
# place, and create() waits for instances to boot and agents to answer. A
# module which reaches one of those wait loops against a fake that never
# changes its answer blocks forever, which is a test failure that reports
# itself as an infinite test run unless something stops it. Found the hard
# way while mutation testing: a mutation which disabled the partial
# connection check sent three tests into exactly that loop.
RUN_TIMEOUT = 60
FULL_CONNECTION = {
    'api_url': 'http://sf-1:13000',
    'auth_namespace': 'system',
    'key': SECRET_AUTH_KEY,
}


def base_params(**overrides):
    """The module arguments every scenario starts from."""
    params = {
        'name': module_harness.CLUSTER_NAME,
        'namespace': module_harness.CLUSTER_NAMESPACE,
    }
    params.update(overrides)
    return params


def _warning_text(warning):
    """The text of a warning, whatever shape the running ansible-core uses.

    Three shapes appear across the range meta/runtime.yml declares: a plain
    string on older versions, and on 2.21 a structured WarningSummary dict
    whose message is nested under "event". Asserting against whichever one
    happens to be installed would make the test pass or fail by
    environment, which is the third instance of this drift this suite has
    had to absorb -- after invocation injection and traceback capture, both
    recorded in shakenfist/client-python-k3s#91.
    """
    if isinstance(warning, str):
        return warning
    if 'msg' in warning:
        return warning['msg']
    return warning.get('event', {}).get('msg', '')


class ModuleResult:
    """What one run of the module produced, as the controller would see it."""

    def __init__(self, stdout, stderr, returncode, diagnostics):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.result = json.loads(stdout)
        self.diagnostics = diagnostics

    @property
    def changed(self):
        return self.result.get('changed')

    @property
    def failed(self):
        return self.result.get('failed', False)

    @property
    def msg(self):
        return self.result.get('msg', '')

    @property
    def log(self):
        return self.result.get('log')

    @property
    def cluster_calls(self):
        return self.diagnostics['cluster_calls']

    @property
    def client_calls(self):
        return self.diagnostics['client_calls']

    @property
    def create_kwargs(self):
        return self.diagnostics['create_kwargs']


class ModuleTestCase(testtools.TestCase):
    """Base class which knows how to run the module once."""

    def setUp(self):
        super().setUp()
        if not os.path.exists(MODULE):
            self.skipTest(
                'collection/ is not part of an installed package, so the '
                'module cannot be located')
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.tempdir = tempdir.name

    def run_module(self, params, expect_failure=False, **spec):
        """Run the module in a subprocess and return what it produced.

        Nothing is written to the test process's own stdout: fd 1 of the
        child is a pipe of its own, which is what makes the JSON assertion
        meaningful. stdin is /dev/null because a module which reads its
        arguments from stdin -- which this one does not, because the
        harness sets _ANSIBLE_ARGS -- would otherwise block on the test
        runner's.
        """
        spec['params'] = params
        spec_path = os.path.join(self.tempdir, 'spec.json')
        with open(spec_path, 'w', encoding='utf-8') as f:
            json.dump(spec, f)

        try:
            process = subprocess.run(
                [sys.executable, HARNESS, MODULE, spec_path],
                stdin=subprocess.DEVNULL, capture_output=True,
                # The module writes no bytes which are not UTF-8, and a
                # test which cannot decode the result should say so rather
                # than silently replacing characters.
                encoding='utf-8', timeout=RUN_TIMEOUT)
        except subprocess.TimeoutExpired:
            self.fail(
                'the module did not finish in %d seconds. Nothing it does '
                'on these paths waits for anything: a run which blocks has '
                'reached orchestration which polls a real cloud, against a '
                'fake which will never answer.' % RUN_TIMEOUT)

        diagnostics = None
        for line in process.stderr.splitlines():
            if line.startswith(module_harness.DIAGNOSTIC_MARKER):
                diagnostics = json.loads(
                    line[len(module_harness.DIAGNOSTIC_MARKER):])
        self.assertIsNotNone(
            diagnostics,
            'the harness did not report: it failed before running the '
            'module.\nstdout: %s\nstderr: %s'
            % (process.stdout, process.stderr))

        if expect_failure:
            self.assertEqual(
                1, process.returncode,
                'expected the module to fail, and it did not: %s'
                % process.stdout)
        else:
            self.assertEqual(
                0, process.returncode,
                'the module failed: %s\n%s' % (process.stdout,
                                               process.stderr))

        # Checked here, before any test gets to look at the result, so that
        # every test in this file is also a test of what reached fd 1.
        # StdoutTestCase is still where the property is named and where the
        # whole set of code paths is walked past it; this is what turns a
        # stray write into a legible failure in the other twenty-odd tests
        # rather than a decoder error with no context.
        try:
            json.loads(process.stdout)
        except ValueError as e:
            self.fail(
                'the module did not put exactly one JSON document on '
                'stdout (%s).\nstdout: %r\nstderr: %s'
                % (e, process.stdout[:500], process.stderr))

        return ModuleResult(process.stdout, process.stderr,
                            process.returncode, diagnostics)

    def assertNothingMutated(self, run):
        self.assertEqual(
            [], run.cluster_calls,
            'a Cluster method which changes something was called')
        self.assertEqual(
            [], run.client_calls,
            'an API call which changes something was made')


class ConnectionTestCase(ModuleTestCase):
    """How the three connection parameters are translated into a client.

    make_client() enforces the all-or-nothing rule and the module does not
    restate it, which is deliberate -- there is one definition of it -- and
    is also why this is worth a test: the module is responsible for the
    parameters reaching make_client() at all, and for what the error looks
    like once it comes back.
    """

    def test_a_partial_connection_set_is_refused(self):
        run = self.run_module(
            base_params(api_url='http://sf-1:13000', auth_namespace='system'),
            expect_failure=True)

        self.assertTrue(run.failed)
        # Asserted against what make_client() actually says, by asking it:
        # the module's contract here is that it passes make_client()'s
        # message through, so the test that proves it is one which has that
        # message to compare with. The review of #90 found the previous
        # version of this test matching on the literal phrase "Got only",
        # and two of its assertions worse than brittle: assertIn('namespace')
        # and assertIn('auth_namespace') were both satisfied by the
        # parenthetical the module appends to every one of these failures,
        # so they held whatever the message said about the parameters --
        # which is the one thing they were there to check. Only the api_url
        # assertion carried information.
        try:
            sf_client.make_client(api_url='http://sf-1:13000',
                                  namespace='system', key=None)
            self.fail('make_client() accepted a partial connection set')
        except ValueError as e:
            expected = str(e)
        self.assertIn(expected, run.msg)
        # And that the passed-through text is the informative part rather
        # than something the module's own suffix could supply: the suffix
        # names auth_namespace and namespace, so a message which named no
        # parameters at all would still contain both words.
        # What this module owes is that make_client()'s message arrives
        # intact, which the assertion above proves. Which parameters that
        # message names is make_client()'s contract and is tested in
        # client.py's own tests -- the previous version of this line split
        # on the literal phrase 'Got only', which is the same brittleness
        # the review of #90 asked to have removed, relocated rather than
        # deleted. Whether the rule itself is right is not this file's
        # question.
        # And that the module owns the one translation it owes: its
        # parameter for make_client()'s namespace has a different name,
        # so a message naming "namespace" is ambiguous without this.
        self.assertIn('auth_namespace', run.msg)

    def test_a_partial_connection_set_reaches_nothing(self):
        """Refused before anything is read, not after."""
        run = self.run_module(
            base_params(api_url='http://sf-1:13000', key=SECRET_AUTH_KEY),
            expect_failure=True)

        self.assertTrue(run.failed)
        self.assertNothingMutated(run)
        self.assertEqual([], run.log)

    def test_an_unconfigured_client_fails_rather_than_tracing(self):
        """Discovery finding nothing is a message, not a module failure.

        make_client() deliberately lets UnconfiguredException out so that a
        library caller keeps the exception. Translating it is the module's
        job, and the thing which goes wrong if it does not is an Ansible
        MODULE FAILURE with a traceback in it, which is why the traceback
        is asserted against as well as the message.
        """
        run = self.run_module(base_params(), unconfigured=True,
                              expect_failure=True)

        self.assertTrue(run.failed)
        self.assertIn('Could not configure the Shaken Fist client',
                      run.msg)
        # Where it looked, so that the fix is in the message.
        self.assertIn('shakenfist.json', run.msg)
        self.assertNotIn('Traceback', run.stderr)

    def test_a_full_connection_set_is_used(self):
        run = self.run_module(
            base_params(**FULL_CONNECTION), cluster_exists=True)

        self.assertFalse(run.changed)


class SecretsTestCase(ModuleTestCase):
    """What must not come back.

    The cluster's namespace metadata holds its kubeconfig, its k3s node
    token and any ssh key it was built with, and delete() writes the whole
    document to the reporter -- at debug level, which a non-verbose
    reporter drops. That makes "the reporter is not verbose" a security
    property of this module rather than a preference about output volume,
    and a property nothing else in the tree would notice the loss of.
    """

    def test_no_cluster_secret_reaches_the_result_of_a_delete(self):
        """delete() dumps the metadata document, and none of it comes back."""
        run = self.run_module(base_params(state='absent'),
                              cluster_exists=True, instance_state='deleted')

        self.assertTrue(run.changed)
        self.assertIn('delete', run.cluster_calls)
        for secret in (module_harness.SECRET_NODE_TOKEN,
                       module_harness.SECRET_KUBECONFIG,
                       module_harness.SECRET_SSH_KEY):
            self.assertNotIn(secret, run.stdout)

    def test_no_cluster_secret_reaches_a_failed_create(self):
        """A worker install which exits non-zero reports no node token.

        This is the path the audit found leaking. The k3s installer has to
        be told which cluster to join, so install_k3s_component() puts
        K3S_TOKEN= on the command line; the API echoes that command line
        back; reap_execute() hands it to CommandFailedError on a non-zero
        return code; and this module puts the exception into
        fail_json(msg=...). The node token is not a module parameter, so it
        is not in no_log_values and Ansible's own remove_values() does not
        scrub it -- which made docs/collection.md's "Secrets never come
        back in the module's output" false for this one path.

        Triggering it needs no privilege: a transient apt failure during a
        worker install is enough, and the CI job log which would then hold
        the token is readable by anyone who can see the repository.
        """
        run = self.run_module(base_params(), expect_failure=True,
                              fake_create=True, worker_install_fails=True)

        self.assertTrue(run.failed)
        # The premise: the failure really did come from the install, so a
        # module which failed earlier for some other reason cannot pass
        # this test by never reaching the leak.
        self.assertIn('Command failed!', run.msg)
        self.assertIn('exit code: 100', run.msg)

        self.assertNotIn(module_harness.SECRET_NODE_TOKEN, run.stdout)
        # And the message is still worth reading.
        self.assertIn('K3S_TOKEN=%s' % progress.REDACTED, run.msg)
        self.assertIn('E: Unable to fetch some archives', run.msg)

    def test_no_cluster_secret_reaches_the_result_of_a_health_report(self):
        run = self.run_module(base_params(), cluster_exists=True)

        for secret in (module_harness.SECRET_NODE_TOKEN,
                       module_harness.SECRET_KUBECONFIG,
                       module_harness.SECRET_SSH_KEY):
            self.assertNotIn(secret, run.stdout)

    def test_the_authentication_key_is_scrubbed_from_the_invocation(self):
        """no_log on key, which Ansible returns module_args without it.

        Ansible puts the module's arguments back in the result as
        invocation.module_args, so a key with no_log left off is returned
        verbatim to the play and whatever logs it. The placeholder is
        asserted on as well as the absence, because a key which simply
        failed to arrive would also satisfy the absence.
        """
        # _ansible_inject_invocation is how a controller asks for the
        # invocation dictionary. ansible-core 2.21 made it opt in and
        # defaults it off; every version from 2.15, which meta/runtime.yml
        # declares as the floor, returns it unconditionally. Asking for it
        # here pins the worst case on every supported version rather than
        # the behaviour of whichever one is installed.
        run = self.run_module(
            base_params(_ansible_inject_invocation=True, **FULL_CONNECTION),
            cluster_exists=True)

        self.assertNotIn(SECRET_AUTH_KEY, run.stdout)
        self.assertEqual(
            'VALUE_SPECIFIED_IN_NO_LOG_PARAMETER',
            run.result['invocation']['module_args']['key'])
        # The other two are not secrets and are returned as given, which is
        # what makes the line above a statement about no_log.
        self.assertEqual('http://sf-1:13000',
                         run.result['invocation']['module_args']['api_url'])


class CheckModeTestCase(ModuleTestCase):
    """Check mode says what would happen without any of it happening.

    Both directions are tested because they are two separate early
    returns in two separate functions, and a module which gets one right
    and the other wrong destroys a cluster during a --check run.
    """

    def test_check_mode_reports_a_create_without_creating(self):
        run = self.run_module(
            base_params(**{'_ansible_check_mode': True}),
            cluster_exists=False, fake_create=True)

        self.assertTrue(run.changed)
        self.assertIsNone(run.result['health'])
        self.assertNothingMutated(run)

    def test_check_mode_reports_a_delete_without_deleting(self):
        run = self.run_module(
            base_params(state='absent', **{'_ansible_check_mode': True}),
            cluster_exists=True)

        self.assertTrue(run.changed)
        self.assertNothingMutated(run)

    def test_check_mode_on_a_cluster_already_at_the_shape_is_no_change(self):
        run = self.run_module(
            base_params(**{'_ansible_check_mode': True}), cluster_exists=True)

        self.assertFalse(run.changed)
        self.assertNothingMutated(run)


class IdempotencyTestCase(ModuleTestCase):
    """Existence is the whole of what this module reconciles.

    Decision 5 of the phase 5 plan: conductor owns worker membership and
    this module owns "a cluster exists". The never-reconciled rule is the
    one a future contributor is most likely to undo with a one line "and
    if it differs, expand", so the mismatch cases are pinned explicitly
    rather than left to follow from the absence of a reconciling branch.
    """

    def test_an_existing_cluster_is_not_a_change(self):
        run = self.run_module(base_params(), cluster_exists=True)

        self.assertFalse(run.changed)
        self.assertTrue(run.result['health']['healthy'])
        self.assertEqual('created', run.result['health']['state'])
        self.assertNothingMutated(run)

    def test_more_workers_than_initial_workers_is_not_a_change(self):
        """Three workers, initial_workers 0: not a change and not resized."""
        run = self.run_module(base_params(initial_workers=0),
                              cluster_exists=True, worker_nodes=3)

        self.assertFalse(run.changed)
        self.assertEqual(
            3, len([n for n in run.result['health']['nodes']
                    if n['role'] == 'worker']),
            'the scenario did not build a cluster whose size differs')
        self.assertNothingMutated(run)

    def test_fewer_workers_than_initial_workers_is_not_a_change(self):
        """And the other direction, which is the one expand_workers() exists for."""
        run = self.run_module(base_params(initial_workers=7),
                              cluster_exists=True, worker_nodes=1)

        self.assertFalse(run.changed)
        self.assertNotIn('expand_workers', run.cluster_calls)
        self.assertNothingMutated(run)

    def test_other_shape_parameters_are_not_reconciled_either(self):
        """Workers are singled out because they have a competing writer.

        The rest are simply verbs this library does not have, and a module
        which tried to reconcile them would have to invent them.
        """
        run = self.run_module(
            base_params(control_plane_count=3, metal_address_count=99,
                        release_channel='v1.26', install_longhorn=False,
                        install_metallb=False),
            cluster_exists=True)

        self.assertFalse(run.changed)
        self.assertNothingMutated(run)

    def test_absent_on_an_absent_cluster_is_not_a_change(self):
        run = self.run_module(base_params(state='absent'),
                              cluster_exists=False)

        self.assertFalse(run.changed)
        self.assertIsNone(run.result['health'])
        self.assertNothingMutated(run)

    def test_absent_on_an_existing_cluster_deletes_it(self):
        run = self.run_module(base_params(state='absent'),
                              cluster_exists=True, instance_state='deleted')

        self.assertTrue(run.changed)
        self.assertIsNone(run.result['health'])
        self.assertIn('delete', run.cluster_calls)
        self.assertIn('delete_instance', run.client_calls)


class InterruptedClusterTestCase(ModuleTestCase):
    """A cluster an earlier run was interrupted while building.

    It is neither absent nor present at the requested shape, and the one
    answer which must not be given is "present": its nodes, tokens and
    kubeconfig are in an unknown combination of there and not there, so a
    play told the cluster is ready hands a broken cluster to whatever
    comes next.
    """

    def test_an_interrupted_cluster_fails(self):
        run = self.run_module(base_params(), cluster_exists=True,
                              cluster_state='initial', expect_failure=True)

        self.assertTrue(run.failed)
        # The state found, because the recovery depends on it and because a
        # message which does not say what was wrong sends the operator to
        # the metadata document to find out.
        self.assertIn('initial', run.msg)
        self.assertIn('state: absent', run.msg)
        self.assertNothingMutated(run)

    def test_an_interrupted_cluster_is_reported_as_interrupted(self):
        """The health report comes back, so a play can see what was found."""
        run = self.run_module(base_params(), cluster_exists=True,
                              cluster_state='initial', expect_failure=True)

        self.assertTrue(run.result['health']['interrupted'])
        self.assertFalse(run.result['health']['healthy'])

    def test_an_interrupted_cluster_can_still_be_deleted(self):
        """Which is the recovery the failure above points at."""
        run = self.run_module(base_params(state='absent'),
                              cluster_exists=True, cluster_state='initial',
                              instance_state='deleted')

        self.assertTrue(run.changed)
        self.assertIn('delete', run.cluster_calls)


class CreateTestCase(ModuleTestCase):
    """What a create is told, and what comes back from one."""

    def test_a_create_is_a_change_and_reports_health(self):
        run = self.run_module(base_params(), cluster_exists=False,
                              fake_create=True)

        self.assertTrue(run.changed)
        self.assertIn('create', run.cluster_calls)
        self.assertTrue(run.result['health']['healthy'])

    def test_initial_workers_is_the_worker_count_create_is_given(self):
        """The module's one use of a worker count.

        initial_workers becomes create()'s worker_count on the path which
        creates a cluster; otherwise it is read only by the floor check
        made before a client is built. The default is 0 rather
        than the command line's 2, because a play which hands the cluster
        straight to a scaler wants control plane nodes and nothing else.
        """
        run = self.run_module(base_params(initial_workers=4),
                              cluster_exists=False, fake_create=True)

        self.assertEqual('4', run.create_kwargs['worker_count'])

    def test_the_default_worker_count_is_zero(self):
        run = self.run_module(base_params(), cluster_exists=False,
                              fake_create=True)

        self.assertEqual('0', run.create_kwargs['worker_count'])

    def test_every_shape_parameter_reaches_create(self):
        """A parameter which is accepted and then dropped is worse than absent."""
        run = self.run_module(
            base_params(initial_workers=2, control_plane_count=3,
                        metal_address_count=9, network='borrowed-net',
                        release_channel='v1.26', install_metallb=False,
                        install_longhorn=False),
            cluster_exists=False, fake_create=True)

        self.assertEqual(
            {'control_plane_count': '3',
             'worker_count': '2',
             'metal_address_count': '9',
             'network': "'borrowed-net'",
             'release_channel': "'v1.26'",
             'sshkey': 'None',
             'install_metallb': 'False',
             'install_longhorn': 'False',
             'manifests': 'None'},
            run.create_kwargs)

    def test_create_is_not_asked_to_touch_the_local_machine(self):
        """write_kubeconfig and refresh_version_cache stay at their defaults.

        Rewriting ~/.kube/config on whichever machine happened to run the
        module is a side effect outside the cluster, and a version cache
        refresh is something a human asks for once rather than a property
        of the cluster a play declares. Both are absent rather than passed
        as False, so this asserts on the whole call above and on their
        absence here.
        """
        run = self.run_module(base_params(), cluster_exists=False,
                              fake_create=True)

        self.assertNotIn('write_kubeconfig', run.create_kwargs)
        self.assertNotIn('refresh_version_cache', run.create_kwargs)


class ExceptionTestCase(ModuleTestCase):
    """A cluster shaped problem is a failure with a message, not a traceback.

    Every exception this library raises for one derives from
    K3sClusterException and renders itself as the sentence the command
    line prints. Letting one out would reach Ansible as a MODULE FAILURE
    with a traceback and, worse, with the collected progress thrown away:
    the log is the only record of how far a twenty minute create got.
    """

    def test_a_cluster_exception_becomes_a_failure(self):
        """A real one, from the real create(), with nothing stubbed out.

        read_manifests() is the first thing create() does, before it looks
        at any other argument or touches the API, so this exercises the
        module's handler against a genuinely raised ManifestError.
        """
        bad = os.path.join(self.tempdir, 'not-a-manifest.txt')
        with open(bad, 'w', encoding='utf-8') as f:
            f.write('irrelevant\n')
        run = self.run_module(base_params(manifests=[bad]),
                              cluster_exists=False, expect_failure=True)

        self.assertTrue(run.failed)
        self.assertIn('not-a-manifest.txt', run.msg)
        self.assertNotIn('Traceback', run.stderr)
        # And a log key even when there was nothing to collect, because a
        # play which reads it unconditionally should not have to guard.
        self.assertEqual([], run.log)

    def test_a_failure_part_way_through_keeps_the_progress(self):
        """The log survives the failure, which is what it is for."""
        run = self.run_module(base_params(), cluster_exists=False,
                              fake_create=True, create_raises=True,
                              expect_failure=True)

        self.assertTrue(run.failed)
        self.assertIn('id_rsa.pub', run.msg)
        self.assertNotIn('Traceback', run.stderr)
        self.assertNotEqual([], run.log)
        self.assertIn(
            'Creating node network', '\n'.join(run.log),
            'the progress emitted before the failure was thrown away')


class ApiFailureTestCase(ModuleTestCase):
    """What the client raises, which Cluster does not wrap.

    The module used to catch only K3sClusterException, on the reasoning
    that the library wraps what goes wrong with a cluster. It does, for
    cluster-shaped problems -- but the API client raises its own
    exceptions and Cluster passes most of them straight through:
    cluster.py catches apiclient.APIException at :761, :2203 and :2250
    and nowhere else, which is itself the evidence that it arrives
    unwrapped. Found by the review of #90.

    What made it worth fixing rather than noting is the second half: an
    escaping exception is reported by Ansible as MODULE FAILURE with a
    traceback, and the collected log goes with it. For a create which ran
    for twenty minutes and then lost its connection, that log is the only
    record of how far it got.
    """

    def test_an_unauthorized_api_is_a_message_not_a_traceback(self):
        run = self.run_module(base_params(), metadata_raises='unauthorized',
                              expect_failure=True)

        self.assertTrue(run.failed)
        self.assertNotIn('Traceback', run.stderr)
        self.assertIn('refused a request', run.msg)
        self.assertIn('namespace ci is not yours', run.msg)

    def test_an_unreachable_api_fails_before_the_client_exists(self):
        """The likeliest misconfiguration of all, at the earliest site.

        apiclient.Client.__init__ calls _collect_capabilities(), which GETs
        base_url, so a wrong api_url raises inside make_client() rather
        than on the first real call. The review of #90 found the
        orchestration handler missing these; sweeping for the same shape
        found the construction handler missing them too, which is the site
        a first run with a typo in the inventory actually reaches.
        """
        run = self.run_module(base_params(**FULL_CONNECTION),
                              client_raises=True, expect_failure=True)

        self.assertTrue(run.failed)
        self.assertNotIn('Traceback', run.stderr)
        self.assertIn('Could not reach the Shaken Fist API', run.msg)
        # The api_url it could not reach, so the fix is in the message.
        self.assertIn(FULL_CONNECTION['api_url'], run.msg)
        # And that nothing was attempted, which is the reassurance the
        # operator needs before they re-run it.
        self.assertIn('nothing has been created', run.msg)
        self.assertEqual([], run.diagnostics['cluster_calls'])

    def test_an_api_error_keeps_the_log(self):
        run = self.run_module(base_params(), metadata_raises='api',
                              expect_failure=True)

        self.assertTrue(run.failed)
        self.assertNotIn('Traceback', run.stderr)
        # The log is the point of catching these at all, so its presence is
        # the assertion rather than its contents: this failure happens on
        # the first client call, before anything has been reported, so an
        # empty list is the correct answer and None is not.
        self.assertEqual([], run.result['log'])
        self.assertIn('log', run.result)

    def test_an_unreachable_api_says_so_and_says_what_to_do(self):
        run = self.run_module(base_params(), metadata_raises='requests',
                              expect_failure=True)

        self.assertTrue(run.failed)
        self.assertNotIn('Traceback', run.stderr)
        self.assertIn('Could not reach the Shaken Fist API', run.msg)
        self.assertIn('connection refused', run.msg)
        # This failure is the first metadata read, before anything has been
        # touched, so the advice is to re-run and nothing more. An earlier
        # version of this test asserted 'state: absent' here, which pinned
        # exactly the misleading wording _Mutation was introduced to
        # remove: telling an operator to delete a cluster this run had not
        # created. The review of #90 found the test locking in the bug.
        self.assertIn('Nothing was changed', run.msg)
        self.assertNotIn('state: absent', run.msg)
        self.assertNotIn('partly built', run.msg)
        self.assertFalse(run.result['changed'])


class ShapeRangeTestCase(ModuleTestCase):
    """Values a playbook can produce that a human would not type.

    A templated control_plane_count which resolved to 0, or an empty
    inventory variable coerced to an int, builds a cluster with no control
    plane -- and says so tens of minutes later, from somewhere that does
    not mention the option. Raised by the review of #90, which noted a
    declarative module is far likelier to receive such a value than the
    CLI is.

    The floors are the library's validate_create_counts(), which the module
    calls before it builds a client, so each refusal is asserted to have
    asked nothing of the API. The messages are asserted whole, because the
    module has the counts named after its own options and the message has
    to name the option the playbook set rather than create()'s parameter.
    """

    def _assert_refused(self, params, message):
        run = self.run_module(base_params(**params), expect_failure=True)

        self.assertEqual(message, run.msg)
        self.assertIsNone(run.result['health'])
        # Refused before anything was asked of the API, which is the point:
        # the alternative is finding out after the instances exist.
        self.assertEqual([], run.diagnostics['cluster_calls'])
        self.assertEqual([], run.diagnostics['client_calls'])
        return run

    def test_a_cluster_with_no_control_plane_is_refused(self):
        self._assert_refused({'control_plane_count': 0},
                             'control_plane_count must be at least 1, not 0.')

    def test_a_negative_worker_count_is_refused(self):
        self._assert_refused({'initial_workers': -1},
                             'initial_workers must be at least 0, not -1.')

    def test_a_negative_address_count_is_refused(self):
        self._assert_refused({'metal_address_count': -5},
                             'metal_address_count must be at least 0, not -5.')

    def test_the_checks_do_not_apply_to_a_delete(self):
        """None of the three means anything when removing a cluster.

        Refusing them there would fail a task whose request is perfectly
        clear, over values it will never read.
        """
        run = self.run_module(base_params(state='absent',
                                          control_plane_count=0))

        self.assertFalse(run.failed)
        self.assertFalse(run.result['changed'])


class NameRuleTestCase(ModuleTestCase):
    """The cluster name rule applies only when this run would create the cluster.

    The rule is create()'s alone, so that a cluster which already exists
    under a name it refuses -- one created before the rule did -- can still
    be declared present, and removed. A play which would create such a
    cluster is refused before anything is built, and must say so with
    changed false and without advising a delete of debris which does not
    exist.
    """

    NAME = 'my.cluster'

    def test_present_on_a_new_cluster_is_refused(self):
        run = self.run_module(base_params(name=self.NAME),
                              cluster_exists=False, expect_failure=True)

        # Not "run it again": a refused name is refused on every run.
        self.assertEqual(
            "%s Nothing was changed. Correct the task's parameters before "
            'running it again.'
            % sf_exceptions.ClusterNameError.invalid_characters(self.NAME),
            run.msg)
        self.assertIs(False, run.result['changed'])
        self.assertIsNone(run.result['health'])
        self.assertNotIn('create', run.cluster_calls)
        self.assertNothingMutated(run)

    def test_check_mode_on_a_new_cluster_predicts_the_refusal(self):
        run = self.run_module(
            base_params(name=self.NAME, **{'_ansible_check_mode': True}),
            cluster_exists=False, expect_failure=True)

        self.assertIn("Cluster name 'my.cluster' cannot be used.", run.msg)
        self.assertIs(False, run.result['changed'])
        self.assertNothingMutated(run)

    def test_present_on_an_existing_cluster_is_not_a_change(self):
        run = self.run_module(base_params(name=self.NAME),
                              cluster_exists=True)

        self.assertFalse(run.failed)
        self.assertFalse(run.changed)
        self.assertNothingMutated(run)

    def test_absent_deletes_an_existing_cluster(self):
        """A cluster created before the rule existed can still be removed.

        The partner of test_absent_is_not_refused below, which finds no
        cluster: here one exists under the refused name, and the module
        has to delete it rather than merely not refuse.
        """
        run = self.run_module(base_params(name=self.NAME, state='absent'),
                              cluster_exists=True, instance_state='deleted')

        self.assertFalse(run.failed)
        self.assertIs(True, run.result['changed'])
        self.assertIn('delete', run.cluster_calls)
        self.assertIn('delete_instance', run.client_calls)

    def test_absent_is_not_refused(self):
        """absent on such a name reaches the library's delete path.

        The fake serves no metadata for this name, so the library finds no
        cluster to delete and the task reports no change.
        """
        run = self.run_module(base_params(name=self.NAME, state='absent'))

        self.assertFalse(run.failed)
        self.assertFalse(run.result['changed'])
        self.assertNotIn('Cluster name', run.msg)


class ReservedNameTestCase(ModuleTestCase):
    """A name whose metadata key is a release cache's fails the task, not the module.

    Cluster() refuses such a name on every verb, and the module builds its
    Cluster outside the handler which turns library exceptions into
    fail_json(). Without its own guard the refusal would reach Ansible as a
    traceback -- a regression for state: present, which used to be refused
    (confusingly, as an interrupted cluster) by create() inside that
    handler.
    """

    def _assert_refused(self, state):
        run = self.run_module(base_params(name='k3s_version_cache',
                                          state=state),
                              expect_failure=True)

        self.assertTrue(run.failed)
        self.assertIn("Cluster name 'k3s_version_cache' cannot be used.",
                      run.msg)
        self.assertIn('orchestrated_k3s_cluster_k3s_version_cache', run.msg)
        self.assertIsNone(run.result['health'])
        # Refused before any verb ran or anything was written.
        self.assertEqual([], run.diagnostics['cluster_calls'])
        self.assertEqual([], run.diagnostics['client_calls'])

    def test_present_is_refused(self):
        self._assert_refused('present')

    def test_absent_is_refused(self):
        # The path which used to be a bare KeyError from delete().
        self._assert_refused('absent')


class MutationStateTestCase(ModuleTestCase):
    """What the failure paths say, and whether they admit to a change.

    One object records how far a run got, and both the advice and the
    changed flag are read off it, so these tests pin the three cases
    together rather than one message at a time. The review of #90 found
    all three wrong in the same direction: unconditional "may be partly
    built" advice, and fail_json() without changed.
    """

    def test_a_failure_before_any_mutation_admits_no_change(self):
        run = self.run_module(base_params(), metadata_raises='api',
                              expect_failure=True)

        self.assertFalse(run.result['changed'])
        self.assertIn('Nothing was changed', run.msg)
        self.assertEqual([], run.diagnostics['cluster_calls'])

    def test_a_failure_during_a_create_reports_the_change(self):
        """The case handlers and callbacks are misled by.

        create() has booted instances and written metadata by the time it
        raises, so a task reported as unchanged tells every handler keyed
        on changed that there is nothing to react to.
        """
        run = self.run_module(base_params(), fake_create=True,
                              create_raises=True, expect_failure=True)

        self.assertTrue(run.failed)
        self.assertTrue(run.result['changed'])
        # And the advice that fits a half-built cluster.
        self.assertIn('partly built', run.msg)
        self.assertIn('state: absent', run.msg)

    def test_a_failure_during_a_delete_says_the_delete_was_interrupted(self):
        """Not 'state: absent removes whatever exists', which is circular.

        The operator already asked for state: absent. Telling them to do
        it again is only useful if the message says why -- that the delete
        itself stopped part way.
        """
        run = self.run_module(
            base_params(state='absent'), cluster_exists=True,
            instance_state='created', delete_raises=True,
            expect_failure=True)

        self.assertTrue(run.failed)
        self.assertTrue(run.result['changed'])
        self.assertIn('delete was interrupted', run.msg)


class ProbeFailureTestCase(ModuleTestCase):
    """A health probe that fails is not a cluster that failed.

    health() reads every node through get_instance() and runs an agent
    operation for its kubectl check, so it can fail transiently while the
    cluster is perfectly fine. The review of #90 found that such a failure
    after a create which had already succeeded was reported with the outer
    handler's "the cluster may be partly built; state: absent removes
    whatever exists" message, and without changed=True -- so an operator
    following the advice would destroy a healthy cluster they did not know
    they had built.
    """

    def test_a_probe_failure_after_a_create_still_reports_the_change(self):
        run = self.run_module(base_params(), fake_create=True,
                              health_raises=True)

        # The create happened, so the play has to be told, whatever the
        # probe did afterwards.
        self.assertTrue(run.result['changed'])
        self.assertFalse(run.failed)
        self.assertIsNone(run.result['health'])
        # And it must not be the destructive advice.
        self.assertNotIn('state: absent', json.dumps(run.result))
        self.assertNotIn('partly built', json.dumps(run.result))

    def test_a_probe_failure_after_a_create_warns(self):
        run = self.run_module(base_params(), fake_create=True,
                              health_raises=True)

        warnings = ' '.join(
            _warning_text(w) for w in run.result.get('warnings', []))
        self.assertIn('cluster was created', warnings)
        self.assertIn('health probe', warnings)
        # The progress of the create it did do is still there.
        self.assertTrue(run.result['log'])

    def test_a_probe_failure_on_an_existing_cluster_changes_nothing(self):
        run = self.run_module(base_params(), cluster_exists=True,
                              health_raises=True, expect_failure=True)

        self.assertTrue(run.failed)
        self.assertFalse(run.result['changed'])
        # It failed, but about the probe rather than about the cluster, and
        # without telling anyone to delete anything.
        self.assertIn('Could not read the health', run.msg)
        self.assertNotIn('state: absent', run.msg)
        self.assertNotIn('partly built', run.msg)
        self.assertNotIn('Traceback', run.stderr)


class MissingLibraryTestCase(ModuleTestCase):
    """A control node with the collection and not the plugin.

    This is the likeliest first-run failure there is, because Galaxy
    installs Ansible content and cannot install a Python package: the
    documented install is two commands and only one of them is
    ansible-galaxy's. An unguarded import makes that a
    ModuleNotFoundError traceback, which tells the operator what Python
    could not find rather than what to do about it.

    Driven without the harness, because the harness imports the same
    package it would have to hide.
    """

    def _run_without_the_plugin(self, params):
        """Run the module with shakenfist_client_k3s shadowed by a stub."""
        stubdir = os.path.join(self.tempdir, 'stub')
        os.makedirs(stubdir)
        with open(os.path.join(stubdir, 'shakenfist_client_k3s.py'), 'w',
                  encoding='utf-8') as f:
            f.write("raise ImportError('stub: not installed')\n")

        env = dict(os.environ)
        # First on the path, so it wins over the real package. PYTHONPATH
        # rather than a sys.path edit because the module runs in its own
        # interpreter, which is also how Ansible runs it.
        env['PYTHONPATH'] = stubdir + os.pathsep + env.get('PYTHONPATH', '')

        # No _ANSIBLE_ARGS to set here, so the module reads its parameters
        # from stdin the way a controller without that variable makes it.
        process = subprocess.run(
            [sys.executable, MODULE],
            input=json.dumps({'ANSIBLE_MODULE_ARGS': params}),
            capture_output=True, encoding='utf-8', env=env,
            timeout=RUN_TIMEOUT)
        return process

    def test_a_missing_plugin_says_to_install_it(self):
        process = self._run_without_the_plugin(base_params())

        self.assertEqual(1, process.returncode)
        result = json.loads(process.stdout)
        self.assertTrue(result['failed'])
        # missing_required_lib()'s wording, which is what makes this
        # recognisable to anyone who has met it in another collection.
        self.assertIn('Failed to import the required Python library',
                      result['msg'])
        self.assertIn('shakenfist_client_k3s', result['msg'])
        # And the reason the usual advice is not enough here: the
        # collection cannot install it for them.
        self.assertIn('requirements.txt', result['msg'])

    def test_a_missing_plugin_keeps_the_traceback_for_diagnosis(self):
        # _ansible_tracebacks_for is asked for explicitly, for the same
        # reason the no_log test asks for the invocation: ansible-core 2.21
        # made traceback capture opt-in, so on that version fail_json()
        # drops the exception key unless a controller enabled it, while
        # every version from 2.15 -- the floor meta/runtime.yml declares --
        # returns it unconditionally. Requesting it pins the behaviour the
        # module is responsible for (handing the traceback to fail_json)
        # rather than whichever ansible-core happens to be installed.
        params = base_params()
        params['_ansible_tracebacks_for'] = ['error']
        process = self._run_without_the_plugin(params)

        result = json.loads(process.stdout)
        # Kept, because a genuinely broken install -- as opposed to an
        # absent one -- is only diagnosable from the import error itself.
        # It belongs in the result rather than on stderr, where Ansible
        # would render it as a crash.
        self.assertIn('ImportError', result['exception'])
        self.assertIn('stub: not installed', result['exception'])
        self.assertNotIn('Traceback', process.stderr)

    def test_the_result_is_still_one_json_document(self):
        """The stdout contract holds on the failure path too."""
        process = self._run_without_the_plugin(base_params())

        decoder = json.JSONDecoder()
        _, end = decoder.raw_decode(process.stdout.strip())
        self.assertEqual(
            len(process.stdout.strip()), end,
            'something follows the JSON document on stdout: %r'
            % process.stdout)


class StdoutTestCase(ModuleTestCase):
    """The most important test in this file.

    A module's stdout is its result. Ansible reads file descriptor 1 and
    parses it as JSON, so one stray write anywhere in the module or in the
    library it imports turns every successful run into a module failure in
    a playbook -- and no assertion about the returned dictionary notices,
    because the dictionary is still right. What is captured below came off
    a real fd 1 of a real subprocess.

    The assertion is not "stdout is empty", which would be false: the
    result is on stdout. It is that stdout holds exactly one JSON
    document, which is what Ansible requires and what a leading progress
    line, a trailing warning or a debug print breaks.
    """

    # Every path the module has, named by what makes it distinct. A write
    # to stdout from any of them is a write Ansible cannot parse, so this
    # is deliberately the whole set rather than a representative sample --
    # the create path in particular emits a great deal of progress, and is
    # the one where the leak would be invisible in every other test.
    SCENARIOS = {
        'an existing healthy cluster': (
            base_params(), {'cluster_exists': True}, False),
        'an absent cluster in check mode': (
            base_params(**{'_ansible_check_mode': True}),
            {'cluster_exists': False}, False),
        'a create which emits progress': (
            base_params(), {'cluster_exists': False, 'fake_create': True},
            False),
        'a create which fails part way': (
            base_params(),
            {'cluster_exists': False, 'fake_create': True,
             'create_raises': True}, True),
        'a delete which dumps metadata at debug level': (
            base_params(state='absent'),
            {'cluster_exists': True, 'instance_state': 'deleted'}, False),
        'absent on an absent cluster': (
            base_params(state='absent'), {'cluster_exists': False}, False),
        'a delete in check mode': (
            base_params(state='absent', **{'_ansible_check_mode': True}),
            {'cluster_exists': True}, False),
        'an interrupted cluster': (
            base_params(),
            {'cluster_exists': True, 'cluster_state': 'initial'}, True),
        'a partial connection set': (
            base_params(api_url='http://sf-1:13000', auth_namespace='system'),
            {}, True),
        'a client which cannot be configured': (
            base_params(), {'unconfigured': True}, True),
    }

    def test_the_module_emits_one_json_document_on_stdout(self):
        for description, (params, spec, fails) in self.SCENARIOS.items():
            run = self.run_module(params, expect_failure=fails, **spec)

            # run_module() has already parsed it, so reaching here is most
            # of the assertion. Say so explicitly anyway, because a future
            # refactor of run_module() could stop parsing.
            raw = run.stdout
            try:
                parsed = json.loads(raw)
            except ValueError as e:
                self.fail(
                    'stdout of %s is not one JSON document (%s): %r'
                    % (description, e, raw[:500]))
            self.assertIsInstance(parsed, dict, description)

            # And that the document ends where stdout ends. json.loads()
            # already refuses trailing content, so this says out loud what
            # passing it proves, with the offending tail named rather than
            # left to a decoder message.
            _, end = json.JSONDecoder().raw_decode(raw.lstrip())
            self.assertEqual(
                '', raw.lstrip()[end:].strip(),
                'something followed the result on stdout of %s' % description)

    def test_a_create_puts_its_progress_in_the_log_not_on_stdout(self):
        """Where the progress went, not merely that stdout parsed.

        The two halves are one property: the reporter catches the output
        and hands it back. A module which discarded the progress entirely
        would pass the stdout assertion above and fail here.
        """
        run = self.run_module(base_params(), cluster_exists=False,
                              fake_create=True)

        self.assertNotEqual([], run.log)
        self.assertIn('Creating node network', '\n'.join(run.log))
        # One line per line, with no line endings left on them, because a
        # play which prints the log should not get blank lines between
        # every pair.
        for line in run.log:
            self.assertNotIn('\n', line)
        # And the only occurrence of that text is inside the JSON, which
        # is the half that says it did not also go to stdout.
        self.assertEqual(
            1, run.stdout.count('Creating node network'),
            'progress reached stdout as well as the log')

    def test_nothing_is_written_to_stdout_before_the_result(self):
        """A leading write is the shape this fails in, so pin the first byte."""
        run = self.run_module(base_params(), cluster_exists=True)

        self.assertEqual('{', run.stdout.lstrip()[0])
        self.assertEqual('}', run.stdout.rstrip()[-1])
