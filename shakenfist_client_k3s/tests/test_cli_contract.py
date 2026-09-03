import difflib
import os

from click.testing import CliRunner
import testtools

import shakenfist_client_k3s


# The current CLI output is the contract this phase's refactor is checked
# against, so the golden fixtures below were generated from the pre-refactor
# tree rather than hand-typed. See the generation note in the class
# docstring for how to regenerate them if a later phase intentionally
# changes user-visible text (it should not, in this phase).
FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cli_contract')

# Pinned so the wrapped help text is identical regardless of the terminal
# (or CI runner's) COLUMNS setting. Verified stable by generating the
# fixtures with COLUMNS=80 and COLUMNS=200 in the environment and diffing
# the results, which were identical.
TERMINAL_WIDTH = 80

SUBCOMMANDS = [
    'list',
    'create',
    'query-k3s-version',
    'query-longhorn-version',
    'getconfig',
    'show',
    'delete',
    'expand-workers',
    'expand-addresses',
    'update-os',
]


class CliContractTestCase(testtools.TestCase):
    """Assert that --help output for the k3s group and its subcommands is unchanged.

    These fixtures were generated from the pre-refactor tree with a
    throwaway script invoking CliRunner against each --help target and
    writing the output to shakenfist_client_k3s/tests/cli_contract/*.txt,
    which was then deleted. They must not be hand-edited: if a later
    change in this phase alters this output, that is a bug, not a fixture
    update, per the phase plan's decision 7.
    """

    def setUp(self):
        super(CliContractTestCase, self).setUp()
        self.runner = CliRunner()

    def _assert_help_matches(self, args, fixture_name):
        result = self.runner.invoke(
            shakenfist_client_k3s.k3s, args, obj={'VERBOSE': False},
            terminal_width=TERMINAL_WIDTH)
        self.assertEqual(0, result.exit_code, result.output)

        fixture_path = os.path.join(FIXTURE_DIR, fixture_name)
        with open(fixture_path) as f:
            expected = f.read()

        if result.output != expected:
            diff = ''.join(difflib.unified_diff(
                expected.splitlines(keepends=True),
                result.output.splitlines(keepends=True),
                fromfile='expected (%s)' % fixture_path,
                tofile='actual'))
            self.fail('--help output for %r does not match golden fixture:\n%s'
                      % (args, diff))

    def test_group_help(self):
        self._assert_help_matches(['--help'], 'group.txt')

    def test_subcommand_help(self):
        for name in SUBCOMMANDS:
            self._assert_help_matches([name, '--help'], '%s.txt' % name)
