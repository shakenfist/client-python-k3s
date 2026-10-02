"""Tests for tools/check-dist.sh, the release gate on wheel contents.

The script is the only thing standing between a packaging accident and a
permanent PyPI upload, so it is worth knowing that it still fails when it
should. Proving that by hand once, at the time it was written, says
nothing about the version in the tree now.

The wheels here are built with zipfile rather than by setuptools: the
script only reads the archive's name list, so a handful of fabricated
entries exercises it exactly as a real wheel would, and does so in
milliseconds.
"""
import os
import subprocess
import tempfile
import zipfile

import testtools


# tools/ is not part of the installed package -- the wheel contains the
# six modules and nothing else, which is the property this script is
# here to defend. Locate it relative to the source tree and skip when
# running against an install.
_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CHECK_DIST = os.path.join(_REPO_ROOT, 'tools', 'check-dist.sh')

GOOD_ENTRIES = [
    'shakenfist_client_k3s/__init__.py',
    'shakenfist_client_k3s/cluster.py',
    'shakenfist_client_k3s-0.1.0.dist-info/METADATA',
    'shakenfist_client_k3s-0.1.0.dist-info/RECORD',
    'shakenfist_client_k3s-0.1.0.dist-info/licenses/LICENSE',
]


class CheckDistTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()
        if not os.path.exists(CHECK_DIST):
            self.skipTest('tools/check-dist.sh is not in an installed package')
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.tempdir = tempdir.name

    def _wheel(self, entries, name='pkg-0.1.0-py3-none-any.whl'):
        path = os.path.join(self.tempdir, name)
        with zipfile.ZipFile(path, 'w') as z:
            for entry in entries:
                z.writestr(entry, 'x\n')
        return path

    def _run(self, *args):
        return subprocess.run(
            [CHECK_DIST] + list(args), capture_output=True, text=True)

    def test_a_clean_wheel_passes(self):
        result = self._run(self._wheel(GOOD_ENTRIES))
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn('no /tests/ paths', result.stdout)

    def test_a_test_path_fails_and_is_named(self):
        wheel = self._wheel(
            GOOD_ENTRIES + ['shakenfist_client_k3s/tests/test_cluster.py'])
        result = self._run(wheel)
        self.assertEqual(1, result.returncode, result.stdout)
        self.assertIn('test paths that must not ship', result.stdout)
        self.assertIn('shakenfist_client_k3s/tests/test_cluster.py',
                      result.stdout)
        self.assertIn('include-package-data', result.stdout)

    def test_a_test_path_behind_a_space_still_fails(self):
        """A space earlier in the path must not hide what follows it.

        This is the regression which an earlier version of the script
        had: it took the first whitespace-delimited field of
        "python3 -m zipfile -l" output, so everything from the first
        space onwards was discarded before the /tests/ match ran. An
        entry like this one was therefore reported as OK.
        """
        wheel = self._wheel(
            GOOD_ENTRIES + ['shakenfist_client_k3s/a b/tests/leak.py'])
        result = self._run(wheel)
        self.assertEqual(
            1, result.returncode,
            'a test path after a space in the name was not noticed: %s'
            % result.stdout)
        self.assertIn('shakenfist_client_k3s/a b/tests/leak.py', result.stdout)

    def test_a_root_level_tests_directory_fails(self):
        """The match cannot require a leading slash.

        A wheel built from a different packages.find configuration could
        put the suite at the archive root as tests/, where a /tests/
        match would not see it. The script is the backstop for the
        configuration being wrong, so it cannot assume the shape the
        current configuration produces.
        """
        result = self._run(self._wheel(GOOD_ENTRIES + ['tests/test_x.py']))
        self.assertEqual(
            1, result.returncode,
            'a root-level tests/ entry was not noticed: %s' % result.stdout)
        self.assertIn('tests/test_x.py', result.stdout)

    def test_too_many_entries_fails_and_lists_them(self):
        wheel = self._wheel(
            ['shakenfist_client_k3s/mod%02d.py' % n for n in range(13)])
        result = self._run(wheel)
        self.assertEqual(1, result.returncode, result.stdout)
        self.assertIn('has 13 entries, expected at most 12', result.stdout)
        self.assertIn('shakenfist_client_k3s/mod12.py', result.stdout)

    def test_exactly_the_maximum_passes(self):
        wheel = self._wheel(
            ['shakenfist_client_k3s/mod%02d.py' % n for n in range(12)])
        result = self._run(wheel)
        self.assertEqual(0, result.returncode, result.stdout)

    def test_every_wheel_is_checked_not_only_the_first(self):
        """release.yml passes dist/*.whl, which may one day match two."""
        good = self._wheel(GOOD_ENTRIES, name='good-0.1.0-py3-none-any.whl')
        bad = self._wheel(
            GOOD_ENTRIES + ['shakenfist_client_k3s/tests/test_x.py'],
            name='bad-0.1.0-py3-none-any.whl')
        result = self._run(good, bad)
        self.assertEqual(
            1, result.returncode,
            'the second wheel was not checked: %s' % result.stdout)
        self.assertIn('shakenfist_client_k3s/tests/test_x.py', result.stdout)

    def test_an_empty_archive_is_not_a_pass(self):
        """An empty wheel contains nothing unwanted, and is still wrong.

        The entry count comes from "grep -c ." behind a "|| true", because
        grep exits 1 when it counts nothing and the script runs under
        set -e. Without an explicit check that leaves a path where the
        script reports success for an archive with no entries at all.
        """
        result = self._run(self._wheel([]))
        self.assertEqual(
            1, result.returncode,
            'an empty archive was reported as acceptable: %s' % result.stdout)
        self.assertIn('no entries at all', result.stdout)

    def test_a_file_that_is_not_a_zip_is_reported_not_traced(self):
        path = os.path.join(self.tempdir, 'bad-0.1.0-py3-none-any.whl')
        with open(path, 'w') as f:
            f.write('this is not a zip archive\n')
        result = self._run(path)
        self.assertEqual(1, result.returncode)
        self.assertIn('not a readable zip archive', result.stdout)
        self.assertNotIn('Traceback', result.stderr)

    def test_no_arguments_is_a_usage_error(self):
        result = self._run()
        self.assertEqual(1, result.returncode)
        self.assertIn('Usage:', result.stdout)

    def test_a_missing_file_is_an_error(self):
        result = self._run(os.path.join(self.tempdir, 'absent.whl'))
        self.assertEqual(1, result.returncode)
        self.assertIn('No such file', result.stdout)
