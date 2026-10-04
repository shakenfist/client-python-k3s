"""The version conversion tools/build-collection.py does before a build.

The collection's version is not a string this project chooses: it is
whatever setuptools_scm derives, reshaped into something
``ansible-galaxy`` will accept. Galaxy does not allow a published version
to be replaced, so a conversion which silently maps two different
upstream versions onto one semver string is a defect that can only be
discovered after it has cost a version number.

Driven by importing the script rather than running it, because the
conversion is pure and the part which needs a git checkout -- asking
setuptools_scm -- is deliberately in a different function.
"""
import importlib.util
import os

import testtools


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
_SCRIPT = os.path.join(_REPO_ROOT, 'tools', 'build-collection.py')


def _load():
    """Import the build script by path.

    It is a script rather than a module in the package, so there is no
    import path to it; and it is not installed, so an installed copy of
    this package cannot run these tests. Returning None lets them skip the
    way test_check_dist.py skips.
    """
    if not os.path.exists(_SCRIPT):
        return None
    spec = importlib.util.spec_from_file_location('build_collection', _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SemverConversionTestCase(testtools.TestCase):

    def setUp(self):
        super().setUp()
        self.build = _load()
        if self.build is None:
            self.skipTest('tools/build-collection.py is not in this tree')

    def test_a_plain_release_is_unchanged(self):
        self.assertEqual('0.1.0', self.build.semver_from('0.1.0'))

    def test_a_release_candidate_gains_a_hyphen(self):
        """semantic_version requires the separator PEP 440 omits."""
        self.assertEqual('0.8.0-rc5', self.build.semver_from('0.8.0rc5'))

    def test_a_development_version_keeps_its_local_segment(self):
        """The shape every untagged build produces, and so the common one."""
        self.assertEqual(
            '0.1.1-dev12+g813d144',
            self.build.semver_from('0.1.1.dev12+g813d144'))

    def test_a_candidate_and_a_dev_segment_are_both_prerelease_parts(self):
        self.assertEqual(
            '0.2.0-rc1.dev3', self.build.semver_from('0.2.0rc1.dev3'))

    def test_a_post_release_is_refused_rather_than_flattened(self):
        """0.1.0.post1 must not quietly become 0.1.0.

        Galaxy will not replace a published version, so flattening it
        means the build succeeds and the publish either collides or
        silently replaces nothing. Failing here costs a re-tag; the
        alternative costs a version number.
        """
        e = self.assertRaises(self.build.UnrepresentableVersion,
                              self.build.semver_from, '0.1.0.post1')
        self.assertIn('post-release', str(e))
        # And it says what it would have collided with, which is the fact
        # that makes the failure actionable.
        self.assertIn('0.1.0', str(e))

    def test_an_epoch_is_refused_for_the_same_reason(self):
        e = self.assertRaises(self.build.UnrepresentableVersion,
                              self.build.semver_from, '1!0.1.0')
        self.assertIn('epoch', str(e))
