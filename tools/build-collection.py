#!/usr/bin/env python3
# Copyright 2019 Michael Still and contributors
#
# Build the shakenfist.k3s ansible-galaxy collection.
#
# Derives the collection's version from setuptools_scm (the same source as
# the shakenfist_client_k3s wheel's version, so the collection and the
# plugin never drift), rewrites it into the collection's galaxy.yml as a
# valid semver string, and builds the collection tarball into
# dist-collection/.
#
# Used by the build-collection job in .github/workflows/release.yml. Run
# from the repository root.
import pathlib
import re
import subprocess
import sys

from packaging.version import Version


COLLECTION_DIR = pathlib.Path('collection')
OUTPUT_DIR = pathlib.Path('dist-collection')


class UnrepresentableVersion(Exception):
    """A PEP 440 version which cannot be expressed as semver."""


def semver_from(raw):
    """Return the semver spelling of a PEP 440 version string.

    ansible-galaxy validates galaxy.yml's version with the semantic_version
    library, which is stricter than PEP 440: a prerelease must be separated
    from the release with '-' (so 0.8.0rc5 -> 0.8.0-rc5), and the dev/local
    segments become dot-separated prerelease identifiers and '+' build
    metadata respectively. Decompose with packaging and reassemble as
    semver.

    A separate function from collection_version() so that it is testable
    without a git checkout: the conversion is the part with the edge cases,
    and asking setuptools_scm for a version is the part that needs a
    repository. Splitting them is what let the post-release guard below be
    covered -- it was added during the review of #90 and survived its own
    mutation, because nothing could reach it.
    """
    v = Version(raw)

    # Refused rather than silently flattened. The decomposition below reads
    # major, minor, micro, pre, dev and local, so a version carrying a
    # post-release or an epoch -- 0.1.0.post1, or 1!0.1.0 -- would come out
    # as plain 0.1.0 and collide with a version already on Galaxy, which
    # does not allow a replacement. setuptools_scm's default scheme produces
    # neither, so this cannot happen by accident; a hand-written tag is what
    # it guards against, and failing at build time is much cheaper than
    # discovering it at publish time.
    if v.post is not None or v.epoch:
        raise UnrepresentableVersion(
            '%s carries a post-release or epoch segment, which cannot be '
            'expressed as semver without colliding with %d.%d.%d. Tag a '
            'plain release instead.' % (raw, v.major, v.minor, v.micro))

    core = '%d.%d.%d' % (v.major, v.minor, v.micro)
    prerelease = []
    if v.pre is not None:
        prerelease.append('%s%d' % (v.pre[0], v.pre[1]))
    if v.dev is not None:
        prerelease.append('dev%d' % v.dev)

    semver = core
    if prerelease:
        semver += '-' + '.'.join(prerelease)
    if v.local:
        semver += '+' + v.local

    return semver


def collection_version():
    """Return (pep440, semver) for the current checkout."""
    raw = subprocess.check_output(
        [sys.executable, '-m', 'setuptools_scm'], text=True).strip()
    try:
        return raw, semver_from(raw)
    except UnrepresentableVersion as e:
        sys.exit('build-collection: %s' % e)


def main():
    raw, semver = collection_version()
    print('shakenfist_client_k3s version %s -> collection version %s' % (raw, semver))

    # Use the ansible-galaxy that belongs to the interpreter running us (the
    # build venv), not whatever bare name happens to be on PATH -- running
    # venv/bin/python3 directly does not put the venv on PATH.
    galaxy_bin = pathlib.Path(sys.executable).parent / 'ansible-galaxy'
    if not galaxy_bin.exists():
        galaxy_bin = pathlib.Path('ansible-galaxy')

    # galaxy.yml is tracked, and the version in it is a placeholder which
    # this script rewrites so the built tarball carries the real one. The
    # rewrite is reverted in the finally below, which matters for a local
    # run rather than for CI: without it the tree is left dirty with a
    # version nobody chose, where a git add -A commits it by accident --
    # the way this branch already committed ansible-lint's working tree
    # twice. A dirty tree also moves setuptools_scm's next computed
    # version, so the next build would disagree with this one for a reason
    # that is nowhere on screen.
    #
    # The revert restores the bytes that were there, rather than writing
    # "0.0.0" back: the placeholder is whatever the file says, and this
    # script has no business deciding what it should be.
    galaxy = COLLECTION_DIR / 'galaxy.yml'
    original = galaxy.read_text(encoding='utf-8')
    try:
        galaxy.write_text(re.sub(
            r'(?m)^version:.*$', 'version: %s' % semver, original),
            encoding='utf-8')

        OUTPUT_DIR.mkdir(exist_ok=True)
        subprocess.check_call([
            str(galaxy_bin), 'collection', 'build', str(COLLECTION_DIR),
            '--output-path', str(OUTPUT_DIR), '--force'])
    finally:
        galaxy.write_text(original, encoding='utf-8')


if __name__ == '__main__':
    main()
