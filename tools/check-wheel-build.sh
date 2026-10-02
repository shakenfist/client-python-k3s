#!/bin/bash
#
# Build a wheel from the current tree and check it with
# tools/check-dist.sh.
#
# release.yml runs that check on the wheel it is about to upload, which
# is too late to be the only place it runs: a packaging regression --
# include-package-data turned back on, a data file added, the exclude
# "fixed" -- would then be found after a tag had been pushed and
# sign-tag had already force-pushed it. This builds the real wheel, so
# it catches what the unit tests cannot: those drive check-dist.sh
# against fabricated archives and so say nothing about what setuptools
# actually produces here.
#
# Needs the git history setuptools_scm reads, so a CI checkout wanting
# this must not be shallow. It also needs the network, to install build
# and then, because the build is isolated, the build requirements
# themselves.
#
# Usage: tools/check-wheel-build.sh

set -e
set -o pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(dirname "${HERE}")

WORKDIR=$(mktemp -d)
trap 'rm -rf "${WORKDIR}"' EXIT

python3 -m venv "${WORKDIR}/venv"
"${WORKDIR}/venv/bin/pip" install --quiet --upgrade pip
"${WORKDIR}/venv/bin/pip" install --quiet build

# setuptools reuses ${ROOT}/build/lib and *.egg-info when they are already
# there, so a tree which has been built before -- or has had "uv pip
# install ." run in it, which the CI step before this one does -- can pack
# leftovers from that earlier build into the wheel. The answer would then
# depend on what happened in this directory earlier, which is the opposite
# of what a gate is for. Both are build output, both are gitignored.
echo "check-wheel-build: clearing previous build output in ${ROOT}"
rm -rf "${ROOT}/build" "${ROOT}"/*.egg-info

# Plain "build" rather than "build --wheel", because that is what
# release.yml runs: it builds the sdist first and then builds the wheel
# from that sdist, not from the git tree. The two paths can disagree --
# setuptools_scm's file finder only sees a git checkout, so a wheel built
# via the sdist is assembled from whatever the sdist captured instead.
# Checking the wheel a release would actually upload means building it the
# way a release builds it.
echo "check-wheel-build: building from ${ROOT} the way release.yml does"
"${WORKDIR}/venv/bin/python" -m build --outdir "${WORKDIR}/dist" "${ROOT}"

"${HERE}/check-dist.sh" "${WORKDIR}"/dist/*.whl
