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
# this must not be shallow.
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

echo "check-wheel-build: building a wheel from ${ROOT}"
"${WORKDIR}/venv/bin/python" -m build --wheel \
    --outdir "${WORKDIR}/dist" "${ROOT}"

"${HERE}/check-dist.sh" "${WORKDIR}"/dist/*.whl
