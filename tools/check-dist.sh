#!/bin/bash
#
# Check that a built wheel is what we intend to publish, not what
# setuptools happened to produce. This exists because
# include_package_data defaults to true under pyproject.toml, and
# setuptools_scm's file finder offers every git-tracked file as
# package data -- the packages.find exclude for
# shakenfist_client_k3s.tests* drops the tests *package* correctly,
# but the tests came back as data anyway, and nothing failed to say
# so. See docs/plans/library-api-and-collection-phase-04-first-release.md.
#
# Usage: tools/check-dist.sh <path-to-wheel>

set -e
set -o pipefail

# 6 dist-info entries plus the 6 modules under shakenfist_client_k3s/.
# If this trips because the wheel legitimately grew, check what the
# new entries are before raising the number: the failure this script
# exists to catch is data sneaking back in silently, not the count
# itself.
MAX_ENTRIES=12

WHEEL="$1"

if [ -z "${WHEEL}" ]; then
    echo "Usage: $0 <path-to-wheel>"
    exit 1
fi

if [ ! -f "${WHEEL}" ]; then
    echo "No such file: ${WHEEL}"
    exit 1
fi

entries=$(python3 -m zipfile -l "${WHEEL}" | tail -n +2 | awk '{print $1}')

test_entries=$(echo "${entries}" | grep '/tests/' || true)
if [ -n "${test_entries}" ]; then
    echo "check-dist: ${WHEEL} contains test paths that must not ship:"
    echo "${test_entries}" | sed 's/^/  /'
    echo "check-dist: check [tool.setuptools] include-package-data in" \
        "pyproject.toml -- it should be false"
    exit 1
fi

count=$(echo "${entries}" | grep -c .)
if [ "${count}" -gt "${MAX_ENTRIES}" ]; then
    echo "check-dist: ${WHEEL} has ${count} entries, expected at most" \
        "${MAX_ENTRIES}:"
    echo "${entries}" | sed 's/^/  /'
    echo "check-dist: check whether the new entries belong in the" \
        "published package before raising MAX_ENTRIES"
    exit 1
fi

echo "check-dist: ${WHEEL} OK (${count} entries, no /tests/ paths)"
