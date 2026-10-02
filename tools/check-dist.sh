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
# Usage: tools/check-dist.sh <path-to-wheel> [<path-to-wheel> ...]

set -e
set -o pipefail

# The twelve entries a correct wheel has today: six modules under
# shakenfist_client_k3s/ (__init__, client, cluster, exceptions,
# primitives, progress), and six under the .dist-info directory --
# METADATA, WHEEL, entry_points.txt, top_level.txt, RECORD, and
# licenses/LICENSE. That last one is there because pyproject.toml sets
# license-files, and it is the entry most likely to surprise someone
# recounting by hand.
#
# If this trips because the wheel legitimately grew, check what the new
# entries are before raising the number: the failure this script exists
# to catch is data sneaking back in silently, not the count itself.
MAX_ENTRIES=12

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <path-to-wheel> [<path-to-wheel> ...]"
    exit 1
fi

# Print each line of the argument indented by two spaces. A read loop
# rather than sed so a path containing a space stays one line, and to
# avoid the SC2001 style warning a sed substitution earns here.
indent() {
    while IFS= read -r line; do
        echo "  ${line}"
    done <<< "$1"
}

check_wheel() {
    local wheel="$1"
    local entries test_entries count

    if [ ! -f "${wheel}" ]; then
        echo "No such file: ${wheel}"
        exit 1
    fi

    # Ask zipfile for the names rather than parsing the column layout of
    # "python3 -m zipfile -l", which is a human-readable listing and not
    # a stable interface. Taking the first whitespace-delimited field of
    # that listing would also truncate any path containing a space,
    # which is how an unwanted entry could slip past the grep below.
    entries=$(python3 -c 'import sys, zipfile
print("\n".join(zipfile.ZipFile(sys.argv[1]).namelist()))' "${wheel}")

    # (^|/)tests/ rather than /tests/, so a tests/ directory at the
    # archive root is caught as well. The packages.find configuration
    # makes that shape unlikely, but this script is the backstop for the
    # configuration being wrong, so it cannot assume the configuration is
    # right.
    test_entries=$(echo "${entries}" | grep -E '(^|/)tests/' || true)
    if [ -n "${test_entries}" ]; then
        echo "check-dist: ${wheel} contains test paths that must not ship:"
        indent "${test_entries}"
        echo "check-dist: check [tool.setuptools] include-package-data in" \
            "pyproject.toml -- it should be false"
        exit 1
    fi

    # grep -c exits 1 when it counts nothing, so the || true is needed to
    # survive set -e -- which means an empty archive would otherwise reach
    # the success message and report "OK (0 entries, no /tests/ paths)".
    # Nothing unwanted is in an empty wheel, but it is not a wheel either,
    # and a check which cannot tell those apart is worse than no check.
    count=$(echo "${entries}" | grep -c . || true)
    if [ "${count}" -eq 0 ]; then
        echo "check-dist: ${wheel} has no entries at all -- not a wheel"
        exit 1
    fi

    if [ "${count}" -gt "${MAX_ENTRIES}" ]; then
        echo "check-dist: ${wheel} has ${count} entries, expected at most" \
            "${MAX_ENTRIES}:"
        indent "${entries}"
        echo "check-dist: check whether the new entries belong in the" \
            "published package before raising MAX_ENTRIES"
        exit 1
    fi

    echo "check-dist: ${wheel} OK (${count} entries, no /tests/ paths)"
}

# Every wheel, not just the first. release.yml passes dist/*.whl, so a
# future change to the build layout which produces two wheels would
# otherwise publish the second one unchecked.
for wheel in "$@"; do
    check_wheel "${wheel}"
done
