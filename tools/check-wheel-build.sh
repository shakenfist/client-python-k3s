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

# The sdist is checked separately, and for a different thing. check-dist.sh
# asserts what the *wheel* ships, and the wheel is narrow: packages.find is
# anchored to shakenfist_client_k3s*, so a stray top level directory cannot
# reach it. The sdist is the opposite -- setuptools_scm's file finder puts
# every tracked file in it -- so it is where an accidentally committed
# build or lint artefact shows up, and nothing was looking. One did:
# ansible-lint's .ansible/ working tree was committed by a "git add -A"
# whose tree predated the .gitignore entry for it, and rode into the sdist
# as eighteen paths while the wheel stayed at twelve entries and the gate
# stayed green.
#
# Checking for tracked artefacts rather than for .ansible by name, because
# the next one will have a different name. These are the directories a
# build or a lint leaves behind which .gitignore is expected to cover.
echo "check-wheel-build: checking the sdist for committed build artefacts"
sdist=$(echo "${WORKDIR}"/dist/*.tar.gz)
artefacts=$(tar tzf "${sdist}" \
    | grep -E '(^|/)(\.ansible|__pycache__|\.tox|\.eggs|build|dist-collection)/' \
    || true)
if [ -n "${artefacts}" ]; then
    echo "check-wheel-build: the sdist carries build or lint artefacts:"
    while IFS= read -r line; do
        echo "  ${line}"
    done <<< "${artefacts}"
    echo "check-wheel-build: these are tracked in git and should not be."
    echo "check-wheel-build: check .gitignore, then git rm --cached them."
    exit 1
fi
echo "check-wheel-build: $(basename "${sdist}") OK (no build artefacts)"

# And the same check for the case the one above cannot see. The artefact
# grep asks whether a known kind of junk directory is present, which only
# works for junk somebody has already met -- its own comment says the next
# one will have a different name, and the next one did. The push audit
# committed ten per-merge diffs under docs/plans/audit/diffs, deliberately
# and for a good reason, and they rode into the sdist as 1.2MB across
# eleven files. No name matched, the wheel stayed at twelve entries, and
# the gate stayed green. MANIFEST.in prunes them now.
#
# The bound is on the whole rather than per file. Of the eleven diffs
# only three were bigger than test_cluster.py, so a per-file cap would
# have had to sit just above the largest legitimate source file to catch
# them, and would still have let the other eight through. Total size and
# entry count are what actually moved: 136 entries and 2.9MB, against
# 126 and 1.7MB once they were pruned. These two are what the sdist
# measures today plus room to grow, not a target -- a legitimate increase is normal and raising them is the
# right response. What is not normal is a jump, which is what this asks
# about.
#
# If this trips, read the listing it prints before changing the numbers.
# The question is always whether the biggest new entries belong in a
# source distribution, not whether the number is too small.
#
# Raised from 2200000 bytes by cumulative health signals phase 2, at 140
# entries and 2355987 bytes: the growth was test_cluster.py, cluster.py
# and the phase plans, all of which are source, spread across the phases
# since the bound was set rather than arriving as a jump.
MAX_SDIST_ENTRIES=160
MAX_SDIST_BYTES=2800000

sdist_entries=$(tar tzf "${sdist}" | wc -l)
sdist_bytes=$(tar tzvf "${sdist}" | awk '{s+=$3} END {print s+0}')
echo "check-wheel-build: sdist has ${sdist_entries} entries," \
    "${sdist_bytes} bytes uncompressed"

# A zero here is not a small sdist, it is a measurement that did not
# happen -- the byte count is the third field of tar's verbose listing,
# and a tar whose output is shaped differently would sum to nothing and
# sail under both bounds without a word. Checked explicitly, because a
# guard which passes when it cannot measure is worse than no guard.
if [ "${sdist_entries}" -lt 1 ] || [ "${sdist_bytes}" -lt 1 ]; then
    echo "check-wheel-build: could not measure the sdist" \
        "(${sdist_entries} entries, ${sdist_bytes} bytes)."
    echo "check-wheel-build: tar's listing is not the shape this expects."
    exit 1
fi

if [ "${sdist_entries}" -gt "${MAX_SDIST_ENTRIES}" ] \
        || [ "${sdist_bytes}" -gt "${MAX_SDIST_BYTES}" ]; then
    echo "check-wheel-build: the sdist has grown past its bounds" \
        "(${MAX_SDIST_ENTRIES} entries, ${MAX_SDIST_BYTES} bytes)."
    echo "check-wheel-build: the largest entries are:"
    tar tzvf "${sdist}" | sort -k3 -n -r | head -15 \
        | awk '{printf "  %10d  %s\n", $3, $NF}'
    echo "check-wheel-build: if they belong in a source release, raise the"
    echo "check-wheel-build: bounds in this script. If they do not, prune"
    echo "check-wheel-build: them in MANIFEST.in."
    exit 1
fi
echo "check-wheel-build: sdist size OK"
