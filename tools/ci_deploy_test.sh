#!/bin/bash

# Deploy a real k3s cluster into this CI runner's own Shaken Fist
# namespace and verify it works. Run by the merge tier of
# .github/workflows/functional-tests.yml on an ephemeral VM runner: the
# runner carries under-cloud credentials in ~/.shakenfist which name its
# per-job namespace, the plugin defaults to that namespace, and the
# namespace (and so anything this script leaks on failure) is torn down
# with the runner. See docs/plans/functional-ci.md for the design.

set -e
set -o pipefail

# Deliberately mixed case. Shaken Fist accepts a capital letter in an
# instance name and Kubernetes does not accept one in a node name, so on a
# cluster named this way the k3s node name and the instance name differ.
# remove-worker is the only verb which addresses a node by name, and this
# is the only tier which can tell whether the name it computes is the one
# k3s actually registered.
CLUSTER=ciMixed
# The second, deliberately minimal cluster: one control plane node and
# none of the optional components. It exists because --no-metallb,
# --no-longhorn and --no-kubeconfig change what create() does on a real
# cluster and nowhere else can tell whether skipping those steps leaves a
# working cluster behind. It is cheaper than the cluster above rather
# than more expensive: no worker nodes, and neither component install.
MINIMAL_CLUSTER=ciMinimal
# This tracks the cluster's k3s channel only loosely, which is fine for
# the simple kubectl operations used here.
# renovate: datasource=github-releases depName=kubernetes/kubernetes
KUBECTL_VERSION=v1.37.0
VENV=/tmp/venv-k3s-ci

status() {
    echo
    echo "=== $1 ==="
}

dump_state() {
    # Best effort diagnostics for the job log; the namespace dies with
    # the runner, so no cleanup is attempted. Cluster metadata (k3s
    # show) is deliberately not dumped: it contains the node token and
    # the cluster admin kubeconfig, and job logs are visible to anyone
    # who can see the repository.
    status 'Failure diagnostics'
    sf-client instance list || true
    kubectl get nodes -o wide || true
    kubectl get pods -A || true
}
on_exit() {
    rc=$?
    if [ "${rc}" -ne 0 ]; then
        dump_state
    fi
}
# An EXIT trap rather than ERR: ERR traps do not fire for explicit
# non-zero exits, and (without errtrace) not for failures inside shell
# functions, which is exactly where the assertions below fail.
trap on_exit EXIT

wait_for_nodes() {
    # kubectl wait --for=condition=Ready --all only considers nodes that
    # have already registered with the API server, and immediately after
    # a create or expand the newest node may not have yet. Poll for the
    # expected count first, then wait for readiness.
    expected=$1
    for _ in $(seq 30); do
        if [ "$(kubectl get nodes --no-headers | wc -l)" -ge "${expected}" ]; then
            break
        fi
        sleep 10
    done
    kubectl wait --for=condition=Ready nodes --all --timeout=300s
    node_count=$(kubectl get nodes --no-headers | wc -l)
    if [ "${node_count}" -ne "${expected}" ]; then
        echo "Expected ${expected} nodes, found ${node_count}"
        exit 1
    fi
}

worker_uuids() {
    # k3s show prints: worker_nodes = ['uuid-one', 'uuid-two']. Captured
    # first for the reason count_routed_addresses() gives below.
    local show_output
    show_output=$(sf-client k3s show "${CLUSTER}")
    echo "${show_output}" | grep 'worker_nodes' | grep -o "'[^']*'" | tr -d "'"
}

count_routed_addresses() {
    # k3s show prints: routed_addresses = ['a.b.c.d', 'e.f.g.h']. The
    # show output is captured first so a failure of sf-client itself
    # aborts the script rather than being masked as a zero count; the
    # || true only covers grep finding no matches.
    local show_output
    show_output=$(sf-client k3s show "${CLUSTER}")
    echo "${show_output}" | grep 'routed_addresses' | grep -o "'[0-9.]*'" | wc -l || true
}

status 'Install sf-client and the plugin under test'
python3 -mvenv "${VENV}"
# shellcheck disable=SC1091
. "${VENV}/bin/activate"
pip install uv
uv pip install shakenfist-client .

status 'Install kubectl'
# The checksum comes from the same host as the binary, so this detects
# corruption and truncation rather than a compromised dl.k8s.io; that is
# the upstream documented install method.
tmpdir=$(mktemp -d)
curl -sfL -o "${tmpdir}/kubectl" \
    "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
curl -sfL -o "${tmpdir}/kubectl.sha256" \
    "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256"
(cd "${tmpdir}" && echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check)
sudo install -m 0755 "${tmpdir}/kubectl" /usr/local/bin/kubectl
rm -rf "${tmpdir}"

status 'Create the cluster'
# A manifest staged at create time, which k3s applies itself the first
# time the server starts. This is the only tier which can answer whether
# the quoted heredoc survives the agent's command transport to land a
# file k3s will parse -- the unit tests run the same command through
# /bin/sh, which pins the quoting but not the transport.
manifest_dir=$(mktemp -d)
cat - > "${manifest_dir}/ci-staged.yaml" <<'MANIFEST'
apiVersion: v1
kind: ConfigMap
metadata:
  name: ci-staged
  namespace: default
data:
  # Metacharacters on purpose: these are what an unquoted heredoc would
  # have expanded on the node before the file was written.
  hazards: "$HOME `id` $(whoami) \"double\""
MANIFEST

sf-client k3s create "${CLUSTER}" \
    --control-plane-count 1 --worker-count 2 --metal-address-count 2 \
    --manifest "${manifest_dir}/ci-staged.yaml"

# The manifest is on the cluster now, so the local copy has done its job.
# Cleaned up here the way the kubectl download's temp directory is, rather
# than left for the ephemeral runner to take with it.
rm -rf "${manifest_dir}"

status 'Fetch cluster credentials with getconfig'
export KUBECONFIG=/tmp/k3s-ci-kubeconfig
sf-client k3s getconfig "${CLUSTER}" > "${KUBECONFIG}"

status 'Verify all nodes become ready'
wait_for_nodes 3

status 'Verify the staged manifest was applied, byte for byte'
staged=$(kubectl get configmap ci-staged -o jsonpath='{.data.hazards}')
# Single quoted, so this side of the comparison is the literal text as
# well: the value above is a YAML double quoted scalar, so the only
# escape in it is the \" pair, and everything else is what k3s parsed.
# shellcheck disable=SC2016
# Nothing here is meant to expand: this is the literal text the manifest
# carried, and the whole point is that nothing on the node expanded it
# either.
expected='$HOME `id` $(whoami) "double"'
if [ "${staged}" != "${expected}" ]; then
    echo "The staged ConfigMap says ${staged}"
    echo "It should say ${expected}"
    exit 1
fi

status 'Verify health reports a healthy cluster'
# --strict is what makes this an assertion rather than a print: without
# it health exits 0 whatever it found.
sf-client k3s health "${CLUSTER}" --strict

status 'Verify a LoadBalancer service gets an address and answers'
# registry.k8s.io rather than Docker Hub: the under-cloud's shared
# egress address makes anonymous Docker Hub pull rate limits a flake
# source, and the tag is immutable.
kubectl create deployment ci-web \
    --image=registry.k8s.io/e2e-test-images/nginx:1.15-alpine --replicas=2
kubectl rollout status deployment ci-web --timeout=300s
kubectl expose deployment ci-web --port=80 --type=LoadBalancer
lb_address=''
for _ in $(seq 30); do
    lb_address=$(kubectl get service ci-web \
        -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    if [ -n "${lb_address}" ]; then
        break
    fi
    sleep 10
done
if [ -z "${lb_address}" ]; then
    echo 'The LoadBalancer service was never assigned an address'
    exit 1
fi
echo "LoadBalancer address is ${lb_address}"
# The runner VM is on a different virtual network to the cluster, so
# this also asserts MetalLB addresses are reachable from outside the
# cluster's own node network.
curl -sf --retry 10 --retry-delay 10 --retry-all-errors --max-time 10 \
    "http://${lb_address}/" > /dev/null

status 'Expand the cluster with an extra worker'
before_workers=$(worker_uuids | sort)
sf-client k3s expand-workers "${CLUSTER}" --worker-count 1
wait_for_nodes 4
after_workers=$(worker_uuids | sort)

status 'Remove the worker which was just added'
new_worker=$(comm -13 <(echo "${before_workers}") <(echo "${after_workers}"))
if [ "$(echo "${new_worker}" | wc -l)" -ne 1 ] || [ -z "${new_worker}" ]; then
    echo "Expected exactly one new worker, found: ${new_worker}"
    exit 1
fi
echo "Removing worker ${new_worker}"
sf-client k3s remove-worker "${CLUSTER}" --worker "${new_worker}"

# The node object has to be gone, not merely NotReady: remove-worker
# drains and then deletes it, and a NotReady node left behind is the
# failure the drain-first ordering exists to prevent.
wait_for_nodes 3
if sf-client k3s show "${CLUSTER}" | grep -q "${new_worker}"; then
    echo "Worker ${new_worker} is still in the cluster metadata"
    exit 1
fi

status 'Expand the MetalLB address pool'
before=$(count_routed_addresses)
sf-client k3s expand-addresses "${CLUSTER}" --address-count 1
after=$(count_routed_addresses)
if [ "${after}" -ne $((before + 1)) ]; then
    echo "Expected $((before + 1)) routed addresses, found ${after}"
    exit 1
fi

status 'Delete the cluster'
sf-client k3s delete "${CLUSTER}"
if sf-client k3s list | grep -q "^${CLUSTER}$"; then
    echo 'The cluster is still listed after deletion'
    exit 1
fi
# --all includes error state instances, which the default listing hides:
# a node the delete failed to remove must not pass this check just
# because it fell into the error state.
remaining=$(sf-client instance list --all | grep "k3s-${CLUSTER}-node" | grep -cv 'deleted' || true)
if [ "${remaining}" -ne 0 ]; then
    echo "Found ${remaining} instances still present after deletion"
    exit 1
fi

status 'Create a cluster with none of the optional components'
# HOME is left alone on purpose: --no-kubeconfig has to be the thing that
# leaves ~/.kube/config alone, not the absence of a home directory.
sf-client k3s create "${MINIMAL_CLUSTER}" \
    --control-plane-count 1 --worker-count 0 --metal-address-count 0 \
    --no-metallb --no-longhorn --no-kubeconfig

status 'Verify --no-kubeconfig wrote no local kubeconfig'
# The main cluster above used getconfig and an explicit KUBECONFIG, so
# nothing in this script has written the default path. If it exists, the
# create did it.
if [ -e "${HOME}/.kube/config" ]; then
    echo "create --no-kubeconfig wrote ${HOME}/.kube/config anyway"
    exit 1
fi

status 'Verify the minimal cluster is healthy and answers kubectl'
# The credentials are in the cluster metadata either way, which is the
# point of the flag: --no-kubeconfig declines to touch the local file, it
# does not decline to build a usable cluster.
sf-client k3s health "${MINIMAL_CLUSTER}" --strict
KUBECONFIG=/tmp/k3s-ci-kubeconfig-minimal
sf-client k3s getconfig "${MINIMAL_CLUSTER}" > "${KUBECONFIG}"
export KUBECONFIG
wait_for_nodes 1

status 'Verify expand-addresses refuses a cluster built without metallb'
# Routing more addresses into a cluster with nothing to hand them out is
# the refusal ComponentNotInstalledError exists for, and this is the only
# tier where the cluster really was built without metallb.
if sf-client k3s expand-addresses "${MINIMAL_CLUSTER}" --address-count 1 \
        > /tmp/k3s-ci-expand-refusal 2>&1; then
    echo 'expand-addresses succeeded on a cluster built without metallb'
    cat /tmp/k3s-ci-expand-refusal
    exit 1
fi
if ! grep -qi 'metallb' /tmp/k3s-ci-expand-refusal; then
    echo 'expand-addresses refused without saying metallb is the reason'
    cat /tmp/k3s-ci-expand-refusal
    exit 1
fi

status 'Delete the minimal cluster'
sf-client k3s delete "${MINIMAL_CLUSTER}" --no-kubeconfig
if sf-client k3s list | grep -q "^${MINIMAL_CLUSTER}$"; then
    echo 'The minimal cluster is still listed after deletion'
    exit 1
fi

status 'Success'
