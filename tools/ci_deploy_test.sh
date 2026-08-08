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

CLUSTER=ci
# This tracks the cluster's k3s channel only loosely, which is fine for
# the simple kubectl operations used here.
# renovate: datasource=github-releases depName=kubernetes/kubernetes
KUBECTL_VERSION=v1.36.3
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

count_routed_addresses() {
    # k3s show prints: routed_addresses = ['a.b.c.d', 'e.f.g.h']. The
    # show output is captured first so a failure of sf-client itself
    # aborts the script rather than being masked as a zero count; the
    # || true only covers grep finding no matches.
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
sf-client k3s create "${CLUSTER}" \
    --control-plane-count 1 --worker-count 2 --metal-address-count 2

status 'Fetch cluster credentials with getconfig'
export KUBECONFIG=/tmp/k3s-ci-kubeconfig
sf-client k3s getconfig "${CLUSTER}" > "${KUBECONFIG}"

status 'Verify all nodes become ready'
wait_for_nodes 3

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
sf-client k3s expand-workers "${CLUSTER}" --worker-count 1
wait_for_nodes 4

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

status 'Success'
