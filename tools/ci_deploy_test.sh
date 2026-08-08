#!/bin/bash

# Deploy a real k3s cluster into this CI runner's own Shaken Fist
# namespace and verify it works. Run by the merge tier of
# .github/workflows/functional-tests.yml on an ephemeral VM runner: the
# runner carries under-cloud credentials in ~/.shakenfist, and its
# per-job namespace (and so anything this script leaks on failure) is
# torn down with the runner. See docs/plans/functional-ci.md for the
# design.

set -e
set -o pipefail

CLUSTER=ci
# Pinned; this tracks the cluster's k3s channel only loosely, which is
# fine for the simple kubectl operations used here.
KUBECTL_VERSION=v1.36.3
VENV=/tmp/venv-k3s-ci

# The ephemeral runner's per-job namespace is named after the runner.
export SHAKENFIST_NAMESPACE=${SHAKENFIST_NAMESPACE:-$(hostname)}

status() {
    echo
    echo "=== $1 ==="
}

dump_state() {
    # Best effort diagnostics for the job log; the namespace dies with
    # the runner, so no cleanup is attempted.
    status 'Failure diagnostics'
    sf-client k3s show "${CLUSTER}" || true
    sf-client instance list || true
    kubectl get nodes -o wide || true
    kubectl get pods -A || true
}
trap dump_state ERR

status 'Install sf-client and the plugin under test'
python3 -mvenv "${VENV}"
# shellcheck disable=SC1091
. "${VENV}/bin/activate"
pip install uv
uv pip install shakenfist-client .

status 'Install kubectl'
curl -sfLO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
curl -sfLO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256"
echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
sudo install -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl kubectl.sha256

status 'Create the cluster'
sf-client k3s create "${CLUSTER}" \
    --control-plane-count 1 --worker-count 2 --metal-address-count 2

status 'Fetch cluster credentials with getconfig'
export KUBECONFIG=/tmp/k3s-ci-kubeconfig
sf-client k3s getconfig "${CLUSTER}" > "${KUBECONFIG}"

status 'Verify all nodes become ready'
kubectl wait --for=condition=Ready nodes --all --timeout=300s
node_count=$(kubectl get nodes --no-headers | wc -l)
if [ "${node_count}" -ne 3 ]; then
    echo "Expected 3 nodes, found ${node_count}"
    exit 1
fi

status 'Verify a LoadBalancer service gets an address and answers'
kubectl create deployment ci-web --image=nginx --replicas=2
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
kubectl wait --for=condition=Ready nodes --all --timeout=300s
node_count=$(kubectl get nodes --no-headers | wc -l)
if [ "${node_count}" -ne 4 ]; then
    echo "Expected 4 nodes after expand-workers, found ${node_count}"
    exit 1
fi

status 'Expand the MetalLB address pool'
count_routed_addresses() {
    # k3s show prints: routed_addresses = ['a.b.c.d', 'e.f.g.h']
    sf-client k3s show "${CLUSTER}" | grep 'routed_addresses' \
        | grep -o "'[0-9.]*'" | wc -l || true
}
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
remaining=$(sf-client instance list | grep -c "k3s-${CLUSTER}-node" || true)
if [ "${remaining}" -ne 0 ]; then
    echo "Found ${remaining} instances still present after deletion"
    exit 1
fi

status 'Success'
