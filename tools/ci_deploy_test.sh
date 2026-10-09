#!/bin/bash

# Deploy a real k3s cluster into this CI runner's own Shaken Fist
# namespace and verify it works. Run by the merge tier of
# .github/workflows/functional-tests.yml on an ephemeral VM runner: the
# runner carries under-cloud credentials in ~/.shakenfist which name its
# per-job namespace, the plugin defaults to that namespace, and the
# namespace (and so anything this script leaks on failure) is torn down
# with the runner. See docs/plans/PLAN-functional-ci.md for the design.

set -e
set -o pipefail

# Deliberately mixed case. Shaken Fist accepts a capital letter in an
# instance name and Kubernetes does not accept one in a node name, so on a
# cluster named this way the k3s node name and the instance name differ.
# remove-worker is the only verb which addresses a node by name, and this
# is the only tier which can tell whether the name it computes is the one
# k3s actually registered.
CLUSTER=ciMixed
# The second, deliberately minimal cluster: one control plane node, one
# worker and none of the optional components. It exists because
# --no-metallb, --no-longhorn and --no-kubeconfig change what create() does
# on a real cluster and nowhere else can tell whether skipping those steps
# leaves a working cluster behind. It is cheaper than the cluster above
# rather than more expensive: one worker, and neither component install.
#
# The worker is there for the node-taint: [] opt-out this cluster carries.
# A cluster with no workers is never tainted, so on one the opt-out
# assertion would pass with the opt-out broken; only a cluster which would
# otherwise be tainted can show the caller's empty list replacing the
# default. It also disables and labels nothing, which makes it the positive
# control for the main cluster's absence checks: Traefik and its svclb pods
# have to appear here, or those checks are looking for the wrong names.
#
# It is also built on a network this script makes and hands it with
# --network, because only a real delete can show that a borrowed network
# outlives the cluster which borrowed it (shakenfist/client-python-k3s#41).
MINIMAL_CLUSTER=ciMinimal
MINIMAL_NETWORK=ciMinimalNet
# Node sizes for the main cluster, shared by its create and the assertions
# which read them back. Every value differs from the 2 / 2048 / 50 default
# and from the other role's, so a dropped flag or a swapped role shows up
# as a wrong number rather than passing. The minimal cluster keeps the
# defaults.
CONTROL_PLANE_CPUS=4
CONTROL_PLANE_MEMORY=4096
CONTROL_PLANE_DISK=30
WORKER_CPUS=3
WORKER_MEMORY=3072
WORKER_DISK=40
# This tracks the cluster's k3s channel only loosely, which is fine for
# the simple kubectl operations used here.
# renovate: datasource=github-releases depName=kubernetes/kubernetes
KUBECTL_VERSION=v1.37.1
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
    # Labels and taints are what several of the node customisation
    # assertions fail on, and the namespace is gone by the time anyone
    # reads this log. Neither carries a secret.
    kubectl get nodes --show-labels || true
    kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints || true
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
    show_output=$(sf-client k3s show "${CLUSTER}") || return 1
    echo "${show_output}" | grep 'worker_nodes' | grep -o "'[^']*'" | tr -d "'"
}

control_plane_uuids() {
    # k3s show prints: control_plane_nodes = ['uuid-one']. Captured first
    # for the reason count_routed_addresses() gives below, and never
    # echoed whole for the reason dump_state() gives.
    local show_output
    show_output=$(sf-client k3s show "${CLUSTER}") || return 1
    echo "${show_output}" | grep '^    control_plane_nodes = ' | grep -o "'[^']*'" | tr -d "'"
}

count_routed_addresses() {
    # k3s show prints: routed_addresses = ['a.b.c.d', 'e.f.g.h']. The
    # show output is captured first so a failure of sf-client itself
    # aborts the script rather than being masked as a zero count; the
    # || true only covers grep finding no matches.
    #
    # The explicit || return 1 is what makes that true. These helpers are
    # called inside $(...), and bash clears set -e in a command
    # substitution, so without it a failed sf-client would carry on to
    # the pipeline below and the function would succeed with no output.
    local show_output
    show_output=$(sf-client k3s show "${CLUSTER}") || return 1
    echo "${show_output}" | grep 'routed_addresses' | grep -o "'[0-9.]*'" | wc -l || true
}

pod_names() {
    # NAMESPACE/NAME for every pod in the cluster. Captured first so a
    # kubectl failure is a failure, not an empty list which every absence
    # check below would read as a pass.
    local pods
    pods=$(kubectl get pods -A --no-headers) || return 1
    echo "${pods}" | awk '{print $1 "/" $2}'
}

cluster_namespace() {
    # The Shaken Fist namespace cluster $1 lives in. k3s show prints:
    # namespace = ns, and is captured first for the reason
    # count_routed_addresses() gives above.
    local show_output namespace
    show_output=$(sf-client k3s show "$1") || return 1
    namespace=$(echo "${show_output}" | sed -n 's/^    namespace = //p')
    [ -n "${namespace}" ] || return 1
    echo "${namespace}"
}

kubeconfig_fqcn() {
    # The name create gives the cluster's kubeconfig user, context and
    # cluster, <name>.<namespace>.
    local namespace
    namespace=$(cluster_namespace "$1") || return 1
    echo "$1.${namespace}"
}

assert_refused() {
    # Run the command after $1 and require it to fail with output
    # containing the fixed string $1.
    local expected=$1 output=/tmp/k3s-ci-refusal
    shift
    if "$@" > "${output}" 2>&1; then
        echo "$* succeeded; it should have been refused"
        cat "${output}"
        exit 1
    fi
    if ! grep -qF "${expected}" "${output}"; then
        echo "$* failed, but its output does not mention ${expected}:"
        cat "${output}"
        exit 1
    fi
}

assert_kubeconfig_entries() {
    # $1 is present or absent, $2 the entry name, $3 the kubeconfig file,
    # named with --kubeconfig so KUBECONFIG plays no part in which file is
    # asked. Asked of kubectl rather than grepped from the file, because
    # delete leaves current-context naming the context it removed.
    local kind names found
    for kind in context user cluster; do
        case "${kind}" in
            context) names=$(kubectl --kubeconfig "$3" config get-contexts -o name) || return 1 ;;
            user) names=$(kubectl --kubeconfig "$3" config get-users) || return 1 ;;
            cluster) names=$(kubectl --kubeconfig "$3" config get-clusters) || return 1 ;;
        esac
        found=absent
        if echo "${names}" | grep -qFx "$2"; then
            found=present
        fi
        if [ "${found}" != "$1" ]; then
            echo "Expected kubeconfig ${kind} $2 to be $1 in $3, but it is ${found}"
            exit 1
        fi
    done
}

assert_node_sizes() {
    # k3s show prints node_sizes = {...} as a Python repr, which
    # ast.literal_eval reads back; the comparison is between dicts, so key
    # order does not matter. Only that one line is ever printed: the rest
    # of the output carries the node token and the admin kubeconfig, for
    # the reason dump_state() gives. This proves what the plugin recorded,
    # not what Shaken Fist built; assert_instance_size() is that half.
    local show_output found expected
    show_output=$(sf-client k3s show "${CLUSTER}")
    found=$(echo "${show_output}" | sed -n 's/^    node_sizes = //p')
    expected="{'control_plane': {'cpus': ${CONTROL_PLANE_CPUS}, "
    expected+="'memory': ${CONTROL_PLANE_MEMORY}, 'disk': ${CONTROL_PLANE_DISK}}, "
    expected+="'worker': {'cpus': ${WORKER_CPUS}, 'memory': ${WORKER_MEMORY}, "
    expected+="'disk': ${WORKER_DISK}}}"
    if [ -z "${found}" ]; then
        echo "Expected k3s show to report node_sizes = ${expected}"
        echo 'Found no node_sizes line at all'
        exit 1
    fi
    if ! python3 -c 'import ast, sys; sys.exit(ast.literal_eval(sys.argv[1]) != ast.literal_eval(sys.argv[2]))' \
            "${found}" "${expected}"; then
        echo "Expected k3s show to report node_sizes = ${expected}"
        echo "Found node_sizes = ${found}"
        exit 1
    fi
}

assert_instance_size() {
    # Shaken Fist's own view of an instance, rather than the plugin's
    # record of what it asked for: a create_instance() which ignored the
    # node's role would leave node_sizes correct and this wrong.
    # sf-client --simple instance show prints cpus:N, memory:N (in MB) and,
    # after a disk_spec,type,bus,size,base header, a
    # disk_spec,TYPE,BUS,SIZE,BASE line per disk; nodes have one disk.
    # Only those three values are ever printed, because the full output
    # carries the instance's user data.
    local role=$1
    local uuid=$2
    local cpus=$3
    local memory=$4
    local disk=$5
    local show_output found_cpus found_memory found_disk
    if [ -z "${uuid}" ]; then
        echo "Expected a ${role} instance UUID to check the size of, found none"
        exit 1
    fi
    show_output=$(sf-client --simple instance show "${uuid}")
    found_cpus=$(echo "${show_output}" | awk -F: '$1 == "cpus" {print $2}')
    found_memory=$(echo "${show_output}" | awk -F: '$1 == "memory" {print $2}')
    found_disk=$(echo "${show_output}" | awk -F, '$1 == "disk_spec" && $4 != "size" {print $4}')
    if [ "${found_cpus}" != "${cpus}" ] || [ "${found_memory}" != "${memory}" ] \
            || [ "${found_disk}" != "${disk}" ]; then
        echo "Expected ${role} ${uuid} to have cpus ${cpus}, memory ${memory} MB, disk ${disk} GB"
        echo "Shaken Fist reports cpus ${found_cpus:-none}, memory ${found_memory:-none} MB," \
            "disk ${found_disk:-none} GB"
        exit 1
    fi
}

network_field() {
    # One field of sf-client --simple network show, which prints NAME:VALUE
    # lines. Fails, and so ends the script, if there is no such network.
    local network=$1
    local field=$2
    local show_output
    show_output=$(sf-client --simple network show "${network}")
    echo "${show_output}" | awk -F: -v field="${field}" '$1 == field {print $2}'
}

count_labelled_nodes() {
    # kubectl prints "No resources found" to stderr and exits 0 when the
    # selector matches nothing, so wc -l is the count either way; a kubectl
    # failure still fails the pipeline under pipefail.
    kubectl get nodes -l "$1" -o name | wc -l
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

status 'Verify a k3s release older than the floor is refused'
# Every create refuses a release older than v1.21.1+k3s1, straight after
# the channel lookup and before the name is registered or anything is
# built, so this costs seconds. The v1.20 channel still resolves to such a
# release, and this is the only check that the live update API's release
# strings still parse: the unit tests hand the check literals. If upstream
# ever drops the v1.20 channel the create fails as an unknown channel
# instead, and the message check below fails saying so rather than
# passing. --worker-count 0 keeps a create which wrongly succeeds small.
too_old_output=/tmp/k3s-ci-floor-refusal
if sf-client k3s create ciTooOld --release-channel v1.20 \
        --worker-count 0 > "${too_old_output}" 2>&1; then
    echo 'create on the v1.20 channel succeeded; it should have been refused'
    exit 1
fi
if ! grep -qF 'older than' "${too_old_output}" \
        || ! grep -qF 'v1.21.1' "${too_old_output}"; then
    echo 'create on the v1.20 channel failed, but not with the release floor refusal:'
    cat "${too_old_output}"
    exit 1
fi
# A here-string rather than a pipe: under pipefail, grep -q exiting early
# can fail the pipe's writer and so the condition.
clusters=$(sf-client k3s list)
if grep -qxF ciTooOld <<< "${clusters}"; then
    echo 'The refused create registered ciTooOld anyway'
    exit 1
fi

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

# k3s configuration for each role, asserted once the cluster is up. The
# bare disable is deliberate: it is the case where the plugin's enforced
# disable+: [servicelb] drop-in has to append to the caller's list rather
# than be replaced by it, and the only place that k3s merge rule can be
# seen working. The labels are how the assertions find out that each
# role's file reached that role's nodes, agents included, without knowing
# any node's name.
cat - > "${manifest_dir}/ci-server.yaml" <<'SERVERCONFIG'
disable: [traefik]
node-label: [ci-role=server]
SERVERCONFIG
cat - > "${manifest_dir}/ci-agent.yaml" <<'AGENTCONFIG'
node-label: [ci-role=agent]
AGENTCONFIG

sf-client k3s create "${CLUSTER}" \
    --control-plane-count 1 --worker-count 2 --metal-address-count 2 \
    --manifest "${manifest_dir}/ci-staged.yaml" \
    --server-config "${manifest_dir}/ci-server.yaml" \
    --agent-config "${manifest_dir}/ci-agent.yaml" \
    --control-plane-cpus "${CONTROL_PLANE_CPUS}" \
    --control-plane-memory "${CONTROL_PLANE_MEMORY}" \
    --control-plane-disk "${CONTROL_PLANE_DISK}" \
    --worker-cpus "${WORKER_CPUS}" \
    --worker-memory "${WORKER_MEMORY}" \
    --worker-disk "${WORKER_DISK}"

# The manifest and both configurations are on the cluster now (and the
# configurations in its metadata), so the local copies have done their job.
# Cleaned up here the way the kubectl download's temp directory is, rather
# than left for the ephemeral runner to take with it.
rm -rf "${manifest_dir}"

status 'Verify the kubeconfig default wrote the local kubeconfig'
# The positive half of the --no-kubeconfig assertion further down. create
# defaults the flag on from the CLI, so this file has to exist and has to
# name the cluster; without this half, a create which quietly stopped
# writing it would make the negative assertion pass for the wrong reason.
if ! grep -q "${CLUSTER}" "${HOME}/.kube/config"; then
    echo "create did not write ${CLUSTER} into ${HOME}/.kube/config"
    exit 1
fi

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

status 'Verify the k3s configuration took effect on every node'
# These run after the LoadBalancer test on purpose. That took minutes, so
# Traefik's install job has long since run if it was going to, and ci-web
# is itself a LoadBalancer Service, so a live servicelb would have made
# svclb-ci-web-* pods by now. Run straight after the create, the absence
# checks could pass because nothing had been scheduled yet. The minimal
# cluster below is their positive control: it disables nothing, and the
# same names have to appear there.
#
# The HelmChart is absent only if kubectl says NotFound: any other failure
# (an API server which is not answering, say) is not evidence of absence.
if traefik_chart=$(kubectl get helmchart -n kube-system traefik -o name 2>&1); then
    echo 'Expected no kube-system/traefik HelmChart, given disable: [traefik] in --server-config'
    echo "Found ${traefik_chart}"
    exit 1
fi
if ! echo "${traefik_chart}" | grep -q 'NotFound'; then
    echo 'Expected kubectl to report the kube-system/traefik HelmChart NotFound'
    echo "It said: ${traefik_chart}"
    exit 1
fi
pods=$(pod_names)
# || true because no match is the expected answer here.
traefik_pods=$(echo "${pods}" | grep 'traefik' || true)
if [ -n "${traefik_pods}" ]; then
    echo 'Expected no Traefik pods, given disable: [traefik] in --server-config'
    echo "Found: ${traefik_pods}"
    exit 1
fi
# servicelb surviving here means the caller's bare disable replaced the
# plugin's enforced disable+: [servicelb] instead of being appended to.
svclb_pods=$(echo "${pods}" | grep '/svclb-' || true)
if [ -n "${svclb_pods}" ]; then
    echo 'Expected no svclb- pods: servicelb is disabled whenever MetalLB is installed'
    echo "Found: ${svclb_pods}"
    exit 1
fi

# Counted by label rather than by node name, which on this mixed case
# cluster the script would have to compute. Exact counts rather than "at
# least one", so the server file landing on agents (or the other way
# round) fails as well as either file going missing.
server_nodes=$(count_labelled_nodes ci-role=server)
if [ "${server_nodes}" -ne 1 ]; then
    echo "Expected 1 node labelled ci-role=server, found ${server_nodes}"
    exit 1
fi
agent_nodes=$(count_labelled_nodes ci-role=agent)
if [ "${agent_nodes}" -ne 2 ]; then
    echo "Expected 2 nodes labelled ci-role=agent, found ${agent_nodes}"
    exit 1
fi

# The default taint, which the caller's --server-config here does not
# touch: it is the positive control for the node-taint: [] opt-out the
# minimal cluster asserts. The empty check matters, because a loop over no
# control plane nodes would pass whatever their taints were.
control_plane_nodes=$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o name)
if [ -z "${control_plane_nodes}" ]; then
    echo 'Expected at least one node labelled node-role.kubernetes.io/control-plane, found none'
    exit 1
fi
for node in ${control_plane_nodes}; do
    taints=$(kubectl get "${node}" \
        -o jsonpath='{range .spec.taints[*]}{.key}:{.effect}{"\n"}{end}')
    if ! echo "${taints}" | grep -qx 'node-role.kubernetes.io/control-plane:NoSchedule'; then
        echo "Expected ${node} to be tainted node-role.kubernetes.io/control-plane:NoSchedule"
        echo "Its taints are: ${taints:-none}"
        exit 1
    fi
done

status 'Verify the nodes were built at the requested sizes'
assert_node_sizes
if ! control_plane_uuid=$(control_plane_uuids | sed -n 1p); then
    echo 'Expected k3s show to list control_plane_nodes, found none'
    exit 1
fi
assert_instance_size 'control plane node' "${control_plane_uuid}" \
    "${CONTROL_PLANE_CPUS}" "${CONTROL_PLANE_MEMORY}" "${CONTROL_PLANE_DISK}"
if ! first_worker=$(worker_uuids | sed -n 1p); then
    echo 'Expected k3s show to list worker_nodes, found none'
    exit 1
fi
assert_instance_size 'worker' "${first_worker}" \
    "${WORKER_CPUS}" "${WORKER_MEMORY}" "${WORKER_DISK}"

status 'Expand the cluster with an extra worker'
before_workers=$(worker_uuids | sort)
sf-client k3s expand-workers "${CLUSTER}" --worker-count 1
wait_for_nodes 4
after_workers=$(worker_uuids | sort)
new_worker=$(comm -13 <(echo "${before_workers}") <(echo "${after_workers}"))
if [ "$(echo "${new_worker}" | wc -l)" -ne 1 ] || [ -z "${new_worker}" ]; then
    echo "Expected exactly one new worker, found: ${new_worker}"
    exit 1
fi

status 'Verify the new worker was built from the recorded configuration'
# expand-workers takes no sizing or configuration flags, so the only way
# the new worker gets the agent label and the worker sizes is from what
# create recorded in the cluster metadata.
agent_nodes=$(count_labelled_nodes ci-role=agent)
if [ "${agent_nodes}" -ne 3 ]; then
    echo "Expected 3 nodes labelled ci-role=agent after expand-workers, found ${agent_nodes}"
    exit 1
fi
assert_instance_size 'new worker' "${new_worker}" \
    "${WORKER_CPUS}" "${WORKER_MEMORY}" "${WORKER_DISK}"

status 'Remove the worker which was just added'
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
# The delete's kubeconfig cleanup acts on ~/.kube/config, which is where
# create wrote this cluster's entries, whatever KUBECONFIG says -- and here
# KUBECONFIG names getconfig's copy, which holds entries of the same name.
# So the real kubectl's delete-* calls have to empty ~/.kube/config and
# leave the copy alone. Present first, so the absence is not vacuous.
main_kubeconfig="${HOME}/.kube/config"
cluster_fqcn=$(kubeconfig_fqcn "${CLUSTER}")
assert_kubeconfig_entries present "${cluster_fqcn}" "${main_kubeconfig}"
assert_kubeconfig_entries present "${cluster_fqcn}" "${KUBECONFIG}"
sf-client k3s delete "${CLUSTER}"
if sf-client k3s list | grep -q "^${CLUSTER}$"; then
    echo 'The cluster is still listed after deletion'
    exit 1
fi
assert_kubeconfig_entries absent "${cluster_fqcn}" "${main_kubeconfig}"
assert_kubeconfig_entries present "${cluster_fqcn}" "${KUBECONFIG}"
# --all includes error state instances, which the default listing hides:
# a node the delete failed to remove must not pass this check just
# because it fell into the error state.
remaining=$(sf-client instance list --all | grep "k3s-${CLUSTER}-node" | grep -cv 'deleted' || true)
if [ "${remaining}" -ne 0 ]; then
    echo "Found ${remaining} instances still present after deletion"
    exit 1
fi

status 'Create a network for the minimal cluster to borrow'
# The shape create allocates for itself: DHCP and NAT, which are
# sf-client's defaults. Waited for here because create looks a borrowed
# network up but does not wait for it to finish being set up.
sf-client network create "${MINIMAL_NETWORK}" 10.0.0.0/16 > /dev/null
minimal_network_uuid=$(network_field "${MINIMAL_NETWORK}" uuid)
minimal_network_state=''
for _ in $(seq 60); do
    minimal_network_state=$(network_field "${minimal_network_uuid}" state)
    if [ "${minimal_network_state}" = 'created' ]; then
        break
    fi
    sleep 2
done
if [ "${minimal_network_state}" != 'created' ]; then
    echo "Network ${MINIMAL_NETWORK} is in state ${minimal_network_state:-unknown} after two minutes"
    exit 1
fi

status 'Create a cluster with none of the optional components'
# HOME is left alone on purpose: --no-kubeconfig has to be the thing that
# leaves ~/.kube/config alone, not the absence of a home directory.
#
# Compared rather than asserted absent: the first create wrote
# ~/.kube/config (asserted above) and its delete removed that cluster's
# entries but not the file, so the file exists whatever this create does.
# What --no-kubeconfig promises is that this create does not touch it, and
# only a before-and-after comparison says that. An earlier version of this
# script asserted the file did not exist, which could never pass.
kubeconfig_before=$(sha256sum "${main_kubeconfig}")

# The opt-out from the default control plane taint. Its replacing the
# taint in config.yaml, rather than being merged with it, is a k3s rule
# only a live node can show; see MINIMAL_CLUSTER above for why this
# cluster has a worker.
minimal_config_dir=$(mktemp -d)
cat - > "${minimal_config_dir}/ci-server.yaml" <<'SERVERCONFIG'
node-taint: []
SERVERCONFIG

sf-client k3s create "${MINIMAL_CLUSTER}" \
    --control-plane-count 1 --worker-count 1 --metal-address-count 0 \
    --no-metallb --no-longhorn --no-kubeconfig \
    --network "${minimal_network_uuid}" \
    --server-config "${minimal_config_dir}/ci-server.yaml"

# On the cluster and in its metadata now, as for the main cluster above.
rm -rf "${minimal_config_dir}"

status 'Verify --no-kubeconfig left the local kubeconfig alone'
kubeconfig_after=$(sha256sum "${main_kubeconfig}")
if [ "${kubeconfig_before}" != "${kubeconfig_after}" ]; then
    echo "create --no-kubeconfig changed ${main_kubeconfig} anyway"
    exit 1
fi
if grep -q "${MINIMAL_CLUSTER}" "${main_kubeconfig}"; then
    echo "create --no-kubeconfig wrote ${MINIMAL_CLUSTER} into ${main_kubeconfig}"
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
wait_for_nodes 2

status 'Verify node-taint: [] removed the control plane taint'
# The main cluster's taint assertion is the positive control: without
# this file, a control plane with a worker beside it is tainted. Checked
# after wait_for_nodes, so the transient not-ready taint has gone and
# anything left came from the k3s configuration.
control_plane_nodes=$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o name)
if [ "$(echo "${control_plane_nodes}" | wc -l)" -ne 1 ] || [ -z "${control_plane_nodes}" ]; then
    echo "Expected exactly one node labelled node-role.kubernetes.io/control-plane, found: ${control_plane_nodes}"
    exit 1
fi
taints=$(kubectl get "${control_plane_nodes}" -o jsonpath='{.spec.taints}')
if [ -n "${taints}" ]; then
    echo "Expected ${control_plane_nodes} to have no taints, given node-taint: [] in --server-config"
    echo "Its taints are: ${taints}"
    exit 1
fi

status 'Verify Traefik and servicelb run where nothing disabled them'
# The positive control for the main cluster's absence checks. This cluster
# disables nothing and was built without MetalLB, so k3s installs Traefik
# from the kube-system/traefik HelmChart and servicelb gives its
# LoadBalancer Service svclb-traefik-* pods. If these never appear, the
# main cluster's checks for the same names prove nothing. Polled because
# Traefik's install pulls its image from the internet; a failure here is
# that poll timing out, not a node customisation fault.
traefik_chart=''
svclb_traefik_pods=''
for _ in $(seq 30); do
    if kubectl get helmchart -n kube-system traefik > /dev/null 2>&1; then
        traefik_chart='present'
    fi
    pods=$(pod_names)
    # || true because the pods may not have been scheduled yet.
    svclb_traefik_pods=$(echo "${pods}" | grep '^kube-system/svclb-traefik-' || true)
    if [ -n "${traefik_chart}" ] && [ -n "${svclb_traefik_pods}" ]; then
        break
    fi
    sleep 10
done
if [ -z "${traefik_chart}" ] || [ -z "${svclb_traefik_pods}" ]; then
    echo 'Positive control failed: after five minutes the minimal cluster, which disables nothing,'
    echo 'still lacks the kube-system/traefik HelmChart or a kube-system/svclb-traefik- pod.'
    echo "HelmChart: ${traefik_chart:-absent}; svclb-traefik pods: ${svclb_traefik_pods:-none}"
    echo "Without them the main cluster's Traefik and svclb absence checks prove nothing."
    exit 1
fi

status 'Verify the minimal cluster carries no ci-role labels'
# The main cluster's labels came from its configuration files, not from
# anything the plugin applies to every cluster.
labelled_nodes=$(count_labelled_nodes ci-role)
if [ "${labelled_nodes}" -ne 0 ]; then
    echo "Expected no nodes with a ci-role label on ${MINIMAL_CLUSTER}, found ${labelled_nodes}"
    exit 1
fi

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

status 'Verify bad names and counts are refused before any API call'
# The library refuses these before it asks the API for anything, so they
# cost seconds. create makes a missing namespace named by --namespace, and
# a refused create must not leave one behind; this one is derived from
# the job's own namespace, so it is unique per run. 'namespace show'
# succeeding is the failure. It fails for a namespace these credentials
# cannot see as well as for a missing one, so on a runner without admin
# the check is as strong as the credentials allow, and no stronger.
refused_namespace="$(cluster_namespace "${MINIMAL_CLUSTER}")-refused"
assert_refused "Cluster name 'my.cluster' cannot be used" \
    sf-client k3s create my.cluster --namespace "${refused_namespace}"
if sf-client namespace show "${refused_namespace}" > /dev/null 2>&1; then
    echo "The refused create made namespace ${refused_namespace} anyway"
    exit 1
fi
# The minimal cluster, which still exists, so the count is the only thing
# wrong with the request.
assert_refused worker_count \
    sf-client k3s expand-workers "${MINIMAL_CLUSTER}" --worker-count 0

status 'Provoke each health signal on the minimal cluster'
# Damages the cluster on purpose -- an OOM killed pod, a killed and then a
# stopped k3s-agent, an etcd snapshot, and a worker disk left full -- and
# asserts that health() reports each. So it runs last, immediately before
# the delete which cleans up after it, and on this cluster rather than the
# main one, whose Longhorn and MetalLB a stopped kubelet would upset. See
# docs/plans/PLAN-cumulative-health-signals-phase-03-live-validation.md.
# python3 is the venv activated above, which has the plugin installed, and
# the tool's kubectl reaches this cluster through the KUBECONFIG exported
# above.
python3 tools/ci_health_signals.py "${MINIMAL_CLUSTER}"

status 'Delete the minimal cluster'
# delete --no-kubeconfig skips the kubeconfig cleanup, which is the half of
# the flag create cannot exercise. The cleanup acts on ~/.kube/config, and
# this cluster was created with --no-kubeconfig, so that file holds nothing
# of its own: entries of its name are planted there first, which a delete
# that ran the cleanup anyway would remove. The file must also come out
# byte-identical. The planted entries are removed afterwards.
minimal_fqcn=$(kubeconfig_fqcn "${MINIMAL_CLUSTER}")
kubectl --kubeconfig "${main_kubeconfig}" config set-cluster "${minimal_fqcn}" \
    --server https://192.0.2.1:6443 > /dev/null
kubectl --kubeconfig "${main_kubeconfig}" config set-credentials "${minimal_fqcn}" \
    --token planted > /dev/null
kubectl --kubeconfig "${main_kubeconfig}" config set-context "${minimal_fqcn}" \
    --cluster "${minimal_fqcn}" --user "${minimal_fqcn}" > /dev/null
assert_kubeconfig_entries present "${minimal_fqcn}" "${main_kubeconfig}"
kubeconfig_before=$(sha256sum "${main_kubeconfig}")
sf-client k3s delete "${MINIMAL_CLUSTER}" --no-kubeconfig
if sf-client k3s list | grep -q "^${MINIMAL_CLUSTER}$"; then
    echo 'The minimal cluster is still listed after deletion'
    exit 1
fi
kubeconfig_after=$(sha256sum "${main_kubeconfig}")
if [ "${kubeconfig_before}" != "${kubeconfig_after}" ]; then
    echo "delete --no-kubeconfig changed ${main_kubeconfig} anyway"
    exit 1
fi
assert_kubeconfig_entries present "${minimal_fqcn}" "${main_kubeconfig}"
for kind in context user cluster; do
    kubectl --kubeconfig "${main_kubeconfig}" config "delete-${kind}" "${minimal_fqcn}" > /dev/null
done

status 'Verify the minimal cluster left the network it borrowed'
# The state rather than the lookup succeeding, because a network Shaken
# Fist has deleted can still be shown, in state deleted.
minimal_network_state=$(network_field "${minimal_network_uuid}" state)
if [ "${minimal_network_state}" != 'created' ]; then
    echo "delete destroyed ${MINIMAL_NETWORK}, which create was given with --network"
    echo "It is in state ${minimal_network_state:-unknown}"
    exit 1
fi
# Left for the namespace teardown to remove, like anything else this
# script makes.

status 'Success'
