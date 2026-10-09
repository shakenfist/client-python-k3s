#!/usr/bin/env python3
"""Provoke each health signal on a real k3s cluster, and check that health() reports it.

This tool damages the cluster it is pointed at. It OOM kills a pod, kills
k3s on the worker, stops k3s on the worker for a while, takes an etcd
snapshot, and finally fills the worker's root filesystem until only 3% is
free -- and it leaves the disk full. It exists to run immediately before
that cluster's delete, which removes the full disk along with the
instance, and that is how tools/ci_deploy_test.sh runs it, on the merge
tier's minimal cluster. Run it by hand only against a throwaway cluster
you are about to delete.

Phases 1 and 2 of the cumulative health signals plan added readings to
Cluster.health() which come from systemd, the kernel and the kubelet, and
their live runs only ever saw a fresh cluster: no counter went up, no
condition went True, no OOM kill was listed. Whether each reading moves
when the thing it reads happens is a question only a real node can
answer, so this provokes each one and asserts the report says so.
Decisions 1 to 7 of
docs/plans/PLAN-cumulative-health-signals-phase-03-live-validation.md are
the design, and decision 4 is the specification of every step, 4a to 4f,
in the order they run here.

It reads what an Ansible play or a daily poll reads: Cluster.health()'s
dict, through a client from make_client()'s own configuration discovery,
rather than parsing the CLI's rendering, which is deliberately not a
contract. Node commands go through Cluster.execute_and_await(), the agent
path create() itself uses, which raises CommandFailedError on a non-zero
exit. Pods go through the kubectl on PATH, against whatever cluster
KUBECONFIG names, and the --strict checks run sf-client itself.

The judgements are the check_*() functions: each takes reports and
returns None, or a message naming the reading which was wrong. They are
pure so that they can be unit tested, in
shakenfist_client_k3s/tests/test_ci_health_signals.py. What they cannot
test is whether systemd, the kernel and the kubelet still behave as the
checks expect on the k3s release the merge tier installs that day; only
a live run says that, which is why every step prints the readings it
compared even when it passes.

Every wait is a poll of health() with a bound, never a fixed sleep, and a
poll which gives up prints what it last read. The first failure ends the
run with exit status 1. Nothing is caught so that a later step can run
anyway, because each step starts from the cluster the one before it left.

Usage:
    python3 tools/ci_health_signals.py CLUSTER_NAME
"""

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
import uuid

from shakenfist_client_k3s.client import make_client
from shakenfist_client_k3s.cluster import Cluster
from shakenfist_client_k3s.cluster import node_name_for_instance


# How often a poll reads health() again. One read costs a few seconds of
# agent operations, so this is about the shortest interval which is not
# mostly spent reading.
POLL_INTERVAL_SECONDS = 5

# Decision 4's bounds. Each is several times what the step is expected to
# take, because the node lifecycle controller's grace period, the kubelet's
# eviction cadence and systemd's RestartSec all vary by k3s release, and the
# merge tier installs whichever release the channel resolves to that day.
# A slow run should pass; a wrong guess should fail with the readings.
OOM_POD_BOUND_SECONDS = 180
OOM_LISTED_BOUND_SECONDS = 60
RESTART_BOUND_SECONDS = 120
NOT_READY_BOUND_SECONDS = 180
READY_BOUND_SECONDS = 180
DISK_PRESSURE_BOUND_SECONDS = 180

# The pod killed at its own memory limit (decision 4b). The image is the
# one ci_deploy_test.sh's ci-web already pulls, from registry.k8s.io rather
# than Docker Hub, whose anonymous pull limits the under-cloud's shared
# egress address runs into. tail on /dev/zero buffers a line which never
# ends, so its memory grows until the limit stops it.
OOM_NAMESPACE = 'default'
OOM_CONTAINER = 'hog'
OOM_IMAGE = 'registry.k8s.io/e2e-test-images/nginx:1.15-alpine'
OOM_MEMORY_LIMIT = '32Mi'
OOM_COMMAND = ['tail', '/dev/zero']

# Commands run on a node by its agent, as root. The agent refuses a command
# line whose first word is not an executable on its PATH, so every one of
# these starts with systemctl, k3s or sh.
#
# --kill-who rather than --kill-whom: systemd 254 added the second
# spelling, and the nodes run Debian 12, whose systemd is 252. Newer
# releases still accept --kill-who, so this works on both. main sends the
# signal to k3s alone, and the installer's KillMode=process means stopping
# the unit leaves the pods' containers running too.
KILL_COMMAND = 'systemctl kill --kill-who=main --signal=KILL k3s-agent'
STOP_COMMAND = 'systemctl stop k3s-agent'
START_COMMAND = 'systemctl start k3s-agent'
SNAPSHOT_COMMAND = 'k3s etcd-snapshot save'

# Decision 4f fills the worker's root filesystem until this much of it is
# free. k3s's kubelet reports DiskPressure below 5% free, and the stock
# kubelet below 10%, so 3% is past both. It is not filled further, because
# the agent and the signals probe still have to work for anyone to see the
# pressure: on a 50 GB disk 3% is about 1.5 GB, and root can also use the
# filesystem's reserved blocks.
DISK_FREE_PERCENT = 3
DISK_FILL_PATH = '/ci-health-signals.fill'

# What the baseline expects each role's unit to be called. Written out
# rather than imported from cluster.py, because a check which reads its
# expectation from the code under test passes whatever that code says.
UNIT_BY_ROLE = {'control_plane': 'k3s', 'worker': 'k3s-agent'}
ROLE_LABELS = {'control_plane': 'control plane', 'worker': 'worker'}

# The same shape health() promises for boot_id, and written out again for
# the same reason as UNIT_BY_ROLE.
BOOT_ID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')

# Kubernetes' two answers for a node which is not Ready. None is not one of
# them: it means the Kubernetes probe could not read readiness at all, and
# a cluster unhealthy for that reason says nothing about a silent kubelet.
NOT_READY_STATUSES = ('False', 'Unknown')

# kubectl's own per-request timeout, and the backstop for any subprocess
# this runs, so that a wedged API server ends the run rather than the
# workflow's timeout doing it an hour later.
KUBECTL_REQUEST_TIMEOUT = '30s'
SUBPROCESS_TIMEOUT_SECONDS = 300


class Failure(Exception):
    """A step found something other than what decision 4 says it should."""


def say(text):
    """Print one line of the run's record.

    Flushed, because sf-client and kubectl write to the same job log from
    their own processes, and a buffered line would land after theirs.
    """
    print(text, flush=True)


def fail_if(message):
    """Raise Failure with message, if a check returned one."""
    if message is not None:
        raise Failure(message)


def _joined(problems):
    """Return a check's result: None for no problems, or one message naming them all."""
    if not problems:
        return None
    return '; '.join(problems)


def _is_count(value):
    """Say whether value is a non-negative integer, as every count and size in a report must be.

    bool is excluded explicitly because it is an int subclass, and True
    would otherwise pass as a count of one.
    """
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _label(node, section, key):
    """Name one reading of node the way the failure messages do, e.g. 'worker signals.k3s_state'."""
    return '%s %s.%s' % (ROLE_LABELS.get(node['role'], node['role']), section, key)


def find_node(report, role):
    """Return the one node in report with role, found by role and never by name.

    The minimal cluster has exactly one control plane node and one worker,
    and decision 4 means those two when it says "the control plane" and
    "the worker". A cluster shaped otherwise makes both phrases ambiguous,
    so this raises Failure rather than choosing one.
    """
    nodes = [node for node in report['nodes'] if node['role'] == role]
    if len(nodes) != 1:
        raise Failure('expected exactly one %s node in the report, found %d: %s'
                      % (ROLE_LABELS.get(role, role), len(nodes),
                         ', '.join(str(node['name']) for node in nodes) or 'none'))
    return nodes[0]


def _boot_problems(before, after):
    """Return a problem if after's node has rebooted since before's, by boot_id.

    Shared by every step which compares a counter with a baseline, because a
    reboot resets every counter but the etcd sizes and would make the
    comparison meaningless (docs/library-api.md, "What signals reports").
    """
    if after['signals']['boot_id'] != before['signals']['boot_id']:
        return ['%s changed from %r to %r: the node rebooted'
                % (_label(after, 'signals', 'boot_id'), before['signals']['boot_id'],
                   after['signals']['boot_id'])]
    return []


def _baseline_node_problems(node):
    """Return what is wrong with one node of a fresh cluster's report (decision 4a)."""
    role = node['role']
    signals = node['signals']
    kubernetes = node['kubernetes']
    problems = []

    # Not one of decision 4a's terms, but the explanation for most of them
    # failing at once: a probe which did not run leaves every reading None.
    if signals['error'] is not None:
        problems.append('%s is %r' % (_label(node, 'signals', 'error'), signals['error']))

    boot_id = signals['boot_id']
    if not isinstance(boot_id, str) or not BOOT_ID_RE.match(boot_id):
        problems.append('%s is %r, not a UUID' % (_label(node, 'signals', 'boot_id'), boot_id))
    if signals['k3s_state'] != 'active':
        problems.append("%s is %r, not 'active'" % (_label(node, 'signals', 'k3s_state'), signals['k3s_state']))
    if signals['k3s_unit'] != UNIT_BY_ROLE[role]:
        problems.append('%s is %r, not %r'
                        % (_label(node, 'signals', 'k3s_unit'), signals['k3s_unit'], UNIT_BY_ROLE[role]))
    for key in ('oom_kills', 'k3s_restarts'):
        if not _is_count(signals[key]):
            problems.append('%s is %r, not an integer of 0 or more' % (_label(node, 'signals', key), signals[key]))

    total = signals['memory_total_bytes']
    available = signals['memory_available_bytes']
    if not _is_count(total) or total <= 0:
        problems.append('%s is %r, not more than 0' % (_label(node, 'signals', 'memory_total_bytes'), total))
    if not _is_count(available) or available <= 0:
        problems.append('%s is %r, not more than 0'
                        % (_label(node, 'signals', 'memory_available_bytes'), available))
    elif _is_count(total) and available > total:
        problems.append('%s is %r, more than memory_total_bytes %r'
                        % (_label(node, 'signals', 'memory_available_bytes'), available, total))

    etcd_bytes = signals['etcd_bytes']
    if role == 'control_plane':
        if not _is_count(etcd_bytes) or etcd_bytes <= 0:
            problems.append('%s is %r, not more than 0' % (_label(node, 'signals', 'etcd_bytes'), etcd_bytes))
    elif etcd_bytes is not None:
        problems.append('%s is %r on a worker, not None' % (_label(node, 'signals', 'etcd_bytes'), etcd_bytes))

    if kubernetes['ready'] != 'True':
        problems.append("%s is %r, not 'True'" % (_label(node, 'kubernetes', 'ready'), kubernetes['ready']))
    if kubernetes['disk_pressure'] != 'False':
        problems.append("%s is %r, not 'False'"
                        % (_label(node, 'kubernetes', 'disk_pressure'), kubernetes['disk_pressure']))
    return problems


def check_baseline(report):
    """Decision 4a: a fresh cluster is healthy and every reading has the shape it should."""
    problems = []
    if report['healthy'] is not True:
        problems.append('healthy is %r on a fresh cluster, not True' % (report['healthy'],))
    for role in ('control_plane', 'worker'):
        problems.extend(_baseline_node_problems(find_node(report, role)))
    return _joined(problems)


def oom_pod_manifest(pod_name, node_name):
    """Return decision 4b's pod, as the dict kubectl apply is handed as JSON.

    nodeName bypasses the scheduler, so the pod runs on the worker whatever
    the scheduler would have chosen, and so it is the worker's oom_kills
    which the kill has to raise. restartPolicy Never keeps the container's
    terminated state, OOMKilled, where kubectl and health() can read it.
    """
    return {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {'name': pod_name, 'namespace': OOM_NAMESPACE},
        'spec': {
            'nodeName': node_name,
            'restartPolicy': 'Never',
            'containers': [{
                'name': OOM_CONTAINER,
                'image': OOM_IMAGE,
                'command': list(OOM_COMMAND),
                'resources': {'limits': {'memory': OOM_MEMORY_LIMIT}},
            }],
        },
    }


def _container_status(pod):
    """Return the hog container's status from a kubectl pod document, or None before it has one."""
    for status in (pod.get('status') or {}).get('containerStatuses') or []:
        if status.get('name') == OOM_CONTAINER:
            return status
    return None


def check_pod_oom_killed(pod):
    """Return None once the hog container has terminated as OOMKilled, else what it is doing instead."""
    status = _container_status(pod) or {}
    terminated = (status.get('state') or {}).get('terminated') or {}
    if terminated.get('reason') == 'OOMKilled':
        return None
    return ('pod phase is %r and container %s state is %s'
            % ((pod.get('status') or {}).get('phase'), OOM_CONTAINER,
               json.dumps(status.get('state'), sort_keys=True)))


def pod_finished_otherwise(pod):
    """Return why the pod can no longer be OOM killed, or None while it still can.

    A container which terminated for another reason, or a pod which
    finished or was evicted without one, will never become OOMKilled, so
    waiting out the bound for it only delays reporting its real cause.
    """
    status = pod.get('status') or {}
    terminated = ((_container_status(pod) or {}).get('state') or {}).get('terminated')
    if terminated is not None and terminated.get('reason') != 'OOMKilled':
        return ('container %s terminated with reason %r and exit code %r, not OOMKilled: %s'
                % (OOM_CONTAINER, terminated.get('reason'), terminated.get('exitCode'),
                   json.dumps(terminated, sort_keys=True)))
    if terminated is None and status.get('phase') in ('Failed', 'Succeeded'):
        return ('pod is %s without container %s having terminated, reason %r: %s'
                % (status.get('phase'), OOM_CONTAINER, status.get('reason'), status.get('message')))
    return None


def _oom_entry(node, pod_name):
    """Return node's oom_killed entry for decision 4b's pod and container, or None."""
    for entry in node['kubernetes']['oom_killed'] or []:
        if (entry.get('namespace') == OOM_NAMESPACE and entry.get('pod') == pod_name
                and entry.get('container') == OOM_CONTAINER):
            return entry
    return None


def check_oom_killed_listed(report, pod_name):
    """Return None once the worker's oom_killed lists decision 4b's container."""
    worker = find_node(report, 'worker')
    if _oom_entry(worker, pod_name) is None:
        return ('%s has no entry for %s/%s/%s: %s'
                % (_label(worker, 'kubernetes', 'oom_killed'), OOM_NAMESPACE, pod_name, OOM_CONTAINER,
                   json.dumps(worker['kubernetes']['oom_killed'], sort_keys=True)))
    return None


def check_oom_kill(baseline, report, pod_name):
    """Decision 4b: a pod killed at its own memory limit is counted, listed, and not judged.

    oom_kills has to rise because /proc/vmstat's oom_kill counts a kill at
    a cgroup's own limit as well as one made when the whole node runs out
    of memory.
    """
    before = find_node(baseline, 'worker')
    worker = find_node(report, 'worker')
    problems = []

    entry = _oom_entry(worker, pod_name)
    if entry is None:
        problems.append(check_oom_killed_listed(report, pod_name))
    elif not _is_count(entry.get('finished_at')):
        problems.append('%s entry for %s has finished_at %r, not an integer'
                        % (_label(worker, 'kubernetes', 'oom_killed'), pod_name, entry.get('finished_at')))

    oom_before = before['signals']['oom_kills']
    oom_after = worker['signals']['oom_kills']
    if not _is_count(oom_before) or not _is_count(oom_after) or oom_after < oom_before + 1:
        problems.append('%s is %r, not at least the baseline %r plus 1: the limit kill was not counted'
                        % (_label(worker, 'signals', 'oom_kills'), oom_after, oom_before))

    problems.extend(_boot_problems(before, worker))

    if report['healthy'] is not True:
        problems.append('healthy is %r after an OOM kill, not True: a kill is history, not judged'
                        % (report['healthy'],))
    return _joined(problems)


def check_automatic_restart(before_report, report):
    """Decision 4c: systemd restarting a killed k3s-agent is counted, and is not a reboot."""
    before = find_node(before_report, 'worker')
    worker = find_node(report, 'worker')
    problems = []
    if worker['signals']['k3s_state'] != 'active':
        problems.append("%s is %r, not 'active'"
                        % (_label(worker, 'signals', 'k3s_state'), worker['signals']['k3s_state']))
    restarts_before = before['signals']['k3s_restarts']
    restarts = worker['signals']['k3s_restarts']
    if not _is_count(restarts_before) or restarts != restarts_before + 1:
        problems.append('%s is %r, not the %r before the kill plus 1'
                        % (_label(worker, 'signals', 'k3s_restarts'), restarts, restarts_before))
    problems.extend(_boot_problems(before, worker))
    return _joined(problems)


def check_kubelet_silent(report):
    """Return None once Kubernetes says the worker is not Ready, in either of its two words for it."""
    worker = find_node(report, 'worker')
    ready = worker['kubernetes']['ready']
    if ready not in NOT_READY_STATUSES:
        return '%s is %r, not one of %s' % (_label(worker, 'kubernetes', 'ready'), ready,
                                            ' or '.join(repr(s) for s in NOT_READY_STATUSES))
    return None


def check_not_ready(report):
    """Decision 4d: a stopped kubelet makes the cluster unhealthy and leaves the node's own entry healthy.

    The two healthy terms are the point (decision 6 of the phase 2 plan
    for the first, decision 3 for the second): readiness is folded into
    the cluster's healthy and kept out of the node's, which is Shaken
    Fist's view of the instance and the gate on whether its agent is
    asked anything.
    """
    worker = find_node(report, 'worker')
    problems = []
    silent = check_kubelet_silent(report)
    if silent is not None:
        problems.append(silent)
    if report['healthy'] is not False:
        problems.append('healthy is %r with the worker not Ready, not False' % (report['healthy'],))
    if worker['healthy'] is not True:
        problems.append("the worker's node level healthy is %r, not True: it must not take Ready"
                        % (worker['healthy'],))
    if worker['signals']['k3s_state'] != 'inactive':
        problems.append("%s is %r after systemctl stop, not 'inactive'"
                        % (_label(worker, 'signals', 'k3s_state'), worker['signals']['k3s_state']))
    return _joined(problems)


def check_health_exit_codes(plain_rc, strict_rc, expected_strict_rc):
    """Decision 4d: sf-client k3s health exits 0, and with --strict exits 0 or 1 by healthy.

    The plain command's 0 matters as much as --strict's 1: it is the
    documented contract that only --strict turns the report into an exit
    code (shakenfist/client-python-k3s#101).
    """
    problems = []
    if plain_rc != 0:
        problems.append('sf-client k3s health exited %r, not 0' % (plain_rc,))
    if strict_rc != expected_strict_rc:
        problems.append('sf-client k3s health --strict exited %r, not %r' % (strict_rc, expected_strict_rc))
    return _joined(problems)


def check_ready_again(report):
    """Decision 4d: once k3s-agent is started by hand, the worker is Ready and the cluster healthy."""
    worker = find_node(report, 'worker')
    problems = []
    if worker['kubernetes']['ready'] != 'True':
        problems.append("%s is %r, not 'True'" % (_label(worker, 'kubernetes', 'ready'), worker['kubernetes']['ready']))
    if worker['signals']['k3s_state'] != 'active':
        problems.append("%s is %r, not 'active'"
                        % (_label(worker, 'signals', 'k3s_state'), worker['signals']['k3s_state']))
    if report['healthy'] is not True:
        problems.append('healthy is %r with the worker Ready again, not True' % (report['healthy'],))
    return _joined(problems)


def check_restarts_reset(report):
    """Decision 4d: systemd resets NRestarts when the unit is started by hand.

    docs/library-api.md tells a caller to expect this, and it is why a
    counter lower than its baseline voids the baseline. If a k3s or
    systemd release stops doing it, the docs are wrong rather than the
    code.
    """
    worker = find_node(report, 'worker')
    restarts = worker['signals']['k3s_restarts']
    if restarts != 0:
        return ('%s is %r after a start by hand, not 0'
                % (_label(worker, 'signals', 'k3s_restarts'), restarts))
    return None


def check_snapshot_saved(before_report, report):
    """Decision 4e: an etcd snapshot makes the control plane's etcd_snapshot_bytes grow.

    A snapshot directory which did not exist before reads None, which
    health() reports rather than 0, and is compared as though it were
    empty: k3s creates the directory with the first snapshot.
    """
    before = find_node(before_report, 'control_plane')
    control_plane = find_node(report, 'control_plane')
    was = before['signals']['etcd_snapshot_bytes']
    now = control_plane['signals']['etcd_snapshot_bytes']
    floor = was if _is_count(was) else 0
    if not _is_count(now) or now <= floor:
        return ('%s is %r after a snapshot, not more than the %r before it'
                % (_label(control_plane, 'signals', 'etcd_snapshot_bytes'), now, was))
    return None


def check_disk_pressure_reported(report):
    """Return None once the worker reports DiskPressure."""
    worker = find_node(report, 'worker')
    if worker['kubernetes']['disk_pressure'] != 'True':
        return ("%s is %r, not 'True'"
                % (_label(worker, 'kubernetes', 'disk_pressure'), worker['kubernetes']['disk_pressure']))
    return None


def check_disk_pressure(report):
    """Decision 4f: disk pressure is reported, and judged by nothing.

    A node under pressure is degraded rather than down (decision 6 of the
    phase 2 plan), so it stays Ready and the cluster stays healthy. The
    other two pressure conditions are checked so that the reading which
    went True is known to be the disk's, not all three at once.
    """
    worker = find_node(report, 'worker')
    kubernetes = worker['kubernetes']
    problems = []
    reported = check_disk_pressure_reported(report)
    if reported is not None:
        problems.append(reported)
    if kubernetes['ready'] != 'True':
        problems.append("%s is %r under disk pressure, not 'True'"
                        % (_label(worker, 'kubernetes', 'ready'), kubernetes['ready']))
    if report['healthy'] is not True:
        problems.append('healthy is %r under disk pressure, not True: pressure is reported, not judged'
                        % (report['healthy'],))
    for key in ('memory_pressure', 'pid_pressure'):
        if kubernetes[key] != 'False':
            problems.append("%s is %r, not 'False'" % (_label(worker, 'kubernetes', key), kubernetes[key]))
    return _joined(problems)


def describe_readings(report, role):
    """Return what a poll which gave up last saw: role's signals and kubernetes, and the cluster's verdict.

    Lenient where find_node() is strict, because this is already
    explaining one failure and must not replace it with another.
    """
    lines = [
        'healthy: %r' % (report.get('healthy'),),
        'api: %s' % json.dumps(report.get('api'), sort_keys=True),
        'kubernetes: %s' % json.dumps(report.get('kubernetes'), sort_keys=True),
    ]
    nodes = [node for node in report.get('nodes') or [] if node.get('role') == role]
    if not nodes:
        lines.append('no %s node in the report' % ROLE_LABELS.get(role, role))
    for node in nodes:
        lines.append('%s %s signals: %s'
                     % (ROLE_LABELS.get(role, role), node.get('name'), json.dumps(node.get('signals'), sort_keys=True)))
        lines.append('%s %s kubernetes: %s'
                     % (ROLE_LABELS.get(role, role), node.get('name'),
                        json.dumps(node.get('kubernetes'), sort_keys=True)))
    return '\n'.join(lines)


def _wait(read, predicate, bound, interval, clock, sleep):
    """Read and judge until predicate returns None or bound seconds have passed.

    Returns the last value read, what predicate last said about it (None
    if satisfied), and the seconds elapsed. The read which finds the bound
    passed is the last: there is no sleep after it.
    """
    started = clock()
    while True:
        value = read()
        waiting = predicate(value)
        elapsed = clock() - started
        if waiting is None or elapsed >= bound:
            return value, waiting, elapsed
        sleep(interval)


def poll(read, predicate, bound, describe, role='worker', interval=POLL_INTERVAL_SECONDS,
         clock=time.monotonic, sleep=time.sleep):
    """Re-read health() every interval seconds until predicate is satisfied, for at most bound seconds.

    read is Cluster.health, or anything which returns a report like it.
    predicate takes a report and, like the check_*() functions, returns
    None when the state being waited for holds and otherwise says what
    does not hold yet. describe says what is being waited for. Returns the
    report which satisfied predicate and how long that took, which the
    step prints, because how long each wait took is what step 3b uses to
    judge the bounds.

    When the bound passes, raises Failure with describe, what predicate
    last said, and role's node's last signals and kubernetes entries and
    the top level healthy, api and kubernetes, so that a failed CI run can
    be diagnosed from its log alone.
    """
    report, waiting, elapsed = _wait(read, predicate, bound, interval, clock, sleep)
    if waiting is not None:
        raise Failure('gave up after %ds (bound %ds) waiting for %s. Last: %s\nLast readings:\n%s'
                      % (elapsed, bound, describe, waiting, describe_readings(report, role)))
    return report, elapsed


def disk_fill_command(free_percent=DISK_FREE_PERCENT, path=DISK_FILL_PATH):
    """Return decision 4f's command, which fills the root filesystem until free_percent of it is free.

    df -P prints one line per filesystem whatever the device name's
    length, and its size and available columns are statfs's f_blocks and
    f_bavail, which are the two the kubelet's nodefs.available is computed
    from. Available excludes the blocks reserved for root, so filling to
    3% of available still leaves root those. 'set --' is how POSIX sh
    splits a command's output into variables; Debian's /bin/sh is dash,
    which has no arrays or here-strings. Nothing is allocated when the
    filesystem is already that full.
    """
    script = (
        'set -e; '
        "set -- $(df -P -k / | awk 'NR == 2 { print $2, $4 }'); "
        'fill=$(( ($2 - $1 * %d / 100) * 1024 )); '
        'if [ "$fill" -gt 0 ]; then fallocate -l "$fill" %s; fi'
        % (free_percent, shlex.quote(path)))
    return 'sh -c %s' % shlex.quote(script)


def kubectl(*args, stdin=None):
    """Run kubectl against KUBECONFIG's cluster and return its stdout, raising Failure if it fails."""
    argv = ['kubectl', '--request-timeout=%s' % KUBECTL_REQUEST_TIMEOUT] + list(args)
    proc = subprocess.run(argv, input=stdin, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_SECONDS)
    if proc.returncode != 0:
        raise Failure('%s exited %d: %s' % (' '.join(argv), proc.returncode, proc.stderr.strip()))
    return proc.stdout


def run_health(name, strict):
    """Run sf-client k3s health name, with --strict if asked, and return the finished process."""
    argv = ['sf-client', 'k3s', 'health', name] + (['--strict'] if strict else [])
    return subprocess.run(argv, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_SECONDS)


def _health_exit_codes(name, expected_strict_rc):
    """Run health with and without --strict, fail unless they exit as expected, and return both codes."""
    plain = run_health(name, False)
    strict = run_health(name, True)
    problem = check_health_exit_codes(plain.returncode, strict.returncode, expected_strict_rc)
    if problem is not None:
        raise Failure('%s\n--- health ---\n%s%s\n--- health --strict ---\n%s%s'
                      % (problem, plain.stdout, plain.stderr, strict.stdout, strict.stderr))
    return plain.returncode, strict.returncode


def _summary(node):
    """Return one node's readings on a line, for the step lines."""
    signals = node['signals']
    kubernetes = node['kubernetes']
    parts = [
        'boot_id=%s' % signals['boot_id'],
        '%s=%s' % (signals['k3s_unit'], signals['k3s_state']),
        'k3s_restarts=%r' % (signals['k3s_restarts'],),
        'oom_kills=%r' % (signals['oom_kills'],),
        'memory_available_bytes=%r of %r' % (signals['memory_available_bytes'], signals['memory_total_bytes']),
    ]
    if node['role'] == 'control_plane':
        parts.append('etcd_bytes=%r' % (signals['etcd_bytes'],))
        parts.append('etcd_snapshot_bytes=%r' % (signals['etcd_snapshot_bytes'],))
    parts.append('ready=%r' % (kubernetes['ready'],))
    parts.append('disk_pressure=%r' % (kubernetes['disk_pressure'],))
    return ', '.join(parts)


def step_baseline(read):
    """4a. Take one report of the untouched cluster, check it, and return it as the baseline."""
    baseline = read()
    fail_if(check_baseline(baseline))
    say('4a baseline: healthy=%r; control plane: %s; worker: %s'
        % (baseline['healthy'], _summary(find_node(baseline, 'control_plane')),
           _summary(find_node(baseline, 'worker'))))
    return baseline


def step_oom_kill(read, baseline):
    """4b. Kill a pod at its own memory limit on the worker, and check the report counts and lists it."""
    worker = find_node(baseline, 'worker')
    # node_name_for_instance() takes an instance from the API, and its only
    # input is the name, which the report's node entry carries as it was
    # read from that instance.
    node_name = node_name_for_instance({'name': worker['name']})
    if node_name is None:
        raise Failure('the worker has no instance name, so its Kubernetes node cannot be named')

    # Asked first so that a KUBECONFIG naming some other cluster fails here,
    # saying so, rather than as a pod pinned to a node which does not exist
    # sitting Pending for three minutes.
    kubectl('get', 'node', node_name, '-o', 'name')

    pod_name = 'ci-oom-%s' % uuid.uuid4().hex[:8]
    kubectl('apply', '-f', '-', stdin=json.dumps(oom_pod_manifest(pod_name, node_name)))

    def read_pod():
        return json.loads(kubectl('get', 'pod', pod_name, '-n', OOM_NAMESPACE, '-o', 'json'))

    def pod_predicate(pod):
        fail_if(pod_finished_otherwise(pod))
        return check_pod_oom_killed(pod)

    pod, waiting, pod_elapsed = _wait(read_pod, pod_predicate, OOM_POD_BOUND_SECONDS, POLL_INTERVAL_SECONDS,
                                      time.monotonic, time.sleep)
    if waiting is not None:
        raise Failure('gave up after %ds (bound %ds) waiting for pod %s to be OOMKilled. Last: %s\n'
                      'Last pod status:\n%s'
                      % (pod_elapsed, OOM_POD_BOUND_SECONDS, pod_name, waiting,
                         json.dumps(pod.get('status'), sort_keys=True)))

    report, listed_elapsed = poll(
        read, lambda r: check_oom_killed_listed(r, pod_name), OOM_LISTED_BOUND_SECONDS,
        "the worker's kubernetes.oom_killed to list %s/%s/%s" % (OOM_NAMESPACE, pod_name, OOM_CONTAINER))
    fail_if(check_oom_kill(baseline, report, pod_name))

    after = find_node(report, 'worker')
    entry = _oom_entry(after, pod_name)
    say('4b pod limit kill: %s OOMKilled on %s after %ds, listed in oom_killed after %ds more '
        '(restarts=%r, finished_at=%r); worker oom_kills %r -> %r; boot_id unchanged (%s); healthy=%r'
        % (pod_name, node_name, pod_elapsed, listed_elapsed, entry['restarts'], entry['finished_at'],
           worker['signals']['oom_kills'], after['signals']['oom_kills'], after['signals']['boot_id'],
           report['healthy']))

    kubectl('delete', 'pod', pod_name, '-n', OOM_NAMESPACE, '--wait=true', '--timeout=120s')


def step_automatic_restart(cluster, read, worker_uuid):
    """4c. SIGKILL the worker's k3s, and check systemd's restart of it is counted."""
    # A report from just before the kill rather than 4a's, so that the
    # comparison is with the counter as it stood when the kill happened.
    before = read()
    cluster.execute_and_await([worker_uuid], [KILL_COMMAND])
    report, elapsed = poll(read, lambda r: check_automatic_restart(before, r), RESTART_BOUND_SECONDS,
                           "systemd to restart the worker's k3s-agent and count it")
    worker = find_node(report, 'worker')
    say('4c automatic restart: worker k3s-agent %s after %ds, k3s_restarts %r -> %r; boot_id unchanged (%s)'
        % (worker['signals']['k3s_state'], elapsed, find_node(before, 'worker')['signals']['k3s_restarts'],
           worker['signals']['k3s_restarts'], worker['signals']['boot_id']))


def step_not_ready(cluster, read, name, worker_uuid):
    """4d. Stop the worker's k3s, check NotReady and --strict, then start it by hand and check again."""
    cluster.execute_and_await([worker_uuid], [STOP_COMMAND])
    report, elapsed = poll(read, check_kubelet_silent, NOT_READY_BOUND_SECONDS,
                           'Kubernetes to stop calling the worker Ready')
    fail_if(check_not_ready(report))
    plain_rc, strict_rc = _health_exit_codes(name, 1)
    worker = find_node(report, 'worker')
    say('4d stopped: worker ready=%r after %ds; healthy=%r, worker node healthy=%r, k3s_state=%r; '
        'health exited %d, health --strict exited %d'
        % (worker['kubernetes']['ready'], elapsed, report['healthy'], worker['healthy'],
           worker['signals']['k3s_state'], plain_rc, strict_rc))

    cluster.execute_and_await([worker_uuid], [START_COMMAND])
    report, elapsed = poll(read, check_ready_again, READY_BOUND_SECONDS,
                           'the worker to be Ready again after starting k3s-agent by hand')
    worker = find_node(report, 'worker')

    # Printed as soon as it is read, before anything below can fail, so
    # that a run in which systemd stopped resetting the count shows what it
    # left. A poll which gave up above has already printed it among the
    # worker's signals.
    say('4d k3s_restarts after the start by hand: %r (0 expected)'
        % (worker['signals']['k3s_restarts'],))

    plain_rc, strict_rc = _health_exit_codes(name, 0)
    say('4d started: worker ready=%r after %ds; healthy=%r; health exited %d, health --strict exited %d'
        % (worker['kubernetes']['ready'], elapsed, report['healthy'], plain_rc, strict_rc))
    fail_if(check_restarts_reset(report))


def step_snapshot(cluster, read):
    """4e. Take an etcd snapshot on the control plane, and check etcd_snapshot_bytes grows."""
    before = read()
    control_plane_uuid = find_node(before, 'control_plane')['uuid']
    cluster.execute_and_await([control_plane_uuid], [SNAPSHOT_COMMAND])
    report = read()
    fail_if(check_snapshot_saved(before, report))
    say('4e etcd snapshot: control plane etcd_snapshot_bytes %r -> %r'
        % (find_node(before, 'control_plane')['signals']['etcd_snapshot_bytes'],
           find_node(report, 'control_plane')['signals']['etcd_snapshot_bytes']))


def step_disk_pressure(cluster, read, worker_uuid):
    """4f. Fill the worker's root filesystem to 3% free, and check DiskPressure is reported and not judged.

    Not undone: the kubelet takes minutes to clear the condition, and the
    cluster's delete removes the file with the instance.
    """
    cluster.execute_and_await([worker_uuid], [disk_fill_command()])
    report, elapsed = poll(read, check_disk_pressure_reported, DISK_PRESSURE_BOUND_SECONDS,
                           'the worker to report DiskPressure')
    fail_if(check_disk_pressure(report))
    worker = find_node(report, 'worker')
    kubernetes = worker['kubernetes']
    say('4f disk pressure: worker disk_pressure=%r after %ds; ready=%r, healthy=%r, memory_pressure=%r, '
        'pid_pressure=%r, memory_available_bytes=%r'
        % (kubernetes['disk_pressure'], elapsed, kubernetes['ready'], report['healthy'],
           kubernetes['memory_pressure'], kubernetes['pid_pressure'], worker['signals']['memory_available_bytes']))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('cluster', help='the k3s cluster to damage, in the namespace sf-client is configured for')
    args = parser.parse_args(argv)

    client = make_client()
    cluster = Cluster(client, args.cluster, client.namespace)
    read = cluster.health

    try:
        baseline = step_baseline(read)
        # Instance uuids do not change, so the baseline's are the ones to
        # send node commands to for the rest of the run.
        worker_uuid = find_node(baseline, 'worker')['uuid']
        step_oom_kill(read, baseline)
        step_automatic_restart(cluster, read, worker_uuid)
        step_not_ready(cluster, read, args.cluster, worker_uuid)
        step_snapshot(cluster, read)
        step_disk_pressure(cluster, read, worker_uuid)
    except Failure as e:
        # Caught only to print it without a traceback, which would say
        # nothing about the cluster; the run still ends here. Anything
        # else, a CommandFailedError from a node command among them,
        # propagates with its traceback and also exits 1.
        say('FAILED: %s' % e)
        return 1

    say('Every provoked signal was reported as decision 4 expects.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
