#!/usr/bin/env python3
"""
Helios distributed benchmark orchestrator.

Launches spot instances, runs Ansible playbooks, collects results, and cleans up.
If spot instances cannot be acquired, skips benchmarks and cleans up.

Usage:
  # Default: YCSB benchmark
  python3 py/bench_aws.py

  # TPC-C with custom terminals
  python3 py/bench_aws.py --bench-type tpcc --bench-terms 1,16,64,128

  # TPC-H serial
  python3 py/bench_aws.py --bench-type tpch

  # Dry run (show what would be launched)
  python3 py/bench_aws.py --dry-run
"""

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ANSIBLE_DIR = SCRIPT_DIR.parent
LOG_FILE = None  # set in main()

# ──────────────────────────────────────────────
# Cluster topology — edit these to change sizes
# ──────────────────────────────────────────────
CLUSTER = {
    "lineairdb": {
        "tag": "helios-lineairdb",
        "instance_type": "c6i.16xlarge",
        "count": 1,
    },
    "mysql": {
        "tag": "helios-mysql",
        "instance_type": "c6i.4xlarge",
        "count": 1,
    },
    "benchbase": {
        "tag": "helios-bench",
        "instance_type": "c6i.16xlarge",
        "count": 1,
    },
}

AWS_DEFAULTS = {
    "region": "ap-southeast-2",
    # Every launch parameter is passed explicitly, without a launch template
    "project_tag": "HeliosPush",   # distinct from Project=Helios: neither harness touches the other's instances
    "ssh_key": "~/.ssh/ordo-aws.pem",
    "ssh_user": "ubuntu",
    # env-only AMI: Ubuntu 24.04 with the runtime libraries, a Java 23 runtime,
    # sysstat, and no Helios binaries.
    "ami_id": "ami-01791c5dbde3b3ba1",
    "security_group": "sg-02d9a0d5948d02dbb",
    # Same-AZ pinning: every role must sit in one AZ or cross-AZ latency lands
    # on the RPC path.
    "subnet": "subnet-01a0d1fe432c2f92e",  # ap-southeast-2a (same-AZ pinning)
}

# Everything this run launched, appended as soon as run-instances returns, so
# cleanup covers a partially launched cluster.
LAUNCHED = {"instances": [], "spot_requests": []}

# vCPU count for EC2 instance sizes (used to build machine_spec locally)
_VCPU_BY_SIZE = {
    "xlarge": 4, "2xlarge": 8, "4xlarge": 16, "8xlarge": 32,
    "12xlarge": 48, "16xlarge": 64, "24xlarge": 96, "32xlarge": 128, "metal": 128,
}


def build_machine_spec():
    """Build machine_spec string from CLUSTER config (e.g. lineairdb-64x1_mysql-16x1_...)."""
    parts = []
    for role, cfg in CLUSTER.items():
        size = cfg["instance_type"].split(".")[-1]  # e.g. "16xlarge"
        vcpu = _VCPU_BY_SIZE.get(size, 0)
        parts.append(f"{role}-{vcpu}x{cfg['count']}")
    return "_".join(parts)


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if LOG_FILE:
        LOG_FILE.write(line + "\n")
        LOG_FILE.flush()


def aws(cmd, region=None):
    """Run an AWS CLI command and return parsed JSON output."""
    full = ["aws"] + cmd
    if region:
        full.extend(["--region", region])
    full.extend(["--no-cli-pager", "--output", "json"])
    result = subprocess.run(full, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws command failed: {' '.join(full)}\n{result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def aws_text(cmd, region=None):
    """Run an AWS CLI command and return text output."""
    full = ["aws"] + cmd
    if region:
        full.extend(["--region", region])
    full.extend(["--no-cli-pager", "--output", "text"])
    result = subprocess.run(full, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws command failed: {' '.join(full)}\n{result.stderr.strip()}")
    return result.stdout.strip()


def run(cmd, check=True, **kwargs):
    """Run a shell command, streaming output to both terminal and log file."""
    log(f"  $ {cmd}")
    if LOG_FILE:
        # Stream stdout+stderr line-by-line to both terminal and log
        proc = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, **kwargs,
        )
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            LOG_FILE.write(line)
            LOG_FILE.flush()
        proc.wait()
        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
        return proc
    return subprocess.run(cmd, shell=True, check=check, **kwargs)


# ──────────────────────────────────────────────
# Phase 1: Launch instances (spot with on-demand fallback)
# ──────────────────────────────────────────────
SPOT_ERRORS = ("InsufficientInstanceCapacity", "SpotMaxPriceTooLow", "MaxSpotInstanceCountExceeded")


def _record_launch(ids, region):
    """Register instance IDs and their spot request IDs for cleanup, first thing."""
    LAUNCHED["instances"].extend(ids)
    try:
        req_ids = aws_text([
            "ec2", "describe-instances", "--instance-ids", *ids,
            "--query", "Reservations[].Instances[].SpotInstanceRequestId",
        ], region=region)
        if req_ids and req_ids != "None":
            LAUNCHED["spot_requests"].extend(
                r for r in req_ids.split() if r and r != "None")
    except RuntimeError as e:
        log(f"  Warning: spot request lookup failed for {ids}: {e}")


def _number_instances(ids, base_tag, region):
    """Tag instances with sequential names: tag-1, tag-2, ... Best-effort:
    a tagging failure must never orphan already-launched instances."""
    for i, iid in enumerate(ids, 1):
        name = f"{base_tag}-{i}" if len(ids) > 1 else base_tag
        try:
            aws(["ec2", "create-tags", "--resources", iid,
                 "--tags", f"Key=Name,Value={name}"], region=region)
        except RuntimeError as e:
            log(f"  Warning: tagging {iid} failed ({e}); continuing")



# Roles whose primary ENI gets more than the EC2 default 8 combined queues.
# ethtool -L cannot raise the EC2-side allocation, so it is set at
# run-instances time; 32 is the per-ENI maximum of the instance family used.
ENA_TUNED_ROLES = {"benchbase"}
ENA_QUEUE_COUNT = 32


def _launch_role(role, cfg, args):
    """Launch instances for a single role, spot by default.

    Fully explicit run-instances (no launch template): env AMI, dead-man
    user-data (self-terminate if forgotten), gp3 root with DeleteOnTermination,
    IMDSv2. On-demand happens only with --on-demand or --allow-od-fallback."""
    region = args.region
    tag_spec = [
        "--tag-specifications",
        f"ResourceType=instance,Tags=["
        f"{{Key=Name,Value={cfg['tag']}}},"
        f"{{Key=Project,Value={args.project_tag}}}"
        f"]",
    ]
    deadman = (f"#!/bin/bash\n"
               f"shutdown +{args.deadman_minutes} \"helios bench dead-man\"\n")
    base_cmd = [
        "ec2", "run-instances",
        "--image-id", args.ami_id,
        "--key-name", Path(args.ssh_key).stem,
        "--count", str(cfg["count"]),
        "--instance-type", cfg["instance_type"],
        "--block-device-mappings",
        f"DeviceName=/dev/sda1,Ebs={{VolumeSize={args.root_gib},"
        f"VolumeType=gp3,DeleteOnTermination=true}}",
        "--metadata-options", "HttpTokens=required,HttpEndpoint=enabled",
        "--instance-initiated-shutdown-behavior", "terminate",
        "--user-data", deadman,
    ]
    if role in ENA_TUNED_ROLES:
        # --network-interfaces subsumes --subnet-id/--security-group-ids;
        # mixing both forms is rejected by the API.
        network_interface = {
            "DeviceIndex": 0,
            "Groups": [args.security_group],
            "AssociatePublicIpAddress": True,
            "DeleteOnTermination": True,
            "EnaQueueCount": ENA_QUEUE_COUNT,
        }
        if args.subnet:
            network_interface["SubnetId"] = args.subnet
        base_cmd += ["--network-interfaces", json.dumps([network_interface])]
    else:
        base_cmd += ["--security-group-ids", args.security_group]
        if args.subnet:
            base_cmd += ["--subnet-id", args.subnet]

    if not args.on_demand:
        log(f"Launching {cfg['count']}x {cfg['instance_type']} for {role} ({cfg['tag']}) [spot]...")
        try:
            spot_cmd = base_cmd + [
                "--instance-market-options",
                '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}',
            ] + tag_spec
            result = aws(spot_cmd, region=region)
            ids = [inst["InstanceId"] for inst in result.get("Instances", [])]
            _record_launch(ids, region)
            _number_instances(ids, cfg["tag"], region)
            log(f"  -> [spot] {ids}")
            return ids
        except RuntimeError as e:
            if any(err in str(e) for err in SPOT_ERRORS):
                if not args.allow_od_fallback:
                    raise RuntimeError(
                        f"Spot unavailable for {role} ({e}); aborting. "
                        f"Pass --allow-od-fallback to permit on-demand.")
                log(f"  Spot unavailable for {role}, falling back to on-demand (--allow-od-fallback)...")
            else:
                raise

    # On-demand (--on-demand flag or authorized fallback)
    log(f"Launching {cfg['count']}x {cfg['instance_type']} for {role} ({cfg['tag']}) [on-demand]...")
    result = aws(base_cmd + tag_spec, region=region)
    ids = [inst["InstanceId"] for inst in result.get("Instances", [])]
    _record_launch(ids, region)
    _number_instances(ids, cfg["tag"], region)
    log(f"  -> [on-demand] {ids}")
    return ids


def launch_instances(args):
    """Launch instances for all roles. Returns list of instance IDs, or None on failure."""
    all_instance_ids = []

    for role, cfg in CLUSTER.items():
        try:
            ids = _launch_role(role, cfg, args)
            all_instance_ids.extend(ids)
        except RuntimeError as e:
            log(f"  LAUNCH FAILED for {role}: {e}")
            # Terminate any instances already launched
            if all_instance_ids:
                log(f"  Rolling back {len(all_instance_ids)} already-launched instances...")
                try:
                    aws_text([
                        "ec2", "terminate-instances",
                        "--instance-ids", *all_instance_ids,
                    ], region=args.region)
                except RuntimeError:
                    pass
            return None

    return all_instance_ids


# ──────────────────────────────────────────────
# Phase 2: Wait for instances
# ──────────────────────────────────────────────
def wait_for_instances(instance_ids, region, timeout=300):
    """Wait until all instances are running."""
    log(f"Waiting for {len(instance_ids)} instances to be running...")
    aws([
        "ec2", "wait", "instance-running",
        "--instance-ids", *instance_ids,
    ], region=region)
    log("All instances running.")


# ──────────────────────────────────────────────
# Phase 3: Generate inventory + wait for SSH
# ──────────────────────────────────────────────
def generate_inventory(args):
    """Run update_inventory.py to generate inventory.ini."""
    log("Generating Ansible inventory...")
    run(
        f"python3 {SCRIPT_DIR / 'update_inventory.py'}"
        f" --region {args.region}"
        f" --key {args.ssh_key}"
        f" --user {args.ssh_user}"
        f" --project-tag {args.project_tag}"
    )


def wait_for_ssh(args):
    """Wait until Ansible can reach all hosts."""
    log("Waiting for SSH connectivity...")
    run(
        f"ansible -i {ANSIBLE_DIR / 'inventory.ini'} all"
        f" -m wait_for_connection -a 'timeout=300 delay=5 sleep=2' -o"
    )
    log("All hosts reachable via SSH.")


# ──────────────────────────────────────────────
# Phase 4: Run Ansible playbooks
# ──────────────────────────────────────────────
def run_playbook(name, extra_vars=None, args=None):
    """Run an Ansible playbook."""
    cmd = f"ansible-playbook -i {ANSIBLE_DIR / 'inventory.ini'} {ANSIBLE_DIR / name}"
    if extra_vars:
        cmd += f' -e "{extra_vars}"'
    run(cmd)


def deploy_infrastructure(args, bundle_path, bundle_sha256):
    """Push the binary bundle to every node, then start roles in parallel."""
    inv = str(ANSIBLE_DIR / "inventory.ini")

    # Push the bundle to every host before any role starts
    log(f"Pushing bundle {bundle_path} ({bundle_sha256[:12]}...) to all nodes...")
    run(f"ansible-playbook -i {inv} {ANSIBLE_DIR / 'push_bundle.yml'}"
        f" -e {shlex.quote(json.dumps({'bundle_path': bundle_path, 'bundle_sha256': bundle_sha256}))}")

    # Start the roles in parallel
    log("Deploying infrastructure (lineairdb + mysql in parallel)...")
    # Write deploy logs alongside bench_aws.log
    deploy_log_dir = Path(LOG_FILE.name).parent if LOG_FILE else None
    procs = []

    for playbook in ["lineairdb.yml", "mysql.yml"]:
        cmd = f"ansible-playbook -i {inv} {ANSIBLE_DIR / playbook}"
        log(f"  $ {cmd}")
        if deploy_log_dir:
            pb_log = open(deploy_log_dir / f"{playbook.replace('.yml', '')}.log", "w")
            p = subprocess.Popen(cmd, shell=True, stdout=pb_log, stderr=subprocess.STDOUT)
            procs.append((playbook, p, pb_log))
        else:
            p = subprocess.Popen(cmd, shell=True)
            procs.append((playbook, p, None))

    failed = []
    for name, p, fh in procs:
        rc = p.wait()
        if fh:
            fh.close()
        if rc != 0:
            failed.append(name)
            log(f"  FAILED: {name} (rc={rc})")
        else:
            log(f"  OK: {name}")

    if failed:
        raise RuntimeError(f"Infrastructure deploy failed: {', '.join(failed)}")
    log("Infrastructure deployed.")


def salvage_server_logs(args, run_id):
    """Best-effort: pull liveness snapshots + service logs off the nodes
    before cleanup destroys the evidence. Never raises."""
    try:
        if not LAUNCHED["instances"]:
            return
        log("Salvaging server logs before cleanup...")
        run_playbook("salvage_logs.yml",
                     extra_vars=f"run_id={run_id} machine_spec={build_machine_spec()}")
        log("Salvage complete.")
    except Exception as e:
        log(f"Salvage failed (continuing to cleanup): {e}")


def run_benchmarks(args, run_id):
    """Run benchbase setup + benchmark execution."""

    bench_vars = f"bench_type={args.bench_type}"
    if args.bench_scalefactor:
        bench_vars += f" bench_scalefactor={args.bench_scalefactor}"

    # run_id + machine_spec let the setup play write load-phase artifacts into
    # the same result dir the sweep uses. Observability only.
    obs_vars = f" run_id={run_id} machine_spec={build_machine_spec()}"
    if args.load_jstack:
        obs_vars += " load_jstack=true"
    if args.perf_stat:
        obs_vars += " perf_stat=true"

    # RTT baseline: topology property, captured once before any bench traffic
    log("Recording RTT baseline between all nodes...")
    run_playbook("ping_baseline.yml", extra_vars=bench_vars + obs_vars)

    # Setup: create schema + load data
    log(f"Setting up {args.bench_type.upper()} (SF={args.bench_scalefactor or 'default'})...")
    run_playbook("benchbase.yml", extra_vars=bench_vars + obs_vars)

    # Execute: benchmark with monitoring
    exec_vars = bench_vars + f" run_id={run_id}"
    if args.bench_time:
        exec_vars += f" bench_time={args.bench_time}"
    if args.bench_terms:
        exec_vars += f" bench_terms=[{args.bench_terms}]"
    if args.bench_type == "tpch" and args.bench_serial is not None:
        exec_vars += f" bench_serial={'true' if args.bench_serial else 'false'}"
    if args.bench_profile:
        exec_vars += f" bench_profile={args.bench_profile}"
    if args.bench_prefetch or args.bench_prefetch_stmt:
        exec_vars += " bench_prefetch=true"
    if args.bench_prefetch_stmt:
        exec_vars += " bench_prefetch_plan=false"
    if args.bench_ndv_drift:
        exec_vars += " bench_ndv_drift=true"
    if args.perf:
        exec_vars += " enable_perf=true"
    if args.perf_stat:
        exec_vars += " perf_stat=true"

    log(f"Running benchmark: {exec_vars}")
    run_playbook("measure_usage.yml", extra_vars=exec_vars)

    # Plot results — pass run_id root; plot scripts auto-detect machine_spec subdir
    result_root = ANSIBLE_DIR / "result" / run_id
    log("Plotting results...")
    run(f"python3 {SCRIPT_DIR / 'plot_throughput.py'} --root {result_root}", check=False)
    run(f"python3 {SCRIPT_DIR / 'plot_cpu.py'} --root {result_root}", check=False)
    if args.bench_type == "tpcc":
        run(f"python3 {SCRIPT_DIR / 'plot_tpcc.py'} --root {result_root}", check=False)
    if args.bench_type == "tpch":
        run(f"python3 {SCRIPT_DIR / 'plot_tpch.py'} --root {result_root}", check=False)
    if args.perf:
        _plot_perf_reports(result_root)

    log(f"Benchmark complete. run_id={run_id}")
    return run_id


def _plot_perf_reports(result_root):
    """Render plot_perf.py for every collected perf-report-*.txt under result_root."""
    config_root = result_root / build_machine_spec()
    plot_dir = config_root / "_plot"
    plot_dir.mkdir(parents=True, exist_ok=True)

    role_modes = {"lineairdb": "server", "mysql": "proxy"}
    for role, mode in role_modes.items():
        cfg = CLUSTER.get(role)
        if not cfg:
            continue
        size = cfg["instance_type"].split(".")[-1]
        vcpu = _VCPU_BY_SIZE.get(size, 0)
        if vcpu == 0:
            continue
        for report in sorted((config_root / role).glob(f"*/perf-report-{mode}-*.txt")):
            output = plot_dir / f"perf_{report.parent.name}.png"
            run(
                f"python3 {SCRIPT_DIR / 'plot_perf.py'} {report} "
                f"--vcpu {vcpu} --mode {mode} --output {output}",
                check=False,
            )


# ──────────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────────
def cleanup(args):
    """Terminate the recorded launches and every instance carrying the project tag.

    Spot requests are cancelled per instance, never by key name: the key pair is shared."""
    region = args.region
    project_tag = args.project_tag
    log(f"Cleaning up (Project={project_tag}, region={region})...")
    ok = True

    # Collect targets: recorded launches plus Project-tagged instances
    try:
        tagged = aws_text([
            "ec2", "describe-instances",
            "--filters",
            f"Name=tag:Project,Values={project_tag}",
            "Name=instance-state-name,Values=pending,running,stopping,stopped",
            "--query", "Reservations[].Instances[].InstanceId",
        ], region=region)
    except RuntimeError as e:
        log(f"  ERROR: describe-instances failed: {e}")
        tagged = ""
        ok = False
    ids = sorted(set(LAUNCHED["instances"]) |
                 set(tagged.split() if tagged and tagged != "None" else []))

    # Cancel only our spot requests, recorded or resolved from our instances
    spot_reqs = set(LAUNCHED["spot_requests"])
    if ids:
        try:
            req_ids = aws_text([
                "ec2", "describe-instances", "--instance-ids", *ids,
                "--query", "Reservations[].Instances[].SpotInstanceRequestId",
            ], region=region)
            if req_ids and req_ids != "None":
                spot_reqs |= {r for r in req_ids.split() if r and r != "None"}
        except RuntimeError as e:
            log(f"  ERROR: resolving spot requests failed: {e}")
            ok = False
    if spot_reqs:
        log(f"  Cancelling {len(spot_reqs)} own spot requests: {sorted(spot_reqs)}")
        try:
            aws_text([
                "ec2", "cancel-spot-instance-requests",
                "--spot-instance-request-ids", *sorted(spot_reqs),
            ], region=region)
        except RuntimeError as e:
            log(f"  ERROR: cancel spot failed: {e}")
            ok = False

    if ids:
        log(f"  Terminating {len(ids)} instances: {ids}")
        try:
            aws_text(["ec2", "terminate-instances", "--instance-ids", *ids],
                     region=region)
        except RuntimeError as e:
            log(f"  ERROR: terminate failed: {e}")
            ok = False
    else:
        log("  Nothing to terminate.")

    # Verify that nothing is still alive under our tag
    try:
        leftovers = aws_text([
            "ec2", "describe-instances",
            "--filters",
            f"Name=tag:Project,Values={project_tag}",
            "Name=instance-state-name,Values=pending,running,stopping,stopped",
            "--query", "Reservations[].Instances[].InstanceId",
        ], region=region)
    except RuntimeError as e:
        log(f"  ERROR: verification failed: {e}")
        leftovers = ""
        ok = False
    if leftovers and leftovers != "None":
        log(f"  ERROR: instances still not shutting down: {leftovers.split()}")
        ok = False
    elif ok:
        log("  Verified: no Project-tagged instances left (terminating/terminated).")

    log("Cleanup done." if ok else "CLEANUP INCOMPLETE: re-run with --cleanup-only and check the console.")
    return ok


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Helios distributed benchmark orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 py/bench_aws.py --bench-type ycsb --bench-profile b
  python3 py/bench_aws.py --bench-type tpcc --bench-terms 1,16,64,128
  python3 py/bench_aws.py --bench-type tpcc --bench-prefetch --bench-terms 1,16,64,128
  python3 py/bench_aws.py --bench-type tpch --bench-scalefactor 0.1
  python3 py/bench_aws.py --bench-type tpch --bench-serial false --bench-terms 1,2,4,8 --bench-scalefactor 0.01
  python3 py/bench_aws.py --cleanup-only
  python3 py/bench_aws.py --dry-run
""",
    )

    # Benchmark options
    parser.add_argument("--bench-type", default="ycsb", choices=["ycsb", "tpcc", "tpch"])
    parser.add_argument("--bench-scalefactor", default=None, help="Scale factor (default: 10 for ycsb/tpcc, 0.1 for tpch)")
    parser.add_argument("--bench-time", type=int, default=None, help="Execution time in seconds")
    parser.add_argument("--bench-terms", default=None, help="Comma-separated terminal counts (e.g. 1,32,64)")
    parser.add_argument("--bench-profile", default=None, help="YCSB profile: a,b,c,e,f")
    parser.add_argument("--bench-serial", default=None, type=lambda x: x.lower() == "true",
                        help="TPC-H serial mode (true/false)")
    parser.add_argument("--bench-prefetch", action="store_true",
                        help="Run BenchBase in Helios prefetch mode "
                             "(SET GLOBAL lineairdb_prefetch_execution=ON on every MySQL "
                             "and HELIOS_PREFETCH_PLAN=1 env for the executor, so the "
                             "TPC-C procedures inject @_tx_plan)")
    parser.add_argument("--bench-prefetch-stmt", action="store_true",
                        help="Statement-scoped autogen prefetch: "
                             "SET GLOBAL lineairdb_prefetch_execution=ON WITHOUT "
                             "HELIOS_PREFETCH_PLAN, so the proxy derives a per-statement "
                             "read plan from the QEP instead of the injected @_tx_plan DSL")
    parser.add_argument("--bench-ndv-drift", action="store_true",
                        help="SET GLOBAL lineairdb_stats_drift_refresh=ON "
                             "(default OFF: the NDV/histogram recompute is synchronous "
                             "on the read path)")
    parser.add_argument("--perf", action="store_true", help="Enable perf profiling on lineairdb + mysql nodes")
    parser.add_argument("--load-jstack", action="store_true",
                        help="Take one thread dump of the loader JVM mid-load (diagnostic; "
                             "costs a JVM safepoint pause, so it is off by default)")
    parser.add_argument("--perf-stat", action="store_true",
                        help="Sample IPC / LLC counters (perf stat counting mode, NOT perf "
                             "record) on the mysql and lineairdb nodes across both the load "
                             "and the sweep; degrades to a logged note if perf is unavailable")

    # AWS options
    parser.add_argument("--region", default=AWS_DEFAULTS["region"])
    parser.add_argument("--project-tag", default=AWS_DEFAULTS["project_tag"])
    parser.add_argument("--ssh-key", default=AWS_DEFAULTS["ssh_key"])
    parser.add_argument("--ssh-user", default=AWS_DEFAULTS["ssh_user"])
    parser.add_argument("--ami-id", default=AWS_DEFAULTS["ami_id"],
                        help="env-only AMI for all roles")
    parser.add_argument("--security-group", default=AWS_DEFAULTS["security_group"])
    parser.add_argument("--subnet", default=AWS_DEFAULTS["subnet"], help="Subnet ID (for AZ pinning)")
    parser.add_argument("--deadman-minutes", type=int, default=240,
                        help="Instances self-terminate after this many minutes (safety net)")
    parser.add_argument("--root-gib", type=int, default=60, help="Root gp3 volume size")
    parser.add_argument("--allow-od-fallback", action="store_true",
                        help="Permit on-demand fallback when spot capacity is unavailable "
                             "(default: abort instead)")

    # Cluster topology overrides
    parser.add_argument("--mysql-count", type=int, default=None, help="Override MySQL node count")
    parser.add_argument("--mysql-instance-type", default=None, help="Override MySQL instance type")
    parser.add_argument("--lineairdb-instance-type", default=None, help="Override LineairDB instance type")
    parser.add_argument("--benchbase-count", type=int, default=None, help="Override BenchBase node count")
    parser.add_argument("--benchbase-instance-type", default=None, help="Override BenchBase instance type")
    # Control options
    parser.add_argument("--bundle", default=None,
                        help="Path to helios bundle tar.gz (default: build one now via build_bundle.sh)")
    parser.add_argument("--on-demand", action="store_true", help="Skip spot, launch all instances as on-demand")
    parser.add_argument("--cleanup-only", action="store_true", help="Only terminate instances, don't launch")
    parser.add_argument("--skip-cleanup", action="store_true", help="Don't terminate instances after benchmark")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be launched")

    args = parser.parse_args()
    if args.deadman_minutes < 1:
        parser.error("--deadman-minutes must be at least 1")
    if args.bench_prefetch and args.bench_prefetch_stmt:
        parser.error("--bench-prefetch and --bench-prefetch-stmt are mutually exclusive")
    if args.bundle is not None:
        args.bundle = str(Path(args.bundle).resolve())
    os.chdir(ANSIBLE_DIR)

    # Apply cluster overrides before building machine_spec
    if args.mysql_count is not None:
        CLUSTER["mysql"]["count"] = args.mysql_count
    if args.mysql_instance_type is not None:
        CLUSTER["mysql"]["instance_type"] = args.mysql_instance_type
    if args.lineairdb_instance_type is not None:
        CLUSTER["lineairdb"]["instance_type"] = args.lineairdb_instance_type
    if args.benchbase_count is not None:
        CLUSTER["benchbase"]["count"] = args.benchbase_count
    if args.benchbase_instance_type is not None:
        CLUSTER["benchbase"]["instance_type"] = args.benchbase_instance_type

    machine_spec = build_machine_spec()

    # Setup log directory: result/<run_id>/<machine_spec>/logs/
    global LOG_FILE
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    if args.bench_prefetch_stmt:
        run_id += "-prefetch-stmt"
    elif args.bench_prefetch:
        run_id += "-prefetch"
    if args.bench_ndv_drift:
        run_id += "-ndvdrift"
    log_dir = ANSIBLE_DIR / "result" / run_id / machine_spec / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    LOG_FILE = open(log_dir / "bench_aws.log", "w")
    log(f"Log directory: {log_dir}")
    log(f"Machine spec: {machine_spec}")

    # Cleanup-only mode
    if args.cleanup_only:
        cleanup_ok = cleanup(args)
        LOG_FILE.close()
        return 0 if cleanup_ok else 1

    # Dry run
    if args.dry_run:
        log("DRY RUN — would launch:")
        for role, cfg in CLUSTER.items():
            log(f"  {role}: {cfg['count']}x {cfg['instance_type']} (tag={cfg['tag']})")
        log(f"Benchmark: {args.bench_type} SF={args.bench_scalefactor or 'default'}")
        if args.bench_terms:
            log(f"Terminals: [{args.bench_terms}]")
        LOG_FILE.close()
        return 0

    # Build or locate the bundle before launching anything
    bundle_path = args.bundle
    if bundle_path is None:
        bundle_path = str(ANSIBLE_DIR / "helios-bundle.tar.gz")
        log("Building bundle via build_bundle.sh...")
        run(f"bash {shlex.quote(str(SCRIPT_DIR / 'build_bundle.sh'))} {shlex.quote(bundle_path)}")
    bundle_path = str(Path(bundle_path).resolve())
    bundle_sha256 = subprocess.run(
        ["sha256sum", bundle_path], capture_output=True, text=True, check=True,
    ).stdout.split()[0]
    log(f"Bundle: {bundle_path} sha256={bundle_sha256}")

    # Full run
    start_time = time.time()

    rc = 1
    try:
        # Phase 1: Launch
        instance_ids = launch_instances(args)
        if instance_ids is None:
            log("LAUNCH FAILED: skipping benchmark.")
        else:
            # Phase 2: Wait
            wait_for_instances(instance_ids, args.region)

            # Phase 3: Inventory + SSH
            generate_inventory(args)
            wait_for_ssh(args)

            # Phase 4: Deploy + Benchmark
            deploy_infrastructure(args, bundle_path, bundle_sha256)
            run_benchmarks(args, run_id)

            elapsed = time.time() - start_time
            log(f"All done in {elapsed / 60:.1f} minutes.")
            rc = 0

    except (RuntimeError, subprocess.CalledProcessError) as e:
        log(f"ERROR: {e}")
        salvage_server_logs(args, run_id)

    except KeyboardInterrupt:
        log("Interrupted by user.")

    finally:
        # Always sweep: a launch whose response was lost has no recorded id
        if not args.skip_cleanup and not cleanup(args):
            rc = 1
        if LOG_FILE:
            log(f"Full log saved to: {log_dir}")
            LOG_FILE.close()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
