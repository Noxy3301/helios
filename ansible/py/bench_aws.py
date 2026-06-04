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
    "haproxy": {
        "tag": "helios-haproxy",
        "instance_type": "c6i.8xlarge",
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
    "launch_template": "lt-0f28a9f6b02e1c019",
    "project_tag": "Helios",
    "ssh_key": "~/.ssh/helios-aws.pem",
    "ssh_user": "ubuntu",
    # On-demand fallback params (extracted from launch template)
    "ami_id": "ami-06b4ab6c66b56fbd8",
    "security_group": "sg-02d9a0d5948d02dbb",
    "subnet": "subnet-0a15ff55a4cae198b",  # ap-southeast-2c (same-AZ pinning)
}

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


def _launch_role(role, cfg, args):
    """Launch instances for a single role. Try spot first, fall back to on-demand."""
    region = args.region
    tag_spec = [
        "--tag-specifications",
        f"ResourceType=instance,Tags=["
        f"{{Key=Name,Value={cfg['tag']}}},"
        f"{{Key=Project,Value={args.project_tag}}}"
        f"]",
    ]
    base_cmd = [
        "ec2", "run-instances",
        "--launch-template", f"LaunchTemplateId={args.launch_template},Version=$Default",
        "--count", str(cfg["count"]),
        "--instance-type", cfg["instance_type"],
    ]
    if args.subnet:
        base_cmd += ["--subnet-id", args.subnet]

    def _number_instances(ids, base_tag, region):
        """Tag instances with sequential names: tag-1, tag-2, ..."""
        for i, iid in enumerate(ids, 1):
            name = f"{base_tag}-{i}" if len(ids) > 1 else base_tag
            aws(["ec2", "create-tags", "--resources", iid,
                 "--tags", f"Key=Name,Value={name}"], region=region)

    if not args.on_demand:
        # Try spot first
        log(f"Launching {cfg['count']}x {cfg['instance_type']} for {role} ({cfg['tag']}) [spot]...")
        try:
            spot_cmd = base_cmd + [
                "--instance-market-options",
                '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}',
            ] + tag_spec
            result = aws(spot_cmd, region=region)
            ids = [inst["InstanceId"] for inst in result.get("Instances", [])]
            _number_instances(ids, cfg["tag"], region)
            log(f"  -> [spot] {ids}")
            return ids
        except RuntimeError as e:
            if any(err in str(e) for err in SPOT_ERRORS):
                log(f"  Spot unavailable for {role}, falling back to on-demand...")
            else:
                raise

    # On-demand (fallback or --on-demand flag)
    # Launch template has InstanceMarketOptions=spot, so we must launch
    # WITHOUT the template and specify all params directly.
    log(f"Launching {cfg['count']}x {cfg['instance_type']} for {role} ({cfg['tag']}) [on-demand]...")
    od_cmd = [
        "ec2", "run-instances",
        "--image-id", args.ami_id,
        "--key-name", Path(args.ssh_key).stem,
        "--security-group-ids", args.security_group,
        "--count", str(cfg["count"]),
        "--instance-type", cfg["instance_type"],
    ]
    if args.subnet:
        od_cmd += ["--subnet-id", args.subnet]
    result = aws(od_cmd + tag_spec, region=region)
    ids = [inst["InstanceId"] for inst in result.get("Instances", [])]
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


def deploy_infrastructure(branch=None):
    """Deploy LineairDB, MySQL, HAProxy in parallel, then wait."""
    log("Deploying infrastructure (lineairdb + mysql + haproxy in parallel)...")
    inv = str(ANSIBLE_DIR / "inventory.ini")
    extra = f' -e "helios_branch={branch}"' if branch else ""
    # Write deploy logs alongside bench_aws.log
    deploy_log_dir = Path(LOG_FILE.name).parent if LOG_FILE else None
    procs = []

    for playbook in ["lineairdb.yml", "mysql.yml", "haproxy.yml"]:
        cmd = f"ansible-playbook -i {inv} {ANSIBLE_DIR / playbook}{extra}"
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


def run_benchmarks(args, run_id):
    """Run benchbase setup + benchmark execution."""

    bench_vars = f"bench_type={args.bench_type}"
    if args.bench_scalefactor:
        bench_vars += f" bench_scalefactor={args.bench_scalefactor}"

    # Setup: create schema + load data
    log(f"Setting up {args.bench_type.upper()} (SF={args.bench_scalefactor or 'default'})...")
    run_playbook("benchbase.yml", extra_vars=bench_vars)

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
    if args.bench_prefetch:
        exec_vars += " bench_prefetch=true"
    if args.perf:
        exec_vars += " enable_perf=true"

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
    """Cancel spot requests first, then terminate instances."""
    region = args.region
    project_tag = args.project_tag
    log(f"Cleaning up (Project={project_tag}, region={region})...")

    # Step 1: Cancel ALL open/active spot requests first (prevents respawn).
    # Spot requests may not have Project tags, so find them via launch template.
    try:
        spot_ids = aws_text([
            "ec2", "describe-spot-instance-requests",
            "--filters",
            "Name=state,Values=open,active",
            f"Name=launch.key-name,Values={Path(args.ssh_key).stem}",
            "--query", "SpotInstanceRequests[].SpotInstanceRequestId",
        ], region=region)
    except RuntimeError:
        spot_ids = ""

    if spot_ids and spot_ids != "None":
        ids = spot_ids.split()
        log(f"  Cancelling {len(ids)} spot requests: {ids}")
        try:
            aws_text([
                "ec2", "cancel-spot-instance-requests",
                "--spot-instance-request-ids", *ids,
            ], region=region)
        except RuntimeError as e:
            log(f"  Warning: cancel spot failed: {e}")
    else:
        log("  No spot requests to cancel.")

    # Step 2: Terminate instances by Project tag
    try:
        instance_ids = aws_text([
            "ec2", "describe-instances",
            "--filters",
            f"Name=tag:Project,Values={project_tag}",
            "Name=instance-state-name,Values=pending,running,stopping,stopped",
            "--query", "Reservations[].Instances[].InstanceId",
        ], region=region)
    except RuntimeError:
        instance_ids = ""

    if instance_ids and instance_ids != "None":
        ids = instance_ids.split()
        log(f"  Terminating {len(ids)} tagged instances: {ids}")
        try:
            aws_text([
                "ec2", "terminate-instances",
                "--instance-ids", *ids,
            ], region=region)
        except RuntimeError as e:
            log(f"  Warning: terminate failed: {e}")
    else:
        log("  No tagged instances to terminate.")

    # Step 3: Also terminate any untagged instances from the same key pair
    # (catches instances spawned by persistent spot requests after cleanup)
    try:
        untagged_ids = aws_text([
            "ec2", "describe-instances",
            "--filters",
            f"Name=key-name,Values={Path(args.ssh_key).stem}",
            "Name=instance-state-name,Values=pending,running,stopping,stopped",
            "--query", "Reservations[].Instances[?!Tags].InstanceId",
        ], region=region)
    except RuntimeError:
        untagged_ids = ""

    if untagged_ids and untagged_ids != "None":
        ids = [i for i in untagged_ids.split() if i not in (instance_ids or "").split()]
        if ids:
            log(f"  Terminating {len(ids)} untagged instances: {ids}")
            try:
                aws_text([
                    "ec2", "terminate-instances",
                    "--instance-ids", *ids,
                ], region=region)
            except RuntimeError as e:
                log(f"  Warning: terminate untagged failed: {e}")

    log("Cleanup done.")


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
                             "and HELIOS_PREFETCH_PLAN=1 env for the executor)")
    parser.add_argument("--perf", action="store_true", help="Enable perf profiling on lineairdb + mysql nodes")

    # AWS options
    parser.add_argument("--region", default=AWS_DEFAULTS["region"])
    parser.add_argument("--launch-template", default=AWS_DEFAULTS["launch_template"])
    parser.add_argument("--project-tag", default=AWS_DEFAULTS["project_tag"])
    parser.add_argument("--ssh-key", default=AWS_DEFAULTS["ssh_key"])
    parser.add_argument("--ssh-user", default=AWS_DEFAULTS["ssh_user"])
    parser.add_argument("--ami-id", default=AWS_DEFAULTS["ami_id"], help="AMI ID for on-demand fallback")
    parser.add_argument("--security-group", default=AWS_DEFAULTS["security_group"])
    parser.add_argument("--subnet", default=AWS_DEFAULTS["subnet"], help="Subnet ID (for AZ pinning)")

    # Cluster topology overrides
    parser.add_argument("--mysql-count", type=int, default=None, help="Override MySQL node count")
    parser.add_argument("--mysql-instance-type", default=None, help="Override MySQL instance type")
    parser.add_argument("--haproxy-instance-type", default=None, help="Override HAProxy instance type")
    parser.add_argument("--benchbase-count", type=int, default=None, help="Override BenchBase node count")
    parser.add_argument("--benchbase-instance-type", default=None, help="Override BenchBase instance type")
    # Control options
    parser.add_argument("--branch", default=None, help="Git branch to checkout on remote nodes (default: keep current)")
    parser.add_argument("--on-demand", action="store_true", help="Skip spot, launch all instances as on-demand")
    parser.add_argument("--cleanup-only", action="store_true", help="Only terminate instances, don't launch")
    parser.add_argument("--skip-cleanup", action="store_true", help="Don't terminate instances after benchmark")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be launched")

    args = parser.parse_args()
    os.chdir(ANSIBLE_DIR)

    # Apply cluster overrides before building machine_spec
    if args.mysql_count is not None:
        CLUSTER["mysql"]["count"] = args.mysql_count
    if args.mysql_instance_type is not None:
        CLUSTER["mysql"]["instance_type"] = args.mysql_instance_type
    if args.haproxy_instance_type is not None:
        CLUSTER["haproxy"]["instance_type"] = args.haproxy_instance_type
    if args.benchbase_count is not None:
        CLUSTER["benchbase"]["count"] = args.benchbase_count
    if args.benchbase_instance_type is not None:
        CLUSTER["benchbase"]["instance_type"] = args.benchbase_instance_type

    machine_spec = build_machine_spec()

    # Setup log directory: result/<run_id>/<machine_spec>/logs/
    global LOG_FILE
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    if args.bench_prefetch:
        run_id += "-prefetch"
    log_dir = ANSIBLE_DIR / "result" / run_id / machine_spec / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    LOG_FILE = open(log_dir / "bench_aws.log", "w")
    log(f"Log directory: {log_dir}")
    log(f"Machine spec: {machine_spec}")

    # Cleanup-only mode
    if args.cleanup_only:
        cleanup(args)
        LOG_FILE.close()
        return 0

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

    # Full run
    start_time = time.time()
    instance_ids = None

    try:
        # Phase 1: Launch
        instance_ids = launch_instances(args)
        if instance_ids is None:
            log("SPOT UNAVAILABLE — skipping benchmark.")
            return 1

        # Phase 2: Wait
        wait_for_instances(instance_ids, args.region)

        # Phase 3: Inventory + SSH
        generate_inventory(args)
        wait_for_ssh(args)

        # Phase 4: Deploy + Benchmark
        deploy_infrastructure(branch=args.branch)
        run_benchmarks(args, run_id)

        elapsed = time.time() - start_time
        log(f"All done in {elapsed / 60:.1f} minutes.")
        return 0

    except (RuntimeError, subprocess.CalledProcessError) as e:
        log(f"ERROR: {e}")
        return 1

    except KeyboardInterrupt:
        log("Interrupted by user.")
        return 1

    finally:
        if not args.skip_cleanup and instance_ids is not None:
            cleanup(args)
        if LOG_FILE:
            log(f"Full log saved to: {log_dir}")
            LOG_FILE.close()


if __name__ == "__main__":
    raise SystemExit(main())
