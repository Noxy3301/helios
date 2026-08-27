#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROLE_MAP = [
    {"tag": "helios-lineairdb", "group": "lineairdb", "prefix": "lineairdb"},
    {"tag": "helios-mysql", "group": "mysql", "prefix": "mysql"},
    {"tag": "helios-bench", "group": "benchbase", "prefix": "bench"},
]
TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "inventory.ini.template"


def run_aws(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr.strip() or "aws command failed", file=sys.stderr)
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"failed to parse aws output: {exc}", file=sys.stderr)
        return None


def build_inventory(instances, args, header_lines=None, group_order=None):
    by_tag = {}
    for inst in instances:
        name = inst.get("Name")
        if not name:
            continue
        by_tag.setdefault(name, []).append(inst)

    lines = header_lines[:] if header_lines else []
    if not lines:
        lines.append("[all:vars]")
        lines.append(f"ansible_user={args.user}")
        lines.append(f"ansible_ssh_private_key_file={args.key}")

    if not group_order:
        group_order = [role["group"] for role in ROLE_MAP]

    role_by_group = {role["group"]: role for role in ROLE_MAP}
    for group in group_order:
        role = role_by_group.get(group)
        if role is None:
            continue
        prefix = role["prefix"]
        tag = role["tag"]
        entries = []
        for t, insts in by_tag.items():
            if t == tag or t.startswith(tag + "-"):
                entries.extend(insts)
        entries.sort(key=lambda item: item.get("PrivIP") or "")
        if lines and lines[-1] != "":
            lines.append("")
        lines.append(f"[{group}]")
        for idx, inst in enumerate(entries, start=1):
            private_ip = inst.get("PrivIP")
            public_ip = inst.get("PubIP")
            if args.ansible_host == "private":
                ansible_host = private_ip or public_ip
            else:
                ansible_host = public_ip or private_ip
            if not ansible_host:
                print(f"skip {tag} missing IP: {inst}", file=sys.stderr)
                continue
            host = f"{prefix}-{idx}"
            lines.append(f"{host} ansible_host={ansible_host} private_ip={private_ip}")

    return "\n".join(lines) + "\n"


def load_template(args):
    if not TEMPLATE_PATH.exists():
        return None, None
    header_lines = []
    group_order = []
    in_header = True
    for raw_line in TEMPLATE_PATH.read_text().splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            group = stripped[1:-1].strip()
            if group != "all:vars":
                in_header = False
                if group not in group_order:
                    group_order.append(group)
            if in_header:
                header_lines.append(line)
            continue
        if in_header:
            if line.startswith("ansible_user="):
                header_lines.append(f"ansible_user={args.user}")
            elif line.startswith("ansible_ssh_private_key_file="):
                header_lines.append(f"ansible_ssh_private_key_file={args.key}")
            else:
                header_lines.append(line)
    if header_lines and header_lines[-1] != "":
        header_lines.append("")
    return header_lines, group_order


def main():
    parser = argparse.ArgumentParser(description="Generate ansible/inventory.ini from AWS tags")
    parser.add_argument("--region", type=str, default="ap-southeast-2", help="AWS region")
    parser.add_argument("--profile", type=str, default=None, help="AWS profile (optional)")
    parser.add_argument("--project-tag", type=str, default="Helios", help="Project tag value")
    parser.add_argument("--run-tag", type=str, default=None,
                        help="Filter to instances tagged Run=<value> (scopes one campaign)")
    parser.add_argument("--user", type=str, default="ubuntu", help="ansible_user")
    parser.add_argument("--key", type=str, default="~/.ssh/helios-aws.pem", help="ssh private key path")
    parser.add_argument(
        "--ansible-host",
        choices=["public", "private"],
        default="public",
        help="which IP to set for ansible_host",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "inventory.ini",
        help="output inventory path",
    )
    args = parser.parse_args()

    tags = [role["tag"] for role in ROLE_MAP]
    filters = ["Name=instance-state-name,Values=running"]
    if args.project_tag:
        filters.append(f"Name=tag:Project,Values={args.project_tag}")
    if args.run_tag:
        filters.append(f"Name=tag:Run,Values={args.run_tag}")
    # Use wildcard to match both "helios-mysql" and "helios-mysql-1" etc.
    tag_patterns = [f"{t},{t}-*" for t in tags]
    filters.append("Name=tag:Name,Values=" + ",".join(tag_patterns))

    cmd = [
        "aws",
        "ec2",
        "describe-instances",
        "--filters",
        *filters,
        "--query",
        "Reservations[].Instances[].{Name:Tags[?Key=='Name']|[0].Value,PrivIP:PrivateIpAddress,PubIP:PublicIpAddress}",
        "--output",
        "json",
    ]
    if args.region:
        cmd.extend(["--region", args.region])
    if args.profile:
        cmd.extend(["--profile", args.profile])

    data = run_aws(cmd)
    if data is None:
        return 1

    header_lines, group_order = load_template(args)
    inventory = build_inventory(data, args, header_lines=header_lines, group_order=group_order)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(inventory)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
