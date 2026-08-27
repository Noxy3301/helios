#!/usr/bin/env python3
import argparse
from datetime import datetime
from pathlib import Path
import re
import sys
from typing import Optional

import matplotlib.pyplot as plt

TIME_RE = re.compile(r"^(\d{1,2}:\d{2}:\d{2})(?:\s+(AM|PM))?\s+(\S+)\s+(.*)$")
ROLE_ORDER = {"lineairdb": 0, "mysql": 1, "benchbase": 2, "other": 3}
ROLE_STYLE = {
    "lineairdb": {"label": "LineairDB", "color": "#1b9e77"},
    "mysql": {"label": "MySQL", "color": "#d95f02"},
    "benchbase": {"label": "BenchBase", "color": "#e31a1c"},
    "other": {"label": "Other", "color": "#666666"},
}
LINESTYLES = ["-", "--", ":", "-."]


def role_for_host(host: str) -> str:
    if host.startswith("lineairdb"):
        return "lineairdb"
    if host.startswith("mysql"):
        return "mysql"
    if host.startswith("bench"):
        return "benchbase"
    return "other"


def sort_key(host: str) -> tuple:
    role = role_for_host(host)
    order = ROLE_ORDER.get(role, 99)
    m = re.match(r"^[A-Za-z]+-(\d+)$", host)
    num = int(m.group(1)) if m else 9999
    return (order, num, host)


def group_hosts(data):
    groups = {}
    for host in data:
        groups.setdefault(role_for_host(host), []).append(host)
    for hosts in groups.values():
        hosts.sort(key=sort_key)
    return groups


def average_series(data, hosts):
    if not hosts:
        return [], []
    series = [data[host] for host in hosts]
    time_sets = [set(item["times"]) for item in series]
    common_times = set.intersection(*time_sets)
    if not common_times:
        return [], []
    maps = [dict(zip(item["times"], item["usage"])) for item in series]
    avg_times = sorted(common_times)
    avg_usage = []
    for t in avg_times:
        vals = [series_map[t] for series_map in maps]
        avg_usage.append(sum(vals) / len(vals))
    return avg_times, avg_usage


def cpu_summary(data, hosts):
    counts = {}
    missing = 0
    for host in hosts:
        cpu = data[host].get("cpu_count")
        if cpu is not None:
            counts[cpu] = counts.get(cpu, 0) + 1
        else:
            missing += 1
    parts = [f"{cpu}CPU x{counts[cpu]}" for cpu in sorted(counts)]
    if missing:
        parts.append(f"unknown x{missing}")
    return ", ".join(parts)


def role_label_with_counts(role, data, hosts):
    label = ROLE_STYLE.get(role, ROLE_STYLE["other"])["label"]
    summary = cpu_summary(data, hosts)
    if summary:
        return f"{label} ({summary})"
    return label


def select_config_root(root: Path, run_id: Optional[str]) -> Path:
    if root.name == "cpu":
        return root.parent
    if (root / "cpu").is_dir():
        return root
    if not root.exists():
        return root
    configs = [path for path in root.iterdir() if path.is_dir()]
    if not configs:
        return root

    def latest_log_mtime(path: Path) -> float:
        cpu_root = path / "cpu"
        logs = list(cpu_root.glob("*/cpu-*.log"))
        if not logs:
            return path.stat().st_mtime
        return max(log.stat().st_mtime for log in logs)

    if run_id:
        matching = [path for path in configs if any((path / "cpu").glob(f"*/cpu-{run_id}.log"))]
        if matching:
            return max(matching, key=latest_log_mtime)
    return max(configs, key=latest_log_mtime)


def parse_time_token(token: str) -> int:
    for fmt in ("%H:%M:%S", "%I:%M:%S %p"):
        try:
            dt = datetime.strptime(token, fmt)
            return dt.hour * 3600 + dt.minute * 60 + dt.second
        except ValueError:
            continue
    raise ValueError(f"unrecognized time format: {token}")


def to_seconds(time_tokens):
    try:
        raw = [parse_time_token(t) for t in time_tokens]
    except ValueError:
        return list(range(len(time_tokens)))
    seconds = []
    offset = 0
    prev = None
    for sec in raw:
        if prev is not None and sec < prev:
            offset += 24 * 3600
        sec += offset
        seconds.append(sec)
        prev = sec
    t0 = seconds[0]
    return [s - t0 for s in seconds]


def parse_mpstat(path: Path):
    time_tokens = []
    usage = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Linux") or line.startswith("Average:"):
            continue
        if " CPU " in line and "%idle" in line:
            continue
        match = TIME_RE.match(line)
        if not match:
            continue
        time_str, ampm, cpu, rest = match.groups()
        if cpu != "all":
            continue
        fields = rest.split()
        if not fields:
            continue
        try:
            idle = float(fields[-1])
        except ValueError:
            continue
        token = time_str + (f" {ampm}" if ampm else "")
        time_tokens.append(token)
        usage.append(100.0 - idle)
    if not time_tokens:
        return [], []
    return to_seconds(time_tokens), usage


def parse_cpu_count(path: Path) -> Optional[int]:
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Linux"):
            match = re.search(r"\((\d+)\s+CPU", line)
            if match:
                return int(match.group(1))
        if TIME_RE.match(line):
            break
    return None


def pick_log(host_dir: Path, run_id: Optional[str]):
    if run_id:
        path = host_dir / f"cpu-{run_id}.log"
        return path if path.exists() else None
    logs = sorted(host_dir.glob("cpu-*.log"), key=lambda p: p.stat().st_mtime)
    return logs[-1] if logs else None


def load_series(root: Path, run_id: Optional[str]):
    data = {}
    for host_dir in sorted(root.iterdir()):
        if not host_dir.is_dir():
            continue
        log_path = pick_log(host_dir, run_id)
        if not log_path:
            print(f"skip {host_dir.name}: no log file", file=sys.stderr)
            continue
        times, usage = parse_mpstat(log_path)
        if not times:
            print(f"skip {host_dir.name}: no data in {log_path.name}", file=sys.stderr)
            continue
        cpu_count = parse_cpu_count(log_path)
        data[host_dir.name] = {
            "times": times,
            "usage": usage,
            "log": log_path,
            "cpu_count": cpu_count,
        }
    return data


def plot_all(data, output: Path, title: Optional[str]):
    fig, ax = plt.subplots(figsize=(12, 6))
    groups = group_hosts(data)
    ordered_roles = sorted(groups.keys(), key=lambda r: ROLE_ORDER.get(r, 99))
    for role in ordered_roles:
        color = ROLE_STYLE.get(role, ROLE_STYLE["other"])["color"]
        hosts = groups[role]
        for idx, host in enumerate(hosts):
            series = data[host]
            ax.plot(
                series["times"],
                series["usage"],
                label=f"_{host}",
                color=color,
                linestyle=LINESTYLES[idx % len(LINESTYLES)],
                linewidth=1,
                alpha=0.5,
            )
        avg_times, avg_usage = average_series(data, hosts)
        if avg_times:
            label = role_label_with_counts(role, data, hosts)
            ax.plot(
                avg_times,
                avg_usage,
                label=label,
                color=color,
                linewidth=2.5,
                alpha=0.9,
            )
    ax.set_xlabel("seconds")
    ax.set_ylabel("CPU usage (%)")
    if title:
        ax.set_title(title)
    ax.legend(loc="upper left", fontsize=9, framealpha=0.8)
    fig.tight_layout()
    fig.savefig(output, dpi=150)


def plot_roles(data, output: Path, title: Optional[str]):
    groups = group_hosts(data)
    ordered_roles = sorted(groups.keys(), key=lambda r: ROLE_ORDER.get(r, 99))
    fig, axes = plt.subplots(len(ordered_roles), 1, sharex=True, figsize=(12, 3 * len(ordered_roles)))
    if len(ordered_roles) == 1:
        axes = [axes]
    for ax, role in zip(axes, ordered_roles):
        color = ROLE_STYLE.get(role, ROLE_STYLE["other"])["color"]
        for idx, host in enumerate(groups[role]):
            series = data[host]
            ax.plot(
                series["times"],
                series["usage"],
                label=host,
                color=color,
                linestyle=LINESTYLES[idx % len(LINESTYLES)],
                linewidth=1,
                alpha=0.7,
            )
        avg_times, avg_usage = average_series(data, groups[role])
        if avg_times:
            ax.plot(
                avg_times,
                avg_usage,
                label="avg",
                color=color,
                linewidth=2.5,
                alpha=0.9,
            )
        ax.set_ylabel("CPU usage (%)")
        ax.set_title(role_label_with_counts(role, data, groups[role]))
        ax.legend(loc="upper left", fontsize=8, framealpha=0.8)
    axes[-1].set_xlabel("seconds (from first sample)")
    if title:
        fig.suptitle(title)
        fig.tight_layout(rect=[0, 0, 1, 0.95])
    else:
        fig.tight_layout()
    fig.savefig(output, dpi=150)


def main():
    parser = argparse.ArgumentParser(description="Plot mpstat CPU usage logs")
    parser.add_argument("--root", type=Path, default=None, help="root directory of result or a config/cpu dir")
    parser.add_argument("--run-id", type=str, default=None, help="run_id to load (cpu-<run_id>.log)")
    parser.add_argument("--output", type=Path, default=None, help="output image path")
    parser.add_argument("--mode", choices=["all", "roles"], default="all", help="plot mode")
    parser.add_argument("--title", type=str, default=None, help="plot title")
    args = parser.parse_args()

    if args.root is None:
        base = Path(__file__).resolve().parent.parent
        args.root = base / "result"

    selected_root = select_config_root(args.root, args.run_id)
    if selected_root != args.root:
        print(f"using config dir: {selected_root}")

    if not selected_root.exists():
        print(f"no data found under {args.root}", file=sys.stderr)
        return 1

    cpu_root = selected_root / "cpu"
    if not cpu_root.exists():
        print(f"no cpu logs under {selected_root}", file=sys.stderr)
        return 1

    config_name = selected_root.name
    if args.title is None:
        args.title = config_name.replace("_", " ")

    if args.output is None:
        suffix = f"-{args.run_id}" if args.run_id else ""
        plot_dir = selected_root / "_plot"
        plot_dir.mkdir(parents=True, exist_ok=True)
        args.output = plot_dir / f"cpu_plot-{config_name}{suffix}.png"

    data = load_series(cpu_root, args.run_id)
    if not data:
        print(f"no data found under {selected_root}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.mode == "roles":
        plot_roles(data, args.output, args.title)
    else:
        plot_all(data, args.output, args.title)

    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
