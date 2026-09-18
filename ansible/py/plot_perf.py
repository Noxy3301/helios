#!/usr/bin/env python3
"""
Parse perf report --stdio output and generate breakdown charts.
Usage: python3 plot_perf.py <perf-report.txt> [--output <output.png>]
"""

import argparse
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")


def parse_perf_report(path):
    """Parse perf report --stdio --no-children -g none output."""
    entries = []
    with open(path) as f:
        for line in f:
            m = re.match(r'\s+(\d+\.\d+)%\s+\S+\s+(\S+)\s+\[([.\w])\]\s+(.*)', line)
            if m:
                pct = float(m.group(1))
                lib = m.group(2)
                kind = m.group(3)
                symbol = m.group(4).strip()
                entries.append((pct, lib, kind, symbol))
    return entries


SCHED_YIELD_SYMBOLS = {
    "clear_bhb_loop", "entry_SYSCALL_64", "update_curr", "__schedule",
    "pvclock_clocksource_read_nowd", "__calc_delta.constprop.0",
    "pick_task_fair", "update_min_vruntime", "__sched_yield",
    "arch_exit_to_user_mode_prepare.isra.0", "pick_eevdf",
    "update_curr_se", "do_syscall_64", "its_return_thunk",
    "syscall_return_via_sysret", "native_queued_spin_lock_slowpath",
    "update_rq_clock", "do_sched_yield", "x64_sys_call",
    "syscall_exit_to_user_mode", "entry_SYSCALL_64_after_hwframe",
    "raw_spin_rq_lock_nested", "pick_next_task_fair", "cpuacct_charge",
    "__pick_next_task", "pick_next_task", "syscall_exit_to_user_mode_prepare",
    "native_write_msr", "cgroup_rstat_updated", "schedule",
    "fpregs_assert_state_consistent", "update_curr_dl_se",
    "raw_spin_rq_unlock_irq", "__cgroup_account_cputime",
    "entry_SYSCALL_64_safe_stack", "rcu_note_context_switch",
    "vruntime_eligible", "sched_clock_cpu", "available_idle_cpu",
    "yield_task_fair", "kvm_sched_clock_read", "dl_server_update",
    "update_sg_lb_stats", "psi_group_change", "entry_SYSRETQ_unsafe_stack",
    "rcu_qs", "__x64_sys_sched_yield", "sched_clock",
    "raw_spin_rq_unlock", "restore_fpregs_from_fpstate",
    "sched_clock_noinstr", "read_tsc", "dequeue_entity",
    "__update_load_avg_se", "finish_task_switch.isra.0",
    "__update_load_avg_cfs_rq", "dequeue_entities",
    "sched_update_worker", "update_load_avg",
    "__raw_spin_lock_irqsave", "_raw_spin_lock_bh",
    "_raw_spin_lock", "arch_scale_cpu_capacity",
}

NETWORK_SYMBOLS_SUBSTR = [
    "tcp_", "ena_", "recv", "send", "skb_", "ip_finish",
    "__dev_xmit", "vfs_writev", "fdget", "__sys_recvfrom",
    "__sys_sendto", "softirq", "aa_inet_msg_perm",
    "kmem_cache_alloc", "skb_release", "check_heap_object",
    "ip_output", "__irqentry",
]

SCHED_SUBCATEGORIES = {
    "CFS scheduler": {
        "update_curr", "__schedule", "__calc_delta.constprop.0",
        "pick_task_fair", "update_min_vruntime", "pick_eevdf",
        "update_curr_se", "pick_next_task_fair", "__pick_next_task",
        "pick_next_task", "update_curr_dl_se", "vruntime_eligible",
        "yield_task_fair", "dequeue_entity", "dequeue_entities",
        "update_load_avg", "__update_load_avg_se", "__update_load_avg_cfs_rq",
        "dl_server_update", "update_sg_lb_stats", "schedule",
    },
    "syscall entry": {
        "clear_bhb_loop", "entry_SYSCALL_64", "entry_SYSCALL_64_after_hwframe",
        "entry_SYSCALL_64_safe_stack", "do_syscall_64", "x64_sys_call",
        "__x64_sys_sched_yield", "do_sched_yield",
    },
    "syscall return": {
        "arch_exit_to_user_mode_prepare.isra.0", "syscall_return_via_sysret",
        "syscall_exit_to_user_mode", "syscall_exit_to_user_mode_prepare",
        "entry_SYSRETQ_unsafe_stack", "its_return_thunk",
        "restore_fpregs_from_fpstate", "fpregs_assert_state_consistent",
    },
    "run-queue spinlock": {
        "_raw_spin_lock", "native_queued_spin_lock_slowpath",
        "raw_spin_rq_lock_nested", "raw_spin_rq_unlock_irq",
        "raw_spin_rq_unlock", "_raw_spin_lock_bh",
        "__raw_spin_lock_irqsave",
    },
    "clock read": {
        "pvclock_clocksource_read_nowd", "update_rq_clock",
        "kvm_sched_clock_read", "sched_clock_cpu", "sched_clock",
        "sched_clock_noinstr", "read_tsc",
    },
    "accounting": {
        "cpuacct_charge", "__cgroup_account_cputime", "cgroup_rstat_updated",
        "native_write_msr", "rcu_note_context_switch", "rcu_qs",
        "psi_group_change", "available_idle_cpu", "sched_update_worker",
        "finish_task_switch.isra.0", "arch_scale_cpu_capacity",
    },
}


def classify(symbol, lib, kind):
    if symbol == "__sched_yield":
        return "sched_yield"
    clean = symbol.split("(")[0].strip()
    if clean in SCHED_YIELD_SYMBOLS:
        return "sched_yield"
    if lib == "libc.so.6" and symbol.startswith("0x"):
        return "sched_yield"
    for substr in NETWORK_SYMBOLS_SUBSTR:
        if substr in symbol.lower():
            return "network"
    if kind == "k":
        return "kernel_other"
    if "jemalloc" in lib:
        return "alloc"
    if "protobuf" in lib:
        return "protobuf"
    if lib == "helios-storage":
        return "storage"
    if "libstdc++" in lib:
        return "libstdcpp"
    return "other_user"


def classify_sched_sub(symbol):
    clean = symbol.split("(")[0].strip()
    for subcat, syms in SCHED_SUBCATEGORIES.items():
        if clean in syms:
            return subcat
    if symbol == "__sched_yield":
        return "__sched_yield (libc)"
    return "other"


# --- Proxy-specific classification ---
PROXY_CATEGORIES = {
    "context_switch": {"finish_task_switch.isra.0"},
    "mysql_parser": {"MYSQLparse", "lex_one_token", "Lex_hash::get_hash_symbol",
                     "digest_add_token", "MYSQLlex"},
    "mysql_optimizer": {"fold_condition", "JOIN::optimize", "open_table", "open_tables",
                        "find_field_in_table", "setup_fields", "Item_field::set_field",
                        "Item_field::fix_fields", "Item::itemize",
                        "Query_block::add_table_to_list", "check_stack_overrun",
                        "dispatch_sql_command", "Sql_cmd_dml::execute",
                        "Sql_cmd_update::update_single_table", "mysql_execute_command"},
    "mysql_executor": {"dispatch_command", "THD::store_cached_properties",
                       "pfs_end_statement_vc", "cleanup_items",
                       "insert_events_statements_history", "pfs_start_stage_v1",
                       "pfs_start_statement_vc", "THD::enter_stage",
                       "decimal2bin", "my_well_formed_len_utf8mb4",
                       "PFS_buffer_scalable_container"},
    "mysql_other": {"my_lfind", "my_strcasecmp_utf8mb3", "my_malloc", "my_free"},
}


def classify_proxy(symbol, lib, kind):
    clean = symbol.split("(")[0].strip()
    # ha_helios
    if "ha_helios" in lib:
        return "ha_helios"
    # protobuf
    if "protobuf" in lib:
        return "protobuf"
    # jemalloc
    if "jemalloc" in lib:
        return "alloc"
    # Network
    for substr in NETWORK_SYMBOLS_SUBSTR:
        if substr in symbol.lower():
            return "network"
    # Proxy-specific categories
    for cat, syms in PROXY_CATEGORIES.items():
        for s in syms:
            if s in clean:
                return cat
    # pthread
    if "pthread" in symbol:
        return "pthread"
    # kernel
    if kind == "k":
        return "kernel_other"
    # libstdc++
    if "libstdc++" in lib:
        return "libstdcpp"
    # mysqld binary (uncategorized)
    if lib == "mysqld":
        return "mysql_other"
    return "other_user"


CATEGORY_LABELS = {
    "sched_yield": "sched_yield (EpochFramework Sync)",
    "network": "Network (TCP + ENA)",
    "kernel_other": "Kernel (other)",
    "storage": "Helios processing",
    "alloc": "Allocator (jemalloc)",
    "protobuf": "Protobuf",
    "libstdcpp": "libstdc++",
    "other_user": "Other",
    "unresolved": "Other (unresolved)",
    # proxy
    "ha_helios": "ha_helios (SE plugin)",
    "mysql_parser": "MySQL parser + lexer",
    "mysql_optimizer": "MySQL optimizer",
    "mysql_executor": "MySQL executor + PFS",
    "mysql_other": "MySQL other",
    "context_switch": "Context switch",
    "pthread": "pthread",
}

CATEGORY_COLORS = {
    "sched_yield": "#e74c3c",
    "network": "#3498db",
    "kernel_other": "#95a5a6",
    "storage": "#2ecc71",
    "alloc": "#f39c12",
    "protobuf": "#9b59b6",
    "libstdcpp": "#1abc9c",
    "other_user": "#bdc3c7",
    "unresolved": "#ecf0f1",
    # proxy
    "ha_helios": "#2ecc71",
    "mysql_parser": "#e67e22",
    "mysql_optimizer": "#d35400",
    "mysql_executor": "#e74c3c",
    "mysql_other": "#f39c12",
    "context_switch": "#c0392b",
    "pthread": "#8e44ad",
}

SCHED_SUB_COLORS = {
    "CFS scheduler": "#c0392b",
    "syscall entry": "#e74c3c",
    "syscall return": "#ff6b6b",
    "run-queue spinlock": "#ee5a24",
    "clock read": "#f8a5a5",
    "accounting": "#fab1a0",
    "__sched_yield (libc)": "#d63031",
    "other": "#e17055",
}


def main():
    parser = argparse.ArgumentParser(description="Plot perf report breakdown")
    parser.add_argument("input", help="perf report --stdio text file")
    parser.add_argument("--output", "-o", default=None, help="Output PNG path")
    parser.add_argument("--vcpu", type=int, default=64, help="vCPU count")
    parser.add_argument("--mode", default="server", choices=["server", "proxy"],
                        help="server (storage) or proxy (mysqld)")
    args = parser.parse_args()

    entries = parse_perf_report(args.input)
    if not entries:
        print("No entries parsed.", file=sys.stderr)
        return 1

    classifier = classify if args.mode == "server" else classify_proxy

    # Aggregate
    categories = {}
    sched_subs = {}
    for pct, lib, kind, symbol in entries:
        cat = classifier(symbol, lib, kind)
        categories[cat] = categories.get(cat, 0.0) + pct
        if cat == "sched_yield":
            sub = classify_sched_sub(symbol)
            sched_subs[sub] = sched_subs.get(sub, 0.0) + pct

    # Add unresolved remainder to reach 100%
    total_parsed = sum(categories.values())
    if total_parsed < 100.0:
        categories["unresolved"] = 100.0 - total_parsed

    ordered = sorted(categories.items(), key=lambda x: -x[1])
    sched_ordered = sorted(sched_subs.items(), key=lambda x: -x[1])
    has_sched = "sched_yield" in categories and categories["sched_yield"] > 5

    # Print table
    total = sum(v for _, v in ordered)
    print(f"{'Category':<30} {'CPU%':>8} {'vCPU':>8}")
    print("-" * 48)
    for cat, pct in ordered:
        vcpu = args.vcpu * pct / 100
        print(f"{cat:<30} {pct:>7.1f}% {vcpu:>7.1f}")
        if cat == "sched_yield":
            for sub, spct in sched_ordered:
                svcpu = args.vcpu * spct / 100
                print(f"  {sub:<28} {spct:>7.1f}% {svcpu:>7.1f}")
    print("-" * 48)
    print(f"{'Total':<30} {total:>7.1f}% {args.vcpu * total / 100:>7.1f}")

    # --- Figure ---
    ncols = 2 if has_sched else 1
    fig, axes = plt.subplots(1, ncols + 1, figsize=(6 * (ncols + 1), 6),
                             gridspec_kw={"width_ratios": [1.2] + [1] * ncols})

    # Panel 1: Pie chart (categories)
    ax_pie = axes[0]
    labels = [CATEGORY_LABELS.get(c, c) for c, _ in ordered]
    sizes = [v for _, v in ordered]
    colors = [CATEGORY_COLORS.get(c, "#cccccc") for c, _ in ordered]

    wedges, texts, autotexts = ax_pie.pie(
        sizes, labels=None,
        autopct=lambda p: '',  # we add custom labels below
        colors=colors, startangle=90, pctdistance=0.78,
        textprops={"fontsize": 10, "fontweight": "bold"},
        wedgeprops=dict(edgecolor="white", linewidth=1.5),
    )
    # Add original % as text on each wedge
    for wedge, pct in zip(wedges, sizes):
        if pct < 2:
            continue
        ang = (wedge.theta2 + wedge.theta1) / 2
        import numpy as np
        x = 0.65 * np.cos(np.deg2rad(ang))
        y = 0.65 * np.sin(np.deg2rad(ang))
        ax_pie.text(x, y, f"{pct:.1f}%", ha="center", va="center",
                    fontsize=9, fontweight="bold",
                    color="white" if pct > 10 else "black")
    ax_pie.legend(wedges, [f"{l} ({s:.1f}%)" for l, s in zip(labels, sizes)],
                  loc="center left", bbox_to_anchor=(-0.45, 0.5), fontsize=7.5)

    mode_label = "Helios server" if args.mode == "server" else "MySQL Proxy"
    ax_pie.set_title(f"{mode_label}\nCPU Breakdown ({args.vcpu} vCPU)", fontsize=12)

    # Panel 2: Top functions bar chart (all categories)
    ax_top = axes[1]
    top_n = 20
    top_entries = sorted(entries, key=lambda x: -x[0])[:top_n]
    func_names = []
    func_pcts = []
    func_colors = []
    for pct, lib, kind, symbol in top_entries:
        cat = classifier(symbol, lib, kind)
        # Shorten symbol name
        name = symbol.split("(")[0].strip()
        if len(name) > 35:
            name = name[:32] + "..."
        func_names.append(name)
        func_pcts.append(pct)
        func_colors.append(CATEGORY_COLORS.get(cat, "#cccccc"))

    bars = ax_top.barh(range(len(func_names) - 1, -1, -1), func_pcts[::-1],
                       color=func_colors[::-1], edgecolor="white", linewidth=0.5)
    ax_top.set_yticks(range(len(func_names) - 1, -1, -1))
    ax_top.set_yticklabels(func_names[::-1], fontsize=7)
    ax_top.set_xlabel("CPU %", fontsize=10)
    ax_top.set_title(f"Top {top_n} Functions", fontsize=12)
    for bar, pct in zip(bars, func_pcts[::-1]):
        if pct > 0.5:
            ax_top.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2,
                        f"{pct:.1f}%", va="center", fontsize=7)

    # Panel 3: sched_yield sub-breakdown (server only)
    if has_sched:
        ax_sub = axes[2]
        sub_names = [s for s, _ in sched_ordered]
        sub_pcts = [p for _, p in sched_ordered]
        sub_vcpus = [args.vcpu * p / 100 for p in sub_pcts]
        sub_colors = [SCHED_SUB_COLORS.get(s, "#e74c3c") for s in sub_names]

        bars = ax_sub.barh(range(len(sub_names) - 1, -1, -1), sub_vcpus[::-1],
                           color=sub_colors[::-1], edgecolor="white", linewidth=0.5)
        ax_sub.set_yticks(range(len(sub_names) - 1, -1, -1))
        ax_sub.set_yticklabels(sub_names[::-1], fontsize=9)
        ax_sub.set_xlabel("vCPU", fontsize=10)
        ax_sub.set_title(f"sched_yield Breakdown\n({categories['sched_yield']:.1f}% = {args.vcpu * categories['sched_yield'] / 100:.1f} vCPU)",
                         fontsize=12, color="#c0392b")
        for bar, v, p in zip(bars, sub_vcpus[::-1], sub_pcts[::-1]):
            if v > 0.5:
                ax_sub.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height() / 2,
                            f"{v:.1f} vCPU ({p:.1f}%)", va="center", fontsize=8)

    plt.tight_layout()
    output = args.output or str(Path(args.input).with_suffix(".png"))
    plt.savefig(output, dpi=150, bbox_inches="tight")
    print(f"\nSaved: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
