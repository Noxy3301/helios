#!/usr/bin/env python3
"""Phase-22 M2b calibration fit (design v4 §5).

Parses the structure-experiment RPC trace (server-side TX_EXECUTE_READ_PLAN.us +
resp_b per probe), fits the PHYSICAL per-row/per-byte/fixed-RPC constants under
prefetch ON, anchors them to the cost-model's units, and reports the gap vs the
current env defaults and the predicted access crossover s*.

Physical model (prefetch ON, server-side, ns):
  full scan        : us ≈ C_rpc_fix + S_scan·N + B_ship·bytes_shipped
  non-cov 2ary range: us ≈ C_rpc_fix + (S_scan + C_mat)·R + B_ship·bytes_shipped
where C_mat = the RANDOM PK-materialise premium over a sequential scan (the
PG random_page_cost analog), the quantity the cost model's HELIOS_C_MATERIALISE
is meant to capture.

Usage: m2b_fit.py /tmp/m2b_struct.jsonl
"""
import sys, json, re
import numpy as np

# --- current env defaults (cost-units) the fit is compared against ---
DEF = dict(C_RPC=50.0, C_BYTE=0.0008, C_ROW=0.10, C_REMOTE=0.05, C_PROBE=0.05,
           C_MATERIALISE=8.0, BATCH=1024.0)

def parse(path):
    pts = []  # (plan, n, us, resp_b)  plan in {noncov,cov,fullscan_filter,scan_n,point}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            tx = json.loads(line)
        except Exception:
            continue
        sql = tx.get("statements", [{}])[0].get("sql", "")
        ep = tx.get("summary_by_type", {}).get("TX_EXECUTE_READ_PLAN")
        if not ep:
            continue
        us, rb = ep["us"], ep["resp_b"]
        if "pk=42" in sql.replace(" ", ""):
            pts.append(("point", 1, us, rb)); continue
        mN = re.search(r'cal_n_(\d+) IGNORE', sql)
        if mN and re.search(r'SELECT\s+k\b', sql):
            pts.append(("scan_n", int(mN.group(1)), us, rb)); continue
        mR = re.search(r'BETWEEN 1 AND (\d+)', sql)
        if not mR:
            continue
        R = int(mR.group(1))
        if "IGNORE INDEX" in sql:
            pts.append(("fullscan_filter", R, us, rb))
        elif re.search(r'SELECT\s+pad', sql):
            pts.append(("noncov", R, us, rb))
        elif re.search(r'SELECT\s+k\b', sql):
            pts.append(("cov", R, us, rb))
    return pts

def med_by(pts, plan):
    d = {}
    for p, n, us, rb in pts:
        if p == plan:
            d.setdefault(n, []).append((us, rb))
    return {n: (float(np.median([u for u, _ in v])), float(np.median([b for _, b in v]))) for n, v in d.items()}

def slope_ci(xs, ys):
    """OLS slope + jackknife-over-design-points 95% CI (ns/unit). Returns (slope, lo, hi, intercept)."""
    xs = np.array(xs, float); ys = np.array(ys, float)
    def fit(x, y):
        A = np.vstack([x, np.ones_like(x)]).T
        m, c = np.linalg.lstsq(A, y, rcond=None)[0]
        return m, c
    m, c = fit(xs, ys)
    jk = []
    for i in range(len(xs)):
        mask = np.arange(len(xs)) != i
        if mask.sum() >= 2:
            jk.append(fit(xs[mask], ys[mask])[0])
    jk = np.array(jk)
    if len(jk):
        se = np.sqrt((len(jk) - 1) / len(jk) * np.sum((jk - jk.mean()) ** 2))
        lo, hi = m - 1.96 * se, m + 1.96 * se
    else:
        lo = hi = m
    return m, lo, hi, c

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/m2b_struct.jsonl"
    pts = parse(path)
    scan = med_by(pts, "scan_n")          # cal_n narrow full scan: isolates S_scan
    noncov = med_by(pts, "noncov")        # cal_w non-cov range
    cov = med_by(pts, "cov")              # cal_w cov (k) range
    fulls = med_by(pts, "fullscan_filter")# cal_w full scan + filter
    point = med_by(pts, "point")          # 1-row staged point read

    print("== measured server-side us (median) ==")
    for name, d in [("scan_n", scan), ("noncov", noncov), ("cov", cov), ("fullscan_filter", fulls)]:
        print(f"  {name}: " + ", ".join(f"{n}:{us:.0f}us/{rb}B" for n, (us, rb) in sorted(d.items())))
    if point:
        print(f"  point(1row): {list(point.values())[0][0]:.0f}us")

    # S_scan (ns/row) from cal_n narrow N-sweep
    Ns = sorted(scan)
    S_scan, sl, sh, scan_int = slope_ci(Ns, [scan[n][0] * 1000 for n in Ns])  # us->ns
    print(f"\nS_scan (sequential scan, cal_n narrow) = {S_scan:.1f} ns/row  [95% {sl:.0f},{sh:.0f}]  intercept={scan_int:.0f}ns")

    # B_ship (ns/byte): from cov vs noncov at matched R (same row count, byte differs)
    Rs = sorted(set(noncov) & set(cov))
    dby = []; dus = []
    for R in Rs:
        dby.append(noncov[R][1] - cov[R][1]); dus.append((noncov[R][0] - cov[R][0]) * 1000)
    B_ship = np.sum(np.array(dus)) / np.sum(np.array(dby))
    print(f"B_ship (server ship) = {B_ship:.3f} ns/byte  (from noncov-cov byte delta)")

    # non-cov range per-row (ns/row) and fullscan ship per-row
    nc_slope, nlo, nhi, _ = slope_ci(Rs, [noncov[R][0] * 1000 for R in Rs])
    Rf = sorted(fulls)
    fs_slope, flo, fhi, fs_int = slope_ci(Rf, [fulls[R][0] * 1000 for R in Rf])  # marginal ship per extra row
    print(f"non-cov range per-row = {nc_slope:.1f} ns/row  [95% {nlo:.0f},{nhi:.0f}]")
    print(f"fullscan marginal ship per-row = {fs_slope:.1f} ns/row ; fullscan fixed (scan-all) intercept = {fs_int:.0f}ns")

    # C_mat = random materialise premium = non-cov per-row - fullscan ship per-row
    C_mat_ns = nc_slope - fs_slope
    print(f"\n*** C_mat (random materialise premium) = noncov_per_row - fullscan_ship_per_row = {C_mat_ns:.1f} ns/row ***")

    # --- anchor: model table_scan cpu = N*(C_ROW+C_REMOTE) <-> measured S_scan ---
    cu_per_ns = (DEF["C_ROW"] + DEF["C_REMOTE"]) / S_scan   # cost-units per ns
    print(f"\n== anchor (S_scan = C_ROW+C_REMOTE = {DEF['C_ROW']+DEF['C_REMOTE']} cu) ==")
    print(f"  1 ns = {cu_per_ns:.6e} cu   (1 cu = {1/cu_per_ns:.0f} ns)")
    phys = {
        "C_BYTE":        B_ship * cu_per_ns,
        "C_MATERIALISE": C_mat_ns * cu_per_ns,
        "C_RPC":         (list(point.values())[0][0] * 1000 * cu_per_ns) if point else float('nan'),
    }
    print("\n== physical constant vs current default (cost-units) ==")
    for k in ["C_RPC", "C_BYTE", "C_MATERIALISE"]:
        d = DEF[k]; p = phys[k]
        print(f"  {k:14}: physical={p:.4g}  default={d:.4g}  ratio(default/phys)={d/p:.1f}x")

    # predicted single-table access crossover s* (range vs fullscan), physical vs default C_mat
    # range cost ~ R*(S_ref+C_mat+byte); fullscan ~ N*S_scan + R*byte. crossover s*=R/N:
    # s* solving R*(per_row_range) = N*S_scan + R*ship  -> s* = S_scan / (range_extra)
    # measured single-table crossover (server us): noncov_per_row vs fullscan(scan N + ship R)
    # s*_meas = where noncov_per_row*R = fs_int(scanN ~ for N=100000) + fs_slope*R
    Ntab = 100000
    s_meas = fs_int / ((nc_slope - fs_slope) * Ntab) if (nc_slope - fs_slope) > 0 else float('nan')
    print(f"\n== access crossover s* (single-table, N={Ntab}) ==")
    print(f"  measured s*_access = fullscan_fixed / ((noncov-fullscan_ship)*N) = {s_meas*100:.1f}% selectivity")
    print("  (model with physical C_mat should predict ~this; with default 8.0 it predicts a far LOWER s* = over-penalises range)")

if __name__ == "__main__":
    main()
