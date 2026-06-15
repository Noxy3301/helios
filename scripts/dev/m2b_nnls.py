#!/usr/bin/env python3
"""Phase-22 M2b (d)+(e): JOINT NNLS fit + identifiability gate.

Replaces the per-class OLS of m2b_fit.py with a single non-negative least-squares
fit over a stacked design matrix, plus a condition-number / VIF identifiability
gate (methodology element (e)). Reuses m2b_fit.parse / med_by for trace parsing.

Model (prefetch ON, server-side us, ns):
  us = C_rpc_fix * n_rpc(=1) + B_ship * bytes + S_scan * n_rows

secondary_flag (covering vs non-covering) is DROPPED -> 4-col reduces to 3-col.
This engine has NO covering execution (no HA_KEYREAD_ONLY, ha_lineairdb.hh:251 /
m2b_findings:30): cov vs noncov differ ONLY in shipped bytes, so secondary_flag is
collinear with the bytes column BY CONSTRUCTION. That is a FINDING (pre-declared in
docs/phase22_m2b_full_design.md (d)), not a fit failure -> we do not loop in (e).

Usage: m2b_nnls.py /tmp/m2b_struct.jsonl
"""
import sys
import numpy as np

# scipy.optimize.nnls if available; else a small projected-gradient NNLS fallback.
try:
    from scipy.optimize import nnls as _nnls
    def nnls(A, b):
        return _nnls(A, b)
except Exception:
    def nnls(A, b, iters=5000, lr=None):
        A = np.asarray(A, float); b = np.asarray(b, float)
        x = np.zeros(A.shape[1])
        AtA = A.T @ A; Atb = A.T @ b
        L = np.linalg.norm(AtA, 2)
        step = 1.0 / (L + 1e-12) if lr is None else lr
        for _ in range(iters):
            x = np.maximum(0.0, x - step * (AtA @ x - Atb))
        return x, float(np.linalg.norm(A @ x - b))

from m2b_fit import parse, med_by, DEF


def vif(A):
    """Variance Inflation Factor per column (collinearity diagnostic)."""
    out = []
    for j in range(A.shape[1]):
        y = A[:, j]
        X = np.delete(A, j, axis=1)
        if X.shape[1] == 0:
            out.append(1.0); continue
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)
        yhat = X @ coef
        ss_res = np.sum((y - yhat) ** 2); ss_tot = np.sum((y - y.mean()) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
        out.append(1.0 / (1.0 - r2) if r2 < 1 else float("inf"))
    return out


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/m2b_struct.jsonl"
    pts = parse(path)
    rows = []  # (label, n_rpc, bytes, n_rows, us_ns)
    for plan in ("point", "scan_n", "noncov", "cov"):
        for n, (us, rb) in med_by(pts, plan).items():
            n_rows = 1 if plan == "point" else n
            rows.append((plan, 1.0, float(rb), float(n_rows), us * 1000.0))  # us->ns
    if not rows:
        print("no probe points parsed (need m2b_taxonomy.sh trace first)", file=sys.stderr)
        sys.exit(1)
    A = np.array([[r[1], r[2], r[3]] for r in rows])  # [n_rpc=1, bytes, n_rows]
    b = np.array([r[4] for r in rows])
    cols = ["n_rpc", "bytes", "n_rows"]

    # --- (e) identifiability gate: cond + VIF on NON-CONSTANT (fittable) axes ---
    # n_rpc is fixed at 1 under prefetch's 1-RPC regime -> a constant INTERCEPT, not
    # a fittable axis (its std=0 blows cond to ~1e12 = the gate CORRECTLY flagging
    # C_rpc as non-identifiable from a fixed-n_rpc design). The real identifiability
    # question is whether the fittable axes (bytes, n_rows) are separable.
    nonconst = [j for j in range(A.shape[1]) if A[:, j].std() > 1e-9]
    csub = [cols[j] for j in nonconst]
    Asub = A[:, nonconst].astype(float)
    Astd = Asub / (Asub.std(axis=0) + 1e-12)
    cond = float(np.linalg.cond(Astd))
    vifs = vif(Asub)
    print(f"== (e) identifiability gate  [{len(rows)} pts; fittable axes {csub}; "
          "n_rpc fixed=1 -> intercept, non-identifiable by design] ==")
    print(f"  condition number (standardized) = {cond:.2f}   gate: <30")
    print("  VIF = " + ", ".join(f"{c}:{v:.1f}" for c, v in zip(csub, vifs)) + "   gate: <10")
    gate_ok = cond < 30 and max(vifs) < 10
    print(f"  GATE: {'PASS' if gate_ok else 'FAIL'}  (cond<30 AND maxVIF<10; fittable axes)")

    # --- (d) joint NNLS (intercept C_rpc + bytes + n_rows) ---
    coef, rnorm = nnls(A, b)
    C_rpc_ns, B_ship_ns, S_scan_ns = coef
    # C_rpc under prefetch = the fixed per-tx RPC overhead == the minimal-work probe
    # (point read, n_rows=1, minimal bytes). The joint intercept is swamped by the
    # large-n_rows scan rows, so report the direct point-read measurement too.
    pr = med_by(pts, "point")
    C_rpc_point_ns = (list(pr.values())[0][0] * 1000) if pr else float("nan")
    print("\n== (d) joint NNLS (non-negative, ns) ==")
    print(f"  B_ship = {B_ship_ns:.3f} ns/byte   S_scan = {S_scan_ns:.1f} ns/row   |resid| = {rnorm:.0f}")
    print(f"  C_rpc_fix: NNLS intercept = {C_rpc_ns:.0f} ns | point-read direct = {C_rpc_point_ns:.0f} ns")

    # --- secondary_flag / C_mat: NON-IDENTIFIABLE by construction (finding) ---
    noncov, cov = med_by(pts, "noncov"), med_by(pts, "cov")
    Rs = sorted(set(noncov) & set(cov))
    if Rs:
        dby = np.array([noncov[R][1] - cov[R][1] for R in Rs], float)
        dus = np.array([(noncov[R][0] - cov[R][0]) * 1000 for R in Rs], float)
        per_byte = dus.sum() / dby.sum() if dby.sum() else float("nan")
        print("\n== secondary_flag / C_materialise: NON-IDENTIFIABLE by construction (finding) ==")
        print(f"  cov vs noncov differ ONLY in bytes (no covering exec); per-byte delta = "
              f"{per_byte:.3f} ns/byte  ~= B_ship {B_ship_ns:.3f}")
        print("  -> C_materialise is NOT a separable column on this engine; the 8.0 default")
        print("     is a STEERING value (not fit from this physical matrix). [pre-registered]")

    # --- anchor to cost-units (S_scan = C_ROW + C_REMOTE) ---
    if S_scan_ns > 0:
        cu = (DEF["C_ROW"] + DEF["C_REMOTE"]) / S_scan_ns
        print(f"\n== physical vs default (cost-units; anchor S_scan = "
              f"{DEF['C_ROW'] + DEF['C_REMOTE']} cu) ==")
        for k, phys_ns in (("C_RPC", C_rpc_point_ns), ("C_BYTE", B_ship_ns)):
            p = phys_ns * cu
            print(f"  {k:8}: physical={p:.4g}  default={DEF[k]:.4g}  ratio(default/phys)={DEF[k]/p:.1f}x"
                  if p else f"  {k:8}: physical=~0")
    # JSON one-liner for the multi-SF stability driver to harvest
    print("\nNNLS_JSON " + __import__("json").dumps(
        {"cond": cond, "vif": dict(zip(csub, vifs)), "gate_ok": bool(gate_ok),
         "C_rpc_point_ns": float(C_rpc_point_ns), "B_ship_ns": float(B_ship_ns),
         "S_scan_ns": float(S_scan_ns), "resid": float(rnorm), "n_pts": len(rows)}))


if __name__ == "__main__":
    main()
