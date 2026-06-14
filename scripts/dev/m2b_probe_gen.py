#!/usr/bin/env python3
"""Phase-22 M2b: generate calibration probe tables (Wu calibration-query method).

Emits CREATE + batched multi-row INSERT SQL on stdout for:
  cal_n_<N>  (pk BIGINT PK, k INT, KEY sk(k))                 narrow, sizes via --sizes
  cal_w_<N>  (pk BIGINT PK, k INT, pad VARCHAR(512), KEY sk(k))  wide

Invariants for clean calibration:
  - pk = 1..N dense; k = pk (dense-unique) so `k BETWEEN 1 AND R` returns EXACTLY R rows.
  - pad = HIGH-ENTROPY 512 chars derived from pk, to DEFEAT the projection-pushdown
    LZ4 compression (identical pad would compress to ~0 and corrupt the C_byte/resp_b
    measurement). See helios-projection-pushdown.
Pipe to: build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock benchbase
"""
import sys, argparse, hashlib

def pad_for(pk: int) -> str:
    # 512 high-entropy chars: chain md5 hexdigests (32 chars each) -> 16*32=512
    out = []
    seed = pk
    while len(out) * 32 < 512:
        out.append(hashlib.md5(f"{seed}:{len(out)}".encode()).hexdigest())
        seed = seed * 1103515245 + 12345
    return "".join(out)[:512]

def kval(p, n, shuffle):
    # shuffle: k = LCG permutation of 1..n so 'k BETWEEN 1 AND R' selects R rows whose
    # base PKs are SCATTERED (genuine random materialise), removing the k=pk contiguity
    # confound (review r4). P coprime to n; (p-1)*P mod n is a bijection on 0..n-1.
    if not shuffle:
        return p
    P = 99991  # prime; coprime to the n's used (powers of 10)
    return ((p - 1) * P) % n + 1

def emit_table(name, n, wide, batch, shuffle):
    cols = "(pk BIGINT PRIMARY KEY, k INT NOT NULL, pad VARCHAR(512) NOT NULL, KEY sk (k))" if wide \
           else "(pk BIGINT PRIMARY KEY, k INT NOT NULL, KEY sk (k))"
    print(f"DROP TABLE IF EXISTS {name};")
    print(f"CREATE TABLE {name} {cols};")
    i = 1
    while i <= n:
        hi = min(i + batch - 1, n)
        if wide:
            vals = ",".join(f"({p},{kval(p,n,shuffle)},'{pad_for(p)}')" for p in range(i, hi + 1))
            print(f"INSERT INTO {name} (pk,k,pad) VALUES {vals};")
        else:
            vals = ",".join(f"({p},{kval(p,n,shuffle)})" for p in range(i, hi + 1))
            print(f"INSERT INTO {name} (pk,k) VALUES {vals};")
        i = hi + 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=["n", "w"], required=True)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--shuffle", action="store_true", help="scatter base PKs (k=LCG perm) to remove the k=pk contiguity confound")
    a = ap.parse_args()
    name = f"cal_{a.kind}_{'shuf_' if a.shuffle else ''}{a.n}"
    emit_table(name, a.n, wide=(a.kind == "w"), batch=a.batch, shuffle=a.shuffle)

if __name__ == "__main__":
    main()
