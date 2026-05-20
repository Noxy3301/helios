#!/usr/bin/env python3
"""
Patch and build BenchBase from third_party/benchbase for Helios.

Applies:
  1. Java version downgrade (23 -> 21) in pom.xml
  2. OCC commit-time deadlock retry in Worker.java (error 1180 + "Got error 149")
  3. Batch-level retry in TPCCLoader.java loadStock() for parallel loading

Usage:
  python3 bench/bin/patch_benchbase.py          # patch + build
  python3 bench/bin/patch_benchbase.py --patch   # patch only (no build)
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BENCHBASE_SRC = ROOT / "third_party" / "benchbase"


def patch_worker_retry():
    """Add error 1180 (ER_ERROR_DURING_COMMIT) retry to Worker.isRetryable()."""
    worker = (
        BENCHBASE_SRC
        / "src/main/java/com/oltpbenchmark/api/Worker.java"
    )
    text = worker.read_text()

    if "errorCode == 1180" in text:
        print("  Worker.java already patched")
        return

    # Insert after the MySQL ER_LOCK_WAIT_TIMEOUT block
    old = """\
    } else if (errorCode == 1205 && sqlState.equals("40001")) {
      // MySQL ER_LOCK_WAIT_TIMEOUT
      return true;
    }"""

    new = """\
    } else if (errorCode == 1205 && sqlState.equals("40001")) {
      // MySQL ER_LOCK_WAIT_TIMEOUT
      return true;
    } else if (errorCode == 1180 && sqlState.equals("HY000")) {
      // OCC commit-time deadlock: HA_ERR_LOCK_DEADLOCK (149) surfaced as
      // ER_ERROR_DURING_COMMIT by MySQL's ha_commit_low().
      String msg = ex.getMessage();
      if (msg != null && msg.contains("Got error 149")) {
        return true;
      }
    }"""

    if old not in text:
        print("  WARNING: Worker.java anchor not found, skipping", file=sys.stderr)
        return

    text = text.replace(old, new)
    worker.write_text(text)
    print("  patched Worker.java (OCC commit-time retry)")


def patch_loader_stock_retry():
    """Add batch-level retry to TPCCLoader.loadStock() for parallel loading."""
    loader = (
        BENCHBASE_SRC
        / "src/main/java/com/oltpbenchmark/benchmarks/tpcc/TPCCLoader.java"
    )
    text = loader.read_text()

    if "maxRetries" in text:
        print("  TPCCLoader.java already patched")
        return

    old = """\
  protected void loadStock(Connection conn, int w_id, int numItems) {

    int k = 0;

    try (PreparedStatement stockPreparedStatement =
        getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK)) {

      for (int i = 1; i <= numItems; i++) {
        Stock stock = new Stock();
        stock.s_i_id = i;
        stock.s_w_id = w_id;
        stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
        stock.s_ytd = 0;
        stock.s_order_cnt = 0;
        stock.s_remote_cnt = 0;

        // s_data
        int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
        int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
        if (randPct > 10) {
          // 90% of time i_data isa random string of length [26 ..
          // 50]
          stock.s_data = TPCCUtil.randomStr(len);
        } else {
          // 10% of time i_data has "ORIGINAL" crammed somewhere
          // in middle
          int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
          stock.s_data =
              TPCCUtil.randomStr(startORIGINAL - 1)
                  + "ORIGINAL"
                  + TPCCUtil.randomStr(len - startORIGINAL - 9);
        }

        int idx = 1;
        stockPreparedStatement.setLong(idx++, stock.s_w_id);
        stockPreparedStatement.setLong(idx++, stock.s_i_id);
        stockPreparedStatement.setLong(idx++, stock.s_quantity);
        stockPreparedStatement.setDouble(idx++, stock.s_ytd);
        stockPreparedStatement.setLong(idx++, stock.s_order_cnt);
        stockPreparedStatement.setLong(idx++, stock.s_remote_cnt);
        stockPreparedStatement.setString(idx++, stock.s_data);
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx, TPCCUtil.randomStr(24));
        stockPreparedStatement.addBatch();

        k++;

        if (k != 0 && (k % workConf.getBatchSize()) == 0) {
          stockPreparedStatement.executeBatch();
          stockPreparedStatement.clearBatch();
        }
      }

      stockPreparedStatement.executeBatch();
      stockPreparedStatement.clearBatch();

    } catch (SQLException se) {
      LOG.error(se.getMessage());
    }
  }"""

    new = """\
  protected void loadStock(Connection conn, int w_id, int numItems) {

    int batchSize = workConf.getBatchSize();
    int maxRetries = 20;

    try (PreparedStatement stmt =
        getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK)) {

      for (int batchStart = 1; batchStart <= numItems; batchStart += batchSize) {
        int batchEnd = Math.min(batchStart + batchSize - 1, numItems);

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
          try {
            for (int i = batchStart; i <= batchEnd; i++) {
              Stock stock = new Stock();
              stock.s_i_id = i;
              stock.s_w_id = w_id;
              stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
              stock.s_ytd = 0;
              stock.s_order_cnt = 0;
              stock.s_remote_cnt = 0;

              int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
              int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
              if (randPct > 10) {
                stock.s_data = TPCCUtil.randomStr(len);
              } else {
                int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                stock.s_data =
                    TPCCUtil.randomStr(startORIGINAL - 1)
                        + "ORIGINAL"
                        + TPCCUtil.randomStr(len - startORIGINAL - 9);
              }

              int idx = 1;
              stmt.setLong(idx++, stock.s_w_id);
              stmt.setLong(idx++, stock.s_i_id);
              stmt.setLong(idx++, stock.s_quantity);
              stmt.setDouble(idx++, stock.s_ytd);
              stmt.setLong(idx++, stock.s_order_cnt);
              stmt.setLong(idx++, stock.s_remote_cnt);
              stmt.setString(idx++, stock.s_data);
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx++, TPCCUtil.randomStr(24));
              stmt.setString(idx, TPCCUtil.randomStr(24));
              stmt.addBatch();
            }

            stmt.executeBatch();
            stmt.clearBatch();
            break;
          } catch (SQLException se) {
            stmt.clearBatch();
            boolean retryable = (se.getErrorCode() == 1213)
                || (se.getErrorCode() == 1180 && se.getMessage() != null
                    && se.getMessage().contains("Got error 149"));
            if (retryable && attempt < maxRetries) {
              LOG.warn("loadStock w_id={} batch [{}-{}] deadlock, retry {}/{}",
                       w_id, batchStart, batchEnd, attempt + 1, maxRetries);
              try { Thread.sleep(10L * (attempt + 1)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            } else {
              throw se;
            }
          }
        }
      }
    } catch (SQLException se) {
      LOG.error(se.getMessage());
    }
  }"""

    if old not in text:
        print("  WARNING: TPCCLoader.java anchor not found, skipping", file=sys.stderr)
        return

    text = text.replace(old, new)
    loader.write_text(text)
    print("  patched TPCCLoader.java (loadStock batch retry)")


def patch_tpch_hash_join():
    """Add USE INDEX () to TPC-H queries to force hash join over NLJ.

    In Helios's RPC-based architecture, hash join (1 RPC per table scan) is
    vastly faster than NLJ with PK lookups (1 RPC per row probe).  MySQL's
    optimizer doesn't model RPC cost, so it always prefers eq_ref (PK lookup).
    Adding USE INDEX () disables all index access, forcing hash join.
    """
    TPCH_TABLES = [
        'lineitem', 'orders', 'customer', 'supplier',
        'partsupp', 'part', 'nation', 'region',
    ]

    procedures_dir = (
        BENCHBASE_SRC
        / "src/main/java/com/oltpbenchmark/benchmarks/tpch/procedures"
    )

    # --- USE INDEX () for all TPC-H queries ---
    # Only apply inside SQL text blocks, only in FROM/JOIN context.
    # Must NOT match: "AS nation", "WHEN nation = ?", column refs.
    # MySQL syntax: table [alias] USE INDEX ()
    table_pattern = '|'.join(TPCH_TABLES)
    text_block_re = re.compile(r'"""(.*?)"""', re.DOTALL)

    # Match table name only when preceded by FROM/JOIN/comma context:
    # - after "FROM\n   " or "FROM " (table in FROM clause)
    # - after ",\n   " or ", " (next table in FROM list)
    # - after "JOIN " or "JOIN\n   " (explicit JOIN)
    # Captures: (table_name)(optional_alias)
    table_ref_re = re.compile(
        r'(?:(?<=FROM\n)|(?<=JOIN\n)|(?<=FROM )|(?<=JOIN )'
        r'|(?<=,\n)|(?<=,))'
        r'(\s*)'                                # leading whitespace
        r'\b(' + table_pattern + r')\b'         # table name
        r'(\s+[a-z]\d+)?'                       # optional alias
        r'(?!\s+USE\s+INDEX)'                   # not already patched
    )

    def add_use_index_in_from(sql):
        """Add USE INDEX () to table references in FROM/JOIN clauses only."""
        lines = sql.split('\n')
        result = []
        in_from = False  # Track if we're inside a FROM clause

        for line in lines:
            stripped = line.strip()
            upper = stripped.upper()

            # Detect FROM clause start
            # Skip EXTRACT(YEAR FROM ...) where FROM is part of EXTRACT
            if upper == 'FROM' or re.match(r'^FROM\s+\w', upper):
                # Check if previous non-empty line ends with EXTRACT(YEAR
                prev = ''
                for j in range(len(result) - 1, -1, -1):
                    prev = result[j].strip()
                    if prev:
                        break
                if not prev.upper().endswith('EXTRACT(YEAR'):
                    in_from = True
            # Detect end of FROM clause (WHERE, GROUP, ORDER, SELECT, HAVING, LIMIT)
            elif in_from and upper and any(
                upper.startswith(kw) for kw in
                ['WHERE', 'GROUP BY', 'ORDER BY', 'SELECT', 'HAVING',
                 'LIMIT', 'AS ', ')']
            ):
                in_from = False

            # Also handle JOIN lines (which are always table references)
            is_join_line = bool(re.match(
                r'^\s*(?:LEFT\s+OUTER\s+|LEFT\s+|RIGHT\s+|INNER\s+|CROSS\s+)?JOIN\s+',
                line, re.IGNORECASE,
            ))

            if (in_from or is_join_line) and 'USE INDEX' not in line:
                # Add USE INDEX () after table names on this line
                line = re.sub(
                    r'\b(' + table_pattern + r')\b'
                    r'(\s+[a-z]\d+)?'
                    r'(?!\s+USE\s+INDEX)',
                    lambda m: m.group(0) + ' USE INDEX ()',
                    line,
                )

            result.append(line)
        return '\n'.join(result)

    def patch_text_block(match):
        sql = match.group(1)
        sql = add_use_index_in_from(sql)
        return '"""' + sql + '"""'

    modified = 0
    for java_file in sorted(procedures_dir.glob("Q*.java")):
        content = java_file.read_text()
        if "USE INDEX" in content:
            continue

        new_content = text_block_re.sub(patch_text_block, content)
        if new_content != content:
            java_file.write_text(new_content)
            modified += 1

    if modified:
        print(f"  patched {modified} TPC-H queries with USE INDEX ()")
    else:
        print("  TPC-H queries already patched with USE INDEX ()")


def build_benchbase():
    """Build BenchBase with MySQL profile."""
    print("\nBuilding BenchBase...")
    result = subprocess.run(
        ["./mvnw", "-DskipTests", "-P", "mysql", "clean", "package"],
        cwd=BENCHBASE_SRC,
    )
    if result.returncode != 0:
        print("ERROR: BenchBase build failed", file=sys.stderr)
        sys.exit(1)

    # Extract the zip
    import zipfile
    zip_path = BENCHBASE_SRC / "target" / "benchbase-mysql.zip"
    extract_dir = BENCHBASE_SRC / "benchbase-mysql"
    if extract_dir.exists():
        import shutil
        shutil.rmtree(extract_dir)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(BENCHBASE_SRC)

    jar = extract_dir / "benchbase.jar"
    if jar.exists():
        print(f"\nBuild successful: {jar}")
    else:
        print("ERROR: benchbase.jar not found after build", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Patch and build BenchBase for Helios")
    parser.add_argument("--patch", action="store_true", help="Patch only, skip build")
    args = parser.parse_args()

    if not BENCHBASE_SRC.exists():
        print(f"ERROR: {BENCHBASE_SRC} not found. Run: git submodule update --init", file=sys.stderr)
        sys.exit(1)

    print("Applying patches to third_party/benchbase...")
    patch_worker_retry()
    patch_loader_stock_retry()
    patch_tpch_hash_join()

    if not args.patch:
        build_benchbase()


if __name__ == "__main__":
    main()
