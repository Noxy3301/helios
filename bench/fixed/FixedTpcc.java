// Fixed-sample TPC-C driver.
//
// Runs the BenchBase TPC-C procedures (the same classes and SQL BenchBase
// runs) on one connection, with inputs drawn from a seeded Random, in blocks
// of a single transaction type. From the same loaded state and the same seed,
// every run issues the same statements with the same parameters in the same
// order, so the work a block asks of mysqld and the storage server is fixed
// and its per-transaction CPU or instruction count can be compared across
// builds without BenchBase's concurrency noise.
//
// Between blocks it reads the CPU time of the processes named by --pid (from
// /proc) and runs --hook with "<begin|end> <label>" (used for callgrind dumps).
//
// It sits in com.oltpbenchmark.api only to reach Procedure.initialize.
package com.oltpbenchmark.api;

import com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Payment;
import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;
import com.oltpbenchmark.types.DatabaseType;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

public final class FixedTpcc {

  static final String[] TYPES = {"NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"};
  static final int[] MIX = {45, 43, 4, 4, 4};

  static final class Block {
    final String label;
    final String type; // null for the mixed block
    final int count;

    Block(String label, String type, int count) {
      this.label = label;
      this.type = type;
      this.count = count;
    }
  }

  // CPU time of one process: utime+stime of the whole process (10 ms ticks,
  // counts exited threads) and the sum of per-thread schedstat (ns, live
  // threads only).
  static long[] cpu(long pid) {
    long ticks = -1;
    long ns = 0;
    try {
      String stat = Files.readString(Paths.get("/proc/" + pid + "/stat"));
      String[] f = stat.substring(stat.lastIndexOf(')') + 2).split(" ");
      ticks = Long.parseLong(f[11]) + Long.parseLong(f[12]);
      try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get("/proc/" + pid + "/task"))) {
        for (Path t : ds) {
          try {
            String s = Files.readString(t.resolve("schedstat")).trim();
            ns += Long.parseLong(s.split(" ")[0]);
          } catch (IOException e) {
            // thread exited between listing and reading
          }
        }
      }
    } catch (IOException e) {
      return new long[] {-1, -1};
    }
    return new long[] {ticks, ns};
  }

  static void hook(String cmd, String phase, String label) {
    if (cmd == null) {
      return;
    }
    try {
      Process p = new ProcessBuilder("/bin/sh", "-c", cmd + " " + phase + " " + label)
          .inheritIO().start();
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("hook failed: " + e);
    }
  }

  static TPCCProcedure make(String type) {
    TPCCProcedure p = switch (type) {
      case "NewOrder" -> new NewOrder();
      case "Payment" -> new Payment();
      case "OrderStatus" -> new OrderStatus();
      case "Delivery" -> new Delivery();
      case "StockLevel" -> new StockLevel();
      default -> throw new IllegalArgumentException(type);
    };
    return p.initialize(DatabaseType.MYSQL);
  }

  public static void main(String[] args) throws Exception {
    String url = "jdbc:mysql://127.0.0.1:3307/benchbase?rewriteBatchedStatements=true&sslMode=DISABLED";
    long seed = 20261102L;
    String plan = "mix:300,NewOrder:300,Payment:300,OrderStatus:300,Delivery:60,StockLevel:300";
    String hookCmd = null;
    String out = null;
    int warehouses = 1;
    Map<String, Long> pids = new LinkedHashMap<>();
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--url" -> url = args[++i];
        case "--seed" -> seed = Long.parseLong(args[++i]);
        case "--plan" -> plan = args[++i];
        case "--hook" -> hookCmd = args[++i];
        case "--out" -> out = args[++i];
        case "--warehouses" -> warehouses = Integer.parseInt(args[++i]);
        case "--pid" -> {
          String[] kv = args[++i].split("=");
          pids.put(kv[0], Long.parseLong(kv[1]));
        }
        default -> throw new IllegalArgumentException("unknown argument " + args[i]);
      }
    }

    List<Block> blocks = new ArrayList<>();
    for (String part : plan.split(",")) {
      String[] kv = part.split(":");
      String label = kv[0];
      String type = label.startsWith("mix") || label.startsWith("idle") ? null : label.replaceAll("[0-9]+$", "");
      blocks.add(new Block(label, type, Integer.parseInt(kv[1])));
    }

    Map<String, TPCCProcedure> procs = new LinkedHashMap<>();
    for (String t : TYPES) {
      procs.put(t, make(t));
    }

    StringBuilder json = new StringBuilder();
    json.append("{\"seed\":").append(seed).append(",\"plan\":\"").append(plan).append("\",\"blocks\":[");
    try (Connection conn = DriverManager.getConnection(url, "root", "")) {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      for (int b = 0; b < blocks.size(); b++) {
        Block blk = blocks.get(b);
        // One Random per block, so a block's inputs depend only on the seed
        // and its position, not on what other blocks drew.
        Random gen = new Random(seed * 1000003L + b);
        Random pick = new Random(seed * 7919L + b);
        long[] lat = new long[blk.count];
        int ok = 0, userAbort = 0, sqlError = 0;
        Map<String, Integer> perType = new LinkedHashMap<>();
        Map<String, String> errors = new LinkedHashMap<>();

        hook(hookCmd, "begin", blk.label);
        Map<String, long[]> cpu0 = new LinkedHashMap<>();
        for (var e : pids.entrySet()) cpu0.put(e.getKey(), cpu(e.getValue()));
        long w0 = System.nanoTime();
        boolean idle = blk.label.startsWith("idle");
        if (idle) {
          // No transactions: measures what the processes burn on their own
          // over blk.count milliseconds (epoch advance, logger, polling).
          Thread.sleep(blk.count);
        }
        for (int i = 0; i < (idle ? 0 : blk.count); i++) {
          String type = blk.type;
          if (type == null) {
            int r = pick.nextInt(100), acc = 0;
            for (int k = 0; k < TYPES.length; k++) {
              acc += MIX[k];
              if (r < acc) {
                type = TYPES[k];
                break;
              }
            }
          }
          perType.merge(type, 1, Integer::sum);
          TPCCProcedure p = procs.get(type);
          long t0 = System.nanoTime();
          try {
            p.run(conn, gen, 1, warehouses, 1, 10, null);
            conn.commit();
            ok++;
          } catch (Procedure.UserAbortException e) {
            conn.rollback();
            userAbort++;
          } catch (SQLException e) {
            try {
              conn.rollback();
            } catch (SQLException ignored) {
            }
            sqlError++;
            errors.merge(e.getSQLState() + "/" + e.getErrorCode(), e.getMessage(), (a, c) -> a);
          }
          lat[i] = System.nanoTime() - t0;
        }
        long wall = System.nanoTime() - w0;
        Map<String, long[]> cpu1 = new LinkedHashMap<>();
        for (var e : pids.entrySet()) cpu1.put(e.getKey(), cpu(e.getValue()));
        hook(hookCmd, "end", blk.label);

        if (idle) lat = new long[] {0};
        long[] sorted = lat.clone();
        Arrays.sort(sorted);
        double mean = Arrays.stream(lat).average().orElse(0);
        if (b > 0) json.append(',');
        json.append(String.format(Locale.ROOT,
            "{\"label\":\"%s\",\"count\":%d,\"ok\":%d,\"user_abort\":%d,\"sql_error\":%d,"
                + "\"wall_ns\":%d,\"lat_mean_ns\":%.0f,\"lat_p50_ns\":%d,\"lat_p90_ns\":%d,\"lat_p99_ns\":%d",
            blk.label, blk.count, ok, userAbort, sqlError, wall, mean,
            sorted[sorted.length / 2], sorted[(int) (sorted.length * 0.9)],
            sorted[Math.min(sorted.length - 1, (int) (sorted.length * 0.99))]));
        json.append(",\"types\":{");
        int k = 0;
        for (var e : perType.entrySet()) {
          if (k++ > 0) json.append(',');
          json.append('"').append(e.getKey()).append("\":").append(e.getValue());
        }
        json.append("},\"cpu\":{");
        k = 0;
        for (var e : pids.entrySet()) {
          long[] a = cpu0.get(e.getKey()), c = cpu1.get(e.getKey());
          if (k++ > 0) json.append(',');
          json.append(String.format(Locale.ROOT, "\"%s\":{\"ticks\":%d,\"sched_ns\":%d}",
              e.getKey(), c[0] - a[0], c[1] - a[1]));
        }
        json.append("},\"errors\":{");
        k = 0;
        for (var e : errors.entrySet()) {
          if (k++ > 0) json.append(',');
          json.append('"').append(e.getKey()).append("\":\"")
              .append(e.getValue().replace("\\", "\\\\").replace("\"", "'").replace("\n", " "))
              .append('"');
        }
        json.append("}}");
        System.err.printf(Locale.ROOT, "%-12s n=%d ok=%d abort=%d err=%d p50=%.3fms wall=%.2fs%n",
            blk.label, blk.count, ok, userAbort, sqlError, sorted[sorted.length / 2] / 1e6, wall / 1e9);
      }
    }
    json.append("]}\n");
    if (out != null) {
      Files.writeString(Paths.get(out), json.toString());
    } else {
      System.out.print(json);
    }
  }
}
