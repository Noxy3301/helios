import os
import sys
import glob
import argparse
import shutil
import time

SERVER_HOST = "127.0.0.1"
MYSQLD_PORT = "3307"
SERVER_PORT = "9999"


QUIET = "> /dev/null 2>&1"

def restart_services():
  """Restart the storage server and MySQL for a clean state."""
  os.system(f"./scripts/stop_mysql.sh {QUIET}")
  os.system(f"./scripts/stop_server.sh {QUIET}")
  # An interrupted run can leave a contract behind in the local overrides.
  if os.path.exists("helios.local.cnf"):
    os.unlink("helios.local.cnf")
  # The storage keeps its PAX catalog and log across restarts even with
  # recovery off, and the suite re-creates tables with different columns.
  # The work directory may be a link onto another volume: clear it, keep it.
  for entry in (os.scandir("helios_wal") if os.path.isdir("helios_wal") else []):
    shutil.rmtree(entry.path, ignore_errors=True) if entry.is_dir(follow_symlinks=False) else os.unlink(entry.path)
  time.sleep(1)
  os.system(f"./scripts/start_server.sh {QUIET}")
  time.sleep(2)
  os.system(f"./scripts/start_mysql.sh --mysqld-port {MYSQLD_PORT} --server-host {SERVER_HOST} --server-port {SERVER_PORT} {QUIET}")

def run_tests(test_files):
  os.system(f"./scripts/build_partial.sh {QUIET}")
  exit_value = 0
  for f in test_files:
    restart_services()
    # TPC-C tests take --host/--port; the core tests connect from the env.
    if "/tpc-c/" in f:
      ret = os.system(f"python3 {f} --host {SERVER_HOST} --port {MYSQLD_PORT}")
    else:
      ret = os.system(f"python3 {f}")
    if ret != 0:
      exit_value = 1
  os.system(f"./scripts/stop_mysql.sh {QUIET}")
  os.system(f"./scripts/stop_server.sh {QUIET}")
  return exit_value

def main():
  # Named tests, or every test under test/pytest.
  if args.tests:
    test_files = []
    for test in args.tests:
      # A path relative to test/pytest/, or a bare file name.
      if os.path.exists(test):
        test_files.append(test)
      elif os.path.exists(os.path.join("test/pytest", test)):
        test_files.append(os.path.join("test/pytest", test))
      elif os.path.exists(os.path.join("test/pytest", f"{test}.py")):
        test_files.append(os.path.join("test/pytest", f"{test}.py"))
      elif os.path.exists(os.path.join("test/pytest/tpc-c", test)):
        test_files.append(os.path.join("test/pytest/tpc-c", test))
      elif os.path.exists(os.path.join("test/pytest/tpc-c", f"{test}.py")):
        test_files.append(os.path.join("test/pytest/tpc-c", f"{test}.py"))
      else:
        print(f"Warning: Test file not found: {test}")
  else:
    test_files = glob.glob(os.path.join("test/pytest", "**", "*.py"), recursive=True)
    # utils/ holds fixtures, not tests.
    test_files = [f for f in test_files if "/utils/" not in f]

  if not test_files:
    print("Error: No test files found")
    sys.exit(1)

  print(f"Running {len(test_files)} test(s):")
  for f in test_files:
    print(f"  - {f}")
  print()

  exit_value = run_tests(test_files)
  sys.exit(exit_value)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Connect to MySQL')
  parser.add_argument('tests', nargs='*',
                      help='specific test files to run (e.g., insert.py, select, test/pytest/update.py). If not specified, all tests will be run.')
  args = parser.parse_args()
  main()
