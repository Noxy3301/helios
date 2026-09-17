import os
import sys
import glob
import argparse
import shutil
import time

SERVER_HOST = "127.0.0.1"
MYSQLD_PORT = "3307"
SERVER_PORT = "9999"


# def CI(test_files):
#   exit_value = 0
#   for f in test_files:
#     os.system("sudo systemctl restart mysql.service")
#     if os.system(f"python3 {f} --password root"): exit_value = 1
#   sys.exit(exit_value)

QUIET = "> /dev/null 2>&1"

def restart_services():
  """LineairDB Server + MySQL を再起動してクリーンな状態にする"""
  os.system(f"./scripts/stop_mysql.sh {QUIET}")
  os.system(f"./scripts/stop_server.sh {QUIET}")
  # The storage keeps its PAX catalog and log across restarts even with
  # recovery off, and the suite re-creates tables with different columns.
  shutil.rmtree("helios_wal", ignore_errors=True)
  time.sleep(1)
  os.system(f"./scripts/start_server.sh {QUIET}")
  time.sleep(2)
  os.system(f"./scripts/start_mysql.sh --mysqld-port {MYSQLD_PORT} --server-host {SERVER_HOST} --server-port {SERVER_PORT} {QUIET}")

def run_tests(test_files):
  os.system(f"./scripts/build_partial.sh {QUIET}")
  exit_value = 0
  for f in test_files:
    restart_services()
    # TPC-Cテストは--host/--portを受け付ける、コアテストは環境変数で接続
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
  # test
  is_ci = args.ci

  # テストファイルの決定：引数で指定された場合はそれを使用、なければ全てのテストを実行
  if args.tests:
    test_files = []
    for test in args.tests:
      # test/pytest/ ディレクトリからの相対パスまたはファイル名を受け付ける
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
    # 引数が指定されていない場合は全てのテストを実行
    test_files = glob.glob(os.path.join("test/pytest", "**", "*.py"), recursive=True)
    # utils/は非テストファイルなので除外
    test_files = [f for f in test_files if "/utils/" not in f]

  if not test_files:
    print("Error: No test files found")
    sys.exit(1)

  print(f"Running {len(test_files)} test(s):")
  for f in test_files:
    print(f"  - {f}")
  print()

  # if is_ci: CI(test_files)
  # else: run_tests(test_files)
  exit_value = run_tests(test_files)
  sys.exit(exit_value)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Connect to MySQL')
  parser.add_argument('-c', '--ci', action='store_true',
                      help='run CI or default tests')
  parser.add_argument('tests', nargs='*',
                      help='specific test files to run (e.g., insert.py, select, test/pytest/update.py). If not specified, all tests will be run.')
  args = parser.parse_args()
  main()
