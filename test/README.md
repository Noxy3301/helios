# Tests

`test/pytest/` holds the SQL tests; each file is a script that connects to a
running MySQL on port 3307 and exits non-zero on failure. `test/pytest/tpc-c/`
holds the TPC-C procedure tests and `test/pytest/utils/` the shared fixtures.

Build the tree and run every test, or a named subset, with the runner, which
starts and stops the storage server and MySQL itself:

```bash
./scripts/build.sh
python3 test/run_tests.py            # every test
python3 test/run_tests.py insert.py select   # named tests
```

The tests need `mysql-connector-python`:

```bash
pip3 install -r test/pytest/requirements.txt
```
