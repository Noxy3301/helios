# Helios storage engine

Modified for Helios.

Helios's storage engine is derived from
[LineairDB](https://github.com/LineairDB/LineairDB), adapted for Helios's
research goals with Silo concurrency control and PAX storage. The public
headers live under `include/helios`:

```cpp
#include "helios/database.h"
```

The API uses the `helios::storage` namespace and the `helios_storage` CMake
target. The API is tailored to Helios; upstream LineairDB examples require
adaptation to use this engine.

## Building

```bash
cmake -S server/storage -B build/storage -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
cmake --build build/storage
ctest --test-dir build/storage -j "$(nproc)"
```

The tests that build a database on the default working directory hold a lock,
so no two of them run at once; the rest run alongside them.
`scripts/storage_tests.sh [build_dir]` runs the three steps in one command.

## Provenance

Derived from [LineairDB](https://github.com/LineairDB/LineairDB) (Apache-2.0,
Nippon Telegraph and Telephone Corporation), by way of the frozen fork at
<https://github.com/Noxy3301/LineairDB> (commit 66288a1e). `LICENSE`,
`LICENSE-3RD-PARTY.md` and `NOTICE` are kept here. The research paper the
engine grew out of is at <https://arxiv.org/abs/1904.08119>.

The implementation builds on that foundation, with substantial reductions
and changes to the API and storage layout for Helios.
