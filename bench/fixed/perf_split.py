#!/usr/bin/env python3
"""Split a perf profile (perf script output on stdin) into user and kernel
time, and attribute kernel samples to the innermost user-space frame that led
into the kernel (recv, send, futex wait, WAL write, ...).

Usage: perf script -i perf.data | perf_split.py [--top 20]
"""
import collections
import re
import sys

top = int(sys.argv[sys.argv.index("--top") + 1]) if "--top" in sys.argv else 20
total = 0
kern = 0
by_user = collections.Counter()
by_kfn = collections.Counter()
sample = []


def flush():
    global total, kern
    if not sample:
        return
    total += 1
    frames = sample
    leaf_kernel = "[kernel.kallsyms]" in frames[0]
    if leaf_kernel:
        kern += 1
        by_kfn[re.sub(r"^\s*\S+\s+", "", frames[0]).split("+")[0].split(" (")[0]] += 1
        user = next((f for f in frames if "[kernel.kallsyms]" not in f), "?")
        # Skip libc syscall stubs to the first frame of the program or a library.
        idx = frames.index(user) if user in frames else -1
        name = user
        for f in frames[idx:]:
            if not re.search(r"libc\.so|libpthread|\[unknown\]", f):
                name = f
                break
        name = re.sub(r"^\s*\S+\s+", "", name)
        name = re.sub(r"\+0x[0-9a-f]+", "", name).split(" (")[0]
        by_user[name[:100]] += 1


for line in sys.stdin:
    if not line.strip():
        flush()
        sample = []
        continue
    if line.startswith((" ", "\t")):
        sample.append(line.strip())
flush()
print(f"samples {total}, kernel {kern} ({kern / total * 100:.1f}%)")
print("kernel samples by the user frame that entered the kernel:")
for k, v in by_user.most_common(top):
    print(f"  {v / total * 100:5.1f}%  {k}")
print("kernel leaf functions:")
for k, v in by_kfn.most_common(10):
    print(f"  {v / total * 100:5.1f}%  {k}")
