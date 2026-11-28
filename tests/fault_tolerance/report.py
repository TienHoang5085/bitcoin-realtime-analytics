"""Summarize a driver log: batch durations, tasks per host, executor and task-failure events.

    python tests/fault_tolerance/report.py <driver.log>
"""
import re
import sys
from collections import Counter
from datetime import datetime

lines = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
started, rows = {}, []
for line in lines:
    m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ \[\w+\] UnifiedStreaming - (Processing batch (\d+)|Batch (\d+) complete)", line)
    if not m:
        continue
    at = datetime.strptime(m[1], "%Y-%m-%d %H:%M:%S")
    if m[3]:
        started[m[3]] = at
    elif m[4] in started:
        rows.append((m[4], started[m[4]], (at - started[m[4]]).total_seconds()))

print("== batches (id, start, seconds)")
for batch, at, seconds in rows:
    print(f"  {batch:>4} {at:%H:%M:%S} {seconds:6.0f}s")

hosts = Counter(re.findall(r"Finished task \S+ in stage \S+ \(TID \d+\) in \d+ ms on (\S+) \(executor", "\n".join(lines)))
print("== finished tasks per host:", dict(hosts))

print("== executor / failure events")
pattern = re.compile(r"Executor added|Executor updated|Executor removed|Lost executor|Removing executor|"
                     r"Lost task|Resubmitted|failed and will be retried|Streaming query failed|"
                     r"Disassociated|Connection .* closed|Granted executor|OutOfMemory")
for line in lines:
    if pattern.search(line):
        print("  " + line[:220])
