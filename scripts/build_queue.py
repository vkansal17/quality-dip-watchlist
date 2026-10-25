#!/usr/bin/env python3
"""Build the merged research-validation queue from the saved pools.

Phase A: the union of the Quality-Dip Top 50 and the Professional Top 50
(names on both lists first, then by average normalized rank).
Phase B: the merged bench — every remaining name in either full pool
(dip candidates / graded non-vetoed professionals), ordered by its best
pool percentile. Pro-vetoed names and the researched exclusions
(NKE, TSLA) never enter the queue.

Output: data/validation_queue.json
"""
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")

pools = json.load(open(os.path.join(DATA, "pools.json")))
dip_rows = json.load(open(os.path.join(DATA, "watchlist.json")))["rows"]
pro_rows = json.load(open(os.path.join(DATA, "professional.json")))["rows"]

dip50 = {r["ticker"]: r for r in dip_rows}
pro50 = {r["ticker"]: r for r in pro_rows}
dip_pool = {r["ticker"]: r for r in pools["dip"]}
pro_pool = {r["ticker"]: r for r in pools["pro"]}

queue = []
seen = set()

phase_a = set(dip50) | set(pro50)


def a_key(t):
    both = 0 if (t in dip50 and t in pro50) else 1
    ranks = []
    if t in dip50:
        ranks.append(dip50[t]["rank"] / 50)
    if t in pro50:
        ranks.append(pro50[t]["rank"] / 50)
    return (both, sum(ranks) / len(ranks))


for t in sorted(phase_a, key=a_key):
    src = dip50.get(t) or pro50.get(t)
    queue.append({"ticker": t, "phase": "A",
                  "dip_rank": dip50[t]["rank"] if t in dip50 else None,
                  "pro_rank": pro50[t]["rank"] if t in pro50 else None,
                  "pro_grade": pro50[t]["grade"] if t in pro50 else None,
                  "last": src.get("last"), "off_high_pct": src.get("off_high_pct")})
    seen.add(t)

n_dip, n_pro = max(len(dip_pool), 1), max(len(pro_pool), 1)


def b_key(t):
    pcts = []
    if t in dip_pool:
        pcts.append(1 - (dip_pool[t]["rank"] - 1) / n_dip)
    if t in pro_pool:
        pcts.append(1 - (pro_pool[t]["rank"] - 1) / n_pro)
    return -max(pcts)


bench = (set(dip_pool) | set(pro_pool)) - seen
for t in sorted(bench, key=b_key):
    src = dip_pool.get(t) or pro_pool.get(t)
    queue.append({"ticker": t, "phase": "B",
                  "dip_rank": dip_pool[t]["rank"] if t in dip_pool else None,
                  "pro_rank": pro_pool[t]["rank"] if t in pro_pool else None,
                  "pro_grade": pro_pool[t].get("grade") if t in pro_pool else None,
                  "last": src.get("last"), "off_high_pct": src.get("off_high_pct")})
    seen.add(t)

json.dump(queue, open(os.path.join(DATA, "validation_queue.json"), "w"), indent=1)
print(f"queue: {len(queue)} names (phase A: {sum(1 for q in queue if q['phase'] == 'A')}, "
      f"phase B: {sum(1 for q in queue if q['phase'] == 'B')})")
print("first 15:", [q["ticker"] for q in queue[:15]])
