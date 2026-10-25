#!/usr/bin/env python3
"""Merge one research batch into data/research_verdicts.json.

Usage: python3 scripts/merge_verdicts.py <batch.json>
Batch file: a JSON array of verdict objects (ticker, flag, summary,
legs, risks, what_changes, sources). Origin strings and dates are
stamped here from the validation queue. final50 is recomputed as the
BUY names in queue order, capped at 50.
"""
import json
import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
VPATH = os.path.join(DATA, "research_verdicts.json")

queue = json.load(open(os.path.join(DATA, "validation_queue.json")))
qix = {q["ticker"]: q for q in queue}
order = {q["ticker"]: i for i, q in enumerate(queue)}

store = {"meta": {"target": 50, "rubric": "v1", "started": "2026-10-25"},
         "verdicts": {}, "final50": []}
if os.path.exists(VPATH):
    store = json.load(open(VPATH))

def rubric_flag(legs):
    """The rubric decides the flag; the agent proposes. BUY = no leg
    fails and at least 4 pass. SELL = 2+ legs fail, or quality AND risk
    both fail (the existential combination). Otherwise HOLD — a lone
    quality or risk fail is serious but survivable by definition when
    the other hard leg passes."""
    vs = [lv.get("v") for lv in legs.values()]
    fails = vs.count("fail")
    passes = vs.count("pass")
    hard_both = (legs.get("quality", {}).get("v") == "fail"
                 and legs.get("risk", {}).get("v") == "fail")
    if fails >= 2 or hard_both:
        return "SELL"
    if fails == 0 and passes >= 4:
        return "BUY"
    return "HOLD"


batch = json.load(open(sys.argv[1]))
today = date.today().isoformat()
for v in batch:
    t = v["ticker"]
    computed = rubric_flag(v.get("legs") or {})
    if computed != v["flag"]:
        v["agent_flag"] = v["flag"]
        v["flag"] = computed
        v["rule_override"] = True
        print(f"  rubric override: {t} agent={v['agent_flag']} -> {computed}")
    q = qix.get(t, {})
    origin = []
    if q.get("dip_rank") and q.get("phase") == "A":
        origin.append(f"Dip Top 50 #{q['dip_rank']}")
    elif q.get("dip_rank"):
        origin.append(f"Dip pool #{q['dip_rank']}")
    if q.get("pro_rank") and q.get("phase") == "A":
        origin.append(f"Pro Top 50 #{q['pro_rank']} ({q.get('pro_grade') or ''})")
    elif q.get("pro_rank"):
        origin.append(f"Pro pool #{q['pro_rank']} ({q.get('pro_grade') or ''})")
    v["origin"] = " · ".join(origin)
    v["date"] = today
    store["verdicts"][t] = v

buys = sorted((t for t, v in store["verdicts"].items() if v["flag"] == "BUY"),
              key=lambda t: order.get(t, 10 ** 9))
store["final50"] = buys[:50]
store["meta"]["updated"] = today
json.dump(store, open(VPATH, "w"), indent=1)

counts = {"BUY": 0, "HOLD": 0, "SELL": 0}
for v in store["verdicts"].values():
    counts[v["flag"]] = counts.get(v["flag"], 0) + 1
print(f"researched: {len(store['verdicts'])} | BUY {counts['BUY']} · "
      f"HOLD {counts['HOLD']} · SELL {counts['SELL']} | final50 slots filled: "
      f"{len(store['final50'])}")
print("this batch:", ", ".join(f"{v['ticker']}={v['flag']}" for v in batch))
