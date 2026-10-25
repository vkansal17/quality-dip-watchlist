#!/usr/bin/env python3
"""Merge one Macro & Themes research batch into data/theme_verdicts.json.

Usage: python3 scripts/merge_theme_verdicts.py <batch.json>
Same verdict schema and rubric as the main validation loop; the flag
is computed from the legs here. Each verdict is stamped with its theme
origin from data/theme_queue.json. `list` is recomputed as the BUY
names in theme-queue order (no cap — the list is its own tab).
"""
import json
import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
VPATH = os.path.join(DATA, "theme_verdicts.json")

_qp = os.path.join(DATA, "macro_sweep_queue.json")
if not os.path.exists(_qp):
    _qp = os.path.join(DATA, "theme_queue.json")
tqueue = json.load(open(_qp))
qix = {q["ticker"]: q for q in tqueue}
order = {q["ticker"]: q["order"] for q in tqueue}

store = {"meta": {"rubric": "v1", "started": "2026-10-25"}, "verdicts": {}, "list": []}
if os.path.exists(VPATH):
    store = json.load(open(VPATH))


def rubric_flag(legs):
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
    if q.get("dip_rank"):
        origin.append(f"Dip pool #{q['dip_rank']}")
    if q.get("pro_rank"):
        origin.append(f"Pro pool #{q['pro_rank']} ({q.get('pro_grade') or ''})")
    v["theme"] = q.get("theme_name", "")
    v["theme_id"] = q.get("theme", "")
    v["origin"] = f"🌐 {q.get('theme_name', 'Macro & Themes')}" + (
        " · " + " · ".join(origin) if origin else "")
    v["date"] = today
    store["verdicts"][t] = v

buys = sorted((t for t, v in store["verdicts"].items() if v["flag"] == "BUY"),
              key=lambda t: order.get(t, 10 ** 9))
store["all_buys"] = buys
store["list"] = buys[:50]
store["reserves"] = buys[50:]
store["meta"]["updated"] = today
json.dump(store, open(VPATH, "w"), indent=1)

counts = {"BUY": 0, "HOLD": 0, "SELL": 0}
for v in store["verdicts"].values():
    counts[v["flag"]] = counts.get(v["flag"], 0) + 1
print(f"theme researched: {len(store['verdicts'])} | BUY {counts['BUY']} · "
      f"HOLD {counts['HOLD']} · SELL {counts['SELL']} | list50: {len(store['list'])} | reserves: {len(store['reserves'])}")
print("this batch:", ", ".join(f"{v['ticker']}={v['flag']}" for v in batch))
