#!/usr/bin/env python3
"""Build data/theme_queue.json from data/themes.json + the merged
validation queue. Each theme candidate gets its queue position in the
Macro & Themes research loop, its dip/pro origin references, and the
reference price (Fri close from pools/queue)."""
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")

themes = json.load(open(os.path.join(DATA, "themes.json")))
queue = json.load(open(os.path.join(DATA, "validation_queue.json")))
qix = {q["ticker"]: q for q in queue}
names = {}
try:
    uni = json.load(open(os.path.join(DATA, "universe_cache.json")))
    names = uni
except Exception:
    pass

out = []
n = 0
for th in themes["themes"]:
    for i, c in enumerate(th["candidates"], 1):
        t = c["ticker"]
        q = qix.get(t, {})
        n += 1
        out.append({
            "ticker": t,
            "theme": th["id"],
            "theme_name": th["name"],
            "theme_rank": i,
            "order": n,
            "fit": c["fit"],
            "name": names.get(t, t),
            "dip_rank": q.get("dip_rank"),
            "pro_rank": q.get("pro_rank"),
            "pro_grade": q.get("pro_grade"),
            "last": q.get("last"),
            "off_high_pct": q.get("off_high_pct"),
        })

json.dump(out, open(os.path.join(DATA, "theme_queue.json"), "w"), indent=1)
print(f"theme queue: {len(out)} names across {len(themes['themes'])} themes")
for th in themes["themes"]:
    ts = [e["ticker"] for e in out if e["theme"] == th["id"]]
    print(f"  {th['name']}: {', '.join(ts)}")
