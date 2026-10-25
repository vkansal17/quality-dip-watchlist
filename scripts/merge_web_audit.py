#!/usr/bin/env python3
"""Merge one web-audit batch into data/web_crosscheck.json.

Usage: python3 scripts/merge_web_audit.py <batch.json>
Batch entries: {"ticker", "store", "legs": {leg: {"status", "note"}},
"issues": [...], "overall": "supported|mixed|unsupported"}.
"""
import json
import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", "web_crosscheck.json")

store = {}
if os.path.exists(OUT):
    store = json.load(open(OUT))
batch = json.load(open(sys.argv[1]))
today = date.today().isoformat()
for e in batch:
    e["date"] = today
    store[f"{e.get('store', 'validated')}:{e['ticker']}"] = e
json.dump(store, open(OUT, "w"), indent=1)
from collections import Counter
print("web audits stored:", len(store),
      Counter(e.get("overall") for e in store.values()))
print("this batch:", ", ".join(
    f"{e['ticker']}={e.get('overall')}" for e in batch))
