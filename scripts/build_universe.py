#!/usr/bin/env python3
"""Build the watchlist universe: top 800 by dollar volume UNION S&P 1000.

Universe basis (user decision, 2026-10-25): keep the original volume
criteria — the 800 most-traded US common stocks by average daily dollar
volume — and add every S&P 1000 constituent (S&P MidCap 400 + S&P
SmallCap 600). The index leg fixes the volume funnel's blind spot: solid
mid/small caps like DORM and ALG (both SmallCap 600) trade under the
top-800 volume bar but are committee-vetted for float and liquidity.

The volume leg is REBUILT here, not reused from the old cache: the
original cache silently lost mega-caps (UNH ~$2.4B/day, V ~$2.3B/day,
TRV, UPS were absent) to fetch failures during its one-time ranking.
This builder fetches with retries, logs every failure by ticker into the
audit file, and refuses to write a universe if the ranking stage looks
broken (fewer than 700 ranked names).

Sources: NASDAQ Trader symbol directory (all listed common stocks);
Wikipedia constituent lists for the two S&P indices, cross-checked
against the directory; Yahoo chart API for the volume ranking (light
2-month fetch, ADV = mean daily dollar volume over the last 20 sessions).

Output: data/universe_cache.json (ticker -> name, the file
pipeline.load_universe reads) and data/index1000.json (audit: index
membership, ranking cutoff, fetch failures, diff vs the previous cache).

Standard library only. Run manually or monthly; the daily refresh never
rewrites the universe on its own.
"""
import csv
import io
import json
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from html.parser import HTMLParser

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

INDEX_PAGES = [
    ("S&P MidCap 400", "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"),
    ("S&P SmallCap 600", "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"),
]

# Name-filter false positives: real common stocks whose names trip the
# whole-word screen. PFBC is Preferred Bank — a bank, not a preferred share.
SPECIAL_CASES = {"PFBC": "Preferred Bank - Common Stock"}

# Whole-word matching matters: naive substrings reject Netflix ("etf"),
# United* ("unit"), Brighthouse / Curtiss-Wright ("right") and
# Communities ("unit") — all ordinary common stocks.
_NONCOMMON = re.compile(r"\b(warrants?|units?|rights?|preferred|etf|funds?)\b"
                        r"|note due", re.IGNORECASE)

TOP_N = 800


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def _fetch_directory_file(fname, min_rows):
    """Fetch one symbol-directory file with retries. The NASDAQ Trader
    endpoint intermittently returns a truncated/empty body or drops the
    connection under rapid successive requests; a short body must never
    be accepted silently — it would shrink the universe without an error."""
    last = ""
    for attempt in range(4):
        try:
            text = http_get(f"https://www.nasdaqtrader.com/dynamic/symdir/{fname}")
            rows = text.count("\n")
            if rows >= min_rows and "File Creation Time" in text:
                return text
            last = f"{rows} rows"
        except Exception as exc:  # throttling can also drop the connection
            last = type(exc).__name__
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{fname}: directory fetch stayed implausible ({last})")


DIRECTORY_SOURCE = "live"


def directory():
    """All US-listed common stocks from the NASDAQ Trader symbol directory:
    no test issues, no ETFs, no warrants/units/rights/preferreds/notes/funds.
    If the live endpoint stays throttled/implausible, fall back to the
    committed snapshot (data/directory_snapshot.csv, same directory as
    captured by the research pipeline) rather than shrinking silently."""
    global DIRECTORY_SOURCE
    out = {}
    try:
        for fname, min_rows in (("nasdaqlisted.txt", 4000),
                                ("otherlisted.txt", 6000)):
            text = _fetch_directory_file(fname, min_rows)
            for row in csv.DictReader(io.StringIO(text), delimiter="|"):
                sym = (row.get("Symbol") or row.get("ACT Symbol") or "").strip()
                name = (row.get("Security Name") or "").strip()
                if not sym or not name or sym == "Symbol":
                    continue
                if (row.get("Test Issue") or "N") == "Y":
                    continue
                if (row.get("ETF") or "N") == "Y":
                    continue
                if "%" in name or _NONCOMMON.search(name):
                    continue
                out[sym.replace(".", "-")] = name
        if len(out) < 5000:
            raise RuntimeError(f"live directory implausibly small ({len(out)})")
    except Exception as exc:
        print(f"live directory unavailable ({exc}); using snapshot", flush=True)
        DIRECTORY_SOURCE = "snapshot"
        out = {}
        snap = os.path.join(DATA, "directory_snapshot.csv")
        for row in csv.DictReader(open(snap)):
            if row.get("kind") != "stock":
                continue
            sym = (row.get("ticker") or "").strip()
            name = (row.get("name") or "").strip()
            if not sym or not name or "%" in name or _NONCOMMON.search(name):
                continue
            out[sym.replace(".", "-")] = name
    for sym, name in SPECIAL_CASES.items():
        out.setdefault(sym, name)
    return out


class Tables(HTMLParser):
    """Collect rows (lists of cell texts) per table, so the caller can pick
    the constituents table — the pages also carry a 'selected changes'
    table whose first cells are former members and dates."""

    def __init__(self):
        super().__init__()
        self.tables = []
        self._table = None
        self._row = None
        self._cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in ("td", "th") and self._row is not None and self._cell is None:
            self._cell = []

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None:
            self._row.append("".join(self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            self.tables.append(self._table)
            self._table = None


TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,6}$")


def index_members(url, valid):
    parser = Tables()
    parser.feed(http_get(url))
    best = []
    for table in parser.tables:
        header = " ".join(table[0]).lower() if table else ""
        if "symbol" in header and len(table) > len(best):
            best = table
    out, dropped = set(), []
    for row in best[1:]:
        sym = row[0].strip().upper()
        if not TICKER_RE.match(sym):
            continue
        norm = sym.replace(".", "-")
        if norm in valid:
            out.add(norm)
        else:
            dropped.append(sym)
    return out, dropped


def adv20(ticker):
    """Mean daily dollar volume over the last 20 sessions (light fetch)."""
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
           f"{ticker}?range=2mo&interval=1d")
    last_exc = None
    for attempt in range(3):
        try:
            j = json.loads(http_get(url, timeout=20))
            res = (j.get("chart") or {}).get("result") or []
            if not res:
                raise ValueError("no chart result")
            quote = (res[0].get("indicators") or {}).get("quote") or [{}]
            closes = quote[0].get("close") or []
            vols = quote[0].get("volume") or []
            dv = [c * v for c, v in zip(closes, vols) if c and v]
            if len(dv) < 10:
                raise ValueError("too few sessions")
            tail = dv[-20:]
            return sum(tail) / len(tail)
        except Exception as exc:
            last_exc = exc
            time.sleep(2 * (attempt + 1))
    raise last_exc


def rank_by_volume(tickers):
    adv, failures = {}, []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for t, val in zip(tickers, ex.map(lambda x: _try_adv(x), tickers)):
            if val is None:
                failures.append(t)
            else:
                adv[t] = val
    ranked = sorted(adv, key=lambda t: -adv[t])
    return ranked, adv, failures


def _try_adv(t):
    try:
        return adv20(t)
    except Exception:
        return None


def main():
    valid = directory()
    print(f"directory common stocks: {len(valid)}", flush=True)
    cache_path = os.path.join(DATA, "universe_cache.json")
    old = json.load(open(cache_path)) if os.path.exists(cache_path) else {}

    members = {}
    for label, url in INDEX_PAGES:
        found, dropped = index_members(url, valid)
        for t in found:
            members.setdefault(t, label)
        print(f"{label}: {len(found)} matched in directory; "
              f"dropped as not-in-directory: {dropped if dropped else 'none'}",
              flush=True)
    print(f"S&P 1000 total: {len(members)}", flush=True)

    print("ranking by dollar volume (this takes a while)...", flush=True)
    ranked, adv, failures = rank_by_volume(sorted(valid))
    print(f"ranked: {len(ranked)}  fetch failures (logged, excluded): "
          f"{len(failures)}", flush=True)
    if len(ranked) < 700:
        raise RuntimeError(f"ranking looks broken ({len(ranked)} names); "
                           "refusing to overwrite the universe")
    top = ranked[:TOP_N]
    cutoff = adv[top[-1]] / 1e6
    print(f"top {TOP_N} cutoff ADV: ${cutoff:,.0f}M/day", flush=True)

    universe = {}
    for t in top:                       # volume leg first, in rank order
        universe[t] = valid[t]
    added = []
    for t in sorted(members):           # index-only names appended A-Z
        if t not in universe:
            universe[t] = valid[t]
            added.append(t)
    print(f"union universe: {len(universe)} (new via index: {len(added)})",
          flush=True)

    json.dump(universe, open(cache_path, "w"), indent=0)
    json.dump({
        "meta": {"built": date.today().isoformat(),
                 "basis": "top 800 by ADV (rebuilt) union S&P 1000",
                 "directory_source": DIRECTORY_SOURCE,
                 "directory": len(valid), "ranked": len(ranked),
                 "fetch_failures": failures,
                 "top800_cutoff_adv_m": round(cutoff, 1),
                 "sp1000_members": len(members), "universe": len(universe),
                 "entered_vs_old_cache": sorted(set(universe) - set(old)),
                 "left_vs_old_cache": sorted(set(old) - set(universe))},
        "members": members,
    }, open(os.path.join(DATA, "index1000.json"), "w"), indent=1)
    for probe in ("DORM", "ALG", "UNH", "V", "TRV", "UPS", "ASML", "SPCX"):
        where = members.get(probe) or ("top-800 volume" if probe in top else
                                        "ABSENT")
        print(f"  {probe}: {where}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
