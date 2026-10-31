#!/usr/bin/env python3
"""Quality-Dip Watchlist pipeline — single file, standard library only.

Screens the most-traded US common stocks for famous, fundamentally strong
companies that have dipped significantly for situational reasons, computes
watchlist KPIs, and builds a static site (index + one detail page per stock)
for GitHub Pages.

Method in one paragraph: universe = the 800 most-traded common stocks by
average daily dollar volume UNION every S&P 1000 constituent (MidCap 400 +
SmallCap 600), rebuilt monthly by scripts/build_universe.py (about 1,600
names; the index leg admits committee-vetted mid/small caps like DORM that
trade under the volume bar); candidates are 10-50% below their 52-week high, not in
freefall (20d momentum > -20%), RSI <= 65, price >= $5, 30d volatility <= 65%.
A composite score rewards dip depth in the 15-35% sweet spot, liquidity
(weighted heavily — the brief is famous brands), stabilization and limited
long-term trend damage. A hand-researched core
(notes in data/research_notes.json) leads the list while its members stay
eligible; two researched exclusions (NKE, TSLA — deteriorating/polarized
stories, not situational dips) are kept off. Analyst consensus and Fortune
500 ranks are curated data files (data/consensus.json, data/fortune500.json);
prices, KPIs, earnings dates (Nasdaq calendar) and P/E (SEC EDGAR diluted
EPS, TTM) refresh on every run.

Run:  python3 scripts/pipeline.py [--skip-pe] [--workers 8]
Outputs (repo root): index.html, stock/<TICKER>.html, data/watchlist.json
Prints a change summary (entrants/exits/movers) for the daily report.
"""
import argparse
import csv
import io
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
UA = "Mozilla/5.0 (compatible; quality-dip-watchlist/1.0)"
EDGAR_UA = "Personal Research Contact research-2026@example.com"

EXCLUDE = {"NKE", "TSLA"}
UNIVERSE_NAME = {}
PINNED = ["AVGO", "NVDA", "META", "GOOGL", "CAT", "BKNG", "ASML", "AMZN",
          "GE", "TMUS", "NFLX", "MCD"]

INDUSTRY = {
    "AVGO": "Semiconductors", "NVDA": "Semiconductors", "META": "Interactive Media",
    "GOOGL": "Interactive Media", "GOOG": "Interactive Media", "CAT": "Machinery",
    "BKNG": "Travel Booking", "ASML": "Semiconductor Equipment", "AMZN": "E-commerce / Cloud",
    "GE": "Aerospace & Defense", "TMUS": "Telecommunications", "NFLX": "Streaming / Entertainment",
    "MCD": "Restaurants", "GS": "Investment Banking", "CSCO": "Networking Equipment",
    "BA": "Aerospace & Defense", "SNPS": "Software — EDA", "ABT": "Health Care",
    "AXP": "Payments / Financials", "SPCX": "Space / Aerospace", "MRNA": "Biotechnology",
    "RTX": "Aerospace & Defense", "MELI": "E-commerce", "COF": "Banking / Financials",
    "NXPI": "Semiconductors", "BX": "Asset Management", "MCHP": "Semiconductors",
    "CMCSA": "Media / Telecom", "CDNS": "Software — EDA", "DASH": "Food Delivery",
    "PEP": "Beverages / Food", "QCOM": "Semiconductors", "SNOW": "Cloud Data Software",
    "NU": "Fintech / Banking", "MDB": "Database Software", "ADI": "Semiconductors",
    "C": "Banking", "MS": "Investment Banking", "CEG": "Utilities / Power",
    "MDT": "Medical Devices", "GEV": "Power Equipment", "CMI": "Machinery / Engines",
    "PLD": "Industrial REIT", "O": "Retail REIT", "EQIX": "Data Center REIT",
    "TTWO": "Video Games", "TLN": "Power Generation", "HBAN": "Regional Banking",
    "APO": "Asset Management", "GIS": "Packaged Food",
}

# ---------------------------------------------------------------------------
# HTTP + market data helpers (adapted from the research pipeline)
# ---------------------------------------------------------------------------

def http_get(url, timeout=25, ua=UA):
    req = urllib.request.Request(url, headers={
        "User-Agent": ua, "Accept": "application/json,text/plain,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def yahoo_symbol(t):
    return t.replace("/", "-").replace(".", "-")


class Bar:
    __slots__ = ("date", "open", "high", "low", "close", "volume")

    def __init__(self, d, o, h, l, c, v):
        self.date, self.open, self.high, self.low = d, o, h, l
        self.close, self.volume = c, v


def fetch_bars(ticker, range_="1y", interval="1d"):
    sym = yahoo_symbol(ticker)
    last_err = None
    for host in ("query2.finance.yahoo.com", "query1.finance.yahoo.com"):
        url = (f"https://{host}/v8/finance/chart/{sym}"
               f"?range={range_}&interval={interval}&events=div,splits")
        try:
            payload = json.loads(http_get(url, timeout=25).decode("utf-8", "replace"))
            result = payload["chart"]["result"][0]
            ts = result.get("timestamp") or []
            q = result["indicators"]["quote"][0]
            bars = []
            for i, tstamp in enumerate(ts):
                vals = [q[k][i] for k in ("open", "high", "low", "close", "volume")]
                if any(v is None for v in vals):
                    continue
                d = datetime.fromtimestamp(tstamp, tz=timezone.utc).strftime("%Y-%m-%d")
                bars.append(Bar(d, float(vals[0]), float(vals[1]), float(vals[2]),
                                float(vals[3]), float(vals[4])))
            if len(bars) < 60:
                raise ValueError(f"only {len(bars)} usable bars")
            return bars
        except Exception as exc:  # noqa: BLE001
            last_err = exc
    raise ValueError(f"fetch_bars({ticker}) failed: {last_err}")


def fetch_bars_retry(ticker, range_="1y", attempts=3):
    for i in range(attempts):
        try:
            return fetch_bars(ticker, range_)
        except Exception:
            if i == attempts - 1:
                raise
            time.sleep(1.5 * (i + 1))


def rsi_wilder(closes, period=14):
    if len(closes) <= period:
        return float("nan")
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


_NAME_SUFFIXES = [
    " - Class A Common Stock", " - Class B Common Stock", " - Class C Common Stock",
    " - Common Stock", " Common Stock", " - Ordinary Shares", " Ordinary Shares",
    " - Class A Ordinary Shares", " - American Depositary Shares",
    " American Depositary Shares", ", Inc.", " Inc.", " Corporation",
    " - Class A Shares", " - Class B Shares", " Class A Common Stock",
]


def clean_company_name(name):
    out = name
    changed = True
    while changed:
        changed = False
        for suf in _NAME_SUFFIXES:
            if out.endswith(suf):
                out = out[: -len(suf)].rstrip(" ,-")
                changed = True
    out = re.sub(r"\s+-\s+.*$", "", out) if " - " in out and len(out) > 34 else out
    return out.strip()


def load_universe(limit=800):
    """The universe lives in data/universe_cache.json, (re)built by
    scripts/build_universe.py: the top 800 common stocks by trailing
    dollar volume (ranked fresh, with fetch failures logged — the first
    cache silently lost UNH/V/TRV to failed fetches) UNION all S&P 1000
    constituents. Rebuilt monthly or on demand; the daily run only reads
    the cache."""
    cache = os.path.join(DATA, "universe_cache.json")
    if os.path.exists(cache):
        return json.load(open(cache))
    raise SystemExit("data/universe_cache.json is missing — build it with "
                     "scripts/build_universe.py (a silent directory-order "
                     "fallback is how the first cache lost UNH/V/TRV)")


# ---------------------------------------------------------------------------
# Screening and KPIs
# ---------------------------------------------------------------------------

def kpis_from_bars(bars):
    closes = [b.close for b in bars]
    last = closes[-1]
    prev = closes[-2]
    high52 = max(closes)
    low52 = min(closes)
    vols = []
    for period in (10, 30):
        seg = closes[-(period + 1):]
        rets = [math.log(seg[i] / seg[i - 1]) for i in range(1, len(seg))]
        mean = sum(rets) / len(rets)
        var = sum((x - mean) ** 2 for x in rets) / max(1, len(rets) - 1)
        vols.append(math.sqrt(var) * math.sqrt(252) * 100)
    dv = [b.close * b.volume for b in bars[-20:]]
    sma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else sum(closes) / len(closes)
    vpt = [0.0]
    for i in range(1, len(bars)):
        prev_c = bars[i - 1].close
        vpt.append(vpt[-1] + (bars[i].volume * (bars[i].close - prev_c) / prev_c
                              if prev_c else 0.0))
    if len(vpt) > 21:
        inc = sum(abs(vpt[i] - vpt[i - 1]) for i in range(len(vpt) - 20, len(vpt)))
        vpt20 = round((vpt[-1] - vpt[-21]) / inc * 100, 1) if inc > 0 else 0.0
    else:
        vpt20 = 0.0
    return {
        "last": round(last, 2),
        "day_pct": round((last / prev - 1) * 100, 2),
        "high52": round(high52, 2),
        "low52": round(low52, 2),
        "off_high_pct": round((last / high52 - 1) * 100, 1),
        "range_pos": round((last - low52) / (high52 - low52) * 100) if high52 > low52 else 100,
        "rsi14": round(rsi_wilder(closes), 1),
        "mom20_pct": round((last / closes[-21] - 1) * 100, 1) if len(closes) > 21 else 0.0,
        "perf1y_pct": round((last / closes[0] - 1) * 100, 1),
        "vs_sma200_pct": round((last / sma200 - 1) * 100, 1),
        "adv_m": round((sum(dv) / len(dv)) / 1e6, 1),
        "vol10": round(vols[0], 1),
        "vol30": round(vols[1], 1),
        "vpt20": vpt20,
        "spark": [round(x, 2) for x in closes[-63:]],
        "chart": [round(x, 2) for x in closes],
        "asof": bars[-1].date,
    }


def dip_score(k):
    # Liquidity carries real weight: the brief is famous, heavily-traded
    # brands, so a perfectly-shaped dip in a thin name must not outrank a
    # slightly-less-perfect dip in a mega-cap.
    off = -k["off_high_pct"]
    sweet = max(0.0, 30 - abs(off - 24) * 1.0)
    liquid = min(26.0, math.log10(max(k["adv_m"], 1)) * 8.5)
    stab = max(0.0, 10 - abs(k["mom20_pct"]) * 0.7) if k["mom20_pct"] > -20 else 0.0
    trend = max(0.0, 8 + k["vs_sma200_pct"] * 0.35)
    calm = max(0.0, 6 - max(0.0, k["vol30"] - 28) * 0.1)
    return round(sweet + liquid + stab + trend + calm, 1)


def is_candidate(k):
    off = -k["off_high_pct"]
    return (10 <= off <= 50 and k["mom20_pct"] > -20 and k["rsi14"] <= 65
            and k["last"] >= 5 and k["adv_m"] >= 25 and k["vol30"] <= 65)


def earnings_dates(days=120):
    """Nasdaq's calendar endpoint answers for ONE day per call and its rows
    carry no date field — the date is the query. Scan day by day; the first
    (earliest) sighting of a symbol wins."""
    out = {}
    today = date.today()
    dates = [today + timedelta(days=i) for i in range(days)]

    def one(d):
        try:
            j = json.loads(http_get(
                f"https://api.nasdaq.com/api/calendar/earnings?date={d.isoformat()}",
                timeout=20).decode("utf-8", "replace"))
            return d, (j.get("data") or {}).get("rows") or []
        except Exception:
            return d, []

    with ThreadPoolExecutor(max_workers=6) as ex:
        for d, rows in ex.map(one, dates):
            for row in rows:
                sym = (row.get("symbol") or "").strip()
                if sym and sym not in out:
                    out[sym] = d.isoformat()
    return out


_EDGAR_CACHE = {}


def edgar_pe(ticker, price):
    try:
        if not _EDGAR_CACHE:
            raw = http_get("https://www.sec.gov/files/company_tickers.json",
                           timeout=25, ua=EDGAR_UA)
            for e in json.loads(raw.decode("utf-8", "replace")).values():
                _EDGAR_CACHE[e["ticker"]] = str(e["cik_str"]).zfill(10)
        cik = _EDGAR_CACHE.get(ticker)
        if not cik:
            return None
        raw = http_get(
            f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}"
            "/us-gaap/EarningsPerShareDiluted.json", timeout=25, ua=EDGAR_UA)
        units = json.loads(raw.decode("utf-8", "replace"))["units"]["USD/shares"]
        periods = []
        for u in units:
            try:
                start = datetime.strptime(u["start"], "%Y-%m-%d")
                end = datetime.strptime(u["end"], "%Y-%m-%d")
                days = (end - start).days
                if 80 <= days <= 100 and u.get("form") in ("10-Q", "10-K"):
                    periods.append((end, float(u["val"])))
            except (KeyError, ValueError):
                continue
        if not periods:
            return None
        latest_end = max(p[0] for p in periods)
        seen, byend = {}, {}
        for end, val in periods:
            byend[end] = val
        ends = sorted(byend)[-4:]
        if byend[ends[-1]] != byend.get(latest_end):
            return None
        eps_ttm = sum(byend[e] for e in ends)
        if eps_ttm <= 0:
            return None
        pe = price / eps_ttm
        return round(pe, 1) if 0 < pe < 500 else None
    except Exception:
        return None


def _edgar_cik(ticker):
    if not _EDGAR_CACHE:
        raw = http_get("https://www.sec.gov/files/company_tickers.json",
                       timeout=25, ua=EDGAR_UA)
        for e in json.loads(raw.decode("utf-8", "replace")).values():
            _EDGAR_CACHE[e["ticker"]] = str(e["cik_str"]).zfill(10)
    return _EDGAR_CACHE.get(ticker)


def _edgar_units(cik, tag):
    try:
        raw = http_get(
            f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}"
            f"/us-gaap/{tag}.json", timeout=25, ua=EDGAR_UA)
        return json.loads(raw.decode("utf-8", "replace"))["units"]["USD"]
    except Exception:
        return None


def _latest_annual(units):
    best = None
    for u in units or []:
        try:
            if u.get("form") != "10-K":
                continue
            s = datetime.strptime(u["start"], "%Y-%m-%d")
            e = datetime.strptime(u["end"], "%Y-%m-%d")
            if 330 <= (e - s).days <= 400 and (best is None or e > best[0]):
                best = (e, float(u["val"]))
        except (KeyError, ValueError):
            continue
    return best[1] if best else None


def _latest_instant(units):
    best = None
    for u in units or []:
        try:
            if u.get("form") not in ("10-K", "10-Q"):
                continue
            e = datetime.strptime(u["end"], "%Y-%m-%d")
            if best is None or e > best[0]:
                best = (e, float(u["val"]))
        except (KeyError, ValueError):
            continue
    return best[1] if best else None


def edgar_roic(ticker):
    """ROIC = operating income x (1 - effective tax rate) / invested capital,
    invested capital = total debt + equity - cash. SEC annuals; None if the
    filings don't provide the pieces."""
    try:
        cik = _edgar_cik(ticker)
        if not cik:
            return None
        oi = _latest_annual(_edgar_units(cik, "OperatingIncomeLoss"))
        if oi is None:
            return None
        pretax = _latest_annual(_edgar_units(
            cik, "IncomeLossFromContinuingOperationsBeforeIncomeTaxes"
                 "ExtraordinaryItemsNoncontrollingInterest"))
        tax = _latest_annual(_edgar_units(cik, "IncomeTaxExpenseBenefit"))
        rate = (tax / pretax) if (tax is not None and pretax and pretax > 0) else 0.21
        rate = min(max(rate, 0.0), 0.40)
        equity = _latest_instant(_edgar_units(cik, "StockholdersEquity"))
        if equity is None:
            equity = _latest_instant(_edgar_units(
                cik, "StockholdersEquityIncludingPortionAttributableTo"
                     "NoncontrollingInterest"))
        cash = _latest_instant(_edgar_units(
            cik, "CashAndCashEquivalentsAtCarryingValue")) or 0.0
        debt_parts = []
        for tag in ("LongTermDebtNoncurrent", "DebtCurrent",
                    "LongtermBorrowingsCurrent", "LongTermBorrowings"):
            v = _latest_instant(_edgar_units(cik, tag))
            if v is not None:
                debt_parts.append(v)
                if len(debt_parts) >= 2:
                    break
        if equity is None or not debt_parts:
            return None
        capital = sum(debt_parts) + equity - cash
        if capital <= 0:
            return None
        roic = oi * (1 - rate) / capital * 100
        return round(roic, 1) if -60 < roic < 120 else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Professional lens — factor model in the spirit of Seeking Alpha Quant /
# Zacks: profitability, estimate-revision momentum, value, safety (Piotroski
# F-score + leverage), growth, momentum; veto grades exclude names outright.
# Fundamentals come from SEC EDGAR *frames* (one call per concept per period
# covers every filer) merged with per-company balance-sheet snapshots.
# Revisions use the free proxy that exists: reported EPS surprise history
# from the Nasdaq calendar (true analyst revision breadth is paid data).
# ---------------------------------------------------------------------------

def _frame(tag, unit, period):
    try:
        j = json.loads(http_get(
            f"https://data.sec.gov/api/xbrl/frames/us-gaap/{tag}/{unit}/{period}.json",
            timeout=40, ua=EDGAR_UA).decode("utf-8", "replace"))
        return j.get("data") or []
    except Exception:
        return []


def _cik_to_ticker(prefer=()):
    if not _EDGAR_CACHE:
        _edgar_cik("AAPL")
    inv = {}
    for t, cik in _EDGAR_CACHE.items():
        c = int(cik)
        cur = inv.get(c)
        # Preferred shares and funds can share the parent's CIK (JPM-PC, VYLD
        # -> JPMorgan). The common stock must win the inversion.
        if (cur is None or (t in prefer and cur not in prefer)
                or ("-" in cur and "-" not in t)):
            inv[c] = t
    return inv


def fetch_fundamentals(tickers_wanted):
    """Returns {ticker: fundamentals dict} using EDGAR frames."""
    inv = _cik_to_ticker(prefer=tickers_wanted)
    dur_tags = {
        "rev": ["RevenueFromContractWithCustomerExcludingAssessedTax",
                "Revenues", "SalesRevenueNet"],
        "gp": ["GrossProfit"], "oi": ["OperatingIncomeLoss"],
        "ni": ["NetIncomeLoss", "ProfitLoss"],
        "cfo": ["NetCashProvidedByUsedInOperatingActivities",
                "NetCashProvidedByOperatingActivities"],
        "capex": ["PaymentsToAcquirePropertyPlantAndEquipment"],
        "da": ["DepreciationDepletionAndAmortization",
               "DepreciationAmortizationAndAccretionNet",
               "DepreciationAndAmortization", "Depreciation"],
        "tax": ["IncomeTaxExpenseBenefit"],
        "int": ["InterestExpense", "InterestExpenseNonoperating"],
        "shares": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
        "cogs": ["CostOfGoodsAndServicesSold", "CostOfRevenue"],
    }
    years = ["CY2025", "CY2024", "CY2023"]
    dur = {}  # key -> {year: {cik: val}}
    for key, tags in dur_tags.items():
        unit = "shares" if key == "shares" else "USD"
        for yr in years:
            merged = {}
            for tag in tags:
                for row in _frame(tag, unit, yr):
                    merged.setdefault(row["cik"], row["val"])
            dur[(key, yr)] = merged
    inst_tags = {
        "assets": ["Assets"], "ca": ["AssetsCurrent"], "cl": ["LiabilitiesCurrent"],
        "equity": ["StockholdersEquity",
                   "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
        "cash": ["CashAndCashEquivalentsAtCarryingValue"],
        "ltd": ["LongTermDebtNoncurrent", "LongTermDebt", "LongTermBorrowings"],
        "debtcur": ["DebtCurrent"],
    }
    quarters = ["CY2025Q1I", "CY2025Q2I", "CY2025Q3I", "CY2025Q4I",
                "CY2026Q1I", "CY2026Q2I"]
    inst = {}  # key -> {cik: {qidx: val}}
    for key, tags in inst_tags.items():
        per_cik = {}
        for qi, q in enumerate(quarters):
            for tag in tags:
                for row in _frame(tag, "USD", q):
                    per_cik.setdefault(row["cik"], {}).setdefault(qi, row["val"])
        inst[key] = per_cik

    def lp(key, cik):
        vals = [(yr, dur[(key, yr)][cik]) for yr in years if cik in dur[(key, yr)]]
        if not vals:
            return None, None, None
        latest = vals[0][1]
        prior = vals[1][1] if len(vals) > 1 else None
        prior2 = vals[2][1] if len(vals) > 2 else None
        return latest, prior, prior2

    def snap(key, cik):
        qs = inst[key].get(cik)
        if not qs:
            return None, None
        latest_q = max(qs)
        prior = qs.get(latest_q - 4)
        if prior is None:
            older = [q for q in qs if q <= latest_q - 2]
            prior = qs[max(older)] if older else None
        return qs[latest_q], prior

    out = {}
    for cik, t in inv.items():
        if t not in tickers_wanted:
            continue
        f = {}
        for key in dur_tags:
            f[key], f[key + "_p"], f[key + "_p2"] = lp(key, cik)
        for suf in ("", "_p", "_p2"):
            if (f["gp" + suf] is None and f["rev" + suf] is not None
                    and f["cogs" + suf] is not None):
                f["gp" + suf] = f["rev" + suf] - f["cogs" + suf]
        for key in inst_tags:
            f[key], f[key + "_p"] = snap(key, cik)
        if f["ni"] is not None and f["assets"]:
            out[t] = f
    return out


def piotroski(f):
    """Classic 9-signal F-score. Needs latest+prior income and two balance
    sheet snapshots; returns None when the pieces aren't there."""
    try:
        a, a_p = f["assets"], f["assets_p"]
        if not a or not a_p or f["ni"] is None or f["ni_p"] is None or f["cfo"] is None:
            return None
        s = 0
        roa, roa_p = f["ni"] / a, f["ni_p"] / a_p
        s += roa > 0
        s += f["cfo"] > 0
        s += roa > roa_p
        s += f["cfo"] > f["ni"]  # earnings backed by cash, not accruals
        lev = (f["ltd"] or 0) / a
        lev_p = (f["ltd_p"] or 0) / a_p
        s += lev < lev_p
        if f["ca"] and f["cl"] and f["ca_p"] and f["cl_p"]:
            s += (f["ca"] / f["cl"]) > (f["ca_p"] / f["cl_p"])
        if f["shares"] and f["shares_p"]:
            s += f["shares"] <= f["shares_p"] * 1.005  # no dilution
        if f["gp"] and f["rev"] and f["gp_p"] and f["rev_p"]:
            s += (f["gp"] / f["rev"]) > (f["gp_p"] / f["rev_p"])
        if f["rev"] and f["rev_p"]:
            s += (f["rev"] / a) > (f["rev_p"] / a_p)
        return int(s)
    except (TypeError, ZeroDivisionError):
        return None


def surprise_history(days=380):
    """Backward Nasdaq calendar scan -> {symbol: [(date, surprise%), ...]}
    most recent first. Reported EPS vs consensus at the time."""
    out = {}
    today = date.today()
    dates = [today - timedelta(days=i) for i in range(1, days)]

    def one(d):
        try:
            j = json.loads(http_get(
                f"https://api.nasdaq.com/api/calendar/earnings?date={d.isoformat()}",
                timeout=20).decode("utf-8", "replace"))
            return d, (j.get("data") or {}).get("rows") or []
        except Exception:
            return d, []

    with ThreadPoolExecutor(max_workers=8) as ex:
        for d, rows in ex.map(one, dates):
            for row in rows:
                sym = (row.get("symbol") or "").strip()
                sp = row.get("surprise")
                if sym and row.get("eps") and sp not in (None, ""):
                    try:
                        out.setdefault(sym, []).append((d.isoformat(), float(sp)))
                    except (TypeError, ValueError):
                        pass
    for sym in out:
        out[sym].sort(reverse=True)
    return out


def _pct_ranks(values):
    """{ticker: value} -> {ticker: percentile 0-100}, higher value = higher pct."""
    items = sorted((v, t) for t, v in values.items() if v is not None)
    n = len(items)
    if n == 0:
        return {}
    out = {}
    for i, (v, t) in enumerate(items):
        out[t] = (i / (n - 1) * 100) if n > 1 else 50.0
    return out


def _grade(pct):
    if pct is None:
        return "—"
    for cut, g in ((93, "A+"), (85, "A"), (77, "B+"), (69, "B"), (60, "C+"),
                   (50, "C"), (40, "D+"), (30, "D"), (20, "D-")):
        if pct >= cut:
            return g
    return "F"


def _grade_cls(g):
    return {"A": "sb", "B": "b", "C": "h", "D": "r", "F": "r"}.get(g[:1], "na")


def _grade_composite(pct):
    """Headline grade bands for the composite percentile. Tighter at the
    top than the factor bands: the Top 50 all sit above the ~93rd
    percentile of the graded pool, and a column where every name reads
    A+ discriminates nothing."""
    if pct is None:
        return "—"
    for cut, g in ((99, "A+"), (96, "A"), (92, "B+"), (87, "B"), (81, "C+"),
                   (74, "C"), (60, "D+"), (45, "D"), (30, "D-")):
        if pct >= cut:
            return g
    return "F"


def build_professional(results, spy_closes):
    """results: {ticker: kpis}. Returns (top50 rows, all graded records)."""
    funds = fetch_fundamentals(set(results))
    surp = surprise_history()
    spy12 = (spy_closes[-21] / spy_closes[-252] - 1) * 100 if len(spy_closes) > 252 else 0.0
    recs = {}
    for t, f in funds.items():
        k = results.get(t)
        if not k:
            continue
        price = k["last"]
        shares = f["shares"]
        shares_p = f["shares_p"]
        if shares and shares < 5e6:
            # Some filers report share counts in thousands despite the
            # "shares" unit (Nutanix: 294,083 filed = 294.1M actual;
            # Repligen: 56,561 = 56.6M); the EDGAR frames API does not
            # apply filing scale attributes. Detect via an implausibly
            # small market cap relative to revenue or total assets.
            mc0 = price * shares
            tiny = mc0 < 1e8 and (
                (f["rev"] and mc0 / f["rev"] < 0.1)
                or (f["assets"] and mc0 < 0.02 * f["assets"])
                or (not f["rev"] and not f["assets"]))
            if tiny:
                shares *= 1000
                shares_p = shares_p * 1000 if shares_p else None
        mktcap = price * shares if shares else None
        fcf = (f["cfo"] - f["capex"]) if (f["cfo"] is not None and f["capex"] is not None) else None
        def _ebitda(suf=""):
            # Operating income + D&A where tagged; otherwise rebuild from
            # net income (NI + interest + tax + D&A) — filers like Nike do
            # not tag OperatingIncomeLoss in the frames years.
            if f["oi" + suf] is not None and f["da" + suf] is not None:
                return f["oi" + suf] + f["da" + suf]
            if all(f[k + suf] is not None for k in ("ni", "tax", "da")):
                # Interest defaults to 0: net-interest reporters (e.g. Nike)
                # tag no gross interest expense at all.
                return (f["ni" + suf] + (f["int" + suf] or 0)
                        + f["tax" + suf] + f["da" + suf])
            return None
        ebitda = _ebitda()
        debt = (f["ltd"] or 0) + (f["debtcur"] or 0)
        net_debt = debt - (f["cash"] or 0)
        ev = (mktcap + net_debt) if mktcap else None
        # Year-ago EV/EBITDA on a consistent basis (prior-year annual
        # EBITDA, price 252 sessions ago, prior share count, prior balance
        # sheet snapshot) — powers the Fallen & Cheap compression gate.
        ebitda_p = _ebitda("_p")
        debt_p = (f["ltd_p"] or 0) + (f["debtcur_p"] or 0)
        net_debt_p = debt_p - (f["cash_p"] or 0)
        _chart = k.get("chart") or []
        # Bars are a 1y window (~252 sessions): the -252 close when the
        # series is long enough, else the first bar (≈1 year ago by
        # construction of the fetch window).
        price_1y = (_chart[-252] if len(_chart) >= 252
                    else _chart[0] if len(_chart) >= 200 else None)
        ev_1y = (price_1y * shares_p + net_debt_p) if (price_1y and shares_p) else None
        ev_ebitda_1y = (ev_1y / ebitda_p) if (ev_1y and ebitda_p
                                              and ebitda_p > 0) else None
        s4 = [s for _, s in surp.get(t, [])[:4]]
        recs[t] = {
            "ticker": t, "fscore": piotroski(f),
            "has_cost_line": (f["gp"] is not None or f["cogs"] is not None),
            "roe": (f["ni"] / f["equity"] * 100) if (f["ni"] is not None and f["equity"]) else None,
            "roic_f": (f["oi"] * 0.79 / (debt + f["equity"] - (f["cash"] or 0)) * 100)
                      if (f["oi"] is not None and f["equity"] and (debt + f["equity"] - (f["cash"] or 0)) > 0) else None,
            "gm": (f["gp"] / f["rev"] * 100) if (f["gp"] is not None and f["rev"]) else None,
            "gm_d": ((f["gp"] / f["rev"] - f["gp_p"] / f["rev_p"]) * 100)
                    if (f["gp"] and f["rev"] and f["gp_p"] and f["rev_p"]) else None,
            "fcfm": (fcf / f["rev"] * 100) if (fcf is not None and f["rev"]) else None,
            "opm": (f["oi"] / f["rev"] * 100) if (f["oi"] is not None and f["rev"]) else None,
            "rev_g": ((f["rev"] / f["rev_p"] - 1) * 100) if (f["rev"] and f["rev_p"]) else None,
            "ni_g": (max(-150.0, min(300.0, (f["ni"] / f["ni_p"] - 1) * 100))
                     if (f["ni"] is not None and f["ni_p"] and f["ni_p"] > 0) else None),
            "fcf_y": (fcf / mktcap * 100) if (fcf is not None and mktcap) else None,
            "earn_y": (f["ni"] / mktcap * 100) if (f["ni"] is not None and mktcap) else None,
            "ev_ebitda": (ev / ebitda) if (ev and ebitda and ebitda > 0) else None,
            "ev_ebitda_1y": ev_ebitda_1y,
            "ev_comp": (((ev / ebitda) / ev_ebitda_1y - 1) * 100
                        if (ev and ebitda and ebitda > 0 and ev_ebitda_1y) else None),
            "ebitda": ebitda,
            "ps": (mktcap / f["rev"]) if (mktcap and f["rev"]) else None,
            "nd_ebitda": (net_debt / ebitda) if (ebitda and ebitda > 0) else None,
            "cover": (f["oi"] / f["int"]) if (f["oi"] is not None and f["int"]) else None,
            "curr": (f["ca"] / f["cl"]) if (f["ca"] and f["cl"]) else None,
            "accrual": ((f["ni"] - f["cfo"]) / f["assets"])
                       if (f["ni"] is not None and f["cfo"] is not None and f["assets"]) else None,
            "surp_mean": (sum(max(-40.0, min(40.0, s)) for s in s4) / len(s4)) if s4 else None,
            "surp_beat": (sum(1 for s in s4 if s > 0) / len(s4) * 100) if s4 else None,
            "surp_n": len(s4), "surp_all_neg": bool(s4) and all(s < 0 for s in s4),
            "mom12_1": (k["chart"][-21] / k["chart"][-252] - 1) * 100 if len(k["chart"]) > 252 else None,
            "fcf": fcf, "ni": f["ni"], "shares_d": ((shares / shares_p - 1) * 100)
                   if (shares and shares_p) else None,
        }
        r = recs[t]
        r["rs12"] = (r["mom12_1"] - spy12) if r["mom12_1"] is not None else None

    def factor(comp):
        """comp: {name: (values_dict, invert)} -> {ticker: avg percentile}"""
        per = {}
        for name, (vals, invert) in comp.items():
            vv = {t: (-v if invert else v) for t, v in vals.items() if v is not None}
            for t, p in _pct_ranks(vv).items():
                per.setdefault(t, []).append(p)
        return {t: sum(ps) / len(ps) for t, ps in per.items() if len(ps) >= 2}

    G = lambda key: {t: r[key] for t, r in recs.items()}  # noqa: E731
    factors = {
        "Profitability": factor({"roe": (G("roe"), False), "roic": (G("roic_f"), False),
                                 "gm": (G("gm"), False), "fcfm": (G("fcfm"), False),
                                 "opm": (G("opm"), False)}),
        "Revisions": factor({"sm": (G("surp_mean"), False), "sb": (G("surp_beat"), False)}),
        "Value": factor({"fy": (G("fcf_y"), False), "ey": (G("earn_y"), False),
                         "ev": (G("ev_ebitda"), True), "ps": (G("ps"), True)}),
        "Safety": factor({"fs": (G("fscore"), False), "nd": (G("nd_ebitda"), True),
                          "cov": (G("cover"), False), "cur": (G("curr"), False),
                          "acc": (G("accrual"), True)}),
        "Growth": factor({"rg": (G("rev_g"), False), "ng": (G("ni_g"), False),
                          "gmd": (G("gm_d"), False)}),
        "Momentum": factor({"m12": (G("mom12_1"), False), "rs": (G("rs12"), False)}),
    }
    weights = {"Profitability": 0.22, "Revisions": 0.20, "Value": 0.20,
               "Safety": 0.15, "Growth": 0.13, "Momentum": 0.10}
    for t, r in recs.items():
        r["factors"] = {fn: factors[fn].get(t) for fn in weights}
        have = {fn: p for fn, p in r["factors"].items() if p is not None}
        r["composite"] = (sum(weights[fn] * p for fn, p in have.items())
                          / sum(weights[fn] for fn in have)) if len(have) >= 4 else None
        r["veto"] = None
    # The letter grade is the composite's percentile rank across the graded
    # pool (relative grading, SA-Quant style): averaging six factor
    # percentiles compresses raw composites, so absolute cutoffs would cap
    # even the best all-rounder at a B+.
    comp_pct = _pct_ranks({t: r["composite"] for t, r in recs.items()
                           if r["composite"] is not None})
    for t, r in recs.items():
        r["grade"] = _grade_composite(comp_pct[t]) if t in comp_pct else None
        if (r["fscore"] is not None and r["fscore"] <= 3 and r["has_cost_line"]):
            # The F-score veto applies to operating companies with a
            # cost-of-sales line; bank/insurer balance sheets break its
            # leverage and liquidity signals, so they are exempt (their
            # F-score still displays, with the financials caveat).
            r["veto"] = f"Piotroski F-score {r['fscore']}/9 — quality veto"
        elif (r["nd_ebitda"] is not None and r["nd_ebitda"] > 6
              and (r["cover"] is None or r["cover"] < 1.5)):
            r["veto"] = f"net debt/EBITDA {r['nd_ebitda']:.1f}× with thin coverage — balance-sheet veto"
        elif r["surp_all_neg"] and r["surp_n"] >= 3 and (r["surp_mean"] or 0) < -8:
            r["veto"] = "three-plus straight EPS misses averaging under −8% — revisions veto"
        elif (r["fcf"] is not None and r["fcf"] < 0 and (r["ni"] or 0) < 0
              and (r["shares_d"] or 0) > 5):
            r["veto"] = "cash-burning, loss-making and diluting (>5% share growth) — burn veto"
    pool = [r for r in recs.values() if r["composite"] is not None and not r["veto"]]
    pool.sort(key=lambda r: r["composite"], reverse=True)
    return pool[:50], recs


def kill_criteria(r, k):
    rules = []
    rules.append("Estimates: two straight negative EPS surprises, or a guidance cut "
                 "at the next print — the professional exit signal.")
    if r["fscore"] is not None and r["fscore"] <= 6:
        rules.append(f"Quality: Piotroski F-score slips below {max(3, r['fscore'] - 2)} "
                     f"(now {r['fscore']}/9).")
    if r["nd_ebitda"] is not None and r["nd_ebitda"] > 2:
        rules.append(f"Balance sheet: net debt/EBITDA rises above {r['nd_ebitda'] + 1.5:.1f}× "
                     f"(now {r['nd_ebitda']:.1f}×).")
    if r["fcf_y"] is not None and r["fcf_y"] < 3:
        rules.append("Cash flow: free cash flow turns negative on a trailing-year basis.")
    if (r["factors"].get("Profitability") or 100) < 45:
        rules.append("Profitability: gross margin compresses for two straight fiscal years.")
    rules.append(f"Price structure: a weekly close below the 52-week low of "
                 f"${k['low52']:,.2f} — structure break, not noise.")
    return rules[:4]


# ---------------------------------------------------------------------------
# Site building
# ---------------------------------------------------------------------------

CSS = """
:root{--bg:#0b1220;--card:#111a2e;--card2:#0e1730;--line:#1e2a45;--txt:#e6ecf7;--mut:#8fa3c8;--acc:#4da3ff;--up:#34d399;--dn:#f87171;--warn:#fbbf24;--rad:12px}
*{box-sizing:border-box} body{margin:0;font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--txt);-webkit-text-size-adjust:100%}
a{color:var(--acc);text-decoration:none} a:hover{text-decoration:underline}
/* ---- v3 shell: left sidebar + content ---- */
.shell{display:flex;align-items:stretch;min-height:100vh}
.sidebar{flex:0 0 232px;width:232px;position:sticky;top:0;height:100vh;overflow-y:auto;background:var(--card2);border-right:1px solid var(--line);padding:18px 14px;display:flex;flex-direction:column;gap:6px;z-index:60}
.brand{font-size:16.5px;font-weight:800;color:var(--txt);letter-spacing:.01em;line-height:1.3}
.brand:hover{text-decoration:none;color:var(--acc)}
.brandsub{color:var(--mut);font-size:11.5px;line-height:1.5;margin:2px 0 12px}
.tabs{display:flex;flex-direction:column;gap:5px}
.tab{padding:9px 12px;border:1px solid transparent;border-radius:10px;color:var(--mut);font-weight:600;font-size:13.5px;white-space:nowrap;display:block}
.tab:hover{text-decoration:none;color:var(--txt);background:var(--card)}
.tab.active{background:var(--acc);color:#081120}
.sidefoot{margin-top:auto;color:var(--mut);font-size:11px;line-height:1.55;padding-top:14px}
.main{flex:1;min-width:0;padding:20px 22px 60px;max-width:1240px}
.wrap{max-width:1280px;margin:0 auto;padding:22px 18px 60px}
h1{font-size:24px;margin:0 0 4px;line-height:1.25} .sub{color:var(--mut);font-size:13px;margin-bottom:14px;line-height:1.55}
.sub b{color:var(--txt)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:12px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:var(--rad);padding:10px 14px}
.card .n{font-size:20px;font-weight:700} .card .l{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.06em;margin-top:2px}
.controls{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0}
.controls input,.controls select{background:var(--card);border:1px solid var(--line);color:var(--txt);border-radius:8px;padding:9px 12px;font-size:14px;min-height:40px}
.controls input{flex:1;min-width:180px}
/* ---- v3 master-detail ---- */
.master{display:grid;grid-template-columns:minmax(0,1fr) 402px;gap:16px;align-items:start}
.tablewrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:var(--rad)}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);border-radius:var(--rad);overflow:hidden}
th,td{padding:7px 9px;font-size:13px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap;vertical-align:middle}
th{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.05em;cursor:pointer;user-select:none;background:var(--card2)}
th:hover{color:var(--txt)} td.num,th.num{text-align:right} td.num{font-variant-numeric:tabular-nums}
tbody tr{cursor:pointer}
tr:hover td{background:#14213d}
tr.sel td{background:#1b2c52}
tr.sel{outline:1px solid var(--acc);outline-offset:-1px}
td.detail{display:none}
td.wrap,th.wrap{white-space:normal}
.sym{font-weight:700;font-size:14.5px} .nm{display:block;color:var(--mut);font-weight:400;font-size:11px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ind{color:var(--mut)} .muted{color:var(--mut)}
.up{color:var(--up)} .dn{color:var(--dn)}
.chip{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600}
.sb{background:#064e3b;color:#6ee7b7}.b{background:#0c3540;color:#67e8f9}.h{background:#3f3413;color:#fcd34d}.r{background:#450a0a;color:#fca5a5}.na{background:#1f2937;color:#9ca3af}
.range{position:relative;width:86px;height:7px;background:#1e2a45;border-radius:4px;display:inline-block;vertical-align:middle}
.range .fill{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,#f87171,#fbbf24,#34d399);border-radius:4px;opacity:.85}
.flag{color:#fca5a5} .foot{color:var(--mut);font-size:12.5px;margin-top:18px;line-height:1.6}
.star{color:#fbbf24}
.panel{position:sticky;top:16px;background:var(--card);border:1px solid var(--line);border-radius:var(--rad);max-height:calc(100vh - 32px);overflow-y:auto;padding:0}
.phead{position:sticky;top:0;background:var(--card2);padding:11px 16px;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;align-items:center;gap:8px;z-index:2}
.phead .pt{font-weight:800;font-size:15px}
.phead .pn{color:var(--mut);font-size:11.5px;display:block;font-weight:400}
#pclose{display:none;background:none;border:1px solid var(--line);color:var(--txt);border-radius:8px;font-size:16px;width:32px;height:32px;cursor:pointer}
#pbody{padding:13px 16px 16px;font-size:13.5px;line-height:1.6;color:#dbe4f5}
#pbody p{margin:6px 0;line-height:1.6}
#pbody h3{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--mut);margin:14px 0 5px}
#pbody ul{margin:5px 0;padding-left:18px} #pbody li{margin:3px 0;line-height:1.55}
#pbody table{margin:8px 0} #pbody td,#pbody th{white-space:normal;cursor:default}
.pfull{display:inline-block;margin-top:12px;padding:9px 14px;background:var(--acc);color:#081120;font-weight:700;border-radius:9px}
.pfull:hover{text-decoration:none;opacity:.9}
.pkpis{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:10px 0}
.pkpi{background:var(--card2);border:1px solid var(--line);border-radius:9px;padding:7px 9px}
.pkpi .v{font-size:14.5px;font-weight:700} .pkpi .l{color:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.05em}
.fbar{height:7px;background:#1e2a45;border-radius:4px;position:relative;margin:3px 0 8px}
.fbar i{position:absolute;left:0;top:0;bottom:0;border-radius:4px;background:var(--acc)}
.crumb{font-size:13px;margin:2px 0 10px}
.fgrade{display:inline-block;min-width:32px;text-align:center;padding:2px 5px;border-radius:6px;font-weight:700;font-size:11.5px;margin-right:3px}
.biggrade{font-size:20px;font-weight:800;padding:5px 12px;border-radius:9px;display:inline-block}
ul.kill{margin:6px 0;padding-left:20px} ul.kill li{line-height:1.6;font-size:14px;color:#dbe4f5;margin:4px 0}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:16px 0}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:var(--rad);padding:12px 14px}
.kpi .v{font-size:19px;font-weight:700} .kpi .l{color:var(--mut);font-size:11.5px;text-transform:uppercase;letter-spacing:.05em;margin-top:2px;line-height:1.4}
section{background:var(--card);border:1px solid var(--line);border-radius:var(--rad);padding:18px 20px;margin:14px 0}
section h2{margin:0 0 8px;font-size:17px} section p{line-height:1.65;font-size:14.5px;color:#dbe4f5;margin:6px 0}
section table{margin:10px 0}
.bigchart{background:var(--card);border:1px solid var(--line);border-radius:var(--rad);padding:16px;margin:14px 0}
.price{font-size:30px;font-weight:800}
.legs{display:inline-flex;gap:4px;vertical-align:middle}
.legdot{width:10px;height:10px;border-radius:50%;display:inline-block;border:1px solid rgba(255,255,255,.25)}
.legdot.pass{background:var(--up)} .legdot.mixed{background:var(--warn)} .legdot.fail{background:var(--dn)}
.srcs{margin:6px 0;padding-left:18px} .srcs li{line-height:1.55;font-size:13px;margin:3px 0}
.srcs .dom{color:var(--mut);font-size:11.5px}
.audit{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600;background:#13294d;color:#9ec2ff;border:1px solid #25497f}
details.gloss{background:var(--card);border:1px solid var(--line);border-radius:var(--rad);padding:14px 18px;margin:14px 0}
details.gloss summary{cursor:pointer;font-weight:700;font-size:14.5px}
details.gloss p,details.gloss li{font-size:13.5px;line-height:1.6;color:#c4d2ec}
details.gloss ul{margin:6px 0;padding-left:20px}
.evline{font-size:11.5px;color:var(--mut);margin-top:4px}
@media(min-width:901px){.master thead th{position:sticky;top:0;z-index:5}}
@media(max-width:1080px){.master{grid-template-columns:minmax(0,1fr) 340px}}
@media(max-width:900px){
.shell{flex-direction:column}
.sidebar{flex:none;width:100%;height:auto;position:sticky;top:0;padding:10px 12px 8px;border-right:none;border-bottom:1px solid var(--line)}
.brandsub{display:none}.sidefoot{display:none}
.tabs{flex-direction:row;overflow-x:auto;-webkit-overflow-scrolling:touch;padding-bottom:4px}
.tab{padding:7px 11px;font-size:12.5px}
.main{padding:14px 12px 48px}
.master{grid-template-columns:minmax(0,1fr)}
.panel{position:fixed;left:0;right:0;bottom:0;top:auto;max-height:74vh;z-index:90;border-radius:16px 16px 0 0;transform:translateY(106%);transition:transform .25s ease;box-shadow:0 -8px 30px rgba(0,0,0,.5)}
.panel.open{transform:none}
#pclose{display:block}
.nm{max-width:104px}.hide-m{display:none}h1{font-size:20px}
th,td{padding:6px 7px;font-size:12.3px}
.cards{grid-template-columns:repeat(4,1fr);gap:7px}.card{padding:7px 9px}.card .n{font-size:16px}.card .l{font-size:9.5px}
}
@media(max-width:640px){.kpis{grid-template-columns:repeat(2,1fr)}
.price{font-size:24px}.cards{grid-template-columns:repeat(2,1fr)}
section{padding:14px 14px}}
"""


def spark_svg(values, w=110, h=30):
    if not values:
        return ""
    lo, hi = min(values), max(values)
    span = (hi - lo) or 1
    pts = " ".join(f"{i * w / (len(values) - 1):.1f},{h - 3 - (v - lo) / span * (h - 6):.1f}"
                   for i, v in enumerate(values))
    color = "#34d399" if values[-1] >= values[0] else "#f87171"
    chg = (values[-1] / values[0] - 1) * 100 if values[0] else 0
    return (f'<svg width="{w}" height="{h}"><polyline points="{pts}" fill="none" '
            f'stroke="{color}" stroke-width="1.6"/></svg> '
            f'<span class="{"up" if chg >= 0 else "dn"}">{chg:+.1f}%</span>')


def chart_svg(values, high52, low52, w=860, h=250):
    if not values:
        return ""
    lo = min(min(values), low52)
    hi = max(max(values), high52)
    span = (hi - lo) or 1
    def y(v):
        return h - 14 - (v - lo) / span * (h - 36)
    pts = " ".join(f"{i * w / (len(values) - 1):.1f},{y(v):.1f}" for i, v in enumerate(values))
    color = "#34d399" if values[-1] >= values[0] else "#f87171"
    return (f'<svg viewBox="0 0 {w} {h}" width="100%">'
            f'<line x1="0" y1="{y(high52):.1f}" x2="{w}" y2="{y(high52):.1f}" stroke="#fbbf24" stroke-dasharray="5 5" stroke-width="1"/>'
            f'<text x="6" y="{y(high52) - 5:.1f}" fill="#fbbf24" font-size="12">52w high ${high52:,.2f}</text>'
            f'<line x1="0" y1="{y(low52):.1f}" x2="{w}" y2="{y(low52):.1f}" stroke="#f87171" stroke-dasharray="5 5" stroke-width="1"/>'
            f'<text x="6" y="{y(low52) + 15:.1f}" fill="#f87171" font-size="12">52w low ${low52:,.2f}</text>'
            f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2"/></svg>')


def load_consensus():
    """data/consensus.json values are [label, '$target (+x%)', note] lists (or
    dicts) — normalize to {ticker: {label, target (float|None), note}}."""
    raw = json.load(open(os.path.join(DATA, "consensus.json")))
    out = {}
    for t, v in raw.items():
        if isinstance(v, list):
            label = v[0] if len(v) > 0 else ""
            tstr = v[1] if len(v) > 1 else ""
            note = v[2] if len(v) > 2 else ""
            m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", tstr or "")
            target = float(m.group(1).replace(",", "")) if m else None
            out[t] = {"label": label, "target": target, "note": note}
        elif isinstance(v, dict):
            out[t] = {"label": v.get("label", ""), "target": v.get("target"),
                      "note": v.get("note", "")}
    return out


def chip_for(label):
    cls = {"Strong Buy": "sb", "Buy": "b", "Moderate Buy": "b",
           "Hold": "h", "Reduce": "r"}.get(label, "na")
    return f'<span class="chip {cls}">{label or "—"}</span>'


def rationale(t, k, notes):
    off = -k["off_high_pct"]
    bits = [f"Down {off:.0f}% from its 52-week high of ${k['high52']:,.2f}, "
            f"trading at {k['range_pos']:.0f}% of its 52-week range."]
    if k["mom20_pct"] >= 0:
        bits.append(f"The fall has stabilized — 20-day momentum is {k['mom20_pct']:+.1f}% "
                    "rather than still accelerating down.")
    else:
        bits.append(f"20-day momentum is {k['mom20_pct']:+.1f}% — easing, inside the "
                    "screen's not-in-freefall limit.")
    bits.append(f"RSI {k['rsi14']:.0f} says washed out rather than euphoric, and "
                f"${k['adv_m']:,.0f}M of average daily dollar volume makes it a large, "
                "liquid name — the dip is in the price, screened against evidence the "
                "business itself is broken.")
    if notes:
        bits.append("It carries a full hand-researched brief below (dip cause, business "
                    "pulse, analyst consensus, risks).")
    return " ".join(bits)


TAB_DEFS = [("dip", "index.html", "📉 Quality Dips"),
            ("pro", "pro.html", "🏦 Factor Grades"),
            ("sectors", "sectors.html", "🌍 Sector Impact"),
            ("validated", "validated.html", "🏆 Vetted Buys"),
            ("macro", "macro.html", "🌐 Theme Buys"),
            ("holds", "holds.html", "🟡 The Bench"),
            ("screener", "screener.html", "🔎 Fallen & Cheap"),
            ("skin", "skin.html", "🤝 Skin in the Game"),
            ("buzz", "buzz.html", "📣 Most Discussed"),
            ("common", "common.html", "🔁 Most Common")]


def tabs_html(active, prefix=""):
    """Shared shell: left sidebar with brand + vertical tab nav, identical
    on every page (collapses to a top strip on mobile). Detail pages live
    under stock/ and pass prefix='../'."""
    tabs = "".join(
        f'<a class="tab{" active" if key == active else ""}" '
        f'href="{prefix}{href}">{label}</a>'
        for key, href, label in TAB_DEFS)
    return (f'<aside class="sidebar">'
            f'<a class="brand" href="{prefix}index.html">◆ Quality-Dip Watchlist</a>'
            f'<div class="brandsub">Ten lenses on one universe — the 800 '
            f'most-traded US stocks + every S&amp;P 1000 member.</div>'
            f'<nav class="tabs">{tabs}</nav>'
            f'<div class="sidefoot">Prices refresh daily at 7:00 AM CT. '
            f'Research verdicts are dated editions.<br>Not investment advice.</div>'
            f'</aside>')


TABS_DIP = tabs_html("dip")
TABS_PRO = tabs_html("pro")
TABS_SECTORS = tabs_html("sectors")
TABS_VALIDATED = tabs_html("validated")
TABS_MACRO = tabs_html("macro")
TABS_HOLDS = tabs_html("holds")
TABS_SCREENER = tabs_html("screener")
TABS_SKIN = tabs_html("skin")
TABS_BUZZ = tabs_html("buzz")
TABS_COMMON = tabs_html("common")


def pro_section(t, prec, k, rank=None):
    if not prec:
        return ""
    fchips = " ".join(
        f'<span class="fgrade chip {_grade_cls(_grade(p))}" title="{fn}">'
        f'{fn[:4]} {_grade(p)}</span>'
        for fn, p in prec["factors"].items() if p is not None)
    veto = (f"<p class='flag'>⛔ Professional veto: {prec['veto']}</p>" if prec.get("veto") else "")
    rankline = (f"Professional rank #{rank} · composite grade "
                f"<b>{prec['grade']}</b>" if rank else
                ("Graded in the professional pool but outside its Top 50"
                 if not prec.get("veto") else "Vetoed out of the professional list"))
    metrics = []
    if prec["fscore"] is not None:
        metrics.append(f"Piotroski F-score <b>{prec['fscore']}/9</b>")
    if prec["fcf_y"] is not None:
        metrics.append(f"FCF yield <b>{prec['fcf_y']:.1f}%</b>")
    if prec["nd_ebitda"] is not None:
        metrics.append(f"net debt/EBITDA <b>{prec['nd_ebitda']:.1f}×</b>")
    if prec["surp_mean"] is not None:
        metrics.append(f"avg EPS surprise (last {prec['surp_n']}) <b>{prec['surp_mean']:+.1f}%</b>")
    kills = "".join(f"<li>{x}</li>" for x in kill_criteria(prec, k))
    return (f"<section><h2>🏦 Professional lens</h2>"
            f"<p>{rankline}</p><p>{fchips}</p>"
            f"<p>{' · '.join(metrics)}</p>{veto}"
            f"<p style='margin-top:10px'><b>Exit rules decided in advance (kill criteria):</b></p>"
            f"<ul class='kill'>{kills}</ul>"
            f"<p class='muted'>Factor grades follow the professional composite style "
            f"(profitability, revision momentum, value, safety, growth, momentum — "
            f"percentile-ranked across the screened pool). Revisions use reported "
            f"EPS-surprise history, the free proxy for analyst revision momentum.</p></section>")


def load_verdicts():
    p = os.path.join(DATA, "research_verdicts.json")
    if os.path.exists(p):
        try:
            return json.load(open(p))
        except Exception:
            pass
    return {"meta": {}, "verdicts": {}, "final50": []}


def load_theme_verdicts():
    p = os.path.join(DATA, "theme_verdicts.json")
    if os.path.exists(p):
        try:
            return json.load(open(p))
        except Exception:
            pass
    return {"meta": {}, "verdicts": {}, "list": []}


def load_themes():
    p = os.path.join(DATA, "themes.json")
    if os.path.exists(p):
        try:
            return json.load(open(p))
        except Exception:
            pass
    return {"themes": []}


def verdict_section(t, verdicts):
    v = (verdicts.get("verdicts") or {}).get(t)
    if not v:
        return ""
    flag = v.get("flag", "")
    cls = {"BUY": "sb", "HOLD": "h", "SELL": "r"}.get(flag, "na")
    legs = v.get("legs") or {}
    leg_rows = "".join(
        f"<tr><td style='text-transform:capitalize'>{ln}</td>"
        f"<td><span class='chip {'sb' if lv.get('v') == 'pass' else 'h' if lv.get('v') == 'mixed' else 'r'}'>"
        f"{lv.get('v', '')}</span></td><td>{lv.get('note', '')}</td></tr>"
        for ln, lv in legs.items())
    risks = "".join(f"<li>{x}</li>" for x in (v.get("risks") or []))
    def _dom(u):
        m = re.search(r"://([^/]+)", u or "")
        return m.group(1).replace("www.", "") if m else ""
    srcs = "".join(
        f"<li><a href='{s.get('url', '')}' target='_blank' rel='noopener'>"
        f"{s.get('title', 'source')}</a> "
        f"<span class='dom'>{_dom(s.get('url', ''))}</span></li>"
        for s in (v.get("sources") or []))
    return (f"<section><h2>🔬 Research verdict</h2>"
            f"<p><span class='chip {cls}' style='font-size:15px'>{flag}</span>"
            f"{audit_badge(t)}"
            f" <span class='muted'>deep-researched {v.get('date', '')} · {v.get('origin', '')}</span></p>"
            f"<p>{v.get('summary', '')}</p>"
            f"<div class='tablewrap'><table><thead><tr><th>Rubric leg</th><th>Finding</th><th>Evidence</th></tr></thead>"
            f"<tbody>{leg_rows}</tbody></table></div>"
            f"<p style='margin-top:10px'><b>Key risks:</b></p><ul>{risks}</ul>"
            f"<p><b>What would change this verdict:</b> {v.get('what_changes', '')}</p>"
            f"<p style='margin-top:10px'><b>Evidence — sources behind this card:</b></p>"
            f"<ul class='srcs'>{srcs}</ul>"
            f"<p class='muted'>A structured research verdict from public sources, "
            f"not a trade instruction.</p></section>")


# ---------------------------------------------------------------------------
# Shared UI helpers (design system v2): canonical KPI tiles, evidence bits
# ---------------------------------------------------------------------------

_CROSSCHECK = None


def load_crosscheck():
    """Web/Gemini evidence-audit results, keyed 'store:ticker' → entry."""
    global _CROSSCHECK
    if _CROSSCHECK is None:
        _CROSSCHECK = {}
        for fn in ("web_crosscheck.json", "gemini_crosscheck.json"):
            p = os.path.join(DATA, fn)
            if os.path.exists(p):
                try:
                    raw = json.load(open(p))
                except Exception:
                    continue
                items = raw.items() if isinstance(raw, dict) else (
                    (f"{e.get('store')}:{e.get('ticker')}", e)
                    for e in raw if isinstance(e, dict))
                for key, e in items:
                    if isinstance(e, dict) and e.get("overall"):
                        _CROSSCHECK.setdefault(key, e)
    return _CROSSCHECK


def audit_badge(t):
    """Cross-check badge for a verdict card, if it was evidence-audited."""
    cc = load_crosscheck()
    for store in ("validated", "theme"):
        e = cc.get(f"{store}:{t}")
        if e:
            overall = e.get("overall", "")
            return (f' <span class="audit" title="Every load-bearing figure on this '
                    f'card was checked against its cited sources in the 2026-10-25 '
                    f'evidence audit (data/web_crosscheck.json). Overall: {overall}.">'
                    f'✓ evidence cross-checked · {overall}</span>')
    return ""


def legs_mini(v):
    """Five rubric legs as dots: pass / mixed / fail, in canonical order."""
    legs = (v or {}).get("legs") or {}
    order = ["quality", "valuation", "sector", "risk", "catalyst"]
    dots = "".join(
        f'<span class="legdot {legs.get(ln, {}).get("v", "na")}" '
        f'title="{ln}: {legs.get(ln, {}).get("v", "—")}"></span>'
        for ln in order)
    return f'<span class="legs">{dots}</span>'


# ---------------------------------------------------------------------------
# UI v3: master-detail panel (list left, evidence box right; bottom sheet
# on mobile). Each list row carries a hidden td.detail whose HTML the panel
# shows on selection; the first row is selected by default.
# ---------------------------------------------------------------------------

def panel_html():
    return ('<aside class="panel" id="panel"><div class="phead"><div>'
            '<span class="pt" id="ptitle">Select a stock</span>'
            '<span class="pn" id="pname"></span></div>'
            '<button id="pclose" aria-label="Close">×</button></div>'
            '<div id="pbody"><p class="muted">Pick a row to see its evidence '
            'here.</p></div></aside>')


PANEL_JS = """<script>
(function(){
var panel=document.getElementById('panel'),pbody=document.getElementById('pbody'),
ptitle=document.getElementById('ptitle'),pname=document.getElementById('pname');
var rows=[].slice.call(document.querySelectorAll('table.mlist tbody tr'));
function visible(r){return r.style.display!=='none';}
function select(r){if(!r)return;
 rows.forEach(function(x){x.classList.toggle('sel',x===r);});
 var sym=r.querySelector('.sym'),det=r.querySelector('td.detail'),nm=r.querySelector('.nm');
 ptitle.textContent=sym?sym.textContent.trim():'';
 pname.textContent=nm?nm.textContent.trim():'';
 pbody.innerHTML=det?det.innerHTML:'<p class="muted">No detail available for this row.</p>';
 pbody.scrollTop=0;
 if(window.innerWidth<=900){panel.classList.add('open');}
}
rows.forEach(function(r){r.addEventListener('click',function(){select(r);});});
var pc=document.getElementById('pclose');
if(pc){pc.addEventListener('click',function(){panel.classList.remove('open');});}
window.__selFirst=function(){var f=rows.filter(visible)[0];if(f)select(f);};
document.addEventListener('keydown',function(e){
 if(e.key==='Escape'){panel.classList.remove('open');return;}
 if(e.key!=='ArrowDown'&&e.key!=='ArrowUp')return;
 var tag=(document.activeElement||{}).tagName;
 if(tag==='INPUT'||tag==='SELECT'||tag==='TEXTAREA')return;
 var cur=rows.filter(visible),sel=document.querySelector('table.mlist tbody tr.sel');
 var i=cur.indexOf(sel); if(i<0)i=0; else i+=(e.key==='ArrowDown'?1:-1);
 i=Math.max(0,Math.min(cur.length-1,i));
 if(cur[i]){select(cur[i]);cur[i].scrollIntoView({block:'nearest'});e.preventDefault();}
});
var q=document.getElementById('q');
if(q&&!q.dataset.custom){q.addEventListener('input',function(){
 var s=q.value.toLowerCase();
 rows.forEach(function(r){r.style.display=(r.dataset.q||'').toLowerCase().indexOf(s)>=0?'':'none';});
 var sel=document.querySelector('table.mlist tbody tr.sel');
 if(!sel||!visible(sel)){window.__selFirst();}
});}
document.querySelectorAll('table.mlist th').forEach(function(th){
 th.title='Click to sort';
 th.addEventListener('click',function(){
  var tbl=th.closest('table'),tb=tbl.querySelector('tbody'),rs=[].slice.call(tb.rows);
  var i=[].slice.call(th.parentNode.children).indexOf(th);
  var num=th.classList.contains('num'),dir=th.dataset.d==='a'?'d':'a';th.dataset.d=dir;
  rs.sort(function(a,b){var x=a.cells[i],y=b.cells[i];
   var xv=x?x.textContent.replace(/[^0-9.+-]/g,''):'',yv=y?y.textContent.replace(/[^0-9.+-]/g,''):'';
   if(num&&xv!==''&&yv!==''){xv=parseFloat(xv);yv=parseFloat(yv);return dir==='a'?xv-yv:yv-xv;}
   var xt=x?x.textContent:'',yt=y?y.textContent:'';
   return dir==='a'?xt.localeCompare(yt):yt.localeCompare(xt);});
  rs.forEach(function(r){tb.appendChild(r);});
 });});
window.__selFirst();
})();
</script>"""


def pkpi_grid(pairs):
    """Compact KPI tiles for the evidence panel: [(value, label), ...]."""
    return ("<div class='pkpis'>" + "".join(
        f"<div class='pkpi'><div class='v'>{v}</div><div class='l'>{l}</div></div>"
        for v, l in pairs) + "</div>")


def verdict_detail_html(v, t, page=None):
    """Panel-sized evidence for a research verdict card: summary, the five
    legs with their evidence notes, key risks, what changes the verdict,
    and the cited sources."""
    flag = v.get("flag", "")
    cls = {"BUY": "sb", "HOLD": "h", "SELL": "r"}.get(flag, "na")
    legs = v.get("legs") or {}
    leg_rows = "".join(
        f"<tr><td style='text-transform:capitalize'>{ln}</td>"
        f"<td><span class='chip {'sb' if lv.get('v') == 'pass' else 'h' if lv.get('v') == 'mixed' else 'r'}'>"
        f"{lv.get('v', '')}</span></td><td>{lv.get('note', '')}</td></tr>"
        for ln, lv in legs.items())
    risks = "".join(f"<li>{x}</li>" for x in (v.get("risks") or []))

    def _dom(u):
        m = re.search(r"://([^/]+)", u or "")
        return m.group(1).replace("www.", "") if m else ""
    srcs = "".join(
        f"<li><a href='{s.get('url', '')}' target='_blank' rel='noopener'>"
        f"{s.get('title', 'source')}</a> "
        f"<span class='dom'>{_dom(s.get('url', ''))}</span></li>"
        for s in (v.get("sources") or []))
    link = (f"<a class='pfull' href='stock/{page}.html'>Full stock page →</a>"
            if page else "")
    return (f"<p><span class='chip {cls}' style='font-size:13px'>{flag}</span>"
            f"{audit_badge(t)} <span class='muted'>researched "
            f"{v.get('date', '')}</span></p>"
            f"<p>{v.get('summary', '')}</p>"
            f"<h3>The five legs — evidence</h3>"
            f"<table><tbody>{leg_rows}</tbody></table>"
            f"<h3>Key risks</h3><ul>{risks}</ul>"
            f"<h3>What would change this verdict</h3>"
            f"<p>{v.get('what_changes', '')}</p>"
            f"<h3>Sources</h3><ul class='srcs'>{srcs}</ul>{link}")


def _tile(v, l, cls=""):
    return f'<div class="kpi"><div class="v {cls}">{v}</div><div class="l">{l}</div></div>'


def kpi_tiles(k, rs20=None, pe=None, earn=None, extra=""):
    """The canonical KPI band used on EVERY stock detail page, in the same
    order: position vs 52w range, RSI, momentum, relative strength, 1Y,
    volatility, P/E, next earnings — then lens-specific tiles (extra)."""
    t = []
    t.append(_tile(f"{k['off_high_pct']:.1f}%",
                   f"vs 52w high (${k['high52']:,.2f})",
                   "dn" if k["off_high_pct"] < 0 else "up"))
    t.append(_tile(f"{k['range_pos']:.0f}%",
                   f"of 52w range (low ${k['low52']:,.2f})"))
    t.append(_tile(f"{k['rsi14']:.0f}", "RSI (14)"))
    t.append(_tile(f"{k['mom20_pct']:+.1f}%", "Momentum (20d)",
                   "up" if k["mom20_pct"] >= 0 else "dn"))
    if rs20 is not None:
        t.append(_tile(f"{rs20:+.1f}%", "Rel. strength vs S&amp;P 500 (20d)",
                       "up" if rs20 >= 0 else "dn"))
    t.append(_tile(f"{k['perf1y_pct']:+.1f}%", "Performance (1y)",
                   "up" if k["perf1y_pct"] >= 0 else "dn"))
    t.append(_tile(f"{k['vol30']:.1f}%", "Volatility (30d, ann.)"))
    t.append(_tile(f"{pe}" if pe else "—", "P/E"))
    t.append(_tile(f"{earn or '—'}", "Next earnings"))
    return '<div class="kpis">' + "".join(t) + extra + "</div>"


def glossary_html():
    return """<details class="gloss"><summary>How to read the KPIs — the same definitions on every tab</summary>
<ul>
<li><b>vs 52w high / 52w range</b> — how far the price sits below its 52-week high, and where it sits between its 52-week low (0%) and high (100%).</li>
<li><b>RSI (14)</b> — 14-day Wilder relative strength index. Below ~30 reads washed-out, above ~70 extended; this screen wants neither freefall nor euphoria.</li>
<li><b>Momentum (20d)</b> — the stock's own 20-day price change. <b>Rel. strength (20d)</b> — that minus the S&amp;P 500's 20-day change: positive means it is beating the market short-term.</li>
<li><b>VPT (20d)</b> — change in the Volume-Price Trend line over 20 days, normalized: positive means volume has been flowing in on up days (accumulation).</li>
<li><b>P/E</b> — price ÷ SEC-reported diluted EPS (TTM) where computable; blank where earnings are negative or filings don't map cleanly. <b>ROIC</b> — after-tax operating income ÷ (debt + equity − cash) from SEC filings; less meaningful for banks/insurers.</li>
<li><b>Professional factors</b> — percentile ranks (0–100) across the graded pool, blended into a letter grade: Profitability 22%, Revisions 20% (reported EPS-surprise history — the free proxy for analyst revision momentum), Value 20%, Safety 15% (incl. Piotroski F-score), Growth 13%, Momentum 10%. <b>F-score</b> — Piotroski's 9 filing signals. <b>Net debt/EBITDA</b> and <b>FCF yield</b> use industrial-company definitions — read cautiously for financials.</li>
<li><b>Research verdict legs</b> — every researched card is scored on five legs (Quality · Valuation · Sector · Risk · Catalyst), each pass / mixed / fail with cited evidence. BUY requires no leg failing and at least four passing. The dots next to a verdict show the five legs in that order.</li>
<li><b>Analyst consensus / targets</b> — aggregator figures (TipRanks / MarketBeat / StockAnalysis), curated: the crowd's opinion, not a forecast. ⚠ marks a name whose consensus or target contradicts the thesis.</li>
</ul>
<p>Research and education only — not investment advice.</p></details>"""


def dip_comment(r, c, n):
    """One-line evidence comment for a Quality-Dip row, from its own data."""
    bits = [f"{abs(r['off_high_pct']):.0f}% below its 52w high"]
    if r["mom20_pct"] >= 0:
        bits.append(f"fall has stabilized (20d {r['mom20_pct']:+.1f}%)")
    else:
        bits.append(f"still easing (20d {r['mom20_pct']:+.1f}%), inside the not-in-freefall limit")
    bits.append(f"RSI {r['rsi14']:.0f}")
    if n:
        d = (n.get("dip") or "").strip()
        if d:
            bits.append(d.split(". ")[0].rstrip(".") + ".")
    elif c.get("label"):
        bits.append(f"consensus {c['label']}"
                    + (f", avg target ${c['target']:,.0f}" if c.get("target") else ""))
    return " · ".join(bits)


def pro_comment(r):
    """One-line evidence comment for a Professional row: strongest/weakest
    factor, F-score, and the headline balance-sheet read."""
    facs = [(fn, p) for fn, p in (r.get("factors") or {}).items() if p is not None]
    bits = []
    if facs:
        hi = max(facs, key=lambda x: x[1])
        lo = min(facs, key=lambda x: x[1])
        bits.append(f"strongest: {hi[0]} ({_grade(hi[1])})")
        bits.append(f"weakest: {lo[0]} ({_grade(lo[1])})")
    if r.get("fscore") is not None:
        bits.append(f"F-score {r['fscore']}/9")
    if r.get("nd_ebitda") is not None:
        bits.append(f"net debt {r['nd_ebitda']:.1f}× EBITDA")
    if r.get("surp_mean") is not None:
        bits.append(f"EPS surprises avg {r['surp_mean']:+.1f}% over the last {r.get('surp_n', 4)}")
    return " · ".join(bits)


def build_site(rows, prev_ranks, meta, pro=None, pro_ranks=None):
    verdicts = load_verdicts()
    cons = load_consensus()
    fortune = json.load(open(os.path.join(DATA, "fortune500.json")))
    notes = json.load(open(os.path.join(DATA, "research_notes.json")))
    asof = meta["asof"]
    pro = pro or {}
    pro_ranks = pro_ranks or {}
    body = []
    for r in rows:
        t = r["ticker"]
        c = cons.get(t, {})
        label = c.get("label", "")
        tgt = c.get("target")
        note = (c.get("note") or "").strip()
        flag = ""
        if label in ("Hold", "Reduce") or (tgt and tgt < r["last"]):
            flag = f' <span class="flag" title="{note}">⚠</span>' if note else ' <span class="flag">⚠</span>'
        prev = prev_ranks.get(t)
        if prev is None:
            mv = '<span class="chip b">NEW</span>'
        else:
            d = prev - r["rank"]
            mv = (f'<span class="up">▲ {d}</span>' if d > 0 else
                  f'<span class="dn">▼ {-d}</span>' if d < 0 else '<span class="muted">—</span>')
        star = ' <span class="star" title="Hand-researched brief">★</span>' if t in notes else ""
        fr = f"#{fortune[t]}" if fortune.get(t) else '<span class="muted">—</span>'
        n = notes.get(t)
        brief = (f"<h3>What caused the dip</h3><p>{n.get('dip', '')}</p>"
                 f"<h3>Business pulse</h3><p>{n.get('business', '')}</p>"
                 if n else "")
        det = (pkpi_grid([
            (f"${r['last']:,.2f}", "Price"),
            (f"{r['day_pct']:+.2f}%", "Day"),
            (f"{r['off_high_pct']:.1f}%", "vs 52w high"),
            (f"{r['rsi14']:.0f}", "RSI"),
            (f"{r['vpt20']:+.0f}%", "VPT 20d"),
            (f"{r['rs20']:+.1f}%", "Rel Str 20d"),
            (f"{r['vol30']:.0f}%", "Volatility 30d"),
            (str(r['pe']) if r['pe'] else "—", "P/E"),
            (("%.1f%%" % r['roic']) if r['roic'] is not None else "—", "ROIC"),
            (f"{r['perf1y_pct']:+.1f}%", "1Y perf"),
            (f"${r['adv_m']:,.0f}M", "Avg $ volume"),
            (str(r['earnings'] or "—"), "Earnings")])
            + f"<p>{dip_comment(r, c, n)}</p>{brief}"
            + f"<a class='pfull' href='stock/{t}.html'>Full stock page →</a>")
        body.append(f"""<tr data-q="{t} {clean_company_name(r['company'])}" data-ind="{INDUSTRY.get(t, '')}">
  <td>{r['rank']}</td>
  <td><a class="sym" href="stock/{t}.html">{t}</a>{star}<span class="nm">{clean_company_name(r['company'])}</span></td>
  <td class="ind hide-m">{INDUSTRY.get(t, '')}</td>
  <td class="num hide-m">{fr}</td>
  <td class="num">${r['last']:,.2f}</td>
  <td class="num {'up' if r['day_pct'] >= 0 else 'dn'}">{r['day_pct']:+.2f}%</td>
  <td class="num">{r['rsi14']:.0f}</td>
  <td class="num hide-m {'up' if r['vpt20'] >= 0 else 'dn'}">{r['vpt20']:+.0f}%</td>
  <td class="num {'up' if r['rs20'] >= 0 else 'dn'}">{r['rs20']:+.1f}%</td>
  <td class="num hide-m">{mv}</td>
  <td><span class="range"><span class="fill" style="width:{r['range_pos']}%"></span></span>
      <span class="dn">{r['off_high_pct']:.1f}%</span></td>
  <td class="hide-m">{spark_svg(r['spark'])}</td>
  <td>{chip_for(label)}{flag}{f"<br><span class='muted'>avg ${tgt:,.0f}</span>" if tgt else ""}</td>
  <td class="num hide-m">{r['earnings'] or '<span class="muted">—</span>'}</td>
  <td class="detail">{det}</td>
</tr>""")
    inds = sorted({INDUSTRY.get(r["ticker"], "") for r in rows} - {""})
    options = "".join(f'<option value="{i}">{i}</option>' for i in inds)
    n_researched = sum(1 for r in rows if r["ticker"] in notes)
    n_sb = sum(1 for r in rows if cons.get(r["ticker"], {}).get("label") == "Strong Buy")
    this_week = [r for r in rows if r["earnings"] and
                 abs((datetime.strptime(r["earnings"], "%Y-%m-%d").date()
                      - date.today()).days) <= 7]
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quality Dips — Top 50 · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_DIP}
<main class="main">
<h1>📉 Quality Dips — Top 50</h1>
<div class="sub">Famous, fundamentally strong brands that have dipped significantly for <b>situational</b>
reasons — the kind of names expected to recover when conditions normalize. Screened from the
{meta['universe_n']} US common stocks — the 800 most-traded by dollar volume, plus every
S&P 1000 member ({meta['screened']} with usable data,
{meta['candidates']} passing the dip screen). Prices as of the <b>{asof}</b> close · refreshed daily at 7:00 AM CT.
<b>Select any row</b> — why it's here, its full KPI set and brief open in the panel. ★ = hand-researched brief.</div>
<div class="cards">
  <div class="card"><div class="n">50</div><div class="l">Names on the list</div></div>
  <div class="card"><div class="n">{n_researched}</div><div class="l">Hand-researched briefs</div></div>
  <div class="card"><div class="n">{n_sb}</div><div class="l">Strong Buy consensus</div></div>
  <div class="card"><div class="n">{len(this_week)}</div><div class="l">Reporting within ±7 days</div></div>
</div>
<div class="controls">
  <input id="q" data-custom="1" type="text" placeholder="Search symbol or company…">
  <select id="ind"><option value="">All industries</option>{options}</select>
</div>
<div class="master">
<div class="tablewrap"><table id="t" class="mlist"><thead><tr>
<th>#</th><th>Symbol</th><th class="hide-m">Industry</th><th class="num hide-m">Fortune 500</th><th class="num">Last</th>
<th class="num">% Change</th><th class="num">RSI</th><th class="num hide-m">VPT 20d</th><th class="num">Rel Str 20d</th>
<th class="num hide-m">vs Prev</th><th>52-week position / off high</th>
<th class="hide-m">Last 3 months</th><th>Analyst consensus</th>
<th class="num hide-m">Earnings</th>
</tr></thead><tbody>
{chr(10).join(body)}
</tbody></table></div>
{panel_html()}
</div>
<p class="foot"><b>How to read this:</b> the 52-week bar shows where the price sits between its low and high;
"off high" is the size of the dip. Volatility is annualized from daily closes. P/E uses SEC-reported
diluted EPS (TTM) where computable — blank where earnings are negative or filings don't map cleanly.
<b>RSI</b> is the 14-day Wilder relative strength index. <b>VPT 20d</b> is the 20-day change in the
Volume-Price Trend line, normalized by the period's total VPT movement — positive means volume has been
flowing in on up days (accumulation). <b>Rel Str 20d</b> is the stock's 20-day return minus the S&amp;P 500's —
short-term relative strength versus the market. <b>ROIC</b> is return on invested capital from SEC filings:
operating income after tax ÷ (debt + equity − cash); blank where filings don't provide the pieces
(financials' capital structure makes classic ROIC less meaningful for banks/insurers).
Analyst consensus/targets are aggregator figures (TipRanks / MarketBeat / StockAnalysis), curated —
the crowd's opinion, not a forecast; ⚠ marks a name whose consensus or target contradicts the dip thesis.
Fortune 500 ranks are Fortune's official 2026 list (by revenue); "—" = not on the list (foreign-HQ,
below the revenue cutoff, or unclassified). Researched exclusions: NKE and TSLA are deliberately kept
off — their dips trace to deteriorating or polarized business stories, not situations.
<b>Research and education only — not investment advice.</b></p>
{glossary_html()}
</main></div>
{PANEL_JS}
<script>
const qq=document.getElementById('q'),ind=document.getElementById('ind');
function filt(){{const s=qq.value.toLowerCase(),f=ind.value;
document.querySelectorAll('#t tbody tr').forEach(tr=>{{
const okS=(tr.dataset.q||'').toLowerCase().includes(s);
const okI=!f||tr.dataset.ind===f; tr.style.display=(okS&&okI)?'':'none';}});
if(window.__selFirst)window.__selFirst();}}
qq.addEventListener('input',filt);ind.addEventListener('change',filt);
</script></body></html>"""
    with open(os.path.join(ROOT, "index.html"), "w") as fh:
        fh.write(html)

    for r in rows:
        t = r["ticker"]
        c = cons.get(t, {})
        n = notes.get(t)
        secs = []
        secs.append(f"<section><h2>Why it's on this list</h2><p>{rationale(t, r, n)}</p></section>")
        if n:
            secs.append(f"<section><h2>What caused the dip</h2><p>{n.get('dip', '')}</p></section>")
            secs.append(f"<section><h2>Business pulse</h2><p>{n.get('business', '')}</p></section>")
            secs.append(f"<section><h2>What analysts say</h2><p>{n.get('analysts', '')}</p></section>")
            secs.append(f"<section><h2>Media &amp; social buzz</h2><p>{n.get('buzz', '')}</p></section>")
            secs.append(f"<section><h2>Risks &amp; what would break the thesis</h2><p>{n.get('risks', '')}</p></section>")
        else:
            if c.get("label") or c.get("note"):
                secs.append(
                    f"<section><h2>Analyst consensus</h2><p>{chip_for(c.get('label', ''))}"
                    f"{f' · average target ${c['target']:,.0f}' if c.get('target') else ''}</p>"
                    f"<p>{c.get('note', '')}</p></section>")
            secs.append("<section><h2>Research depth</h2><p class='muted'>This name entered via the "
                        "quantitative screen and carries aggregator consensus data. The ★ names on the "
                        "list carry full hand-researched briefs (dip cause, business pulse, buzz, risks).</p></section>")
        psec = pro_section(t, pro.get(t), r, pro_ranks.get(t))
        if psec:
            secs.append(psec)
        vsec = verdict_section(t, verdicts) or verdict_section(t, load_theme_verdicts())
        if vsec:
            secs.append(vsec)
        tiles = kpi_tiles(r, rs20=r["rs20"], pe=r["pe"], earn=r["earnings"], extra=(
            _tile(f"{r['vpt20']:+.0f}%", "VPT trend (20d)",
                  "up" if r["vpt20"] >= 0 else "dn")
            + _tile(f"{('%.1f%%' % r['roic']) if r['roic'] is not None else '—'}",
                    "ROIC (SEC filings)")
            + _tile(f"${r['adv_m']:,.0f}M", "Avg daily $ volume (20d)")))
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(r['company'])} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("dip", "../")}
<main class="main">
<p class="crumb"><a href="../index.html">← Back to Quality Dips</a></p>
<h1>{t} — {clean_company_name(r['company'])}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Rank #{r['rank']} on today's list</div>
<div><span class="price">${r['last']:,.2f}</span>
<span class="{'up' if r['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{r['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
{tiles}
<div class="bigchart">{chart_svg(r['chart'], r['high52'], r['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a> ·
<a href="https://www.google.com/finance/quote/{t}:NASDAQ" target="_blank" rel="noopener">Google Finance — {t}</a></p>
<p class="muted">Data: Yahoo Finance daily bars, Nasdaq earnings calendar, SEC EDGAR, aggregator
analyst consensus (curated). Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)
    # prune stale detail pages
    keep = {r["ticker"] for r in rows} | set(meta.get("keep_extra") or [])
    sdir = os.path.join(ROOT, "stock")
    for fn in os.listdir(sdir):
        if fn.endswith(".html") and fn[:-5] not in keep:
            os.remove(os.path.join(sdir, fn))


def build_pro_page(pro_rows, prev_pro_ranks, meta, n_pool, n_vetoed):
    cons = load_consensus()
    asof = meta["asof"]
    body = []
    for r in pro_rows:
        t = r["ticker"]
        prev = prev_pro_ranks.get(t)
        if prev is None:
            mv = '<span class="chip b">NEW</span>'
        else:
            d = prev - r["rank"]
            mv = (f'<span class="up">▲ {d}</span>' if d > 0 else
                  f'<span class="dn">▼ {-d}</span>' if d < 0 else '<span class="muted">—</span>')
        fchips = " ".join(
            f'<span class="fgrade chip {_grade_cls(_grade(p))}" title="{fn}">{fn[:4]} {_grade(p)}</span>'
            for fn, p in r["factors"].items() if p is not None)
        surp = (f"{r['surp_mean']:+.1f}% · {r['surp_beat']:.0f}% beats" if r["surp_mean"] is not None
                else '<span class="muted">—</span>')
        fbars = "".join(
            f"<div style='font-size:12px'>{fn} — <b>{_grade(p)}</b> "
            f"<span class='muted'>({p:.0f}th pct)</span></div>"
            f"<div class='fbar'><i style='width:{p:.0f}%'></i></div>"
            for fn, p in r["factors"].items() if p is not None)
        kills = "".join(f"<li>{x}</li>" for x in (r.get("kill") or [])[:4])
        det = (pkpi_grid([
            (f"{r['composite']:.0f}", "Composite /100"),
            (f"{r['fscore']}/9" if r["fscore"] is not None else "—", "Piotroski F"),
            (("%.1f%%" % r["fcf_y"]) if r["fcf_y"] is not None else "—", "FCF yield"),
            (("%.1f×" % r["nd_ebitda"]) if r["nd_ebitda"] is not None else "—", "Net debt/EBITDA"),
            (surp, "EPS surprise (last 4)"),
            (str(r["earnings"] or "—"), "Earnings")])
            + f"<p>{pro_comment(r)}</p>"
            + f"<h3>Factor profile — percentile in the graded pool</h3>{fbars}"
            + (f"<h3>Pre-written exit rules</h3><ul>{kills}</ul>" if kills else "")
            + f"<a class='pfull' href='stock/{t}.html'>Full stock page →</a>")
        body.append(f"""<tr data-q="{t} {clean_company_name(r['company'])}" data-ind="{INDUSTRY.get(t, '')}">
  <td>{r['rank']}</td>
  <td><a class="sym" href="stock/{t}.html">{t}</a><span class="nm">{clean_company_name(r['company'])}</span></td>
  <td class="ind hide-m">{INDUSTRY.get(t, '')}</td>
  <td><span class="biggrade chip {_grade_cls(r['grade'])}">{r['grade']}</span></td>
  <td class="hide-m">{fchips}</td>
  <td class="num">{r['fscore'] if r['fscore'] is not None else '—'}/9</td>
  <td class="num">{('%.1f%%' % r['fcf_y']) if r['fcf_y'] is not None else '—'}</td>
  <td class="num hide-m">{('%.1f×' % r['nd_ebitda']) if r['nd_ebitda'] is not None else '—'}</td>
  <td class="num hide-m">{surp}</td>
  <td class="num hide-m">{r['pe'] if r['pe'] else '—'}</td>
  <td class="num">${r['last']:,.2f}</td>
  <td class="num hide-m">{mv}</td>
  <td class="num">{r['earnings'] or '<span class="muted">—</span>'}</td>
  <td class="detail">{det}</td>
</tr>""")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Factor Grades — Professional Top 50 · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_PRO}
<main class="main">
<h1>🏦 Factor Grades — Professional Top 50</h1>
<div class="sub">The professionals' lens, applied to the same {meta['universe_n']} US stocks
(top 800 by trading volume, plus the full S&P 1000):
<b>cash-flow quality and balance-sheet survivability first, price last</b> — a factor composite in the
spirit of Seeking Alpha Quant and the Zacks Rank. Six factors, percentile-ranked across
{n_pool} names with full SEC filing data: Profitability (22%), Revisions (20%), Value (20%),
Safety (15%), Growth (13%), Momentum (10%). Veto grades remove names outright
({n_vetoed} vetoed today: Piotroski F-score ≤ 3, extreme leverage with thin coverage,
persistent EPS misses, or cash-burn with dilution). Prices as of the <b>{asof}</b> close.
<b>Select any row</b> — its factor breakdown and pre-written exit rules open in the panel.</div>
<div class="controls"><input id="q" type="text" placeholder="Search symbol or company…"></div>
<div class="master">
<div class="tablewrap"><table id="t" class="mlist"><thead><tr>
<th>#</th><th>Symbol</th><th class="hide-m">Industry</th><th>Grade</th><th class="hide-m">Factors (Prof · Rev · Val · Safe · Grow · Mom)</th>
<th class="num">F-score</th><th class="num">FCF yield</th><th class="num hide-m">Net debt/EBITDA</th>
<th class="num hide-m">EPS surprise (last 4)</th><th class="num hide-m">P/E</th>
<th class="num">Last</th><th class="num hide-m">vs Prev</th><th class="num">Earnings</th>
</tr></thead><tbody>
{chr(10).join(body)}
</tbody></table></div>
{panel_html()}
</div>
<p class="foot"><b>How to read this:</b> the composite grade is a percentile blend, not a recommendation.
<b>Revisions</b> uses reported EPS-surprise history (mean surprise and beat rate over the last four
quarters) — the free proxy for analyst revision momentum; clean revision-breadth history is paid data,
and it is the first upgrade this lens would buy. <b>Safety</b> blends the Piotroski F-score (9 filing
signals: profitability, leverage, liquidity, dilution, margins, turnover), net debt/EBITDA, interest
coverage, current ratio and accrual quality. <b>Value</b> blends FCF yield, earnings yield, EV/EBITDA
and price/sales. Balance-sheet factors use industrial-company definitions — read them cautiously for
banks, insurers and REITs, whose filings structure debt differently; the F-score veto therefore
applies only to companies that report a cost-of-sales line. Fundamentals are the latest
reported fiscal year from SEC EDGAR. <b>Research and education only — not investment advice.</b></p>
{glossary_html()}
</main></div>
{PANEL_JS}</body></html>"""
    with open(os.path.join(ROOT, "pro.html"), "w") as fh:
        fh.write(html)


def build_pro_details(pro_rows, dip_tickers, meta):
    """Detail pages for Professional-50 names that are not on the dip list
    (dip-list names already got pages, with the pro section embedded)."""
    cons = load_consensus()
    verdicts = load_verdicts()
    fortune = json.load(open(os.path.join(DATA, "fortune500.json")))
    asof = meta["asof"]
    for r in pro_rows:
        t = r["ticker"]
        if t in dip_tickers:
            continue
        k = r["_kpi"]
        secs = [pro_section(t, r, k, r["rank"])]
        vsec = verdict_section(t, verdicts) or verdict_section(t, load_theme_verdicts())
        if vsec:
            secs.append(vsec)
        c = cons.get(t, {})
        if c.get("label") or c.get("note"):
            secs.append(
                f"<section><h2>Analyst consensus</h2><p>{chip_for(c.get('label', ''))}"
                f"{f' · average target ${c['target']:,.0f}' if c.get('target') else ''}</p>"
                f"<p>{c.get('note', '')}</p></section>")
        tiles = kpi_tiles(k, rs20=k["mom20_pct"] - meta.get("spy_mom20", 0.0),
                          pe=r["pe"], earn=r["earnings"], extra=(
            _tile(r["grade"], "Professional composite grade")
            + _tile(f"{r['fscore']}/9" if r["fscore"] is not None else "—",
                    "Piotroski F-score")
            + _tile(f"{k['vpt20']:+.0f}%", "VPT trend (20d)",
                    "up" if k["vpt20"] >= 0 else "dn")))
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(r['company'])} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("pro", "../")}
<main class="main">
<p class="crumb"><a href="../pro.html">← Back to Factor Grades</a></p>
<h1>{t} — {clean_company_name(r['company'])}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Professional rank #{r['rank']} · composite grade {r['grade']}</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
{tiles}
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a> ·
<a href="https://www.google.com/finance/quote/{t}:NASDAQ" target="_blank" rel="noopener">Google Finance — {t}</a></p>
<p class="muted">Data: Yahoo Finance daily bars, Nasdaq earnings calendar, SEC EDGAR.
Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_sectors_page(meta):
    p = os.path.join(DATA, "sectors.json")
    if not os.path.exists(p):
        return
    d = json.load(open(p))
    dir_cls = {"Positive": "sb", "Negative": "r", "Mixed": "h"}
    forces = "".join(
        f"<div class='kpi' style='text-align:left'><div style='font-weight:700'>{f['title']}</div>"
        f"<div class='muted' style='font-size:13px'>{f['detail']}</div></div>"
        for f in d.get("forces", []))
    cards = []
    for s in d.get("sectors", []):
        ev = "".join(f"<li>{x}</li>" for x in s.get("evidence", []))
        tags = " ".join(f"<span class='chip na'>{x}</span>" for x in s.get("drivers", []))
        arts = "".join(
            f"<li><a href='{a['url']}' target='_blank' rel='noopener'>{a['title']}</a>"
            f" <span class='muted'>— {a.get('source', '')}, {a.get('date', '')}</span></li>"
            for a in s.get("articles", []))
        cards.append(
            f"<section><h2>#{s['rank']} · {s['name']} "
            f"<span class='chip {dir_cls.get(s.get('direction'), 'na')}'>{s.get('direction', '')}</span> "
            f"<span class='chip na'>{s.get('severity', '')}</span></h2>"
            f"<p>{s.get('summary', '')}</p><p>{tags}</p>"
            f"<p><b>Evidence:</b></p><ul>{ev}</ul>"
            f"<p><b>Watch next:</b> {s.get('watch', '')}</p>"
            f"<p><b>Read:</b></p><ul>{arts}</ul></section>")
    nm = "".join(f"<tr><td>{x['name']}</td><td>{x['note']}</td></tr>"
                 for x in d.get("near_misses", []))
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sector Impact — Top 10 Most Impacted · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_SECTORS}
<main class="main">
<h1>🌍 Sector Impact — Top 10 Areas Most Impacted Right Now</h1>
<div class="sub">Ranked by magnitude of impact from the current economic, market, trade, inflation,
jobs, political and geopolitical environment · research snapshot as of {d.get('asof', meta['asof'])}</div>
<div class="cards">
  <div class="card"><div class="n">{len(d.get('forces', []))}</div><div class="l">Cross-cutting forces</div></div>
  <div class="card"><div class="n">{len(d.get('sectors', []))}</div><div class="l">Areas ranked by impact</div></div>
  <div class="card"><div class="n">{sum(1 for s in d.get('sectors', []) if s.get('direction') == 'Negative')}</div><div class="l">Facing headwinds</div></div>
  <div class="card"><div class="n">{sum(1 for s in d.get('sectors', []) if s.get('direction') == 'Positive')}</div><div class="l">Getting tailwinds</div></div>
  <div class="card"><div class="n">{len(d.get('near_misses', []))}</div><div class="l">Near misses</div></div>
</div>
<section><h2>The six cross-cutting forces</h2>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px">{forces}</div></section>
{chr(10).join(cards)}
<section><h2>Near misses</h2>
<div class="tablewrap"><table><thead><tr><th>Area</th><th>Why it missed the Top 10</th></tr></thead><tbody>{nm}</tbody></table></div>
<p class="muted">Compiled from wire and major-press reporting (Reuters, Bloomberg, AP and others,
linked per sector). A snapshot, refreshed by research rather than by the daily screener.
Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
    with open(os.path.join(ROOT, "sectors.html"), "w") as fh:
        fh.write(html)


def build_validated_page(meta, verdicts, kpi_ix=None):
    vs = verdicts.get("verdicts") or {}
    final = verdicts.get("final50") or []
    pools_p = os.path.join(DATA, "pools.json")
    pools = json.load(open(pools_p)) if os.path.exists(pools_p) else {"dip": [], "pro": []}
    dip_ix = {r["ticker"]: r for r in pools.get("dip", [])}
    pro_ix = {r["ticker"]: r for r in pools.get("pro", [])}
    kpi_ix = kpi_ix or {}
    cc = load_crosscheck()
    counts = {"BUY": 0, "HOLD": 0, "SELL": 0}
    for v in vs.values():
        if v.get("flag") in counts:
            counts[v["flag"]] += 1
    n_checked = sum(1 for t in final if f"validated:{t}" in cc or f"theme:{t}" in cc)
    rows = []
    for i, t in enumerate(final, 1):
        v = vs.get(t, {})
        d, pr = dip_ix.get(t), pro_ix.get(t)
        origin = []
        if d:
            origin.append(f"Dip #{d['rank']}")
        if pr:
            origin.append(f"Pro #{pr['rank']} ({pr.get('grade', '')})")
        src = d or pr or {}
        k = kpi_ix.get(t, {})
        last = k.get("last", src.get("last", 0))
        off = k.get("off_high_pct", src.get("off_high_pct", 0))
        rp = k.get("range_pos")
        range_html = (f"<span class='range'><span class='fill' style='width:{rp}%'></span></span> "
                      if rp is not None else "")
        day = k.get("day_pct")
        day_html = (f"<span class='{'up' if day >= 0 else 'dn'}'>{day:+.2f}%</span>"
                    if day is not None else '<span class="muted">—</span>')
        rsi = k.get("rsi14")
        rs = k.get("rs20")
        ev = (f"<div class='evline'>{len(v.get('sources') or [])} sources cited · "
              f"researched {v.get('date', '')}{audit_badge(t)}</div>")
        rsi_html = f"{rsi:.0f}" if rsi is not None else "—"
        rs_html = (f"<span class='{'up' if rs >= 0 else 'dn'}'>{rs:+.1f}%</span>"
                   if rs is not None else "—")
        earn_html = k.get("earnings") or '<span class="muted">—</span>'
        det = (pkpi_grid([
            (f"${last:,.2f}", "Price"), (day_html, "Day"),
            (f"{off:.1f}%", "vs 52w high"), (rsi_html, "RSI"),
            (rs_html, "Rel Str 20d"), (str(earn_html), "Earnings")])
            + verdict_detail_html(v, t, page=t))
        rows.append(
            f"<tr data-q='{t} {clean_company_name(UNIVERSE_NAME.get(t, t))}'><td>{i}</td>"
            f"<td><a class='sym' href='stock/{t}.html'>{t}</a>"
            f"<span class='nm'>{clean_company_name(UNIVERSE_NAME.get(t, t))}</span></td>"
            f"<td><span class='chip sb'>BUY</span> {legs_mini(v)}</td>"
            f"<td class='hide-m'>{' · '.join(origin)}</td>"
            f"<td class='num'>${last:,.2f}</td>"
            f"<td class='num'>{day_html}</td>"
            f"<td>{range_html}<span class='dn'>{off:.1f}%</span></td>"
            f"<td class='num hide-m'>{rsi_html}</td>"
            f"<td class='num'>{earn_html}</td>"
            f"<td class='detail'>{det}</td></tr>")
    status = (f"Research complete: {len(final)} names passed the research rubric as BUY. "
              f"Researched in total: {len(vs)} ({counts['BUY']} buy · {counts['HOLD']} hold · "
              f"{counts['SELL']} sell/not-justified)." if len(final) >= 50 else
              f"Research in progress: {len(vs)} names researched so far "
              f"({counts['BUY']} buy · {counts['HOLD']} hold · {counts['SELL']} sell/not-justified); "
              f"{len(final)} buys validated. The list below grows as the research loop works "
              f"through the merged candidate queue.")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vetted Buys · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_VALIDATED}
<main class="main">
<h1>🏆 Vetted Buys — Research-Validated Top 50</h1>
<div class="sub">One merged list from both screens (Quality Dips + Factor Grades), where every name
also passed a dedicated deep-research pass · prices as of {meta['asof']} close · the dots beside
each verdict are its five rubric legs (Quality · Valuation · Sector · Risk · Catalyst).
<b>Select any row</b> — its full evidence opens in the panel.</div>
<div class="cards">
  <div class="card"><div class="n">{len(final)}</div><div class="l">Validated BUY names</div></div>
  <div class="card"><div class="n">{len(vs)}</div><div class="l">Names researched</div></div>
  <div class="card"><div class="n">{counts['BUY']} · {counts['HOLD']} · {counts['SELL']}</div><div class="l">Buy · Hold · Sell verdicts</div></div>
  <div class="card"><div class="n">{n_checked}</div><div class="l">Cards evidence cross-checked</div></div>
</div>
<p class="sub">{status}</p>
<div class="controls"><input id="q" type="text" placeholder="Search symbol or company…"></div>
<div class="master">
<div class="tablewrap"><table id="t" class="mlist"><thead><tr><th>#</th><th>Stock</th><th>Verdict · legs</th><th class="hide-m">Origin</th>
<th class="num">Price</th><th class="num">Day</th><th>vs 52w high</th>
<th class="num hide-m">RSI</th><th class="num">Earnings</th></tr></thead>
<tbody>{chr(10).join(rows)}</tbody></table></div>
{panel_html()}
</div>
<p class="foot">Method: candidates from both Top 50 lists first, then the
merged bench in rank order, were deep-researched one by one against a five-leg rubric — business
quality intact, valuation/dip justified, sector wind (see the Sector Impact tab), risk survivability, and a
visible catalyst path. A name is BUY only with no leg failing and at least four passing; HOLD and
SELL verdicts drop out and the next bench name is researched, until 50 BUY names stand. Every stock
page carries the full verdict with evidence, risks, and what would change it. Verdicts are a dated
research snapshot (not refreshed by the daily screener) from public sources — structured research
opinion, not investment advice or a trade instruction.</p>
{glossary_html()}
</main></div>
{PANEL_JS}</body></html>"""
    with open(os.path.join(ROOT, "validated.html"), "w") as fh:
        fh.write(html)


def build_validated_details(meta, verdicts, covered, results, pro_recs, earn):
    """Detail pages for validated names covered by neither list builder."""
    fortune = json.load(open(os.path.join(DATA, "fortune500.json")))
    asof = meta["asof"]
    for t in verdicts.get("final50") or []:
        if t in covered:
            continue
        k = results.get(t)
        if not k:
            continue
        secs = []
        prec = pro_recs.get(t)
        psec = pro_section(t, prec, k, None)
        if psec:
            secs.append(psec)
        vsec = verdict_section(t, verdicts) or verdict_section(t, load_theme_verdicts())
        if vsec:
            secs.append(vsec)
        pe = (round(100 / prec["earn_y"], 1)
              if prec and prec.get("earn_y") and prec["earn_y"] > 0 else None)
        tiles = kpi_tiles(k, rs20=k["mom20_pct"] - meta.get("spy_mom20", 0.0),
                          pe=pe, earn=earn.get(t), extra=(
            _tile(f"{k['vpt20']:+.0f}%", "VPT trend (20d)",
                  "up" if k["vpt20"] >= 0 else "dn")))
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("validated", "../")}
<main class="main">
<p class="crumb"><a href="../validated.html">← Back to Vetted Buys</a></p>
<h1>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Research-validated Top 50</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
{tiles}
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a></p>
<p class="muted">Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)


def build_macro_page(meta, tverdicts, themes, kpi_ix=None):
    vs = tverdicts.get("verdicts") or {}
    lst = tverdicts.get("list") or []
    pools_p = os.path.join(DATA, "pools.json")
    pools = json.load(open(pools_p)) if os.path.exists(pools_p) else {"dip": [], "pro": []}
    src_ix = {}
    for r in pools.get("dip", []):
        src_ix.setdefault(r["ticker"], r)
    for r in pools.get("pro", []):
        src_ix.setdefault(r["ticker"], r)
    kpi_ix = kpi_ix or {}
    counts = {"BUY": 0, "HOLD": 0, "SELL": 0}
    for v in vs.values():
        if v.get("flag") in counts:
            counts[v["flag"]] += 1
    by_theme = {}
    for t in lst:
        by_theme.setdefault((vs.get(t) or {}).get("theme_id", ""), []).append(t)

    def mrow(t):
        v = vs.get(t, {})
        s = src_ix.get(t, {})
        k = kpi_ix.get(t, {})
        last = k.get("last", s.get("last", 0))
        off = k.get("off_high_pct", s.get("off_high_pct", 0))
        rp = k.get("range_pos")
        range_html = (f"<span class='range'><span class='fill' style='width:{rp}%'></span></span> "
                      if rp is not None else "")
        day = k.get("day_pct")
        day_html = (f"<span class='{'up' if day >= 0 else 'dn'}'>{day:+.2f}%</span>"
                    if day is not None else '<span class="muted">—</span>')
        rsi = k.get("rsi14")
        rsi_html = f"{rsi:.0f}" if rsi is not None else "—"
        rs = k.get("rs20")
        rs_html = (f"<span class='{'up' if rs >= 0 else 'dn'}'>{rs:+.1f}%</span>"
                   if rs is not None else "—")
        earn_html = k.get("earnings") or '<span class="muted">—</span>'
        det = (pkpi_grid([
            (f"${last:,.2f}", "Price"), (day_html, "Day"),
            (f"{off:.1f}%", "vs 52w high"), (rsi_html, "RSI"),
            (rs_html, "Rel Str 20d"), (str(earn_html), "Earnings")])
            + verdict_detail_html(v, t, page=t))
        return (f"<tr data-q='{t} {clean_company_name(UNIVERSE_NAME.get(t, t))}'>"
                f"<td><a class='sym' href='stock/{t}.html'>{t}</a>"
                f"<span class='nm'>{clean_company_name(UNIVERSE_NAME.get(t, t))}</span></td>"
                f"<td><span class='chip sb'>BUY</span> {legs_mini(v)}</td>"
                f"<td class='num'>${last:,.2f}</td>"
                f"<td class='num'>{day_html}</td>"
                f"<td>{range_html}<span class='dn'>{off:.1f}%</span></td>"
                f"<td class='num hide-m'>{rsi_html}</td>"
                f"<td class='num'>{earn_html}</td>"
                f"<td class='detail'>{det}</td></tr>")

    sections = []
    for th in themes.get("themes", []):
        tid = th["id"]
        names = by_theme.get(tid, [])
        table = ("<div class='tablewrap'><table class='mlist'><thead><tr><th>Stock</th><th>Verdict · legs</th>"
                 "<th class=\"num\">Price</th><th class=\"num\">Day</th><th>vs 52w high</th>"
                 "<th class=\"num hide-m\">RSI</th>"
                 "<th class=\"num\">Earnings</th></tr></thead>"
                 "<tbody>" + chr(10).join(mrow(t) for t in names) + "</tbody></table></div>"
                 ) if names else \
            "<p class='muted'>No name from this theme has passed the rubric as BUY yet.</p>"
        sections.append(
            f"<section><h2>{th.get('icon', '')} {th.get('name', '')} "
            f"<span class='chip na'>{len(names)} BUY</span></h2>"
            f"<p>{th.get('thesis', '')}</p>"
            f"<p class='muted'><b>Watch:</b> {th.get('watch', '')}</p>{table}</section>")
    res = tverdicts.get("reserves") or []
    status = (f"Macro sweep: {len(vs)} names carry a macro verdict so far "
              f"({counts['BUY']} buy · {counts['HOLD']} hold · {counts['SELL']} sell/not-justified); "
              f"{len(lst)} stand on the Top 50 list below, grouped by primary theme"
              + (f", with {len(res)} further BUY names as reserves ({', '.join(res)})" if res else "")
              + ".")
    theme_counts = " · ".join(
        f"{th.get('name', th['id'])} {len(by_theme.get(th['id'], []))}"
        for th in themes.get("themes", []))
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Theme Buys · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_MACRO}
<main class="main">
<h1>🌐 Theme Buys — Macro &amp; Themes Top 50</h1>
<div class="sub">A separate list whose ORIGIN is the macro map — geopolitics, inflation & rates,
the economy, employment, innovation and politics — not the dip or factor screens ·
prices as of {meta['asof']} close · the dots beside each verdict are its five rubric legs
(Quality · Valuation · Sector · Risk · Catalyst). <b>Select any row</b> — its evidence opens in the panel.</div>
<div class="cards">
  <div class="card"><div class="n">{len(lst)}</div><div class="l">Macro Top 50 names</div></div>
  <div class="card"><div class="n">{len(vs)}</div><div class="l">Names with a macro verdict</div></div>
  <div class="card"><div class="n">{counts['BUY']} · {counts['HOLD']} · {counts['SELL']}</div><div class="l">Buy · Hold · Sell verdicts</div></div>
  <div class="card"><div class="n">{len(res)}</div><div class="l">BUY reserves beyond the 50</div></div>
</div>
<section>{status}
<p class="muted"><b>By theme:</b> {theme_counts}. Method: the full 682-name merged candidate pool was swept by theme fit — every
name tagged with the macro theme that genuinely drives its case (geopolitics &amp; energy security,
inflation &amp; rates, economics &amp; growth, employment &amp; labor, innovation, politics &amp;
policy) — and researched against the same five-leg rubric as the Vetted Buys (quality,
valuation, sector, risk, catalyst — BUY only with no leg failing and at least four passing).
Verdicts already earned in the earlier research editions were carried in with their theme tag;
the rest of the pool was researched in theme-fit order until 50 BUYs stood. There is no quota
per theme — counts are whatever the sweep produced. This list sits alongside the Vetted Buys
and does not change it. Verdicts are a dated research snapshot from public sources — structured
research opinion, not investment advice.</p></section>
<div class="controls"><input id="q" type="text" placeholder="Search symbol or company across all themes…"></div>
<div class="master"><div>
{chr(10).join(sections)}
</div>
{panel_html()}
</div>
{glossary_html()}
</main></div>
{PANEL_JS}</body></html>"""
    with open(os.path.join(ROOT, "macro.html"), "w") as fh:
        fh.write(html)


def build_theme_details(meta, tverdicts, covered, results, pro_recs, earn):
    """Detail pages for Macro & Themes names covered by neither list builder."""
    fortune = json.load(open(os.path.join(DATA, "fortune500.json")))
    asof = meta["asof"]
    for t in tverdicts.get("list") or []:
        if t in covered:
            continue
        k = results.get(t)
        if not k:
            continue
        secs = []
        prec = pro_recs.get(t)
        psec = pro_section(t, prec, k, None)
        if psec:
            secs.append(psec)
        vsec = verdict_section(t, tverdicts)
        if vsec:
            secs.append(vsec)
        pe = (round(100 / prec["earn_y"], 1)
              if prec and prec.get("earn_y") and prec["earn_y"] > 0 else None)
        tiles = kpi_tiles(k, rs20=k["mom20_pct"] - meta.get("spy_mom20", 0.0),
                          pe=pe, earn=earn.get(t), extra=(
            _tile(f"{k['vpt20']:+.0f}%", "VPT trend (20d)",
                  "up" if k["vpt20"] >= 0 else "dn")))
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("macro", "../")}
<main class="main">
<p class="crumb"><a href="../macro.html">← Back to Theme Buys</a></p>
<h1>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Macro & Themes validated list</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
{tiles}
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a></p>
<p class="muted">Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)


def rank_holds(verdicts, tverdicts, pools):
    """All HOLD-verdict names from both research stores, ranked by distance
    to BUY. A name BUY in either store is never a hold. The card comes from
    the validated store when present, else the theme store. Ranking is
    mechanical, from the card's own rubric legs: most legs passing first,
    fewest failing next; the Professional composite breaks ties, then the
    Quality-Dip score, then the ticker (deterministic order)."""
    rv = verdicts.get("verdicts") or {}
    tv = tverdicts.get("verdicts") or {}
    buy_any = {t for t, v in list(rv.items()) + list(tv.items())
               if v.get("flag") == "BUY"}
    pro_ix = {r["ticker"]: r for r in pools.get("pro", [])}
    dip_ix = {r["ticker"]: r for r in pools.get("dip", [])}
    out = []
    for t in sorted(set(rv) | set(tv)):
        if t in buy_any:
            continue
        store, v = (("validated", rv[t]) if rv.get(t, {}).get("flag") == "HOLD"
                    else ("theme", tv.get(t)) if tv.get(t, {}).get("flag") == "HOLD"
                    else (None, None))
        if not v:
            continue
        legs = v.get("legs") or {}
        vs_ = [lv.get("v") for lv in legs.values() if isinstance(lv, dict)]
        passes, fails = vs_.count("pass"), vs_.count("fail")
        blocked = [ln for ln, lv in legs.items()
                   if isinstance(lv, dict) and lv.get("v") == "fail"]
        out.append({"ticker": t, "card": v, "store": store,
                    "passes": passes, "fails": fails,
                    "mixed": vs_.count("mixed"), "blocked": blocked,
                    "pro_comp": (pro_ix.get(t) or {}).get("composite"),
                    "dip_score": (dip_ix.get(t) or {}).get("score")})
    out.sort(key=lambda r: (-r["passes"], r["fails"],
                            -(r["pro_comp"] if r["pro_comp"] is not None else -1),
                            -(r["dip_score"] if r["dip_score"] is not None else -1),
                            r["ticker"]))
    return out


def build_holds_page(meta, verdicts, tverdicts, kpi_ix=None):
    pools_p = os.path.join(DATA, "pools.json")
    pools = json.load(open(pools_p)) if os.path.exists(pools_p) else {"dip": [], "pro": []}
    ranked = rank_holds(verdicts, tverdicts, pools)
    top = ranked[:50]
    dip_ix = {r["ticker"]: r for r in pools.get("dip", [])}
    pro_ix = {r["ticker"]: r for r in pools.get("pro", [])}
    kpi_ix = kpi_ix or {}
    n_four = sum(1 for r in ranked if r["passes"] >= 4)
    rows = []
    for i, r in enumerate(top, 1):
        t, v = r["ticker"], r["card"]
        d, pr = dip_ix.get(t), pro_ix.get(t)
        origin = []
        if d:
            origin.append(f"Dip #{d['rank']}")
        if pr:
            origin.append(f"Pro #{pr['rank']} ({pr.get('grade', '')})")
        origin.append("Validated bench" if r["store"] == "validated" else "Macro bench")
        src = d or pr or {}
        k = kpi_ix.get(t, {})
        last = k.get("last", src.get("last", 0))
        off = k.get("off_high_pct", src.get("off_high_pct", 0))
        rp = k.get("range_pos")
        range_html = (f"<span class='range'><span class='fill' style='width:{rp}%'></span></span> "
                      if rp is not None else "")
        day = k.get("day_pct")
        day_html = (f"<span class='{'up' if day >= 0 else 'dn'}'>{day:+.2f}%</span>"
                    if day is not None else '<span class="muted">—</span>')
        rsi = k.get("rsi14")
        rsi_html = f"{rsi:.0f}" if rsi is not None else "—"
        rs = k.get("rs20")
        rs_html = (f"<span class='{'up' if rs >= 0 else 'dn'}'>{rs:+.1f}%</span>"
                   if rs is not None else "—")
        earn_html = k.get("earnings") or '<span class="muted">—</span>'
        blocked = (" · ".join(x.capitalize() for x in r["blocked"])
                   if r["blocked"] else "— no failing leg")
        profile = f"{r['passes']} pass · {r['mixed']} mixed · {r['fails']} fail"
        ev = (f"<div class='evline'>{len(v.get('sources') or [])} sources cited · "
              f"researched {v.get('date', '')}{audit_badge(t)}</div>")
        flip = (f"<div class='evline'><b>What flips it:</b> "
                f"{v.get('what_changes', '')}</div>")
        sym = (f"<a class='sym' href='stock/{t}.html'>{t}</a>" if t in kpi_ix
               else f"<span class='sym'>{t}</span>")
        det = (pkpi_grid([
            (f"${last:,.2f}", "Price"), (day_html, "Day"),
            (f"{off:.1f}%", "vs 52w high"), (rsi_html, "RSI"),
            (rs_html, "Rel Str 20d"), (str(earn_html), "Earnings")])
            + f"<p class='muted'>{profile} · blocked by: {blocked}</p>"
            + verdict_detail_html(v, t,
                                  page=t if t in kpi_ix else None))
        rows.append(
            f"<tr data-q='{t} {clean_company_name(UNIVERSE_NAME.get(t, t))}'><td>{i}</td>"
            f"<td>{sym}<span class='nm'>{clean_company_name(UNIVERSE_NAME.get(t, t))}</span></td>"
            f"<td><span class='chip h'>HOLD</span> {legs_mini(v)}"
            f"<div class='evline'>{profile}</div></td>"
            f"<td>{blocked}</td>"
            f"<td class='hide-m'>{' · '.join(origin)}</td>"
            f"<td class='num'>${last:,.2f}</td>"
            f"<td class='num'>{day_html}</td>"
            f"<td>{range_html}<span class='dn'>{off:.1f}%</span></td>"
            f"<td class='num hide-m'>{rsi_html}</td>"
            f"<td class='num'>{earn_html}</td>"
            f"<td class='detail'>{det}</td></tr>")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>The Bench — Top 50 Holds · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_HOLDS}
<main class="main">
<h1>🟡 The Bench — Top 50 Holds</h1>
<div class="sub">Every deep-researched name that landed on HOLD, ranked by distance to BUY ·
prices as of {meta['asof']} close · the dots beside each verdict are its five rubric legs
(Quality · Valuation · Sector · Risk · Catalyst). <b>Select any row</b> — the leg that blocks it
and the evidence that would flip it open in the panel.</div>
<div class="cards">
  <div class="card"><div class="n">{len(top)}</div><div class="l">Holds shown</div></div>
  <div class="card"><div class="n">{len(ranked)}</div><div class="l">HOLD verdicts in total</div></div>
  <div class="card"><div class="n">{n_four}</div><div class="l">With 4 legs passing (one flip from BUY)</div></div>
  <div class="card"><div class="n">{len(ranked) - len(top)}</div><div class="l">Further holds below the cut</div></div>
</div>
<p class="sub">A HOLD is an unproven case, not a rejected one: exactly one rubric leg failing, or only
three passing with none failing.</p>
<div class="controls"><input id="q" type="text" placeholder="Search symbol or company…"></div>
<div class="master">
<div class="tablewrap"><table id="t" class="mlist"><thead><tr><th>#</th><th>Stock</th><th>Verdict · legs</th><th>Blocked by</th><th class="hide-m">Origin</th>
<th class="num">Price</th><th class="num">Day</th><th>vs 52w high</th>
<th class="num hide-m">RSI</th><th class="num">Earnings</th></tr></thead>
<tbody>{chr(10).join(rows)}</tbody></table></div>
{panel_html()}
</div>
<p class="foot">Method: the population is every HOLD verdict in the two
research stores (Vetted bench and Theme bench); a name that is BUY in either store never appears
here. Ranking is mechanical, computed from each card's own rubric legs — legs passing (descending),
legs failing (ascending), then the Factor Grades composite, then the Quality Dips score. Verdicts are
a dated research snapshot (researched 2026-10-25; not re-researched by the daily screener, which
refreshes prices and KPIs only) from public sources — structured research opinion, not investment
advice or a trade instruction.</p>
{glossary_html()}
</main></div>
{PANEL_JS}</body></html>"""
    with open(os.path.join(ROOT, "holds.html"), "w") as fh:
        fh.write(html)
    return ranked


def build_holds_details(meta, ranked_top, verdicts, tverdicts, covered,
                        results, pro_recs, earn):
    """Detail pages for Top-50 Holds covered by no other list builder."""
    fortune = json.load(open(os.path.join(DATA, "fortune500.json")))
    asof = meta["asof"]
    stores = {"validated": verdicts, "theme": tverdicts}
    for r in ranked_top:
        t = r["ticker"]
        if t in covered:
            continue
        k = results.get(t)
        if not k:
            continue
        secs = []
        prec = pro_recs.get(t)
        psec = pro_section(t, prec, k, None)
        if psec:
            secs.append(psec)
        vsec = verdict_section(t, stores[r["store"]])
        if vsec:
            secs.append(vsec)
        pe = (round(100 / prec["earn_y"], 1)
              if prec and prec.get("earn_y") and prec["earn_y"] > 0 else None)
        tiles = kpi_tiles(k, rs20=k["mom20_pct"] - meta.get("spy_mom20", 0.0),
                          pe=pe, earn=earn.get(t), extra=(
            _tile(f"{k['vpt20']:+.0f}%", "VPT trend (20d)",
                  "up" if k["vpt20"] >= 0 else "dn")))
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("holds", "../")}
<main class="main">
<p class="crumb"><a href="../holds.html">← Back to The Bench</a></p>
<h1>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Top 50 Holds — research bench</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
{tiles}
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a></p>
<p class="muted">Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)


def _edgar_insider_cluster(t, today):
    """Open-market insider buys/sells for one ticker over the last 12 months,
    parsed from the company's own Form 4 filings on SEC EDGAR (submissions
    API + filing XML; transaction codes P = open-market purchase, S = sale).
    Returns a summary dict or None on any failure (the badge then simply
    doesn't render)."""
    import xml.etree.ElementTree as ET
    cik = _edgar_cik(t)
    if not cik:
        return None
    try:
        raw = http_get(f"https://data.sec.gov/submissions/CIK{cik}.json",
                       timeout=25, ua=EDGAR_UA)
        sub = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        return None
    recent = (sub.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    dates = recent.get("filingDate") or []
    accs = recent.get("accessionNumber") or []
    docs = recent.get("primaryDocument") or []
    cutoff = today - timedelta(days=365)
    targets = []
    for form, fdate, acc, doc in zip(forms, dates, accs, docs):
        if form != "4" or len(targets) >= 80:
            continue
        try:
            fd = date.fromisoformat(fdate)
        except (TypeError, ValueError):
            continue
        if fd < cutoff - timedelta(days=10) or fd > today + timedelta(days=2):
            continue
        # primaryDocument often points at the XSLT-rendered view
        # (xslF345X06/<file>.xml), which returns HTML; the raw XML is the
        # same file without that directory prefix.
        doc = re.sub(r"^xslF345X0\d/", "", doc)
        targets.append((f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
                        f"{acc.replace('-', '')}/{doc}"))

    def parse_filing(url):
        try:
            root = ET.fromstring(http_get(url, timeout=20, ua=EDGAR_UA))
        except Exception:
            return []
        owner = root.findtext(".//rptOwnerName") or "?"
        txns = []
        for txn in root.iter("nonDerivativeTransaction"):
            code = txn.findtext(".//transactionCode") or ""
            if code not in ("P", "S"):
                continue
            try:
                d = date.fromisoformat(
                    (txn.findtext(".//transactionDate/value") or "")[:10])
                shares = float(txn.findtext(
                    ".//transactionShares/value") or 0)
                price = float(txn.findtext(
                    ".//transactionPricePerShare/value") or 0)
            except (TypeError, ValueError):
                continue
            if cutoff <= d <= today:
                txns.append((code, owner, d, shares * price))
        return txns

    buyers, sellers = {}, {}
    buy_val = sell_val = 0.0
    last_buy = None
    with ThreadPoolExecutor(max_workers=6) as ex:
        for txns in ex.map(parse_filing, targets):
            for code, owner, d, val in txns:
                if code == "P":
                    buyers[owner] = buyers.get(owner, 0) + 1
                    buy_val += val
                    if last_buy is None or d > last_buy:
                        last_buy = d
                else:
                    sellers[owner] = sellers.get(owner, 0) + 1
                    sell_val += val
    return {"asof": today.isoformat(), "buyers": len(buyers),
            "buy_val": round(buy_val),
            "last_buy": last_buy.isoformat() if last_buy else None,
            "sellers": len(sellers), "sell_val": round(sell_val)}


def fetch_insider_clusters(tickers, max_age_days=7):
    """Weekly-cached insider-cluster summaries for the screener finalists."""
    cache_p = os.path.join(DATA, "insider_cache.json")
    cache = {}
    if os.path.exists(cache_p):
        try:
            cache = json.load(open(cache_p))
        except Exception:
            cache = {}
    today = date.today()
    out, dirty = {}, False
    for t in tickers:
        e = cache.get(t)
        fresh = False
        if e and e.get("asof"):
            try:
                fresh = (today - date.fromisoformat(e["asof"])).days < max_age_days
            except ValueError:
                fresh = False
        if fresh:
            out[t] = e
            continue
        e = _edgar_insider_cluster(t, today)
        if e:
            cache[t] = e
            out[t] = e
            dirty = True
    if dirty:
        try:
            json.dump(cache, open(cache_p, "w"), indent=1)
        except Exception:
            pass
    return out


def rank_screener(results, pro_recs):
    """The Fallen & Cheap screen — a mechanical reconstruction of the
    'fallen + cheap + compressing multiple' approach: deep drawdown,
    EV/EBITDA <= 15 and compressing vs a year ago on a consistent EDGAR
    basis, positive FCF, net debt/EBITDA <= 3. Ranked cheapest-multiple
    first (the shared screener's apparent order), then deepest
    compression, then deepest drawdown. Purely mechanical: the Dip tab's
    research exclusions do NOT apply here; researched verdicts are shown
    as badges instead."""
    out = []
    funnel = {"pool": 0, "drawdown": 0, "multiple": 0, "leverage": 0,
              "fcf": 0, "tradable": 0}
    watch = {}
    for t, r in pro_recs.items():
        k = results.get(t)
        if not k:
            continue
        funnel["pool"] += 1
        off = k["off_high_pct"]
        evx, evx1 = r.get("ev_ebitda"), r.get("ev_ebitda_1y")
        nd = r.get("nd_ebitda")
        stage = None
        if not (-65 <= off <= -20):
            stage = "drawdown"
        elif not evx or evx > 15 or not evx1 or evx >= evx1:
            stage = "multiple"
        elif nd is None or nd > 3:
            stage = "leverage"
        elif r.get("fcf") is None or r["fcf"] < 0:
            stage = "fcf"
        elif k["last"] < 3 or k.get("adv_m", 0) < 5:
            stage = "tradable"
        if t in ("NKE", "LULU", "ALG", "DORM"):
            watch[t] = (f"off {off:.1f}%, EV/EBITDA {evx and round(evx, 1)} "
                        f"vs 1y {evx1 and round(evx1, 1)}, ND/EBITDA "
                        f"{nd and round(nd, 1)}, FCF "
                        f"{r.get('fcf') and round(r['fcf'] / 1e6)}M -> "
                        f"{stage or 'PASSES'}")
        if stage:
            funnel[stage] += 1
            continue
        out.append({"ticker": t, "ev_ebitda": evx, "ev_ebitda_1y": evx1,
                    "ev_comp": r.get("ev_comp"), "nd_ebitda": nd,
                    "fcf": r["fcf"], "grade": r.get("grade"),
                    "off_high_pct": off, "last": k["last"]})
    out.sort(key=lambda r: (r["ev_ebitda"],
                            -(r["ev_comp"] if r["ev_comp"] is not None else 0),
                            r["off_high_pct"], r["ticker"]))
    print(f"SCREENER FUNNEL: pool {funnel['pool']} -> failed drawdown "
          f"{funnel['drawdown']}, multiple {funnel['multiple']}, leverage "
          f"{funnel['leverage']}, fcf {funnel['fcf']}, tradable "
          f"{funnel['tradable']} -> passed {len(out)}")
    for t, w in watch.items():
        print(f"SCREENER WATCH {t}: {w}")
    return out


def _verdict_chip(t, verdicts, tverdicts):
    for store in (verdicts, tverdicts):
        v = (store.get("verdicts") or {}).get(t)
        if v:
            cls = {"BUY": "sb", "HOLD": "h", "SELL": "r"}.get(v.get("flag", ""), "na")
            return (f" <span class='chip {cls}' title='Researched verdict "
                    f"({v.get('date', '')}) — a separate lens; this screen is "
                    f"purely mechanical'>{v.get('flag', '')}</span>")
    return ""


def _insider_html(ins):
    if not ins:
        return '<span class="muted">—</span>'
    b = ins.get("buyers", 0)
    if b >= 2:
        return (f"<span class='chip sb' title='Open-market insider purchases, "
                f"last 12 months (SEC Form 4 filings)'>◆ cluster: {b} buyers · "
                f"${ins.get('buy_val', 0):,}</span>")
    if b == 1:
        return (f"<span class='chip h' title='One insider buying open-market, "
                f"last 12 months (SEC Form 4 filings)'>1 buyer · "
                f"${ins.get('buy_val', 0):,}</span>")
    s = ins.get("sellers", 0)
    if s:
        return (f"<span class='muted' title='No open-market insider buys in the "
                f"last 12 months; {s} insider(s) sold'>no buys · {s} seller(s)</span>")
    return '<span class="muted">no insider trades 12mo</span>'


def build_screener_page(meta, rows, insiders, verdicts, tverdicts, kpi_ix=None):
    kpi_ix = kpi_ix or {}
    top = rows[:50]
    n_cluster = sum(1 for r in top
                    if (insiders.get(r["ticker"]) or {}).get("buyers", 0) >= 2)
    med = (sorted(r["ev_ebitda"] for r in top)[len(top) // 2] if top else 0)
    trs = []
    for i, r in enumerate(top, 1):
        t = r["ticker"]
        k = kpi_ix.get(t, {})
        day = k.get("day_pct")
        day_html = (f"<span class='{'up' if day >= 0 else 'dn'}'>{day:+.2f}%</span>"
                    if day is not None else '<span class="muted">—</span>')
        rp = k.get("range_pos")
        range_html = (f"<span class='range'><span class='fill' style='width:{rp}%'></span></span> "
                      if rp is not None else "")
        comp = r.get("ev_comp")
        comp_html = (f"<span class='up'>{comp:.0f}%</span>" if comp is not None
                     else "—")
        fcf_m = r["fcf"] / 1e6
        why = (f"{abs(r['off_high_pct']):.0f}% off its high, EV/EBITDA compressed "
               f"from {r['ev_ebitda_1y']:.1f}× to {r['ev_ebitda']:.1f}× in a year, "
               f"FCF ${fcf_m:,.0f}M, leverage {r['nd_ebitda']:.1f}×")
        sym = (f"<a class='sym' href='stock/{t}.html'>{t}</a>" if t in kpi_ix
               else f"<span class='sym'>{t}</span>")
        ins = insiders.get(t)
        ins_det = ""
        if ins:
            ins_det = (f"<h3>Insider record — SEC Form 4, last 12 months</h3>"
                       f"<p><b>{ins.get('buyers', 0)} open-market buyer(s), "
                       f"${ins.get('buy_val', 0):,}</b>"
                       + (f", most recent {ins.get('last_buy')}"
                          if ins.get("last_buy") else "")
                       + f"; {ins.get('sellers', 0)} seller(s), "
                       f"${ins.get('sell_val', 0):,}.</p>")
        v = (verdicts.get("verdicts") or {}).get(t) or \
            (tverdicts.get("verdicts") or {}).get(t)
        det = (pkpi_grid([
            (f"${r['last']:,.2f}", "Price"), (day_html, "Day"),
            (f"{r['off_high_pct']:.1f}%", "vs 52w high"),
            (f"{r['ev_ebitda']:.1f}×", "EV/EBITDA now"),
            (f"{r['ev_ebitda_1y']:.1f}×", "EV/EBITDA 1y ago"),
            (f"{r['nd_ebitda']:.1f}×", "Net debt/EBITDA")])
            + f"<h3>Why it's here</h3><p>{why}</p>{ins_det}"
            + (verdict_detail_html(v, t, page=t if t in kpi_ix else None)
               if v else
               (f"<a class='pfull' href='stock/{t}.html'>Full stock page →</a>"
                if t in kpi_ix else "")))
        trs.append(
            f"<tr data-q='{t} {clean_company_name(UNIVERSE_NAME.get(t, t))}'><td>{i}</td>"
            f"<td>{sym}<span class='nm'>{clean_company_name(UNIVERSE_NAME.get(t, t))}</span>"
            f"{_verdict_chip(t, verdicts, tverdicts)}</td>"
            f"<td class='num'>${r['last']:,.2f}</td>"
            f"<td class='num'>{day_html}</td>"
            f"<td>{range_html}<span class='dn'>{r['off_high_pct']:.1f}%</span></td>"
            f"<td class='num'><b>{r['ev_ebitda']:.1f}×</b></td>"
            f"<td class='num hide-m'>{r['ev_ebitda_1y']:.1f}×</td>"
            f"<td class='num'>{comp_html}</td>"
            f"<td class='num hide-m'>{r['nd_ebitda']:.1f}×</td>"
            f"<td>{_insider_html(ins)}</td>"
            f"<td class='detail'>{det}</td></tr>")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fallen &amp; Cheap · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_SCREENER}
<main class="main">
<h1>🔎 Fallen &amp; Cheap — Screener Top 50</h1>
<div class="sub">A mechanical value screen in the style of the classic fallen-names approach:
deep drawdown, a low EV/EBITDA that is compressing against the company's own history, and a
balance sheet that survives · prices as of {meta['asof']} close. <b>Select any row</b> — its
numbers and insider record open in the panel.</div>
<div class="cards">
  <div class="card"><div class="n">{len(top)}</div><div class="l">Names shown</div></div>
  <div class="card"><div class="n">{len(rows)}</div><div class="l">Passed every gate</div></div>
  <div class="card"><div class="n">{n_cluster}</div><div class="l">With insider buying clusters (12mo)</div></div>
  <div class="card"><div class="n">{med:.1f}×</div><div class="l">Median EV/EBITDA of the list</div></div>
</div>
<p class="sub">The gates, applied to the same 1,654-name universe: <b>20–65% below the 52-week high</b> ·
<b>EV/EBITDA ≤ 15×</b> · <b>the multiple lower than a year ago</b> (SEC EDGAR basis: current price
and balance sheet vs the year-ago price, prior-year EBITDA and balance sheet) ·
<b>net debt/EBITDA ≤ 3×</b> · <b>positive free cash flow</b> · price ≥ $3, ADV ≥ $5M. Ranking is
cheapest multiple first, then deepest compression — the order the classic screen produces.
Insider clusters (≥2 distinct open-market buyers in 12 months, SEC Form 4) are shown as a
signal, not a gate. This lens is purely mechanical — it does not apply the research exclusions of
the other tabs; where a name carries a researched verdict, it is badged beside the ticker.</p>
<div class="controls"><input id="q" type="text" placeholder="Search symbol or company…"></div>
<div class="master">
<div class="tablewrap"><table id="t" class="mlist"><thead><tr><th>#</th><th>Stock</th>
<th class="num">Price</th><th class="num">Day</th><th>vs 52w high</th>
<th class="num">EV/EBITDA</th><th class="num hide-m">1y ago</th><th class="num">Compression</th>
<th class="num hide-m">Net debt/EBITDA</th><th>Insider buying (12mo)</th></tr></thead>
<tbody>{chr(10).join(trs)}</tbody></table></div>
{panel_html()}
</div>
<p class="foot">Method and limits: EV/EBITDA uses EDGAR annual filings
(operating income + D&amp;A as the EBITDA proxy, rebuilt from net income + interest + tax + D&amp;A
where operating income is not tagged) — a consistent basis for the now-vs-year-ago
comparison, but not identical to vendor trailing-twelve-month figures. The screen cannot see
guidance cuts, one-off items inside earnings, or off-balance-sheet obligations; the researched
tabs exist for that. Micro-caps outside the universe (top-800 by volume ∪ S&amp;P 1000) cannot
appear here at all. Structured research output, not investment advice or a trade instruction.</p>
{glossary_html()}
</main></div>
{PANEL_JS}</body></html>"""
    with open(os.path.join(ROOT, "screener.html"), "w") as fh:
        fh.write(html)
    return top


def screener_section(t, row, ins):
    if not row:
        return ""
    ins_line = ""
    if ins:
        ins_line = (f"<p>Insider record (SEC Form 4, last 12 months): "
                    f"<b>{ins.get('buyers', 0)} open-market buyer(s), "
                    f"${ins.get('buy_val', 0):,}</b>"
                    + (f", most recent {ins.get('last_buy')}" if ins.get("last_buy") else "")
                    + f"; {ins.get('sellers', 0)} seller(s), ${ins.get('sell_val', 0):,}.</p>")
    return (f"<section><h2>🔎 Fallen &amp; Cheap screen</h2>"
            f"<p>On the mechanical screener list at #{row['rank']}: "
            f"<b>{abs(row['off_high_pct']):.1f}% below its 52-week high</b>, "
            f"EV/EBITDA <b>{row['ev_ebitda']:.1f}×</b> vs {row['ev_ebitda_1y']:.1f}× a year ago "
            f"({row.get('ev_comp', 0):.0f}% compression), net debt/EBITDA "
            f"{row['nd_ebitda']:.1f}×, free cash flow ${row['fcf'] / 1e6:,.0f}M.</p>"
            f"{ins_line}"
            f"<p class='muted'>A purely mechanical screen (drawdown + cheap, compressing "
            f"multiple + survivability). It does not read guidance, one-offs or filings text — "
            f"see the research verdict, where present, for that layer.</p></section>")


def build_screener_details(meta, top, insiders, verdicts, tverdicts,
                           results, pro_recs, earn):
    """Detail pages for screener names covered by no other list builder."""
    fortune = json.load(open(os.path.join(DATA, "fortune500.json")))
    asof = meta["asof"]
    rank_ix = {r["ticker"]: dict(r, rank=i + 1) for i, r in enumerate(top)}
    for t, row in rank_ix.items():
        if os.path.exists(os.path.join(ROOT, "stock", f"{t}.html")):
            continue
        k = results.get(t)
        if not k:
            continue
        secs = [screener_section(t, row, insiders.get(t))]
        prec = pro_recs.get(t)
        psec = pro_section(t, prec, k, None)
        if psec:
            secs.append(psec)
        vsec = verdict_section(t, verdicts) or verdict_section(t, tverdicts)
        if vsec:
            secs.append(vsec)
        pe = (round(100 / prec["earn_y"], 1)
              if prec and prec.get("earn_y") and prec["earn_y"] > 0 else None)
        tiles = kpi_tiles(k, rs20=k["mom20_pct"] - meta.get("spy_mom20", 0.0),
                          pe=pe, earn=earn.get(t), extra=(
            _tile(f"{k['vpt20']:+.0f}%", "VPT trend (20d)",
                  "up" if k["vpt20"] >= 0 else "dn")))
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("screener", "../")}
<main class="main">
<p class="crumb"><a href="../screener.html">← Back to Fallen &amp; Cheap</a></p>
<h1>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Fallen &amp; Cheap — Screener Top 50</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
{tiles}
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a></p>
<p class="muted">Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)


def load_skin():
    """The Skin in the Game RESEARCH OVERLAY (data/skin_in_game.json) — a
    curated, dated research store like the verdict stores. The pipeline
    LOADS it and never re-scores or re-researches it. Tab membership no
    longer comes from this store: since 2026-10-26 the tab is a daily
    mechanical re-screen (skin_prepass + rank_skin), and the store only
    supplies per-ticker researched scorecards for the panel layer."""
    p = os.path.join(DATA, "skin_in_game.json")
    if os.path.exists(p):
        try:
            return json.load(open(p))
        except Exception:
            pass
    return {"meta": {}, "members": []}


_SKIN_CRITERIA = [("brand", "Brand"), ("fallen", "Fallen"),
                  ("trough", "Trough"), ("balance", "Balance sheet"),
                  ("insider", "Insider buying"), ("turnaround", "Turnaround")]


def _crit_chip(v):
    cls = {"pass": "sb", "mixed": "h", "fail": "r"}.get(v, "na")
    return f"<span class='chip {cls}'>{v}</span>"


def skin_scorecard_rows(m):
    crit = m.get("criteria") or {}
    notes = m.get("notes") or {}
    return "".join(
        f"<tr><td>{label}</td><td>{_crit_chip(crit.get(key, ''))}</td>"
        f"<td>{notes.get(key, '')}</td></tr>"
        for key, label in _SKIN_CRITERIA)


def _skin_verdict(t, verdicts, tverdicts):
    for store in (verdicts, tverdicts):
        v = (store.get("verdicts") or {}).get(t)
        if v:
            return v
    return None


def skin_prepass(results, pro_recs):
    """Skin in the Game gates 1-5 — everything except the insider gate,
    which needs the Form 4 cache: fallen at least 35% off the 52-week
    high, EV/EBITDA in (0, 15] and strictly compressing vs a year ago,
    net debt/EBITDA <= 3, FCF >= 0, price >= $3 and ADV >= $5M. Uses the
    same EDGAR-basis records as the Fallen & Cheap screen, so the two
    tabs agree on every number. Returns (rows, funnel) where the funnel
    counts survivors after each stage."""
    rows = []
    fun = {"pool": 0, "fallen": 0, "multiple": 0, "leverage": 0, "fcf": 0}
    for t, r in pro_recs.items():
        k = results.get(t)
        if not k:
            continue
        fun["pool"] += 1
        off = k["off_high_pct"]
        evx, evx1 = r.get("ev_ebitda"), r.get("ev_ebitda_1y")
        nd = r.get("nd_ebitda")
        if off > -35:
            continue
        fun["fallen"] += 1
        if not evx or evx > 15 or not evx1 or evx >= evx1:
            continue
        fun["multiple"] += 1
        if nd is None or nd > 3:
            continue
        fun["leverage"] += 1
        if (r.get("fcf") is None or r["fcf"] < 0
                or k["last"] < 3 or k.get("adv_m", 0) < 5):
            continue
        fun["fcf"] += 1
        rows.append({"ticker": t, "ev_ebitda": evx, "ev_ebitda_1y": evx1,
                     "ev_comp": r.get("ev_comp"), "nd_ebitda": nd,
                     "fcf": r["fcf"], "grade": r.get("grade"),
                     "off_high_pct": off, "last": k["last"]})
    return rows, fun


def rank_skin(pre, insiders):
    """Gate 6 — the signature insider gate: at least 2 distinct
    open-market Form 4 buyers in the trailing 12 months, from the EDGAR
    insider cache. A name with no readable cache record fails the gate
    for that run (unverified never passes). Survivors are ranked by
    insider conviction — distinct buyers, then dollars bought — then by
    cheapest EV/EBITDA."""
    out = []
    for r in pre:
        ins = insiders.get(r["ticker"]) or {}
        if ins.get("buyers", 0) < 2:
            continue
        out.append({**r, "buyers": ins.get("buyers", 0),
                    "buy_val": ins.get("buy_val", 0)})
    out.sort(key=lambda r: (-r["buyers"], -r["buy_val"],
                            r["ev_ebitda"], r["ticker"]))
    for i, r in enumerate(out, 1):
        r["rank"] = i
    return out


def skin_section(t, m, skin_meta):
    """The researched overlay as a stock-page section: the six-criterion
    Nike-test scorecard from the dated research edition. Display only —
    it never gates the tab's daily mechanical membership."""
    if not m:
        return ""
    hon = (f"<p><span class='chip na'>HONORARY</span> {m.get('honorary_note', '')}</p>"
           if m.get("honorary") else "")
    tmpl = ("<p><b>The template name.</b> The six criteria were "
            "reverse-engineered from the case for Nike itself; Nike appears "
            "on this tab only when it passes the same daily mechanical "
            "gates as every other name.</p>" if m.get("template") else "")
    verd = ""
    if m.get("verdict"):
        vcls = {"BUY": "sb", "HOLD": "h", "SELL": "r"}.get(m["verdict"], "na")
        verd = (f"<p><span class='chip {vcls}' style='font-size:13px'>"
                f"{m['verdict']}</span> <span class='muted'>our standing "
                f"research verdict · {m.get('verdict_date', '')}</span></p>"
                f"<p>{m.get('verdict_note', '')}</p>")
    return (f"<section><h2>🤝 Skin in the Game — researched scorecard</h2>"
            f"<p>Researched {skin_meta.get('researched', '')} — one of the "
            f"names scored by hand against the Nike-justification template: "
            f"famous durable brand, fallen hard to near-decade-cheapest "
            f"multiples, a company-specific trough, a balance sheet that "
            f"clearly survives, insiders buying the dip in a verified Form 4 "
            f"cluster, and a turnaround underway.</p>{tmpl}{verd}{hon}"
            f"<div class='tablewrap'><table><thead><tr><th>Criterion</th><th>Score</th>"
            f"<th>Evidence</th></tr></thead><tbody>{skin_scorecard_rows(m)}</tbody>"
            f"</table></div>"
            f"<p style='margin-top:10px'><b>What's broken:</b> "
            f"{m.get('whats_broken', '')}</p>"
            f"<p><b>Insider detail (SEC Form 4, trailing 12 months at the "
            f"edition date):</b> {m.get('insider_summary', '')}</p>"
            f"<p><b>Turnaround &amp; next test:</b> "
            f"{m.get('turnaround_test', '')}</p>"
            f"<p class='muted'>Scores are the dated research edition "
            f"({skin_meta.get('edition', '')}); the tab's membership is set "
            f"by the daily mechanical screen, not by this overlay.</p></section>")


def skin_screen_section(t, row, ins):
    """The mechanical-qualifier section for a stock page (mirrors the
    Fallen & Cheap screener_section)."""
    if not row:
        return ""
    ins_line = ""
    if ins:
        ins_line = (f"<p>Insider record (SEC Form 4, last 12 months): "
                    f"<b>{ins.get('buyers', 0)} open-market buyer(s), "
                    f"${ins.get('buy_val', 0):,}</b>"
                    + (f", most recent {ins.get('last_buy')}" if ins.get("last_buy") else "")
                    + f"; {ins.get('sellers', 0)} seller(s), ${ins.get('sell_val', 0):,}.</p>")
    return (f"<section><h2>🤝 Skin in the Game — daily screen</h2>"
            f"<p>Qualifies at #{row['rank']} on the daily re-screen: "
            f"<b>{abs(row['off_high_pct']):.1f}% below its 52-week high</b>, "
            f"EV/EBITDA <b>{row['ev_ebitda']:.1f}×</b> vs {row['ev_ebitda_1y']:.1f}× "
            f"a year ago, net debt/EBITDA {row['nd_ebitda']:.1f}×, free cash "
            f"flow ${row['fcf'] / 1e6:,.0f}M — and the signature gate: "
            f"<b>{row.get('buyers', 0)} distinct open-market insider buyers, "
            f"${row.get('buy_val', 0):,}</b> bought in the last 12 months.</p>"
            f"{ins_line}"
            f"<p class='muted'>Membership is mechanical and re-screened "
            f"daily (fallen + cheap, compressing multiple + survivability + "
            f"a verified insider cluster). Brand and turnaround judgments "
            f"are not part of the screen — where a hand-researched scorecard "
            f"exists it appears below as a separate, dated layer.</p></section>")


def build_skin_page(meta, rows, skin, insiders, verdicts, tverdicts,
                    kpi_ix=None, skin_pages=None, n_pre=0):
    kpi_ix = kpi_ix or {}
    skin_pages = skin_pages or set()
    smeta = skin.get("meta") or {}
    overlay = {m["ticker"]: m for m in skin.get("members") or []}
    total_buyers = sum(r.get("buyers", 0) for r in rows)
    trs = []
    for r in rows:
        t = r["ticker"]
        ov = overlay.get(t)
        k = kpi_ix.get(t, {})
        last = k.get("last", r["last"])
        off = k.get("off_high_pct", r["off_high_pct"])
        rp = k.get("range_pos")
        range_html = (f"<span class='range'><span class='fill' style='width:{rp}%'></span></span> "
                      if rp is not None else "")
        day = k.get("day_pct")
        day_html = (f"<span class='{'up' if day >= 0 else 'dn'}'>{day:+.2f}%</span>"
                    if day is not None else '<span class="muted">—</span>')
        chips = _verdict_chip(t, verdicts, tverdicts)
        if ov and ov.get("verdict") and not chips:
            vcls = {"BUY": "sb", "HOLD": "h", "SELL": "r"}.get(ov["verdict"], "na")
            chips += (f" <span class='chip {vcls}' title='Researched verdict "
                      f"({ov.get('verdict_date', '')}) — see the panel'>"
                      f"{ov['verdict']}</span>")
        if ov and ov.get("template"):
            chips += (" <span class='chip na' title='The template name — "
                      "the six criteria were reverse-engineered from this "
                      "stock'>TEMPLATE</span>")
        if ov and ov.get("honorary"):
            chips += " <span class='chip na'>HONORARY</span>"
        sym = (f"<a class='sym' href='stock/{t}.html'>{t}</a>" if t in skin_pages
               else f"<span class='sym'>{t}</span>")
        ins = insiders.get(t)
        ins_det = ""
        if ins:
            ins_det = (f"<h3>Insider record — SEC Form 4, last 12 months</h3>"
                       f"<p><b>{ins.get('buyers', 0)} open-market buyer(s), "
                       f"${ins.get('buy_val', 0):,}</b>"
                       + (f", most recent {ins.get('last_buy')}"
                          if ins.get("last_buy") else "")
                       + f"; {ins.get('sellers', 0)} seller(s), "
                       f"${ins.get('sell_val', 0):,}.</p>")
        why = (f"{abs(off):.0f}% off its 52-week high, EV/EBITDA compressed "
               f"from {r['ev_ebitda_1y']:.1f}× to {r['ev_ebitda']:.1f}× in a "
               f"year, net debt/EBITDA {r['nd_ebitda']:.1f}×, FCF "
               f"${r['fcf'] / 1e6:,.0f}M — and {r.get('buyers', 0)} distinct "
               f"insiders bought ${r.get('buy_val', 0):,} of it in the last "
               f"12 months.")
        v = _skin_verdict(t, verdicts, tverdicts)
        vblock = ""
        if v:
            vblock = verdict_detail_html(
                v, t, page=t if t in skin_pages else None)
        elif ov and ov.get("verdict"):
            vcls = {"BUY": "sb", "HOLD": "h", "SELL": "r"}.get(ov["verdict"], "na")
            vblock = (f"<p><span class='chip {vcls}' style='font-size:13px'>"
                      f"{ov['verdict']}</span> <span class='muted'>our "
                      f"standing research verdict · "
                      f"{ov.get('verdict_date', '')}</span></p>"
                      f"<p>{ov.get('verdict_note', '')}</p>")
        oblock = ""
        if ov:
            hon_det = (f"<p><span class='chip na'>HONORARY</span> "
                       f"{ov.get('honorary_note', '')}</p>"
                       if ov.get("honorary") else "")
            tmpl_det = ("<p><b>The template name.</b> The six criteria were "
                        "reverse-engineered from the case for Nike itself; it "
                        "is listed only because it passes the same daily "
                        "gates as every other member.</p>"
                        if ov.get("template") else "")
            oblock = (f"<h3>Researched {smeta.get('researched', '')} — "
                      f"the Nike test</h3>"
                      + tmpl_det + hon_det
                      + f"<table><tbody>{skin_scorecard_rows(ov)}</tbody></table>"
                      + f"<h3>Insider detail — at the research date</h3>"
                      f"<p>{ov.get('insider_summary', '')}</p>"
                      f"<h3>What's broken</h3><p>{ov.get('whats_broken', '')}</p>"
                      f"<h3>Turnaround &amp; next test</h3>"
                      f"<p>{ov.get('turnaround_test', '')}</p>")
        det = (pkpi_grid([
            (f"${last:,.2f}", "Price"), (day_html, "Day"),
            (f"{off:.1f}%", "vs 52w high"),
            (f"{r['ev_ebitda']:.1f}×", "EV/EBITDA now"),
            (f"{r['ev_ebitda_1y']:.1f}×", "EV/EBITDA 1y ago"),
            (f"{r['nd_ebitda']:.1f}×", "Net debt/EBITDA"),
            (f"${r['fcf'] / 1e6:,.0f}M", "Free cash flow")])
            + f"<h3>Why it's here</h3><p>{why}</p>{ins_det}"
            + vblock + oblock
            + (f"<a class='pfull' href='stock/{t}.html'>Full stock page →</a>"
               if t in skin_pages and not v else ""))
        test_label = ((ov.get("test_label") if ov else None)
                      or k.get("earnings") or "—")
        trs.append(
            f"<tr data-q='{t} {clean_company_name(UNIVERSE_NAME.get(t, t))}'><td>{r['rank']}</td>"
            f"<td>{sym}<span class='nm'>{clean_company_name(UNIVERSE_NAME.get(t, t))}</span>"
            f"{chips}</td>"
            f"<td class='num'>${last:,.2f}</td>"
            f"<td>{range_html}<span class='dn'>{off:.1f}%</span></td>"
            f"<td class='num'><b>{r['ev_ebitda']:.1f}×</b></td>"
            f"<td>{_insider_html(ins)}</td>"
            f"<td class='num'>{test_label}</td>"
            f"<td class='detail'>{det}</td></tr>")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Skin in the Game · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_SKIN}
<main class="main">
<h1>🤝 Skin in the Game</h1>
<div class="sub">A daily mechanical re-screen for the Nike pattern — fallen hard, cheap on a
compressing multiple, a balance sheet that survives, and insiders buying the dip with their
own money · prices as of {meta['asof']} close. <b>Select any row</b> — its evidence opens in
the panel. Where a member has been hand-researched, its six-criterion Nike-test scorecard
appears in the panel under the research date; brand and turnaround judgments live only in
that research layer and never gate membership.</div>
<div class="cards">
  <div class="card"><div class="n">{len(rows)}</div><div class="l">Qualifiers shown (Top 20)</div></div>
  <div class="card"><div class="n">{total_buyers}</div><div class="l">Distinct insider buyers across the list (12mo)</div></div>
  <div class="card"><div class="n">{n_pre}</div><div class="l">Passed the price &amp; fundamental gates</div></div>
  <div class="card"><div class="n">{meta['asof']}</div><div class="l">Prices as of</div></div>
</div>
<p class="sub">The gates, applied to the same 1,654-name universe and re-run every morning:
<b>at least 35% below the 52-week high</b> · <b>EV/EBITDA ≤ 15× and lower than a year ago</b>
(SEC EDGAR basis, the same figures as the Fallen &amp; Cheap tab) · <b>net debt/EBITDA ≤ 3×</b> ·
<b>positive free cash flow</b> · price ≥ $3, ADV ≥ $5M · and the hard gate — <b>an insider
buying cluster: at least 2 distinct open-market buyers in SEC Form 4 filings over the last
12 months</b>. Ranking is insider conviction first (distinct buyers, then dollars bought),
then cheapest multiple. Membership changes as the numbers do: a researched name that stops
qualifying leaves the tab, and a new qualifier enters without waiting for a research pass.</p>
<div class="controls"><input id="q" type="text" placeholder="Search symbol or company…"></div>
<div class="master">
<div class="tablewrap"><table id="t" class="mlist"><thead><tr><th>#</th><th>Stock</th>
<th class="num">Price</th><th>vs 52w high</th><th class="num">EV/EBITDA</th>
<th>Insider buying (12mo)</th><th class="num">Next test</th></tr></thead>
<tbody>{chr(10).join(trs)}</tbody></table></div>
{panel_html()}
</div>
<p class="foot">Method and limits: EV/EBITDA uses EDGAR annual filings (operating income +
D&amp;A as the EBITDA proxy, rebuilt from net income + interest + tax + D&amp;A where
operating income is not tagged) — a consistent basis for the now-vs-year-ago comparison, not
identical to vendor trailing-twelve-month figures. Insider records come from each company's
own Form 4 filings, aggregated in a weekly cache (data/insider_cache.json); a name whose
filings cannot be read fails the insider gate for that run rather than passing unverified.
The screen cannot see brand strength, guidance quality or whether a turnaround is real —
those judgments exist only in the dated research overlay ({smeta.get('edition', '')}) shown
in the panels where present, and the overlay never admits or removes a member. Micro-caps
outside the universe (top-800 by volume ∪ S&amp;P 1000) cannot appear here at all. Structured
research output, not investment advice or a trade instruction.</p>
{glossary_html()}
</main></div>
{PANEL_JS}</body></html>"""
    with open(os.path.join(ROOT, "skin.html"), "w") as fh:
        fh.write(html)
    return rows


def build_skin_details(meta, rows, skin, insiders, verdicts, tverdicts,
                       results, pro_recs, earn):
    """Detail pages for Skin in the Game qualifiers covered by no other
    list builder."""
    fortune = json.load(open(os.path.join(DATA, "fortune500.json")))
    asof = meta["asof"]
    smeta = skin.get("meta") or {}
    overlay = {m["ticker"]: m for m in skin.get("members") or []}
    for r in rows:
        t = r["ticker"]
        if os.path.exists(os.path.join(ROOT, "stock", f"{t}.html")):
            continue
        k = results.get(t)
        if not k:
            continue
        secs = [skin_screen_section(t, r, insiders.get(t))]
        ov = overlay.get(t)
        if ov:
            secs.append(skin_section(t, ov, smeta))
        prec = pro_recs.get(t)
        psec = pro_section(t, prec, k, None)
        if psec:
            secs.append(psec)
        vsec = verdict_section(t, verdicts) or verdict_section(t, tverdicts)
        if vsec:
            secs.append(vsec)
        pe = (round(100 / prec["earn_y"], 1)
              if prec and prec.get("earn_y") and prec["earn_y"] > 0 else None)
        tiles = kpi_tiles(k, rs20=k["mom20_pct"] - meta.get("spy_mom20", 0.0),
                          pe=pe, earn=earn.get(t), extra=(
            _tile(f"{k['vpt20']:+.0f}%", "VPT trend (20d)",
                  "up" if k["vpt20"] >= 0 else "dn")))
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("skin", "../")}
<main class="main">
<p class="crumb"><a href="../skin.html">← Back to Skin in the Game</a></p>
<h1>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Skin in the Game — daily screen #{r['rank']}</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
{tiles}
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a></p>
<p class="muted">Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)


def load_buzz():
    """The Most Discussed store (data/buzz.json) — a dated, hand-measured
    research edition like the verdict stores. The pipeline LOADS it and
    never re-measures or overwrites it; a new edition is a research task,
    not part of the daily refresh."""
    p = os.path.join(DATA, "buzz.json")
    if os.path.exists(p):
        try:
            return json.load(open(p))
        except Exception:
            pass
    return {"meta": {}, "entries": []}


def _buzz_mix(counts):
    labels = (("analyst", "analyst"), ("features", "feature"),
              ("trending", "trending"), ("social", "social"))
    return " · ".join(f"{counts.get(k, 0)} {lab}" for k, lab in labels)


def _buzz_breakdown(e):
    c = e.get("counts") or {}
    terms = []
    if c.get("analyst"):
        terms.append(f"{c['analyst']} analyst action{'s' if c['analyst'] != 1 else ''} × 3")
    if c.get("features"):
        terms.append(f"{c['features']} feature{'s' if c['features'] != 1 else ''} × 2")
    if c.get("trending"):
        terms.append(f"{c['trending']} trending list{'s' if c['trending'] != 1 else ''} × 1")
    if c.get("social"):
        terms.append(f"{c['social']} social × 1")
    return " + ".join(terms) if terms else "—"


def _buzz_sources_html(e):
    items = "".join(
        f"<li><b>{s.get('outlet', '')}</b> <span class='muted'>"
        f"{s.get('type', '')} · {s.get('date', '')}</span><br>{s.get('gist', '')}</li>"
        for s in e.get("sources") or [])
    return f"<ul class='srcs'>{items}</ul>"


def buzz_section(t, e, bmeta):
    """The Most Discussed evidence as a stock-page section: the crowd's
    pitch, the score breakdown and the dated source list."""
    if not e:
        return ""
    att = ("<p><span class='chip na'>ATTENTION ONLY</span> Listed on "
           "trending lists, but no dated analyst action or feature pitch "
           "was verified in the window.</p>" if e.get("attention_only") else "")
    return (f"<section><h2>📣 Most Discussed — {bmeta.get('window', '')} edition</h2>"
            f"<p>Buzz score <b>{e.get('score', '')}</b> "
            f"({sum((e.get('counts') or {}).values())} distinct sources: "
            f"{_buzz_mix(e.get('counts') or {})}).</p>"
            f"<p><b>The crowd's pitch:</b> {e.get('thesis', '')}</p>{att}"
            f"<p><b>Score breakdown:</b> {_buzz_breakdown(e)} = {e.get('score', '')}</p>"
            f"<p style='margin-top:10px'><b>Sources behind the buzz:</b></p>"
            f"{_buzz_sources_html(e)}"
            f"<p class='muted'>An attention measure from the dated edition "
            f"of {bmeta.get('edition', '')} — who was pitching the stock, "
            f"not whether the pitch is right. The daily refresh never "
            f"re-measures it.</p></section>")


def _also_on_map():
    """Which of the other tabs carry each ticker, computed from the stock
    links on the built tab pages (buzz is built last, so every other page
    on disk is this run's). Returns {ticker: [short tab labels]}."""
    pages = [("index.html", "Quality Dips"), ("pro.html", "Factor Grades"),
             ("sectors.html", "Sector Impact"), ("validated.html", "Vetted Buys"),
             ("macro.html", "Theme Buys"), ("holds.html", "The Bench"),
             ("screener.html", "Fallen & Cheap"), ("skin.html", "Skin in the Game")]
    out = {}
    for fn, label in pages:
        p = os.path.join(ROOT, fn)
        if not os.path.exists(p):
            continue
        for t in set(re.findall(r"stock/([A-Z0-9][A-Z0-9.\-]*)\.html",
                                open(p).read())):
            out.setdefault(t, []).append(label)
    return out


def build_buzz_page(meta, entries, bmeta, verdicts, tverdicts, kpi_ix=None,
                    also_on=None, buzz_pages=None):
    kpi_ix = kpi_ix or {}
    also_on = also_on or {}
    buzz_pages = buzz_pages or set()
    n_also = sum(1 for e in entries if also_on.get(e["ticker"]))
    trs = []
    for e in entries:
        t = e["ticker"]
        counts = e.get("counts") or {}
        nsrc = sum(counts.values())
        k = kpi_ix.get(t, {})
        if k:
            price_html = f"${k['last']:,.2f}"
            off_html = f"{k['off_high_pct']:.1f}%"
            price_v, off_v = price_html, off_html
        elif e.get("close"):
            price_v = f"${e['close']:,.2f}"
            price_html = (f"${e['close']:,.2f} <span class='muted' title='"
                          f"{e.get('close_basis', 'research close')}'>*</span>")
            off_v = "— (outside universe)"
        else:
            price_v, price_html, off_v = "—", "—", "— (outside universe)"
        chips = _verdict_chip(t, verdicts, tverdicts)
        if e.get("attention_only"):
            chips += (" <span class='chip na' title='Trending-list attention "
                      "only — no dated analyst action or feature pitch was "
                      "verified in the window'>ATTENTION ONLY</span>")
        sym = (f"<a class='sym' href='stock/{t}.html'>{t}</a>" if t in buzz_pages
               else f"<span class='sym'>{t}</span>")
        also = also_on.get(t) or []
        also_html = (" ".join(f"<span class='chip na'>{x}</span>" for x in also)
                     if also else '<span class="muted">—</span>')
        v = _skin_verdict(t, verdicts, tverdicts)
        vblock = ""
        if v:
            vblock = verdict_detail_html(v, t, page=t if t in buzz_pages else None)
        att_det = ("<p><span class='chip na'>ATTENTION ONLY</span> This name "
                   "is here on trending-list placement alone — no dated "
                   "analyst action or feature pitch was verified in the "
                   "window.</p>" if e.get("attention_only") else "")
        det = (pkpi_grid([
            (str(e.get("score", "")), "Buzz score"),
            (str(nsrc), "Distinct sources"),
            (price_v, "Price"),
            (off_v, "vs 52w high")])
            + f"<h3>The crowd's pitch</h3><p>{e.get('thesis', '')}</p>{att_det}"
            + f"<h3>Score breakdown</h3><p>{_buzz_breakdown(e)} = "
              f"<b>{e.get('score', '')}</b></p>"
            + f"<h3>Sources — {bmeta.get('window', '')}</h3>"
            + _buzz_sources_html(e)
            + (f"<h3>Also on</h3><p>{also_html}</p>" if also else "")
            + vblock
            + (f"<a class='pfull' href='stock/{t}.html'>Full stock page →</a>"
               if t in buzz_pages and not v else ""))
        trs.append(
            f"<tr data-q='{t} {clean_company_name(e.get('company', t))}'><td>{e.get('rank', '')}</td>"
            f"<td>{sym}<span class='nm'>{clean_company_name(e.get('company', t))}</span>"
            f"{chips}</td>"
            f"<td class='num'><b>{e.get('score', '')}</b></td>"
            f"<td>{_buzz_mix(counts)}</td>"
            f"<td>{also_html}</td>"
            f"<td class='num'>{price_html}</td>"
            f"<td class='detail'>{det}</td></tr>")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Most Discussed · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_BUZZ}
<main class="main">
<h1>📣 Most Discussed</h1>
<div class="sub">The attention lens: stocks the financial world was actively pitching
<b>as an opportunity</b> in {bmeta.get('window', '')} — upgrades, target raises, "best stocks
to buy" features and trending lists, hand-verified source by source. This measures
<b>who is talking, not who is right</b>: it is not a quality verdict, and several names here
fail this site's research tests outright. <b>Select any row</b> — its sources open in the
panel. Edition of {bmeta.get('edition', '')}; prices as of {meta['asof']} close where the
name sits in the watchlist universe.</div>
<div class="cards">
  <div class="card"><div class="n">{len(entries)}</div><div class="l">Names clearing the evidence bar (≥2 distinct sources)</div></div>
  <div class="card"><div class="n">{bmeta.get('sources_counted', '')}</div><div class="l">Distinct opportunity sources counted</div></div>
  <div class="card"><div class="n">{n_also}</div><div class="l">Also carried on another tab</div></div>
  <div class="card"><div class="n">{bmeta.get('window', '')}</div><div class="l">Discussion window</div></div>
</div>
<p class="sub">Scoring, applied uniformly: a verified <b>analyst action</b> (upgrade, target
raise, bullish initiation) = <b>3</b>; a major-outlet <b>opportunity feature</b> or
fund-manager pitch = <b>2</b>; a <b>trending-list</b> appearance = <b>1</b>; verified bullish
<b>social</b> discussion = <b>1</b> per platform, capped at 2. Ranked by score, then by
distinct-source count. Mere mentions, earnings news without an opportunity pitch, and
bearish coverage never count.</p>
<div class="controls"><input id="q" type="text" placeholder="Search symbol or company…"></div>
<div class="master">
<div class="tablewrap"><table id="t" class="mlist"><thead><tr><th>#</th><th>Stock</th>
<th class="num">Buzz</th><th>Source mix</th><th>Also on</th>
<th class="num">Price</th></tr></thead>
<tbody>{chr(10).join(trs)}</tbody></table></div>
{panel_html()}
</div>
<p class="foot">Method and limits: {bmeta.get('methodology', '')} Sources were read
Oct 27, 2026; Barron's and some aggregator items were visible only as snippets, and
some widely-reported actions were excluded when verification dated them outside
the window (the Micron bank raises, for instance, proved to be dated June 25 and
September 23, and its Zacks growth-screen pick is what is scored here). Prices and % vs 52-week high come
from the watchlist pool for names in the 1,654-name universe; names outside it show a
research close (*) or no price. Buzz says nothing about valuation, balance sheet or
whether the crowd's thesis is sound — the other eight lenses exist for that. Structured
research output, not investment advice or a trade instruction.</p>
{glossary_html()}
</main></div>
{PANEL_JS}</body></html>"""
    with open(os.path.join(ROOT, "buzz.html"), "w") as fh:
        fh.write(html)
    return entries


def build_buzz_details(meta, entries, bmeta, verdicts, tverdicts,
                       results, pro_recs, earn):
    """Detail pages for Most Discussed names covered by no other list
    builder. Names inside the universe get the full page (chart + KPI
    band); names outside it get a compact page carrying the buzz
    evidence, so every listed name resolves."""
    fortune = json.load(open(os.path.join(DATA, "fortune500.json")))
    asof = meta["asof"]
    for e in entries:
        t = e["ticker"]
        if os.path.exists(os.path.join(ROOT, "stock", f"{t}.html")):
            continue
        name = clean_company_name(e.get("company", t))
        bsec = buzz_section(t, e, bmeta)
        ys = yahoo_symbol(t)
        if t in results:
            k = results[t]
            secs = [bsec]
            prec = pro_recs.get(t)
            psec = pro_section(t, prec, k, None)
            if psec:
                secs.append(psec)
            vsec = verdict_section(t, verdicts) or verdict_section(t, tverdicts)
            if vsec:
                secs.append(vsec)
            pe = (round(100 / prec["earn_y"], 1)
                  if prec and prec.get("earn_y") and prec["earn_y"] > 0 else None)
            tiles = kpi_tiles(k, rs20=k["mom20_pct"] - meta.get("spy_mom20", 0.0),
                              pe=pe, earn=earn.get(t), extra=(
                _tile(f"{k['vpt20']:+.0f}%", "VPT trend (20d)",
                      "up" if k["vpt20"] >= 0 else "dn")))
            page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {name} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("buzz", "../")}
<main class="main">
<p class="crumb"><a href="../buzz.html">← Back to Most Discussed</a></p>
<h1>{t} — {name}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Most Discussed #{e.get('rank', '')} — {bmeta.get('window', '')} edition</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
{tiles}
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a></p>
<p class="muted">Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        else:
            close_line = (f"<div><span class='price'>${e['close']:,.2f}</span>"
                          f"<span class='muted'> · research close "
                          f"({e.get('close_basis', '')})</span></div>"
                          if e.get("close") else
                          "<p class='muted'>Outside the watchlist universe — "
                          "no pool price for this name.</p>")
            vsec = verdict_section(t, verdicts) or verdict_section(t, tverdicts)
            secs = [bsec] + ([vsec] if vsec else [])
            page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {name} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{tabs_html("buzz", "../")}
<main class="main">
<p class="crumb"><a href="../buzz.html">← Back to Most Discussed</a></p>
<h1>{t} — {name}</h1>
<div class="sub">Most Discussed #{e.get('rank', '')} — {bmeta.get('window', '')} edition
· outside the watchlist universe (top-800 by volume ∪ S&amp;P 1000)</div>
{close_line}
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a></p>
<p class="muted">Research and education only — not investment advice.</p></section>
</main></div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)


def compute_common(dip_rows, pro_rows, verdicts, tverdicts, ranked_holds,
                   scr_top, skin_rows, bentries):
    """Cross-tab overlap: which tickers the displayed lists carry, counted
    across the eight stock-carrying tabs (Sector Impact carries no stocks;
    Most Common never counts itself). Returns (rows, stats) where rows are
    the tickers on >= 3 tabs, sorted by tab count desc, then ticker."""
    tab_lists = [
        ("Quality Dips", "index.html",
         {r["ticker"]: f"#{r['rank']}" for r in dip_rows}),
        ("Factor Grades", "pro.html",
         {r["ticker"]: f"#{r['rank']}" for r in pro_rows}),
        ("Vetted Buys", "validated.html",
         {t: f"#{i}" for i, t in enumerate(verdicts.get("final50") or [], 1)}),
        ("Theme Buys", "macro.html",
         {t: f"#{i}" for i, t in enumerate(tverdicts.get("list") or [], 1)}),
        ("The Bench", "holds.html",
         {r["ticker"]: f"#{i}" for i, r in enumerate(ranked_holds[:50], 1)}),
        ("Fallen & Cheap", "screener.html",
         {r["ticker"]: f"#{i}" for i, r in enumerate(scr_top, 1)}),
        ("Skin in the Game", "skin.html",
         {r["ticker"]: f"#{i}" for i, r in enumerate(skin_rows, 1)}),
        ("Most Discussed", "buzz.html",
         {e["ticker"]: f"#{e.get('rank', '')} · score {e.get('score', '')}"
          for e in bentries}),
    ]
    seen = {}
    slots = 0
    for label, href, members in tab_lists:
        slots += len(members)
        for t, note in members.items():
            seen.setdefault(t, []).append(
                {"label": label, "href": href, "note": note})
    rows = [{"ticker": t, "tabs": tabs, "count": len(tabs)}
            for t, tabs in seen.items() if len(tabs) >= 3]
    rows.sort(key=lambda r: (-r["count"], r["ticker"]))
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    stats = {"slots": slots, "unique": len(seen),
             "max_tabs": max((len(v) for v in seen.values()), default=0)}
    return rows, stats


def build_common_page(meta, common, stats, kpi_ix, verdicts, tverdicts):
    kpi_ix = kpi_ix or {}
    trs = []
    for r in common:
        t = r["ticker"]
        k = kpi_ix.get(t, {})
        price_html = f"${k['last']:,.2f}" if k else "—"
        off_html = f"{k['off_high_pct']:.1f}%" if k else "—"
        price_v = price_html
        off_v = off_html
        earn_v = k.get("earnings") or "—"
        chips = _verdict_chip(t, verdicts, tverdicts)
        has_page = os.path.exists(os.path.join(ROOT, "stock", f"{t}.html"))
        sym = (f"<a class='sym' href='stock/{t}.html'>{t}</a>" if has_page
               else f"<span class='sym'>{t}</span>")
        tabs_html_cell = " ".join(
            f"<span class='chip na'>{x['label']}</span>" for x in r["tabs"])
        where = "".join(
            f"<p>· <a href='{x['href']}'>{x['label']}</a> — {x['note']}</p>"
            for x in r["tabs"])
        v = _skin_verdict(t, verdicts, tverdicts)
        vblock = (verdict_detail_html(v, t, page=t if has_page else None)
                  if v else "")
        det = (pkpi_grid([(str(r["count"]), "Tabs"),
                          (price_v, "Price"),
                          (off_v, "vs 52w high"),
                          (earn_v, "Earnings")])
               + f"<h3>Where it appears</h3>{where}"
               + vblock
               + (f"<a class='pfull' href='stock/{t}.html'>Full stock page →</a>"
                  if has_page and not v else ""))
        trs.append(
            f"<tr data-q='{t} {clean_company_name(UNIVERSE_NAME.get(t, t))}'>"
            f"<td>{r['rank']}</td>"
            f"<td>{sym}<span class='nm'>"
            f"{clean_company_name(UNIVERSE_NAME.get(t, t))}</span>{chips}</td>"
            f"<td class='num'><b>{r['count']}</b></td>"
            f"<td>{tabs_html_cell}</td>"
            f"<td class='num'>{price_html}</td>"
            f"<td class='num'>{off_html}</td>"
            f"<td class='detail'>{det}</td></tr>")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Most Common · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="shell">
{TABS_COMMON}
<main class="main">
<h1>🔁 Most Common</h1>
<div class="sub">The overlap lens: stocks carried by <b>three or more</b> of this site's
stock lists at once — a dip screen, a factor model, a dated research verdict, an insider
gate, the crowd. Membership is recomputed on every refresh from the displayed lists
themselves (the Top 50s, Skin's qualifiers, Most Discussed's edition); this tab counts
the other lists and never counts itself. <b>Select any row</b> — where it appears opens
in the panel. Prices as of {meta['asof']} close.</div>
<div class="cards">
  <div class="card"><div class="n">{len(common)}</div><div class="l">Names on 3 or more tabs</div></div>
  <div class="card"><div class="n">{stats['max_tabs']}</div><div class="l">Most tabs for any one name</div></div>
  <div class="card"><div class="n">{stats['unique']}</div><div class="l">Unique stocks across all stock lists</div></div>
  <div class="card"><div class="n">{stats['slots']}</div><div class="l">Total list slots counted</div></div>
</div>
<div class="controls"><input id="q" type="text" placeholder="Search symbol or company…"></div>
<div class="master">
<div class="tablewrap"><table id="t" class="mlist"><thead><tr><th>#</th><th>Stock</th>
<th class="num">Tabs</th><th>Appears on</th>
<th class="num">Price</th><th class="num">vs 52w high</th></tr></thead>
<tbody>{chr(10).join(trs)}</tbody></table></div>
{panel_html()}
</div>
<p class="foot">Method and limits: a name counts once per list it appears on, across
Quality Dips, Factor Grades, Vetted Buys, Theme Buys, The Bench (displayed Top 50),
Fallen &amp; Cheap (displayed Top 50), Skin in the Game and Most Discussed, and is
listed here at 3 or more. Ranked by tab count, then ticker. Overlap is not independent
confirmation in every case: Theme Buys was swept from vetted BUY verdicts, so a name on
both Vetted Buys and Theme Buys has cleared one research process, not two — the panel
shows exactly which lists each name sits on so the mix is visible. The mechanical lists
re-rank daily, so membership moves with them. Structured research output, not
investment advice or a trade instruction.</p>
{glossary_html()}
</main></div>
{PANEL_JS}</body></html>"""
    with open(os.path.join(ROOT, "common.html"), "w") as fh:
        fh.write(html)
    return common


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--skip-pe", action="store_true")
    args = ap.parse_args()
    t0 = time.time()
    universe = load_universe()
    global UNIVERSE_NAME
    UNIVERSE_NAME = universe
    tickers = sorted(universe)
    print(f"Universe: {len(tickers)} common stocks (top-800 volume ∪ S&P 1000)", flush=True)

    results = {}
    fails = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(fetch_bars_retry, t): t for t in tickers}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                results[t] = kpis_from_bars(fut.result())
            except Exception:
                fails += 1
    print(f"Bars: {len(results)} ok, {fails} failed ({time.time() - t0:.0f}s)", flush=True)

    cands = {t: k for t, k in results.items() if is_candidate(k) and t not in EXCLUDE}
    for k in cands.values():
        k["score"] = dip_score(k)
    print(f"Candidates: {len(cands)}", flush=True)

    earn = earnings_dates()
    spy_mom20 = 0.0
    spy_closes = []
    try:
        spy_bars = fetch_bars_retry("SPY")
        spy_closes = [b.close for b in spy_bars]
        spy_mom20 = (spy_bars[-1].close / spy_bars[-21].close - 1) * 100
    except Exception:
        pass
    selected = [t for t in PINNED if t in cands]
    rest = sorted((t for t in cands if t not in selected),
                  key=lambda t: cands[t]["score"], reverse=True)
    selected += rest[: max(0, 50 - len(selected))]
    selected = selected[:50]

    prev_ranks = {}
    prev_path = os.path.join(DATA, "watchlist.json")
    if os.path.exists(prev_path):
        try:
            for row in json.load(open(prev_path))["rows"]:
                prev_ranks[row["ticker"]] = row["rank"]
        except Exception:
            pass

    rows = []
    for i, t in enumerate(selected, 1):
        k = dict(results[t])
        k.pop("score", None)
        rows.append({
            "rank": i, "ticker": t, "company": universe.get(t, t),
            "earnings": earn.get(t, ""), "pe": None, "roic": None,
            "rs20": round(k["mom20_pct"] - spy_mom20, 1),
            "prev_rank": prev_ranks.get(t),
            **{kk: k[kk] for kk in ("last", "day_pct", "high52", "low52",
                                    "off_high_pct", "range_pos", "rsi14",
                                    "mom20_pct", "perf1y_pct", "vs_sma200_pct",
                                    "adv_m", "vol10", "vol30", "vpt20",
                                    "spark", "chart", "asof")},
        })

    if not args.skip_pe:
        for r in rows:
            if r["ticker"] != "BKNG":
                # BKNG skipped for P/E only: Apr 2026 25:1 split corrupts its
                # filing EPS series. ROIC uses levels, not per-share data.
                r["pe"] = edgar_pe(r["ticker"], r["last"])
                time.sleep(0.11)
            r["roic"] = edgar_roic(r["ticker"])
            time.sleep(0.11)

    asof = max(r["asof"] for r in rows)
    meta = {"asof": asof, "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "universe_n": len(tickers), "screened": len(results),
            "candidates": len(cands), "spy_mom20": spy_mom20}
    json.dump({"meta": meta, "rows": rows}, open(prev_path, "w"), indent=1)

    print("Building the professional lens (EDGAR frames + surprise history)…", flush=True)
    pro_top, pro_recs = build_professional(results, spy_closes)
    prev_pro_ranks = {}
    pro_path = os.path.join(DATA, "professional.json")
    if os.path.exists(pro_path):
        try:
            for row in json.load(open(pro_path))["rows"]:
                prev_pro_ranks[row["ticker"]] = row["rank"]
        except Exception:
            pass
    pro_rows = []
    for i, rec in enumerate(pro_top, 1):
        t = rec["ticker"]
        k = results[t]
        pro_rows.append({**rec, "rank": i, "company": universe.get(t, t),
                         "last": k["last"], "day_pct": k["day_pct"],
                         "earnings": earn.get(t, ""),
                         "pe": (round(100 / rec["earn_y"], 1)
                                if rec["earn_y"] and rec["earn_y"] > 0 else None),
                         "_kpi": {kk: k[kk] for kk in ("last", "day_pct", "high52",
                                                       "low52", "off_high_pct",
                                                       "range_pos", "rsi14",
                                                       "mom20_pct", "vpt20",
                                                       "perf1y_pct", "vol30",
                                                       "chart")}})
    n_vetoed = sum(1 for r in pro_recs.values() if r.get("veto"))
    n_pool = sum(1 for r in pro_recs.values() if r.get("composite") is not None)
    json.dump({"meta": meta,
               "rows": [{kk: v for kk, v in r.items() if kk != "_kpi"} for r in pro_rows]},
              open(pro_path, "w"), indent=1)
    # Persist the full candidate pools: the research-validation bench.
    dip_pool = [{"ticker": t, "rank": i + 1, "score": round(k["score"], 1),
                 "last": k["last"], "off_high_pct": k["off_high_pct"]}
                for i, (t, k) in enumerate(sorted(
                    cands.items(), key=lambda kv: kv[1]["score"], reverse=True))]
    pro_pool = [{"ticker": r["ticker"], "rank": i + 1,
                 "composite": round(r["composite"], 1), "grade": r["grade"],
                 "last": results[r["ticker"]]["last"],
                 "off_high_pct": results[r["ticker"]]["off_high_pct"]}
                for i, r in enumerate(sorted(
                    (r for r in pro_recs.values()
                     if r.get("composite") is not None and not r.get("veto")),
                    key=lambda r: r["composite"], reverse=True))]
    json.dump({"meta": meta, "dip": dip_pool, "pro": pro_pool,
               "vetoed": sorted(t for t, r in pro_recs.items() if r.get("veto"))},
              open(os.path.join(DATA, "pools.json"), "w"), indent=1)
    verdicts = load_verdicts()
    tverdicts = load_theme_verdicts()
    themes = load_themes()
    meta["keep_extra"] = ([r["ticker"] for r in pro_rows]
                          + list(verdicts.get("final50") or [])
                          + list(tverdicts.get("list") or []))
    build_site(rows, prev_ranks, meta, pro=pro_recs,
               pro_ranks={r["ticker"]: r["rank"] for r in pro_rows})
    build_pro_page(pro_rows, prev_pro_ranks, meta, n_pool, n_vetoed)
    build_pro_details(pro_rows, {r["ticker"] for r in rows}, meta)
    build_sectors_page(meta)
    # Canonical KPI lookup for the research pages (Validated / Macro), so
    # every tab shows the same KPIs for the same stock on the same day.
    kpi_ix = {}
    for t, k in results.items():
        rec = pro_recs.get(t) or {}
        pe = (round(100 / rec["earn_y"], 1)
              if rec.get("earn_y") and rec["earn_y"] > 0 else None)
        kpi_ix[t] = {"last": k["last"], "day_pct": k["day_pct"],
                     "off_high_pct": k["off_high_pct"],
                     "range_pos": k["range_pos"], "rsi14": k["rsi14"],
                     "rs20": round(k["mom20_pct"] - spy_mom20, 1),
                     "earnings": earn.get(t, ""), "pe": pe}
    for r in rows:
        if r["ticker"] in kpi_ix and r.get("pe"):
            kpi_ix[r["ticker"]]["pe"] = r["pe"]
    build_validated_page(meta, verdicts, kpi_ix)
    build_validated_details(meta, verdicts,
                            {r["ticker"] for r in rows} | {r["ticker"] for r in pro_rows},
                            results, pro_recs, earn)
    build_macro_page(meta, tverdicts, themes, kpi_ix)
    build_theme_details(meta, tverdicts,
                        {r["ticker"] for r in rows} | {r["ticker"] for r in pro_rows},
                        results, pro_recs, earn)
    ranked_holds = build_holds_page(meta, verdicts, tverdicts, kpi_ix)
    build_holds_details(meta, ranked_holds[:50], verdicts, tverdicts,
                        {r["ticker"] for r in rows} | {r["ticker"] for r in pro_rows}
                        | set(verdicts.get("final50") or [])
                        | set(tverdicts.get("list") or []),
                        results, pro_recs, earn)
    print(f"HOLDS: {len(ranked_holds)} HOLD verdicts ranked; top 50 on holds.html; "
          f"top 5: {', '.join(r['ticker'] for r in ranked_holds[:5])}")
    scr_rows = rank_screener(results, pro_recs)
    scr_top = scr_rows[:50]
    skin_pre, skin_fun = skin_prepass(results, pro_recs)
    # The insider cache now serves two gates: the screener's Top 50
    # finalists (badge) and every Skin in the Game gate 1-5 passer
    # (hard gate 6), so populate it for the union.
    insiders = fetch_insider_clusters(
        sorted({r["ticker"] for r in scr_top}
               | {r["ticker"] for r in skin_pre}))
    build_screener_page(meta, scr_rows, insiders, verdicts, tverdicts, kpi_ix)
    build_screener_details(meta, scr_top, insiders, verdicts, tverdicts,
                           results, pro_recs, earn)
    print(f"SCREENER: {len(scr_rows)} passed gates; top 5: "
          f"{', '.join(r['ticker'] for r in scr_top[:5])}")
    skin = load_skin()
    skin_all = rank_skin(skin_pre, insiders)
    skin_rows = skin_all[:20]
    print(f"SKIN FUNNEL: pool {skin_fun['pool']} -> fallen {skin_fun['fallen']} "
          f"-> multiple {skin_fun['multiple']} -> leverage {skin_fun['leverage']} "
          f"-> fcf/price {skin_fun['fcf']} -> insider-gate {len(skin_all)}")
    skin_pages = {r["ticker"] for r in skin_rows
                  if r["ticker"] in results
                  or os.path.exists(os.path.join(
                      ROOT, "stock", f"{r['ticker']}.html"))}
    build_skin_page(meta, skin_rows, skin, insiders, verdicts, tverdicts,
                    kpi_ix, skin_pages, n_pre=skin_fun["fcf"])
    build_skin_details(meta, skin_rows, skin, insiders, verdicts, tverdicts,
                       results, pro_recs, earn)
    _mem = ", ".join(r["ticker"] for r in skin_rows)
    _extra = (f" (+{len(skin_all) - len(skin_rows)} more below the Top 20 cap)"
              if len(skin_all) > len(skin_rows) else "")
    print(f"SKIN: {len(skin_all)} qualifiers on skin.html; members: {_mem}{_extra}")

    buzz = load_buzz()
    bmeta = buzz.get("meta") or {}
    bentries = buzz.get("entries") or []
    build_buzz_details(meta, bentries, bmeta, verdicts, tverdicts,
                       results, pro_recs, earn)
    buzz_pages = {e["ticker"] for e in bentries
                  if os.path.exists(os.path.join(
                      ROOT, "stock", f"{e['ticker']}.html"))}
    build_buzz_page(meta, bentries, bmeta, verdicts, tverdicts, kpi_ix,
                    _also_on_map(), buzz_pages)
    print(f"BUZZ: {len(bentries)} names in edition "
          f"{bmeta.get('edition', '')} (window {bmeta.get('window', '')})")

    common, common_stats = compute_common(rows, pro_rows, verdicts, tverdicts,
                                          ranked_holds, scr_top, skin_rows,
                                          bentries)
    common_path = os.path.join(DATA, "common.json")
    prev_common = set()
    if os.path.exists(common_path):
        try:
            prev_common = {r["ticker"] for r in
                           json.load(open(common_path))["rows"]}
        except Exception:
            pass
    json.dump({"meta": meta,
               "rows": [{"rank": r["rank"], "ticker": r["ticker"],
                         "count": r["count"],
                         "tabs": [x["label"] for x in r["tabs"]]}
                        for r in common]},
              open(common_path, "w"), indent=1)
    build_common_page(meta, common, common_stats, kpi_ix, verdicts, tverdicts)
    print(f"COMMON: {len(common)} names in 3+ tabs; members: "
          f"{', '.join(r['ticker'] for r in common)}")
    _cnew = {r["ticker"] for r in common}
    print("COMMON ENTRANTS:", ", ".join(sorted(_cnew - prev_common)) or "none")
    print("COMMON EXITS:", ", ".join(sorted(prev_common - _cnew)) or "none")

    new_set = {r["ticker"] for r in rows}
    old_set = set(prev_ranks)
    print(f"Built site: index + {len(rows)} detail pages (as of {asof})")
    print("ENTRANTS:", ", ".join(sorted(new_set - old_set)) or "none")
    print("EXITS:", ", ".join(sorted(old_set - new_set)) or "none")
    movers = sorted(((abs(prev_ranks[r["ticker"]] - r["rank"]), r["ticker"],
                      prev_ranks[r["ticker"]] - r["rank"]) for r in rows
                     if r["ticker"] in prev_ranks and prev_ranks[r["ticker"]] != r["rank"]),
                    reverse=True)[:6]
    print("TOP MOVERS:", ", ".join(f"{t} {'+' if d > 0 else ''}{d}" for _, t, d in movers) or "none")
    wk = [f"{r['ticker']} {r['earnings']}" for r in rows if r["earnings"] and
          abs((datetime.strptime(r["earnings"], "%Y-%m-%d").date() - date.today()).days) <= 7]
    print("REPORTING ±7D:", ", ".join(wk) or "none")
    print(f"PRO POOL: {n_pool} graded, {n_vetoed} vetoed")
    print("PRO TOP5:", ", ".join(f"{r['ticker']}({r['grade']})" for r in pro_rows[:5]))
    pnew = {r["ticker"] for r in pro_rows}
    pold = set(prev_pro_ranks)
    print("PRO ENTRANTS:", ", ".join(sorted(pnew - pold)) or "none")
    print("PRO EXITS:", ", ".join(sorted(pold - pnew)) or "none")
    print(f"Done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
