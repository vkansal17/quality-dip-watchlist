#!/usr/bin/env python3
"""Quality-Dip Watchlist pipeline — single file, standard library only.

Screens the most-traded US common stocks for famous, fundamentally strong
companies that have dipped significantly for situational reasons, computes
watchlist KPIs, and builds a static site (index + one detail page per stock)
for GitHub Pages.

Method in one paragraph: universe = 800 most-traded common stocks by average
daily dollar volume; candidates are 10-50% below their 52-week high, not in
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
    """Most-traded common stocks by trailing dollar volume needs prices, so the
    liquidity ranking reuses the previous run's watchlist universe cache when
    present; on a cold start it ranks by the directory order after a quick
    spark pass is overkill — instead we screen the 800 largest-ADV names from
    the cached stage file if available, else the first 800 directory names.
    In practice the daily job always has yesterday's data/cache."""
    cache = os.path.join(DATA, "universe_cache.json")
    if os.path.exists(cache):
        return json.load(open(cache))
    universe = {}
    for fname in ("nasdaqlisted.txt", "otherlisted.txt"):
        text = http_get(f"https://www.nasdaqtrader.com/dynamic/symdir/{fname}",
                        timeout=30).decode("utf-8", "replace")
        for row in csv.DictReader(io.StringIO(text), delimiter="|"):
            sym = (row.get("Symbol") or row.get("ACT Symbol") or "").strip()
            name = (row.get("Security Name") or "").strip()
            if not sym or not name or sym == "Symbol":
                continue
            if (row.get("Test Issue") or "N") == "Y":
                continue
            if (row.get("ETF") or "N") == "Y":
                continue
            low = name.lower()
            if any(h in low for h in ("warrant", "unit", "right", "preferred",
                                     "note due", "%", "etf", "fund")):
                continue
            if "%" in name:
                continue
            universe[sym] = name
    return universe


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
            and k["last"] >= 5 and k["adv_m"] >= 50 and k["vol30"] <= 65)


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
        "da": ["DepreciationDepletionAndAmortization"],
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
        ebitda = (f["oi"] + f["da"]) if (f["oi"] is not None and f["da"] is not None) else None
        debt = (f["ltd"] or 0) + (f["debtcur"] or 0)
        net_debt = debt - (f["cash"] or 0)
        ev = (mktcap + net_debt) if mktcap else None
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
:root{--bg:#0b1220;--card:#111a2e;--line:#1e2a45;--txt:#e6ecf7;--mut:#8fa3c8;--acc:#4da3ff;--up:#34d399;--dn:#f87171}
*{box-sizing:border-box} body{margin:0;font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--txt)}
a{color:var(--acc);text-decoration:none} a:hover{text-decoration:underline}
.wrap{max-width:1280px;margin:0 auto;padding:28px 18px 60px}
h1{font-size:26px;margin:0 0 4px} .sub{color:var(--mut);font-size:13.5px;margin-bottom:18px;line-height:1.55}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:18px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px 16px}
.card .n{font-size:24px;font-weight:700} .card .l{color:var(--mut);font-size:12px;text-transform:uppercase;letter-spacing:.06em;margin-top:2px}
.controls{display:flex;gap:10px;flex-wrap:wrap;margin:14px 0}
.controls input,.controls select{background:var(--card);border:1px solid var(--line);color:var(--txt);border-radius:8px;padding:9px 12px;font-size:14px}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);border-radius:12px;overflow:hidden}
th,td{padding:9px 10px;font-size:13.5px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
th{color:var(--mut);font-size:11.5px;text-transform:uppercase;letter-spacing:.05em;cursor:pointer;user-select:none;background:#0e1730}
th:hover{color:var(--txt)} td.num,th.num{text-align:right} td.num{font-variant-numeric:tabular-nums}
tr:hover td{background:#14213d}
.sym{font-weight:700;font-size:15px} .nm{display:block;color:var(--mut);font-weight:400;font-size:11.5px;max-width:200px;overflow:hidden;text-overflow:ellipsis}
.ind{color:var(--mut)} .muted{color:var(--mut)}
.up{color:var(--up)} .dn{color:var(--dn)}
.chip{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11.5px;font-weight:600}
.sb{background:#064e3b;color:#6ee7b7}.b{background:#0c3540;color:#67e8f9}.h{background:#3f3413;color:#fcd34d}.r{background:#450a0a;color:#fca5a5}.na{background:#1f2937;color:#9ca3af}
.range{position:relative;width:110px;height:8px;background:#1e2a45;border-radius:4px;display:inline-block;vertical-align:middle}
.range .fill{position:absolute;left:0;top:0;bottom:0;background:linear-gradient(90deg,#f87171,#fbbf24,#34d399);border-radius:4px;opacity:.85}
.flag{color:#fca5a5} .foot{color:var(--mut);font-size:12.5px;margin-top:18px;line-height:1.6}
.star{color:#fbbf24}
.tabs{display:flex;gap:8px;margin:0 0 16px}
.tab{padding:9px 16px;border:1px solid var(--line);border-radius:999px;color:var(--mut);font-weight:600;font-size:14px;background:var(--card)}
.tab:hover{text-decoration:none;color:var(--txt)}
.tab.active{background:var(--acc);border-color:var(--acc);color:#081120}
.fgrade{display:inline-block;min-width:34px;text-align:center;padding:2px 5px;border-radius:6px;font-weight:700;font-size:12px;margin-right:3px}
.biggrade{font-size:21px;font-weight:800;padding:5px 12px;border-radius:9px;display:inline-block}
ul.kill{margin:6px 0;padding-left:20px} ul.kill li{line-height:1.6;font-size:14.5px;color:#dbe4f5;margin:4px 0}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:18px 0}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px 14px}
.kpi .v{font-size:19px;font-weight:700} .kpi .l{color:var(--mut);font-size:11.5px;text-transform:uppercase;letter-spacing:.05em}
section{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:18px 20px;margin:14px 0}
section h2{margin:0 0 8px;font-size:17px} section p{line-height:1.65;font-size:14.5px;color:#dbe4f5;margin:6px 0}
.bigchart{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px;margin:14px 0}
.price{font-size:30px;font-weight:800}
@media(max-width:900px){.nm{max-width:110px}}
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


TABS_DIP = ('<div class="tabs"><a class="tab active" href="index.html">📉 Quality-Dip Top 50</a>'
            '<a class="tab" href="pro.html">🏦 Professional Top 50</a>'
            '<a class="tab" href="sectors.html">🌍 Sectors</a>'
            '<a class="tab" href="validated.html">🏆 Validated Top 50</a></div>')
TABS_PRO = ('<div class="tabs"><a class="tab" href="index.html">📉 Quality-Dip Top 50</a>'
            '<a class="tab active" href="pro.html">🏦 Professional Top 50</a>'
            '<a class="tab" href="sectors.html">🌍 Sectors</a>'
            '<a class="tab" href="validated.html">🏆 Validated Top 50</a></div>')
TABS_SECTORS = ('<div class="tabs"><a class="tab" href="index.html">📉 Quality-Dip Top 50</a>'
                '<a class="tab" href="pro.html">🏦 Professional Top 50</a>'
                '<a class="tab active" href="sectors.html">🌍 Sectors</a>'
                '<a class="tab" href="validated.html">🏆 Validated Top 50</a></div>')
TABS_VALIDATED = ('<div class="tabs"><a class="tab" href="index.html">📉 Quality-Dip Top 50</a>'
                  '<a class="tab" href="pro.html">🏦 Professional Top 50</a>'
                  '<a class="tab" href="sectors.html">🌍 Sectors</a>'
                  '<a class="tab active" href="validated.html">🏆 Validated Top 50</a></div>')


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
    srcs = " · ".join(f"<a href='{s.get('url', '')}' target='_blank' rel='noopener'>{s.get('title', 'source')}</a>"
                       for s in (v.get("sources") or []))
    return (f"<section><h2>🔬 Research-agent verdict</h2>"
            f"<p><span class='chip {cls}' style='font-size:15px'>{flag}</span>"
            f" <span class='muted'>deep-researched {v.get('date', '')} · {v.get('origin', '')}</span></p>"
            f"<p>{v.get('summary', '')}</p>"
            f"<table><thead><tr><th>Rubric leg</th><th>Finding</th><th>Evidence</th></tr></thead>"
            f"<tbody>{leg_rows}</tbody></table>"
            f"<p style='margin-top:10px'><b>Key risks:</b></p><ul>{risks}</ul>"
            f"<p><b>What would change this verdict:</b> {v.get('what_changes', '')}</p>"
            f"<p class='muted'>Sources: {srcs}</p>"
            f"<p class='muted'>A structured research verdict from public sources, "
            f"not a trade instruction.</p></section>")


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
        body.append(f"""<tr data-ind="{INDUSTRY.get(t, '')}">
  <td>{r['rank']}</td>
  <td><a class="sym" href="stock/{t}.html">{t}</a>{star}<span class="nm">{clean_company_name(r['company'])}</span></td>
  <td class="ind">{INDUSTRY.get(t, '')}</td>
  <td class="num">{fr}</td>
  <td class="num">${r['last']:,.2f}</td>
  <td class="num {'up' if r['day_pct'] >= 0 else 'dn'}">{r['day_pct']:+.2f}%</td>
  <td class="num">{r['rsi14']:.0f}</td>
  <td class="num {'up' if r['vpt20'] >= 0 else 'dn'}">{r['vpt20']:+.0f}%</td>
  <td class="num {'up' if r['rs20'] >= 0 else 'dn'}">{r['rs20']:+.1f}%</td>
  <td class="num">{mv}</td>
  <td><span class="range"><span class="fill" style="width:{r['range_pos']}%"></span></span>
      <span class="dn">{r['off_high_pct']:.1f}%</span></td>
  <td>{spark_svg(r['spark'])}</td>
  <td class="num">{r['vol10']:.1f}% / {r['vol30']:.1f}%</td>
  <td>{chip_for(label)}{flag}{f"<br><span class='muted'>avg ${tgt:,.0f}</span>" if tgt else ""}</td>
  <td class="num">{r['earnings'] or '<span class="muted">—</span>'}</td>
  <td class="num">{r['pe'] if r['pe'] else '<span class="muted">—</span>'}</td>
  <td class="num">{('%.1f%%' % r['roic']) if r['roic'] is not None else '<span class="muted">—</span>'}</td>
  <td class="num {'up' if r['perf1y_pct'] >= 0 else 'dn'}">{r['perf1y_pct']:+.1f}%</td>
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
<title>Quality-Dip Watchlist — Top 50</title><style>{CSS}</style></head><body>
<div class="wrap">
{TABS_DIP}
<h1>📉 Quality-Dip Watchlist — Top 50</h1>
<div class="sub">Famous, fundamentally strong brands that have dipped significantly for <b>situational</b>
reasons — the kind of names expected to recover when conditions normalize. Screened from the
{meta['universe_n']} most-traded US common stocks ({meta['screened']} with usable data,
{meta['candidates']} passing the dip screen). Prices as of the <b>{asof}</b> close · refreshed daily at 7:00 AM CT.
Click any symbol for its detailed analysis. ★ = hand-researched brief.</div>
<div class="cards">
  <div class="card"><div class="n">50</div><div class="l">Names on the list</div></div>
  <div class="card"><div class="n">{n_researched}</div><div class="l">Hand-researched briefs</div></div>
  <div class="card"><div class="n">{n_sb}</div><div class="l">Strong Buy consensus</div></div>
  <div class="card"><div class="n">{len(this_week)}</div><div class="l">Reporting within ±7 days</div></div>
</div>
<div class="controls">
  <input id="q" type="text" placeholder="Search symbol or company…" size="28">
  <select id="ind"><option value="">All industries</option>{options}</select>
</div>
<table id="t"><thead><tr>
<th>#</th><th>Symbol</th><th>Industry</th><th class="num">Fortune 500</th><th class="num">Last</th>
<th class="num">% Change</th><th class="num">RSI</th><th class="num">VPT 20d</th><th class="num">Rel Str 20d</th>
<th class="num">vs Prev</th><th>52-week position / off high</th>
<th>Last 3 months</th><th class="num">Vol 10d / 30d</th><th>Analyst consensus</th>
<th class="num">Earnings</th><th class="num">P/E</th><th class="num">ROIC</th><th class="num">1Y perf</th>
</tr></thead><tbody>
{chr(10).join(body)}
</tbody></table>
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
</div>
<script>
const q=document.getElementById('q'),ind=document.getElementById('ind');
function filt(){{const s=q.value.toLowerCase(),f=ind.value;
document.querySelectorAll('#t tbody tr').forEach(tr=>{{
const okS=tr.textContent.toLowerCase().includes(s);
const okI=!f||tr.dataset.ind===f; tr.style.display=(okS&&okI)?'':'none';}});}}
q.addEventListener('input',filt);ind.addEventListener('change',filt);
document.querySelectorAll('#t th').forEach((th,i)=>{{th.title='Click to sort';
th.addEventListener('click',()=>{{const tb=document.querySelector('#t tbody');
const rows=[...tb.rows];const num=th.classList.contains('num');
const dir=th.dataset.d==='a'?'d':'a';th.dataset.d=dir;
rows.sort((a,b)=>{{let x=a.cells[i].textContent.replace(/[^0-9.+-]/g,''),y=b.cells[i].textContent.replace(/[^0-9.+-]/g,'');
if(num&&x!==''&&y!==''){{x=parseFloat(x);y=parseFloat(y);return dir==='a'?x-y:y-x;}}
return dir==='a'?a.cells[i].textContent.localeCompare(b.cells[i].textContent):b.cells[i].textContent.localeCompare(a.cells[i].textContent);}});
rows.forEach(r=>tb.appendChild(r));}});}});
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
        vsec = verdict_section(t, verdicts)
        if vsec:
            secs.append(vsec)
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(r['company'])} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="wrap">
<p><a href="../index.html">← Back to the Top 50</a></p>
<h1>{t} — {clean_company_name(r['company'])}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Rank #{r['rank']} on today's list</div>
<div><span class="price">${r['last']:,.2f}</span>
<span class="{'up' if r['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{r['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
<div class="kpis">
  <div class="kpi"><div class="v">{r['off_high_pct']:.1f}%</div><div class="l">Below 52w high (${r['high52']:,.2f})</div></div>
  <div class="kpi"><div class="v">{r['range_pos']:.0f}%</div><div class="l">Of 52w range (low ${r['low52']:,.2f})</div></div>
  <div class="kpi"><div class="v">{r['rsi14']:.0f}</div><div class="l">RSI (14)</div></div>
  <div class="kpi"><div class="v">{r['mom20_pct']:+.1f}%</div><div class="l">20-day momentum</div></div>
  <div class="kpi"><div class="v {'up' if r['perf1y_pct'] >= 0 else 'dn'}">{r['perf1y_pct']:+.1f}%</div><div class="l">1-year performance</div></div>
  <div class="kpi"><div class="v">{r['vol30']:.1f}%</div><div class="l">Volatility (30d, ann.)</div></div>
  <div class="kpi"><div class="v">{r['pe'] if r['pe'] else '—'}</div><div class="l">P/E (SEC TTM EPS)</div></div>
  <div class="kpi"><div class="v">{('%.1f%%' % r['roic']) if r['roic'] is not None else '—'}</div><div class="l">ROIC (SEC filings)</div></div>
  <div class="kpi"><div class="v {'up' if r['vpt20'] >= 0 else 'dn'}">{r['vpt20']:+.0f}%</div><div class="l">VPT trend (20d)</div></div>
  <div class="kpi"><div class="v {'up' if r['rs20'] >= 0 else 'dn'}">{r['rs20']:+.1f}%</div><div class="l">Rel. strength vs S&amp;P 500 (20d)</div></div>
  <div class="kpi"><div class="v">{r['earnings'] or '—'}</div><div class="l">Next earnings</div></div>
</div>
<div class="bigchart">{chart_svg(r['chart'], r['high52'], r['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a> ·
<a href="https://www.google.com/finance/quote/{t}:NASDAQ" target="_blank" rel="noopener">Google Finance — {t}</a></p>
<p class="muted">Data: Yahoo Finance daily bars, Nasdaq earnings calendar, SEC EDGAR, aggregator
analyst consensus (curated). Research and education only — not investment advice.</p></section>
</div></body></html>"""
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
        body.append(f"""<tr data-ind="{INDUSTRY.get(t, '')}">
  <td>{r['rank']}</td>
  <td><a class="sym" href="stock/{t}.html">{t}</a><span class="nm">{clean_company_name(r['company'])}</span></td>
  <td class="ind">{INDUSTRY.get(t, '')}</td>
  <td><span class="biggrade chip {_grade_cls(r['grade'])}">{r['grade']}</span></td>
  <td>{fchips}</td>
  <td class="num">{r['fscore'] if r['fscore'] is not None else '—'}/9</td>
  <td class="num">{('%.1f%%' % r['fcf_y']) if r['fcf_y'] is not None else '—'}</td>
  <td class="num">{('%.1f×' % r['nd_ebitda']) if r['nd_ebitda'] is not None else '—'}</td>
  <td class="num">{surp}</td>
  <td class="num {'up' if (r['mom12_1'] or 0) >= 0 else 'dn'}">{('%+.1f%%' % r['mom12_1']) if r['mom12_1'] is not None else '—'}</td>
  <td class="num">{r['pe'] if r['pe'] else '—'}</td>
  <td class="num">${r['last']:,.2f}</td>
  <td class="num">{mv}</td>
  <td class="num">{r['earnings'] or '<span class="muted">—</span>'}</td>
</tr>""")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Professional Top 50 — Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="wrap">
{TABS_PRO}
<h1>🏦 Professional Top 50</h1>
<div class="sub">The professionals' lens, applied to the same {meta['universe_n']} most-traded US stocks:
<b>cash-flow quality and balance-sheet survivability first, price last</b> — a factor composite in the
spirit of Seeking Alpha Quant and the Zacks Rank. Six factors, percentile-ranked across
{n_pool} names with full SEC filing data: Profitability (22%), Revisions (20%), Value (20%),
Safety (15%), Growth (13%), Momentum (10%). Veto grades remove names outright
({n_vetoed} vetoed today: Piotroski F-score ≤ 3, extreme leverage with thin coverage,
persistent EPS misses, or cash-burn with dilution). Prices as of the <b>{asof}</b> close.
Click any symbol for its factor breakdown and pre-written exit rules.</div>
<table id="t"><thead><tr>
<th>#</th><th>Symbol</th><th>Industry</th><th>Grade</th><th>Factors (Prof · Rev · Val · Safe · Grow · Mom)</th>
<th class="num">F-score</th><th class="num">FCF yield</th><th class="num">Net debt/EBITDA</th>
<th class="num">EPS surprise (last 4)</th><th class="num">Momentum 12-1</th><th class="num">P/E</th>
<th class="num">Last</th><th class="num">vs Prev</th><th class="num">Earnings</th>
</tr></thead><tbody>
{chr(10).join(body)}
</tbody></table>
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
</div></body></html>"""
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
        vsec = verdict_section(t, verdicts)
        if vsec:
            secs.append(vsec)
        c = cons.get(t, {})
        if c.get("label") or c.get("note"):
            secs.append(
                f"<section><h2>Analyst consensus</h2><p>{chip_for(c.get('label', ''))}"
                f"{f' · average target ${c['target']:,.0f}' if c.get('target') else ''}</p>"
                f"<p>{c.get('note', '')}</p></section>")
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(r['company'])} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="wrap">
{TABS_PRO}
<p><a href="../pro.html">← Back to the Professional Top 50</a></p>
<h1>{t} — {clean_company_name(r['company'])}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Professional rank #{r['rank']} · composite grade {r['grade']}</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
<div class="kpis">
  <div class="kpi"><div class="v">{k['off_high_pct']:.1f}%</div><div class="l">Below 52w high (${k['high52']:,.2f})</div></div>
  <div class="kpi"><div class="v">{k['rsi14']:.0f}</div><div class="l">RSI (14)</div></div>
  <div class="kpi"><div class="v {'up' if k['perf1y_pct'] >= 0 else 'dn'}">{k['perf1y_pct']:+.1f}%</div><div class="l">1-year performance</div></div>
  <div class="kpi"><div class="v">{k['vol30']:.1f}%</div><div class="l">Volatility (30d, ann.)</div></div>
  <div class="kpi"><div class="v">{r['pe'] if r['pe'] else '—'}</div><div class="l">P/E</div></div>
  <div class="kpi"><div class="v">{r['earnings'] or '—'}</div><div class="l">Next earnings</div></div>
</div>
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a> ·
<a href="https://www.google.com/finance/quote/{t}:NASDAQ" target="_blank" rel="noopener">Google Finance — {t}</a></p>
<p class="muted">Data: Yahoo Finance daily bars, Nasdaq earnings calendar, SEC EDGAR.
Research and education only — not investment advice.</p></section>
</div></body></html>"""
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
<title>Sectors — Top 10 most impacted · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="wrap">
<h1>🌍 Top 10 areas most impacted right now</h1>
<div class="sub">Ranked by magnitude of impact from the current economic, market, trade, inflation,
jobs, political and geopolitical environment · research snapshot as of {d.get('asof', meta['asof'])}</div>
{TABS_SECTORS}
<section><h2>The six cross-cutting forces</h2>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px">{forces}</div></section>
{chr(10).join(cards)}
<section><h2>Near misses</h2>
<table><thead><tr><th>Area</th><th>Why it missed the Top 10</th></tr></thead><tbody>{nm}</tbody></table>
<p class="muted">Compiled from wire and major-press reporting (Reuters, Bloomberg, AP and others,
linked per sector). A snapshot, refreshed by research rather than by the daily screener.
Research and education only — not investment advice.</p></section>
</div></body></html>"""
    with open(os.path.join(ROOT, "sectors.html"), "w") as fh:
        fh.write(html)


def build_validated_page(meta, verdicts):
    vs = verdicts.get("verdicts") or {}
    final = verdicts.get("final50") or []
    pools_p = os.path.join(DATA, "pools.json")
    pools = json.load(open(pools_p)) if os.path.exists(pools_p) else {"dip": [], "pro": []}
    dip_ix = {r["ticker"]: r for r in pools.get("dip", [])}
    pro_ix = {r["ticker"]: r for r in pools.get("pro", [])}
    counts = {"BUY": 0, "HOLD": 0, "SELL": 0}
    for v in vs.values():
        if v.get("flag") in counts:
            counts[v["flag"]] += 1
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
        rows.append(
            f"<tr><td>{i}</td><td><a href='stock/{t}.html'><b>{t}</b></a></td>"
            f"<td>{clean_company_name(UNIVERSE_NAME.get(t, t))}</td>"
            f"<td><span class='chip sb'>BUY</span></td>"
            f"<td>{' · '.join(origin)}</td>"
            f"<td class='num'>${src.get('last', 0):,.2f}</td>"
            f"<td class='num'>{src.get('off_high_pct', 0):.1f}%</td>"
            f"<td>{v.get('summary', '')}</td></tr>")
    status = (f"<p>Research complete: {len(final)} names passed the research rubric as BUY. "
              f"Researched in total: {len(vs)} ({counts['BUY']} buy · {counts['HOLD']} hold · "
              f"{counts['SELL']} sell/not-justified).</p>" if len(final) >= 50 else
              f"<p>Research in progress: {len(vs)} names researched so far "
              f"({counts['BUY']} buy · {counts['HOLD']} hold · {counts['SELL']} sell/not-justified); "
              f"{len(final)} buys validated. The list below grows as the research loop works "
              f"through the merged candidate queue.</p>")
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Validated Top 50 · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="wrap">
<h1>🏆 Research-Validated Top 50</h1>
<div class="sub">One merged list from both screens (Quality-Dip + Professional), where every name
also passed a dedicated deep-research pass · prices as of {meta['asof']} close</div>
{TABS_VALIDATED}
<section>{status}
<table id="t"><thead><tr><th>#</th><th>Stock</th><th>Company</th><th>Verdict</th><th>Origin</th>
<th class="num">Price</th><th class="num">vs 52w high</th><th>Research summary</th></tr></thead>
<tbody>{chr(10).join(rows)}</tbody></table>
<p class="muted" style="margin-top:12px">Method: candidates from both Top 50 lists first, then the
merged bench in rank order, were deep-researched one by one against a five-leg rubric — business
quality intact, valuation/dip justified, sector wind (see the Sectors tab), risk survivability, and a
visible catalyst path. A name is BUY only with no leg failing and at least four passing; HOLD and
SELL verdicts drop out and the next bench name is researched, until 50 BUY names stand. Every stock
page carries the full verdict with evidence, risks, and what would change it. Verdicts are a dated
research snapshot (not refreshed by the daily screener) from public sources — structured research
opinion, not investment advice or a trade instruction.</p></section>
</div></body></html>"""
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
        vsec = verdict_section(t, verdicts)
        if vsec:
            secs.append(vsec)
        ys = yahoo_symbol(t)
        page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))} · Quality-Dip Watchlist</title><style>{CSS}</style></head><body>
<div class="wrap">
<p><a href="../validated.html">← Back to the Validated Top 50</a></p>
<h1>{t} — {clean_company_name(UNIVERSE_NAME.get(t, t))}</h1>
<div class="sub">{INDUSTRY.get(t, '')}{' · Fortune 500 #' + str(fortune[t]) if fortune.get(t) else ''}
· Research-validated Top 50</div>
<div><span class="price">${k['last']:,.2f}</span>
<span class="{'up' if k['day_pct'] >= 0 else 'dn'}" style="font-size:17px">{k['day_pct']:+.2f}% today</span>
<span class="muted"> · as of {asof} close</span></div>
<div class="kpis">
  <div class="kpi"><div class="v">{k['off_high_pct']:.1f}%</div><div class="l">Below 52w high (${k['high52']:,.2f})</div></div>
  <div class="kpi"><div class="v">{k['rsi14']:.0f}</div><div class="l">RSI (14)</div></div>
  <div class="kpi"><div class="v {'up' if k['perf1y_pct'] >= 0 else 'dn'}">{k['perf1y_pct']:+.1f}%</div><div class="l">1-year performance</div></div>
  <div class="kpi"><div class="v">{k['vol30']:.1f}%</div><div class="l">Volatility (30d, ann.)</div></div>
  <div class="kpi"><div class="v">{earn.get(t) or '—'}</div><div class="l">Next earnings</div></div>
</div>
<div class="bigchart">{chart_svg(k['chart'], k['high52'], k['low52'])}
<div class="muted" style="font-size:12px">One year of daily closes.</div></div>
{chr(10).join(secs)}
<section><h2>Dig deeper</h2><p>
<a href="https://finance.yahoo.com/quote/{ys}" target="_blank" rel="noopener">Yahoo Finance — {t}</a> ·
<a href="https://stockanalysis.com/stocks/{t.lower()}/" target="_blank" rel="noopener">StockAnalysis — {t}</a></p>
<p class="muted">Research and education only — not investment advice.</p></section>
</div></body></html>"""
        with open(os.path.join(ROOT, "stock", f"{t}.html"), "w") as fh:
            fh.write(page)


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
    print(f"Universe: {len(tickers)} most-traded common stocks", flush=True)

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
            "candidates": len(cands)}
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
                                                       "low52", "off_high_pct", "rsi14",
                                                       "perf1y_pct", "vol30", "chart")}})
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
    meta["keep_extra"] = ([r["ticker"] for r in pro_rows]
                          + list(verdicts.get("final50") or []))
    build_site(rows, prev_ranks, meta, pro=pro_recs,
               pro_ranks={r["ticker"]: r["rank"] for r in pro_rows})
    build_pro_page(pro_rows, prev_pro_ranks, meta, n_pool, n_vetoed)
    build_pro_details(pro_rows, {r["ticker"] for r in rows}, meta)
    build_sectors_page(meta)
    build_validated_page(meta, verdicts)
    build_validated_details(meta, verdicts,
                            {r["ticker"] for r in rows} | {r["ticker"] for r in pro_rows},
                            results, pro_recs, earn)

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
