# Quality-Dip Watchlist — Top 50

**Live site:** https://vkansal17.github.io/quality-dip-watchlist/

Famous, fundamentally strong brands that have dipped significantly for
*situational* reasons — names expected to recover when conditions normalize —
screened daily from the 800 most-traded US common stocks.

## How it works

- **Screen** (`scripts/pipeline.py`, standard-library-only Python): candidates
  are 10–50% below their 52-week high, not in freefall (20-day momentum > −20%),
  RSI ≤ 65, price ≥ $5, average daily dollar volume ≥ $50M, 30-day volatility
  ≤ 65%. A composite score rewards dip depth in the 15–35% sweet spot,
  liquidity (weighted heavily — the brief is famous brands), stabilization,
  and limited long-term trend damage.
- **Researched core:** a hand-researched set of names (dip cause, business
  pulse, analyst consensus, buzz, risks — `data/research_notes.json`) leads
  the list while its members stay eligible. ★ marks them on the site.
- **Researched exclusions:** NKE and TSLA are deliberately kept off — their
  dips trace to deteriorating or polarized business stories, not situations
  (validated criterion-by-criterion, Oct 2026).
- **Data:** prices/KPIs/charts from Yahoo Finance daily bars; earnings dates
  from the Nasdaq calendar; P/E from SEC EDGAR diluted EPS (TTM); analyst
  consensus curated in `data/consensus.json`; Fortune 500 ranks (2026 list)
  in `data/fortune500.json`; universe cached in `data/universe_cache.json`.
- **Refresh:** the pipeline runs every morning at 7:00 AM CT, rebuilds the
  site (`index.html` + `stock/<TICKER>.html`), and pushes; GitHub Pages
  serves the result. The first column movement (▲/▼/NEW) is versus the
  previous day's list.

## The Professional tab

The second tab (`pro.html`) ranks the same universe the way institutional
screens do — quality and revision momentum first, price last — in the style
of published composites like Seeking Alpha Quant and the Zacks Rank:

- **Six factors, percentile-scored across ~700 graded names:**
  Profitability 22% (ROE, ROIC, margins), Revisions 20% (reported
  EPS-surprise history — the free proxy for analyst revision momentum,
  since true revision breadth is paid data), Value 20% (FCF yield,
  earnings yield, EV/EBITDA, price/sales), Safety 15% (Piotroski F-score,
  leverage, coverage, accruals), Growth 13%, Momentum 10% (12-1 month).
  The letter grade is the composite's percentile rank in the pool.
- **Hard vetoes** exclude names regardless of score: F-score ≤ 3
  (operating companies), net debt/EBITDA > 6× with thin coverage,
  three-plus straight EPS misses averaging under −8%, or cash-burning
  + loss-making + diluting > 5%.
- **Kill criteria:** every name carries exit rules written in advance
  (estimate, quality, leverage, cash-flow and price-structure triggers)
  on its detail page — the professional habit this screen is modeled on.
- Fundamentals come from SEC EDGAR bulk frames (latest reported fiscal
  year); the F-score veto applies only to companies reporting a
  cost-of-sales line, since bank/insurer balance sheets break its
  leverage and liquidity signals.

## The Sectors tab

The third tab (`sectors.html`) maps the current environment: the six
cross-cutting forces at work (an oil shock, inflation and rates, a
strong-but-pricey economy, cooling jobs, tariffs, and the midterms) and
the ten sectors they are hitting hardest — direction, severity, drivers,
evidence and what to watch next for each, with source articles. The
research-validation rubric below scores every candidate against this map.

## The Validated Top 50 tab

The fourth tab (`validated.html`) is the research-validated list. A
dedicated research loop worked through a merged queue of both pools
(the two Top 50s first, then the bench in pool-percentile order) and
scored each name on five legs — Quality, Valuation, Sector, Risk and
Catalyst — under `RESEARCH_RUBRIC.md`. The flag is computed from the
legs, not asserted: **BUY** = no leg fails and at least four pass;
**SELL** = two or more legs fail, or quality and risk both fail;
otherwise **HOLD**. Only BUY names enter the list, in queue order.

First edition (researched 2026-10-25, prices as of the Fri 2026-10-23
close): 180 names researched — 52 BUY, 125 HOLD, 3 SELL — filling all
50 slots, with two further BUY names (TRMB, BKR) as first reserves.
Every validated name's detail page carries its full verdict card:
per-leg evidence, key risks, what would change the verdict, and
sources. These are dated research verdicts with evidence, not trade
instructions, entries or targets.

## The Macro & Themes tab — Top 50

The fifth tab (`macro.html`) is the companion Top 50 whose origin is
macro and thematic rather than either ranking. It was built by an
independent sweep of the full 682-name merged candidate pool: every
name was tagged with the one macro theme that genuinely drives its
case — Geopolitics & Energy Security, Inflation & Rates, Economics &
Growth, Employment & Labor, Innovation, or Politics & Policy — and
researched under the same five-leg rubric until 50 BUYs stood. BUY
verdicts already earned in the earlier research editions were carried
in with their theme tag; the rest of the pool was worked in theme-fit
order (strong fits first). There is no per-theme quota — theme counts
are whatever the sweep produced, and BUYs past the 50 stand as named
reserves.

Sweep edition (researched 2026-10-25, prices as of the Fri 2026-10-23
close): 48 of the 61 existing BUYs carried a genuine theme tag (13
were company stories with no macro driver and stay off this list);
the first research wave over the strongest bench fits added 30 more
verdicts. Final tally across the macro store: 59 BUY, 68 HOLD, 2 SELL.
The Top 50 by theme: Innovation 17, Geopolitics & Energy Security 13,
Inflation & Rates 9, Economics & Growth 8, Politics & Policy 3 —
Employment & Labor's only BUY (WDAY) landed just past the cap and
stands as a reserve, alongside EOG, BURL, OVV, CI, DINO, MET, ENB
and LHX. The sweep's two SELLs are both homebuilders (NVR, PHM —
quality and sector legs failed in the map's worst sector).
Each verdict carries the same dated evidence card on its detail page
as the Validated Top 50.

## Run it yourself

```bash
python3 scripts/pipeline.py            # full run (~5-10 min)
python3 scripts/pipeline.py --skip-pe  # faster, leaves P/E blank
```

## Notes

- Research and education only — **not investment advice**. Consensus figures
  are aggregator data (TipRanks / MarketBeat / StockAnalysis), i.e. the
  crowd's opinion, not a forecast.
- Leveraged ETFs (e.g. TMF) are structurally out of scope: no business, no
  earnings, and daily-reset leverage decay makes "hold the dip" the exact
  strategy they punish.
