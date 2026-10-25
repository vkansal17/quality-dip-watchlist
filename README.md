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
