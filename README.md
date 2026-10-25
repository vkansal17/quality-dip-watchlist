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
