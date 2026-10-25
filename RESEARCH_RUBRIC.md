# Research-Agent Rubric v1 — validation of watchlist candidates

Applied uniformly by the research agent to every candidate, using current
public sources (wire/major press, company releases, analyst actions as
reported). Prices referenced are the Fri 2026-10-23 close from the
watchlist pipeline. Research/education output — structured research
verdicts, not trade instructions.

## The five legs (each: pass / mixed / fail, with one-line cited evidence)

1. **Quality** — business quality intact. Latest reported quarter(s) and
   guidance show no thesis-breaking deterioration: revenue/EPS trend
   stable or growing, no guidance slash, no structural break (lost anchor
   customer, broken franchise). Fail = deteriorating fundamentals with
   management guiding down.
2. **Valuation** — price justified. Either dipped meaningfully versus its
   own history and fundamentals with the business intact, or reasonably
   valued for demonstrated quality (P/E, FCF yield, EV/EBITDA vs peers;
   analyst consensus treated as crowd context, not proof). Fail = still
   pricing perfection with no margin of safety.
3. **Sector** — wind direction. Map the company to the Sectors research
   (Top-10 impacted areas, data/sectors.json): Positive sector =
   tailwind; Mixed = company evidence decides; Severe-negative sector =
   fail unless a specific company-level offset is evidenced.
4. **Risk** — survivability. Balance sheet (leverage, liquidity,
   coverage), concentration and cliff risks (patent expiry, single
   customer/product, regulatory), dilution or cash burn. Fail =
   existential or near-term cliff without mitigation.
5. **Catalyst** — visible path. An identifiable driver within ~2
   quarters: earnings setup, guidance/revision turn, product or
   regulatory event, buyback/insider support, sector catalyst. Fail =
   no identifiable driver; "cheap and stuck."

## Verdict rule

- **BUY** — no leg fails AND at least 4 legs pass.
- **HOLD** — exactly one leg fails, or only 3 legs pass with no leg
  failing.
- **SELL** (not justified) — two or more legs fail, or quality AND risk
  both fail (the existential combination). A lone quality or risk fail
  with the other hard leg passing is serious but survivable — HOLD.

The flag is computed from the legs by `scripts/merge_verdicts.py`
(`rubric_flag`); the researching agent proposes, the rubric decides, and
any override is recorded on the verdict (`rule_override`).

## Output per stock

flag; 2–3 sentence summary; per-leg verdict + one-line evidence;
2–4 key risks; what would change the verdict; 3–6 sources (title + URL);
research date.
