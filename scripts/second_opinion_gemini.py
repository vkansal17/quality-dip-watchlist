#!/usr/bin/env python3
"""Second-opinion cross-check of the watchlist research verdicts by Gemini.

Advisory only — modeled on the Nemotron second-opinion pattern: Gemini
never changes a stored verdict. Two checks per verdict:

  regrade — Gemini scores the five rubric legs BLIND (it never sees our
            flag or notes), using Google Search grounding for current
            facts, and its flag is computed from its legs with the same
            mechanical rule as merge_verdicts.rubric_flag().
  audit   — Gemini reads our verdict card (legs, notes, summary, cited
            sources) and flags claims the evidence does not support,
            figures that look wrong, and weak citations.

Results accumulate in data/gemini_crosscheck.json (re-runs skip names
already checked, so the run is resumable). The credential comes from
the registered custom.gemini connection via Muse's surrogate flow —
no key is read from disk or the environment.

Usage:
  python3 scripts/second_opinion_gemini.py --tickers APA,TMUS,LUV
  python3 scripts/second_opinion_gemini.py --store both --limit 20
  python3 scripts/second_opinion_gemini.py --mode audit
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/bin")
from dynamic_credentials import add_surrogate_to_request  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
OUT = os.path.join(DATA, "gemini_crosscheck.json")
HOST = "generativelanguage.googleapis.com"
MODELS = ["gemini-3.5-flash", "gemini-3-flash-preview", "gemini-3.8-flash"]

RUBRIC = """Rubric (five legs, each pass / mixed / fail):
- quality: business intact, no thesis-breaking deterioration (guidance cuts, volume declines = fail)
- valuation: price justified for quality (consensus targets are crowd context only)
- sector: map the company to this sector map — 1 AI hyperscalers/data centers Negative/Severe, 2 Oil & gas/refiners Positive/Severe, 3 Semiconductors/memory Negative/Severe, 4 Housing/builders/mortgage Negative/Severe, 5 Airlines/travel Negative/Severe, 6 Defense Mixed/Severe, 7 Banks/financials/insurers Mixed/High, 8 Consumer discretionary/retail/staples Negative/High, 9 Agriculture & food Negative/High, 10 Small caps Negative/High. A Severe-negative sector = fail absent a concrete, evidenced company-specific offset.
- risk: leverage, liquidity, cliffs, dilution, concentration — survivability
- catalyst: identifiable driver within ~2 quarters
Flag rule (computed mechanically): BUY = no leg fails AND at least 4 pass; SELL = 2+ legs fail, or quality AND risk both fail; otherwise HOLD.
Reference point: verdicts were researched on 2026-10-25 against Fri 2026-10-23 closing prices. Judge the evidence as of that date; later news does not invalidate a verdict."""


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


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        raise RuntimeError("redirect refused, verify the endpoint")


class QuotaStop(Exception):
    """Daily quota exhausted — stop the run; progress so far is saved."""


def gemini(prompt, grounded=True, timeout=240):
    body = {"contents": [{"parts": [{"text": prompt}]}]}
    if grounded:
        body["tools"] = [{"google_search": {}}]
    last = None
    for model in MODELS:
        req = urllib.request.Request(
            f"https://{HOST}/v1beta/models/{model}:generateContent",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        add_surrogate_to_request(req, "custom.gemini", allowed_hosts=[HOST])
        for attempt in range(2):
            try:
                with urllib.request.build_opener(NoRedirect()).open(
                        req, timeout=timeout) as r:
                    out = json.loads(r.read())
                cand = out["candidates"][0]
                return "".join(p.get("text", "")
                               for p in cand["content"]["parts"]), model
            except urllib.error.HTTPError as e:
                detail = e.read().decode(errors="replace")[:200]
                last = f"{model} HTTP {e.code}: {detail}"
                if e.code == 429:
                    raise QuotaStop(last)
                if e.code == 503:
                    time.sleep(15 * (attempt + 1))
                    continue
                break  # other errors: fall through to next model
            except Exception as e:  # timeouts etc.
                last = f"{model} {type(e).__name__}: {str(e)[:120]}"
                time.sleep(10)
        print(f"  model fallback after: {last}", file=sys.stderr)
    raise RuntimeError(f"all models failed; last: {last}")


def extract_json(text):
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("no JSON object in response")
    return json.loads(text[start:end + 1])


def regrade_prompt(ticker, company, v):
    theme = v.get("theme") or ""
    theme_line = f" Its macro theme tag is: {theme}." if theme else ""
    return f"""You are an independent equity research analyst. Research the stock {ticker} ({company}) using web search and score it under this rubric. You are grading blind — no other analyst's opinion is provided, and none should be assumed.{theme_line}

{RUBRIC}

Return ONLY a JSON object, no prose around it:
{{"legs": {{"quality": {{"v": "pass|mixed|fail", "note": "one line of evidence"}},
"valuation": {{"v": "...", "note": "..."}}, "sector": {{"v": "...", "note": "..."}},
"risk": {{"v": "...", "note": "..."}}, "catalyst": {{"v": "...", "note": "..."}}}},
"summary": "2-3 sentences"}}"""


def audit_prompt(ticker, company, v, grounded=True):
    how = ("Using web search, audit it:" if grounded else
           "Audit it from the card itself and your own knowledge — you "
           "have NO web access this run, so mark any claim you cannot "
           "verify as questionable rather than supported:")
    card = {
        "flag": v.get("flag"),
        "summary": v.get("summary"),
        "legs": v.get("legs"),
        "risks": v.get("risks"),
        "what_changes": v.get("what_changes"),
        "sources": v.get("sources"),
    }
    return f"""You are an evidence auditor. Below is a research verdict card for {ticker} ({company}), produced by another analyst on 2026-10-25 (prices as of the Fri 2026-10-23 close). {how}

1. For each leg, judge whether the note's claims are supported: "supported", "questionable" (weakly sourced, overstated, or imprecise) or "unsupported" (contradicted by evidence or no basis).
2. List specific issues: figures that appear wrong, claims the cited sources do not establish, citations that look weak or mismatched.
Do not re-grade the stock; audit only the evidence quality of THIS card.

CARD:
{json.dumps(card, indent=1)}

Return ONLY a JSON object, no prose around it:
{{"legs": {{"quality": {{"status": "supported|questionable|unsupported", "note": "one line"}},
"valuation": {{"status": "...", "note": "..."}}, "sector": {{"status": "...", "note": "..."}},
"risk": {{"status": "...", "note": "..."}}, "catalyst": {{"status": "...", "note": "..."}}}},
"issues": ["specific issue", "..."], "overall": "supported|mixed|unsupported"}}"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", choices=["validated", "macro", "both"],
                    default="both")
    ap.add_argument("--tickers", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--mode", choices=["regrade", "audit", "both"],
                    default="both")
    ap.add_argument("--no-ground", action="store_true",
                    help="skip Google Search grounding (free-tier quota)")
    ap.add_argument("--sleep", type=float, default=4.0)
    args = ap.parse_args()

    names = json.load(open(os.path.join(DATA, "universe_cache.json")))
    stores = {}
    if args.store in ("validated", "both"):
        stores["validated"] = json.load(
            open(os.path.join(DATA, "research_verdicts.json")))["verdicts"]
    if args.store in ("macro", "both"):
        stores["macro"] = json.load(
            open(os.path.join(DATA, "theme_verdicts.json")))["verdicts"]

    results = {}
    if os.path.exists(OUT):
        results = json.load(open(OUT))

    wanted = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    jobs = []
    for sname, verdicts in stores.items():
        for t, v in verdicts.items():
            if wanted and t not in wanted:
                continue
            key = f"{sname}:{t}"
            done = results.get(key, {})
            need = [m for m in ("regrade", "audit")
                    if args.mode in (m, "both") and m not in done]
            if need:
                jobs.append((sname, t, v, need))
    if args.limit:
        jobs = jobs[:args.limit]
    print(f"jobs: {len(jobs)}")

    for i, (sname, t, v, need) in enumerate(jobs, 1):
        key = f"{sname}:{t}"
        company = names.get(t, t)
        entry = results.setdefault(key, {"ticker": t, "store": sname,
                                         "our_flag": v.get("flag")})
        print(f"[{i}/{len(jobs)}] {key} ours={v.get('flag')}")
        for mode in need:
            try:
                if mode == "audit":
                    # The same card can live in both stores (carried
                    # verdicts) — reuse an audit already done under the
                    # other store's key instead of paying for it twice.
                    twin = next((e["audit"] for k2, e in results.items()
                                 if k2 != key and e.get("ticker") == t
                                 and e.get("audit")), None)
                    if twin:
                        entry["audit"] = {**twin, "reused_from_twin": True}
                        print("  audit: reused twin-store audit "
                              f"(overall={twin.get('overall')})")
                        json.dump(results, open(OUT, "w"), indent=1)
                        continue
                if mode == "regrade":
                    text, model = gemini(regrade_prompt(t, company, v),
                                         grounded=not args.no_ground)
                    g = extract_json(text)
                    gflag = rubric_flag(g.get("legs") or {})
                    entry["regrade"] = {
                        "legs": g.get("legs"), "summary": g.get("summary"),
                        "gemini_flag_computed": gflag, "model": model,
                        "agree": gflag == v.get("flag")}
                    print(f"  regrade: gemini={gflag} "
                          f"agree={gflag == v.get('flag')} ({model})")
                else:
                    text, model = gemini(
                        audit_prompt(t, company, v,
                                     grounded=not args.no_ground),
                        grounded=not args.no_ground)
                    a = extract_json(text)
                    entry["audit"] = {**a, "model": model}
                    print(f"  audit: overall={a.get('overall')} "
                          f"issues={len(a.get('issues') or [])} ({model})")
            except QuotaStop as e:
                json.dump(results, open(OUT, "w"), indent=1)
                print(f"\nQUOTA EXHAUSTED — stopping; progress saved. {e}")
                return
            except Exception as e:
                entry[f"{mode}_error"] = str(e)[:300]
                print(f"  {mode} FAILED: {str(e)[:160]}")
            json.dump(results, open(OUT, "w"), indent=1)
            time.sleep(args.sleep)

    agree = [e for e in results.values()
             if e.get("regrade") and "agree" in e["regrade"]]
    n_agree = sum(1 for e in agree if e["regrade"]["agree"])
    audits = [e for e in results.values() if e.get("audit")]
    print(f"\nregrade agreement: {n_agree}/{len(agree)} | "
          f"audits done: {len(audits)} | results in {OUT}")


if __name__ == "__main__":
    main()
