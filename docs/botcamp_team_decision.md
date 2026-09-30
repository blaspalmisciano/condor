# Botcamp Agent Builders Cup — Team (Squad) Selection Decision

*Decision recorded 2026-09-13. Agent: "Self-Improving Market Making Fleet" (PMM Autopilot).*
*Grounded in a critical, web-researched review of all 6 sponsor squads.*

---

## DECISION

**Top 1 = Gate. Top 2 = Bitget.**

Optional override: **swap Bitget → XRPL for Top 2** *only* if you conclude the **community-vote**
component of scoring is weighted heavily and you want a standout differentiation narrative over
raw Volume/P&L score. Base case stays Gate + Bitget.

---

## The cup mechanics (verified from the botcamp page / Hummingbot materials)

- **6 sponsor squads + Botcamp; 2 agent seats each = 14 total seats.** Registration window ran
  through Sep 30; squads select their "Agent Drivers" from applicants (two-sided match).
- **Prize pool $20,000+ USDC.** **$800 real starting capital per agent** (builder keeps P&L).
  No entry fee.
- **Finals = 48-hour live trading (Oct 1–2).** Code freeze Sep 30. Winners announced Oct 7 at
  Token2049 Singapore.
- **Scoring = Volume + P&L + community vote.** Exact weighting is **NOT public** (key caveat).
- **Strategy-family classification (confirmed by Hummingbot):**
  - Bitget = CLOB CEX · Gate = CLOB CEX · XRPL = CLOB DEX · Derive = CLOB DEX (options/perps/spot)
  - Meteora = AMM DEX · Orca = AMM DEX
- **Hummingbot connectors exist for all six** (`bitget`/`bitget_perpetual`, `gate_io`,
  `xrpl` native CLOB, `derive`/`derive_perpetual`, `meteora`, `orca`).

## Why Gate + Bitget (the corrected reasoning)

The agent is a **CLOB order-book market maker** (`pmm_mister`) whose objective is
**business PnL = market PnL + volume × rebate**, plus raw volume. The cup scores on
**Volume + P&L**. That objective↔metric alignment is the whole case — **but volume is only
attainable on a deep order book with maker rebates.**

- **Gate (Top 1):** native spot CLOB CEX; **emptiest applicant field of any CLOB venue**
  (0 shown at fetch time); deep liquidity so the volume-maximizing agent can actually score on
  the Volume metric; **lowest-friction port** from the current Binance setup. Best expected value.
- **Bitget (Top 2):** same deep-CLOB-CEX fit; its only applicants at fetch were **both
  perp-focused**, leaving an **open spot-MM lane**. Slightly more contested than Gate.

## Sponsor selection criteria — VERIFIED (received 2026-09-13, from the squad pages)

Both squads' published criteria describe this agent almost word-for-word — strong confirmation.

- **Gate:** "Strategies must trade on Gate spot or perpetuals. Preference for high-volume
  **pure market making** on top BTC/ETH/SOL/USDT pairs, or cross-exchange arbitrage against
  another eligible CEX. Bonus points for sophisticated funding-rate arbitrage and/or perp-basis
  strategies."
- **Bitget:** "Strategies must trade on Bitget spot or perpetuals. Preference for high-volume
  market making on top BTC/ETH/USDT pairs, or cross-exchange arbitrage against another eligible
  venue. Bonus points for funding-rate arbitrage and/or perp-basis strategies."

What this establishes:
- **The agent IS the primary preference.** It's a high-volume pure market maker → exact bullseye.
  Gate's literal phrase "pure market making" = `pmm_mister` (not arb, not directional).
- **RESOLVED: the squad dictates the venue.** "Must trade on Gate/Bitget spot or perpetuals" →
  you WILL port off Binance BTC-BRL to a **USDT pair** on the sponsor's book (BRL doesn't travel).
  Targets: Gate → BTC/ETH/**SOL**-USDT; Bitget → BTC/ETH-USDT (no SOL). **BTC-USDT is the safe
  deep default on both**, closest to the tested BTC-BRL. Low-friction port (connector + pair swap).
- **Honest gap:** bonus points are for funding-rate arb / perp-basis / cross-exchange arb — this
  agent does NONE of those. So no bonus, but the PRIMARY preference (high-volume pure MM) is
  nailed. Building an arb/funding agent under the deadline is out of scope — stay in the MM lane.
- Gate's "pure market making" wording makes it an especially clean Top-1 fit.

## Ranked verdict for THIS agent (best → worst)

1. **Gate** — native spot CLOB, thinnest field, deep liquidity, cheapest port. Best EV.
2. **Bitget** — same CLOB-CEX fit; open spot-MM lane (applicants all perp).
3. **XRPL** — genuine order-book fit + best differentiation/community-vote story, BUT thin
   on-chain DEX volume caps the exact metric the agent is built to win; heavier port. Dark horse.
4. **Derive** — CLOB connector exists but it's an options/vol venue with a specialist field;
   the 1s spot-parity edge doesn't transfer (needs funding/greeks). Wrong instrument.
5. **Meteora** — AMM DLMM. Wrong strategy family; no order book to quote.
6. **Orca** — AMM CLMM. Wrong strategy family; no order book to quote.

## Assumptions I was WRONG about earlier (corrected here)

- ❌ "Bitget is the most crowded" — **false.** At fetch: Gate 0, Bitget 2 (both perp), XRPL 4,
  Derive 5. Bitget was not the crowded one.
- ❌ "Bitget is the headline sponsor" — **unverified**; public materials list all six equally.
  Dropped as a decision factor.
- ✅ AMM dismissal (Meteora/Orca) and the final ranking held.

## Porting caveat (applies to EVERY pick)

The proven, tested edge is **Binance BTC-BRL**. Binance is an *eligible exchange* but **not a
sponsor squad**, so any squad choice requires porting off the tested pair. The framework
(pmm_mister + 1s parity loop + reshape) is venue-agnostic across CLOB CEXs, so **Gate/Bitget
ports are low-friction** (quote a liquid USDT pair — BRL liquidity does not travel off Binance).
XRPL/Derive ports are materially harder.

## Confirm before submitting (could NOT be verified)

1. **Exact scoring weights** among Volume / P&L / community vote — not public. The "deep-CEX
   favors you" case assumes Volume carries real weight; if it's mostly community vote, XRPL's
   case strengthens (→ consider the Top-2 override).
2. ~~Whether your squad dictates your trading venue.~~ **RESOLVED 2026-09-13:** the Gate/Bitget
   criteria state strategies MUST trade on that sponsor's spot/perp book. You will port to a
   USDT pair (BTC-USDT default). BRL does not travel off Binance.
3. **Live applicant counts** — snapshots are cached; re-check team pages right before submitting
   (Gate being empty is most likely to change).
4. **BRL pair availability on Gate/Bitget** — not confirmed; assume a USDT pair, not BRL.

## Sources
Botcamp Agent Builders Cup page + team pages (bitget/gate/xrpl/derive); Hummingbot Newsletter
July 2026; hummingbot.org/exchanges; Bitget / Gate.io / Derive connector docs; Hummingbot CLOB
connectors list.
