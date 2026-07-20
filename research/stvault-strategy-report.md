# stVault Weekly Strategy Report — 2026-07-20

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

No prior formal report exists in this repository — **this is the baseline report**. It formalizes and updates the verbal rev-3 briefing dated 2026-07-13.

Relative to that briefing, this week's research surfaced:

- **No change** to Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold), the 5% node-operator fee, or the Early Adopters infrastructure-fee waiver — still tracking toward the 2026-08-31 end date, though no source dated this week explicitly reconfirmed that date (see Data Gaps).
- **Rate environment (stETH APR, Aave Prime WETH borrow):** best available evidence this week is consistent with no material move — the stETH-minus-borrow spread remains well under the 0.5pp re-entry trigger. Confidence in this read is lower than usual this week; see Data Gaps.
- **New candidate identified:** Lido's own curated "GGV" (GG Vault) product, which on stale (Nov 2025) data would nominally clear the 4% curated-vault yield trigger. It is flagged below but **not recommended** — it fails the 150% health-factor floor and its yield figure could not be reconfirmed this week.
- **New risk information (not in the 2026-07-13 briefing):** Balancer was exploited for ~$110–128M in November 2025, and Balancer Labs (the corporate entity) announced a wind-down in March 2026, with protocol TVL down ~95% from peak. This effectively removes Balancer stETH pools from consideration going forward.
- **New:** Symbiotic pivoted from a restaking model to "collateral markets" with its Core V2 launch on 2026-07-01, so it no longer offers a clean, comparable AVS-restaking yield figure.
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; see Data Gaps — a fresh dashboard pull could not be completed this week).

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) ≈ $7.07M at $1,866/ETH (2026-07-20) |
| stETH liability | 3,509.2 stETH (~93% of total value) ≈ $6.55M |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH (~$0.20M); a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH, ~$6.55M) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below.

---

## Vault Parameters & Fee Structure (confirmed, no change this week)

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold.
- **Infrastructure fee:** waived under the Early Adopters campaign through 2026-08-31 (per prior briefing; not independently reconfirmed this week — flag if this changes, and re-verify before the deadline).
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of 2.22%, this equals **2.36%**, consistent with the prior briefing.

---

## Rate Environment (as of 2026-07-20)

### (a) Lido DAO / stVault governance and fees
No change to Basic Tier 1 parameters or fee structure. The main governance activity this week (LIP-33/LIP-35, concluded 2026-07-17, plus two open Snapshot proposals) concerns Curated Module v2 / Community Staking Module v3 — unrelated to stVault Basic Tier parameters. Early Adopters waiver end-date (2026-08-31) is unchanged from the prior briefing but not freshly reconfirmed this week.
Sources: [Lido V3 Is Live](https://blog.lido.fi/lido-v3-is-live-modular-infrastructure-for-a-new-paradigm-of-ethereum-staking/), [Identified Node Operators](https://docs.lido.fi/run-on-lido/stvaults/node-operators-identification/), [Lido DAO governance vote, TradingView](https://www.tradingview.com/news/coindar:41d61bfc2094b:0-lido-dao-to-conclude-governance-vote-on-july-17th/), [stVaults overview](https://lido.fi/stvaults), [Lido Protocol Fee](https://lido.fi/how-lido-works/protocol-fee).

### (b) stETH APR
Best estimate: **~2.2%**, essentially unchanged from the 2026-07-13 briefing (2.22%). This week's live re-check was inconclusive: search results ranged from ~2.0% to 2.6% depending on whether the source cited gross network staking APR or net LST-holder yield (Lido applies a protocol fee that separates the two). The 2.6% figure (stakingrewards.com) appears undated/generic; a KuCoin industry piece referencing late-May 2026 data (~2.78% gross network APR, 2.0–2.2% net LST yield) is more consistent with the prior week's figure and was weighted more heavily.
Sources: [stakingrewards.com — Lido Staked Ether](https://www.stakingrewards.com/asset/staked-ether), [Lido APR and Rewards Calculator](https://lido.fi/how-lido-works/apr-and-rewards-calculator), [DeFiLlama — STETH yields](https://defillama.com/yields/pool/747c1d2a-c668-4682-b9f9-296708a3dd90) (referenced via search, direct fetch blocked).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance, `app.aave.com/markets/?marketName=proto_lido_v3`):** WETH borrow APY ~2.03%, essentially flat vs. the prior 2.00%. wstETH supply-side figures returned inconsistent (<0.01% to ~0.44%) and are not treated as reliable this week.
**Morpho:** data unreliable — the only figures surfaced were sized far too small (~$206k total borrow) to be the Ethereum mainnet wstETH/WETH market and are likely a different chain's market; no usable read obtained.
Net read: stETH APR (~2.2%) minus Aave Prime WETH borrow (~2.0%) ≈ **0.2pp spread**, well below the 0.5pp re-entry trigger. Confidence is lower than a typical week (see Data Gaps).
Sources: [Aave Prime Market](https://app.aave.com/markets/?marketName=proto_lido_v3), [Aavescan — wstETH on Ethereum V3](https://aavescan.com/ethereum-v3/wsteth) (referenced via search, direct fetch blocked).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** ~2.0–2.5% APY, no active CRV/LDO incentives (Lido ended LDO mining on this pool post-Shapella). TVL has shrunk to ~$35M (per a June 2025 Curve governance proposal to cut the pool's fee to attract flow — it currently captures ~0% of CowSwap volume). A ~$6.55M entry would represent ~19% of pool TVL — **not viable at this position size** regardless of the rate.
**Balancer:** **disqualified.** Balancer V2 was exploited for ~$110–128M in November 2025; Balancer Labs announced a corporate wind-down in March 2026, BAL emissions have ended, and protocol TVL is down ~95% from peak (~$3.5B → ~$157M). No reliable current APY exists and counterparty risk is elevated.
Sources: [DeFiLlama — Curve ETH-STETH pool](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce), [Curve governance — stETH/ETH pool fee cut](https://gov.curve.finance/t/lower-steth-eth-pool-35m-tvl-one-fee-from-0-04-to-0-008/10669), [Lido — Concerning stETH Liquidity](https://blog.lido.fi/concerning-steth-liquidity/), [CoinDesk — Balancer exploit](https://www.coindesk.com/markets/2025/11/03/balancer-hit-by-apparent-exploit-as-usd70m-in-crypto-moves-to-new-wallets), [CoinDesk — Balancer Labs wind-down](https://www.coindesk.com/tech/2026/03/24/balancer-labs-will-shut-down-as-corporate-entity-became-a-liability-after-usd110-million-exploit).

### (e) Pendle PT-stETH
**Data gap — could not be verified this week.** `api-v2.pendle.finance` and `yields.llama.fi` are blocked by this session's egress policy (403 on CONNECT, confirmed at the proxy level, not a retry-able failure), and WebSearch results for Pendle's stETH/wstETH markets were internally inconsistent (fixed APY cited variously as 2.71%, 3.5%, and 4–5%; liquidity cited variously as ~$10M and ~$800M for what was described as the same pool — an ~80x discrepancy). The most internally consistent read suggests total Pendle stETH/wstETH-related liquidity across all maturities may be closer to ~$10M, which would not support a $6.55M position regardless of the fixed rate, but this is not confirmed. **Recommend a manual check of `app.pendle.finance/trade/markets` before relying on this trigger.**

### (f) Curated ("GGV-class") vaults
Identified a specific match: Lido's own **"GG Vault" (GGV)**, launched 2026-09-03 (per source dating) with Veda Labs, auto-allocating ETH/WETH/stETH/wstETH across ~7 protocols (Aave, Morpho, Euler, Balancer, Gearbox, Fluid, Uniswap). It is open to deposits at `stake.lido.fi/earn/ggv/deposit`. Reported net APY ~**5%** (after a 10% performance fee), TVL >40,000 ETH (~$175M), per reporting dated November 2025 — **this figure is stale (~8 months old) and could not be reconfirmed live this week** (stake.lido.fi returned 403 to fetch). Nominally this clears the 4% curated-vault trigger, but see the ranking section below for why it is not recommended despite that.
Sources: [Lido blog — GGV overview](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/), [Cryptonomist — GGV launch](https://en.cryptonomist.ch/2025/09/04/lido-launches-gg-vault-automated-defi-yields-on-eth-weth-steth-and-wsteth-in-the-earn-tab/), [Dune — Lido GGV dashboard](https://dune.com/lido/lido-ggv).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** established AVSs (e.g. EigenDA) add roughly 1–2% APY on top of base ETH staking (blended total commonly cited 3.8–6% APY); TVL >$15B (Feb 2026). This has not been scoped operationally for this vault's own 0x02 validators (native EigenLayer restaking for Lido-managed stVault validators vs. requiring a separate LRT wrapper is unconfirmed), and would add AVS slashing risk without touching the stETH liability or health factor at all — it is a different lever from the mint-deployment strategies below, not a substitute for them.
**Symbiotic:** pivoted from restaking to "collateral markets" with Core V2 (launched 2026-07-01); no longer offers a standardized, comparable AVS-restaking APY. A cited example (an RWA-focused "instant liquidity" vault at 8.9% APY on $10M) is not representative of a typical established-AVS yield and should not be used as a benchmark.
Sources: [EigenLayer restaking & AVS rewards](https://medium.com/@eigenlayer2/eigenlayer-staking-eth-restaking-and-avs-rewards-26d99a682a2e), [BlockEden — EigenLayer TVL](https://blockeden.xyz/blog/2026/02/08/eigenlayer-restaking-empire-liquid-restaking-ethereum/), [The Block — Symbiotic Core V2 pivot](https://www.theblock.co/post/406862/symbiotic-officially-pivots-to-collateral-markets-with-core-v2-launch), [GlobeNewswire — Symbiotic Core V2](https://www.globenewswire.com/news-release/2026/07/01/3320589/0/en/symbiotic-launches-core-v2-bringing-shared-collateral-to-insurance-credit-and-tokenized-assets.html).

### (h) MEV relay market share
No material change this week. 24h snapshot (2026-07-19, relayscan.io): ultrasound.money 26.0%, Titan 21.5%, bloXroute Max Profit 21.0%, bloXroute Regulated 17.9%, Aestus 7.5%, Flashbots 3.3%, Agnostic 1.9%. The vault's four registered relays together cover ~85% of payloads. There is a slow multi-month drift of share from ultrasound toward bloXroute (e.g. ultrasound was ~34% in April 2026), but nothing moved specifically this week, and no relay shutdowns, launches, or reliability/censorship incidents were reported. No relay-selection change is warranted.
Sources: [relayscan.io](https://www.relayscan.io/), [MEV Watch](https://www.mevwatch.info/), [The Block — MEV-Boost relay share](https://www.theblock.co/data/on-chain-metrics/ethereum/percentage-of-blocks-proposed-each-mev-boost-relay).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint (~$6.55M), currently held outside the vault.

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | ~+93 ETH/yr *nominal*, computed as (5% reported net APY − 2.36% cost of carry) × 3,509.2 stETH — **not verified this week, ~8-month-old rate source** | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger but fails the 150% HF floor by a wide margin, and leaves the position sitting near the 100% forced-rebalance line with no buffer improvement. Needs a live rate reconfirmation regardless. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread (~0.2pp) below the 0.5pp trigger | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable — $6.55M entry ≈19% of $35M pool TVL | Unchanged | Insufficient depth regardless of rate. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | Disqualified — post-exploit, protocol winding down. |
| 6 | Pendle PT-stETH | Unverified this week; best evidence suggests total relevant liquidity ~$10M, insufficient for this position | Unchanged | Data gap — recommend manual check. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable this week; Symbiotic no longer offers a comparable figure; EigenLayer ~1–2% incremental not confirmed operationally available for this vault's validators | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all — every external deployment leaves the position sitting near its current ~103% health factor, close to the 100% forced-rebalance threshold, regardless of the yield earned externally.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-07-20) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Best estimate ~0.2pp. Confidence lower than usual this week — see Data Gaps; worth a precise re-check next week given some source dispersion. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV (~5% net, per Nov 2025 data) but **unconfirmed this week** and, per the ranking above, disqualified separately on the 150% HF-floor test even if the rate holds. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Not met / unverifiable.** Best available (low-confidence) evidence points to total relevant liquidity near ~$10M, well short of the depth needed for a $6.55M position. |

---

## Data Gaps & Methodology Notes

This week's research was materially constrained by egress restrictions in the research environment:

- `yields.llama.fi` and `api-v2.pendle.finance` were **blocked by organizational egress policy** (403 on CONNECT at the proxy level — confirmed via proxy diagnostics, not a transient failure).
- Direct fetches to `stake.lido.fi`, `app.aave.com`, `app.morpho.org`, `curve.finance`, `balancer.fi`, `docs.lido.fi`, and `research.lido.fi` also returned 403 in this session.
- As a result, most figures in this report are drawn from **search-engine result snippets rather than live dashboard pulls**, which are less reliably dated and in a few cases (Pendle liquidity, Morpho borrow size) were internally inconsistent enough to be discarded rather than reported as fact.
- **Recommend for next week:** a manual (browser-based) spot-check of `stake.lido.fi` (stETH APR, GGV live APY), `app.aave.com/markets/?marketName=proto_lido_v3` (wstETH/WETH rates), and `app.pendle.finance/trade/markets` (PT-stETH fixed yields and depth) to firm up the numbers this report had to estimate from indirect sources.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it is the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield (+14–24 ETH/yr) after the 5% operator fee and cost of carry. Monitor the stETH-APR-minus-borrow spread and the Lido GGV live rate closely — both are the most likely triggers to change the recommendation, and neither is currently confirmed with high confidence.
