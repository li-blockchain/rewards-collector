# stVault Weekly Strategy Report — 2026-10-05

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-09-28 report:

- **Balancer wind-down vote: resolved — passed.** The Snapshot vote (quorum 5M BAL) closed September 29, 2026. BAL holders approved the wind-down and rejected the alternative of launching a forked chain. The formal wind-down phase begins November 2026, with low-risk permissions revoked between November and December 2026; treasury distribution (~$9M) to BAL holders is still slated to begin around May 2027 and run roughly six months. This closes out the "pending vote" caveat carried for the last two weeks. **No ranking change** — Balancer stETH/wstETH pools were already disqualified on the standing post-exploit basis before the vote, and remain disqualified now that the wind-down is confirmed.
- **Curve stETH/ETH pool TVL: discrepancy substantially resolved, upward — but conclusion unchanged.** Two independently-sourced reads this week converge on a materially larger pool than the ~$35M figure this report had carried forward for months: a Curve-Monitor-style read of **$110.68M TVL** and a DefiLlama-style read of **$107.87M TVL / 1.26% supply APY**. This corroborates, rather than contradicts, last week's flagged $110.68M figure, and the ~$35M figure now looks like it was a stale or mislabeled reference (the governance-thread title it came from referred to "the 35m tvl one," which in hindsight likely named the pool to distinguish it from a different pool, not described this pool's current size). **Re-sized against ~$108–110M, a ~$9.39M entry is roughly 8–9% of pool TVL — a much weaker depth objection than the ~27% implied by $35M.** This does **not** change the ranking, however: the pool's base APR (~1.2–1.3%, consistent across this week's and last week's reads) sits well below the vault's ~2.34% cost of carry, so the strategy fails on yield alone regardless of depth. Depth is no longer the binding constraint for this venue, but the strategy is still not viable.
- **EigenLayer TVL: a sentiment flag from recent weeks is now confirmed with hard numbers — a material sector-wide decline, not a lever for this vault either way.** Multiple sources this week converge on EigenLayer TVL now standing at approximately **$4.67B**, down from the ~$18.37B figure this report carried through mid-September. The decline traces to the **Kelp DAO exploit of April 18, 2026** — a ~$292M cross-chain bridge compromise (a 1-of-1 LayerZero DVN configuration let an attacker forge rsETH) that cascaded into Aave, where the attacker borrowed against the forged collateral, triggering roughly $5.4B in withdrawals within 46 minutes and 100% WETH utilization at the time. Reported TVL trajectory: ~$22.06B (Aug 2025 peak) → ~$7B (April 2026, immediately post-exploit) → ~$5.10B (September 2026) → ~$4.67B (current). This is a real, large, and now well-sourced decline — not the same low-quality source flagged in prior weeks — but it remains **not a lever for this vault's own (non-restaked) validators**; it mainly confirms that the restaking sector's contraction (previously only visible as an ether.fi sentiment signal) is real and significant. No change to this strategy's "not actionable without further scoping" status.
- **Aave Prime/Lido-instance WETH borrow rate: a fresher, slightly less favorable figure found.** This week's search surfaced a specific instance-relevant read: WETH borrow rate **3.30%**, offset by a wstETH supply rate of **1.10%**, for a **net borrowing cost of ~2.20%** — following a Chaos Labs Risk Steward +40bps adjustment to the WETH Slope1 parameter on the Lido instance, made (per the proposal text) "as a result of consistently elevated wstETH supply rates." This is a firmer, more specific figure than the ~2.14% carried forward for the past seven weeks, though the exact adjustment date could not be independently pinned down this week (`governance.aave.com` and `app.aave.com` both blocked on direct fetch again). Against the carried stETH APR modeling assumption (~2.2%), the spread narrows to roughly **0.0pp**, slightly below last week's ~0.06pp read. **Trigger status unchanged (not met)**, now on a somewhat firmer and less favorable number.
- **ETH/USD price: a data-quality problem this week, not a market event.** Searches for the current ETH/USD price returned wildly inconsistent figures — individual reads ranged from ~$2,400 to ~$4,200 across different snippets and apparent timestamps, with no single reliable live quote obtainable (several hits appear to be speculative "price prediction" pages rather than live tickers). This is a sourcing problem, not a claimed 50%+ weekly price move. **USD conversions are omitted from this week's report** rather than published on an unreliable figure; all comparisons below stay ETH-denominated, consistent with this report's existing methodology.
- **Lido V3 tier parameters: additional confirmed detail found, no change to the vault's own tier.** This week's search surfaced the minting-cap side of the tier table: Tier 1 — 5% Reserve Ratio / 47,500 stETH cap; Tier 2 — 6% RR / 47,000 stETH cap; Tier 3 — 9% RR / 182,000 stETH cap. This is additional confirmed detail (not previously quantified in this report), not a change — the vault's own Basic Tier 1 parameters (5% RR, 4.75% Forced Rebalance Threshold) are unaffected.
- **Infrastructure fee: no change, no fresher date confirmation.** No new information moved this item. The fee was last directly confirmed by the client as 0% as of 2026-09-28; this automated refresh did not seek a fresh client confirmation (no client-contact channel is part of this task). Search this week again surfaced only the same "extended through October 31, 2026" claim (via CryptoBriefing, still not independently verified against a primary Lido source — `research.lido.fi` and `cryptobriefing.com` both blocked on direct fetch again). One new piece of context: a separate source indicates the waiver's original expiry was **March 31, 2026**, consistent with a pattern of serial extensions (March 31 → August 31 → October 31) rather than a single standing deadline. With October 31 now 26 days away, this remains a low-urgency watch item worth a direct recheck once that date passes.
- **stETH APR, GGV, Morpho, MEV relay shares: no material change.** stETH APR modeling assumption stays at ~2.2% net-to-holders (this week's search only surfaced generic, likely non-Lido-specific "3–4%" marketing copy, not treated as an update). GGV: no fresher dated figure found; the recurring $98.6M TVL / 7.1% APY figure is carried forward for a tenth week (a separate $175M/~5% APY figure also found this week is dated November 2025 — eleven months stale — and is not a current-week update). Morpho: no Aave-Prime/Lido-instance-specific loop figure found; carried tracker unchanged. MEV relay shares: no fresher-than-September-20 snapshot found (an "August 2026" relay-share figure surfaced this week is older than the September 20 figures already on file, so the September 20 reads remain the most recent and are carried forward unchanged).
- **Pendle PT-stETH: still unverified, and the specific pool reference this report has carried is now flagged as likely stale by maturity, not just by sourcing.** The ~$8M-liquidity "September 2026 maturity" pool this report has cited for months has, by calendar, likely already matured and closed as of this report (2026-10-05). No fresher maturity-specific figures were found this week; `app.pendle.finance` remains blocked. **The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 is now outstanding for thirteen consecutive weeks**, and is increasingly the only way to get a currently-valid (non-expired) maturity, liquidity, and yield reading for this venue.
- **No change** to the confirmed Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold), the 5% node-operator fee, or the vault's relay registrations.
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was not obtained this week — see Data Gaps). **USD conversions are omitted this week** due to unreliable ETH/USD price reads (see Changes Since Last Week); all figures are ETH-denominated, consistent with this report's methodology, under which the underlying ETH/stETH position figures are unaffected by any price move.

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) |
| stETH liability | 3,509.2 stETH (~93% of total value) |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH; a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below.

---

## Vault Parameters & Fee Structure

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold, 47,500 stETH minting cap (the minting-cap figure is newly confirmed this week via search; not previously quantified in this report). For reference, Tier 2 is 6% RR / 47,000 stETH cap and Tier 3 is 9% RR / 182,000 stETH cap — this vault remains on Tier 1.
- **Infrastructure fee:** last directly confirmed by the client at **0%** as of 2026-09-28; no fresher client confirmation sought this week. Search continues to point to an extension through October 31, 2026 (unconfirmed against a primary Lido source); the original waiver deadline was reportedly March 31, 2026, consistent with a pattern of serial extensions. All figures below are modeled on the last-confirmed 0% fee.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, unchanged from prior reports.

---

## Rate Environment (as of 2026-10-05)

### (a) Lido DAO / stVault governance and fees
No DAO governance action changing stVault tier parameters was found this week. New (non-changing) detail surfaced on the tier table's minting caps (Tier 1: 47,500 stETH; Tier 2: 47,000 stETH; Tier 3: 182,000 stETH) and on the Early Adopters waiver's original March 31, 2026 deadline, consistent with the serial-extension pattern already tracked. No primary-source confirmation of the October 31, 2026 expiry was obtained this week.
Sources: [Default risk assessment framework and fees parameters for Lido V3 (stVaults) — Lido Governance](https://research.lido.fi/t/default-risk-assessment-framework-and-fees-parameters-for-lido-v3-stvaults/10504) (referenced via search, direct fetch blocked again), [Lido V3 introduces tier-based staking credit rating — 4Pillars](https://research.4pillars.io/en/research/lido-v3-introduces-tier-based-staking-credit-rating) (new this week, source of the tier-cap table and March 31 deadline detail), [Lido extends 0% infrastructure fee for stVaults until October 31 — CryptoBriefing](https://cryptobriefing.com/lido-stvaults-zero-fee-extension-october/) (carried, direct fetch blocked again).

### (b) stETH APR
Best estimate for modeling: **~2.2% net-to-holders**, unchanged. This week's search surfaced only generic "3–4%" marketing copy that does not read as Lido-specific or freshly dated; not treated as an update. Modeling assumption stays anchored to the vault's own confirmed dashboard figure (2.24% net).
Sources: generic staking-yield marketing pages (not cited individually; treated as low-confidence and not used to move the modeling assumption), [Ethereum Staking Weekly Report, September 7, 2026 — Bitget](https://www.bitget.com/news/detail/12560605798507) (carried).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** a fresher, specific figure surfaced this week — WETH borrow rate **3.30%**, offset by wstETH supply rate **1.10%**, net borrowing cost **~2.20%** — following a reported Chaos Labs +40bps Slope1 adjustment on the Lido instance. Exact adjustment date not independently confirmed this week (`governance.aave.com`, `app.aave.com`, `aavescan.com` all blocked on direct fetch). This supersedes the ~2.14% figure carried for the prior seven weeks. Against stETH APR (~2.2%), the implied spread is now **~0.0pp**, slightly worse than last week's ~0.06pp.
**Morpho:** no Aave-Prime/Lido-instance-specific loop figure found this week; unrelated Morpho market quotes (USDC/wstETH, USDT/wstETH, a Flagship ETH vault citing ~10% APY) surfaced but are not comparable to the loop this report tracks. Carried tracker (dated August 12, 2026) unchanged.
Net read: re-entry trigger **not met**, on a firmer and marginally less favorable spread than last week.
Sources: [Risk Stewards — August 2026 WETH Interest Rate Adjustment — Aave Governance](https://governance.aave.com/t/risk-stewards-august-2026-weth-interest-rate-adjustment/25535) (referenced via search, direct fetch blocked; this specific proposal covers Ethereum Core/Arbitrum/Optimism, not the Prime/Lido instance, and is cited for context only), [ARFC Prime Instance — wstETH Borrow Rate + rsETH Supply Cap Update — Aave Governance](https://governance.aave.com/t/arfc-prime-instance-wsteth-borrow-rate-rseth-supply-cap-update/20644) (referenced via search, direct fetch blocked; source of the 3.30%/1.10%/2.20% figures), [Failed to understand source of yield for Flagship ETH vaults — Morpho Forum](https://forum.morpho.org/t/failed-to-understand-source-of-yield-for-flagship-eth-vaults/1236) (new this week, context only).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** the TVL discrepancy flagged last week is substantially resolved upward. Two independent reads this week converge on **~$108–110M TVL** (a Curve-Monitor-style $110.68M and a DefiLlama-style $107.87M / 1.26% supply APY), corroborating last week's flagged figure and superseding the ~$35M figure this report had carried forward for months (likely a stale or mislabeled reference). Re-sized against ~$108–110M, a ~$9.39M entry is ~8–9% of pool TVL — a materially weaker depth objection than before. **Conclusion still not viable, but now on yield rather than depth**: the pool's ~1.2–1.3% base APR is well below the vault's ~2.34% cost of carry.
**Balancer:** the wind-down Snapshot vote closed September 29, 2026 and **passed** (quorum 5M BAL met); the alternative forked-chain proposal was rejected. Wind-down phase begins November 2026 (permission revocation November–December 2026); treasury distribution (~$9M) to BAL holders is still slated for ~May 2027 over six months. Still **disqualified**, now on a confirmed rather than pending basis.
Sources: [Yield Chart - ETH-STETH(Curve DEX) — DefiLlama](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (referenced via search, direct fetch blocked again), [Curve.finance :: Pools](https://www.curve.finance/dex/ethereum/pools) and [Pools — Curve Monitor](https://curvemonitor.com/platform/pools) (referenced via search, direct fetch blocked), [Lower stETH/ETH Pool (35m tvl one) Fee from 0.04% to 0.008% — Curve Governance](https://gov.curve.finance/t/lower-steth-eth-pool-35m-tvl-one-fee-from-0-04-to-0-008/10669) (carried; now believed stale/mislabeled rather than current), [balancer aprueba su liquidacion y descarta crear una cadena bifurcada — DiarioBitcoin](https://www.diariobitcoin.com/criptomonedas/balancer-aprueba-su-liquidacion-y-descarta-crear-una-cadena-bifurcada/) (new this week, vote-result confirmation), [docs.balancer.fi governance / Snapshot process](https://docs.balancer.fi/concepts/governance/snapshot.html) (new this week, context).

### (e) Pendle PT-stETH
**Still not independently verifiable**, and the specific pool this report has cited (a ~$8M-liquidity "September 2026 maturity") has likely already matured and closed as of this report's date, making it stale by calendar as well as by sourcing. `app.pendle.finance` remains blocked. No fresher maturity-specific figures were found this week. General commentary continues to describe PT-stETH fixed yields in roughly a 4–6% range depending on maturity, with Pendle's liquidity fragmented across 50+ maturity markets and the top 5 holding ~80% of TVL — consistent with, but not a refresh of, prior weeks' figures. **The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 is now outstanding for thirteen consecutive weeks**, and is the only way to obtain a currently-valid reading now that the previously-cited maturity has likely expired.
Sources: [Pendle Finance V2 — wikidocs](https://wikidocs.net/326676) (new this week, context), [What is Pendle Finance: The Complete 2026 Guide — EarnPark](https://earnpark.com/en/posts/what-is-pendle-finance-the-complete-2026-guide-to-yield-tokenisation-pt-yt-mechanics-and-boros/) (new this week, context).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked again this week). The recurring **$98.6M TVL / 7.1% APY** figure is carried forward for a tenth consecutive week; no fresher-dated figure was found. A separate figure (~$175M TVL, ~5% APY, ~40,000 ETH deposited) also surfaced this week but is dated November 2025 — eleven months stale — and is not treated as a current update. GGV remains disqualified on the health-factor test regardless of rate.
Sources: [Lido Deposit | Earn stETH/WETH Vault Rewards with GGV — lido.today](https://lido.today/) (carried, source of the $98.6M/7.1% figure), [Veda Golden Goose Vault — StakingRewards](https://www.stakingrewards.com/defi/0xef417fce1883c6653e7dc6af7c6f85ccde84aa09) (new this week, referenced via search only, direct fetch blocked), [docs.veda.tech vault deployments](https://docs.veda.tech/vault-deployments/lido) (new this week, context, source of the November-2025-dated figure).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** a material, now well-sourced decline. TVL is reported at approximately **$4.67B currently**, down from ~$18.37B carried through mid-September and from a peak of ~$22.06B in August 2025. The decline traces to the **Kelp DAO exploit (April 18, 2026)** — a ~$292M cross-chain bridge compromise (a 1-of-1 LayerZero DVN configuration allowed forged rsETH) whose attacker borrowed against the forged collateral on Aave, triggering ~$5.4B in withdrawals within 46 minutes and 100% WETH-pool utilization at the time. Reported trajectory: ~$22.06B (Aug 2025) → ~$7B (April 2026) → ~$5.10B (Sept 2026) → ~$4.67B (current). This corroborates, with hard numbers, the ether.fi weETH sentiment signal flagged in prior weeks. **Not a lever for this vault's own (non-restaked) validators.**
**Symbiotic:** no new TVL figure obtained this week beyond the already-flagged Core V2 "collateral markets" pivot (July 1, 2026); search this week places the broader restaking-adjacent sector at "$19B+" without a Symbiotic-specific current figure. Not operationally scoped for this vault either way.
Sources: [What Is KelpDAO? How Its $292M Hack Shook the Crypto Market in 2026 — KuCoin](https://www.kucoin.com/blog/vn-kelpdao-hack-2026-rseth-exploit-analysis) (new this week), [Aave Lost $6.6B in TVL After Kelp Exploit — Phemex](https://phemex.com/blogs/aave-lost-deposits-bad-debt-kelp-fallout-defi-lending) (new this week), [Kelp DAO Bridge Drained for $292M in 2026's Biggest DeFi Hack — CryptoTimes](https://www.cryptotimes.io/2026/04/19/kelp-dao-bridge-drained-for-292m-in-2026s-biggest-defi-hack/) (new this week), [Symbiotic officially pivots to collateral markets with Core V2 launch — The Block](https://www.theblock.co/post/406862/symbiotic-officially-pivots-to-collateral-markets-with-core-v2-launch) (carried).

### (h) MEV relay market share
**No fresher-than-last-week snapshot.** The most recent dated figures remain those "as of September 20, 2026" per relayscan.io (direct fetch blocked again this week): ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%. An "August 2026" relay-share figure (Ultra Sound 35.1%, Titan 29.8%, bloXroute Regulated 23.8%, Aestus 7.2%) also surfaced this week but is older than the September 20 reads already on file, so it is not treated as an update. No relay-registration action is indicated — the vault remains registered with ultrasound.money, Titan, and both bloXroute relays, all of which continue to show meaningful share.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV boost relay censorship shift 2026 — shattered.io](https://shattered.io/mev-boost-relay-censorship-shift-2026/) (new this week, source of the August 2026 figures, superseded by the already-carried September 20 reads).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint, currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated; USD conversions are omitted this week (see Changes Since Last Week).

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the single figure carried forward for ten weeks — still not independently verified or clearly dated | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger but fails the 150% HF floor by a wide margin. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread now read at **~0.0pp** (stETH ~2.2% vs. a fresher Aave Prime/Lido-instance WETH net borrowing cost of ~2.20%), slightly worse than last week's ~0.06pp | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | **Depth objection substantially resolved this week** (pool now read at ~$108–110M TVL, not ~$35M; a ~$9.39M entry is only ~8–9% of pool TVL) — but still not viable: the pool's ~1.2–1.3% base APR sits well below the ~2.34% cost of carry | Unchanged | Not viable on yield, independent of depth. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | **Disqualified, now on a confirmed basis** — the post-exploit wind-down vote passed September 29, 2026. |
| 6 | Pendle PT-stETH | Unverified; the specific pool previously cited has likely matured and expired by now | Unchanged | Data gap — manual check outstanding for thirteen weeks, now more urgent since the cited maturity is stale. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable as a lever for this vault; EigenLayer TVL has fallen materially (~$4.67B, down from ~$18.37B, following the April 2026 Kelp DAO exploit) | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all. Two items that had been open data-quality questions — the Curve TVL discrepancy and the Balancer vote outcome — are both resolved this week, and neither changes the ranking.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-10-05) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Now read at ~0.0pp (stETH ~2.2%, Aave Prime/Lido-instance WETH net borrowing cost ~2.20% on a fresher figure this week), slightly worse than last week's ~0.06pp. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV at 7.1% (unchanged for ten weeks), but disqualified separately on the 150% HF-floor test regardless of rate. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (4–6% commonly cited); **depth leg still unverifiable, and the specific pool previously used to assess depth has likely expired.** Manual check of `app.pendle.finance/trade/markets` remains outstanding after thirteen weeks and is now the only way to get a currently-valid reading. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `defillama.com`, `research.lido.fi`, `cryptobriefing.com`, `governance.aave.com`, `app.aave.com`, `aavescan.com`, `blog.vaults.fyi`, `www.relayscan.io`, `stake.lido.fi`, and `www.stakingrewards.com` all returned egress-blocked errors on direct fetch this week.
- As a result, this week's figures — including the Aave WETH borrow rate, the GGV figure, the Curve TVL figures, the MEV relay shares, and the EigenLayer TVL trajectory — are drawn from search-engine snippets rather than live dashboard or primary-source pulls, with the same reliability caveats as prior weeks.
- **Resolved this week — Curve TVL discrepancy:** the ~$35M figure carried for months is now believed stale or mislabeled; two independent reads this week converge on ~$108–110M. Does not change the ranking (fails on yield instead of depth).
- **Resolved this week — Balancer wind-down vote:** confirmed passed September 29, 2026. No ranking change.
- **New this week — ETH/USD price data quality:** search results for the current ETH/USD price were inconsistent across sources (roughly $2,400–$4,200), with no reliable single live quote obtainable. USD conversions are omitted from this report as a result; this is a sourcing gap, not a claimed market move, and does not affect any ETH-denominated figure in this report.
- **New this week — Pendle maturity staleness:** the ~$8M-liquidity "September 2026 maturity" pool this report has cited for months has likely already matured and closed by this report's date (2026-10-05), on top of the pre-existing sourcing gap. The outstanding manual check of `app.pendle.finance/trade/markets` is now the only way to get a currently-valid reading.
- **Carried forward — content-quality caution:** the low-quality/gamed-looking GitHub "market-intelligence-reports" source flagged in prior weeks was not encountered again this week; no new instance to report.
- **Carried forward:** no fresh client reconfirmation of the infrastructure fee was sought this week (this refresh ran without a client-contact step); the last confirmed status (0%, as of 2026-09-28) is carried forward. A manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY/TVL), `app.aave.com/markets/?marketName=proto_lido_v3` (Prime/Lido-instance WETH/wstETH rate), and `app.pendle.finance/trade/markets` (current, non-expired PT-stETH maturities, yields, and depth) would materially firm up several figures this report has had to estimate indirectly.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to the recommended strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry.

**Two long-standing data-quality questions were resolved this week, with no change to the ranking:** the Curve stETH/ETH pool's TVL discrepancy is now better supported at ~$108–110M (versus the ~$35M figure carried for months), but the venue remains not viable because its yield, not its depth, falls short of the cost of carry. The Balancer DAO wind-down vote closed and passed on September 29, 2026, confirming — rather than changing — that venue's disqualified status.

**One item moved against the incumbent's margin slightly, without changing the recommendation:** a fresher Aave Prime/Lido-instance WETH borrow read (net cost ~2.20%, versus ~2.14% carried for seven weeks) narrows the stETH-minus-borrow spread to roughly 0.0pp, still well short of the 0.5pp re-entry trigger.

**One item is a hard, well-sourced confirmation of a trend already flagged on softer evidence:** EigenLayer's restaking TVL has fallen to roughly $4.67B (from ~$18.37B) following the April 2026 Kelp DAO exploit — a real and material sector contraction, though still not a lever for this vault's own non-restaked validators.

**Outstanding items for next week:** a manual check of `app.pendle.finance/trade/markets` is increasingly overdue (thirteen weeks) and now doubly needed, since the specific maturity this report has cited for Pendle's depth assessment has likely already expired. The infrastructure-fee waiver's alternate October 31, 2026 expiry date is now 26 days away and worth a direct recheck once it passes, regardless of whether a fresh client confirmation is obtained before then.
