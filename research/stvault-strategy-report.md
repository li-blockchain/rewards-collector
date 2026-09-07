# stVault Weekly Strategy Report — 2026-09-07

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-08-31 report:

- **Infrastructure-fee waiver: now one full week past its confirmed 2026-08-31 expiry, still unconfirmed against a primary source — and this week's search signal is actually less clear than last week's, not more.** Last week's report noted that search results had, for the first time, converged on "August 31, 2026" as the campaign's expiration — matching the confirmed vault fact this model has used throughout. This week, the same style of search returned the **older** "March 31, 2026" and "June 30, 2026" figures again, with no fresh mention of an August 31 date or any extension/reversion announcement. This looks like stale SEO content resurfacing rather than a genuine update, but it means the model has no better information this week than last week on whether the 0% infrastructure fee is still in effect. `docs.lido.fi`, `blog.lido.fi`, `stake.lido.fi`, `lido.fi`, and `research.lido.fi` direct fetches were all blocked again (the same egress restriction as nine straight prior weeks). **Figures below continue to assume the 0% fee holds** (the standing confirmed fact), but this is now a full week of unverified assumption past the fee's own confirmed end date. This remains the report's top item to verify directly (vault dashboard or a primary Lido source) before the next refresh. See the Recommendation section for a sensitivity note on what a reversion to 1% would mean.
- **Aave Prime (Lido instance) WETH borrow rate — still no fresher instance-specific figure.** `app.aave.com`, `aavescan.com`, and `governance.aave.com` direct fetches were all blocked again. Search this week surfaced only older (2024-dated) Aave governance ARFC threads on wstETH/WETH Lido-instance rate curves — no 2026-dated proposal or confirmed current rate. The last Prime/Lido-instance-relevant read (WETH borrow ≈2.14%, now carried forward four weeks) is retained unchanged. Against stETH APR (~2.2%), the implied spread stays **~0.06pp**, well under the 0.5pp re-entry trigger.
- **Morpho: no reliable current loop-APY figure.** Search this week returned a different data point than prior weeks — a Morpho wstETH/weETH market (a different, though related, collateral pair) showing net APY of 0.07%–0.65% as of an August 2026 comparison — plus the same stale January–April 2025 backtest (~6.2% leveraged vs. 3.1% unleveraged) already noted in prior reports. Neither is a current, position-specific wstETH/ETH loop APY. Not treated as authoritative, unchanged from prior weeks.
- **Curve stETH/ETH pool: the ~$35M TVL carried-forward figure received independent corroboration this week, though still undated.** A Curve DAO governance forum thread this week was titled "Lower stETH/ETH Pool (35m tvl one) Fee from 0.04% to 0.008%" — an independent (non-search-engine-summary) source using the same ~$35M figure this report has carried forward for nine weeks. This raises confidence in the figure's rough accuracy but does not supply a current, dated TVL. Conclusion unchanged: **not viable at this position size** (~$8.7M entry against a ~$35M pool).
- **Balancer: LST-pool retention confirmed with slightly firmer language, still not pool-specific.** This week's search explicitly lists "stables/LST pools" as one of the narrower set of product lines Balancer is retaining post-restructuring (alongside reCLAMM, LBPs, weighted pools, fewer EVM chains) — consistent with, and marginally more specific than, last week's read. Still no confirmation this specific stETH/wstETH pool is included or currently viable. **Remains disqualified** on the standing corporate-wind-down basis; not acted on this week.
- **Pendle PT-stETH: no change.** 4–5% fixed-yield commentary is unchanged. No pool-specific depth figure for a ~$8.7M position was obtained. The manual `app.pendle.finance/trade/markets` check recommended since 2026-07-13 is now outstanding for **nine** consecutive weeks.
- **GGV: figure unchanged for a sixth week.** Search again returned the recurring **$98.6M TVL / 7.1% APY** figure (unchanged since the week of 2026-08-03), alongside older stale figures (~$175M TVL / ~5% APY from November 2025, $105M / ~10.6% from September 2025) that this report has already discounted as outdated. No fresh "as of" date obtained. Still fails the 150% health-factor floor test regardless of rate.
- **EigenLayer: TVL citations widened further this week rather than converging.** A new figure this week put EigenLayer TVL at **~$5B** ("by mid-2026"), which sits well below both last week's $12.9B figure and the $15.3–19.7B range cited in recent weeks. This is now a roughly 4x spread across sources on the same metric — the cross-source data-quality flag noted for several weeks running is, if anything, getting worse rather than resolving. Base + AVS yield commentary (4–7%, largely EIGEN-emission-driven per this week's search) is unchanged. Not an actionable lever for this vault regardless.
- **Symbiotic: similar bifurcation as prior weeks, not a new development.** Search this week returned both a ~$329M figure (DeFiLlama base-layer-only methodology, explicitly excluding LRT-routed ETH) and a ~$1.6–1.7B figure (broader comparison methodology, consistent with the figure this report has carried forward). No change to the carried-forward ~$1.6–1.7B figure or the "not operationally scoped for this vault's validators" conclusion.
- **MEV relay market share: search-engine snapshot is stale (identical to last week's figures), but one independent signal points to a real trend worth watching.** The default relay-share search again returned the exact same numbers already used in last week's report (ultrasound.money 35.26%, Titan 27.99%, bloXroute Regulated 25.40%, Aestus 6.95%, Flashbots 2.39%, Agnostic 1.00%, labeled "as of 2026-08-31") — this looks like a cached/unrefreshed search result rather than a new pull. Separately, a relay-market commentary post from a known MEV researcher (via X/Twitter, undated precisely but recent) states that Titan "has steadily gained share over recent months, largely at the expense of bloxroute and ultrasoundmoney," attributed to vertical integration. This is directional and unquantified, but it is a new, non-recurring signal — worth a direct `relayscan.io` check next week to see whether it shows up in the hard numbers. No relay-registration action is indicated this week. (A separate search hit about "a major MEV relay shutting down, four entities controlling 90% of blocks" was checked and confirmed to be a stale 2023 Blocknative story re-surfacing in search results, not a 2026 event — discarded.)
- **stETH APR: no change to modeling.** This week's search again returned a live 2.22% APY figure (stakingrewards.com, via search snippet; direct fetch blocked) plus a ~2.4% network-wide APR context figure, consistent with the ~2.2% modeling assumption and the vault's own confirmed figures (2.24% dashboard net APR, ~2.6% gross consensus yield).
- **No change** to the confirmed Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold) or the 5% node-operator fee. No new DAO governance action on stVault tier parameters or fees was found this week (search this week returned only older, previously-seen background material on the general stVaults fee/tier framework, nothing dated to this week).
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week. The recommendation's economics remain contingent on the infrastructure-fee waiver, whose confirmed expiry is now a full week in the past with no primary-source resolution — see Recommendation for a sensitivity note.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was not obtained this week — see Data Gaps). $ conversions below use today's approximate ETH price (~$2,490, 2026-09-07, essentially flat vs. ~$2,485 on 2026-08-31); the underlying ETH/stETH figures are unchanged from the 2026-07-13 pull.

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) ≈ $9.44M at ~$2,490/ETH (2026-09-07) |
| stETH liability | 3,509.2 stETH (~93% of total value) ≈ $8.74M |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH (~$0.27M at current price); a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH, ~$8.74M at current ETH price) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below. The ETH price move this week (~$2,485 → ~$2,490) is a minor market/USD-translation effect only; it does not change the underlying ETH-denominated position, health factor, or ETH/yr yield comparisons, which is why the ranked comparison below is expressed in ETH/yr terms throughout.

---

## Vault Parameters & Fee Structure

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold (confirmed vault-specific figures; a conflicting generic docs figure noted in earlier reports could not be re-checked again this week, `docs.lido.fi` blocked).
- **Infrastructure fee:** modeled as waived under the Early Adopters campaign through the confirmed end date of **2026-08-31**, which is now **one week in the past**. No primary-source confirmation of extension, renewal, or reversion to 1% has been obtained this week or last week. This is the report's single highest-priority item to verify directly — the model has no visibility into whether the fee has already reverted.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, consistent with prior reports.

---

## Rate Environment (as of 2026-09-07)

### (a) Lido DAO / stVault governance and fees
No new DAO governance action affecting stVault tier parameters or fee terms was found this week. The infrastructure-fee waiver's confirmed end date (2026-08-31) is now one week past, with search results this week actually reverting to older, less-specific cached figures ("March 31" / "June 30, 2026") rather than confirming or refuting the August 31 date. See Changes Since Last Week and Vault Parameters above.
Sources: [Lido V3 Is Live](https://blog.lido.fi/lido-v3-is-live-modular-infrastructure-for-a-new-paradigm-of-ethereum-staking/) (referenced via search, direct fetch blocked), [Default risk assessment framework and fees parameters for Lido V3 (stVaults) — Lido Governance](https://research.lido.fi/t/default-risk-assessment-framework-and-fees-parameters-for-lido-v3-stvaults/10504) (referenced via search, direct fetch blocked), [🧱 Basic stVault with optional liquidity — Lido Docs](https://docs.lido.fi/run-on-lido/stvaults/building-guides/basic-stvault/) (referenced via search, direct fetch blocked).

### (b) stETH APR
Best estimate for modeling: **~2.2%**, unchanged. This week's search again shows a live ~2.22% APY figure (stakingrewards.com) and a ~2.4% network-wide APR context figure, consistent with this vault's own confirmed figures (2.24% dashboard net APR, ~2.6% gross consensus yield).
Sources: [Lido Staked Ether (stETH) Liquid Staking — stakingrewards.com](https://www.stakingrewards.com/asset/staked-ether) (referenced via search, direct fetch blocked), [ETH Staking Statistics 2026 — coinlaw.io](https://coinlaw.io/eth-staking-statistics/).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher instance-specific figure obtained this week (`app.aave.com`, `aavescan.com`, `governance.aave.com` all blocked on direct fetch). Search surfaced only 2024-dated ARFC threads on Lido-instance rate curves, none current. The last Prime/Lido-relevant read (**WETH borrow ≈2.14%**) is carried forward for a fourth week. Against stETH APR (~2.2%), this implies a spread of **~0.06pp**, well under the 0.5pp re-entry trigger.
**Morpho:** no reliable current wstETH/ETH loop-APY figure obtained this week; search returned a related-but-different wstETH/weETH market figure (0.07%–0.65% net APY, August 2026) and the same stale January–April 2025 backtest already discounted in prior reports. Not treated as authoritative.
Net read: re-entry trigger **not met**.
Sources: [Aave — Lido case study](https://aave.com/blog/lido-aave-case-study), [Morpho WETH/wstETH Market — Fensory](https://www.fensory.com/invest/money-markets/morpho-weth-wsteth-ethereum), [Morpho APY, Fees & Interest Rates Explained (2026) — earnpark.com](https://earnpark.com/en/posts/morpho-apy-fees-interest-rates-explained-2026/).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** `defillama.com` direct fetch blocked again this week. The ~$35M TVL figure carried forward for nine weeks received independent corroboration this week via a Curve DAO governance thread titled "Lower stETH/ETH Pool (35m tvl one) Fee..." — raising confidence in the rough figure without supplying a fresh dated pull. At that TVL, a ~$8.7M entry would still represent a large fraction of pool TVL — **not viable at this position size**.
**Balancer:** still effectively **disqualified** on the standing corporate-wind-down basis. This week's search gives slightly firmer (though still not pool-specific) confirmation that "stables/LST pools" are among the narrow set of product lines retained post-restructuring. Not acted on this week.
Sources: [Lower stETH/ETH Pool (35m tvl one) Fee from 0.04% to 0.008% — Curve Governance](https://gov.curve.finance/t/lower-steth-eth-pool-35m-tvl-one-fee-from-0-04-to-0-008/10669) (referenced via search, direct fetch blocked), [Balancer Labs to shut down — CoinDesk](https://www.coindesk.com/tech/2026/03/24/balancer-labs-will-shut-down-as-corporate-entity-became-a-liability-after-usd110-million-exploit) (carried).

### (e) Pendle PT-stETH
**Still not independently verifiable.** `app.pendle.finance` remains blocked by this session's egress policy. General commentary again describes PT-stETH fixed yields in a **4–5%** range, unchanged. No pool-specific TVL/depth figure for a ~$8.7M position was obtained. **The manual check of `app.pendle.finance/trade/markets` recommended since the 2026-07-13 briefing is now outstanding for nine consecutive weeks.**
Sources: [Pendle Finance Review — Coin Bureau](https://coinbureau.com/review/pendle-finance-review), [A Complete Guide on How to Use Pendle Finance in 2026 — Coin Bureau](https://coinbureau.com/guides/how-to-use-pendle-finance) (carried).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked). Search again returned the recurring **$98.6M TVL / 7.1% APY** figure, unchanged for a sixth consecutive week, alongside older stale figures already discounted in prior reports (~$175M/~5% APY, Nov 2025; $105M/~10.6% APY, Sept 2025). No fresh "as of" date obtained. GGV remains disqualified on the health-factor test regardless of rate.
Sources: [Lido blog — GGV overview](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/) (referenced via search, direct fetch blocked), [GGV deposit — stake.lido.fi](https://stake.lido.fi/earn/ggv/deposit) (referenced via search, direct fetch blocked).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** base AVS yield still commonly cited in the ~4–7% range, mostly EIGEN-emission-driven per this week's search rather than AVS fee revenue — consistent with prior weeks' note that the fee-revenue case has not yet materialized at scale. TVL citations widened further this week: a new **~$5B** figure ("by mid-2026") sits well below both last week's $12.9B figure and the $15.3–19.7B range cited in recent weeks, roughly a 4x cross-source spread on the same metric. This data-quality flag is worsening, not resolving, over successive weeks. Slashing is enforceable (live since April 2025); risk should be modeled conservatively.
**Symbiotic:** similar bifurcation as prior weeks — ~$329M (DeFiLlama base-layer-only methodology) vs. ~$1.6–1.7B (broader comparison methodology, consistent with this report's carried-forward figure). Not a new development. Not an actionable lever for this vault's own validators, which remain not operationally scoped for restaking.
Sources: [EigenLayer Restaking in 2026: A Complete Guide — Chainlabo](https://www.chainlabo.com/blog/eigenlayer-restaking-2026-guide-ethereum-validators), [What Is Restaking? EigenLayer, LRTs Explained 2026 — Blockchainreporter](https://blockchainreporter.net/what-is-restaking-how-eigenlayer-lrts-and-shared-security-actually-work-in-2026/), [EigenLayer vs Symbiotic vs Babylon: Restaking Compared (2026) — Protofire](https://protofire.io/guides/restaking-protocols/), [Symbiotic Restaking Hits $1.7B — CoinGabbar](https://www.coingabbar.com/en/crypto-blogs-details/symbiotic-restaking-eigenlayer-rival).

### (h) MEV relay market share
**Search-engine snapshot appears stale (unrefreshed from last week); one new directional signal outside the recurring figures.** The default search again returned the identical figures already used in last week's report (ultrasound.money 35.26%, Titan 27.99%, bloXroute Regulated 25.40%, Aestus 6.95%, Flashbots 2.39%, Agnostic 1.00%, labeled "as of 2026-08-31") — most likely a cached search result rather than a fresh pull, so it is **not** treated as new confirmation this week. Separately, an MEV researcher's market commentary (via X/Twitter, undated precisely but recent) states that Titan has been steadily gaining relay share "at the expense of bloxroute and ultrasoundmoney" due to vertical integration — directional, unquantified, but a genuinely new (non-recurring) signal worth confirming against `relayscan.io` directly next week. `relayscan.io` and `mevwatch.info` direct fetches were both blocked again this week. A separate search hit referencing "a major relay shutdown, four entities controlling 90% of blocks" was checked and traced to a stale 2023 Blocknative story re-surfacing in search results — confirmed irrelevant and discarded. Net: no relay-registration action indicated this week; the Titan-share-gain signal is worth tracking.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV Watch](https://www.mevwatch.info/) (referenced via search, direct fetch blocked), [@nero_eth relay market update — X](https://x.com/nero_eth/status/2004972045516292339) (referenced via search, direct fetch not attempted — social media source, directional only).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint (~$8.74M at current ETH price), currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated.

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week; assumes the infrastructure-fee waiver holds — **its confirmed end date is now one week in the past, unverified against a primary source**) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. Contingent on the fee-waiver status — see Recommendation for a sensitivity note. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the single figure carried forward for six weeks — still not independently verified or clearly dated | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger but fails the 150% HF floor by a wide margin, and leaves the position near the 100% forced-rebalance line with no buffer improvement. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread carried forward at **~0.06pp** (stETH ~2.2% vs. Aave Prime/Lido-instance WETH borrow ~2.14%; no fresher instance-specific figure obtained this week) | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable — ~$8.7M entry would be a large fraction of ~$35M pool TVL (figure independently corroborated this week via a Curve governance thread, still not freshly dated) | Unchanged | Insufficient depth regardless of rate. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | Disqualified — post-exploit corporate wind-down continuing; LST pools confirmed among the narrow retained product lines but not independently confirmed for this specific pool. |
| 6 | Pendle PT-stETH | Unverified this week; depth still unconfirmed | Unchanged | Data gap — manual check outstanding for nine weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable this week; not a substitute lever for the deployment decision | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all — every external deployment leaves the position sitting near its current ~103% health factor, close to the 100% forced-rebalance threshold, regardless of the yield earned externally. **The recommended strategy's economics assume the infrastructure-fee waiver holds; its confirmed end date is now one week in the past, and this report still could not confirm its post-expiry status against a primary source (see Data Gaps and Recommendation).**

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-09-07) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Carried forward at ~0.06pp (stETH ~2.2%, Aave Prime/Lido-instance WETH borrow ~2.14%); no fresher instance-specific figure obtained this week. Still well short of the threshold on the last confirmed read. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV at 7.1% (unchanged for six weeks), but disqualified separately on the 150% HF-floor test regardless of rate. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (4–5% cited, consistent for nine weeks running); **depth leg still unverifiable.** Manual check of `app.pendle.finance/trade/markets` remains outstanding after nine weeks and is the fastest way to resolve this trigger either way. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `yields.llama.fi`/`defillama.com` pool pages, `app.pendle.finance`, `stake.lido.fi`, `lido.fi`, `research.lido.fi`, `app.aave.com`, `aavescan.com`, `governance.aave.com`, `docs.lido.fi`, `blog.lido.fi`, `relayscan.io`, `gov.curve.finance`, and `mevwatch.info` all returned egress-blocked errors on direct fetch this week.
- As a result, this week's figures — including the carried-forward Aave WETH borrow rate, the GGV figure, the Curve TVL figure, and the MEV relay/builder shares — are drawn from search-engine snippets rather than live dashboard pulls, with the same reliability caveats as prior weeks, even where a specific number is quoted or independently corroborated (as with Curve's ~$35M TVL this week).
- **Highest-priority item, now maximally overdue:** the confirmed infrastructure-fee waiver end date, 2026-08-31, is now **one full week in the past**. Neither this week's nor last week's search results have produced a primary-source confirmation of extension, renewal, or reversion to the standard 1% fee. This week's search results were, if anything, less specific than last week's (reverting to older cached "March 31" / "June 30" figures). The vault dashboard or a primary Lido source should be checked directly, as soon as possible, to determine the fee's actual current status — the recommended-strategy figures (modeled throughout on the fee remaining at 0%) should be revisited immediately once known.
- **Carried forward:** a manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY, TVL, and "as of" date), `app.aave.com/markets/?marketName=proto_lido_v3` (to refresh the Prime/Lido-instance WETH/wstETH rate, now four weeks stale), `app.pendle.finance/trade/markets` (PT-stETH fixed yields and depth, nine weeks outstanding), and `relayscan.io` (to check whether the Titan share-gain signal noted this week shows up in the hard numbers) would materially firm up several figures this report has had to estimate indirectly.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward with only the ETH/USD conversion refreshed (this week's ETH price move, ~$2,485 → ~$2,490, is a minor USD-translation effect only and does not affect any ETH-denominated figure in this report).
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to the recommended strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry.

**Action item that should not wait for next week's refresh:** the confirmed infrastructure-fee waiver's end date, 2026-08-31, is now a full week past, and its post-expiry status still could not be confirmed against a primary source this week. This should be confirmed directly — via the vault dashboard or a primary Lido source, not web search — at the earliest opportunity.

**Sensitivity note:** all figures above continue to assume the 0% infrastructure fee holds, per the standing confirmed fact. If the fee has in fact reverted to its standard 1% (charged on expected staking rewards, separate from the 5% node-operator fee and the 6.5%-of-APR liquidity fee), the internal re-staking strategy's net ETH/yr would be modestly lower than the +14–24 ETH/yr range shown above; this report does not have the underlying rewards-accrual detail needed to restate that range precisely without confirmation of the fee's current status, and is flagging the gap rather than guessing at a revised figure. Even under a 1% infrastructure fee, internal re-staking would very likely remain the top-ranked strategy, since it is the only option that materially improves the health factor — but the exact net-yield figure should be treated as provisional until the fee status is confirmed.

**Secondary item worth a direct look next week:** an MEV researcher's commentary suggests Titan has been gaining relay share at the expense of bloXroute and ultrasound.money. This does not currently indicate any action (the vault is already registered with Titan, ultrasound.money, and both bloXroute relays), but a direct `relayscan.io` pull would confirm whether the trend is real and quantify its size.
