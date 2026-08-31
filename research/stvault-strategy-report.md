# stVault Weekly Strategy Report — 2026-08-31

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-08-24 report:

- **Infrastructure-fee waiver: today is the confirmed expiry date itself, and the waiver's status remains unresolved.** The confirmed end date of the Early Adopters 0% infrastructure-fee campaign, 2026-08-31, is **today**. This week's web search — for the first time in eight consecutive weeks of tracking this item — converged on "August 31, 2026" as the campaign's expiration in a search-engine-summarized snippet (distinct from the "March 31" and "June 30" figures that had circulated in earlier weeks), which is consistent with the confirmed vault-specific fact this report has modeled against all along. However, this remains a search-engine summary, not a primary-source confirmation: `docs.lido.fi`, `blog.lido.fi`, `stake.lido.fi`, `lido.fi`, and `research.lido.fi` direct fetches were all blocked again this week (the same egress restriction as seven straight prior weeks), and no announcement of a renewal, extension, or confirmed lapse was found. The figures below continue to assume the 0% fee holds, per standing confirmed facts, but **this assumption now covers the exact day it was scheduled to end, with no confirmation either way.** This is unambiguously the report's top item to verify directly (vault dashboard or a primary Lido source) before the next refresh — the current model does not know whether tomorrow's accrual reverts to the 1% fee.
- **Aave Prime (Lido instance) WETH borrow rate — still no fresher instance-specific figure; two adjacent-but-not-applicable governance actions surfaced this week.** `app.aave.com`, `aavescan.com`, and `governance.aave.com` direct fetches were all blocked again. Two Aave governance items dated this week are relevant context but not directly usable: (1) a Risk Stewards proposal dated 2026-08-26 recommending WETH Slope 1 be reduced to 2.20% on the **Core** instance (plus Arbitrum and Optimism) "to realign borrowing costs with the prevailing LST yield environment" — this is the Core instance, not the Prime/Lido instance this report models, but is a directional signal that WETH borrow costs may be easing; (2) a GHO Stewards update dated 2026-08-27 describing the Ethereum **Prime** market's base rate rising 75bps to 2.75% in two steps — this concerns the Prime market's base/GHO-linked rate mechanics, not a confirmed WETH borrow APY figure, so it is not used to update the modeled spread either. The last Prime/Lido-instance-relevant read (WETH borrow ≈2.14%, carried forward three weeks now) is retained unchanged. Against stETH APR (~2.2%), the implied spread stays **~0.06pp**, still well under the 0.5pp re-entry trigger. Given the two conflicting directional signals (Core rate proposed down; Prime base rate reported up), the actual Prime-instance spread this week is genuinely uncertain and worth a direct dashboard check.
- **GGV: figure unchanged for a fifth week; "as of" date still unavailable.** Search this week again returned only the recurring **$98.6M TVL / 7.1% APY** figure, now unchanged since the week of 2026-08-03. No independent confirmation or "as of" date was found this week either. Still fails the 150% health-factor floor test regardless.
- **MEV relay share — a data-quality wrinkle this week: bloXroute's two relays did not appear as separate line items.** A relayscan.io-derived search result (labeled "24h payload share, as of 2026-08-31," though this label may simply reflect the search date rather than a genuine source timestamp) returned ultrasound.money 35.26%, Titan 27.99%, bloXroute Regulated 25.40%, Aestus 6.95%, Flashbots 2.39%, Agnostic 1.00% — with **no separate bloXroute Max Profit line item**, unlike every prior week's read (22.18% last week). It is not clear from this snippet whether Max Profit's share has collapsed, been folded into the "Regulated" figure by the summarizer, or simply been omitted. Treating the three explicit vault-relevant figures at face value, combined share of the vault's registered relays (ultrasound + Titan + bloXroute Regulated) is **~88.65%**, close to last week's ~89.0% combined four-relay figure — so the total looks broadly stable even though the composition is unverified. On the **builder** side, a separate read this week gave Titan 47.64%, BuilderNet 19.25%, Quasar 18.01% — Titan down from 55.78% last week, a reversal of the three-week rebound trend, again unconfirmed against a direct relayscan.io pull (blocked again this week). Given the missing category and the reversal, this week's relay/builder read should be treated as lower-confidence than usual; no relay-registration action is indicated either way.
- **stETH APR: no change to modeling.** Search results this week again show ~2.6% (base network rewards framing), consistent with the vault's own confirmed figures (2.24% dashboard net APR, ~2.6% gross consensus yield). The ~2.2% modeling assumption is unchanged.
- **Curve, Balancer, Pendle, EigenLayer/Symbiotic: no material change.** Curve stETH/ETH pool TVL still not obtained directly (`defillama.com` blocked); the ~$35M carried-forward figure (now eight weeks stale) remains the basis for the depth conclusion — still not viable at this position size. Balancer remains disqualified on the standing corporate-wind-down basis, unchanged from last week's nuance (LST pools among the five retained product lines, still not independently confirmed as resolving the uncertainty). Pendle PT-stETH commentary (4–5% fixed yield) is unchanged; the manual `app.pendle.finance/trade/markets` depth check recommended since 2026-07-13 is now outstanding for **eight** consecutive weeks. EigenLayer TVL citations remain internally inconsistent (a new figure this week, $12.9B "surging 11% in one week," sits below the $15.3–19.7B range cited in recent weeks — the same unresolved cross-source data-quality flag, now with a wider spread). Symbiotic TVL not refreshed this week; ~$1.6B carried forward. ether.fi's 2026-08-06 exit from EigenLayer restaking (all restaking exposure removed from weETH, delegating into Symbiotic instead) is unchanged from last week's report, not a new development.
- **No change** to the confirmed Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold) or the 5% node-operator fee. No new DAO governance action on stVault tier parameters or fees was found this week.
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week. The recommendation's economics are contingent on the infrastructure-fee waiver, whose confirmed expiry is today — see above.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was not obtained this week — see Data Gaps). $ conversions below use today's approximate ETH price (~$2,485, 2026-08-31, essentially flat vs. ~$2,470 on 2026-08-24); the underlying ETH/stETH figures are unchanged from the 2026-07-13 pull.

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) ≈ $9.42M at ~$2,485/ETH (2026-08-31) |
| stETH liability | 3,509.2 stETH (~93% of total value) ≈ $8.72M |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH (~$0.27M at current price); a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH, ~$8.72M at current ETH price) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below. The ETH price move this week (~$2,470 → ~$2,485) is a minor market/USD-translation effect only; it does not change the underlying ETH-denominated position, health factor, or ETH/yr yield comparisons, which is why the ranked comparison below is expressed in ETH/yr terms throughout.

---

## Vault Parameters & Fee Structure

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold (confirmed vault-specific figures; a conflicting generic docs figure of 2.50% noted in earlier reports could not be re-checked again this week, `docs.lido.fi` blocked).
- **Infrastructure fee:** modeled as waived under the Early Adopters campaign through the confirmed end date of **2026-08-31 — today**. This week's search results converged for the first time on "August 31, 2026" as the campaign's expiration, consistent with the confirmed fact this report has used throughout, but this remains unconfirmed against a primary source (all relevant Lido domains blocked on direct fetch again this week). **This is the report's single highest-priority item to verify directly before the next refresh** — the model has no visibility into whether the fee reverts to 1% starting today or tomorrow.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, consistent with prior reports.

---

## Rate Environment (as of 2026-08-31)

### (a) Lido DAO / stVault governance and fees
No new DAO governance action affecting stVault tier parameters or fee terms was found this week. The infrastructure-fee waiver's confirmed end date (2026-08-31) arrives today; see Changes Since Last Week and Vault Parameters above for the status and its unresolved confirmation.
Sources: [Lido V3 Is Live](https://blog.lido.fi/lido-v3-is-live-modular-infrastructure-for-a-new-paradigm-of-ethereum-staking/) (referenced via search, direct fetch blocked), [Lido Introduces New Node Operator Tiers and Extended Minting Caps in V3 Update](https://cryptonews.net/news/defi/32507188/), [Lido V3 stVaults — BlockEden.xyz](https://blockeden.xyz/blog/2026/02/09/lido-v3-stvaults-ethereum-staking/), [stVaults fees approach — Lido Governance](https://research.lido.fi/t/stvaults-fees-approach/9979) (referenced via search, direct fetch blocked).

### (b) stETH APR
Best estimate for modeling: **~2.2%**, unchanged. Search results this week again show ~2.6% (base network rewards), consistent with this vault's own confirmed figures (2.24% dashboard net APR, ~2.6% gross consensus yield).
Sources: [Best Crypto Staking Platforms & Highest APY Rates — August 2026 — Ventureburn](https://ventureburn.com/best-crypto-staking-platforms/), [Ethereum Staking Guide 2026 — CryptoTimes](https://www.cryptotimes.io/learn/ethereum-staking/) (carried).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher instance-specific figure obtained this week (`app.aave.com`, `aavescan.com`, `governance.aave.com` all blocked on direct fetch). Two governance items dated this week are directionally relevant but not directly usable: a Risk Stewards proposal (2026-08-26) recommending WETH Slope 1 reduced to 2.20% on the **Core** instance (not Prime), and a GHO Stewards update (2026-08-27) describing the **Prime** market's base rate rising 75bps to 2.75% — a base-rate mechanic, not a confirmed WETH borrow APY. Neither is used to update the modeled figure. The last Prime/Lido-relevant read (**WETH borrow ≈2.14%**) is carried forward unchanged for a third week. Against stETH APR (~2.2%), this implies a spread of **~0.06pp**, well under the 0.5pp re-entry trigger — though given this week's conflicting directional signals, the actual current spread is uncertain and worth a direct check.
**Morpho:** no reliable current loop-APY figure obtained this week; search returned only general commentary and a stale January–April 2025 backtest (~6.2% under rebalancing assumptions not verified as current). Not treated as authoritative.
Net read: re-entry trigger **not met**.
Sources: [Aave — Lido case study](https://aave.com/blog/lido-aave-case-study), [Risk Stewards: August 2026 — WETH Interest Rate Adjustment](https://governance.aave.com/t/risk-stewards-august-2026-weth-interest-rate-adjustment/25535) (referenced via search, direct fetch blocked), [GHO Stewards: August 2026 — GHO Borrow Rate and Aave Savings Rate Update](https://governance.aave.com/t/gho-stewards-august-2026-gho-borrow-rate-and-aave-savings-rate-update/25534) (referenced via search, direct fetch blocked).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** `defillama.com` direct fetch blocked again this week; no fresher TVL figure than the ~$35M carried forward for eight weeks. At that carried-forward TVL, a ~$6.7-8.7M entry would still represent a large fraction of pool TVL — **not viable at this position size** regardless of the exact current rate.
**Balancer:** still effectively **disqualified** on the standing corporate-wind-down basis; unchanged from last week (LST pools noted as one of five retained post-restructuring product lines, not independently confirmed as resolving the uncertainty for this specific pool). Not acted on this week.
Sources: [Balancer Labs to shut down — CoinDesk](https://www.coindesk.com/tech/2026/03/24/balancer-labs-will-shut-down-as-corporate-entity-became-a-liability-after-usd110-million-exploit) (carried), [Curve Finance TVL, Fees, Revenue & Volume — DefiLlama](https://defillama.com/protocol/curve-finance) (referenced via search, direct fetch blocked).

### (e) Pendle PT-stETH
**Still not independently verifiable this week.** `app.pendle.finance` remains blocked by this session's egress policy. General commentary again describes PT-stETH fixed yields in a **4–5%** range, unchanged. No pool-specific TVL/depth figure for a ~$6.7-8.7M position was obtained this week. **The manual check of `app.pendle.finance/trade/markets` recommended since the 2026-07-13 briefing still has not been performed and is now outstanding for eight consecutive weeks.**
Sources: [Pendle Finance Review — Coin Bureau](https://coinbureau.com/review/pendle-finance-review), [A Complete Guide on How to Use Pendle Finance in 2026 — Coin Bureau](https://coinbureau.com/guides/how-to-use-pendle-finance) (carried).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked). This week's search again returned only the recurring **$98.6M TVL / 7.1% APY** figure, unchanged for a fifth consecutive week, still with no clear "as of" date or independent (non-search-engine) confirmation. GGV remains disqualified on the health-factor test regardless of rate.
Sources: [Lido blog — GGV overview](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/) (referenced via search, direct fetch blocked), [GGV deposit — stake.lido.fi](https://stake.lido.fi/earn/ggv/deposit) (referenced via search, direct fetch blocked).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** base AVS yield still commonly cited in the ~4–7% range (3–4% base staking + 1–6% AVS rewards depending on operator/AVS), consistent with prior weeks. TVL citations remain internally inconsistent and, if anything, wider this week: a new figure of **$12.9B** ("surging 11% in one week") sits well below the $15.3–19.7B range cited in recent weeks — the same unresolved cross-source data-quality flag noted for several weeks running, now with a larger spread. Slashing is enforceable (live since April 2025); risk should be modeled conservatively.
**Symbiotic:** TVL not refreshed this week; ~$1.6B carried forward. ether.fi's 2026-08-06 exit from EigenLayer restaking (all restaking exposure removed from weETH, delegating into Symbiotic instead) is unchanged from last week, not a new development this week. Not an actionable lever for this vault's own validators, which remain not operationally scoped for restaking.
Sources: [EigenLayer TVL hits $12.9 billion and surges 11% in one week — Cryptopolitan](https://www.cryptopolitan.com/eigenlayer-tvl-hits-12-9-billion-surges-11/), [EigenLayer Crosses $18B in Restaked ETH — BlockEden.xyz](https://blockeden.xyz/blog/2026/03/20/eigenlayer-18b-tvl-vertical-avs-specialization-restaking-evolution/), [EigenLayer Review 2026 — Coin Bureau](https://coinbureau.com/review/eigenlayer-review), [Restaking Protocols Compared — Protofire](https://protofire.io/guides/restaking-protocols/).

### (h) MEV relay market share
**Refreshed snapshot this week, with a data-quality caveat.** A relayscan.io-derived search result (labeled 24h payload share, "as of 2026-08-31" — a label that may simply reflect the search date rather than a genuine source timestamp) returned: ultrasound.money 35.26%, Titan 27.99%, bloXroute Regulated 25.40%, Aestus 6.95%, Flashbots 2.39%, Agnostic 1.00%. Unlike every prior week's read, **no separate bloXroute Max Profit line item appeared** (it stood at 22.18% last week) — it is unclear whether this reflects a genuine collapse, a relabeling/merge by the search summarizer, or an omission. Taking the three explicit vault-relevant relays at face value, combined share is **~88.65%** (35.26 + 27.99 + 25.40), close to last week's ~89.0% four-relay combined figure — so the aggregate looks broadly stable even though the underlying composition is unverified this week. On the **builder** side, a separate read gave Titan 47.64%, BuilderNet 19.25%, Quasar 18.01% — Titan down from 55.78% last week, reversing the three-week rebound noted in recent reports; also unconfirmed against a direct pull. `relayscan.io` direct fetch remained blocked. Net read: given the missing relay category and the builder-share reversal, this week's figures should be treated as lower-confidence than usual; no relay-registration action is indicated regardless.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV-Boost Builder Profitability — relayscan.io](https://www.relayscan.io/builder-profit?t=24h) (referenced via search, direct fetch blocked), [MEV Watch](https://www.mevwatch.info/).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint (~$8.72M at current ETH price), currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated.

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week; assumes the infrastructure-fee waiver holds — **its confirmed end date is today, unverified against a primary source**) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. Contingent on the fee-waiver status — see Data Gaps. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the single figure carried forward for five weeks — still not independently verified or clearly dated | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger but fails the 150% HF floor by a wide margin, and leaves the position near the 100% forced-rebalance line with no buffer improvement. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread carried forward at **~0.06pp** (stETH ~2.2% vs. Aave Prime/Lido-instance WETH borrow ~2.14%; no fresher instance-specific figure obtained this week; two adjacent Core/Prime-base-rate governance items this week were not usable to update this figure), well below the 0.5pp trigger | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable — ~$6.7-8.7M entry would be a large fraction of ~$35M pool TVL (carried forward for eight weeks, no fresh figure this week) | Unchanged | Insufficient depth regardless of rate. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | Disqualified — post-exploit corporate wind-down continuing; LST pools reportedly among retained product lines but not independently confirmed for this pool. |
| 6 | Pendle PT-stETH | Unverified this week; depth still unconfirmed | Unchanged | Data gap — manual check outstanding for eight weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable this week; not a substitute lever for the deployment decision | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all — every external deployment leaves the position sitting near its current ~103% health factor, close to the 100% forced-rebalance threshold, regardless of the yield earned externally. **The recommended strategy's economics assume the infrastructure-fee waiver holds; its confirmed end date is today, and this report could not confirm its post-expiry status against a primary source (see Data Gaps).**

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-08-31) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Carried forward at ~0.06pp (stETH ~2.2%, Aave Prime/Lido-instance WETH borrow ~2.14%); no fresher instance-specific figure obtained this week. Two adjacent governance items (Core-instance rate proposal down; Prime base rate reported up) point in different directions and neither is directly usable — worth a direct dashboard check given the ambiguity. Still well short of the threshold on the last confirmed read. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV at 7.1% (unchanged for five weeks), but disqualified separately on the 150% HF-floor test regardless of rate. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (4–5% cited, consistent for eight weeks running); **depth leg still unverifiable.** Manual check of `app.pendle.finance/trade/markets` remains outstanding after eight weeks and is the fastest way to resolve this trigger either way. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `yields.llama.fi`/`defillama.com` pool pages, `app.pendle.finance`, `stake.lido.fi`, `lido.fi`, `research.lido.fi`, `app.aave.com`, `aavescan.com`, `governance.aave.com`, `docs.lido.fi`, `blog.lido.fi`, and `relayscan.io` all returned egress-blocked errors on direct fetch this week.
- As a result, this week's figures — including the carried-forward Aave WETH borrow rate, the GGV figure, and the MEV relay/builder shares — are drawn from search-engine snippets rather than live dashboard pulls, with the same reliability caveats as prior weeks, even where a specific number is quoted.
- **Highest-priority item this week, and now maximally time-sensitive:** the confirmed infrastructure-fee waiver end date, **2026-08-31, is today.** This week's search results converged for the first time on this exact date (versus the "March 31" / "June 30" figures seen in earlier weeks), consistent with the confirmed fact used throughout this report's modeling — but this remains unconfirmed against a primary source; all relevant Lido domains were blocked on direct fetch again this week. The vault dashboard or a primary Lido source should be checked directly, as soon as possible, to determine whether the fee reverted to 1% today, was extended, or lapsed on a different schedule than assumed — and the recommended-strategy figures (currently modeled on the fee remaining at 0%) should be revisited immediately once known.
- **Carried forward:** a manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY, TVL, and "as of" date), `app.aave.com/markets/?marketName=proto_lido_v3` (to refresh the Prime/Lido-instance WETH/wstETH rate, now three weeks stale, with this week's conflicting governance signals making a fresh read more valuable than usual), `app.pendle.finance/trade/markets` (PT-stETH fixed yields and depth, eight weeks outstanding), and `relayscan.io` (to resolve this week's missing bloXroute Max Profit line item and the builder-share reversal) would materially firm up several figures this report has had to estimate indirectly.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward with only the ETH/USD conversion refreshed (this week's ETH price move, ~$2,470 → ~$2,485, is a minor USD-translation effect only and does not affect any ETH-denominated figure in this report).
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to the recommended strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry. **The action item that should not wait for next week's refresh:** the confirmed infrastructure-fee waiver's end date, 2026-08-31, is today, and its post-expiry status could not be confirmed against a primary source this week (search results converged on this date for the first time, but no direct dashboard or governance-page confirmation was obtainable). This should be confirmed directly — via the vault dashboard or a primary Lido source, not web search — at the earliest opportunity, and the recommended-strategy figures revisited immediately if the fee has reverted to 1%.
