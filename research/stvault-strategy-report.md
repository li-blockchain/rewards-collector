# stVault Weekly Strategy Report — 2026-11-16

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-11-09 report:

- **Recommendation and ranking: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.
- **Infrastructure fee / Early Adopters campaign: still unresolved, now 16 days past the October 31, 2026 deadline, with zero new sourcing either way.** Lido's blog, research.lido.fi, the Node Operator Portal, and Lido's official X/Twitter account were all searched again this week for anything dated after October 31 — nothing was found. `eth-api.lido.fi` remains unreachable from this environment, so a direct protocol-level check was not possible either. One new piece of context: Lido's own economy page (lido.fi/how-lido-works/economy) confirms the **standard, non-waived infrastructure fee is 1%** — clarifying that the Early Adopters waiver is a discount from a 1% baseline, not from an unspecified default. This does not change the modeling assumption (still carried at the last client-confirmed 0%, as of 2026-09-28) but sharpens what is at stake if the waiver has lapsed: a reversion to 1% on vault rewards, on top of the existing 5% node-operator fee. **Top priority, unchanged: direct client reconfirmation of the current infrastructure fee rate.**
- **Lido V3 tier parameters: unchanged.** No governance action altering Tier 1 (5% RR / 4.75% FRT / 47,500 stETH cap) was found this week. The pre-existing third-party discrepancy on Tier 1's reserve ratio (2.5% vs. 5% across sources) remains unresolved but does not change the vault's own confirmed parameters.
- **stETH APR: no fresher dated figure; two new but stale secondary data points surfaced.** An Ebunker weekly report (dated June 22, 2026) cites a 7-day average APR of 2.73%; a KuCoin piece (June 2026) cites ~2.78% base plus 0.5–1% MEV. Both are roughly five months stale and do not supersede the carried ~2.2% net / 2.24% dashboard figures. `eth-api.lido.fi/v1/protocol/steth/apr/` — identified last week as the authoritative live-data endpoint — remains unreachable from this environment. This gap is now carried for a second consecutive week with no progress.
- **Aave Prime/Lido-instance WETH borrow, Morpho loop: no fresher instance-specific rate, but a new compression data point reinforces the "spread not widening" read.** A TokenLogic-sourced figure shows the wstETH exchange-rate APR declined from a 2.70% monthly average in August 2025 to 2.19% in August 2026 — a yield-compression signal, not a borrow rate, but directionally consistent with the carried ~0.0pp spread (stETH ~2.2% vs. Aave Prime WETH net borrowing cost ~2.20%) rather than suggesting it is about to widen past the 0.5pp trigger. No fresher Morpho wstETH/ETH leverage-loop-specific figure was found (consistent with every prior week). Re-entry trigger **not met**, unchanged.
- **Curve and Balancer: no material change.** No fresher Curve stETH/ETH reading was obtained; new undated DefiLlama snippets (main pool 1.26% APY/$76.87M TVL; yvCurve-stETH-f 1.20%/$4.36M; stETH/frxETH 1.34%; stETH-ng 1.41%) are consistent with the carried 1.26%–1.94% range and remain well below the ~2.34% cost of carry regardless. On Balancer, the wind-down vote (BIP-928, 99.2% approval) is now independently dated to September 29, 2026; no source still names which pools, if any (including stETH/wstETH), received post-October-30 treatment. Still **disqualified** on the standing post-exploit basis.
- **Pendle PT-stETH: no verification possible; gap now 19 consecutive weeks.** `app.pendle.finance` and `defillama.com` both failed to resolve directly again this week. No new evidence was found to confirm or refute last week's low-confidence, non-primary BlockEden figure (~4.1% fixed yield, PT-stETH maturing December 2026). The depth question remains entirely unaddressed. The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 is now outstanding for **nineteen** consecutive weeks.
- **Lido GGV curated vault: a new finding suggests the vault may now be closed to new deposits — unconfirmed, flagged as a watch item.** A search snippet of Lido's own GGV deposit page (stake.lido.fi/earn/ggv/deposit) states deposits are no longer being accepted, alongside a 10% platform fee + 1% allocation fee structure. This could not be independently confirmed by direct fetch (stake.lido.fi is unreachable from this environment), and no fresh GGV-specific dated APY/TVL figure was obtained. **This does not change the strategy's ranking**, since GGV was already disqualified on the 150% health-factor floor regardless of rate or deposit status (external deployment does not reduce the vault's own stETH liability). Separately, the blended EarnETH meta-vault (GGV + stRATEGY, not GGV-specific) now shows $198.1M TVL / 3.4% APY per a Lido homepage snippet — down from the ~4.33%/$196M figure cited last week — still not adopted as a GGV-specific read. The carried GGV-specific figure ($98.6M TVL / 7.1% APY) is retained for the comparison table, now **16 consecutive weeks stale**, with an added caveat that the vault may no longer be accepting deposits at all.
- **EigenLayer / Symbiotic: no new yield figure; a new Symbiotic TVL data point reinforces the "compressing, incentive-heavy" framing.** DefiLlama (dated October 20, 2026) shows Symbiotic TVL at $455.63M, down sharply from a previously reported ~650k ETH peak to ~213k ETH — alongside a 21.5% average APY across 13 pools that is very likely incentive/emissions-driven rather than a durable restaking yield (low confidence; not adopted). AVS yield estimates continue to cluster at 4.5–6.5% for EigenLayer, consistent with prior weeks. **Still not a lever** for this vault's own (non-restaked) validators.
- **MEV relay market share: no fresher dated snapshot, but a new governance development is worth watching.** No relayscan.io reading newer than September 20, 2026 was obtained (direct fetch still blocked). A Lido NOM working-group proposal — dated around November 4, 2026 per secondary coverage (Cryip, Coinotag, TokenPost; the original research.lido.fi post itself could not be retrieved) — describes a prospective two-tier ePBS MEV-bidding structure: open, protocol-guaranteed bids alongside an allowlisted-builder tier that can pay extra outside the protocol (1 ETH minimum builder deposit). Sources disagree on timing — one suggests a DAO vote as soon as November 2026, another ties implementation to Ethereum's Glamsterdam/ePBS mainnet upgrade, which has no confirmed date. **Flagged as a watch item only, not adopted.** No relay-registration action is indicated this week; the vault remains registered with ultrasound.money, Titan, and both bloXroute relays.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was again not obtained this week — now over eighteen weeks stale; see Data Gaps). USD conversions remain omitted due to unreliable ETH/USD price reads in prior weeks; all figures are ETH-denominated, consistent with this report's methodology, under which the underlying ETH/stETH position figures are unaffected by any price move.

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

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold, 47,500 stETH minting cap. For reference, Tier 2 is 6% RR / 47,000 stETH cap and Tier 3 is 9% RR / 182,000 stETH cap — this vault remains on Tier 1. (The third-party Kiln conflict — 2.5% RR / 48,750 stETH cap — remains unresolved but does not affect this confirmed figure.)
- **Infrastructure fee:** last directly confirmed by the client at **0%** as of 2026-09-28; no fresher client confirmation sought this week. **The standard (non-waived) infrastructure fee is confirmed this week at 1% via Lido's own economy page** — the Early Adopters waiver through October 31, 2026 is a discount from that baseline. What applies after October 31 remains unconfirmed by any source, now 16 days past the deadline. This continues to be the top-priority open item in this report (see Changes Since Last Week). All figures below continue to be modeled on the last-confirmed 0% fee, pending reconfirmation.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, unchanged from prior reports.

---

## Rate Environment (as of 2026-11-16)

### (a) Lido DAO / stVault governance and fees
No DAO governance action confirmed to change stVault tier parameters was found this week. The infrastructure-fee waiver's post-October-31, 2026 status remains unconfirmed by any source, now 16 days past the deadline — searches of Lido's blog, research.lido.fi, the Node Operator Portal, and Lido's official X account returned nothing dated after the extension-to-October-31 announcements already on file. New this week: confirmation that the standard infrastructure fee (absent any waiver) is 1%, via Lido's own economy page — this sharpens what a lapse would mean but does not itself resolve whether one occurred.
Sources: [Lido economy — How Lido Works](https://lido.fi/how-lido-works/economy) (new this week, source of the 1% standard-fee figure), [Default risk assessment framework and fees parameters for Lido V3 (stVaults) — Lido Governance](https://research.lido.fi/t/default-risk-assessment-framework-and-stvaults/10504) (carried, direct fetch blocked), [Lido extends 0% infrastructure fee for stVaults until October 31 — CryptoBriefing](https://cryptobriefing.com/lido-stvaults-zero-fee-extension-october/) (carried, still the only source for the October 31 date — remains secondary, unconfirmed by any primary Lido post).

### (b) stETH APR
Best estimate for modeling: **~2.2% net-to-holders**, unchanged. No November 2026-dated figure was found. Two stale secondary data points surfaced this week (Ebunker: 2.73% 7-day avg APR, dated June 22, 2026; KuCoin: ~2.78% base + 0.5–1% MEV, June 2026) but are roughly five months old and do not supersede the carried figures. `eth-api.lido.fi/v1/protocol/steth/apr/` — the authoritative live-data endpoint identified last week — remains unreachable from this environment for a second consecutive week.
Sources: [Ethereum Staking Weekly Report — Ebunker, June 22, 2026](https://medium.com/@ebunker.io/ethereum-staking-weekly-report-9e18944d36df) (new this week, stale), [Ethereum Staking in 2026 — KuCoin, June 2026](https://www.kucoin.com/blog/ethereum-staking-in-2026-yield-trends-validator-queue-dynamics-and-mev-impact-exlained) (new this week, stale), [Lido APR and Rewards Calculator](https://www.lido.fi/how-lido-works/apr-and-rewards-calculator) (carried; confirmed this week to show an illustrative example figure, 2.6%, not a live rate), `eth-api.lido.fi/v1/protocol/steth/apr/` (carried, not reached).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher, instance-specific borrow/supply rate this week; direct fetch of aavescan.com and governance.aave.com both blocked again. A new TokenLogic-sourced data point shows the wstETH exchange-rate APR compressed from a 2.70% monthly average (August 2025) to 2.19% (August 2026) — a yield-compression signal that reinforces, rather than contradicts, the carried ~0.0pp spread read. The carried net borrowing cost (~2.20%, from the ~August 2026 Chaos Labs Risk Steward adjustment: WETH borrow 3.30%, offset by wstETH supply 1.10%) stands.
**Morpho:** no wstETH/ETH leverage-loop-specific figure (as opposed to unrelated stablecoin-pair, plain WETH-supply vaults, or non-comparable products like Origin's ARM) was found this week. Not adopted; carried tracker unchanged.
Net read: re-entry trigger **not met**, unchanged from last week.
Sources: [Risk Stewards Interest Rate Update — WETH and wstETH Ethereum — Aave Governance](https://governance.aave.com/t/risk-stewards-interest-rate-update-weth-and-wsteth-ethereum/22625) (carried, direct fetch blocked), [Aavescan — Prime V3 wstETH](https://aavescan.com/prime-v3/wsteth) and [Aavescan — Prime V3 WETH](https://aavescan.com/prime-v3/weth) (carried, direct fetch blocked), [Risk Stewards — August 2026 WETH Interest Rate Adjustment](https://governance.aave.com/t/risk-stewards-august-2026-weth-interest-rate-adjustment/25535) (carried, content still unretrievable, source of the TokenLogic wstETH-exchange-rate-APR compression figure via secondary citation).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** no fresher reading obtained this week; direct DefiLlama pool-page fetch blocked again. New undated snippets (main ETH-stETH pool 1.26% APY/$76.87M TVL; yvCurve-stETH-f 1.20%/$4.36M; stETH/frxETH 1.34%; stETH-ng factory pool 1.41%) are consistent with the carried 1.26%–1.94% range. **Still not viable regardless of the exact figure**: every reading found sits well below the vault's ~2.34% cost of carry.
**Balancer:** the wind-down vote (BIP-928) is now independently dated to September 29, 2026 (99.2% approval, ~17.2M BAL), with the October 16 partner extension-request deadline and October 30 withdrawals-only transition both having passed per prior reporting. No source names which pools, if any (including stETH/wstETH), received an extension or what their current status is; ~$52.4M remains in V2/V3 pools with non-custodial withdrawals open and treasury redemptions scheduled to begin May 2027. Still **disqualified** on the standing post-exploit basis.
Sources: [DefiLlama — ETH-STETH pool](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (carried, direct fetch blocked again, search-snippet only), [Balancer sets shutdown dates after BAL holders approve wind-down — The Defiant](https://thedefiant.io/news/defi/balancer-sets-shutdown-dates-after-bal-holders-approve-wind-down) (carried, source of the BIP-928 date and redemption schedule).

### (e) Pendle PT-stETH
**Still not independently verifiable; now a nineteen-week-old gap.** `app.pendle.finance` and `defillama.com` both failed to resolve directly again this week. No new evidence was found to confirm or refute last week's low-confidence BlockEden figure (~4.1% fixed yield, PT-stETH maturing December 2026); nothing on pool depth was found either. **Not adopted as fact.** The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 is now outstanding for **nineteen** consecutive weeks.
Sources: [Pendle V3: Fixed-Rate Yield Tokenization for Institutional DeFi — BlockEden, April 2026](https://blockeden.xyz/blog/2026/04/17/pendle-v3-fixed-rate-yield-tokenization-institutional-defi-bonds/) (carried, unverified, not adopted), Pendle Academy / handbook.pendle.finance (new this week, generic evergreen documentation, not current rate data, context only).

### (f) Curated ("GGV-class") vaults
**New finding this week, unconfirmed: Lido's GGV vault may now be closed to new deposits.** A search snippet of Lido's own GGV deposit page (stake.lido.fi/earn/ggv/deposit) states deposits are no longer accepted, with a 10% platform fee + 1% allocation fee structure — this could not be confirmed by direct fetch (stake.lido.fi is unreachable from this environment) and is flagged as a watch item, not fact. No fresh GGV-specific dated TVL/APY figure was obtained. The blended EarnETH meta-vault (GGV + stRATEGY, not GGV-specific) now shows $198.1M TVL / 3.4% APY per a Lido homepage snippet, down from the ~4.33%/$196M figure cited last week — still not adopted in place of the carried GGV-specific figure. The carried **$98.6M TVL / 7.1% APY** figure is retained for modeling, now **16 consecutive weeks stale**, with the added caveat that the vault's open/closed status for new deposits is now in question. GGV remains disqualified on the health-factor test regardless of rate or deposit status, since external deployment leaves the vault's own stETH liability untouched.
Sources: stake.lido.fi/earn/ggv/deposit (new this week, search-snippet only, source of the deposits-closed finding, unconfirmed), [Lido Deposit | Earn stETH/WETH Vault Rewards with GGV — lido.today](https://lido.today/) (carried, source of the $98.6M/7.1% figure), Lido homepage snippet (new this week, source of the $198.1M/3.4% EarnETH figure), [Aave × Lido case study](https://aave.com/blog/lido-aave-case-study) (carried, source of the Nov-2025 ~$175M/~5% figure).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** no reliable, dated November 2026 yield figure obtained. AVS yield estimates continue to cluster at 4.5–6.5% (3–4% base ETH staking plus 1–3% AVS rewards), with revenue concentrated in a handful of fee-generating AVSs (EigenDA, EigenAI, EigenCompute) while the rest remains emissions/points-driven, consistent with prior weeks.
**Symbiotic:** a new TVL data point (DefiLlama, dated October 20, 2026) shows TVL at $455.63M, down sharply from a previously reported ~650k ETH peak to ~213k ETH (mostly wstETH). A cited 21.5% average APY across 13 pools is very likely incentive/emissions-driven rather than a durable restaking yield and is **not adopted**.
**Not a lever** for this vault's own (non-restaked) validators, either way.
Sources: [EigenLayer AVS revenue reality check — blockeden.xyz](https://blockeden.xyz/blog/2026/04/20/eigenlayer-avs-revenue-reality-check-15b-restaking/) (carried), DefiLlama Symbiotic TVL page (new this week, dated October 20, 2026, search-snippet only).

### (h) MEV relay market share
**No fresher-than-September-20 dated snapshot.** The carried September 20, 2026 relayscan.io figures (ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%) stand; direct fetch of relayscan.io and mevwatch.info remains blocked. **New this week: a prospective governance change worth tracking.** A Lido NOM working-group proposal, dated around November 4, 2026 per secondary coverage, describes a two-tier ePBS MEV-bidding structure (open protocol-guaranteed bids vs. an allowlisted-builder tier able to pay extra outside the protocol, with a 1 ETH minimum builder deposit). The original research.lido.fi forum post could not be retrieved; secondary sources disagree on whether a DAO vote is imminent or contingent on Ethereum's Glamsterdam/ePBS mainnet upgrade, which has no confirmed date. **Flagged as a watch item only, not adopted.** No relay-registration action is indicated — the vault remains registered with ultrasound.money, Titan, and both bloXroute relays, all of which continue to show meaningful share across every reading found.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (carried, direct fetch blocked), secondary coverage of the November 4, 2026 Lido NOM ePBS proposal (Cryip, Coinotag, TokenPost; new this week, original forum post unretrieved).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint, currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated; USD conversions are omitted (see Position Snapshot).

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the carried figure — now 16 weeks stale, contested by conflicting third-party reads (~3.4% blended-metavault to ~10.6% across sources), and now flagged (unconfirmed) as possibly closed to new deposits | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Fails the 150% HF floor by a wide margin regardless of rate or deposit status. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread remains at **~0.0pp** (stETH ~2.2% vs. Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); a new compression data point this week points toward the spread narrowing further rather than widening | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable: every reading found this week and in prior weeks (1.20%–1.94% across conflicting sources) sits well below the ~2.34% cost of carry | Unchanged | Not viable on yield, independent of depth. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | **Disqualified**; protocol wind-down in progress, pool-level post-deadline outcome still unconfirmed. |
| 6 | Pendle PT-stETH | Unverified | Unchanged | Data gap — manual check outstanding for nineteen weeks; the unconfirmed ~4.1% yield figure on file, if real, would nominally clear the yield leg, but depth remains entirely unconfirmed. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable as a lever for this vault | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping; Symbiotic TVL decline reinforces the "compressing, incentive-heavy" framing. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all. The ranking is unchanged, though the infrastructure-fee-waiver uncertainty (see above) is a live risk to every figure in this table if it resolves to a lapse.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-11-16) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Remains at ~0.0pp (stETH ~2.2%, Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); a new data point this week (wstETH exchange-rate APR compression, 2.70%→2.19% YoY) points toward further narrowing rather than a widening toward the trigger. |
| Curated vault open at ≥ 4% net | — | **Status now uncertain.** The carried GGV figure (7.1%) would nominally meet this threshold, but it is 16 weeks stale, contested by conflicting reads (as low as ~3.4% on a blended, non-GGV-specific basis), and GGV itself may no longer be open to new deposits (unconfirmed). Disqualified separately on the 150% HF-floor test regardless. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Unresolved for a nineteenth consecutive week.** The yield leg has an unconfirmed, non-primary figure (~4.1%) on file; the depth leg has no data at all. Manual check of `app.pendle.finance/trade/markets` remains the only way to get a currently-valid reading on both legs. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `research.lido.fi`, `stake.lido.fi`, `defillama.com`, `governance.aave.com`, `aavescan.com`, `app.pendle.finance`, `www.relayscan.io`, `mevwatch.info`, and `eth-api.lido.fi` all returned DNS-resolution errors on direct fetch this week — the same set of domains blocked in prior weeks. As a result, this week's figures are drawn from search-engine snippets rather than live dashboard or primary-source pulls, with the same reliability caveats as prior weeks.
- **Carried forward — the infrastructure-fee waiver's post-October-31, 2026 status remains unresolved**, now 16 days past the deadline. This is the highest-priority open item in this report; see Changes Since Last Week and Vault Parameters & Fee Structure. A direct client reconfirmation of the current fee rate, and/or a primary Lido governance or operator-portal post covering the November 2026 rollover, would resolve this.
- **New this week — GGV's open/closed status for new deposits is now in question.** A search snippet suggests deposits may be closed; this is unconfirmed by direct fetch and does not change this strategy's ranking (already disqualified on the HF floor), but should be verified if GGV is ever reconsidered.
- **Carried forward — Curve TVL/APY sourcing remains noisy:** undated readings in the 1.20%–1.94% APY range remain on file. Does not change the strategy's not-viable conclusion either way, since every reading is far below the ~2.34% cost of carry.
- **Carried forward — Balancer's stETH/wstETH-pool-specific extension outcome remains unconfirmed**, now with the wind-down vote date independently confirmed (September 29, 2026) but no source stating which, if any, pools (including stETH/wstETH) received an extension or what their current status is.
- **Carried forward — ETH/USD price data quality:** prior weeks' searches returned inconsistent ETH/USD figures; USD conversions remain omitted and were not re-attempted this week. All figures in this report stay ETH-denominated.
- **Carried forward:** no fresh client reconfirmation of the infrastructure fee was sought this week (this refresh ran without a client-contact step); the last confirmed status (0%, as of 2026-09-28) is carried forward pending the reconfirmation flagged above as top priority.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward and are now over eighteen weeks old.
- A manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY/TVL/open status — now higher priority given this week's closed-deposits signal), `app.aave.com/markets/?marketName=proto_lido_v3` (Prime/Lido-instance WETH/wstETH rate), `defillama.com` (Curve pool TVL/APY), `app.pendle.finance/trade/markets` (current, non-expired PT-stETH maturities, yields, and depth), `eth-api.lido.fi/v1/protocol/steth/apr/` (live stETH APR), and `research.lido.fi`/`blog.lido.fi` (infrastructure-fee status) would materially firm up several figures this report has had to estimate indirectly — the fee-status check remains the most urgent of these.
- **Carried forward — this report's recurring network-access limitation has an unresolved structural fix on file** (a request to allowlist the primary research hosts above, prepared in a prior week), which requires a one-time interactive approval from a person and was not re-attempted this week, since this is an unattended scheduled run and no such approval is available during it. This remains an item for the client to action outside this automated workflow, not something this report will re-request on its own in future unattended runs.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

**No change to the recommended strategy this week.** Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry.

**The item requiring the closest attention next week remains the infrastructure-fee waiver**, now 16 days past its last-confirmed expiry (October 31, 2026) with no source — primary or secondary — stating what applies now. Since every client-facing figure in this report assumes the fee is still 0%, this gap should be resolved as a priority, ideally through direct client reconfirmation, and the ranking table revisited immediately if the fee is found to have reverted to the standard 1% rate, since that would reduce internal re-staking's net yield advantage without changing which option is recommended.

**A secondary item for next week: verify GGV's deposit status.** This week's unconfirmed finding that Lido's GGV vault may be closed to new deposits does not change today's recommendation, but should be confirmed (via a manual `stake.lido.fi/earn/ggv` check) before GGV is cited in any future client-facing context, since a closed vault cannot be a viable deployment regardless of its rate.

**Several items added detail without changing conclusions:** the standard (non-waived) Lido infrastructure fee is now confirmed at 1%, sharpening the stakes of the fee-waiver question without resolving it; a new wstETH exchange-rate compression data point reinforces (rather than challenges) the "no trigger" read on the Aave/Morpho spread; a new Symbiotic TVL decline reinforces the "compressing, incentive-heavy" framing for restaking; and a prospective Lido ePBS MEV-policy change was flagged as a watch item with no near-term action implied.

**Outstanding items for next week:** direct client reconfirmation of the infrastructure fee is the top priority. The manual check of `app.pendle.finance/trade/markets` is now nineteen weeks overdue. A manual `stake.lido.fi/earn/ggv` check would resolve both the open/closed-status question and the rate uncertainty. The Balancer stETH/wstETH-pool extension outcome should be checked for a result if one surfaces. A fresh Lido dashboard pull for the vault's own position is also overdue — the position snapshot in this report is now based on data from over four months ago. A direct `eth-api.lido.fi/v1/protocol/steth/apr/` pull would resolve the stETH APR gap outright.
