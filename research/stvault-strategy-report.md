# stVault Weekly Strategy Report — 2026-11-02

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-10-26 report:

- **Recommendation and ranking: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.
- **Infrastructure fee / Early Adopters campaign: the secondary-sourced October 31, 2026 expiry has now passed with no confirmation either way — this is the single most important development this week.** No post-October-26 Lido governance action, monthly update, or news coverage was found confirming either a further extension or a lapse to the standard fee. The primary Lido governance forum post continues to confirm only through August 31, 2026; the commonly-cited October 31 date has only ever rested on two secondary sources (CryptoBriefing, CryptoSlate, both carried from prior weeks), and no fresher corroboration or contradiction of that date was found this week either. **This report cannot currently state whether the 0% infrastructure fee is still in effect.** No change is made to the modeling assumption (0%, last directly confirmed by the client on 2026-09-28), but this gap is materially more significant than last week's "5 days away" framing — the window has now closed without resolution, and every client-facing return figure in this report would change if the fee reverted to the standard rate. **Top priority for next week: direct client reconfirmation of the current infrastructure fee, and a renewed search for a primary Lido governance or monthly-update post covering September–October 2026.**
- **Lido V3 tier parameters: unresolved, unchanged.** No DAO governance action changing stVault tier parameters was found this week. The third-party conflict on Tier 1's reserve ratio (2.5%/48,750 stETH per Kiln vs. 5%/47,500 stETH per the vault's own confirmed figures) persists with no new information either way. This report continues to model the vault's own **confirmed** parameters.
- **stETH APR: no update.** No Lido-sourced, dated figure newer than the carried ~2.2% modeling assumption was found. The official Lido APR API endpoint remains unreachable in this environment. Low-confidence secondary figures (~2.6%, ~2.36%) were found but not adopted, consistent with prior weeks.
- **Curve stETH/ETH pool: sourcing got noisier again, not clearer, but the conclusion is unaffected.** Two additional undated DefiLlama-sourced snapshots surfaced this week ($79.77M TVL/1.53% APY and $19.18M TVL/1.94% APY), on top of the two conflicting undated factory-pool figures carried from last week ($18.61M/1.41%, $16.47M/1.73%) and the $76.87M/1.26% figure carried from two weeks prior. Four to five mutually inconsistent, mostly undated readings are now on file. **No ranking change**: every reading found this week or in prior weeks (1.26%–1.94% APY) sits well below the vault's ~2.34% cost of carry, so the strategy continues to fail on yield regardless of which figure is most current.
- **Balancer wind-down: the October 30, 2026 withdrawals-only transition is now confirmed to have taken effect; the stETH/wstETH-pool-specific outcome remains unresolved.** A new source this week states that pausable Balancer V2/V3 pools moved to withdrawals-only on October 30, 2026 (now passed), with BAL-treasury-asset redemptions to begin at the end of May 2027, and roughly $52.4M still sitting in V2/V3 pools as of the report. This corroborates (rather than contradicts) last week's wind-down timeline, but **no source names a final per-pool list**, so whether the stETH/wstETH pools specifically received an extension past the October 16 partner-request deadline is still unconfirmed. Still **disqualified** on the standing post-exploit basis; this additional detail does not change the ranking.
- **Lido GGV curated vault: no fresh figure.** No dated TVL/APY figure newer than the carried $98.6M/7.1% was found this week; the same conflicting third-party reads (Nov-2025 Aave case study ~$175M/~5%, Veda >$66M, third-party $105M/10.6%) remain on file, with no newer corroboration of any of them. The carried figure is now **14 consecutive weeks stale**. GGV remains disqualified on the health-factor test regardless of which rate figure is used, since external deployment does not reduce the vault's own stETH liability.
- **Aave Prime/Lido-instance WETH borrow, Morpho loop: no material change.** A new Aave governance thread ("[Risk Stewards] Interest Rate Update - WETH and wstETH Ethereum") surfaced but is undated for this report's purposes and its passage status is unconfirmed — not adopted. A separate undated Aavescan WETH snapshot showing a lower 30-day average borrow APR (2.31% vs. the carried 3.30%) could not be confirmed as current or Prime-instance-specific, and was not adopted for the same reason. No Morpho wstETH/ETH leverage-loop-specific figure (as opposed to unrelated stablecoin-pair or WETH-supply vaults) was found. The carried net borrowing cost (~2.20%) and ~0.0pp spread against stETH APR stand, and the re-entry trigger remains **not met**.
- **EigenLayer / Symbiotic: no reliable fresher figure; framing unchanged.** Search results this week were dominated by stale or wildly inconsistent TVL claims spanning 2024–2026; nothing rises to a reliable update on the carried ~$4.67B/post-peak-pullback narrative. AVS yield estimates (4–7%, split between base staking and AVS rewards) are directionally consistent with prior commentary but not newly confirmed, and analysts continue to flag AVS reward income as emissions-heavy and likely to compress. No Symbiotic ETH-restaking yield figure for 2026 was found. **Still not a lever** for this vault's own (non-restaked) validators.
- **MEV relay market share: no fresher dated snapshot.** relayscan.io remains unreachable via direct fetch. No dated figure newer than the carried September 20, 2026 relayscan.io reading (ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%) or the August 29, 2026 shattered.io snapshot was found. An undated secondary glossary split was found but carries no citable date and is not adopted as an update. No relay-registration action is indicated; all three registered relay groups continue to show meaningful share across every reading found.
- **Pendle PT-stETH: data gap persists, now seventeen consecutive weeks, with new (unconfirmed) signals pointing toward insufficient depth.** `app.pendle.finance` remains unreachable via direct fetch. No reliable, dated, stETH-specific fixed-yield figure was found. Two new but low-confidence, non-primary depth readings surfaced — a September 2026 market at roughly $30M TVL/$3M daily volume/45bps spread, and a July 2026 snapshot of an individual stETH maturity at only ~$459k TVL — both suggesting thin liquidity relative to the ~$6M+ depth this report's trigger requires, but neither is DefiLlama- or Pendle-primary-sourced, so **neither is adopted as fact**. The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 remains outstanding.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was again not obtained this week — now over sixteen weeks stale; see Data Gaps). USD conversions remain omitted this week due to unreliable ETH/USD price reads in prior weeks; all figures are ETH-denominated, consistent with this report's methodology, under which the underlying ETH/stETH position figures are unaffected by any price move.

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

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold, 47,500 stETH minting cap. For reference, Tier 2 is 6% RR / 47,000 stETH cap and Tier 3 is 9% RR / 182,000 stETH cap — this vault remains on Tier 1. (See Changes Since Last Week re: an unresolved third-party sourcing discrepancy on Tier 1's RR that does not affect this confirmed figure.)
- **Infrastructure fee:** last directly confirmed by the client at **0%** as of 2026-09-28; no fresher client confirmation sought this week. **The secondary-sourced October 31, 2026 extension deadline has now passed with no primary or secondary confirmation of either a further extension or a lapse.** This is now the top-priority open item in this report (see Changes Since Last Week). All figures below continue to be modeled on the last-confirmed 0% fee, pending reconfirmation.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, unchanged from prior reports.

---

## Rate Environment (as of 2026-11-02)

### (a) Lido DAO / stVault governance and fees
No DAO governance action changing stVault tier parameters was found this week. The secondary-sourced October 31, 2026 infrastructure-fee-waiver expiry has now passed with no primary governance confirmation of its outcome — a further extension and a lapse to the standard fee are both equally unconfirmed. No September or October 2026 monthly update was found. The third-party discrepancy on Tier 1's reserve ratio (2.5% vs. 5% across sources) remains unresolved but does not change the vault's own confirmed parameters.
Sources: [Default risk assessment framework and fees parameters for Lido V3 (stVaults) — Lido Governance](https://research.lido.fi/t/default-risk-assessment-framework-and-fees-parameters-for-lido-v3-stvaults/10504) (direct fetch blocked again; confirms only through Aug 31, 2026 per search), [Lido extends 0% infrastructure fee for stVaults until October 31 — CryptoBriefing](https://cryptobriefing.com/lido-stvaults-zero-fee-extension-october/) (carried), CryptoSlate coverage (carried, same Oct 31 claim), [stVaults Monthly Updates: May-June 2026 — Lido blog](https://blog.lido.fi/stvaults-monthly-updates-may-june-2026/) (carried, no newer edition found), Kiln stVaults risk disclosure (carried, source of the conflicting 2.5%/48,750 stETH figure).

### (b) stETH APR
Best estimate for modeling: **~2.2% net-to-holders**, unchanged. The official Lido APR API endpoint was unreachable this week. Search again surfaced only low-confidence, non-primary figures (~2.6%, ~2.36%, both undated or weakly dated); neither is treated as an update. Modeling assumption stays anchored to the vault's own confirmed dashboard figure (2.24% net).
Sources: low-confidence third-party yield listings (not cited individually), [Ethereum Staking Weekly Report, September 7, 2026 — Bitget](https://www.bitget.com/news/detail/12560605798507) (carried).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher, reliably-dated, instance-specific figure this week. A new Aave governance thread ("[Risk Stewards] Interest Rate Update - WETH and wstETH Ethereum") was found but is undated for this report's purposes and its passage status is unconfirmed — not adopted. An undated Aavescan snapshot implying a lower WETH borrow rate (~2.31% 30-day average vs. the carried 3.30%) could not be confirmed as current or instance-specific — not adopted. The carried net borrowing cost (~2.20%, from the ~August 2026 Chaos Labs Risk Steward adjustment: WETH borrow 3.30%, offset by wstETH supply 1.10%) stands. Against the carried stETH APR (~2.2%), the spread remains **~0.0pp**.
**Morpho:** no wstETH/ETH leverage-loop-specific figure (as opposed to unrelated stablecoin-pair or plain WETH-supply vaults) was found this week. Not adopted; carried tracker unchanged.
Net read: re-entry trigger **not met**, unchanged from last week.
Sources: [Aavescan — Prime V3 wstETH](https://aavescan.com/prime-v3/wsteth) and [Aavescan — Prime V3 WETH](https://aavescan.com/prime-v3/weth) (referenced via search, direct fetch blocked), [ARFC Prime Instance — wstETH Borrow Rate + rsETH Supply Cap Update — Aave Governance](https://governance.aave.com/t/arfc-prime-instance-wsteth-borrow-rate-rseth-supply-cap-update/20644) (carried; source of the 3.30%/1.10%/2.20% figures), "[Risk Stewards] Interest Rate Update - WETH and wstETH Ethereum — Aave Governance" (new this week, direct fetch blocked, undated passage status, context only — [governance.aave.com/t/risk-stewards-interest-rate-update-weth-and-wsteth-ethereum/22625](https://governance.aave.com/t/risk-stewards-interest-rate-update-weth-and-wsteth-ethereum/22625)).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** direct DefiLlama pool-page fetch blocked again this week. Two new undated DefiLlama-sourced snapshots surfaced ($79.77M TVL/1.53% APY and $19.18M TVL/1.94% APY), in addition to the two conflicting undated factory-pool figures carried from last week ($18.61M/1.41%, $16.47M/1.73%) and the $76.87M/1.26% figure carried from two weeks prior. **Still not viable regardless of the exact TVL or APY figure**: every reading found across all five snapshots (1.26%–1.94% APY) sits well below the vault's ~2.34% cost of carry.
**Balancer:** a new source confirms pausable Balancer V2/V3 pools moved to withdrawals-only on October 30, 2026 (now passed), with BAL-treasury-asset redemptions to begin end of May 2027, and ~$52.4M still in V2/V3 pools. This corroborates (does not contradict) last week's wind-down timeline (contributor notice/bug-bounty wind-down through Oct 30; V3 vaults stop Nov 30, 2026; partner extension-request window closed Oct 16, 2026). No source confirms which pools, if any, received an extension, or whether the stETH/wstETH pools specifically are covered. Still **disqualified**.
Sources: [DefiLlama — Curve Finance yields](https://defillama.com/protocol/yields/curve-finance) (new this week, search-snippet only, source of the $79.77M/1.53% and $19.18M/1.94% figures), [DefiLlama — ETH-STETH pool](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (carried, direct fetch blocked again; source of the carried $76.87M/1.26%, $18.61M/1.41%, $16.47M/1.73% figures), [Balancer sets shutdown dates after BAL holders approve wind-down — The Defiant](https://thedefiant.io/news/defi/balancer-sets-shutdown-dates-after-bal-holders-approve-wind-down) (carried, source of the withdrawals-only/redemption dates and the ~$52.4M figure), [Balancer holders approve wind-down, reject official fork — The Defiant](https://thedefiant.io/news/defi/balancer-holders-approve-wind-down-reject-official-fork) (new this week, context on the BIP-928/BIP-929 vote).

### (e) Pendle PT-stETH
**Still not independently verifiable; now a seventeen-week-old gap.** `app.pendle.finance` remains unreachable directly. No reliable, dated, stETH-specific fixed-yield figure was found this week. Two new, low-confidence, non-primary depth signals surfaced — a September 2026 market cited at roughly $30M TVL/$3M daily volume/45bps spread, and a July 2026 snapshot of an individual stETH maturity at only ~$459k TVL — both suggesting depth below the ~$6M+ this report's trigger requires, but neither is sourced from Pendle or DefiLlama directly, so **neither is adopted as fact**. A generic, non-dated guide again estimates stETH fixed APY broadly at 4–5% with PT typically 1–3pp below spot — same low-confidence source type carried in prior reports, not adopted as an update. **The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 is now outstanding for seventeen consecutive weeks.**
Sources: [Pendle Finance fixed yield guide 2026](https://quakr-site.pages.dev/blog/pendle-finance-fixed-yield-guide-2026) (new this week, low-confidence, different market maturity than any currently open, not adopted), Pendle liquidity-fragmentation community gist (new this week, undated, unverified, source of the $30M TVL figure, context only), [Portals.fi — stETH 27AUG2026 market](https://explorer.portals.fi/yield/ethereum:0xba1cbaece600beec76dabc0a4ead31e0339cbe37.md) (new this week, dated July 11, 2026, source of the ~$459k TVL figure, context only; that market's maturity has likely passed), [DefiLlama — Pendle V2 protocol TVL](https://defillama.com/protocol/pendle-v2?staking=true) (new this week, $1.013B as of Oct 30, 2026, down 19.3% over 30 days, context only, not stETH-specific).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** appears to remain listed as an open Earn vault per title-level search results; direct fetch of `stake.lido.fi/earn/ggv/deposit` and `help.lido.fi` was blocked again this week, so live confirmation of open/closed status and current rate was not possible. No fresher dated TVL/APY figure was obtained; the same conflicting set from recent weeks stands (carried $98.6M/7.1% aggregator figure; a Nov-2025 Aave case study at ~40,000 ETH/>$175M/~5%; an undated Veda post at >$66M; an undated third-party post at $105M/10.6%). The carried **$98.6M TVL / 7.1% APY** figure is retained for modeling, now **14 consecutive weeks stale**. GGV remains disqualified on the health-factor test regardless of which rate figure is used, since external deployment leaves the vault's own stETH liability untouched.
Sources: [Lido Deposit | Earn stETH/WETH Vault Rewards with GGV — lido.today](https://lido.today/) (carried, source of the $98.6M/7.1% figure), [Aave × Lido case study](https://aave.com/blog/lido-aave-case-study) (carried, source of the Nov-2025 ~$175M/~5% figure), Veda (vault curator) social post (carried, source of the ">$66M" figure), third-party social post (carried, source of the "$105M/10.6%" figure), [Lido GGV vault blog post](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/) and [help.lido.fi GGV overview](https://help.lido.fi/en/articles/12148136-vault-overview-gg-vault-ggv) (titles found via search, pages unreachable this week).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** no reliable, dated November 2026 figure obtained. Search results this week were dominated by stale or inconsistent TVL claims spanning 2024–2026 ($240M–$20B range). AVS yield estimates cluster at 4–7% (3–4% base ETH staking plus 1–3% AVS rewards) per a mid-2026 guide, with analysts flagging AVS reward income as largely emissions-driven and likely to compress as subsidies roll off. Directionally consistent with the carried ~$4.67B/post-$15B-peak-pullback narrative but not independently reconfirmed this week.
**Symbiotic:** no 2026 ETH-restaking yield figure was found; the only figure surfaced was an unverified January 2026 BTC-restaking figure (7.8% APR, $850M TVL) from a low-quality aggregator, not applicable here.
**Not a lever** for this vault's own (non-restaked) validators, either way.
Sources: [EigenLayer restaking guide 2026](https://quakr-site.pages.dev/blog/eigenlayer-restaking-guide-2026) (new this week, source of the 4–7% AVS yield estimate), [EigenLayer AVS revenue reality check — blockeden.xyz, April 2026](https://blockeden.xyz/blog/2026/04/20/eigenlayer-avs-revenue-reality-check-15b-restaking/) (new this week, context on emissions-driven AVS revenue), KuCoin Square post on EigenLayer TVL and the Kelp DAO exploit (carried).

### (h) MEV relay market share
**No fresher-than-September-20 dated snapshot.** The most recent dated figures remain those "as of September 20, 2026" per relayscan.io (direct fetch blocked again): ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%. The carried August 29, 2026 shattered.io snapshot (Ultra Sound 35.1%, Titan 29.8%, bloXroute Regulated 23.8%, Flashbots 2.4%) also stands. An undated secondary glossary split (Ultrasound ~28%, bloXroute Max Profit ~23%, bloXroute Regulated ~20%, Titan ~17%, Aestus ~8%, Flashbots ~2%) was found this week but carries no citable date, so it is noted for context only and not adopted as superseding. No relay-registration action is indicated — the vault remains registered with ultrasound.money, Titan, and both bloXroute relays, all of which continue to show meaningful share across every reading found.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV boost relay censorship shift 2026 — shattered.io](https://shattered.io/mev-boost-relay-censorship-shift-2026/) (carried), undated secondary MEV-relay glossary (new this week, context only, not individually cited).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint, currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated; USD conversions are omitted (see Position Snapshot).

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the carried figure — now 14 weeks stale and contested by conflicting third-party reads (~5% to ~10.6% across sources) | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger under the carried figure, but fails the 150% HF floor by a wide margin regardless of which rate figure is used. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread remains at **~0.0pp** (stETH ~2.2% vs. Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); unchanged this week | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable: every reading found this week and in prior weeks (1.26%–1.94% across five conflicting sources) sits well below the ~2.34% cost of carry | Unchanged | Not viable on yield, independent of depth. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | **Disqualified**; October 30 withdrawals-only transition now confirmed to have taken effect without changing the status. |
| 6 | Pendle PT-stETH | Unverified | Unchanged | Data gap — manual check outstanding for seventeen weeks; new unconfirmed signals suggest thin depth. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable as a lever for this vault | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all. The ranking is unchanged, though the infrastructure-fee-waiver uncertainty (see above) is a live risk to every figure in this table if it resolves to a lapse.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-11-02) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Remains at ~0.0pp (stETH ~2.2%, Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); unchanged this week. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV under the carried 7.1% figure (now 14 weeks stale and contested by conflicting reads), but disqualified separately on the 150% HF-floor test regardless of rate. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (general commentary only); **depth leg still unverifiable**, and new unconfirmed secondary signals this week (sub-$30M pool-level TVL) point toward insufficient depth if taken at face value. Manual check of `app.pendle.finance/trade/markets` remains outstanding after seventeen weeks and is the only way to get a currently-valid reading. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `research.lido.fi`, `blog.lido.fi`, `defillama.com`, `cryptobriefing.com`, `governance.aave.com`, `aavescan.com`, `stake.lido.fi`, `help.lido.fi`, `app.pendle.finance`, and `www.relayscan.io` all returned DNS-resolution errors on direct fetch this week — the same set of domains blocked in prior weeks.
- As a result, this week's figures — including the Curve pool-depth reads, the Balancer withdrawals-only confirmation, the Aave/Morpho rate reads, the GGV status check, the Pendle depth signals, the MEV relay shares, and the EigenLayer/Symbiotic context — are drawn from search-engine snippets rather than live dashboard or primary-source pulls, with the same reliability caveats as prior weeks.
- **New this week — the infrastructure-fee waiver's secondary-sourced expiry date (October 31, 2026) has passed with no resolution found.** This is the highest-priority open item in this report; see Changes Since Last Week and Vault Parameters & Fee Structure. A direct client reconfirmation of the current fee rate, and/or a primary Lido governance or monthly-update post covering September–October 2026, would resolve this.
- **Carried forward — Curve TVL/APY sourcing remains noisy:** five mutually inconsistent, mostly undated readings (1.26%–1.94% APY) are now on file. Does not change the strategy's not-viable conclusion either way, since every reading is far below the ~2.34% cost of carry.
- **Carried forward — Balancer's stETH/wstETH-pool-specific extension outcome remains unconfirmed** despite the October 30 withdrawals-only transition now being confirmed to have taken effect — no source states which, if any, pools (including stETH/wstETH) received an extension past the October 16 request deadline.
- **Carried forward — ETH/USD price data quality:** prior weeks' searches returned inconsistent ETH/USD figures; USD conversions remain omitted and were not re-attempted this week. All figures in this report stay ETH-denominated.
- **Carried forward:** no fresh client reconfirmation of the infrastructure fee was sought this week (this refresh ran without a client-contact step); the last confirmed status (0%, as of 2026-09-28) is carried forward pending the reconfirmation flagged above as top priority.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward and are now over sixteen weeks old.
- A manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY/TVL/open status), `app.aave.com/markets/?marketName=proto_lido_v3` (Prime/Lido-instance WETH/wstETH rate), `defillama.com` (Curve pool TVL/APY), `app.pendle.finance/trade/markets` (current, non-expired PT-stETH maturities, yields, and depth), and `research.lido.fi`/`blog.lido.fi` (infrastructure-fee status) would materially firm up several figures this report has had to estimate indirectly — the fee-status check is now the most urgent of these.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

**No change to the recommended strategy this week.** Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry.

**The item requiring the closest attention next week is the infrastructure-fee waiver, whose secondary-sourced October 31, 2026 expiry has now passed without any confirmation of outcome.** No primary governance post or monthly update confirming either a further extension or a lapse was found this week. Since every client-facing figure in this report assumes the fee is still 0%, this gap should be resolved as a priority — ideally through direct client reconfirmation — and the ranking table revisited immediately if the fee is found to have reverted to the standard rate, since that would reduce internal re-staking's net yield advantage without changing which option is recommended.

**Several items added detail without changing conclusions:** Curve stETH/ETH pool readings grew more numerous (now five conflicting figures, 1.26%–1.94% APY) without approaching the ~2.34% cost-of-carry hurdle. The Balancer wind-down's October 30 withdrawals-only transition is now confirmed to have taken effect, without resolving whether the stETH/wstETH pools specifically received an extension — the venue remains disqualified on the standing post-exploit basis regardless. Pendle gained two low-confidence depth signals suggesting thin liquidity, but neither is reliable enough to close the data gap.

**Outstanding items for next week:** direct client reconfirmation of the infrastructure fee is now the top priority. The manual check of `app.pendle.finance/trade/markets` is now seventeen weeks overdue. The Balancer stETH/wstETH-pool extension outcome should be checked for a result if one surfaces. A fresh Lido dashboard pull for the vault's own position is also overdue — the position snapshot in this report is now based on data from nearly four months ago. A direct `stake.lido.fi/earn/ggv` check would help resolve the ongoing GGV rate and open/closed-status uncertainty, and a direct DefiLlama pull would help resolve the growing Curve pool-reading conflict.
