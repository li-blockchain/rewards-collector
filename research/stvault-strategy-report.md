# stVault Weekly Strategy Report — 2026-10-19

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-10-12 report:

- **Recommendation and ranking: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.
- **Infrastructure fee / Early Adopters campaign: no new primary confirmation; the secondary-sourced expiry date is now 12 days away.** This week's search again surfaced only the same two secondary sources (CryptoBriefing, and CryptoSlate's coverage of Lido's August operator update) for the October 31, 2026 expiry, and the same primary forum thread confirming extensions only through August 31, 2026. No further extension, lapse, or DAO action was found. October 31 is now **12 days away** (vs. 19 days last week) — the standing recommendation to directly recheck this once the date passes still stands. No change to the modeling assumption (0% fee, last directly confirmed by the client as of 2026-09-28).
- **Lido V3 tier parameters: unresolved, unchanged.** No DAO governance action changing stVault tier parameters was found this week. The same third-party conflict on Tier 1's reserve ratio (2.5%/48,750 stETH per some sources, including a Kiln disclosure, vs. 5%/47,500 stETH per the vault's own confirmed figures and other sources) persists with no new information either way. This report continues to model the vault's own **confirmed** parameters.
- **Curve stETH/ETH pool: the lower TVL reading from last week repeated rather than reverting, suggesting it may be a new baseline rather than one-off noise.** This week's DefiLlama pool-specific read again shows **$76.87M TVL / 1.26% APY** — effectively identical to last week's reading of the same figure, and still well below the ~$108–110M this report carried for months before that. Two consecutive weeks at the lower level raises confidence that this reflects actual pool TVL rather than a transient sampling artifact, though no source provides an explicit snapshot date to confirm the trend directly. **No ranking change**: the pool's ~1.26% base APR remains far below the vault's ~2.34% cost of carry, so the strategy continues to fail on yield regardless of the exact depth reading.
- **Balancer wind-down: additional operational detail, and the pool-extension-request deadline mentioned last week has now passed.** This week's search adds detail to the wind-down timeline: contributor notice runs through October 31, 2026; from November 1, 2026 only a minimal stack needed to process exits remains. The October 16, 2026 deadline for partners to request an extension for certain v3 pools (noted last week) has now passed as of this report's date (October 19) — no source found confirming which, if any, pools received an extension, or whether the stETH/wstETH pools specifically are among them. Still **disqualified** on the standing post-exploit basis; this additional detail does not change the ranking.
- **Lido GGV curated vault: additional conflicting figures surfaced, none dated to the present and none changing the ranking.** This week's search turned up three further data points, none superseding the carried figure: an Aave case study citing GGV at ~40,000 ETH / >$175M TVL / ~5% average APY as of **November 2025**; an undated Veda (the vault's curator) post citing ">$66M TVL"; and an undated third-party social post citing "$105M TVL / ~10.6% APY." These conflict with each other and with the carried **$98.6M TVL / 7.1% APY** figure (itself undated, sourced from an aggregator page) by wide margins, with no way to establish which is most current. The carried figure is retained for modeling, now **12 consecutive weeks stale**. GGV remains disqualified on the health-factor test regardless of which rate figure is used.
- **Aave Prime/Lido-instance WETH borrow, Morpho loop: no material change, with two new data points considered and rejected as non-superseding.** (1) Aavescan's wstETH-market page shows a borrow APR of ~0.19% and supply APR of ~0.05% (dated May 13, 2026) — this is the rate for *borrowing* wstETH itself, a different market from the WETH-borrow leg this report models for the leverage loop, and does not supersede the carried Prime-instance WETH figures. (2) A TokenLogic governance proposal dated August 26, 2026 recommends reducing the WETH Slope1 target to 2.20% (from 2.35%) on Aave's **Core** instance (plus Arbitrum and Optimism) — this is scoped to Core, not confirmed for the Prime/Lido instance this vault's loop analysis uses, and the proposal's passage status is unconfirmed. Neither point is adopted into modeling. The carried net borrowing cost (~2.20%, from the ~August 2026 Chaos Labs Risk Steward adjustment) and ~0.0pp spread against stETH APR stand. Morpho: no fresher Aave-Prime/Lido-instance-specific loop figure found.
- **EigenLayer: a new yield-range estimate surfaces as context, without changing the vault's exposure.** A Lambda Finance breakdown (dated April 2026) estimates a net restaker yield of roughly 5.5–8.0% after operator commission and LRT fees (base staking ~3.0–3.5% + AVS rewards ~2.0–5.0% + EIGEN incentives ~1.0–2.0%), which is new granularity but not a fresher TVL reading — the carried ~$4.67B current TVL figure is unchanged. **Still not a lever** for this vault's own non-restaked validators.
- **MEV relay market share: an additional undated, conflicting source found; no fresher-than-September-20 dated figure obtained.** Rated's "PBS landscape" page (undated) shows yet another breakdown (Ultra Sound Money 30.56%, bloXroute Max Profit 24.93%, bloXroute Regulated 17.55%, Agnostic 14.38%, Flashbots 6.82%, Titan 2.85%, Aestus 2.54%) that conflicts with both of the dated figures already on file. Since it carries no date, it is not treated as superseding either the September 20 relayscan.io reading (ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%) or the August 29 shattered.io reading, both of which are carried forward unchanged. No relay-registration action is indicated.
- **Pendle, stETH APR, ETH/USD price, Symbiotic: no material change.** Pendle remains unreachable; the manual `app.pendle.finance` check is now outstanding for **fifteen** consecutive weeks. stETH APR modeling assumption stays at ~2.2%. ETH/USD searches again returned inconsistent figures spanning roughly $2,060–$4,500 with no reliable current-dated quote; USD conversions remain omitted. Symbiotic: no yield or TVL figure newer than the already-carried Core V2 pivot; not operationally scoped for this vault either way.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was again not obtained this week — now over fourteen weeks stale; see Data Gaps). **USD conversions are omitted this week** due to unreliable ETH/USD price reads (see Changes Since Last Week); all figures are ETH-denominated, consistent with this report's methodology, under which the underlying ETH/stETH position figures are unaffected by any price move.

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
- **Infrastructure fee:** last directly confirmed by the client at **0%** as of 2026-09-28; no fresher client confirmation sought this week. The primary Lido governance forum confirms extensions through August 31, 2026; a further extension to October 31, 2026 continues to rest on two corroborating secondary sources only (CryptoBriefing, CryptoSlate), not a primary post. October 31 is now 12 days away. All figures below are modeled on the last-confirmed 0% fee.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, unchanged from prior reports.

---

## Rate Environment (as of 2026-10-19)

### (a) Lido DAO / stVault governance and fees
No DAO governance action changing stVault tier parameters was found this week. The primary forum thread for the Early Adopters campaign still confirms extensions only through August 31, 2026; the commonly-cited October 31, 2026 expiry remains secondary-sourced only (same two sources as last week), now 12 days out. The third-party discrepancy on Tier 1's reserve ratio (2.5% vs. 5% across sources) remains unresolved but does not change the vault's own confirmed parameters.
Sources: [Default risk assessment framework and fees parameters for Lido V3 (stVaults) — Lido Governance](https://research.lido.fi/t/default-risk-assessment-framework-and-fees-parameters-for-lido-v3-stvaults/10504) (direct fetch blocked again; extension chain through Aug 31, 2026 per search), [Lido extends 0% infrastructure fee for stVaults until October 31 — CryptoBriefing](https://cryptobriefing.com/lido-stvaults-zero-fee-extension-october/) (carried), CryptoSlate coverage of Lido's August operator update (carried, same Oct 31 claim), [lido v3 introduces tier based staking credit rating — 4Pillars](https://research.4pillars.io/en/research/lido-v3-introduces-tier-based-staking-credit-rating) (carried), Kiln stVaults risk disclosure (carried, source of the conflicting 2.5%/48,750 stETH figure — [docs.kiln.fi](https://docs.kiln.fi/v1/kiln-products/onchain/lido-v3-kiln-stvaults/smart-contract-and-protocol-risk-disclosure-kiln-stvaults-on-lido-v3)), [stVaults Monthly Updates: April 2026 — Lido blog](https://blog.lido.fi/stvaults-monthly-updates-april-2026/) (new this week, no September/October 2026 edition found; context only on the fee-waiver rationale — tied to validator entry-queue length).

### (b) stETH APR
Best estimate for modeling: **~2.2% net-to-holders**, unchanged. This week's search again surfaced only generic, non-Lido-specific marketing figures (~3–4%, one from a non-authoritative domain); not treated as an update. Modeling assumption stays anchored to the vault's own confirmed dashboard figure (2.24% net).
Sources: generic staking-yield marketing pages (not cited individually; low-confidence), [Ethereum Staking Weekly Report, September 7, 2026 — Bitget](https://www.bitget.com/news/detail/12560605798507) (carried).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher instance-specific figure this week. An Aavescan wstETH-market read (borrow ~0.19% / supply ~0.05%, as of May 13, 2026) was found but is for a different market (borrowing wstETH itself) than the WETH-borrow leg this report models, and does not supersede the carried ~August 2026 Chaos Labs Risk Steward adjustment (WETH borrow 3.30%, offset by wstETH supply 1.10%, net borrowing cost ~2.20%). A separate TokenLogic proposal (August 26, 2026) recommending a lower WETH Slope1 target (2.20%, down from 2.35%) is scoped to Aave's Core instance, not confirmed for Prime/Lido, and its passage status is unconfirmed — not adopted into modeling. Against the carried stETH APR (~2.2%), the spread remains **~0.0pp**.
**Morpho:** no Aave-Prime/Lido-instance-specific loop figure found this week. Carried tracker (dated August 12, 2026) unchanged.
Net read: re-entry trigger **not met**, unchanged from last week.
Sources: [Aavescan — Prime V3 wstETH](https://aavescan.com/prime-v3/wsteth) (referenced via search, direct fetch blocked; source of the non-comparable 0.19%/0.05% figures), [ARFC Prime Instance — wstETH Borrow Rate + rsETH Supply Cap Update — Aave Governance](https://governance.aave.com/t/arfc-prime-instance-wsteth-borrow-rate-rseth-supply-cap-update/20644) (carried; source of the 3.30%/1.10%/2.20% figures), [Risk Stewards — August 2026 WETH Interest Rate Adjustment — Aave Governance](https://governance.aave.com/t/risk-stewards-august-2026-weth-interest-rate-adjustment/25535) (new this week, direct fetch blocked; source of the Core-instance 2.20%-Slope1 proposal, context only), [Failed to understand source of yield for Flagship ETH vaults — Morpho Forum](https://forum.morpho.org/t/failed-to-understand-source-of-yield-for-flagship-eth-vaults/1236) (carried, context only).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** this week's DefiLlama pool-specific read again shows **$76.87M TVL / 1.26% APY** — unchanged from last week's reading of the same figure, and the second consecutive week at this level rather than the ~$108–110M carried for months before. No source provides an explicit snapshot date to confirm whether this is a stable new baseline or still-noisy sampling, but the repetition raises confidence it is not a one-off anomaly. **Still not viable regardless of the exact TVL figure**: the pool's ~1.26% base APR remains well below the vault's ~2.34% cost of carry.
**Balancer:** following the September 29, 2026 wind-down vote (passed), this week's search adds operational detail: contributor notice runs through October 31, 2026; from November 1, 2026 only a minimal stack needed to process exits remains. The October 16, 2026 deadline for certain v3-pool extension requests (noted last week) has now passed; no source confirms which pools, if any, received an extension, or whether the stETH/wstETH pools specifically are covered. Still **disqualified**.
Sources: [Yield Chart - ETH-STETH(Curve DEX) — DefiLlama](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (carried, direct fetch blocked again; source of the $76.87M/1.26% figure), [Balancer sets shutdown dates after BAL holders approve wind-down — The Defiant](https://thedefiant.io/news/defi/balancer-sets-shutdown-dates-after-bal-holders-approve-wind-down) (carried, source of the Oct 30/Oct 16/Nov 30 dates), [Balancer Proposes Wind-Down, $9M Treasury to Go to BAL Holders — KuCoin](https://www.kucoin.com/news/flash/balancer-proposes-wind-down-9m-treasury-to-go-to-bal-holders) (carried, source of the Oct 31 contributor-notice / Nov 1 minimal-stack detail).

### (e) Pendle PT-stETH
**Still not independently verifiable.** No fresher PT-stETH-specific maturity, liquidity, or yield figure was found this week; `app.pendle.finance` remains unreachable directly. This week's search again returned only general Pendle mechanics and the same previously-identified stale "~$8M liquidity, September 2026 maturity" figure. **The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 is now outstanding for fifteen consecutive weeks.**
Sources: [Pendle Finance: Yield Tokenization Works — Medium](https://medium.com/@pycheng9/pendle-finance-yield-tokenization-87791c922cca) (carried, understood to be the stale source), general Pendle-mechanics pages (new this week, context only, not cited individually).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked again). This week's search surfaced three additional, mutually conflicting figures — an Aave case study (~40,000 ETH / >$175M TVL / ~5% APY, dated **November 2025**), an undated Veda post (">$66M TVL"), and an undated third-party post ("$105M TVL / ~10.6% APY") — none of which carries a clearer current date than the already-carried aggregator figure. The carried **$98.6M TVL / 7.1% APY** figure is retained for modeling, now **12 consecutive weeks stale**. GGV remains disqualified on the health-factor test regardless of which rate figure is used.
Sources: [Lido Deposit | Earn stETH/WETH Vault Rewards with GGV — lido.today](https://lido.today/) (carried, source of the $98.6M/7.1% figure), Aave × Lido case study (new this week, source of the Nov-2025 $175M/~5% figure — [aave.com/blog/lido-aave-case-study](https://aave.com/blog/lido-aave-case-study)), Veda (vault curator) social post (new this week, source of the ">$66M" figure), third-party social post (new this week, source of the "$105M/10.6%" figure), [Lido EarnETH vault maintains fee structure as TVL reaches $196 million — CryptoBriefing](https://cryptobriefing.com/lido-earneth-vault-fee-update/) (carried, EarnETH meta-vault context only, not GGV-specific).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** current TVL reading carried at ~$4.67B, unchanged. A new Lambda Finance estimate (dated April 2026) breaks down a net restaker yield of roughly **5.5–8.0%** after operator commission and LRT fees — new granularity, not a fresher TVL figure, and the underlying AVS-demand sustainability concerns already on file are unchanged. **Not a lever** for this vault's own (non-restaked) validators.
**Symbiotic:** no figure newer than the already-carried Core V2 "collateral markets" pivot (July 1, 2026) was found. Not operationally scoped for this vault either way.
Sources: Lambda Finance restaking breakdown (new this week, source of the 5.5–8.0% net-yield estimate), KuCoin Square post on EigenLayer TVL and the Kelp DAO exploit (carried), [Symbiotic officially pivots to collateral markets with Core V2 launch — The Block](https://www.theblock.co/post/406862/symbiotic-officially-pivots-to-collateral-markets-with-core-v2-launch) (carried).

### (h) MEV relay market share
**No fresher-than-September-20 dated snapshot.** The most recent dated figures remain those "as of September 20, 2026" per relayscan.io (direct fetch blocked again): ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%. This week's search surfaced an additional undated source (Rated's "PBS landscape" page: Ultra Sound Money 30.56%, bloXroute Max Profit 24.93%, bloXroute Regulated 17.55%, Agnostic 14.38%, Flashbots 6.82%, Titan 2.85%, Aestus 2.54%) that conflicts with both figures already on file; carrying no date, it is not treated as superseding either one. No relay-registration action is indicated — the vault remains registered with ultrasound.money, Titan, and both bloXroute relays, all of which continue to show meaningful share across every reading found.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV boost relay censorship shift 2026 — shattered.io](https://shattered.io/mev-boost-relay-censorship-shift-2026/) (carried), [PBS Landscape — Rated](https://rated.network/relays) (new this week, undated, context only).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint, currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated; USD conversions are omitted this week (see Changes Since Last Week).

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the carried figure — now 12 weeks stale and increasingly contested by conflicting third-party reads (~5% to ~10.6% across sources this week) | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger under the carried figure, but fails the 150% HF floor by a wide margin regardless of which rate figure is used. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread remains at **~0.0pp** (stETH ~2.2% vs. Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); unchanged this week | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable: pool yield (~1.26% on this week's reading, repeated from last week) sits well below the ~2.34% cost of carry | Unchanged | Not viable on yield, independent of depth. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | **Disqualified**; wind-down timeline firmed up further this week (contributor notice through Oct 31; minimal stack only from Nov 1) without changing the status. |
| 6 | Pendle PT-stETH | Unverified | Unchanged | Data gap — manual check outstanding for fifteen weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable as a lever for this vault; EigenLayer TVL remains at ~$4.67B | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all. The Curve TVL reading held at last week's lower level rather than reverting, and the Balancer wind-down timeline gained more operational detail — neither changes the underlying conclusions.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-10-19) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Remains at ~0.0pp (stETH ~2.2%, Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); unchanged this week. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV under the carried 7.1% figure (now 12 weeks stale and increasingly contested by conflicting reads this week), but disqualified separately on the 150% HF-floor test regardless of rate. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (general commentary only); **depth leg still unverifiable**. Manual check of `app.pendle.finance/trade/markets` remains outstanding after fifteen weeks and is the only way to get a currently-valid reading. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `research.lido.fi`, `defillama.com`, `cryptobriefing.com`, `governance.aave.com`, `aavescan.com`, `stake.lido.fi`, `app.pendle.finance`, and `www.relayscan.io` all returned DNS-resolution errors on direct fetch this week — the same set of domains blocked in prior weeks.
- As a result, this week's figures — including the Curve TVL read, the Balancer wind-down detail, the Aave rate reads, the GGV conflicting figures, the MEV relay shares, and the EigenLayer yield breakdown — are drawn from search-engine snippets rather than live dashboard or primary-source pulls, with the same reliability caveats as prior weeks.
- **New this week — Curve TVL reading repeated rather than reverting:** the ~$76.87M/1.26% figure matches last week's reading exactly, raising (but not confirming, absent an explicit snapshot date) the likelihood that this is a stable new baseline rather than sampling noise. Does not change the strategy's not-viable conclusion either way.
- **New this week — Balancer's pool-extension-request deadline (Oct 16, 2026) has passed with no outcome confirmed:** no source found on which, if any, v3 pools received an extension to November 30, or whether the stETH/wstETH pools specifically are covered.
- **New this week — GGV sourcing has become more contested, not less:** three additional figures (ranging ~5% to ~10.6% APY, and ~$66M to ~$175M TVL, across three different undated-or-stale sources) surfaced alongside the already-carried $98.6M/7.1% figure, with no way to establish which is current. Does not change GGV's disqualification on the health-factor test.
- **New this week — a TokenLogic Aave Core-instance rate proposal (Aug 26, 2026) was found but not adopted:** it targets Core, not the Prime/Lido instance this report models, and its passage status is unconfirmed.
- **Carried forward — ETH/USD price data quality:** searches this week again returned inconsistent ETH/USD figures (~$2,060–$4,500 across sources, no reliable live quote with a confirmed current date). USD conversions remain omitted; all figures in this report stay ETH-denominated.
- **Carried forward:** no fresh client reconfirmation of the infrastructure fee was sought this week (this refresh ran without a client-contact step); the last confirmed status (0%, as of 2026-09-28) is carried forward. A manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY/TVL — now a particular priority given this week's conflicting third-party figures), `app.aave.com/markets/?marketName=proto_lido_v3` (Prime/Lido-instance WETH/wstETH rate), `defillama.com` (Curve pool TVL, to confirm whether the lower reading is a stable baseline), and `app.pendle.finance/trade/markets` (current, non-expired PT-stETH maturities, yields, and depth) would materially firm up several figures this report has had to estimate indirectly.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward and are now over fourteen weeks old.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

**No change to the recommended strategy this week.** Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry.

**Two items firmed up this week without changing the ranking:** the Curve stETH/ETH pool's TVL reading repeated last week's lower figure (~$76.87M/1.26%) rather than reverting, which raises confidence it reflects a real level rather than noise — but the venue was already disqualified on yield, not depth, so this has no practical effect. The Balancer wind-down timeline gained more detail (contributor notice through Oct 31; minimal stack only from Nov 1), and its pool-extension-request deadline has now passed with no outcome confirmed — this firms up the picture without changing the disqualified status.

**One item became more uncertain, not less:** the Lido GGV curated vault's rate and TVL figures are now contested across four mutually inconsistent sources (ranging ~5–10.6% APY). This does not affect the ranking, since GGV already fails the 150% health-factor floor regardless of the exact yield figure, but it does mean the carried 7.1% figure used in the trigger table should be treated with somewhat lower confidence than before.

**Outstanding items for next week:** the manual check of `app.pendle.finance/trade/markets` is now fifteen weeks overdue. The infrastructure-fee waiver's secondary-sourced October 31, 2026 expiry is 12 days away and should be directly rechecked once it passes, regardless of whether a fresh client confirmation is obtained before then. The Balancer extension-request outcome (due Oct 16) should be checked for a result. A fresh Lido dashboard pull for the vault's own position is also overdue — the position snapshot in this report is now based on data from over three months ago. A direct `stake.lido.fi/earn/ggv` check would help resolve this week's conflicting GGV figures.
