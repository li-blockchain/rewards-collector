# stVault Weekly Strategy Report — 2026-10-26

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-10-19 report:

- **Recommendation and ranking: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.
- **Infrastructure fee / Early Adopters campaign: no new primary confirmation, and the secondary-sourced expiry date is now imminent — 5 days away.** This week's search surfaced a May–June 2026 Lido stVaults Digest that explicitly restates the Aug 31, 2026 extension language (and lists new stVault ecosystem products — Luganodes, Nansen ETH Vault, Pro-Delegators, Linea Yield Boost — context only, not vault-specific), but still no primary governance post confirming the commonly-cited October 31, 2026 expiry beyond the same two secondary sources (CryptoBriefing, CryptoSlate) already on file. No September or October 2026 monthly update was found. **October 31 is now 5 days away** (vs. 12 last week) — this is the single most time-sensitive item in this report and should be directly rechecked at the next refresh regardless of outcome. No change to the modeling assumption (0% fee, last directly confirmed by the client as of 2026-09-28).
- **Lido V3 tier parameters: unresolved, unchanged.** No DAO governance action changing stVault tier parameters was found this week. The third-party conflict on Tier 1's reserve ratio (2.5%/48,750 stETH per Kiln vs. 5%/47,500 stETH per the vault's own confirmed figures) persists with no new information either way. This report continues to model the vault's own **confirmed** parameters.
- **Curve stETH/ETH pool: source noise increased rather than resolving.** Two new, mutually inconsistent, undated pool-level snapshots surfaced this week ($18.61M TVL / 1.41% APY and $16.47M TVL / 1.73% APY), both below the $76.87M/1.26% figure carried for the prior two weeks. None of the three readings can be reconciled without a direct DefiLlama pull (still unreachable). **No ranking change**: every reading found, across all three figures, sits well below the vault's ~2.34% cost of carry, so the strategy continues to fail on yield regardless of which depth/APY figure is most current.
- **Balancer wind-down: vote mechanics now confirmed in more detail; the stETH/wstETH-pool-specific extension outcome remains unresolved.** This week's search confirms the wind-down vote (BIP-928) passed with over 99% of ~17.2M BAL voting, and a competing continuation proposal (BIP-929) was rejected (~70% against). Confirmed dates are unchanged from last week: contributor notice / bug-bounty wind-down through October 30, 2026; V3 vaults stop November 30, 2026; the partner extension-request window closed October 16, 2026. **No source found on whether the stETH/wstETH pools specifically received an extension** — same gap as last week. Still **disqualified** on the standing post-exploit basis; this additional detail does not change the ranking.
- **Lido GGV curated vault: clarifying detail, not a fresher rate.** This week's search adds fee-structure detail (10% performance fee + 1% platform fee on GGV) and flags that a widely-circulated "$196M TVL / ~4.33% APY" figure belongs to Lido's **EarnETH meta-vault** (which wraps GGV plus stRATEGY), not to GGV itself — a clarification that should prevent future misattribution, not a new GGV-specific figure. The carried **$98.6M TVL / 7.1% APY** figure is retained for modeling, now **13 consecutive weeks stale**. GGV remains disqualified on the health-factor test regardless of which rate figure is used.
- **Aave Prime/Lido-instance WETH borrow, Morpho loop: no material change, with two new data points considered and rejected as non-superseding.** (1) A new Aave governance thread ("[Risk Stewards] - Reduce wstETH Borrow Rate") surfaced but is undated/unconfirmed for passage — not adopted. (2) A Staking Rewards snapshot shows a Morpho "Alpha WETH Core" vault at 3.78% net APY (down from 4.76% trailing 30d) — this is a WETH-supply vault, not the wstETH/ETH leverage loop this report models, and is undated for instance-specificity — does not supersede. The carried net borrowing cost (~2.20%) and ~0.0pp spread against stETH APR stand.
- **EigenLayer: TVL pullback narrative gets a fresher corroborating source, no new number.** An October-2026-dated review corroborates the already-carried ~$4.67B TVL figure and describes a sharp pullback from the >$15B 2026 peak, consistent with prior reporting. New token-economics color (EIGEN ~$0.27, ~$258M market cap, ~$80M cumulative AVS rewards distributed with payouts slowing) is context only. **Still not a lever** for this vault's own non-restaked validators.
- **MEV relay market share: a second dated-but-still-not-current secondary source, no action implied.** A shattered.io 24-hour snapshot dated August 29, 2026 shows Ultra Sound 35.1%, Titan 29.8%, bloXroute Regulated 23.8%, Flashbots 2.4% — close in ordering but numerically distinct from the already-carried September 20, 2026 relayscan.io figure (ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%). Neither is current to this week; both are retained as distinct dated data points. The undated Rated.network "PBS Landscape" figure (carried, conflicting) is unchanged. No relay-registration action is indicated.
- **Pendle, stETH APR, ETH/USD price, Symbiotic: no material change.** Pendle remains unreachable; the manual `app.pendle.finance` check is now outstanding for **sixteen** consecutive weeks. stETH APR modeling assumption stays at ~2.2% (one new MEXC marketing page cites ~2.6% post-fee, closer to the vault's own measured gross yield but not a Lido-sourced, dated figure — not adopted). ETH/USD searches again returned inconsistent figures ($2,060–$4,500, one weakly dated at ~$2,694); USD conversions remain omitted. Symbiotic: all search results this week were 2024/2025-dated; no 2026 figure found; not operationally scoped for this vault either way.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was again not obtained this week — now over fifteen weeks stale; see Data Gaps). **USD conversions are omitted this week** due to unreliable ETH/USD price reads (see Changes Since Last Week); all figures are ETH-denominated, consistent with this report's methodology, under which the underlying ETH/stETH position figures are unaffected by any price move.

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
- **Infrastructure fee:** last directly confirmed by the client at **0%** as of 2026-09-28; no fresher client confirmation sought this week. The primary Lido governance forum confirms extensions through August 31, 2026; a further extension to October 31, 2026 continues to rest on two corroborating secondary sources only (CryptoBriefing, CryptoSlate), not a primary post. **October 31 is now 5 days away** — this should be directly rechecked once it passes. All figures below are modeled on the last-confirmed 0% fee.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, unchanged from prior reports.

---

## Rate Environment (as of 2026-10-26)

### (a) Lido DAO / stVault governance and fees
No DAO governance action changing stVault tier parameters was found this week. A May–June 2026 Lido stVaults Digest corroborates the Aug 31, 2026 extension language already on file and adds ecosystem context (new stVault products: Luganodes, Nansen ETH Vault, Pro-Delegators, Linea Yield Boost) but does not touch this vault's parameters. The commonly-cited October 31, 2026 expiry remains secondary-sourced only (same two sources as last week), now **5 days out**. The third-party discrepancy on Tier 1's reserve ratio (2.5% vs. 5% across sources) remains unresolved but does not change the vault's own confirmed parameters.
Sources: [Default risk assessment framework and fees parameters for Lido V3 (stVaults) — Lido Governance](https://research.lido.fi/t/default-risk-assessment-framework-and-fees-parameters-for-lido-v3-stvaults/10504) (direct fetch blocked again; extension chain through Aug 31, 2026 per search), [Lido extends 0% infrastructure fee for stVaults until October 31 — CryptoBriefing](https://cryptobriefing.com/lido-stvaults-zero-fee-extension-october/) (carried), CryptoSlate coverage of Lido's August operator update (carried, same Oct 31 claim), [lido v3 introduces tier based staking credit rating — 4Pillars](https://research.4pillars.io/en/research/lido-v3-introduces-tier-based-staking-credit-rating) (carried), Kiln stVaults risk disclosure (carried, source of the conflicting 2.5%/48,750 stETH figure — [docs.kiln.fi](https://docs.kiln.fi/v1/kiln-products/onchain/lido-v3-kiln-stvaults/smart-contract-and-protocol-risk-disclosure-kiln-stvaults-on-lido-v3)), [stVaults Monthly Updates: May-June 2026 — Lido blog](https://blog.lido.fi/stvaults-monthly-updates-may-june-2026/) (new this week, no September/October 2026 edition found; context only on new stVault products and the Aug 31 extension restatement).

### (b) stETH APR
Best estimate for modeling: **~2.2% net-to-holders**, unchanged. This week's search again surfaced generic, non-Lido-specific marketing figures (~3–4% from a non-authoritative domain, and a ~2.6% post-fee figure from an exchange marketing page); neither is treated as an update. Modeling assumption stays anchored to the vault's own confirmed dashboard figure (2.24% net).
Sources: generic staking-yield marketing pages and an exchange marketing page citing ~2.6% post-fee (not cited individually; low-confidence), [Ethereum Staking Weekly Report, September 7, 2026 — Bitget](https://www.bitget.com/news/detail/12560605798507) (carried).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher instance-specific figure this week. A new Aave governance thread titled "[Risk Stewards] - Reduce wstETH Borrow Rate" was found but is undated for this report's purposes and its passage status is unconfirmed — not adopted into modeling. The carried net borrowing cost (~2.20%, from the ~August 2026 Chaos Labs Risk Steward adjustment: WETH borrow 3.30%, offset by wstETH supply 1.10%) stands. Against the carried stETH APR (~2.2%), the spread remains **~0.0pp**.
**Morpho:** a Staking Rewards snapshot shows a Morpho "Alpha WETH Core" vault at 3.78% net APY (down from 4.76% trailing 30 days) — this is a WETH-supply vault, not the wstETH/ETH leverage loop this report models, and is not confirmed Aave-Prime/Lido-instance-specific. Not adopted; carried tracker (dated August 12, 2026) unchanged.
Net read: re-entry trigger **not met**, unchanged from last week.
Sources: [Aavescan — Prime V3 wstETH](https://aavescan.com/prime-v3/wsteth) (referenced via search, direct fetch blocked), [ARFC Prime Instance — wstETH Borrow Rate + rsETH Supply Cap Update — Aave Governance](https://governance.aave.com/t/arfc-prime-instance-wsteth-borrow-rate-rseth-supply-cap-update/20644) (carried; source of the 3.30%/1.10%/2.20% figures), "[Risk Stewards] - Reduce wstETH Borrow Rate — Aave Governance" (new this week, direct fetch blocked, undated passage status, context only — [governance.aave.com/t/risk-stewards-reduce-wsteth-borrow-rate/21196](https://governance.aave.com/t/risk-stewards-reduce-wsteth-borrow-rate/21196)), Staking Rewards — Morpho Alpha WETH Core vault (new this week, source of the 3.78%/4.76% figures, not instance-confirmed), [Failed to understand source of yield for Flagship ETH vaults — Morpho Forum](https://forum.morpho.org/t/failed-to-understand-source-of-yield-for-flagship-eth-vaults/1236) (carried, context only).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** direct DefiLlama pool-page fetch blocked again this week. Two new, mutually inconsistent, undated pool-level snapshots surfaced ($18.61M TVL / 1.41% APY and $16.47M TVL / 1.73% APY), both below the $76.87M/1.26% figure carried for the prior two weeks; a protocol-level DefiLlama read shows overall Curve DEX TVL at ~$1.219B as of October 16, 2026 (down from ~$1.3B late September) as directional context only, not pool-specific. None of the three pool-level readings can be reconciled without a direct pull. **Still not viable regardless of the exact TVL or APY figure**: every reading found sits well below the vault's ~2.34% cost of carry.
**Balancer:** the wind-down vote (BIP-928) is confirmed to have passed with over 99% of ~17.2M BAL voting; a competing continuation proposal (BIP-929) was rejected (~70% against). Confirmed dates, unchanged from last week: contributor notice / bug-bounty wind-down through October 30, 2026; V3 vaults stop November 30, 2026; the partner extension-request window closed October 16, 2026. No source confirms which pools, if any, received an extension, or whether the stETH/wstETH pools specifically are covered. Still **disqualified**.
Sources: [Yield Chart - ETH-STETH(Curve DEX) — DefiLlama](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (carried, direct fetch blocked again; source of the carried $76.87M/1.26% figure), DefiLlama protocol-level Curve DEX TVL read (new this week, ~$1.219B as of Oct 16, 2026, context only), two undated third-party stETH-ng-f pool snapshots (new this week, source of the $18.61M/1.41% and $16.47M/1.73% figures), [Balancer sets shutdown dates after BAL holders approve wind-down — The Defiant](https://thedefiant.io/news/defi/balancer-sets-shutdown-dates-after-bal-holders-approve-wind-down) (carried, source of the Oct 30/Oct 16/Nov 30 dates), [Balancer Proposes Wind-Down, $9M Treasury to Go to BAL Holders — KuCoin](https://www.kucoin.com/news/flash/balancer-proposes-wind-down-9m-treasury-to-go-to-bal-holders) (carried), new coverage of the BIP-928 vote tally and the rejected BIP-929 continuation proposal (new this week, context only).

### (e) Pendle PT-stETH
**Still not independently verifiable.** No fresher PT-stETH-specific maturity, liquidity, or yield figure was found this week; `app.pendle.finance` remains unreachable directly. Search results this week were dominated by unrelated October-2026 Ethena PT markets (PT-sUSDe/PT-srUSDe on Aave, 22-Oct-2026 maturity), which do not apply to stETH. A secondary, non-dated guide estimates stETH fixed APY on Pendle at roughly 4–5% with PT typically 1–3pp below spot — low-confidence, roughly consistent with the already-carried stale figure, not adopted as an update. **The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 is now outstanding for sixteen consecutive weeks.**
Sources: [Pendle Finance: Yield Tokenization Works — Medium](https://medium.com/@pycheng9/pendle-finance-yield-tokenization-87791c922cca) (carried, understood to be the stale source), secondary Pendle stETH fixed-yield guide (new this week, undated, low-confidence, context only), general Ethena-PT coverage (new this week, not applicable to stETH, not cited individually).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked again). No fresher dated TVL/APY figure was obtained this week; the same conflicting set from recent weeks stands (carried $98.6M/7.1% aggregator figure; a Nov-2025 Aave case study at ~40,000 ETH/>$175M/~5%; an undated Veda post at >$66M; an undated third-party post at $105M/10.6%). New this week: GGV's fee structure (10% performance fee + 1% platform fee) was confirmed, and a widely-circulated ~$196M TVL/~4.33% APY figure was traced to Lido's **EarnETH meta-vault** (which wraps GGV plus stRATEGY), not to GGV itself — this clarifies that the EarnETH figure should not be conflated with GGV's own rate, but does not supply a fresher GGV-specific number. The carried **$98.6M TVL / 7.1% APY** figure is retained for modeling, now **13 consecutive weeks stale**. GGV remains disqualified on the health-factor test regardless of which rate figure is used.
Sources: [Lido Deposit | Earn stETH/WETH Vault Rewards with GGV — lido.today](https://lido.today/) (carried, source of the $98.6M/7.1% figure), Aave × Lido case study (carried, source of the Nov-2025 $175M/~5% figure — [aave.com/blog/lido-aave-case-study](https://aave.com/blog/lido-aave-case-study)), Veda (vault curator) social post (carried, source of the ">$66M" figure), third-party social post (carried, source of the "$105M/10.6%" figure), [Lido EarnETH vault maintains fee structure as TVL reaches $196 million — CryptoBriefing](https://cryptobriefing.com/lido-earneth-vault-fee-update/) (carried, confirmed this week to be EarnETH-specific, not GGV — context only), new coverage of GGV's 10%-performance/1%-platform fee structure (new this week, context only).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** current TVL reading carried at ~$4.67B, now corroborated by an October-2026-dated review describing a sharp pullback from the >$15B 2026 peak — directionally consistent, no new TVL figure. New token-economics context (EIGEN trading ~$0.27, ~$258M market cap, ~$80M cumulative AVS rewards distributed with payouts slowing) does not change the yield picture. **Not a lever** for this vault's own (non-restaked) validators.
**Symbiotic:** no figure newer than the already-carried Core V2 "collateral markets" pivot (July 1, 2026) was found; this week's search results were all 2024/2025-dated. Not operationally scoped for this vault either way.
Sources: October-2026-dated EigenLayer TVL/pullback review (new this week, corroborates the carried $4.67B figure), KuCoin Square post on EigenLayer TVL and the Kelp DAO exploit (carried), [Symbiotic officially pivots to collateral markets with Core V2 launch — The Block](https://www.theblock.co/post/406862/symbiotic-officially-pivots-to-collateral-markets-with-core-v2-launch) (carried).

### (h) MEV relay market share
**No fresher-than-September-20 dated snapshot.** The most recent dated figures remain those "as of September 20, 2026" per relayscan.io (direct fetch blocked again): ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%. This week's search surfaced a second dated secondary source — shattered.io's 24-hour snapshot for August 29, 2026 — showing Ultra Sound 35.1%, Titan 29.8%, bloXroute Regulated 23.8%, Flashbots 2.4%; this is numerically distinct from (though directionally similar in ordering to) the carried September 20 figure, and being older is not treated as superseding it. The undated Rated.network "PBS Landscape" figure (carried, conflicting with both dated sources) is unchanged. No relay-registration action is indicated — the vault remains registered with ultrasound.money, Titan, and both bloXroute relays, all of which continue to show meaningful share across every reading found.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV boost relay censorship shift 2026 — shattered.io](https://shattered.io/mev-boost-relay-censorship-shift-2026/) (carried, source of both the Aug 29 snapshot now cited explicitly and prior context), [PBS Landscape — Rated](https://rated.network/relays) (carried, undated, context only).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint, currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated; USD conversions are omitted this week (see Changes Since Last Week).

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the carried figure — now 13 weeks stale and contested by conflicting third-party reads (~5% to ~10.6% across sources) | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger under the carried figure, but fails the 150% HF floor by a wide margin regardless of which rate figure is used. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread remains at **~0.0pp** (stETH ~2.2% vs. Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); unchanged this week | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable: every reading found this week (1.26%, 1.41%, 1.73% across three conflicting sources) sits well below the ~2.34% cost of carry | Unchanged | Not viable on yield, independent of depth. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | **Disqualified**; wind-down vote mechanics confirmed in more detail this week without changing the status. |
| 6 | Pendle PT-stETH | Unverified | Unchanged | Data gap — manual check outstanding for sixteen weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable as a lever for this vault; EigenLayer TVL remains at ~$4.67B | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all. Curve's underlying readings grew noisier rather than clearer, and Balancer's wind-down gained procedural detail — neither changes the underlying conclusions.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-10-26) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Remains at ~0.0pp (stETH ~2.2%, Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); unchanged this week. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV under the carried 7.1% figure (now 13 weeks stale and contested by conflicting reads), but disqualified separately on the 150% HF-floor test regardless of rate. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (general commentary only); **depth leg still unverifiable**. Manual check of `app.pendle.finance/trade/markets` remains outstanding after sixteen weeks and is the only way to get a currently-valid reading. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `research.lido.fi`, `defillama.com`, `cryptobriefing.com`, `governance.aave.com`, `aavescan.com`, `stake.lido.fi`, `app.pendle.finance`, and `www.relayscan.io` all returned DNS-resolution errors on direct fetch this week — the same set of domains blocked in prior weeks.
- As a result, this week's figures — including the Curve pool-depth reads, the Balancer wind-down vote detail, the Aave/Morpho rate reads, the GGV fee-structure and meta-vault clarification, the MEV relay shares, and the EigenLayer TVL corroboration — are drawn from search-engine snippets rather than live dashboard or primary-source pulls, with the same reliability caveats as prior weeks.
- **New this week — Curve TVL/APY sourcing became noisier, not clearer:** two additional undated pool-level snapshots ($18.61M/1.41% and $16.47M/1.73%) conflict with each other and with the $76.87M/1.26% figure carried for the prior two weeks. Does not change the strategy's not-viable conclusion either way, since every reading is far below the ~2.34% cost of carry.
- **New this week — Balancer's stETH/wstETH-pool-specific extension outcome remains unconfirmed** despite more vote-mechanics detail (BIP-928 passage, BIP-929 rejection) — the Oct 16 extension-request deadline passed with no source confirming which, if any, pools (including stETH/wstETH) received an extension.
- **New this week — a GGV/EarnETH misattribution was caught and corrected:** the widely-cited ~$196M/~4.33% figure belongs to the EarnETH meta-vault, not GGV itself; the carried $98.6M/7.1% GGV-specific figure (now 13 weeks stale) remains the modeling basis.
- **Carried forward — ETH/USD price data quality:** searches this week again returned inconsistent ETH/USD figures (~$2,060–$4,500 across sources, only one weakly dated at ~$2,694). USD conversions remain omitted; all figures in this report stay ETH-denominated.
- **Carried forward:** no fresh client reconfirmation of the infrastructure fee was sought this week (this refresh ran without a client-contact step); the last confirmed status (0%, as of 2026-09-28) is carried forward. The October 31, 2026 secondary-sourced fee-waiver expiry is now 5 days away and is the single highest-priority item to directly recheck at the next refresh, whatever the outcome. A manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY/TVL), `app.aave.com/markets/?marketName=proto_lido_v3` (Prime/Lido-instance WETH/wstETH rate), `defillama.com` (Curve pool TVL/APY, to resolve this week's three-way conflict), and `app.pendle.finance/trade/markets` (current, non-expired PT-stETH maturities, yields, and depth) would materially firm up several figures this report has had to estimate indirectly.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward and are now over fifteen weeks old.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

**No change to the recommended strategy this week.** Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry.

**The item requiring the closest attention next week is the infrastructure-fee waiver's secondary-sourced October 31, 2026 expiry, now 5 days away.** No primary governance confirmation of either a further extension or a lapse was found this week. Since the vault's modeling currently assumes 0%, a lapse would directly affect every client-facing return figure in this report — this should be directly rechecked at the next refresh regardless of whether a fresh client confirmation is obtained before then.

**Two items added source noise without changing conclusions:** the Curve stETH/ETH pool now has three mutually inconsistent readings (1.26%, 1.41%, 1.73% APY across different sources), none of which approaches the ~2.34% cost-of-carry hurdle, so the ranking is unaffected. The Balancer wind-down gained procedural detail (vote tallies for BIP-928/BIP-929) without resolving whether the stETH/wstETH pools specifically received an extension — the venue remains disqualified on the standing post-exploit basis regardless.

**One item was clarified rather than updated:** a widely-cited ~$196M/~4.33% figure was confirmed to belong to Lido's EarnETH meta-vault, not GGV — this prevents a future misattribution but does not change GGV's carried $98.6M/7.1% figure (now 13 weeks stale) or its disqualification on the 150% health-factor floor.

**Outstanding items for next week:** the manual check of `app.pendle.finance/trade/markets` is now sixteen weeks overdue. The infrastructure-fee waiver expiry (Oct 31) is the top priority to recheck. The Balancer stETH/wstETH-pool extension outcome (request window closed Oct 16) should be checked for a result if one surfaces. A fresh Lido dashboard pull for the vault's own position is also overdue — the position snapshot in this report is now based on data from over three months ago. A direct `stake.lido.fi/earn/ggv` check would help resolve the ongoing GGV rate uncertainty, and a direct DefiLlama pull would help resolve this week's three-way Curve pool conflict.
