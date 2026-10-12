# stVault Weekly Strategy Report — 2026-10-12

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-10-05 report:

- **Recommendation and ranking: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.
- **Curve stETH/ETH pool: TVL reads materially lower this week, but conclusion unchanged.** Two reads this week show the pool at **~$77–80M TVL** (a DefiLlama-protocol-page read of $79.77M/1.53% APY and a pool-specific read of $76.87M/1.26% APY) — well below the ~$108–110M figure this report carried last week. Neither read carries an explicit snapshot date, so this may reflect normal TVL movement rather than a data error, but it should be treated as noisy rather than a confirmed trend. Re-sized against ~$77–80M, a ~$9.39M entry is ~12% of pool TVL, a somewhat weaker depth picture than last week's ~8–9% but still far better than the ~27% implied by the ~$35M figure this report carried for months before last week. **No ranking change**: the pool's base APR (~1.3–1.5%) remains well below the vault's ~2.34% cost of carry, so the strategy continues to fail on yield regardless of the exact depth reading.
- **Balancer wind-down: concrete pool-level dates surfaced, confirming rather than changing last week's conclusion.** Following the September 29, 2026 Snapshot vote (BIP-928, ~99.2% approval, ~17.2M BAL), this week's search found specific operational dates: pools continue operating normally through **October 30, 2026**; partners could request an extension for certain v3 pools until **October 16, 2026**, which if granted would extend those pools to **November 30, 2026** before the v3 Vault is paused; first-phase treasury distribution to BAL holders is still slated for **late May 2027**. No source found confirming whether the stETH/wstETH pools specifically received an extension. Still **disqualified** on the standing post-exploit basis — this additional detail does not change the ranking.
- **Infrastructure fee / Early Adopters campaign: no primary-source confirmation obtained, but the extension chain is now better documented.** The primary Lido governance forum thread (`research.lido.fi/t/...10504`) confirms a documented chain of extensions — 0% fee from the original 60-day window (through end of March 2026) → extended through June 30, 2026 → extended again through August 31, 2026. No primary post for a further extension to October 31, 2026 was found this week; that date continues to rest on two independent secondary sources (CryptoBriefing and, newly found this week, CryptoSlate's coverage of Lido's August operator update), which corroborate each other but neither is a primary Lido source. **This date is now 19 days away** — worth a direct recheck once it passes, per the task's standing instruction to flag any change to this waiver. No change to the modeling assumption (0% fee, last directly confirmed by the client as of 2026-09-28).
- **Lido V3 tier parameters: conflicting third-party figures found, but the vault's own confirmed parameters are unaffected.** This week's search surfaced disagreement among third-party sources on Tier 1's reserve ratio — some (a March 2026 analysis, Kiln's risk disclosure) describe a 2.5% RR / ~48,750 stETH cap tier, while others (including the figure already on file) describe 5% RR / 47,500 stETH cap. This may reflect different tiers, a terminology mismatch between "identified operator" sub-categories, or simply inconsistent secondary reporting — it could not be resolved this week. This report continues to model the vault's own **confirmed** parameters (Basic Tier 1, 5% RR, 4.75% Forced Rebalance Threshold), which are client-confirmed facts and not contingent on resolving this third-party discrepancy. Flagged as a sourcing item to watch, not a parameter change.
- **EigenLayer: TVL figure reconfirmed by an additional source, with a mildly different peak figure.** A new source this week (KuCoin) independently puts EigenLayer TVL at the same ~$4.67B currently cited, attributing the decline to the Kelp DAO exploit, consistent with last week's finding. It describes the prior peak as "above $15 billion," somewhat lower than the ~$18.37B/~$22.06B peak figures this report has carried — the discrepancy is in the peak reference point, not the current ~$4.67B figure, which is now corroborated by a second source. Still **not a lever** for this vault's own non-restaked validators; no ranking change.
- **Aave Prime/Lido-instance WETH borrow, Morpho loop, GGV, MEV relay shares, Pendle, stETH APR, ETH/USD price: no material change.** A dated Aavescan WETH figure found this week (2.33% borrow / 1.78% supply, as of March 15, 2026) is **older** than the ~August 2026 Chaos Labs adjustment already on file (3.30% borrow / 1.10% supply, wstETH-specific) and does not supersede it; the carried net borrowing cost (~2.20%) and ~0.0pp spread against stETH APR stand. Morpho: no Aave-Prime/Lido-instance-specific loop figure found; unrelated Morpho USDC/wstETH market quotes (~4% supply/4–5% borrow) surfaced but aren't comparable. GGV: a newly-dated 2026 figure for the parent **EarnETH** meta-vault (~4.33% APY, ~$196M TVL) was found, but this is the meta-vault figure, not GGV's own — the carried GGV-specific figure ($98.6M TVL / 7.1% APY) is now 11 weeks stale with no fresher reading obtained. MEV relay shares: this week's "late August 2026" read (Ultra Sound 35.1%, Titan 29.8%, bloXroute Regulated 23.8%, Flashbots 2.4%) is the same stale figure already superseded last week by the September 20, 2026 relayscan.io reads (ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%), which remain the most recent and are carried forward. Pendle: the previously-flagged "~$8M liquidity, September 2026 maturity" source is now dated by context to roughly 346 days old (per a Coin Bureau-style cross-reference), confirming it was stale well before being cited and is almost certainly expired by now; no fresher PT-stETH-specific depth or yield figure was found. **The manual check of `app.pendle.finance/trade/markets` is now outstanding for fourteen consecutive weeks.** stETH APR: no fresher Lido-specific dated figure found; modeling assumption stays at ~2.2% (anchored to the vault's own 2.24% dashboard figure). ETH/USD: searches again returned inconsistent prices (~$2,060–$4,485 across sources with no reliable live quote); USD conversions remain omitted.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was again not obtained this week — now over three months stale; see Data Gaps). **USD conversions are omitted this week** due to unreliable ETH/USD price reads (see Changes Since Last Week); all figures are ETH-denominated, consistent with this report's methodology, under which the underlying ETH/stETH position figures are unaffected by any price move.

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

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold, 47,500 stETH minting cap. For reference, Tier 2 is 6% RR / 47,000 stETH cap and Tier 3 is 9% RR / 182,000 stETH cap — this vault remains on Tier 1. (See Changes Since Last Week re: a third-party sourcing discrepancy on Tier 1's RR that does not affect this confirmed figure.)
- **Infrastructure fee:** last directly confirmed by the client at **0%** as of 2026-09-28; no fresher client confirmation sought this week. The primary Lido governance forum confirms extensions through August 31, 2026; a further extension to October 31, 2026 continues to rest on two corroborating secondary sources only (CryptoBriefing, CryptoSlate), not a primary post. October 31 is now 19 days away. All figures below are modeled on the last-confirmed 0% fee.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, unchanged from prior reports.

---

## Rate Environment (as of 2026-10-12)

### (a) Lido DAO / stVault governance and fees
No DAO governance action changing stVault tier parameters was found this week. The primary forum thread for the Early Adopters campaign confirms extensions through August 31, 2026; the commonly-cited October 31, 2026 expiry remains secondary-sourced only (now corroborated by two independent secondary sources rather than one, but still no primary post located). A separate third-party discrepancy on Tier 1's reserve ratio (2.5% vs. 5% across sources) surfaced but does not change the vault's own confirmed parameters.
Sources: [Default risk assessment framework and fees parameters for Lido V3 (stVaults) — Lido Governance](https://research.lido.fi/t/default-risk-assessment-framework-and-fees-parameters-for-lido-v3-stvaults/10504) (direct fetch blocked again; extension chain through Aug 31, 2026 per search), [Lido extends 0% infrastructure fee for stVaults until October 31 — CryptoBriefing](https://cryptobriefing.com/lido-stvaults-zero-fee-extension-october/) (carried), CryptoSlate coverage of Lido's August operator update (new this week, referenced via search, same Oct 31 claim), [lido v3 introduces tier based staking credit rating — 4Pillars](https://research.4pillars.io/en/research/lido-v3-introduces-tier-based-staking-credit-rating) (carried), Kiln stVaults risk disclosure (new this week, source of the conflicting 2.5%/48,750 stETH figure — [docs.kiln.fi](https://docs.kiln.fi/v1/kiln-products/onchain/lido-v3-kiln-stvaults/smart-contract-and-protocol-risk-disclosure-kiln-stvaults-on-lido-v3)).

### (b) stETH APR
Best estimate for modeling: **~2.2% net-to-holders**, unchanged. This week's search again surfaced only generic, non-Lido-specific marketing figures (~3–4%); not treated as an update. Modeling assumption stays anchored to the vault's own confirmed dashboard figure (2.24% net).
Sources: generic staking-yield marketing pages (not cited individually; low-confidence), [Ethereum Staking Weekly Report, September 7, 2026 — Bitget](https://www.bitget.com/news/detail/12560605798507) (carried).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher instance-specific figure this week. A dated Aavescan WETH-market read (2.33% borrow / 1.78% supply, as of March 15, 2026) was found but **predates** the ~August 2026 Chaos Labs Risk Steward adjustment already on file (WETH borrow 3.30%, offset by wstETH supply 1.10%, net borrowing cost ~2.20%) and does not supersede it. Against the carried stETH APR (~2.2%), the spread remains **~0.0pp**.
**Morpho:** no Aave-Prime/Lido-instance-specific loop figure found this week; unrelated Morpho USDC/wstETH market quotes (~4% supply / 4–5% borrow, undated) surfaced but are not comparable. Carried tracker (dated August 12, 2026) unchanged.
Net read: re-entry trigger **not met**, unchanged from last week.
Sources: [Aavescan — Prime V3 WETH](https://aavescan.com/prime-v3/weth) (referenced via search, direct fetch blocked; source of the superseded March 2026 figures), [ARFC Prime Instance — wstETH Borrow Rate + rsETH Supply Cap Update — Aave Governance](https://governance.aave.com/t/arfc-prime-instance-wsteth-borrow-rate-rseth-supply-cap-update/20644) (carried; source of the 3.30%/1.10%/2.20% figures), [Risk Stewards — August 2026 WETH Interest Rate Adjustment — Aave Governance](https://governance.aave.com/t/risk-stewards-august-2026-weth-interest-rate-adjustment/25535) (carried, context only), [Failed to understand source of yield for Flagship ETH vaults — Morpho Forum](https://forum.morpho.org/t/failed-to-understand-source-of-yield-for-flagship-eth-vaults/1236) (carried, context only).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** two reads this week show **~$77–80M TVL** (DefiLlama protocol page: $79.77M / 1.53% APY; pool-specific page: $76.87M / 1.26% APY) — lower than the ~$108–110M read carried last week. Neither read carries an explicit snapshot date, so this could reflect genuine TVL movement or simply sampling noise between page views; treated here as the latest available reading rather than a confirmed trend. Re-sized against ~$77–80M, a ~$9.39M entry is ~12% of pool TVL — a somewhat weaker depth picture than last week's ~8–9% but still much better than the ~$35M-based ~27% estimate carried for months before last week. **Still not viable regardless of the exact TVL figure**: the pool's ~1.3–1.5% base APR remains well below the vault's ~2.34% cost of carry.
**Balancer:** following the September 29, 2026 wind-down vote (passed), this week's search found concrete operational dates: pools continue functioning normally through **October 30, 2026**; an extension window for certain v3 pools closes **October 16, 2026**, and if granted would extend those pools to **November 30, 2026** before the v3 Vault is paused; first-phase treasury distribution (~$9M) to BAL holders is slated for late May 2027. No source confirms whether the stETH/wstETH pools specifically obtained an extension. Still **disqualified**.
Sources: [Curve Finance — DeFi Dashboard, Yields — DefiLlama](https://defillama.com/protocol/yields/curve-finance) (new this week, referenced via search, direct fetch blocked; source of the $79.77M/1.53% figure), [Yield Chart - ETH-STETH(Curve DEX) — DefiLlama](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (referenced via search, direct fetch blocked; source of the $76.87M/1.26% figure), [Balancer sets shutdown dates after BAL holders approve wind-down — The Defiant](https://thedefiant.io/news/defi/balancer-sets-shutdown-dates-after-bal-holders-approve-wind-down) (new this week, source of the Oct 30/Oct 16/Nov 30 dates), [Balancer Proposes Wind-Down, $9M Treasury to Go to BAL Holders — KuCoin](https://www.kucoin.com/news/flash/balancer-proposes-wind-down-9m-treasury-to-go-to-bal-holders) (new this week, context).

### (e) Pendle PT-stETH
**Still not independently verifiable.** The specific "~$8M liquidity, September 2026 maturity" pool this report has cited for months is now understood, via cross-reference this week, to originate from a source roughly 346 days old at the time it was first surfaced — meaning it was already stale on day one and is almost certainly expired now. No fresher PT-stETH-specific maturity, liquidity, or yield figure was found this week; `app.pendle.finance` remains unreachable directly. A Q1 2026 industry estimate puts aggregate Pendle TVL (all assets) at roughly $5–7B, which is not stETH-specific and does not substitute for a market-level reading. **The manual check of `app.pendle.finance/trade/markets` recommended since 2026-07-13 is now outstanding for fourteen consecutive weeks.**
Sources: Coin Bureau — Pendle Finance guide/review (new this week, context, source of the ~346-day staleness cross-reference), [Pendle Finance: Yield Tokenization Works — Medium](https://medium.com/@pycheng9/pendle-finance-yield-tokenization-87791c922cca) (carried, now understood to be the stale source), Q1 2026 Pendle strategy analysis (new this week, aggregate TVL context only).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked again). No fresher GGV-specific figure was found this week; the carried **$98.6M TVL / 7.1% APY** figure is now **11 consecutive weeks stale**. A newly-found 2026-dated figure for the parent **EarnETH** meta-vault (GGV's enclosing product) shows ~4.33% APY and ~$196M TVL — informative context on the broader product, but not a GGV-specific reading and not used to update the GGV figure above. GGV remains disqualified on the health-factor test regardless of rate.
Sources: [Lido Deposit | Earn stETH/WETH Vault Rewards with GGV — lido.today](https://lido.today/) (carried, source of the $98.6M/7.1% figure), [Lido EarnETH vault maintains fee structure as TVL reaches $196 million — CryptoBriefing](https://cryptobriefing.com/lido-earneth-vault-fee-update/) (new this week, source of the EarnETH meta-vault figure), [Veda Golden Goose Vault — StakingRewards](https://www.stakingrewards.com/defi/0xef417fce1883c6653e7dc6af7c6f85ccde84aa09) (carried, direct fetch blocked).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** the ~$4.67B current TVL figure carried from last week is corroborated by an additional independent source this week (KuCoin), which attributes the decline to the Kelp DAO exploit, consistent with last week's sourcing. This source describes the prior peak as "above $15 billion," a somewhat lower reference point than the ~$18.37B/~$22.06B peak figures already on file — the discrepancy is in historical peak framing, not the current reading, which is now corroborated twice. **Not a lever** for this vault's own (non-restaked) validators.
**Symbiotic:** no figure newer than the already-carried Core V2 "collateral markets" pivot (July 1, 2026) was found. Not operationally scoped for this vault either way.
Sources: KuCoin Square post on EigenLayer TVL and the Kelp DAO exploit (new this week), [What Is KelpDAO? How Its $292M Hack Shook the Crypto Market in 2026 — KuCoin](https://www.kucoin.com/blog/vn-kelpdao-hack-2026-rseth-exploit-analysis) (carried), [Symbiotic officially pivots to collateral markets with Core V2 launch — The Block](https://www.theblock.co/post/406862/symbiotic-officially-pivots-to-collateral-markets-with-core-v2-launch) (carried).

### (h) MEV relay market share
**No fresher-than-September-20 snapshot.** The most recent dated figures remain those "as of September 20, 2026" per relayscan.io (direct fetch blocked again): ultrasound.money 32.86%, bloXroute Regulated 27.19%, Titan 27.17%, Aestus 9.14%. This week's search again surfaced only the same "late August 2026" figures (Ultra Sound 35.1%, Titan 29.8%, bloXroute Regulated 23.8%, Flashbots 2.4%) already identified last week as older than, and superseded by, the September 20 reads. No relay-registration action is indicated — the vault remains registered with ultrasound.money, Titan, and both bloXroute relays, all of which continue to show meaningful share.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV boost relay censorship shift 2026 — shattered.io](https://shattered.io/mev-boost-relay-censorship-shift-2026/) (carried, source of the superseded August 2026 figures).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint, currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated; USD conversions are omitted this week (see Changes Since Last Week).

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the single figure carried forward for eleven weeks — still not independently verified or clearly dated | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger but fails the 150% HF floor by a wide margin. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread remains at **~0.0pp** (stETH ~2.2% vs. Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); unchanged this week | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable: pool yield (~1.3–1.5% on this week's reads) sits well below the ~2.34% cost of carry, regardless of this week's lower TVL reading (~$77–80M vs. ~$108–110M last week) | Unchanged | Not viable on yield, independent of depth. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | **Disqualified**, now with concrete shutdown dates (pools normal through Oct 30; possible extension to Nov 30 for some v3 pools) confirming the wind-down. |
| 6 | Pendle PT-stETH | Unverified; the previously-cited depth figure is now understood to have been stale even before it was first cited | Unchanged | Data gap — manual check outstanding for fourteen weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable as a lever for this vault; EigenLayer TVL remains at ~$4.67B (down sharply from pre-Kelp-exploit levels), now corroborated by a second source | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all. The Curve TVL reading moved down this week without changing the conclusion (the venue fails on yield, not depth), and the Balancer wind-down now has confirmed operational dates without changing its disqualified status.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-10-12) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Remains at ~0.0pp (stETH ~2.2%, Aave Prime/Lido-instance WETH net borrowing cost ~2.20%); unchanged this week. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV at 7.1% (unchanged for eleven weeks), but disqualified separately on the 150% HF-floor test regardless of rate. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (4–6% commonly cited in general commentary); **depth leg still unverifiable**, and the previously-cited depth figure is now understood to have been stale from the outset. Manual check of `app.pendle.finance/trade/markets` remains outstanding after fourteen weeks and is the only way to get a currently-valid reading. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `research.lido.fi`, `defillama.com`, `aavescan.com`, `cryptobriefing.com`, `governance.aave.com`, `app.aave.com`, `stake.lido.fi`, `app.pendle.finance`, `www.relayscan.io`, and `www.stakingrewards.com` all returned egress-blocked or DNS-resolution errors on direct fetch this week.
- As a result, this week's figures — including the Curve TVL reads, the Balancer wind-down dates, the Aave rate reads, the GGV/EarnETH figures, the MEV relay shares, and the EigenLayer TVL corroboration — are drawn from search-engine snippets rather than live dashboard or primary-source pulls, with the same reliability caveats as prior weeks.
- **New this week — Curve TVL moved down on the latest reads, cause unconfirmed:** this week's reads (~$77–80M) are materially lower than last week's (~$108–110M), with neither read carrying an explicit snapshot date. This may be genuine pool movement or sampling/indexing noise; it does not change the strategy's not-viable conclusion (fails on yield, not depth) either way.
- **New this week — Balancer wind-down dates confirmed at the pool-operations level:** pools normal through October 30, 2026; a possible extension to November 30, 2026 for certain v3 pools (extension requests due October 16, 2026); no confirmation found on whether the stETH/wstETH pools specifically are covered by any extension.
- **New this week — Pendle's previously-cited depth figure traced to a stale-from-the-outset source:** the "~$8M liquidity, September 2026 maturity" figure this report carried for months appears to have originated from a source already ~346 days old when first surfaced, reinforcing (with a firmer basis) last week's suspicion that it was stale by calendar. The outstanding manual check of `app.pendle.finance/trade/markets` is now 14 weeks overdue.
- **New this week — Lido Tier 1 reserve-ratio sourcing conflict:** third-party sources disagree (2.5% vs. 5% RR for what may or may not be the same tier/category). Does not affect the vault's own client-confirmed parameters (5% RR, 4.75% Forced Rebalance Threshold), which this report continues to model.
- **Carried forward — ETH/USD price data quality:** searches this week again returned inconsistent ETH/USD figures (~$2,060–$4,485 across sources, no reliable live quote with a confirmed current date). USD conversions remain omitted; all figures in this report stay ETH-denominated.
- **Carried forward:** no fresh client reconfirmation of the infrastructure fee was sought this week (this refresh ran without a client-contact step); the last confirmed status (0%, as of 2026-09-28) is carried forward. A manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY/TVL), `app.aave.com/markets/?marketName=proto_lido_v3` (Prime/Lido-instance WETH/wstETH rate), `defillama.com` (Curve pool TVL, to resolve this week's downward move), and `app.pendle.finance/trade/markets` (current, non-expired PT-stETH maturities, yields, and depth) would materially firm up several figures this report has had to estimate indirectly.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward and are now over three months old.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

**No change to the recommended strategy this week.** Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry.

**Two items moved this week without changing the ranking:** the Curve stETH/ETH pool's TVL read came in materially lower (~$77–80M vs. ~$108–110M last week), but the venue was already disqualified on yield rather than depth, so this has no practical effect. The Balancer wind-down now has confirmed pool-operations dates (normal through October 30, 2026; possible extension to November 30 for some v3 pools), which confirms rather than changes that venue's disqualified status.

**One item firms up a long-standing data gap without resolving it:** the Pendle PT-stETH depth figure this report has carried for months now appears to trace to a source that was already stale when first cited, reinforcing the case that the manual `app.pendle.finance` check — outstanding for fourteen weeks — is the only way to get a currently valid reading for that venue.

**Outstanding items for next week:** the manual check of `app.pendle.finance/trade/markets` is now fourteen weeks overdue. The infrastructure-fee waiver's secondary-sourced October 31, 2026 expiry is 19 days away and should be directly rechecked once it passes, regardless of whether a fresh client confirmation is obtained before then. A fresh Lido dashboard pull for the vault's own position is also overdue — the position snapshot in this report is now based on data from three months ago.
