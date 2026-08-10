# stVault Weekly Strategy Report — 2026-08-10

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-08-03 report:

- **Infrastructure-fee waiver: confirmed end date supersedes web search, but the date is now only three weeks out.** This week's brief carries a confirmed end date of **2026-08-31** for the Early Adopters infrastructure-fee waiver, sourced independently of web search. That is treated as authoritative for this report's modeling (figures below continue to assume 0% infrastructure fee). For context, this week's web search again surfaced the same conflicting picture tracked for four straight weeks — one result cited "March 31st, 2026" as the waiver's original end date and, in the same result, referenced the campaign being "extended through June 30" — i.e., web search continues to point at dates that have already passed, none traced to a primary source. This is now read as a weak signal that the waiver may have already lapsed by web-search-visible terms, even though the confirmed 2026-08-31 date is used here. **Action item for next week, elevated:** with the confirmed date only three weeks away, next week's refresh should explicitly confirm whether the waiver was renewed, allowed to expire, or replaced — this is now the report's top open item.
- **Aave Prime (Lido instance) WETH borrow rate — obtained for the first time.** After four weeks of this rate being unobtainable, a search-derived read of the live Aave app data this week gives **WETH borrow ≈ 2.14% variable APY** and **wstETH borrow ≈ <0.01% variable APY** on the Lido instance. This is a genuinely new data point, not merely carried forward, though it is not an independently timestamped dashboard pull and should be treated with the same caution as other search-derived figures. Against stETH APR (~2.2%), this implies a spread of **~0.06pp**, consistent with — and now quantifying for the first time — the "well below the 0.5pp trigger" read carried in prior reports.
- **No change** to the confirmed Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold) or the 5% node-operator fee. Note: generic Lido documentation surfaced this week describes a "Tier 1" reserve ratio of 2.50%, which conflicts with this vault's confirmed 5% figure. Since the 5%/4.75% figures are given as confirmed, vault-specific facts (as opposed to a general docs page, which may describe a different tier scheme or have drifted), they are retained as-is; this is flagged as a minor discrepancy, not acted on.
- **MEV relay share — Titan rebounds on the relay side, but continues to lose builder share.** A newer relayscan.io read (2026-08-09, 24h) shows ultrasound.money 26.31%, Titan 22.19%, bloXroute Max Profit 20.96%, bloXroute Regulated 17.57% (no Aestus figure returned this week). Versus the 2026-07-27 snapshot (ultrasound 31.75%, bloXroute Max Profit 22.04%, bloXroute Regulated 18.48%, Titan 16.18%, Aestus 7.04%), Titan's relay-side share jumped sharply (16.18% → 22.19%, +6.0pp) while ultrasound fell (31.75% → 26.31%, −5.4pp) and both bloXroute relays eased slightly. Combined share of the vault's four registered relays eased marginally (88.5% → 87.0%). Separately, on the **builder** side, Titan's block-building share continues to decline — cited at ~43.4% this week (2026-08-09 relayscan builder-profit window: Titan 43.36%, Quasar 25.82%, Eureka 18.31%), down from ~51.6% last week and ~50% in February 2026, with commentary describing Titan's builder dominance as "moderating" as Quasar gains share. Net read: no action needed on relay registration this week — the vault's four relays still capture the large majority of payloads — but the Aestus candidacy flagged last week could not be re-evaluated (no share figure surfaced this week) and remains open.
- **Rate environment (stETH APR):** ~2.2% again this week (2.18–2.21% across trackers), no change to modeling.
- **Curve, Balancer, Pendle, GGV, EigenLayer, Symbiotic:** no material new data this week beyond what is carried forward; see sections (d)–(g) below. All direct dashboard fetches (DeFiLlama, Pendle, Aavescan, relayscan.io, docs.lido.fi) were blocked again this week — see Data Gaps.
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was not obtained this week either — see Data Gaps). $ conversions below use today's approximate ETH price (~$1,920, 2026-08-10); the underlying ETH/stETH figures are unchanged from the 2026-07-13 pull.

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) ≈ $7.28M at ~$1,920/ETH (2026-08-10) |
| stETH liability | 3,509.2 stETH (~93% of total value) ≈ $6.74M |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH (~$0.21M); a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH, ~$6.74M) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below.

---

## Vault Parameters & Fee Structure

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold (confirmed vault-specific figures; see the flag above re: a conflicting generic docs figure of 2.50%, not acted on).
- **Infrastructure fee:** modeled as waived under the Early Adopters campaign through the confirmed end date of **2026-08-31**. Web search continues to surface conflicting, already-passed dates (Mar 31 / Jun 30, 2026) not traced to a primary source — this discrepancy is noted but not used to override the confirmed date. With the confirmed waiver end date now three weeks away, this is the report's top item to verify next week.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, consistent with prior reports.

---

## Rate Environment (as of 2026-08-10)

### (a) Lido DAO / stVault governance and fees
No change to the confirmed Basic Tier 1 parameters or the 5% operator fee. Broader Lido news this week is unrelated to stVault fee terms: a core staking-module upgrade went live 2026-07-24, and Curated Module v2 (custom fee curves, validator marketplace) remains slated for a Q4 2026 second phase — neither touches this vault's tier or fee structure. The infrastructure-fee waiver end date remains the recurring open item on web search (see Changes Since Last Week); the confirmed 2026-08-31 date is used for modeling.
Sources: [Lido V3 Is Live](https://blog.lido.fi/lido-v3-is-live-modular-infrastructure-for-a-new-paradigm-of-ethereum-staking/), [Lido Unveils Curated Module v2 — The Defiant](https://thedefiant.io/news/blockchains/lido-unveils-curated-module-v2-in-ethereum-staking-overhaul), [Lido Docs — Basic stVault](https://docs.lido.fi/run-on-lido/stvaults/building-guides/basic-stvault/) (referenced via search, direct fetch blocked).

### (b) stETH APR
Best estimate: **~2.2%**, unchanged (2.18–2.21% range across trackers this week). No change to modeling.
Sources: [Lido Staked Ether (stETH) — Staking Rewards](https://www.stakingrewards.com/asset/staked-ether), [Lido's Ethereum Staking APY — Lido Help](https://help.lido.fi/en/articles/5230594-lido-s-ethereum-staking-apy).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** a live-data read this week gives **WETH borrow ≈ 2.14%** and **wstETH borrow ≈ <0.01%** (variable APY) — the first concrete figures obtained in five weeks of tracking, though not an independently timestamped dashboard pull (direct fetch to `app.aave.com` and `aavescan.com` both remained blocked). Against stETH APR (~2.2%), this implies a spread of **~0.06pp**, well under the 0.5pp re-entry trigger. This is broadly consistent with the qualitative "well below trigger" read carried since mid-July, now with a number attached.
**Morpho:** no reliable current figure obtained this week; the previously-cited 6.2% APY figure for leveraged wstETH/WETH positions is a Jan–Apr 2025 backtest, not a current rate, and remains untreated as authoritative.
Net read: re-entry trigger **not met**, now with directly-sourced (if not fully verified) supporting data rather than an estimate.
Sources: [Aave — Lido case study](https://aave.com/blog/lido-aave-case-study), [Aave app — Lido instance markets](https://app.aave.com/markets/?marketName=proto_lido_v3) (referenced via search, direct fetch blocked), [Risk Stewards: Supply and Borrow Cap Increases — 2026.07.20](https://governance.aave.com/t/risk-stewards-supply-and-borrow-cap-increases-on-aave-v3-2026-07-20/25347).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** direct fetch to DeFiLlama blocked (403) again this week; no fresher figure than the ~$35M TVL carried forward for four weeks. At this TVL, a ~$6.7M entry would still represent a large fraction of pool TVL — **not viable at this position size** regardless of the exact current rate.
**Balancer:** still **disqualified.** No material change this week; the corporate wind-down (announced March 2026, post-$110M exploit) continues, with TVL down roughly 95% from its 2021 peak and the DAO-governed protocol continuing to operate in leaner form (BAL emissions ended, veBAL wound down, 100% of protocol fees redirected to treasury).
Sources: [DeFiLlama — Curve ETH-STETH pool](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (referenced via search, direct fetch blocked), [Balancer Labs to shut down — CoinDesk](https://www.coindesk.com/tech/2026/03/24/balancer-labs-will-shut-down-as-corporate-entity-became-a-liability-after-usd110-million-exploit).

### (e) Pendle PT-stETH
**Still not independently verifiable this week.** `app.pendle.finance` remains blocked by this session's egress policy. General commentary again describes PT-stETH fixed yields in a **4–5%** range, unchanged, describing stETH as Pendle's largest and original underlying market. No pool-specific TVL/depth figure for a ~$6.7M position was obtained this week. **The manual check of `app.pendle.finance/trade/markets` recommended since the 2026-07-13 briefing still has not been performed and remains the fastest way to close this gap — now outstanding for five consecutive weeks.**
Sources: [Pendle Finance Review — Coin Bureau](https://coinbureau.com/review/pendle-finance-review), [What Is Pendle Finance? 2026 Guide — EarnPark](https://earnpark.com/en/posts/what-is-pendle-finance-the-complete-2026-guide-to-yield-tokenisation-pt-yt-mechanics-and-boros/).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked, 403). No fresher figure than last week's undated **$98.6M TVL / 7.1% APY**, alongside the stale November 2025 figure (~$175M / ~5% net APY). Neither carries a clear "as of" date or explicit gross/net-of-performance-fee statement, so neither is treated as authoritative — both are retained for illustration only. GGV remains disqualified on the health-factor test regardless of which figure is used.
Sources: [Lido blog — GGV overview](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/), [Cryptonomist — GGV launch](https://en.cryptonomist.ch/2025/09/04/lido-launches-gg-vault-automated-defi-yields-on-eth-weth-steth-and-wsteth-in-the-earn-tab/).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** TVL cited at an all-time high of ~$19.7B; base AVS yield still commonly cited in the ~4–7% range (3–4% base staking + 1–2% AVS rewards), consistent with prior weeks. Note: a $300M exploit at Kelp (an EigenLayer-integrated LRT) in April 2026 triggered ~$5.4B in restaking-sector withdrawals — a relevant counterparty-risk data point, though it predates this report's tracking window and does not change the "not operationally scoped" status for this vault's own validators.
**Symbiotic:** TVL cited at ~$1.6B (second to EigenLayer) by one source, though a base-layer DeFiLlama count of ~$329M was also cited, illustrating continued measurement inconsistency across sources for this protocol. No standard, comparable AVS-restaking yield figure exists for this comparison. Still not actionable as a lever for this vault.
Sources: [Coin Bureau — EigenLayer Review 2026](https://coinbureau.com/review/eigenlayer-review), [BlockchainReporter — What Is Restaking? 2026](https://blockchainreporter.net/what-is-restaking-how-eigenlayer-lrts-and-shared-security-actually-work-in-2026/), [CryptoRank — Symbiotic crosses $1B TVL](https://cryptorank.io/news/feed/11ebb-symbiotic-tvl-crosses-1-billion).

### (h) MEV relay market share
**Refreshed snapshot this week:** relayscan.io, 24h payload share as of 2026-08-09 — ultrasound.money 26.31%, Titan 22.19%, bloXroute Max Profit 20.96%, bloXroute Regulated 17.57% (no Aestus figure returned this week). Versus 2026-07-27 (ultrasound 31.75%, bloXroute Max Profit 22.04%, bloXroute Regulated 18.48%, Titan 16.18%, Aestus 7.04%), Titan's relay-side share rebounded sharply (+6.0pp) while ultrasound gave back most of last week's gain (−5.4pp); combined share of the vault's four registered relays eased slightly (88.5% → 87.0%) but remains high. On the **builder** side, a separate relayscan read (builder-profit, 2026-08-09) shows Titan builder 43.36%, Quasar 25.82%, Eureka 18.31% — Titan's builder share continues a multi-week decline (≈50% in Feb 2026 → ~51.6% on 2026-07-27 → 43.36% now), with commentary attributing the shift to Quasar gaining share. Net read: no relay-registration action needed this week; the Aestus candidacy flagged last week remains open pending a share figure, which was not available this week.
Sources: [relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [relayscan.io — Builder Profitability](https://www.relayscan.io/builder-profit?t=12h) (referenced via search, direct fetch blocked), [MEV Watch](https://www.mevwatch.info/).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint (~$6.74M), currently held outside the vault.

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week; assumes the infrastructure-fee waiver holds through 2026-08-31 per confirmed facts) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | Illustrative only, two unreconciled inputs: (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+164 ETH/yr**, or (5% − 2.34%) × 3,509.2 stETH ≈ **+93 ETH/yr** on the prior, stale figure — **neither rate is independently confirmed or clearly dated** | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger under either figure but fails the 150% HF floor by a wide margin, and leaves the position near the 100% forced-rebalance line with no buffer improvement. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread now directly read at **~0.06pp** (stETH ~2.2% vs. Aave Lido-instance WETH borrow ~2.14%), well below the 0.5pp trigger | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable — ~$6.7M entry would be a large fraction of ~$35M pool TVL | Unchanged | Insufficient depth regardless of rate. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | Disqualified — post-exploit corporate wind-down continuing. |
| 6 | Pendle PT-stETH | Unverified this week; depth still unconfirmed | Unchanged | Data gap — manual check outstanding for five weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable this week; not a substitute lever for the deployment decision | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all — every external deployment leaves the position sitting near its current ~103% health factor, close to the 100% forced-rebalance threshold, regardless of the yield earned externally. This week's Aave/Morpho spread figure (~0.06pp) is the first directly-sourced number for that trigger and reinforces, rather than changes, the standing conclusion.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-08-10) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Now directly read at ~0.06pp (stETH ~2.2%, Aave Lido-instance WETH borrow ~2.14%) — the first concrete figure obtained for this trigger; still well short of the threshold. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV under either the current (7.1%) or prior stale (5%) figure, but disqualified separately on the 150% HF-floor test regardless of which rate is used. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (4–5% cited, consistent for five weeks running); **depth leg still unverifiable.** Manual check of `app.pendle.finance/trade/markets` remains outstanding after five weeks and is the fastest way to resolve this trigger either way. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `yields.llama.fi`/`defillama.com` pool pages, `app.pendle.finance`, `stake.lido.fi`, `app.aave.com`, `aavescan.com`, `docs.lido.fi`, and `relayscan.io` (direct fetch) all returned egress-blocked errors this week.
- As a result, this week's figures — including the new Aave WETH borrow rate and the MEV relay/builder shares — are drawn from search-engine snippets rather than live dashboard pulls, with the same reliability caveats as prior weeks, even where a specific number is quoted.
- **Highest-priority item for next week:** with the confirmed infrastructure-fee waiver end date (2026-08-31) now three weeks away, confirm directly whether it is renewed, allowed to lapse, or replaced, and update the modeled figures accordingly if it changes.
- **Carried forward:** a manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY and its "as of" date), `app.aave.com/markets/?marketName=proto_lido_v3` (to corroborate this week's WETH/wstETH rate read against a live dashboard timestamp), and `app.pendle.finance/trade/markets` (PT-stETH fixed yields and depth) would materially firm up several figures this report has had to estimate indirectly for five weeks running.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward with only the ETH/USD conversion refreshed.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to the recommended strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry. The action item that should not wait for next week's refresh: **the confirmed infrastructure-fee waiver expires 2026-08-31 — three weeks out.** Given four consecutive weeks of conflicting, unconfirmable web-search results on this same fact (each pointing at an already-passed date), the waiver's status at that point should be confirmed directly against the vault dashboard or a primary Lido source rather than web search, and the recommended-strategy figures revisited if the fee reverts to 1%.
