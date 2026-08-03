# stVault Weekly Strategy Report — 2026-08-03

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-07-27 report:

- **Infrastructure-fee waiver end date: now a third conflicting value found.** This week's research repeatedly surfaced **"March 31st, 2026"** as the Early Adopters infrastructure-fee waiver end date (0% infra fee for identified vaults with Total Value > 250 ETH — this vault, at 3,791 ETH, clears that threshold). That is a *third* distinct date across three consecutive weeks of research: the 2026-07-13 briefing carried August 31, 2026; last week's report surfaced June 30, 2026; this week surfaces March 31, 2026. All three dates except August 31 have now passed. No search result in any of the three weeks has traced back to a primary, dated source (`docs.lido.fi` and `blog.lido.fi` again returned 403 to direct fetch this week). Three inconsistent answers in three weeks is treated as a signal that this specific fact is not reliably resolvable via web search in this environment, rather than as three separate governance events. **This is carried forward as the report's top action item, now with higher urgency: confirm the live fee directly against the vault dashboard, which should show a per-vault effective infrastructure fee, rather than relying on further web search.** Figures below continue to assume the waiver holds (0%), consistent with prior reports, but this assumption is now weaker than at any point since tracking began.
- **No change** to Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold) or the 5% node-operator fee.
- **MEV relay share — refreshed snapshot.** A newer relayscan.io read (2026-07-27, 24h) shows ultrasound.money 31.75%, bloXroute Max Profit 22.04%, bloXroute Regulated 18.48%, Titan 16.18%, and Aestus 7.04% — a real update versus the 2026-07-19 snapshot used for the past two reports. Within the vault's existing four registered relays (ultrasound, Titan, both bloXroute), combined measured share rose slightly (~88.5% vs ~86.4%), but the mix shifted: Titan's share fell by roughly a third (21.5% → 16.18%) while ultrasound and bloXroute Max Profit both gained. Aestus (7.04%) is not one of the vault's registered relays. See (h) below for a new observation on this.
- **Rate environment (stETH APR, Aave/Morpho borrow):** stETH APR reads ~2.2% again this week (one third-party tracker: 2.21%), consistent with the carried-forward estimate — no change to modeling. Aave Prime and Morpho wstETH/WETH borrow rates remain unobtainable live (same egress restrictions as the past two weeks); best available read is unchanged: the stETH-minus-borrow spread stays well under the 0.5pp re-entry trigger.
- **Lido GGV — new but unreliable data point.** A more recent-looking (undated) aggregator figure puts GGV at $98.6M TVL / 7.1% net-ish APY, versus the stale November 2025 figure (~$175M TVL / ~5% net APY) this report has carried for three weeks. A separate, uncorroborated social-media post claims $105M TVL / ~10.6% APY. Neither figure carries a clear date or a clear statement of whether it is gross or net of GGV's performance fee, and one description of GGV's underlying strategy mix names Balancer as one of the protocols GGV allocates into — worth noting given Balancer's ongoing corporate wind-down (see (d)/(f) below). This does not change the ranking: GGV remains disqualified on the 150% health-factor floor regardless of which yield figure is used.
- **EigenLayer:** TVL now cited at ~$19.7B / 4.6M ETH (up modestly from ~$19B last week); base AVS yield still cited at 3.8–6%, with one source this week citing 10–15% achievable via LRT-loop strategies specifically (a materially riskier, leveraged construction, not a like-for-like comparison to the base AVS yield used in prior weeks). Still not operationally scoped for this vault's own validators.
- **Symbiotic:** Core V2 (launched July 1, 2026) continues its shift toward a shared-collateral-market platform; a new "Liquid Lane" product (launched June 2026) has ~$550M secured across credit, insurance, and RWA applications, and Symbiotic TVL is now cited at ~$1.6B (second to EigenLayer). Still no standard, comparable AVS-restaking yield figure — this is a different product category than the restaking yield this report tracks, and remains not actionable as a lever for this vault.
- **Balancer / Curve:** unchanged. Balancer's corporate wind-down (announced March 2026, post-$110M exploit) continues; the DAO-governed protocol keeps operating in leaner form, but it remains disqualified here on counterparty-risk grounds. Curve stETH/ETH pool data remains unobtainable live (DeFiLlama 403 again this week); the ~$35M TVL figure is carried forward and the position would still represent a large fraction of that pool.
- **Pendle PT-stETH:** still not independently verifiable via direct fetch (`app.pendle.finance` blocked again). General commentary this week again cites PT-stETH fixed yield in the 4–5% range (unchanged), with stETH described as Pendle's largest, original underlying market. No pool-specific depth figure for a ~$6.9M position was found this week either — the manual browser check recommended in the 2026-07-13 briefing and repeated in every report since still has not been performed.
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull could not be completed this week either — see Data Gaps). $ conversions below use today's approximate ETH price (~$1,866, 2026-08-01, most recent price point found — down ~5% from the $1,969 used last week); the underlying ETH/stETH figures are unchanged from the 2026-07-13 pull.

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) ≈ $7.07M at ~$1,866/ETH (2026-08-01) |
| stETH liability | 3,509.2 stETH (~93% of total value) ≈ $6.55M |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH (~$0.20M); a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH, ~$6.55M) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below.

---

## Vault Parameters & Fee Structure

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold. No change this week.
- **Infrastructure fee:** modeled as waived under the Early Adopters campaign — **but see the flag above, now at its highest confidence-discount to date.** Three weeks of research have produced three different end dates (Aug 31, Jun 30, Mar 31, 2026), none traced to a primary source. The figures below still assume the waiver holds, consistent with prior reports, but this should be treated as the weakest-confidence assumption in this report and verified directly against the vault dashboard before next week's refresh.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.36%**, consistent with prior reports.

---

## Rate Environment (as of 2026-08-03)

### (a) Lido DAO / stVault governance and fees
No change to Basic Tier 1 parameters or the 5% operator fee. The Early Adopters infrastructure-fee waiver end date is the recurring open item — this week's research surfaced "March 31st, 2026" (with the 250 ETH minimum Total Value eligibility threshold restated, which this vault clears at 3,791 ETH), a third distinct date versus the June 30 figure last week and the August 31 figure originally carried. `docs.lido.fi` and `blog.lido.fi` again returned 403 to direct fetch. **Action item, now higher priority given three straight weeks of conflicting answers: verify the current effective infrastructure fee directly against the vault's own dashboard rather than continuing to rely on web search for this fact.**
Sources: [Lido V3 Is Live](https://blog.lido.fi/lido-v3-is-live-modular-infrastructure-for-a-new-paradigm-of-ethereum-staking/), [Lido V3 stVaults — Luganodes](https://www.luganodes.com/blog/lido-v3-stvaults-institutional-eth-staking), [Identified Node Operators | Lido Docs](https://docs.lido.fi/run-on-lido/stvaults/node-operators-identification/) (referenced via search, direct fetch blocked).

### (b) stETH APR
Best estimate: **~2.2%**, unchanged from last week. A third-party tracker (stakingrewards.com, via search) cites 2.21% APY currently, consistent with the carried-forward figure. No repeat of last week's transient rebase-miss issue was reported this week. Net effect on this report's modeling: none — the ~2.2% carried-forward estimate is retained.
Sources: [Lido's Ethereum Staking APY | Lido Help](https://help.lido.fi/en/articles/5230594-lido-s-ethereum-staking-apy), [Lido Staked Ether (stETH) — Staking Rewards](https://www.stakingrewards.com/asset/staked-ether), [APR and Rewards Calculator — Lido](https://lido.fi/how-lido-works/apr-and-rewards-calculator) (referenced via search, direct fetch blocked).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance, `app.aave.com/markets/?marketName=proto_lido_v3`) and Aavescan:** direct fetch blocked (403) again this week. Search results confirm the market remains active (Aave's Prime instance holds >$2B supplied, WETH utilization regularly >90%) and note a July 20, 2026 risk-steward action increasing supply/borrow caps on Aave V3 markets generally, but no dated, current WETH borrow or wstETH supply figure for the Lido instance specifically was obtained.
**Morpho:** no reliable current figure obtained this week either; backtested (not live) figures citing up to 6.2% APY for leveraged wstETH/WETH positions surfaced but reflect a Jan–Apr 2025 backtest window, not a current rate, and are not treated as authoritative.
Net read: unchanged from last two weeks — best evidence is consistent with the stETH-minus-borrow spread remaining **well below the 0.5pp re-entry trigger**, with the same lower-than-usual confidence flagged in prior reports.
Sources: [Aave — Lido case study](https://aave.com/blog/lido-aave-case-study), [Aavescan — wstETH on Ethereum V3](https://aavescan.com/ethereum-v3/wsteth) (referenced via search, direct fetch blocked), [Risk Stewards: Supply and Borrow Cap Increases — 2026.07.20](https://governance.aave.com/t/risk-stewards-supply-and-borrow-cap-increases-on-aave-v3-2026-07-20/25347).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** direct fetch to DeFiLlama blocked (403) again this week; no fresher figure than the ~$35M TVL / ~2.0–2.5% APY carried forward for three weeks. At this TVL, a ~$6.6M entry would still represent a large fraction of pool TVL — **not viable at this position size** regardless of the exact current rate.
**Balancer:** still **disqualified.** No material change this week beyond confirming the corporate wind-down (announced March 2026) continues while the DAO-governed protocol keeps operating leaner (zero BAL emissions, veBAL wind-down, treasury buyback program). Notably, one description of Lido's GGV vault this week names Balancer as one of the protocols GGV allocates into (see (f)) — worth flagging as an indirect counterparty-risk consideration for GGV, separate from this report's standalone Balancer-pool disqualification.
Sources: [DeFiLlama — Curve ETH-STETH pool](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (referenced via search, direct fetch blocked), [Balancer Labs to shut down — CoinDesk](https://www.coindesk.com/tech/2026/03/24/balancer-labs-will-shut-down-as-corporate-entity-became-a-liability-after-usd110-million-exploit), [Balancer Labs shutdown, tokenomics restructure — BeInCrypto](https://beincrypto.com/balancer-labs-shutdown-tokenomics-restructure/).

### (e) Pendle PT-stETH
**Still not independently verifiable this week.** `app.pendle.finance` and related endpoints remain blocked by this session's egress policy. General commentary again describes PT-stETH fixed yields in a **4–5%** range, unchanged from prior weeks, with stETH described as Pendle's largest and original underlying market. No pool-specific TVL/depth figure for a position of this size (~$6.6M) was obtained this week. **The manual check of `app.pendle.finance/trade/markets` recommended since the 2026-07-13 briefing still has not been performed and remains the fastest way to close this gap.**
Sources: [Pendle Finance Review — Coin Bureau](https://coinbureau.com/review/pendle-finance-review), [What Is Pendle Finance? 2026 Guide — EarnPark](https://earnpark.com/en/posts/what-is-pendle-finance-the-complete-2026-guide-to-yield-tokenisation-pt-yt-mechanics-and-boros/).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked, 403). This week produced a more current-looking but still undated figure of **$98.6M TVL / 7.1% APY**, distinct from both the stale November 2025 figure (~$175M / ~5% net APY, after a 10% performance fee) carried in prior reports and an uncorroborated social-media claim of $105M / ~10.6% APY. None of these three figures has a clear "as of" date or an explicit statement of gross vs. net-of-performance-fee, so none is treated as authoritative — the 7.1% figure is used below for illustration only, alongside the prior 5% figure for comparison. Regardless of which figure is used, GGV remains disqualified on the health-factor test (see Ranked Strategy Comparison).
Sources: [Lido blog — GGV overview](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/), [Cryptonomist — GGV launch](https://en.cryptonomist.ch/2025/09/04/lido-launches-gg-vault-automated-defi-yields-on-eth-weth-steth-and-wsteth-in-the-earn-tab/) (7.1% APY / $98.6M TVL figure referenced via aggregator search, not independently dated).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** TVL now cited at ~$19.7B / ~4.6M ETH restaked (up modestly from ~$19B last week); base AVS yield still commonly cited in the 3.8–6% APY range, consistent with prior reports. One source this week separately cites 10–15% APY achievable via LRT-loop strategies — a materially different, leveraged risk profile, not a substitute for the base AVS figure used in this comparison. Still not operationally scoped for this vault's own 0x02 validators.
**Symbiotic:** Core V2 (launched 2026-07-01) continues its pivot to shared-collateral markets; a new "Liquid Lane" product (launched June 2026) secures ~$550M across credit/insurance/RWA applications, and total Symbiotic TVL is now cited at ~$1.6B. Still no standard, comparable AVS-restaking yield figure exists for this comparison — this is a structurally different product than the restaking yield tracked here.
Sources: [BlockEden — EigenLayer $18B TVL](https://blockeden.xyz/blog/2026/03/20/eigenlayer-18b-tvl-vertical-avs-specialization-restaking-evolution/), [PistachioFi — EigenLayer Restaking Guide 2026](https://www.pistachio.fi/blog/eigenlayer-restaking-guide-2026), [Symbiotic launches Core V2 — Digital Today](https://www.digitaltoday.co.kr/en/view/78167/symbiotic-launches-core-v2-shifts-beyond-restaking-to-collateral-market-platform).

### (h) MEV relay market share
**Refreshed snapshot this week:** relayscan.io, 24h payload share as of 2026-07-27 — ultrasound.money 31.75%, bloXroute Max Profit 22.04%, bloXroute Regulated 18.48%, Titan 16.18%, Aestus 7.04%. Versus the 2026-07-19 snapshot used in the past two reports (ultrasound 26.0%, Titan 21.5%, bloXroute Max Profit 21.0%, bloXroute Regulated 17.9%), the combined share of this vault's four registered relays rose slightly (~86.4% → ~88.5%), but the composition shifted meaningfully: Titan's share fell by roughly a third while ultrasound and bloXroute Max Profit both gained. Builder-side concentration remains high (Titan builder ~51.6% of blocks per the same window), which is a separate metric from relay payload share and not directly actionable by relay registration. **New observation:** Aestus (7.04% share) is not among this vault's four registered relays; given Titan's declining relay-side share, it may be worth evaluating whether adding Aestus as a fifth registered relay would improve MEV capture — this is a candidate item for next week rather than an immediate action, since one week of data is not enough to confirm a trend.
Sources: [relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV Watch](https://www.mevwatch.info/), [KuCoin — Ethereum Staking in 2026](https://www.kucoin.com/blog/ethereum-staking-in-2026-yield-trends-validator-queue-dynamics-and-mev-impact-exlained).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint (~$6.55M), currently held outside the vault.

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week; **this figure assumes the infrastructure-fee waiver still holds — see flag above, now at its lowest-confidence point in three weeks**) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | Illustrative only, two unreconciled inputs: (7.1% − 2.36% cost of carry) × 3,509.2 stETH ≈ **+166 ETH/yr**, or (5% − 2.36%) × 3,509.2 stETH ≈ **+93 ETH/yr** on the prior, stale figure — **neither rate is independently confirmed or clearly dated this week** | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger under either figure but fails the 150% HF floor by a wide margin, and leaves the position near the 100% forced-rebalance line with no buffer improvement. Live rate reconfirmation still needed. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread assessed below the 0.5pp trigger | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable — ~$6.6M entry would be a large fraction of ~$35M pool TVL | Unchanged | Insufficient depth regardless of rate. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | Disqualified — post-exploit corporate wind-down continuing. |
| 6 | Pendle PT-stETH | Unverified this week; depth still unconfirmed | Unchanged | Data gap — manual check still recommended and still outstanding after four weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable this week; not a substitute lever for the deployment decision | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all — every external deployment leaves the position sitting near its current ~103% health factor, close to the 100% forced-rebalance threshold, regardless of the yield earned externally. This ranking is unaffected by the infrastructure-fee open question, since that fee applies to vault rewards under any deployment choice — but the specific "+14–24 ETH/yr" figure for the recommended strategy should be treated as provisional until the fee status is confirmed, and confirming it is now the report's single highest-priority open item after three consecutive weeks of conflicting search results.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-08-03) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** No evidence of movement from the ~0.2pp estimate carried forward for three weeks; confidence remains lower than usual — see Data Gaps. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV under either the new (7.1%) or prior stale (5%) figure, but disqualified separately on the 150% HF-floor test regardless of which rate is used. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (4–5% cited, consistent for three weeks running); **depth leg still unverifiable.** Manual check of `app.pendle.finance/trade/markets` remains outstanding after four weeks and is the fastest way to resolve this trigger either way. |

---

## Data Gaps & Methodology Notes

Same structural constraint as the past three weeks — this research environment could not reach most primary DeFi dashboards directly:

- `yields.llama.fi`/`defillama.com` pool pages, `app.pendle.finance`, `stake.lido.fi`, `app.aave.com`, `aavescan.com`, `docs.lido.fi`, `blog.lido.fi`, and `relayscan.io` (direct fetch) all returned 403 this week.
- As a result, this week's figures are again drawn from search-engine snippets rather than live dashboard pulls, with the same reliability caveats as prior weeks.
- **Highest-priority item for next week, escalated:** resolve the infrastructure-fee waiver end-date discrepancy against a primary source or the vault dashboard directly. Three weeks running have each produced a different candidate date (Aug 31 / Jun 30 / Mar 31, 2026) from web search, none traceable to a primary source — this specific fact should be treated as unresolvable by this research method going forward and should be pulled from the vault dashboard UI directly.
- **Carried forward:** a manual (browser-based) spot-check of `stake.lido.fi` (stETH APR, GGV live APY and its "as of" date), `app.aave.com/markets/?marketName=proto_lido_v3` (wstETH/WETH rates), and `app.pendle.finance/trade/markets` (PT-stETH fixed yields and depth) would materially firm up several figures this report has had to estimate indirectly for four weeks running.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward with only the ETH/USD conversion refreshed.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to the recommended strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry. The one action item that should not wait for next week's refresh, now carried at higher urgency after a third consecutive week of conflicting search results: **confirm the actual current status of the Early Adopters infrastructure-fee waiver directly against the vault dashboard or Lido documentation** — not via further web search, which has now produced three different, mutually exclusive end dates in three weeks and should be treated as an unreliable channel for this specific fact.
