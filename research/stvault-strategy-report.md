# stVault Weekly Strategy Report — 2026-07-27

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-07-20 baseline report:

- **Flag — Early Adopters infrastructure-fee waiver end date does not match what this report has been carrying.** This week's research repeatedly surfaced the waiver as **"extended through June 30" (2026)** — not August 31, 2026, which is the date this report (and the 2026-07-13 briefing before it) has been tracking toward. June 30, 2026 has already passed as of today. No source found this week or last week explicitly confirms an August 31, 2026 extension; the August 31 date could not be independently corroborated in either week's research. This is flagged as the most consequential open item in this report — **see the dedicated note under (a) below and verify directly against the vault dashboard or Lido docs before next week's refresh**, since if the waiver has in fact lapsed, the vault has been paying the 1% infrastructure fee (previously modeled at 0%) for approximately four weeks.
- **New, informational:** a stETH rebase this week came in below target (2.04% vs. an expected 2.15%) due to an oracle underreporting a 32 ETH validator deposit; Lido identified the cause and corrected it in a subsequent rebase (reported APR recovered to 2.29%). No funds were at risk. This does not change the stETH APR trend used in this report but is new information relative to last week.
- **No change** to Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold) or the 5% node-operator fee.
- **Rate environment (stETH APR, Aave Prime WETH borrow):** no evidence of material movement. Precise live figures for the Aave Prime wstETH/WETH market remain unobtainable in this research environment (same egress blocks as last week); best available read is that the stETH-minus-borrow spread stays well under the 0.5pp re-entry trigger, with the same lower-than-usual confidence flagged last week.
- **MEV relay share:** the only current data point available this week is the same 2026-07-19 24h snapshot used in last week's report — no new snapshot surfaced. Treat relay-share as **not re-confirmed this week** rather than as a fresh read; no relay-selection change is warranted regardless.
- **Lido GGV:** still only the same stale (November 2025) ~5% net APY / >40,000 ETH TVL figures found last week; still unconfirmed live.
- **Balancer / Symbiotic:** unchanged from last week — Balancer remains disqualified (post-exploit wind-down continuing), Symbiotic's Core V2 pivot away from restaking is now a month old with no new comparable AVS yield figure.
- **EigenLayer:** TVL estimates trend slightly higher this week (~$19B / 4.6M ETH / ~1,900 operators cited, vs. >$15B / 4.3M ETH previously) with AVS yield still cited in the same 3.8–6% range. Not a change to the operational conclusion — still not scoped as an available lever for this vault's own validators.
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week. This conclusion does **not** depend on the infrastructure-fee question above, since that fee applies to vault (validator) rewards regardless of which deployment strategy is chosen for the minted stETH — but it does affect the net ETH/yr figures reported for vault-reward-bearing strategies, and should be corrected once the actual fee status is confirmed.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull could not be completed this week either — see Data Gaps). $ conversions below use today's ETH price (~$1,969, 2026-07-27); the underlying ETH/stETH figures are unchanged from the 2026-07-13 pull.

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) ≈ $7.46M at $1,969/ETH (2026-07-27) |
| stETH liability | 3,509.2 stETH (~93% of total value) ≈ $6.91M |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH (~$0.21M); a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH, ~$6.91M) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below.

---

## Vault Parameters & Fee Structure

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold. No change this week.
- **Infrastructure fee:** modeled as waived under the Early Adopters campaign — **but see the flag above.** This week's research consistently surfaced a "through June 30" extension date rather than the August 31, 2026 date this report has carried forward; June 30 has already passed. Status is genuinely uncertain pending direct verification; the figures below still assume the waiver holds, consistent with prior reports, but this assumption is now weaker than last week.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.36%**, consistent with prior reports.

---

## Rate Environment (as of 2026-07-27)

### (a) Lido DAO / stVault governance and fees
No change to Basic Tier 1 parameters or the 5% operator fee. The one governance-adjacent item worth flagging is the Early Adopters infrastructure-fee waiver end date discrepancy described above: independent search results this week (an aixbt/X post, general Lido V3 coverage) describe the waiver as running "through June 30," which — if accurate and not itself stale — would mean it lapsed roughly four weeks ago. `docs.lido.fi` and `blog.lido.fi` could not be fetched directly this week (403, consistent with last week's egress restrictions), so this could not be resolved to a primary source. **Action item: confirm directly with the vault dashboard, Lido's Discord/governance channels, or docs.lido.fi (via browser) whether the infrastructure fee is currently 0% or has reverted to 1%, and if the latter, since what date.**
Sources: [Lido V3 Is Live](https://blog.lido.fi/lido-v3-is-live-modular-infrastructure-for-a-new-paradigm-of-ethereum-staking/), [aixbt — stVaults 0% infra fee through June 30](https://x.com/nero_eth/status/2004972045516292339), [Lido V3 stVaults — Luganodes](https://www.luganodes.com/blog/lido-v3-stvaults-institutional-eth-staking).

### (b) stETH APR
Best estimate: **~2.2–2.6%**, in the same range as last week (2.2%). One outlet (ventureburn, a staking-rate roundup) cites 2.6% "as of July 2026," which is consistent with the "measured gross consensus yield" already tracked for this vault's own validators rather than a change in the LST-wide net rate. Separately, a rebase this week briefly under-reported (2.04% vs. an expected 2.15% APR) due to an oracle missing a 32 ETH validator deposit; Lido identified and corrected the issue in the next rebase (APR recovered to 2.29%), with no funds at risk. Net effect on this report's modeling: none — the ~2.2% carried-forward estimate is retained.
Sources: [AMBCrypto — stETH rebase miss](https://ambcrypto.com/lidos-steth-rebase-misses-target-after-a-32-eth-accounting-delay-will-it-recover/), [CryptoBriefing — Lido oracle update](https://cryptobriefing.com/lido-steth-rebase-oracle-update/), [ventureburn — staking rates July 2026](https://ventureburn.com/best-crypto-staking-platforms/).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance, `app.aave.com/markets/?marketName=proto_lido_v3`):** direct fetch blocked (403) this week, same as last week. Search results confirm the market exists and note total wstETH borrowed across Aave's Core + Lido markets is ~$247.8M, and that Aave governance has an active rate-curve mechanism (ARFCs) for wstETH/WETH on the Lido instance, but no dated, current WETH borrow or wstETH supply figure could be obtained.
**Morpho:** no reliable current figure obtained this week either; a "stETH ARM market" reference (~2.9% borrow, launched March 2026) surfaced but is not clearly the same market tracked in prior reports and is not treated as authoritative.
Net read: unchanged from last week — best evidence is consistent with the stETH-minus-borrow spread remaining **well below the 0.5pp re-entry trigger**, but confidence in the precise spread stays lower than a typical week.
Sources: [Aave — Lido case study](https://aave.com/blog/lido-aave-case-study), [Aavescan — wstETH on Ethereum V3](https://aavescan.com/ethereum-v3/wsteth) (referenced via search, direct fetch blocked), [ARFC — wstETH/WETH Lido Borrow Rate Update](https://governance.aave.com/t/arfc-wsteth-weth-lido-borrow-rate-update/19867).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** direct fetch to DeFiLlama blocked (403) this week; no fresher figure than the ~$35M TVL / ~2.0–2.5% APY carried forward from last week. At this TVL, a ~$6.9M entry would still represent roughly a fifth of pool TVL — **not viable at this position size** regardless of the exact current rate.
**Balancer:** still **disqualified.** No new information this week beyond confirming the wind-down is continuing (Balancer Labs' March 2026 shutdown announcement stands; the DAO is narrowing product scope to five pool types and ending BAL emissions). No usable current stETH pool APY exists, and counterparty risk remains elevated.
Sources: [DeFiLlama — Curve ETH-STETH pool](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (referenced via search, direct fetch blocked), [BeInCrypto — Balancer Labs shutdown](https://beincrypto.com/balancer-labs-shutdown-tokenomics-restructure/), [CoinDesk — Balancer Labs wind-down](https://www.coindesk.com/tech/2026/03/24/balancer-labs-will-shut-down-as-corporate-entity-became-a-liability-after-usd110-million-exploit).

### (e) Pendle PT-stETH
**Still not independently verifiable this week.** `app.pendle.finance` and related API endpoints remain blocked by this session's egress policy. General (non-dated, non-pool-specific) commentary this week describes PT-stETH fixed yields in a 4–5% range, similar to before, and separately notes Pendle's total protocol TVL has grown ~29% over the trailing 30 days to ~$1.2B — a positive directional signal for on-platform liquidity generally, but not a confirmed PT-stETH-specific depth figure. **The 2026-07-13 briefing's recommendation to manually check `app.pendle.finance/trade/markets` in a browser still stands and has not yet been done.**
Sources: [Pendle TVL — DeFiLlama](https://defillama.com/protocol/pendle) (referenced via search), [Coin Bureau — Pendle Finance review 2026](https://coinbureau.com/review/pendle-finance-review).

### (f) Curated ("GGV-class") vaults
No change from last week: Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit`, but the only APY and TVL figures obtainable are the same stale November 2025 data (~5% net APY after a 10% performance fee, >40,000 ETH / ~$175M TVL at the time). `stake.lido.fi` continues to return 403 to direct fetch. Nominally still clears the 4% curated-vault trigger on stale data, but unconfirmed and — per the ranking below — disqualified separately on the health-factor test.
Sources: [Lido blog — GGV overview](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/), [Cryptonomist — GGV launch](https://en.cryptonomist.ch/2025/09/04/lido-launches-gg-vault-automated-defi-yields-on-eth-weth-steth-and-wsteth-in-the-earn-tab/).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** TVL estimates trend modestly higher this week (~$19B / ~4.6M ETH restaked / ~1,900 operators, vs. >$15B / 4.3M ETH cited previously); AVS yield still commonly cited in the 3.8–6% APY range (base staking + ~1–2pp from established AVSs like EigenDA), consistent with prior reports. Still not operationally scoped for this vault's own 0x02 validators, and still a different lever (adds AVS slashing risk, does not touch the stETH liability or health factor) rather than a substitute for the deployment strategies below.
**Symbiotic:** unchanged — Core V2 (launched 2026-07-01) remains focused on shared-collateral markets rather than a standard, comparable AVS-restaking yield. The cited 8.9% APY instant-liquidity vault example remains a non-representative RWA product, not a benchmark for this comparison.
Sources: [PistachioFi — EigenLayer Restaking Guide 2026](https://www.pistachio.fi/blog/eigenlayer-restaking-guide-2026), [The Block — Symbiotic Core V2 pivot](https://www.theblock.co/post/406862/symbiotic-officially-pivots-to-collateral-markets-with-core-v2-launch).

### (h) MEV relay market share
**No new snapshot obtained this week** — the only figure surfaced (relayscan.io, 24h) is dated 2026-07-19, identical to the data point already used in last week's report (ultrasound.money 26.0%, Titan 21.5%, bloXroute Max Profit 21.0%, bloXroute Regulated 17.9%). Treat this as "not re-confirmed" rather than "unchanged," though there is no reason to expect a material shift over one week absent any reported relay incident. No relay-selection change is warranted.
Sources: [relayscan.io](https://www.relayscan.io/), [MEV Watch](https://www.mevwatch.info/).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint (~$6.91M), currently held outside the vault.

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week; **this figure assumes the infrastructure-fee waiver still holds — see flag above and re-verify**) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | ~+93 ETH/yr *nominal*, computed as (5% reported net APY − 2.36% cost of carry) × 3,509.2 stETH — **not verified this week, ~8-month-old rate source** | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger but fails the 150% HF floor by a wide margin, and leaves the position sitting near the 100% forced-rebalance line with no buffer improvement. Needs a live rate reconfirmation regardless. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread assessed below the 0.5pp trigger | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable — ~$6.9M entry would be a large fraction of ~$35M pool TVL | Unchanged | Insufficient depth regardless of rate. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | Disqualified — post-exploit, protocol winding down. |
| 6 | Pendle PT-stETH | Unverified this week; depth still unconfirmed | Unchanged | Data gap — manual check still recommended and still outstanding. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable this week; not a substitute lever for the deployment decision | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all — every external deployment leaves the position sitting near its current ~103% health factor, close to the 100% forced-rebalance threshold, regardless of the yield earned externally. This ranking is unaffected by the infrastructure-fee open question, since that fee applies to vault rewards under any deployment choice — but the specific "+14–24 ETH/yr" figure for the recommended strategy should be treated as provisional until the fee status is confirmed.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-07-27) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** No evidence of movement from last week's ~0.2pp estimate; confidence remains lower than usual — see Data Gaps. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV (~5% net, per Nov 2025 data, still unconfirmed) but disqualified separately on the 150% HF-floor test even if the rate holds. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Not met / unverifiable.** No update from last week; manual check of `app.pendle.finance` remains outstanding. |

---

## Data Gaps & Methodology Notes

Same structural constraint as last week — this research environment could not reach most primary DeFi dashboards directly:

- `yields.llama.fi`/`defillama.com` pool pages, `api-v2.pendle.finance` / `app.pendle.finance`, `stake.lido.fi`, `app.aave.com`, `aavescan.com`, `docs.lido.fi`, and `blog.lido.fi` all returned 403 to direct fetch this week.
- As a result, this week's figures are again drawn from search-engine snippets rather than live dashboard pulls, with the same reliability caveats as last week.
- **New, higher-priority item for next week:** resolve the infrastructure-fee waiver end-date discrepancy (June 30 vs. August 31, 2026) against a primary source or the vault dashboard directly. This has real fee-cost implications if the waiver has lapsed.
- **Carried forward:** a manual (browser-based) spot-check of `stake.lido.fi` (stETH APR, GGV live APY), `app.aave.com/markets/?marketName=proto_lido_v3` (wstETH/WETH rates), and `app.pendle.finance/trade/markets` (PT-stETH fixed yields and depth) would materially firm up several figures this report has had to estimate indirectly for three weeks running.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to the recommended strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry. The one action item that should not wait for next week's refresh: **confirm the actual current status of the Early Adopters infrastructure-fee waiver** (0% vs. reverted to 1%) directly against the vault dashboard or Lido documentation, since this week's research raised a credible possibility that it lapsed on June 30, 2026 rather than continuing through August 31 as previously modeled.
