# stVault Weekly Strategy Report — 2026-08-17

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-08-10 report:

- **Infrastructure-fee waiver: urgency escalated again — now 14 days from the confirmed end date.** Today (2026-08-17) is 14 days before the confirmed **2026-08-31** waiver expiry used in this report's modeling. Web search this week again surfaced the same unresolved, conflicting picture tracked for five straight weeks — official Lido blog language citing "March 31st, 2026" as the original end date, and a separate (dated, but unverified) social-media post referencing "0% infrastructure fees through June 30." No renewal, extension, or expiration announcement was found. Figures below continue to assume the confirmed 0% infrastructure fee holds through 2026-08-31, per standing confirmed facts, but **this is now the single most time-sensitive open item in this report**: the next scheduled refresh (2026-08-24) will land only 7 days before expiry, and the following one lands on the expiry date itself. A direct check against the vault dashboard or a primary Lido source — not web search — is strongly recommended before, or at, next week's refresh.
- **Aave Prime (Lido instance) WETH borrow rate — no fresher figure obtained this week; reverts to carried-forward.** Last week's report obtained a live-data-adjacent read of WETH borrow ≈2.14% / wstETH borrow <0.01%. This week's search returned only older governance/rate-recommendation material (a Chaos Labs proposal referencing a wstETH base-rate change, and an Aave Prime wstETH supply APY figure, both dated late 2024) — no confirmation or update of the 2.14% figure. That number is carried forward unchanged, with the same caveat as prior weeks (search-derived, not an independently timestamped dashboard pull; `app.aave.com` and `aavescan.com` direct fetches both remained blocked). Against stETH APR (~2.2%), the implied spread stays **~0.06pp**, still well under the 0.5pp re-entry trigger.
- **GGV: a new, higher, unreconciled yield figure surfaced.** In addition to the recurring **$98.6M TVL / 7.1% APY** figure (unchanged for three weeks and still without a clear "as of" date), a dated social-media post this week cites **$105M TVL / ~10.6% APY**. Neither figure carries independent verification or a confirmed timestamp from a primary Lido source (`stake.lido.fi` direct fetch remained blocked); both are presented below as illustrative only. Under either figure, GGV still fails the 150% health-factor floor test, so this does not change the ranking or recommendation.
- **MEV relay share — combined vault-relay share improved, and Titan's builder-side decline sharply reversed.** A relayscan.io-derived read (2026-08-15, 24h payload share) shows ultrasound.money 26.81%, Titan 22.06%, bloXroute Max Profit 21.81%, bloXroute Regulated 18.61%, Aestus 6.59%, with three smaller relays splitting the remainder. Combined share of the vault's four registered relays rose to **~89.3%** (from 87.0% on 2026-08-09) — a positive development for payload capture. On the **builder** side, Titan's share **rebounded sharply to 53.34%** (from 43.36% last week), reversing the multi-week decline noted in the two prior reports; Quasar (24.40%) and Eureka (10.26%) round out the top three. Separately, **Blocknative shut down its MEV-Boost relay** this week (previously ~13% of block production), with search commentary noting the exit further concentrates settlement among the remaining four major relays (Flashbots, bloXroute, and others gaining the redistributed share). Blocknative was not one of this vault's four registered relays, so there is no direct action, but the shutdown reinforces the already-high concentration among the vault's existing relay set. Net read: no relay-registration action needed this week; the standing Aestus-candidacy question (open since two reports ago, no fresh share commentary this week either) remains unresolved but low priority given the vault's already-strong combined coverage.
- **stETH APR: mixed but broadly consistent signals, no change to modeling.** Search this week surfaced a "2.4%" figure (described as Lido's APR net of its 10% protocol fee) alongside a "~2.6% base rewards" figure tied to record network staking participation (~41.4M ETH staked). Both are directionally consistent with this vault's own confirmed figures (2.24% dashboard net staking APR; ~2.6% measured gross consensus yield) and do not warrant a change to the ~2.2% figure used for cost-of-carry modeling, which is kept conservative and consistent with weeks of tracking.
- **No change** to the confirmed Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold) or the 5% node-operator fee. `docs.lido.fi` direct fetch remained blocked again this week, so the generic-docs-vs-vault-specific discrepancy noted in prior reports (2.50% vs. this vault's confirmed 5%) could not be re-checked and is carried forward, unacted on.
- **EigenLayer / Symbiotic: minor data-quality flag, one new industry item.** One source this week describes EigenLayer TVL as having "peaked above $15B early 2026," which is inconsistent with the ~$19.7B all-time-high figure cited in the two prior reports — flagged as a cross-source measurement inconsistency, not acted on (restaking remains not operationally scoped for this vault's validators regardless). Symbiotic TVL unchanged at ~$1.6B. New this week: ether.fi published "Hardening weETH" (2026-08-06), moving Symbiotic-restaking exposure into a separate, opt-in track for its weETH product — a signal of increased institutional caution around restaking risk more broadly, but not an actionable lever for this vault's own validators.
- **Curve, Balancer, Pendle:** no material new data this week. Curve's ETH/stETH pool was noted as having climbed to #7 in Curve's own weekly trading-volume rankings (Week 32, 2026), but no fresh TVL figure was obtained, so the ~$35M TVL carried forward for five weeks remains the basis for the depth conclusion. Pendle PT-stETH fixed-yield commentary (4–5%) is unchanged; pool-specific depth remains unverified — the manual `app.pendle.finance/trade/markets` check recommended since 2026-07-13 is now outstanding for **six** consecutive weeks. Balancer remains disqualified (unchanged; post-exploit corporate wind-down continuing).
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was not obtained this week either — see Data Gaps). $ conversions below use today's approximate ETH price (~$1,900, 2026-08-17); the underlying ETH/stETH figures are unchanged from the 2026-07-13 pull.

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) ≈ $7.20M at ~$1,900/ETH (2026-08-17) |
| stETH liability | 3,509.2 stETH (~93% of total value) ≈ $6.67M |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH (~$0.20M); a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH, ~$6.67M) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below.

---

## Vault Parameters & Fee Structure

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold (confirmed vault-specific figures; see the flag above re: a conflicting generic docs figure of 2.50%, not acted on).
- **Infrastructure fee:** modeled as waived under the Early Adopters campaign through the confirmed end date of **2026-08-31** — now 14 days away. Web search continues to surface conflicting, unconfirmed dates (Mar 31 / Jun 30, 2026) not traced to a primary source. This is the report's top item to verify directly (not via web search) before or at next week's refresh.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, consistent with prior reports.

---

## Rate Environment (as of 2026-08-17)

### (a) Lido DAO / stVault governance and fees
No change to the confirmed Basic Tier 1 parameters or the 5% operator fee this week; no new DAO governance action affecting stVault fee terms was found. The infrastructure-fee waiver end date remains the recurring open item on web search (see Changes Since Last Week); the confirmed 2026-08-31 date is used for modeling, now 14 days out.
Sources: [Lido V3 Is Live](https://blog.lido.fi/lido-v3-is-live-modular-infrastructure-for-a-new-paradigm-of-ethereum-staking/), [Lido Introduces New Node Operator Tiers and Extended Minting Caps in V3 Update](https://cryptonews.net/news/defi/32507188/), [Lido V3 stVaults — BlockEden.xyz](https://blockeden.xyz/blog/2026/02/09/lido-v3-stvaults-ethereum-staking/), [Lido Docs — Basic stVault](https://docs.lido.fi/run-on-lido/stvaults/building-guides/basic-stvault/) (referenced via search, direct fetch blocked).

### (b) stETH APR
Best estimate for modeling: **~2.2%**, unchanged. New search results this week cite a "2.4%" Lido APR (net of 10% protocol fee) and "~2.6%" base network rewards tied to record staking participation (~41.4M ETH staked, ~34% of supply) — both broadly consistent with this vault's own confirmed figures (2.24% dashboard net APR, ~2.6% gross consensus yield) and not treated as grounds to change the modeling assumption.
Sources: [Ethereum Staking Guide 2026 — CryptoTimes](https://www.cryptotimes.io/learn/ethereum-staking/), [ETH Staking Statistics 2026 — CoinLaw](https://coinlaw.io/eth-staking-statistics/), [Lido's Ethereum Staking APY — Lido Help](https://help.lido.fi/en/articles/5230594-lido-s-ethereum-staking-apy).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher figure obtained this week (`app.aave.com` and `aavescan.com` direct fetches both remained blocked; search surfaced only late-2024 governance/rate material). Last week's read of **WETH borrow ≈2.14%** and **wstETH borrow ≈<0.01%** (variable APY) is carried forward unchanged. Against stETH APR (~2.2%), this implies a spread of **~0.06pp**, well under the 0.5pp re-entry trigger.
**Morpho:** no reliable current loop-APY figure obtained this week; search returned only a wide, unattributed range ("0.03%–3.07%" across six tracked vaults) and the previously-cited 6.2% APY figure remains a Jan–Apr 2025 backtest, not a current rate. Neither is treated as authoritative.
Net read: re-entry trigger **not met**.
Sources: [Aave — Lido case study](https://aave.com/blog/lido-aave-case-study), [wstETH on Ethereum V3 — Aavescan](https://aavescan.com/ethereum-v3/wsteth) (referenced via search, direct fetch blocked), [wstETH APY — vaults.fyi](https://blog.vaults.fyi/wsteth-yield/).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** `defillama.com` direct fetch blocked (403) again this week; no fresher TVL figure than the ~$35M carried forward for five weeks. Curve's own Week 32 2026 metrics note the ETH/stETH pool climbed to #7 in trading-volume rankings, but this is a volume signal, not a depth (TVL) figure. At the carried-forward ~$35M TVL, a ~$6.7M entry would still represent a large fraction of pool TVL — **not viable at this position size** regardless of the exact current rate.
**Balancer:** still **disqualified.** No change this week; the corporate wind-down (announced March 2026, post-$110M exploit) continues.
Sources: [Curve Best Yields & Key Metrics — Week 32, 2026](https://news.curve.finance/curve-best-yields/), [DeFiLlama — Curve ETH-STETH pool](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (referenced via search, direct fetch blocked), [Balancer Labs to shut down — CoinDesk](https://www.coindesk.com/tech/2026/03/24/balancer-labs-will-shut-down-as-corporate-entity-became-a-liability-after-usd110-million-exploit).

### (e) Pendle PT-stETH
**Still not independently verifiable this week.** `app.pendle.finance` remains blocked by this session's egress policy. General commentary again describes PT-stETH fixed yields in a **4–5%** range, unchanged. No pool-specific TVL/depth figure for a ~$6.7M position was obtained this week — Pendle's overall protocol TVL is cited at ~$5B, but with no stETH-pool-specific breakdown. **The manual check of `app.pendle.finance/trade/markets` recommended since the 2026-07-13 briefing still has not been performed and is now outstanding for six consecutive weeks.**
Sources: [Pendle TVL, Fees, Revenue & Volume — DeFiLlama](https://defillama.com/protocol/pendle) (referenced via search, direct fetch blocked), [Pendle Finance Review — Coin Bureau](https://coinbureau.com/review/pendle-finance-review).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked, 403). Two unreconciled figures circulated this week: the recurring **$98.6M TVL / 7.1% APY** (unchanged for three weeks, no clear "as of" date), and a new social-media-sourced **$105M TVL / ~10.6% APY** figure, also undated in any primary-source sense. Neither is independently confirmed. GGV remains disqualified on the health-factor test regardless of which figure is used.
Sources: [Lido blog — GGV overview](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/), [GGV deposit — stake.lido.fi](https://stake.lido.fi/earn/ggv/deposit) (referenced via search, direct fetch blocked).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** base AVS yield still commonly cited in the ~4–7% range (3–4% base staking + 1–3% AVS rewards), consistent with prior weeks. TVL citations diverged this week — one source states TVL "peaked above $15B early 2026," inconsistent with the ~$19.7B all-time-high figure cited in the two prior reports; flagged as a cross-source measurement inconsistency, not acted on. Slashing is enforceable (live since April 2025); risk should be modeled conservatively.
**Symbiotic:** TVL unchanged at ~$1.6B (second to EigenLayer). New this week: ether.fi's "Hardening weETH" post (2026-08-06) moves Symbiotic-restaking exposure into a separate opt-in track for its weETH product — an institutional-caution signal, not an actionable lever for this vault's own validators, which remain not operationally scoped for restaking.
Sources: [What Is EigenLayer Restaking? — ChainUp](https://www.chainup.com/blog/what-is-eigenlayer-restaking/), [EigenLayer Restaking in 2026 — Chainlabo](https://www.chainlabo.com/blog/eigenlayer-restaking-2026-guide-ethereum-validators), [Top 8 Restaking Crypto Projects — BingX](https://bingx.com/en/learn/article/top-restaking-crypto-projects-to-know), [weETH Drops Automatic Restaking — Bitzo](https://bitzo.com/2026/08/weeth-drops-auto-restaking-two-token-choice).

### (h) MEV relay market share
**Refreshed snapshot this week:** relayscan.io-derived, 24h payload share as of 2026-08-15 — ultrasound.money 26.81%, Titan 22.06%, bloXroute Max Profit 21.81%, bloXroute Regulated 18.61%, Aestus 6.59%, Flashbots 2.11%, Agnostic 1.11%, Ethgas 0.90%. Versus 2026-08-09 (ultrasound 26.31%, Titan 22.19%, bloXroute Max Profit 20.96%, bloXroute Regulated 17.57%), combined share of the vault's four registered relays **rose to ~89.3%** (from 87.0%), a positive development. On the **builder** side, Titan's share **rebounded sharply to 53.34%** (from 43.36% the prior week), reversing the multi-week decline flagged in the two prior reports; Quasar (24.40%) and Eureka (10.26%) trail. Separately, **Blocknative shut down its MEV-Boost relay this week** (previously ~13% of block production) — not one of this vault's four registered relays, but the exit further concentrates settlement among the remaining major relays, reinforcing the vault's already-high combined coverage. Net read: no relay-registration action needed this week.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV-Boost Builder Profitability — relayscan.io](https://www.relayscan.io/builder-profit?t=12h) (referenced via search, direct fetch blocked), [Blocknative suspending MEV-Boost Relay — The Block](https://www.theblock.co/post/253035/blocknative-suspending-mev-boost-relay-to-focus-on-economically-viable-opportunities), [MEV Watch](https://www.mevwatch.info/).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint (~$6.67M), currently held outside the vault.

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week; assumes the infrastructure-fee waiver holds through 2026-08-31 per confirmed facts) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | Illustrative only, three unreconciled inputs: (10.6% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+290 ETH/yr** on the new, unverified social-media figure; (7.1% − 2.34%) × 3,509.2 ≈ **+167 ETH/yr** on the recurring figure; (5% − 2.34%) × 3,509.2 ≈ **+93 ETH/yr** on the prior, stale figure — **none of the three rates is independently confirmed or clearly dated** | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger under any of the three figures but fails the 150% HF floor by a wide margin, and leaves the position near the 100% forced-rebalance line with no buffer improvement. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread carried forward at **~0.06pp** (stETH ~2.2% vs. Aave Lido-instance WETH borrow ~2.14%; no fresher figure obtained this week), well below the 0.5pp trigger | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable — ~$6.7M entry would be a large fraction of ~$35M pool TVL (carried forward; no fresh TVL figure this week) | Unchanged | Insufficient depth regardless of rate. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | Disqualified — post-exploit corporate wind-down continuing. |
| 6 | Pendle PT-stETH | Unverified this week; depth still unconfirmed | Unchanged | Data gap — manual check outstanding for six weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable this week; not a substitute lever for the deployment decision | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all — every external deployment leaves the position sitting near its current ~103% health factor, close to the 100% forced-rebalance threshold, regardless of the yield earned externally. The new, higher (and unverified) GGV figure this week (~10.6%) does not change this conclusion — it only widens the illustrative yield range for an option that is disqualified on health-factor grounds regardless of rate.

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-08-17) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Carried forward at ~0.06pp (stETH ~2.2%, Aave Lido-instance WETH borrow ~2.14%); no fresher figure obtained this week. Still well short of the threshold. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV under any of this week's three figures (10.6% / 7.1% / prior stale 5%), but disqualified separately on the 150% HF-floor test regardless of which rate is used. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (4–5% cited, consistent for six weeks running); **depth leg still unverifiable.** Manual check of `app.pendle.finance/trade/markets` remains outstanding after six weeks and is the fastest way to resolve this trigger either way. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `yields.llama.fi`/`defillama.com` pool pages, `app.pendle.finance`, `stake.lido.fi`, `app.aave.com`, `aavescan.com`, `docs.lido.fi`, `relayscan.io`, and (new this week) `theblockbeats.info` all returned egress-blocked errors on direct fetch.
- As a result, this week's figures — including the carried-forward Aave WETH borrow rate, the new GGV figures, and the MEV relay/builder shares — are drawn from search-engine snippets rather than live dashboard pulls, with the same reliability caveats as prior weeks, even where a specific number is quoted.
- **Highest-priority item for next week, further elevated:** the confirmed infrastructure-fee waiver end date (2026-08-31) is now 14 days away and will be only 7 days away at next week's scheduled refresh. Confirm directly (vault dashboard or a primary Lido source, not web search) whether it is renewed, allowed to lapse, or replaced, and update the modeled figures immediately if it changes.
- **Carried forward:** a manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY, TVL, and "as of" date — now with two conflicting figures to reconcile), `app.aave.com/markets/?marketName=proto_lido_v3` (to refresh the WETH/wstETH rate, now one week stale), and `app.pendle.finance/trade/markets` (PT-stETH fixed yields and depth, six weeks outstanding) would materially firm up several figures this report has had to estimate indirectly.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward with only the ETH/USD conversion refreshed.
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to the recommended strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry. The action item that should not wait for next week's refresh: **the confirmed infrastructure-fee waiver expires 2026-08-31 — 14 days out as of this report.** Given five consecutive weeks of conflicting, unconfirmable web-search results on this same fact (each pointing at an already-passed date), the waiver's status should be confirmed directly against the vault dashboard or a primary Lido source before it lapses, and the recommended-strategy figures revisited immediately if the fee reverts to 1%.
