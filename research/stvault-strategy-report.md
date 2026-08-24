# stVault Weekly Strategy Report — 2026-08-24

**Vault:** `0xd402937b3ff3c187f727c1146a9e846275e9f711` (Lido V3 stVault, Basic Tier 1)
**Validators:** 2213909, 2213910 (0x02 compounding)
**Prepared for:** libc

---

## Changes Since Last Week

Compared against the 2026-08-17 report:

- **Infrastructure-fee waiver: today IS the "7 days out" milestone flagged last week — still unresolved, now the report's single highest-priority item.** The confirmed **2026-08-31** waiver expiry used in this report's modeling is now **7 days away**, and next week's scheduled refresh (2026-08-31) will land on the expiry date itself. Direct fetches of `docs.lido.fi`, `blog.lido.fi`, and `stake.lido.fi` were all blocked again this week (same egress restriction as six straight prior weeks), so this could not be confirmed against a primary source. Web search again surfaced only the same recurring, dated "0% fee until March 31st, 2026" campaign-announcement language — no renewal, extension, or expiration announcement was found anywhere this week either. Figures below continue to assume the confirmed 0% infrastructure fee holds through 2026-08-31, per standing confirmed facts, but **this is now unverified for seven consecutive weeks running while the deadline itself has arrived.** A direct check against the vault dashboard or a primary Lido source — not web search — should happen this week, ahead of (not at) next week's refresh, since next week's refresh coincides with the expiry date itself and would be too late to catch a lapse before it affects the modeled figures.
- **Aave Prime (Lido instance) WETH borrow rate — still no fresher instance-specific figure; a same-week search surfaced a different Aave market's rate, correctly excluded.** This week's search returned a WETH borrow APY of 1.32% and wstETH borrow <0.01% — but for Aave's **Horizon** market, not the Prime/Lido instance this vault's cost-of-carry modeling depends on. That figure is not used here. `app.aave.com` and `aavescan.com` direct fetches both remained blocked again. The last Prime/Lido-instance-relevant read (WETH borrow ≈2.14%, carried forward two weeks now) is retained unchanged. Against stETH APR (~2.2%), the implied spread stays **~0.06pp**, still well under the 0.5pp re-entry trigger.
- **GGV: the unreconciled higher figure from last week did not recur — data quality improved.** Search this week returned only the recurring **$98.6M TVL / 7.1% APY** figure (now unchanged for four weeks); the one-off **$105M / ~10.6%** social-media figure from last week's report did not reappear in this week's results and is treated as unreliable / not corroborated. The $98.6M/7.1% figure still lacks a clear "as of" date and independent (non-search-engine) verification, but is now the sole figure in use, simplifying the illustrative calculation below. Under this figure, GGV still fails the 150% health-factor floor test, so this does not change the ranking or recommendation.
- **MEV relay share — combined vault-relay share essentially flat; Titan's builder-side rebound continued, and a new builder entrant appeared.** A relayscan.io-derived read (2026-08-23, 24h payload share) shows ultrasound.money 26.39%, bloXroute Max Profit 22.18%, Titan 21.45%, bloXroute Regulated 19.00%, Aestus 6.66%, Flashbots 2.35%, Agnostic 1.40%, Ethgas 0.58%. Combined share of the vault's four registered relays is **~89.0%** (26.39 + 21.45 + 22.18 + 19.00), essentially unchanged from 89.3% on 2026-08-15 — still a strong, stable read. On the **builder** side, Titan's share continued its rebound to **55.78%** (from 53.34% last week, from 43.36% two weeks ago) — a third consecutive weekly increase. Quasar fell to 19.71% (from 24.40%) and Eureka to 7.36% (from 10.26%), with a new entrant, **BuilderNet, appearing at 14.36%** — not previously seen in this report's builder-share tracking. A separate headline this week ("another major MEV relay service has been shut down, four companies to control over 90% of settlement") appears, based on its wording, to be later coverage of the same Blocknative shutdown already noted last week, not a second distinct closure — treated as such pending contrary evidence. Net read: no relay-registration action needed this week; combined coverage remains strong and stable.
- **stETH APR: no change to modeling.** Search this week again returned the same "2.4%" (net of Lido's 10% protocol fee) and "~2.6%" (base network rewards) figures seen last week, both consistent with this vault's own confirmed figures (2.24% dashboard net staking APR; ~2.6% measured gross consensus yield). The ~2.2% modeling assumption is unchanged.
- **Balancer: a new nuance, not yet enough to lift the disqualification.** This week's search surfaced additional detail on the corporate wind-down: Balancer's post-restructuring product scope is narrowing to five retained areas, one of which is explicitly "stablecoin and liquid staking token pools" — i.e., stETH-type pools are among the categories being kept rather than sunset. This softens the wind-down narrative slightly but does not amount to confirmation that this specific stETH pool continues operating normally, and no current rate or TVL figure was obtained. Balancer remains **disqualified** this week on the standing corporate-uncertainty basis; worth re-checking directly if the wind-down process concludes.
- **EigenLayer / Symbiotic: ether.fi's rotation away from EigenLayer restaking sharpened.** This week's sources state ether.fi has now removed **all** restaking exposure from weETH, with **less than 1%** of its assets remaining restaked with EigenLayer as of 2026-08-06 — a more definitive version of last week's "Hardening weETH" note (which described a softer "separate opt-in track"). The same sources describe weETH deposits as now delegating into **Symbiotic** instead. This is a continued signal of restaking-demand rotation between the two protocols, not an actionable lever for this vault's own validators (still not operationally scoped for restaking). EigenLayer TVL citations remain internally inconsistent this week (figures ranging $15.3B–$19B, alongside a separate $19.7B all-time-high figure carried from prior weeks) — the same cross-source data-quality flag as last week, not resolved, not acted on. Symbiotic TVL unchanged at ~$1.6B. AVS yield range (4–7%) unchanged.
- **Curve, Pendle:** Curve's ETH/stETH pool was cited this week with a specific 24h volume figure (~$17.1M, consistent with the "climbed to #7 in weekly volume rankings" note from last week) — a volume signal, still not a TVL/depth figure; `defillama.com` direct fetch remained blocked, so the ~$35M TVL carried forward for six weeks remains the basis for the depth conclusion (not viable at this position size). Pendle PT-stETH commentary (4–5% fixed yield) is unchanged; `app.pendle.finance` remains blocked; the manual `app.pendle.finance/trade/markets` depth check recommended since 2026-07-13 is now outstanding for **seven** consecutive weeks.
- **No change** to the confirmed Basic Tier 1 parameters (5% Reserve Ratio / 4.75% Forced Rebalance Threshold) or the 5% node-operator fee. `docs.lido.fi` direct fetch remained blocked again, so the generic-docs-vs-vault-specific discrepancy noted in prior reports (2.50% vs. this vault's confirmed 5%) could not be re-checked and is carried forward, unacted on.
- **Ranking / recommendation: unchanged.** Internal re-staking of the mint into the vault remains the recommended deployment. No external option clears both the yield hurdle and the 150% health-factor floor this week.

---

## Position Snapshot

Source: Lido dashboard, 2026-07-13 (most recent confirmed pull; a fresh dashboard pull was not obtained this week — see Data Gaps). $ conversions below use today's approximate ETH price (~$2,470, 2026-08-24, up sharply from ~$1,900 on 2026-08-17); the underlying ETH/stETH figures are unchanged from the 2026-07-13 pull.

| Metric | Value |
|---|---|
| Total vault value | 3,791 ETH (incl. ~32 ETH unstaked) ≈ $9.36M at ~$2,470/ETH (2026-08-24) |
| stETH liability | 3,509.2 stETH (~93% of total value) ≈ $8.67M |
| Remaining minting capacity | ~90.6 stETH |
| Health factor | 102.9% (forced rebalance at 100%) |
| Buffer to forced rebalance | ~2.8% of value ≈ 107 ETH (~$0.26M at current price); a breach deleverages ~20x the shortfall |
| Dashboard net staking APR | 2.24% |
| Carry spread | +0.18% |
| Measured gross consensus yield (validators) | ~2.6% APR |

The minted stETH (3,509.2 stETH, ~$8.67M at current ETH price) currently sits **outside** the vault — this is the capital being allocated in the strategy comparison below. Note the ETH price move (~$1,900 → ~$2,470, +30% week-over-week) is a market/USD-translation effect only; it does not change the underlying ETH-denominated position, health factor, or ETH/yr yield comparisons, which is why the ranked comparison below is expressed in ETH/yr terms throughout.

---

## Vault Parameters & Fee Structure

- **Tier:** Basic Tier 1 — 5% Reserve Ratio, 4.75% Forced Rebalance Threshold (confirmed vault-specific figures; see the flag above re: a conflicting generic docs figure of 2.50%, not acted on).
- **Infrastructure fee:** modeled as waived under the Early Adopters campaign through the confirmed end date of **2026-08-31** — now **7 days away**. Web search continues to surface only the recurring, dated "through March 31, 2026" campaign-announcement text, not traced to a confirmation of the vault's actual 2026-08-31 date or any extension/lapse. This is the report's top item to verify directly (not via web search) **this week**, ahead of next week's refresh, which lands on the expiry date itself.
- **Node-operator fee:** 5% on vault (validator) rewards — all client-facing return figures below are net of this fee where it applies.
- **Lido liquidity fee:** 6.5% of APR on minted stETH rewards, the second component of cost of carry.
- **Relays:** ultrasound.money, Titan, both bloXroute relays; fee recipient = vault.

**Cost of carry** (as defined for this report) = stETH rebase + 6.5%-of-APR liquidity fee = stETH APR × 1.065. At the carried-forward stETH APR of ~2.2%, this equals **~2.34%**, consistent with prior reports.

---

## Rate Environment (as of 2026-08-24)

### (a) Lido DAO / stVault governance and fees
No change to the confirmed Basic Tier 1 parameters or the 5% operator fee this week; no new DAO governance action affecting stVault fee terms was found. The infrastructure-fee waiver end date remains the recurring open item on web search (see Changes Since Last Week); the confirmed 2026-08-31 date is used for modeling, now 7 days out — the report's top priority item to verify directly this week.
Sources: [Lido V3 Is Live](https://blog.lido.fi/lido-v3-is-live-modular-infrastructure-for-a-new-paradigm-of-ethereum-staking/) (referenced via search, direct fetch blocked), [Lido Introduces New Node Operator Tiers and Extended Minting Caps in V3 Update](https://cryptonews.net/news/defi/32507188/), [Lido V3 stVaults — BlockEden.xyz](https://blockeden.xyz/blog/2026/02/09/lido-v3-stvaults-ethereum-staking/), [Lido Docs — Basic stVault](https://docs.lido.fi/run-on-lido/stvaults/building-guides/basic-stvault/) (referenced via search, direct fetch blocked).

### (b) stETH APR
Best estimate for modeling: **~2.2%**, unchanged. Search results this week repeat the "2.4%" (Lido APR net of 10% protocol fee) and "~2.6%" (base network rewards) figures from last week, both broadly consistent with this vault's own confirmed figures (2.24% dashboard net APR, ~2.6% gross consensus yield).
Sources: [Ethereum Staking Guide 2026 — CryptoTimes](https://www.cryptotimes.io/learn/ethereum-staking/), [ETH Staking Statistics 2026 — CoinLaw](https://coinlaw.io/eth-staking-statistics/).

### (c) Aave / Morpho wstETH-ETH leverage loop
**Aave Prime (Lido instance):** no fresher instance-specific figure obtained this week (`app.aave.com` and `aavescan.com` direct fetches both remained blocked). A same-week search surfaced a WETH borrow rate of 1.32% and wstETH borrow <0.01%, but for Aave's **Horizon** market — a different instance from the Prime/Lido market this report's modeling depends on — and is excluded from the figures below. The last Prime/Lido-relevant read (**WETH borrow ≈2.14%**, wstETH borrow ≈<0.01% variable APY) is carried forward unchanged for a second week. Against stETH APR (~2.2%), this implies a spread of **~0.06pp**, well under the 0.5pp re-entry trigger.
**Morpho:** no reliable current loop-APY figure obtained this week; search returned only general commentary (wstETH lending APY "4-6%+ during high borrowing demand," a "3.07%" Fusion-strategy vault figure, and the previously-cited "6.2%" backtest) — none is a current, position-relevant loop APY and none is treated as authoritative.
Net read: re-entry trigger **not met**.
Sources: [Aave — Lido case study](https://aave.com/blog/lido-aave-case-study), [Aave DAO Launches Lido-Specific Market — Avara](https://avara.xyz/blog/aave-dao-launches-lido-specific-market), [wstETH APY — vaults.fyi](https://blog.vaults.fyi/wsteth-yield/) (referenced via search, direct fetch blocked).

### (d) Curve and Balancer stETH pools
**Curve stETH/ETH:** `defillama.com` direct fetch blocked again this week; no fresher TVL figure than the ~$35M carried forward for six weeks. A 24h volume figure (~$17.1M) was obtained this week, consistent with the "climbed to #7 in Curve's weekly volume rankings" note from last week — a volume signal, not a depth (TVL) figure. At the carried-forward ~$35M TVL, a ~$6.7-8.7M entry would still represent a large fraction of pool TVL — **not viable at this position size** regardless of the exact current rate.
**Balancer:** still effectively **disqualified**, with a new nuance: this week's sources describe Balancer's post-wind-down product scope as retaining "stablecoin and liquid staking token pools" as one of five core areas, rather than sunsetting them — softening (but not resolving) the standing corporate-uncertainty concern. No current rate or TVL figure obtained. Not acted on this week; worth a direct re-check once the wind-down process is further along.
Sources: [Curve Best Yields & Key Metrics — Week 32, 2026](https://news.curve.finance/curve-best-yields/), [DeFiLlama — Curve ETH-STETH pool](https://defillama.com/yields/pool/57d30b9c-fc66-4ac2-b666-69ad5f410cce) (referenced via search, direct fetch blocked), [Balancer Labs to shut down — CoinDesk](https://www.coindesk.com/tech/2026/03/24/balancer-labs-will-shut-down-as-corporate-entity-became-a-liability-after-usd110-million-exploit).

### (e) Pendle PT-stETH
**Still not independently verifiable this week.** `app.pendle.finance` remains blocked by this session's egress policy. General commentary again describes PT-stETH fixed yields in a **4–5%** range, unchanged. No pool-specific TVL/depth figure for a ~$6.7-8.7M position was obtained this week. **The manual check of `app.pendle.finance/trade/markets` recommended since the 2026-07-13 briefing still has not been performed and is now outstanding for seven consecutive weeks.**
Sources: [Pendle Finance Review — Coin Bureau](https://coinbureau.com/review/pendle-finance-review), [A Complete Guide on How to Use Pendle Finance in 2026 — Coin Bureau](https://coinbureau.com/guides/how-to-use-pendle-finance).

### (f) Curated ("GGV-class") vaults
Lido's **GG Vault (GGV)** remains open for deposits at `stake.lido.fi/earn/ggv/deposit` (direct fetch blocked). This week's search returned only the recurring **$98.6M TVL / 7.1% APY** figure — the higher, unreconciled $105M/~10.6% figure cited last week did not reappear and is treated as not corroborated. The $98.6M/7.1% figure still lacks a clear "as of" date or independent (non-search-engine) confirmation. GGV remains disqualified on the health-factor test regardless.
Sources: [Lido blog — GGV overview](https://blog.lido.fi/lido-ggv-vault-access-to-defi-strategies/) (referenced via search, direct fetch blocked), [GGV deposit — stake.lido.fi](https://stake.lido.fi/earn/ggv/deposit) (referenced via search, direct fetch blocked).

### (g) EigenLayer / Symbiotic restaking
**EigenLayer:** base AVS yield still commonly cited in the ~4–7% range (3–4% base staking + 1–3% AVS rewards), consistent with prior weeks. TVL citations remain internally inconsistent this week ($15.3B–$19B across sources, alongside the ~$19.7B all-time-high figure carried from prior weeks) — the same cross-source data-quality flag as last week, unresolved, not acted on. Slashing is enforceable (live since April 2025); risk should be modeled conservatively.
**Symbiotic:** TVL unchanged at ~$1.6B (second to EigenLayer). This week's sources give a sharper version of last week's ether.fi note: weETH has now had **all** restaking exposure removed, with **<1%** of assets remaining restaked with EigenLayer as of 2026-08-06, with weETH deposits instead delegating into **Symbiotic**. A continued institutional-rotation signal between the two restaking ecosystems, not an actionable lever for this vault's own validators, which remain not operationally scoped for restaking.
Sources: [EigenLayer Review 2026 — Coin Bureau](https://coinbureau.com/review/eigenlayer-review), [EigenLayer Restaking in 2026 — Chainlabo](https://www.chainlabo.com/blog/eigenlayer-restaking-2026-guide-ethereum-validators), [weETH Drops Automatic Restaking — Bitzo](https://bitzo.com/2026/08/weeth-drops-auto-restaking-two-token-choice), [Symbiotic crosses $1B TVL — CryptoRank](https://cryptorank.io/news/feed/11ebb-symbiotic-tvl-crosses-1-billion).

### (h) MEV relay market share
**Refreshed snapshot this week:** relayscan.io-derived, 24h payload share as of 2026-08-23 — ultrasound.money 26.39%, bloXroute Max Profit 22.18%, Titan 21.45%, bloXroute Regulated 19.00%, Aestus 6.66%, Flashbots 2.35%, Agnostic 1.40%, Ethgas 0.58%. Versus 2026-08-15 (ultrasound 26.81%, Titan 22.06%, bloXroute Max Profit 21.81%, bloXroute Regulated 18.61%), combined share of the vault's four registered relays is **~89.0%** (from ~89.3%) — essentially flat, still a strong, stable read. On the **builder** side, Titan's share extended its rebound to **55.78%** (from 53.34% the prior week, 43.36% two weeks ago) — a third straight weekly increase. Quasar fell to 19.71% (from 24.40%) and Eureka to 7.36% (from 10.26%); a new entrant, **BuilderNet**, appeared at 14.36%, not previously tracked in this report. Separately, a headline referencing "another major MEV relay shut down, four firms to control over 90% of settlement" surfaced this week — based on its wording this appears to be later coverage of the Blocknative shutdown already noted last week rather than a second, distinct closure, but this has not been independently confirmed either way. Net read: no relay-registration action needed this week; combined coverage remains strong.
Sources: [MEV-Boost Relay & Builder Stats — relayscan.io](https://www.relayscan.io/) (referenced via search, direct fetch blocked), [MEV-Boost Builder Profitability — relayscan.io](https://www.relayscan.io/builder-profit?t=12h) (referenced via search, direct fetch blocked), [Another major MEV relay shut down — BlockBeats](https://www.theblockbeats.info/en/news/45838), [MEV Watch](https://www.mevwatch.info/).

---

## Ranked Strategy Comparison

Capital base: 3,509.2 stETH mint (~$8.67M at current ETH price), currently held outside the vault. Figures are expressed in ETH/yr since the position and its yield are ETH-denominated; the week's sharp ETH/USD move does not change any ETH-denominated figure below.

| Rank | Strategy | Net ETH/yr to vault owner | Resulting health factor | Status |
|---|---|---|---|---|
| 1 | **Internal re-staking** (mint → new validators inside the vault) — *incumbent* | +14–24 ETH/yr (carried forward; underlying rates ~unchanged this week; assumes the infrastructure-fee waiver holds through 2026-08-31 per confirmed facts — **now 7 days from expiry, unverified**) | ~198% | **Recommended.** Only option that both pays down the stETH liability and clears the 150% HF floor. |
| 2 | Lido GGV curated vault | (7.1% − 2.34% cost of carry) × 3,509.2 stETH ≈ **+167 ETH/yr**, on the single figure obtained this week (last week's unreconciled 10.6% figure did not recur) — still not independently verified or clearly dated | **~103%** (unchanged — external deployment does not reduce the stETH liability) | **Not recommended.** Nominally clears the 4% yield trigger but fails the 150% HF floor by a wide margin, and leaves the position near the 100% forced-rebalance line with no buffer improvement. |
| 3 | Aave/Morpho wstETH-ETH leverage loop | Not viable — spread carried forward at **~0.06pp** (stETH ~2.2% vs. Aave Prime/Lido-instance WETH borrow ~2.14%; no fresher instance-specific figure obtained this week), well below the 0.5pp trigger | Unchanged (~103%) | Trigger not met. |
| 4 | Curve stETH/ETH pool | Not viable — ~$6.7-8.7M entry would be a large fraction of ~$35M pool TVL (carried forward; no fresh TVL figure this week, only a volume figure) | Unchanged | Insufficient depth regardless of rate. |
| 5 | Balancer stETH/wstETH pools | Not viable | Unchanged | Disqualified — post-exploit corporate wind-down continuing, though this week's sources note LST pools are among the product lines being retained; re-check once the wind-down concludes. |
| 6 | Pendle PT-stETH | Unverified this week; depth still unconfirmed | Unchanged | Data gap — manual check outstanding for seven weeks. |
| 7 | EigenLayer / Symbiotic restaking | Not quantifiable this week; not a substitute lever for the deployment decision | Unchanged (does not touch the stETH liability) | Not actionable without further operational scoping. |

**No external option clears both the yield hurdle and the 150% health-factor floor this week.** Internal re-staking remains the only strategy that improves the health factor at all — every external deployment leaves the position sitting near its current ~103% health factor, close to the 100% forced-rebalance threshold, regardless of the yield earned externally. The recommended strategy's economics still assume the infrastructure-fee waiver holds; this is the report's most time-sensitive open item this week (see Data Gaps).

---

## Re-entry Triggers

| Trigger | Threshold | Status (2026-08-24) |
|---|---|---|
| stETH APR minus Aave/Morpho WETH borrow | ≥ 0.5pp | **Not met.** Carried forward at ~0.06pp (stETH ~2.2%, Aave Prime/Lido-instance WETH borrow ~2.14%); no fresher instance-specific figure obtained this week (a same-week Horizon-market figure was excluded as not applicable). Still well short of the threshold. |
| Curated vault open at ≥ 4% net | — | **Nominally met** by Lido GGV at 7.1% (single figure this week, improved data quality vs. last week's two unreconciled figures), but disqualified separately on the 150% HF-floor test regardless of rate. |
| Pendle PT-stETH fixed ≥ 3.5% with $6M+ depth | — | **Yield leg plausibly met** (4–5% cited, consistent for seven weeks running); **depth leg still unverifiable.** Manual check of `app.pendle.finance/trade/markets` remains outstanding after seven weeks and is the fastest way to resolve this trigger either way. |

---

## Data Gaps & Methodology Notes

Same structural constraint as prior weeks — this research environment could not reach most primary DeFi dashboards directly:

- `yields.llama.fi`/`defillama.com` pool pages, `app.pendle.finance`, `stake.lido.fi`, `app.aave.com`, `aavescan.com`, `docs.lido.fi`, `blog.lido.fi`, and `relayscan.io` all returned egress-blocked errors on direct fetch this week.
- As a result, this week's figures — including the carried-forward Aave WETH borrow rate, the GGV figure, and the MEV relay/builder shares — are drawn from search-engine snippets rather than live dashboard pulls, with the same reliability caveats as prior weeks, even where a specific number is quoted.
- **Highest-priority item this week, at its most urgent point yet:** the confirmed infrastructure-fee waiver end date (2026-08-31) is now **7 days away**, and next week's scheduled refresh lands on the expiry date itself — too late to catch a lapse before it. Confirm directly (vault dashboard or a primary Lido source, not web search) this week whether it is renewed, allowed to lapse, or replaced, and update the modeled figures immediately if it changes.
- **Carried forward:** a manual (browser-based) spot-check of `stake.lido.fi` (GGV live APY, TVL, and "as of" date), `app.aave.com/markets/?marketName=proto_lido_v3` (to refresh the Prime/Lido-instance WETH/wstETH rate, now two weeks stale), and `app.pendle.finance/trade/markets` (PT-stETH fixed yields and depth, seven weeks outstanding) would materially firm up several figures this report has had to estimate indirectly.
- A fresh Lido dashboard pull for the vault's own position (total value, stETH liability, health factor) was again not obtained this week; the 2026-07-13 figures continue to be carried forward with only the ETH/USD conversion refreshed (this week's ETH price move, ~$1,900 → ~$2,470, is a USD-translation effect only and does not affect any ETH-denominated figure in this report).
- No on-chain transactions were executed or simulated as part of this research.

---

## Recommendation

No change to the recommended strategy this week. Continue holding the internal re-staking plan (mint → new validators inside the vault) as the recommended deployment: it remains the only option that improves the health factor (to ~198%) while capturing a positive, if modest, net yield after the operator fee and cost of carry. The action item that should not wait for next week's refresh: **the confirmed infrastructure-fee waiver expires 2026-08-31 — 7 days out as of this report, and next week's refresh will land on the expiry date itself.** Given seven consecutive weeks of conflicting, unconfirmable web-search results on this same fact, the waiver's status should be confirmed directly against the vault dashboard or a primary Lido source this week — not held over to the next refresh — and the recommended-strategy figures revisited immediately if the fee reverts to 1%.
