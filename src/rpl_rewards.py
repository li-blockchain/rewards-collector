#!/usr/bin/env python3
"""
RPL reward reporting (billable).

Rocket Pool RPL inflation rewards are distributed once per ~28-day reward
interval via Merkle trees and claimed with the smartnode. Two practical notes
that drive this implementation:

  * The per-interval reward tree CIDs are only exposed in RocketRewardsPool
    ``RewardSnapshot`` events (a large struct) and fetched from IPFS — brittle to
    reconstruct reliably.
  * Rocket Pool's Saturn upgrade changed the node staking ABI
    (``getNodeRPLStake`` reverts on the current contract), so a simple on-chain
    per-node RPL figure isn't available either.

Because RPL is claimed quarterly and the operator already knows the amount (the
sample invoice's "1378 RPL" was entered from the smartnode), the RPL earned in a
billing period is supplied explicitly — via the client config (``rpl_period``)
or the invoice CLI/bot (``--rpl``). The amount is then priced and fee-adjusted
exactly like the other billable streams.

FUTURE: auto-derive by decoding RocketRewardsPool ``RewardSnapshot`` logs for
each interval ending in the period, fetching the tree from IPFS, and summing the
node addresses' ``nodeRPL`` entries. Left as an enhancement for reliability.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def resolve_period_rpl(manual_amount: Optional[float],
                       client_default: Optional[float]) -> float:
    """
    The RPL earned in the billing period.

    Precedence: explicit ``manual_amount`` (CLI/bot) -> client config default
    -> 0.0 (no RPL line). Kept tiny on purpose so the billing path is reliable;
    on-chain auto-derivation can replace this later without changing callers.
    """
    if manual_amount is not None:
        return float(manual_amount)
    if client_default is not None:
        return float(client_default)
    return 0.0
