#!/usr/bin/env python3
"""
MEV-Boost relay client for block reward attribution.

When a validator proposes a block that was built through MEV-Boost, the
winning relay records the delivered payload (including its bid ``value``)
on the standard data API endpoint::

    GET {relay}/relay/v1/data/bidtraces/proposer_payload_delivered?block_number=N

Querying the relays directly lets us attribute the block to a relay
(Flashbots, Ultra Sound, ...) and read the exact MEV value in Wei - the
same ``mev_source`` / ``amount`` information previously taken from
Beaconcha.in's ``/execution/block`` endpoint.

The relay ``tag`` strings below intentionally match the tags Beaconcha.in
used, so historical and migrated data stay directly comparable.
"""

import logging
from typing import Dict, List, Optional, Any

import requests

logger = logging.getLogger(__name__)

# tag -> base URL. Tags match Beaconcha.in's relay tags for data parity.
# Order matters: get_payload() returns the first relay that delivered the
# block, so the most-used relays are listed first to minimise queries.
DEFAULT_RELAYS: Dict[str, str] = {
    'ultrasound-relay': 'https://relay.ultrasound.money',
    'flashbots': 'https://boost-relay.flashbots.net',
    'bloxroute-max-profit-relay': 'https://bloxroute.max-profit.blxrbdn.com',
    'bloxroute-regulated-relay': 'https://bloxroute.regulated.blxrbdn.com',
    'agnostic-relay': 'https://agnostic-relay.net',
    'aestus-relay': 'https://mainnet.aestus.live',
    'titan-relay': 'https://global.titanrelay.xyz',
}

_DELIVERED_PATH = "/relay/v1/data/bidtraces/proposer_payload_delivered"


class MEVRelayClient:
    """Queries MEV-Boost relays for delivered-payload (block reward) data."""

    def __init__(self, relays: Optional[Dict[str, str]] = None, timeout: float = 10.0):
        self.relays = dict(relays) if relays is not None else dict(DEFAULT_RELAYS)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/json'})

    def query_relay(self, tag: str, block_number: int) -> Optional[Dict[str, Any]]:
        """
        Ask a single relay whether it delivered ``block_number``.

        Returns the delivered-payload trace (with a ``relay`` tag and a Wei
        integer ``value`` added) or ``None`` if the relay did not deliver this
        block or could not be reached.
        """
        base = self.relays.get(tag)
        if not base:
            return None

        url = f"{base.rstrip('/')}{_DELIVERED_PATH}"
        try:
            resp = self.session.get(url, params={'block_number': block_number},
                                    timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            # A single relay being unreachable must not abort attribution -
            # other relays may still have the block.
            logger.warning(f"Relay {tag} query failed for block {block_number}: {e}")
            return None
        except ValueError as e:
            logger.warning(f"Relay {tag} returned malformed JSON for block {block_number}: {e}")
            return None

        if not data:
            return None

        trace = dict(data[0])
        trace['relay'] = tag
        try:
            trace['value'] = int(trace['value'])  # normalise Wei string -> int
        except (KeyError, ValueError, TypeError):
            logger.warning(f"Relay {tag} trace for block {block_number} missing/invalid value")
            return None
        return trace

    def query_all_relays(self, block_number: int) -> List[Dict[str, Any]]:
        """
        Query every configured relay and return all that delivered the block.

        Normally at most one relay delivers a given block; more than one
        result indicates relays reporting the same delivery and is logged.
        """
        matches = []
        for tag in self.relays:
            trace = self.query_relay(tag, block_number)
            if trace is not None:
                matches.append(trace)

        if len(matches) > 1:
            tags = [m['relay'] for m in matches]
            logger.info(f"Block {block_number} reported by multiple relays: {tags}")
        return matches

    def get_payload(self, block_number: int) -> Optional[Dict[str, Any]]:
        """
        Best single attribution for a block.

        Returns the first relay (in configured priority order) that delivered
        the block, or ``None`` if no relay did - meaning the block was built
        locally and its reward must come from the execution layer instead.
        """
        for tag in self.relays:
            trace = self.query_relay(tag, block_number)
            if trace is not None:
                return trace
        return None
