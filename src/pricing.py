#!/usr/bin/env python3
"""
Fiat pricing for invoice generation.

Fetches USD prices for the assets we bill (ETH, RPL; stETH ~ ETH) from
CoinGecko's free simple-price API. Invoices are denominated in USD, so each
reward stream's ETH/RPL amount is multiplied by the price at invoice time.

Two ways to supply a price (mirrors the manual "Rate" column on the Zoho
invoices, where a specific ETH/USD rate is sometimes chosen):

  * live   - fetched from CoinGecko (default)
  * pinned - passed in via ``overrides`` so an invoice can lock a chosen rate

Prices are cached for the lifetime of a ``PriceClient`` instance so a single
invoice run makes at most one network call per asset.
"""

import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

# CoinGecko coin ids for the assets we price.
COINGECKO_IDS = {
    'ETH': 'ethereum',
    'RPL': 'rocket-pool',
    'stETH': 'staked-ether',
}

COINGECKO_URL = 'https://api.coingecko.com/api/v3/simple/price'


class PriceUnavailable(RuntimeError):
    """Raised when a price could not be obtained (no live value, no override)."""


class PriceClient:
    """USD price source for invoice assets, with optional pinned overrides."""

    def __init__(self, overrides: Optional[Dict[str, float]] = None,
                 vs_currency: str = 'usd', timeout: float = 12.0):
        # Pinned prices win over live ones; keys are asset symbols (e.g. 'ETH').
        self.overrides = {k.upper(): float(v) for k, v in (overrides or {}).items()}
        self.vs_currency = vs_currency
        self.timeout = timeout
        self._cache: Dict[str, float] = {}
        self.session = requests.Session()

    def _normalize(self, symbol: str) -> str:
        return symbol.upper() if symbol.upper() != 'STETH' else 'stETH'

    def get_price(self, symbol: str) -> float:
        """
        USD price for ``symbol`` ('ETH', 'RPL', 'stETH').

        Resolution order: pinned override -> cache -> live CoinGecko fetch.
        Raises :class:`PriceUnavailable` if none yield a value.
        """
        key = self._normalize(symbol)
        upper = key.upper()

        if upper in self.overrides:
            return self.overrides[upper]
        if key in self._cache:
            return self._cache[key]

        coingecko_id = COINGECKO_IDS.get(key)
        if coingecko_id is None:
            raise PriceUnavailable(f"No CoinGecko id configured for {symbol!r}")

        try:
            resp = self.session.get(
                COINGECKO_URL,
                params={'ids': coingecko_id, 'vs_currencies': self.vs_currency},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            price = float(resp.json()[coingecko_id][self.vs_currency])
        except (requests.RequestException, KeyError, ValueError, TypeError) as e:
            raise PriceUnavailable(f"Could not fetch {symbol} price: {e}") from e

        self._cache[key] = price
        logger.info(f"💵 {key} = {price:.2f} {self.vs_currency.upper()}")
        return price

    def get_prices(self, symbols) -> Dict[str, float]:
        """Batch convenience wrapper returning {symbol: usd_price}."""
        return {s: self.get_price(s) for s in symbols}

    # Named helpers ----------------------------------------------------
    def eth_usd(self) -> float:
        return self.get_price('ETH')

    def rpl_usd(self) -> float:
        return self.get_price('RPL')

    def steth_usd(self) -> float:
        # stETH tracks ETH; fall back to ETH if the stETH feed is unavailable.
        try:
            return self.get_price('stETH')
        except PriceUnavailable:
            return self.get_price('ETH')
