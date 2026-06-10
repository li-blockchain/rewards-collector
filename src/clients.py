#!/usr/bin/env python3
"""
Per-client invoicing configuration.

An invoice is produced *for a client*, and each client maps to a different set
of reward sources and a billing rate. This module loads ``clients.json`` and
exposes typed ``Company`` / ``Client`` objects the invoice pipeline consumes.

Each client declares:
  * fee_rate            - service fee applied to billable streams (default 5%)
  * rp_node_ids         - parquet ``node`` labels for the client's RP-ETH earnings
  * csm_operator_ids    - Lido CSM node-operator id(s) (LIBC is operator 105)
  * stvault             - {vault, dashboard} addresses (REPORT-ONLY, never billed)
  * rpl_node_addresses  - RP node addresses for RPL reward attribution

``Company`` is the constant issuer block (Long Island Blockchain) shown on every
invoice, taken from the existing Zoho invoices.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_FEE_RATE = 0.05
CONFIG_PATH = Path(__file__).parent / 'clients.json'


@dataclass
class Company:
    """The invoice issuer (LIBC)."""
    name: str
    address_lines: List[str] = field(default_factory=list)
    logo_url: str = 'https://libc.fi/libc-logo.png'
    notes: str = 'Thanks for your business.'
    terms: str = 'Due on Receipt'


@dataclass
class Client:
    """A billing target and its reward sources."""
    id: str
    name: str
    bill_to: str
    bill_to_address_lines: List[str] = field(default_factory=list)
    fee_rate: float = DEFAULT_FEE_RATE
    rp_node_ids: List[str] = field(default_factory=list)
    csm_operator_ids: List[int] = field(default_factory=list)
    stvault: Optional[Dict[str, str]] = None
    rpl_node_addresses: List[str] = field(default_factory=list)
    # Default RPL earned in a billing period (RPL is supplied manually; the
    # invoice CLI/bot --rpl overrides this). None -> no RPL line unless provided.
    rpl_period: Optional[float] = None

    @property
    def fee_pct(self) -> float:
        """Fee rate as a percentage (e.g. 5.0)."""
        return self.fee_rate * 100


def _load(path: Path = CONFIG_PATH) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Client config not found: {path}")
    with open(path) as f:
        return json.load(f)


def load_company(path: Path = CONFIG_PATH) -> Company:
    data = _load(path).get('company', {})
    return Company(
        name=data.get('name', 'Long Island Blockchain'),
        address_lines=data.get('address_lines', []),
        logo_url=data.get('logo_url', 'https://libc.fi/libc-logo.png'),
        notes=data.get('notes', 'Thanks for your business.'),
        terms=data.get('terms', 'Due on Receipt'),
    )


def get_client(client_id: str, path: Path = CONFIG_PATH) -> Client:
    """Load a single client by id, applying defaults for omitted fields."""
    clients = _load(path).get('clients', {})
    if client_id not in clients:
        raise KeyError(f"Unknown client {client_id!r}. "
                       f"Known: {', '.join(sorted(clients)) or '(none)'}")
    c = clients[client_id]
    # rp_node_ids are compared against the parquet 'node' column (strings).
    rp_node_ids = [str(n) for n in c.get('rp_node_ids', [])]
    return Client(
        id=client_id,
        name=c.get('name', client_id),
        bill_to=c.get('bill_to', c.get('name', client_id)),
        bill_to_address_lines=c.get('bill_to_address_lines', []),
        fee_rate=float(c.get('fee_rate', DEFAULT_FEE_RATE)),
        rp_node_ids=rp_node_ids,
        csm_operator_ids=[int(x) for x in c.get('csm_operator_ids', [])],
        stvault=c.get('stvault'),
        rpl_node_addresses=c.get('rpl_node_addresses', []),
        rpl_period=(float(c['rpl_period']) if c.get('rpl_period') is not None else None),
    )


def list_clients(path: Path = CONFIG_PATH) -> List[str]:
    return sorted(_load(path).get('clients', {}).keys())
