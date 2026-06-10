#!/usr/bin/env python3
"""
Invoice data model.

``build_invoice`` unifies every reward stream for one client over an epoch range
into a single structure the PDF and XLS generators both render:

    {
      "meta":        invoice number/dates/terms,
      "company":     issuer (LIBC),
      "client":      bill-to + fee rate,
      "line_items":  [billable streams -> Balance Due],
      "subtotal_usd"/"total_usd"/"balance_due_usd",
      "metrics":     report-only figures (incl. stVault), shown on page 2,
      "period":      epochs + dates,
    }

Billable streams (fee applied -> USD): RP-ETH (this module), and later CSM-ETH
and RPL. stVault is report-only and lives under ``metrics`` (never billed).

A billable line mirrors the Zoho invoice columns:
    Qty   = fee portion of the earned amount (earned * fee_rate)
    Rate  = USD price per unit
    Amount= Qty * Rate
with the gross/net earned amount shown as the line's sub-detail.
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from clients import Client, Company, get_client, load_company
from pricing import PriceClient
from generate_invoice import InvoiceGenerator
from reward_utils import get_validator_type_label

logger = logging.getLogger(__name__)

# Mainnet genesis for epoch->date (matches generate_invoice.epoch_to_date).
GENESIS_TS = 1606824000
SECONDS_PER_EPOCH = 384


def _default_rpc_url() -> str:
    return (os.getenv('EXECUTION_RPC_URL') or os.getenv('RPC_URL')
            or 'http://libc-prod2:8545')


def epoch_to_datetime(epoch: int) -> datetime:
    return datetime.fromtimestamp(GENESIS_TS + epoch * SECONDS_PER_EPOCH, timezone.utc)


@dataclass
class LineItem:
    description: str
    sub_detail: str          # e.g. "18.851 ETH" (the earned amount)
    token_symbol: str        # ETH / RPL
    earned: float            # net earned in token units
    fee_portion: float       # earned * fee_rate (the billable Qty)
    usd_rate: float          # price per token
    amount_usd: float        # fee_portion * usd_rate

    def as_dict(self) -> Dict[str, Any]:
        return {
            'description': self.description,
            'sub_detail': self.sub_detail,
            'token_symbol': self.token_symbol,
            'earned': self.earned,
            'qty': self.fee_portion,
            'rate': self.usd_rate,
            'amount': self.amount_usd,
        }


def _billable_line(description: str, earned: float, token_symbol: str,
                   fee_rate: float, usd_rate: float,
                   sub_detail: Optional[str] = None) -> LineItem:
    fee_portion = earned * fee_rate
    return LineItem(
        description=description,
        sub_detail=sub_detail if sub_detail is not None else f"{earned:.3f} {token_symbol}",
        token_symbol=token_symbol,
        earned=earned,
        fee_portion=fee_portion,
        usd_rate=usd_rate,
        amount_usd=fee_portion * usd_rate,
    )


def _rp_eth_stream(client: Client, parquet_file: str, start_epoch: int,
                   end_epoch: int) -> Dict[str, Any]:
    """
    RP-node ETH earnings for the client (commission-adjusted net), plus metrics.

    Reuses InvoiceGenerator.calculate_earnings for the exit/commission handling,
    filtered to the client's parquet ``node`` labels.
    """
    gen = InvoiceGenerator(parquet_file)
    df = gen.load_data(start_epoch, end_epoch)
    if client.rp_node_ids:
        df = df[df['node'].astype(str).isin(client.rp_node_ids)].copy()

    if df.empty:
        return {'net_eth': 0.0, 'earnings': None, 'metrics': {
            'rp_net_eth': 0.0, 'rp_gross_eth': 0.0, 'blocks_proposed': 0,
            'mev_blocks': 0, 'local_blocks': 0, 'mev_value_eth': 0.0,
            'withdrawal_count': 0, 'validators': 0, 'node_breakdown': [],
            'type_counts': {}}}

    earnings = gen.calculate_earnings(df)

    # Extra metrics straight from the filtered frame.
    proposals = df[df['record_type'] == 'proposal']
    mev_blocks = proposals[proposals['mev_source'].fillna('') != '']
    local_blocks = proposals[proposals['mev_source'].fillna('') == '']
    withdrawals = df[df['record_type'] == 'withdrawal']
    gross_eth = (proposals['amount'].sum() / 1e18) + (withdrawals['amount'].sum() / 1e9)

    # Validator type mix (LEB8 / LEB16 / 32 ETH) among the client's validators.
    vtypes = df.groupby('validator_index')['validator_type'].first()
    type_counts: Dict[str, int] = {}
    for vt in vtypes:
        label = get_validator_type_label(vt)
        type_counts[label] = type_counts.get(label, 0) + 1

    node_breakdown = [
        {'node': r['node'], 'record_type': r['record_type'],
         'amount_eth': float(r['amount_eth']), 'validators': int(r['validator_index'])}
        for _, r in earnings['node_breakdown'].iterrows()
    ]

    return {
        'net_eth': float(earnings['grand_total']),
        'earnings': earnings,
        'metrics': {
            'rp_net_eth': float(earnings['grand_total']),
            'rp_gross_eth': float(gross_eth),
            'rp_withdrawals_eth': float(earnings['total_withdrawals']),
            'rp_proposals_eth': float(earnings['total_proposals']),
            'rp_exits_eth': float(earnings.get('total_exits', 0.0)),
            'blocks_proposed': int(len(proposals)),
            'mev_blocks': int(len(mev_blocks)),
            'local_blocks': int(len(local_blocks)),
            'mev_value_eth': float(mev_blocks['amount'].sum() / 1e18),
            'withdrawal_count': int(len(withdrawals)),
            'validators': int(earnings['total_validators']),
            'node_breakdown': node_breakdown,
            'type_counts': type_counts,
        },
    }


def _csm_stream(client: Client, start_ts: int, end_ts: int,
                rpc_url: str) -> Optional[Dict[str, Any]]:
    """CSM ETH rewards for the client's operator id(s) over the period (or None)."""
    if not client.csm_operator_ids:
        return None
    try:
        from lido_csm import CSMRewardsClient
        csm = CSMRewardsClient(rpc_url)
        return csm.get_period_rewards(client.csm_operator_ids, start_ts, end_ts)
    except Exception as e:
        logger.warning(f"⚠️  CSM rewards unavailable ({e}); skipping CSM line")
        return None


def build_invoice(client_id: str, start_epoch: int, end_epoch: int,
                  parquet_file: str = 'rewards_data/rewards_master.parquet',
                  price_client: Optional[PriceClient] = None,
                  eth_price_override: Optional[float] = None,
                  invoice_number: Optional[str] = None,
                  rpc_url: Optional[str] = None,
                  include_onchain: bool = True,
                  config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Assemble the full invoice model for ``client_id`` over [start_epoch, end_epoch].

    Billable streams -> line items: RP-ETH (parquet), CSM-ETH (operator id),
    and RPL (later). stVault is report-only (metrics). ``include_onchain=False``
    skips chain reads (offline / tests).
    """
    company: Company = load_company(config_path) if config_path else load_company()
    client: Client = get_client(client_id, config_path) if config_path else get_client(client_id)
    price_client = price_client or PriceClient(
        overrides={'ETH': eth_price_override} if eth_price_override else None)
    rpc_url = rpc_url or _default_rpc_url()

    eth_usd = price_client.eth_usd()

    start_date = epoch_to_datetime(start_epoch)
    end_date = epoch_to_datetime(end_epoch)
    start_ts, end_ts = int(start_date.timestamp()), int(end_date.timestamp())

    metrics: Dict[str, Any] = {'prices': {'ETH_USD': eth_usd}}
    line_items: List[LineItem] = []

    # --- RP-node ETH (billable) ---
    rp = _rp_eth_stream(client, parquet_file, start_epoch, end_epoch)
    metrics.update(rp['metrics'])
    if rp['net_eth'] > 0:
        line_items.append(_billable_line(
            description='Ethereum Node Monthly Maintenance (ETH)',
            earned=rp['net_eth'], token_symbol='ETH',
            fee_rate=client.fee_rate, usd_rate=eth_usd,
        ))

    # --- Lido CSM ETH (billable) ---
    if include_onchain:
        csm = _csm_stream(client, start_ts, end_ts, rpc_url)
        if csm is not None:
            metrics['csm'] = {
                'operator_ids': csm['operator_ids'],
                'period_eth': csm['period_eth'],
                'cumulative_eth': csm['cumulative_eth'],
            }
            if csm['period_eth'] > 0:
                line_items.append(_billable_line(
                    description='Lido CSM ETH Earnings',
                    earned=csm['period_eth'], token_symbol='ETH',
                    fee_rate=client.fee_rate, usd_rate=eth_usd,
                ))

    subtotal = sum(li.amount_usd for li in line_items)
    now = datetime.now(timezone.utc)

    return {
        'meta': {
            'invoice_number': invoice_number or f"INV-{now.strftime('%Y%m%d')}-{start_epoch}",
            'invoice_date': now.strftime('%d %b %Y'),
            'due_date': now.strftime('%d %b %Y'),
            'terms': company.terms,
        },
        'company': company,
        'client': client,
        'line_items': [li.as_dict() for li in line_items],
        'subtotal_usd': subtotal,
        'total_usd': subtotal,
        'balance_due_usd': subtotal,
        'metrics': metrics,
        'period': {
            'start_epoch': start_epoch, 'end_epoch': end_epoch,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'days': (end_date - start_date).days,
        },
    }
