"""
Unit tests for the invoice pipeline (network-free).

Covers the billing math (fee %, USD conversion), RPL precedence, CSM Merkle-tree
parsing, and snapshot period diffs. Chain/price calls are avoided via
``include_onchain=False`` and pinned price overrides.
"""

import json
import sys
import os

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pricing import PriceClient
from rpl_rewards import resolve_period_rpl
from lido_csm import CSMRewardsClient
import snapshots
import invoice_data


# --------------------------------------------------------------------------
# pricing + RPL precedence
# --------------------------------------------------------------------------
def test_price_override_wins():
    pc = PriceClient(overrides={'ETH': 2350.0})
    assert pc.eth_usd() == 2350.0  # no network call


def test_rpl_precedence():
    assert resolve_period_rpl(1378.0, 50.0) == 1378.0   # explicit wins
    assert resolve_period_rpl(None, 50.0) == 50.0       # client default
    assert resolve_period_rpl(None, None) == 0.0        # none -> no line


# --------------------------------------------------------------------------
# CSM tree parsing (pure, no chain)
# --------------------------------------------------------------------------
def test_csm_cumulative_shares_in_tree():
    tree = {'format': 'standard-v1', 'leafEncoding': ['uint256', 'uint256'],
            'values': [{'value': ['104', '111']}, {'value': ['105', '5819615975568454600']}]}
    assert CSMRewardsClient.cumulative_shares_in_tree(tree, 105) == 5819615975568454600
    assert CSMRewardsClient.cumulative_shares_in_tree(tree, 999) == 0  # absent -> 0


# --------------------------------------------------------------------------
# snapshot diffs
# --------------------------------------------------------------------------
def test_snapshot_period_value(tmp_path):
    f = tmp_path / 'snap.parquet'
    pd.DataFrame([
        {'ts': 100, 'block': 1, 'metric': 'vault_total_value', 'key': '0xabc', 'value': 10.0},
        {'ts': 200, 'block': 2, 'metric': 'vault_total_value', 'key': '0xabc', 'value': 12.5},
        {'ts': 300, 'block': 3, 'metric': 'vault_total_value', 'key': '0xabc', 'value': 15.0},
    ]).to_parquet(f, index=False)
    # nearest-at-or-before semantics
    assert snapshots.value_at(str(f), 'vault_total_value', '0xABC', 250) == 12.5
    assert snapshots.period_value(str(f), 'vault_total_value', '0xabc', 100, 300) == 5.0
    assert snapshots.value_at(str(f), 'vault_total_value', '0xabc', 50) is None  # before first


# --------------------------------------------------------------------------
# end-to-end billing math (synthetic parquet, no chain)
# --------------------------------------------------------------------------
@pytest.fixture
def synthetic(tmp_path):
    # One solo (type 32) validator on node '1' with a 1 ETH withdrawal + 0.5 ETH proposal.
    rows = [
        {'record_type': 'withdrawal', 'validator_index': 1, 'amount': 1_000_000_000,  # 1 ETH gwei
         'epoch': 100, 'datetime': 1700000000, 'validator_type': '32', 'node': '1',
         'minipool': '', 'mev_source': None, 'exec_block_number': None, 'is_exit': False},
        {'record_type': 'proposal', 'validator_index': 1, 'amount': 500_000_000_000_000_000,  # 0.5 ETH wei
         'epoch': 100, 'datetime': 1700000000, 'validator_type': '32', 'node': '1',
         'minipool': '', 'mev_source': 'flashbots', 'exec_block_number': 123, 'is_exit': False},
    ]
    pq = tmp_path / 'rewards.parquet'
    pd.DataFrame(rows).to_parquet(pq, index=False)

    cfg = tmp_path / 'clients.json'
    cfg.write_text(json.dumps({
        'company': {'name': 'LIBC', 'address_lines': []},
        'clients': {'test': {'name': 'Test', 'bill_to': 'Test', 'fee_rate': 0.05,
                             'rp_node_ids': ['1']}},
    }))
    return str(pq), cfg


def test_billing_math(synthetic):
    pq, cfg = synthetic
    inv = invoice_data.build_invoice(
        'test', 100, 100, parquet_file=pq,
        price_client=PriceClient(overrides={'ETH': 2000.0}),
        include_onchain=False, config_path=cfg)

    # Solo validator -> 100% of rewards: 1 ETH withdrawal + 0.5 ETH proposal = 1.5 ETH net.
    assert len(inv['line_items']) == 1
    li = inv['line_items'][0]
    assert li['earned'] == pytest.approx(1.5)
    assert li['qty'] == pytest.approx(1.5 * 0.05)        # 5% fee portion
    assert li['rate'] == 2000.0
    assert li['amount'] == pytest.approx(1.5 * 0.05 * 2000.0)  # $150
    assert inv['balance_due_usd'] == pytest.approx(150.0)
    # report-only metrics present, MEV block counted
    assert inv['metrics']['blocks_proposed'] == 1
    assert inv['metrics']['mev_blocks'] == 1


def test_no_onchain_means_no_csm_or_stvault(synthetic):
    pq, cfg = synthetic
    inv = invoice_data.build_invoice(
        'test', 100, 100, parquet_file=pq,
        price_client=PriceClient(overrides={'ETH': 2000.0}),
        include_onchain=False, config_path=cfg)
    assert 'csm' not in inv['metrics']
    assert 'stvault' not in inv['metrics']
