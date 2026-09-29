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


class _Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code} Client Error")

    def json(self):
        return self._body


def test_price_falls_back_to_coinbase_on_coingecko_403(monkeypatch):
    monkeypatch.delenv('COINGECKO_API_KEY', raising=False)
    pc = PriceClient()
    calls = []

    def get(url, **kw):
        calls.append(url)
        if 'coingecko' in url:
            return _Resp(403)
        return _Resp(200, {'data': {'amount': '2735.765', 'base': 'ETH', 'currency': 'USD'}})

    pc.session.get = get
    assert pc.eth_usd() == 2735.765
    assert pc.eth_usd() == 2735.765          # cached
    assert len(calls) == 2
    assert calls[1] == 'https://api.coinbase.com/v2/prices/ETH-USD/spot'


def test_price_sends_coingecko_demo_key():
    pc = PriceClient(coingecko_api_key='CG-test')
    seen = {}

    def get(url, headers=None, **kw):
        seen['headers'] = headers
        return _Resp(200, {'rocket-pool': {'usd': 2.03}})

    pc.session.get = get
    assert pc.rpl_usd() == 2.03
    assert seen['headers'] == {'x-cg-demo-api-key': 'CG-test'}


def test_price_unavailable_when_all_sources_fail(monkeypatch):
    from pricing import PriceUnavailable
    monkeypatch.delenv('COINGECKO_API_KEY', raising=False)
    pc = PriceClient()
    pc.session.get = lambda url, **kw: _Resp(403)
    with pytest.raises(PriceUnavailable, match='CoinGecko.*Coinbase'):
        pc.eth_usd()


def test_steth_falls_back_to_eth_without_coinbase_pair(monkeypatch):
    monkeypatch.delenv('COINGECKO_API_KEY', raising=False)
    pc = PriceClient()

    def get(url, **kw):
        if 'coingecko' in url:
            return _Resp(403)
        assert 'ETH-USD' in url                 # never asks Coinbase for stETH
        return _Resp(200, {'data': {'amount': '2700.0'}})

    pc.session.get = get
    assert pc.steth_usd() == 2700.0


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
        config_path=cfg)

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


def test_gross_rewards_exclude_exit_principal(synthetic):
    """Regression, INV-20260929-464963: page 2 'Gross rewards' summed exit
    principal in (2,078 ETH gross against 32 ETH net)."""
    pq, cfg = synthetic
    df = pd.read_parquet(pq)
    exit_row = df.iloc[0].copy()
    exit_row['validator_index'] = 2
    exit_row['amount'] = 32_435_000_000                  # 32 ETH principal + 0.435 excess
    exit_row['is_exit'] = True
    pd.concat([df, exit_row.to_frame().T], ignore_index=True).to_parquet(pq, index=False)

    inv = invoice_data.build_invoice(
        'test', 100, 100, parquet_file=pq,
        price_client=PriceClient(overrides={'ETH': 2000.0}),
        config_path=cfg)

    m = inv['metrics']
    assert m['rp_exits_eth'] == pytest.approx(32.0)
    # 1 ETH skim + 0.435 exit excess + 0.5 ETH proposal; principal excluded.
    assert m['rp_gross_eth'] == pytest.approx(1.935)
    assert m['rp_net_eth'] == pytest.approx(1.935)       # solo: gross == net
    assert inv['line_items'][0]['earned'] == pytest.approx(1.935)


def test_vault_report_event_decode():
    """A synthetic VaultReportApplied data payload decodes to the right fields."""
    from eth_abi import encode, decode
    from lido_vault import _REPORT_EVENT_TYPES
    # (reportTimestamp, totalValue, inOutDelta, cumulativeLidoFees, liab, maxliab, slash)
    values = [1780833611, 3780 * 10**18, 3706 * 10**18, 1562 * 10**15, 2831 * 10**18, 0, 0]
    data = encode(_REPORT_EVENT_TYPES, values)
    ts, tv, io, cfees, liab, maxliab, slash = decode(_REPORT_EVENT_TYPES, data)
    assert ts == 1780833611
    assert (tv - io) / 1e18 == pytest.approx(74.0)        # rewards = tv - io
    assert cfees / 1e18 == pytest.approx(1.562)


def test_vault_period_delta(monkeypatch):
    """period rewards/fees = (last report in window) − (report before window)."""
    from lido_vault import StVaultClient
    sv = StVaultClient.__new__(StVaultClient)  # skip __init__/network
    monkeypatch.setattr(sv, 'get_state', lambda: {
        'total_value_eth': 3780.0, 'cumulative_rewards_eth': 73.6,
        'lido_fees_cumulative_eth': 1.562, 'lido_fees_settled_eth': 1.388,
        'fee_rates_bps': {'reserve_ratio': 500, 'infra': 0, 'liquidity': 650, 'reservation': 0},
        'no_fee_rate_pct': 5.0, 'no_fee_accrued_eth': 0.48, 'last_report_ts': 0})
    monkeypatch.setattr(sv, 'get_reports', lambda a, b: [
        {'ts': 100, 'total_value_eth': 3778.0, 'inOutDelta': 0,
         'rewards_eth': 70.0, 'cumulative_lido_fees_eth': 1.0},   # before period
        {'ts': 200, 'total_value_eth': 3780.0, 'inOutDelta': 0,
         'rewards_eth': 73.0, 'cumulative_lido_fees_eth': 1.2},   # within
    ])
    out = sv.get_period_report(150, 300)
    assert out['reports_in_period'] == 1
    assert out['period_rewards_eth'] == pytest.approx(3.0)        # 73 - 70
    assert out['lido_fees_period_eth'] == pytest.approx(0.2)      # 1.2 - 1.0
    assert out['report_only'] is True


def test_no_onchain_means_no_csm_or_stvault(synthetic):
    pq, cfg = synthetic
    inv = invoice_data.build_invoice(
        'test', 100, 100, parquet_file=pq,
        price_client=PriceClient(overrides={'ETH': 2000.0}),
        config_path=cfg)
    assert 'csm' not in inv['metrics']
    assert 'stvault' not in inv['metrics']
