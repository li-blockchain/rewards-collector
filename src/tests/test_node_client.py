"""
Unit tests for the local node clients (node_client.py).

Covers LighthouseClient (beacon API parsing, withdrawal/proposal extraction,
missed-slot handling, exit detection) and ExecutionClient (producer reward
computation), all against mocked HTTP responses.
"""

import sys
import os
from unittest.mock import Mock

import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from node_client import (
    LighthouseClient,
    ExecutionClient,
    MAINNET_GENESIS_TIME,
    MAINNET_SECONDS_PER_SLOT,
)


def make_response(status_code=200, json_data=None):
    """Build a stand-in requests.Response."""
    resp = Mock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}

    def raise_for_status():
        if status_code >= 400:
            raise requests.HTTPError(f"{status_code} Error")
    resp.raise_for_status.side_effect = raise_for_status
    return resp


def block_message(slot, proposer_index, block_number, withdrawals=None, fee_recipient='0xabc'):
    """Build a beacon block 'message' as returned under data.message."""
    return {
        'slot': str(slot),
        'proposer_index': str(proposer_index),
        'body': {
            'execution_payload': {
                'block_number': str(block_number),
                'fee_recipient': fee_recipient,
                'withdrawals': withdrawals or [],
            }
        }
    }


class TestLighthouseHelpers:
    def test_slot_to_timestamp(self):
        lh = LighthouseClient()
        assert lh.slot_to_timestamp(0) == MAINNET_GENESIS_TIME
        assert lh.slot_to_timestamp(10) == MAINNET_GENESIS_TIME + 10 * MAINNET_SECONDS_PER_SLOT

    def test_epoch_slots(self):
        lh = LighthouseClient()
        slots = list(lh.epoch_slots(2))
        assert slots[0] == 64 and slots[-1] == 95 and len(slots) == 32

    def test_is_validator_exited(self):
        assert LighthouseClient.is_validator_exited('withdrawal_done') is True
        assert LighthouseClient.is_validator_exited('exited_slashed') is True
        assert LighthouseClient.is_validator_exited('active_ongoing') is False
        assert LighthouseClient.is_validator_exited('') is False


class TestLighthouseFinalizedEpoch:
    def test_returns_int_epoch(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.return_value = make_response(
            200, {'data': {'finalized': {'epoch': '453630'}}})
        assert lh.get_finalized_epoch() == 453630

    def test_returns_zero_on_error(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.side_effect = requests.ConnectionError("down")
        assert lh.get_finalized_epoch() == 0


class TestLighthouseGetBlock:
    def test_missed_slot_returns_none(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.return_value = make_response(404)
        assert lh.get_block(123) is None

    def test_present_block_returns_message(self):
        lh = LighthouseClient()
        lh.session = Mock()
        msg = block_message(100, 5, 999)
        lh.session.get.return_value = make_response(200, {'data': {'message': msg}})
        block = lh.get_block(100)
        assert block['proposer_index'] == '5'

    def test_connection_error_propagates(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.get.side_effect = requests.ConnectionError("timeout")
        with pytest.raises(requests.ConnectionError):
            lh.get_block(100)


class TestLighthouseWithdrawals:
    def test_filters_to_wanted_validators_and_keeps_gwei(self):
        lh = LighthouseClient()
        wds = [
            {'index': '1', 'validator_index': '100', 'address': '0x1', 'amount': '17692142'},
            {'index': '2', 'validator_index': '200', 'address': '0x2', 'amount': '50000000'},
            {'index': '3', 'validator_index': '999', 'address': '0x3', 'amount': '1'},
        ]
        blocks = [block_message(slot=320, proposer_index=7, block_number=1, withdrawals=wds)]
        out = lh.get_withdrawals(['100', '200'], epoch=10, blocks=blocks)
        assert len(out) == 2
        by_idx = {w['validatorindex']: w for w in out}
        assert by_idx[100]['amount'] == 17692142  # Gwei preserved, int
        assert by_idx[100]['epoch'] == 10
        assert by_idx[100]['timestamp'] == lh.slot_to_timestamp(320)
        assert 999 not in by_idx

    def test_empty_when_no_match(self):
        lh = LighthouseClient()
        blocks = [block_message(320, 7, 1, withdrawals=[
            {'index': '1', 'validator_index': '5', 'address': '0x1', 'amount': '10'}])]
        assert lh.get_withdrawals(['100'], epoch=10, blocks=blocks) == []


class TestLighthouseProposals:
    def test_extracts_proposals_for_wanted(self):
        lh = LighthouseClient()
        blocks = [
            block_message(slot=320, proposer_index=100, block_number=555),
            block_message(slot=321, proposer_index=200, block_number=556),
        ]
        out = lh.get_proposals(['100'], epoch=10, blocks=blocks)
        assert len(out) == 1
        assert out[0]['proposerindex'] == 100
        assert out[0]['exec_block_number'] == 555
        assert out[0]['slot'] == 320

    def test_skips_block_without_execution_payload(self):
        lh = LighthouseClient()
        # Pre-merge style block: no block_number in payload.
        msg = {'slot': '320', 'proposer_index': '100',
               'body': {'execution_payload': {'withdrawals': []}}}
        assert lh.get_proposals(['100'], epoch=10, blocks=[msg]) == []


class TestLighthouseGetEpochBlocks:
    def test_skips_missed_slots(self):
        lh = LighthouseClient()
        lh.session = Mock()

        # Slot 0 present, slot 1 missed (404), rest missed.
        def get(url, **kwargs):
            if url.endswith('/0'):
                return make_response(200, {'data': {'message': block_message(0, 1, 1)}})
            return make_response(404)
        lh.session.get.side_effect = get
        lh.slots_per_epoch = 4  # shrink for test
        blocks = lh.get_epoch_blocks(0)
        assert len(blocks) == 1


class TestLighthouseValidatorStatuses:
    def test_maps_index_to_status(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.post.return_value = make_response(200, {'data': [
            {'index': '100', 'status': 'active_ongoing'},
            {'index': '200', 'status': 'withdrawal_done'},
        ]})
        statuses = lh.get_validator_statuses(['100', '200'])
        assert statuses == {'100': 'active_ongoing', '200': 'withdrawal_done'}

    def test_empty_indices_returns_empty(self):
        lh = LighthouseClient()
        assert lh.get_validator_statuses([]) == {}

    def test_error_returns_empty_dict(self):
        lh = LighthouseClient()
        lh.session = Mock()
        lh.session.post.side_effect = requests.ConnectionError("down")
        assert lh.get_validator_statuses(['100']) == {}


class TestExecutionClientProducerReward:
    def _client_with(self, base_fee, receipts):
        ec = ExecutionClient()
        ec.session = Mock()

        def post(url, json=None, **kwargs):
            method = json['method']
            if method == 'eth_getBlockByNumber':
                return make_response(200, {'result': {'baseFeePerGas': hex(base_fee)}})
            if method == 'eth_getBlockReceipts':
                return make_response(200, {'result': receipts})
            raise AssertionError(f"unexpected method {method}")
        ec.session.post.side_effect = post
        return ec

    def test_sums_priority_fees_in_wei(self):
        # base fee 100; two txs paying 150 and 200 effective with gas 1000 each.
        receipts = [
            {'gasUsed': hex(1000), 'effectiveGasPrice': hex(150)},
            {'gasUsed': hex(1000), 'effectiveGasPrice': hex(200)},
        ]
        ec = self._client_with(base_fee=100, receipts=receipts)
        # (150-100)*1000 + (200-100)*1000 = 50_000 + 100_000 = 150_000
        assert ec.get_producer_reward(123) == 150_000

    def test_ignores_negative_tip(self):
        # effective price below base fee should never happen but must not subtract.
        receipts = [{'gasUsed': hex(1000), 'effectiveGasPrice': hex(50)}]
        ec = self._client_with(base_fee=100, receipts=receipts)
        assert ec.get_producer_reward(123) == 0

    def test_raises_on_rpc_error(self):
        ec = ExecutionClient()
        ec.session = Mock()
        ec.session.post.return_value = make_response(200, {'error': {'message': 'boom'}})
        with pytest.raises(RuntimeError):
            ec.get_producer_reward(123)
