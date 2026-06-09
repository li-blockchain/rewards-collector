"""
Unit tests for MEVRelayClient (mev_relay_client.py).

Covers single-relay queries, multi-relay scanning, priority-ordered
attribution, value normalisation (Wei string -> int), and graceful handling
of unreachable relays / empty results / malformed payloads - all mocked.
"""

import sys
import os
from unittest.mock import Mock

import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mev_relay_client import MEVRelayClient


def make_response(status_code=200, json_data=None, raise_json=False):
    resp = Mock()
    resp.status_code = status_code
    if raise_json:
        resp.json.side_effect = ValueError("bad json")
    else:
        resp.json.return_value = json_data if json_data is not None else []

    def raise_for_status():
        if status_code >= 400:
            raise requests.HTTPError(f"{status_code} Error")
    resp.raise_for_status.side_effect = raise_for_status
    return resp


def delivered_trace(value, slot=14515840, builder='0xbuilder'):
    """A proposer_payload_delivered entry as the relays return it."""
    return [{
        'slot': str(slot),
        'block_hash': '0xhash',
        'builder_pubkey': builder,
        'proposer_pubkey': '0xproposer',
        'value': str(value),  # Wei, as a string
    }]


# Two-relay registry so tests don't depend on the production relay list.
TWO_RELAYS = {
    'ultrasound-relay': 'https://relay.ultrasound.money',
    'flashbots': 'https://boost-relay.flashbots.net',
}


class TestQueryRelay:
    def test_returns_trace_with_tag_and_int_value(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.return_value = make_response(200, delivered_trace(18990254566602138))
        trace = client.query_relay('ultrasound-relay', 25280368)
        assert trace['relay'] == 'ultrasound-relay'
        assert trace['value'] == 18990254566602138
        assert isinstance(trace['value'], int)

    def test_empty_array_returns_none(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.return_value = make_response(200, [])
        assert client.query_relay('flashbots', 25280368) is None

    def test_unknown_tag_returns_none(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        assert client.query_relay('does-not-exist', 1) is None

    def test_connection_error_returns_none(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.side_effect = requests.ConnectionError("down")
        assert client.query_relay('flashbots', 1) is None

    def test_http_error_returns_none(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.return_value = make_response(500)
        assert client.query_relay('flashbots', 1) is None

    def test_malformed_json_returns_none(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.return_value = make_response(200, raise_json=True)
        assert client.query_relay('flashbots', 1) is None

    def test_missing_value_returns_none(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.return_value = make_response(200, [{'slot': '1'}])  # no value
        assert client.query_relay('flashbots', 1) is None


class TestGetPayload:
    def test_returns_first_relay_in_priority_order(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()

        # ultrasound (first in order) delivers; flashbots would too but
        # should never be queried once a match is found.
        def get(url, params=None, **kwargs):
            if 'ultrasound' in url:
                return make_response(200, delivered_trace(111))
            return make_response(200, delivered_trace(999))
        client.session.get.side_effect = get

        payload = client.get_payload(25280368)
        assert payload['relay'] == 'ultrasound-relay'
        assert payload['value'] == 111

    def test_returns_none_when_no_relay_delivered(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.return_value = make_response(200, [])
        assert client.get_payload(25280368) is None

    def test_falls_through_to_second_relay(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()

        def get(url, params=None, **kwargs):
            if 'ultrasound' in url:
                return make_response(200, [])  # ultrasound didn't deliver
            return make_response(200, delivered_trace(777))
        client.session.get.side_effect = get

        payload = client.get_payload(25280368)
        assert payload['relay'] == 'flashbots'
        assert payload['value'] == 777


class TestQueryAllRelays:
    def test_collects_all_matches(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.return_value = make_response(200, delivered_trace(500))
        matches = client.query_all_relays(25280368)
        assert len(matches) == 2
        assert {m['relay'] for m in matches} == {'ultrasound-relay', 'flashbots'}

    def test_empty_when_none_match(self):
        client = MEVRelayClient(relays=TWO_RELAYS)
        client.session = Mock()
        client.session.get.return_value = make_response(200, [])
        assert client.query_all_relays(25280368) == []
