#!/usr/bin/env python3
"""
Reward data sources.

A *data source* turns ``(validator_indices, epoch)`` into the flat reward
records that ``ParquetWriter`` persists. Two implementations exist:

  * ``LocalNodeDataSource``  - queries the self-hosted Lighthouse / Nethermind
        nodes and MEV relays (the migration target).

  * ``BeaconchainDataSource`` - wraps the legacy Beaconcha.in client. Kept as
        a fallback during the migration and for epochs older than the local
        node's retention window.

Both emit *identical* record dictionaries so the rest of the pipeline -
and the migration comparison tests - are source-agnostic::

    withdrawal: record_type='withdrawal', amount in Gwei, mev_source=None
    proposal:   record_type='proposal',   amount in Wei,  mev_source=<tag or ''>
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _base_record(record_type: str, validator_index: int, amount: int, epoch: int,
                 timestamp: Optional[int], validator_info: Optional[Dict[str, str]],
                 mev_source: Optional[str], exec_block_number: Optional[int],
                 is_exit: bool) -> Dict[str, Any]:
    """Build a record matching the canonical Parquet schema."""
    return {
        'record_type': record_type,
        'validator_index': validator_index,
        'amount': amount,
        'epoch': epoch,
        'datetime': timestamp,
        'validator_type': validator_info['type'] if validator_info else '',
        'node': validator_info['node'] if validator_info else '',
        'minipool': validator_info['minipool'] if validator_info else '',
        'mev_source': mev_source,
        'exec_block_number': exec_block_number,
        'is_exit': is_exit,
    }


class LocalNodeDataSource:
    """Collects rewards directly from local nodes and MEV relays."""

    def __init__(self, validator_reader, lighthouse, execution, mev_relay):
        self.validator_reader = validator_reader
        self.lighthouse = lighthouse
        self.execution = execution
        self.mev_relay = mev_relay
        # Single-epoch block cache so withdrawals and proposals for the same
        # epoch only trigger one pass over the 32 slots.
        self._cache_epoch: Optional[int] = None
        self._cache_blocks: Optional[List[Dict[str, Any]]] = None
        # Validator status is a HEAD query - identical for every epoch in a run,
        # so cache it per validator-set instead of re-fetching 14k times.
        self._status_cache: Dict[tuple, Dict[str, str]] = {}

    def _get_statuses(self, validator_indices: List[str]) -> Dict[str, str]:
        key = tuple(validator_indices)
        statuses = self._status_cache.get(key)
        if statuses is None:
            statuses = self.lighthouse.get_validator_statuses(validator_indices)
            self._status_cache[key] = statuses
        return statuses

    def _get_blocks(self, epoch: int) -> List[Dict[str, Any]]:
        if self._cache_epoch != epoch or self._cache_blocks is None:
            self._cache_blocks = self.lighthouse.get_epoch_blocks(epoch)
            self._cache_epoch = epoch
        return self._cache_blocks

    def collect_withdrawals(self, validator_indices: List[str], epoch: int) -> List[Dict[str, Any]]:
        blocks = self._get_blocks(epoch)
        statuses = self._get_statuses(validator_indices)
        raw = self.lighthouse.get_withdrawals(validator_indices, epoch, blocks=blocks)

        records = []
        exit_count = 0
        for w in raw:
            index = str(w['validatorindex'])
            info = self.validator_reader.get_validator_by_index(index)
            is_exit = self.lighthouse.is_validator_exited(statuses.get(index, ''))
            if is_exit:
                exit_count += 1
            records.append(_base_record(
                'withdrawal', w['validatorindex'], w['amount'], w['epoch'],
                w.get('timestamp'), info, None, None, is_exit,
            ))
        if exit_count:
            logger.info(f"🚪 Found {exit_count} exit withdrawals in epoch {epoch}")
        return records

    def collect_proposals(self, validator_indices: List[str], epoch: int) -> List[Dict[str, Any]]:
        blocks = self._get_blocks(epoch)
        raw = self.lighthouse.get_proposals(validator_indices, epoch, blocks=blocks)

        records = []
        for p in raw:
            block_number = p['exec_block_number']
            mev_source, amount = self._resolve_proposal_reward(block_number)
            index = str(p['proposerindex'])
            info = self.validator_reader.get_validator_by_index(index)
            records.append(_base_record(
                'proposal', p['proposerindex'], amount, p['epoch'],
                p.get('timestamp'), info, mev_source, block_number, False,
            ))
        return records

    def _resolve_proposal_reward(self, block_number: int) -> Tuple[str, int]:
        """
        Resolve a proposal's reward and attribution.

        If an MEV relay delivered the block, use its (Wei) value and tag.
        Otherwise fall back to the execution-layer producer reward (priority
        fees), matching Beaconcha.in's relay / producerReward distinction.
        """
        payload = self.mev_relay.get_payload(block_number)
        if payload is not None:
            logger.info(f"🚪 MEV relay {payload['relay']} delivered block {block_number}")
            return payload['relay'], payload['value']

        logger.info(f"🚫 No MEV relay for block {block_number}; using EL producer reward")
        return '', self.execution.get_producer_reward(block_number)

    def collect_epoch(self, validator_indices: List[str], epoch: int
                      ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        return (self.collect_withdrawals(validator_indices, epoch),
                self.collect_proposals(validator_indices, epoch))


class BeaconchainDataSource:
    """Legacy Beaconcha.in data source (fallback during migration)."""

    def __init__(self, validator_reader, api, processor, chunk_size: int = 100):
        self.validator_reader = validator_reader
        self.api = api
        self.processor = processor
        self.chunk_size = chunk_size

    def _chunks(self) -> List[List[str]]:
        return self.validator_reader.chunk_validators(self.chunk_size)

    async def collect_withdrawals(self, validator_indices: List[str], epoch: int) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for chunk in self._chunks():
            statuses = self.api.get_validator_statuses(chunk)
            data = self.api.get_withdrawals(chunk, epoch)
            records.extend(await self.processor.process_withdrawals(data, epoch, statuses))
        return records

    async def collect_proposals(self, validator_indices: List[str], epoch: int) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for chunk in self._chunks():
            data = self.api.get_proposals(chunk, epoch)
            records.extend(await self.processor.process_proposals(data, epoch))
        return records
