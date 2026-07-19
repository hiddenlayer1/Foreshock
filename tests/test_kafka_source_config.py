"""Tests for the Kafka source's configuration and mode guard.

No broker involved: these cover the settings handed to librdkafka and the guard
that keeps the two consumption modes from being mixed.
"""

from __future__ import annotations

import pytest

from foreshock.kafka_source import MCL_VERSIONED_TOPIC, MclSource, SourceConfig


def test_defaults_target_the_versioned_log() -> None:
    """Timeseries MCL carries profiling stats, which cannot break a consumer."""
    assert SourceConfig().topic == MCL_VERSIONED_TOPIC


def test_offsets_are_committed_manually() -> None:
    """Auto-commit would acknowledge an event before its walk finished."""
    assert SourceConfig().consumer_settings()["enable.auto.commit"] is False


def test_address_family_is_pinned_to_ipv4() -> None:
    """Podman publishes on IPv4 while the broker advertises "localhost".

    On a dual-stack host that resolves to ::1 and every connection fails after
    bootstrap has already succeeded, which reads as a broker problem.
    """
    assert SourceConfig().consumer_settings()["broker.address.family"] == "v4"


def test_assign_from_end_refuses_a_subscribed_source() -> None:
    """Mixing the modes fails silently in librdkafka, so refuse it up front.

    A subscribed consumer that is later assigned still reports a populated
    assignment() and then delivers nothing at all. Raising keeps that failure
    one line to diagnose instead of an empty stream.
    """
    source = MclSource.__new__(MclSource)
    source._subscribed = True  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="subscribe=False"):
        source.assign_from_end()
