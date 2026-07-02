"""
Tests for the dated file-cache around 17lands' /data/filters endpoint
(spells.card_data_files._fetch_filters / expansion_start_date).

No network calls are made: wget.download is monkeypatched per-test to either
simulate a successful fetch (writes fake content to the requested path) or a
failure (raises), and the dated cache directory is pre-seeded directly where a
test wants to exercise the "already cached" or "stale fallback" paths.
"""

import datetime
import json

import pytest

from spells import card_data_files, cache


FAKE_FILTERS_TODAY = {"start_dates": {"TST": "2026-01-01T15:00:00Z"}}
FAKE_FILTERS_STALE = {"start_dates": {"TST": "2025-06-01T15:00:00Z"}}


@pytest.fixture()
def isolated_data_home(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    yield tmp_path


def _write_filters_cache(data_home, fetch_date: datetime.date, content: dict):
    target_dir, filename = cache.filters_file_path(fetch_date)
    from pathlib import Path

    Path(target_dir).mkdir(parents=True, exist_ok=True)
    (Path(target_dir) / filename).write_text(json.dumps(content))


def test_fetch_filters_uses_todays_cache_without_network(isolated_data_home, monkeypatch):
    _write_filters_cache(isolated_data_home, datetime.date.today(), FAKE_FILTERS_TODAY)

    def _no_network(*args, **kwargs):
        raise AssertionError("must not hit the network when today's cache exists")

    monkeypatch.setattr(card_data_files.wget, "download", _no_network)

    result = card_data_files._fetch_filters()
    assert result == FAKE_FILTERS_TODAY


def test_fetch_filters_downloads_and_caches_when_missing(isolated_data_home, monkeypatch):
    def _fake_download(url, out):
        assert url == card_data_files.FILTERS_URL
        from pathlib import Path

        Path(out).write_text(json.dumps(FAKE_FILTERS_TODAY))
        return out

    monkeypatch.setattr(card_data_files.wget, "download", _fake_download)

    result = card_data_files._fetch_filters()
    assert result == FAKE_FILTERS_TODAY

    # Cached to today's dated file, so a second call needs no network at all.
    monkeypatch.setattr(
        card_data_files.wget,
        "download",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should be cached")),
    )
    assert card_data_files._fetch_filters() == FAKE_FILTERS_TODAY


def test_fetch_filters_falls_back_to_stale_cache_when_offline(isolated_data_home, monkeypatch, caplog):
    stale_date = datetime.date.today() - datetime.timedelta(days=3)
    _write_filters_cache(isolated_data_home, stale_date, FAKE_FILTERS_STALE)

    def _offline(*args, **kwargs):
        raise OSError("network unreachable")

    monkeypatch.setattr(card_data_files.wget, "download", _offline)

    result = card_data_files._fetch_filters()
    assert result == FAKE_FILTERS_STALE


def test_fetch_filters_raises_when_offline_and_no_cache(isolated_data_home, monkeypatch):
    def _offline(*args, **kwargs):
        raise OSError("network unreachable")

    monkeypatch.setattr(card_data_files.wget, "download", _offline)

    with pytest.raises(RuntimeError, match="Could not fetch"):
        card_data_files._fetch_filters()


def test_expansion_start_date_parses_iso_timestamp(isolated_data_home, monkeypatch):
    _write_filters_cache(isolated_data_home, datetime.date.today(), FAKE_FILTERS_TODAY)
    assert card_data_files.expansion_start_date("TST") == datetime.date(2026, 1, 1)


def test_expansion_start_date_raises_for_unknown_set(isolated_data_home, monkeypatch):
    _write_filters_cache(isolated_data_home, datetime.date.today(), FAKE_FILTERS_TODAY)
    with pytest.raises(ValueError, match="No known start date"):
        card_data_files.expansion_start_date("NOPE")
