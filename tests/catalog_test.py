import datetime
from dataclasses import replace
import json
from pathlib import Path

import pytest

from spells import catalog
from spells.catalog import Catalog, Freshness, RemoteFile, Target
from spells.enums import EventType, View

FIXTURE = Path(__file__).parent / "data" / "prismic_public_data.json"


@pytest.fixture
def parsed() -> Catalog:
    with open(FIXTURE) as f:
        return catalog.parse(json.load(f))


def test_parse_reads_every_dataset(parsed):
    assert len(parsed.datasets) == 5


def test_parse_maps_known_formats_to_event_types(parsed):
    premier = parsed.get("MSH", EventType.PREMIER)
    assert premier is not None
    assert premier.format_name == "PremierDraft"
    assert premier.is_supported


def test_parse_extracts_s3_urls_from_hyperlink_spans(parsed):
    premier = parsed.get("MSH", EventType.PREMIER)
    assert premier.draft_url == (
        "https://17lands-public.s3.amazonaws.com/analysis_data/draft_data/"
        "draft_data_public.MSH.PremierDraft.csv.gz"
    )
    assert premier.url(View.GAME).endswith("game_data_public.MSH.PremierDraft.csv.gz")


def test_parse_marks_unpublished_dataset_as_no_url(parsed):
    """KHM renders a literal "-" for draft data; the object really is 403."""
    khm = parsed.get("KHM", EventType.PREMIER)
    assert khm.draft_url is None
    assert khm.game_url is not None


def test_parse_keeps_unsupported_formats_but_leaves_event_type_unset(parsed):
    sealed = [d for d in parsed.datasets if d.format_name == "Sealed"]
    assert len(sealed) == 1
    assert sealed[0].event_type is None
    assert not sealed[0].is_supported


def test_get_never_matches_an_unsupported_format(parsed):
    """`get` is keyed by EventType, so Sealed rows are unreachable through it."""
    for dataset in parsed.datasets:
        if not dataset.is_supported:
            continue
        assert parsed.get(dataset.expansion, dataset.event_type) is not None


def test_parse_reads_last_updated_as_a_date(parsed):
    assert parsed.get("BLB", EventType.TRADITIONAL).last_updated == datetime.date(
        2024, 9, 5
    )


def test_expansions_are_deduplicated_in_order(parsed):
    assert parsed.expansions == ("MSH", "KHM", "BLB")


def test_for_expansion_groups_every_format(parsed):
    assert len(parsed.for_expansion("MSH")) == 3


def test_parse_skips_rows_missing_identity():
    assert catalog.parse({"data": {"datasets": [{"expansion": [], "format": []}]}}) == (
        Catalog(datasets=())
    )


def test_parse_tolerates_an_empty_document():
    assert catalog.parse({}).datasets == ()


def test_fetch_falls_back_when_17lands_is_unreachable(monkeypatch):
    import urllib.error

    monkeypatch.setattr(catalog, "_cached", None)

    def unreachable(url):
        raise urllib.error.URLError("no network")

    monkeypatch.setattr(catalog, "_get_json", unreachable)

    result = catalog.fetch()
    assert result.is_fallback
    assert result.datasets == ()


def test_fallback_url_matches_the_published_layout(parsed):
    assert (
        catalog.fallback_url("MSH", View.DRAFT, EventType.PREMIER)
        == parsed.get("MSH", EventType.PREMIER).draft_url
    )


UTC = datetime.timezone.utc


def _remote(last_modified: datetime.datetime | None) -> RemoteFile:
    return RemoteFile(url="https://example/x.csv.gz", last_modified=last_modified)


def test_compare_reports_stale_when_remote_is_newer():
    local = datetime.datetime(2026, 7, 1, tzinfo=UTC).timestamp()
    remote = _remote(datetime.datetime(2026, 7, 27, tzinfo=UTC))
    assert catalog.compare(local, remote) == Freshness.STALE


def test_compare_reports_current_when_local_is_newer():
    local = datetime.datetime(2026, 8, 5, tzinfo=UTC).timestamp()
    remote = _remote(datetime.datetime(2026, 7, 27, tzinfo=UTC))
    assert catalog.compare(local, remote) == Freshness.CURRENT


def test_compare_reports_absent_when_published_but_not_downloaded():
    remote = _remote(datetime.datetime(2026, 7, 27, tzinfo=UTC))
    assert catalog.compare(None, remote) == Freshness.ABSENT


def test_compare_reports_unpublished_when_neither_side_has_it():
    assert catalog.compare(None, None) == Freshness.UNPUBLISHED


def test_compare_will_not_call_a_local_file_unpublished():
    """We have the data, so whatever the remote is doing, it is not "unpublished"."""
    local = datetime.datetime(2026, 8, 5, tzinfo=UTC).timestamp()
    assert catalog.compare(local, None) == Freshness.UNKNOWN


def test_compare_is_unknown_without_a_last_modified_header():
    local = datetime.datetime(2026, 8, 5, tzinfo=UTC).timestamp()
    assert catalog.compare(local, _remote(None)) == Freshness.UNKNOWN


STALE_MTIME = datetime.datetime(2026, 1, 1, tzinfo=UTC).timestamp()


@pytest.fixture
def no_head(monkeypatch):
    """Every HEAD reports the same recent mtime, so freshness turns purely on
    what the caller passes as the local mtime."""
    calls = []

    def fake_head(url):
        calls.append(url)
        return RemoteFile(
            url=url, last_modified=datetime.datetime(2026, 7, 27, tzinfo=UTC)
        )

    monkeypatch.setattr(catalog, "head", fake_head)
    return calls


def test_resolve_heads_only_files_we_already_hold(parsed, no_head):
    targets = [
        Target("MSH", EventType.PREMIER, View.DRAFT, STALE_MTIME),
        Target("MSH", EventType.PREMIER, View.GAME, None),
    ]
    rows = catalog.resolve(targets, parsed)

    assert len(no_head) == 1
    assert rows[0].freshness == Freshness.STALE
    assert rows[1].freshness == Freshness.ABSENT


def test_resolve_reports_absent_with_the_url_it_would_fetch(parsed, no_head):
    (row,) = catalog.resolve(
        [Target("MSH", EventType.PREMIER, View.GAME, None)], parsed
    )
    assert row.remote.url.endswith("game_data_public.MSH.PremierDraft.csv.gz")


def test_resolve_calls_an_unpublished_dataset_unpublished(parsed, no_head):
    (row,) = catalog.resolve(
        [Target("KHM", EventType.PREMIER, View.DRAFT, None)], parsed
    )
    assert row.freshness == Freshness.UNPUBLISHED
    assert not no_head


def test_resolve_will_not_claim_unpublished_when_the_catalog_is_unreachable(no_head):
    (row,) = catalog.resolve(
        [Target("MSH", EventType.PREMIER, View.DRAFT, None)],
        Catalog(datasets=(), is_fallback=True),
    )
    assert row.freshness == Freshness.UNKNOWN


def test_resolve_preserves_target_order(parsed, no_head):
    targets = [
        Target("KHM", EventType.PREMIER, View.DRAFT, None),
        Target("MSH", EventType.PREMIER, View.DRAFT, STALE_MTIME),
        Target("BLB", EventType.TRADITIONAL, View.GAME, None),
    ]
    rows = catalog.resolve(targets, parsed)
    assert [r.target for r in rows] == targets


def test_needs_download_covers_stale_and_absent_only(parsed, no_head):
    rows = catalog.resolve(
        [
            Target("MSH", EventType.PREMIER, View.DRAFT, STALE_MTIME),
            Target("MSH", EventType.PREMIER, View.GAME, None),
            Target("KHM", EventType.PREMIER, View.DRAFT, None),
            Target("BLB", EventType.TRADITIONAL, View.DRAFT, _now()),
        ],
        parsed,
    )
    assert [r.needs_download for r in rows] == [True, True, False, False]


def _now() -> float:
    return datetime.datetime(2026, 8, 7, tzinfo=UTC).timestamp()


def test_is_addable_requires_published_draft_data(parsed):
    """KHM's game data is published but its draft data 403s, so it cannot be added."""
    assert parsed.is_addable("MSH")
    assert not parsed.is_addable("KHM")


def test_updated_takes_the_newest_across_event_types(parsed):
    assert parsed.updated("MSH") == datetime.date(2026, 7, 26)


def test_unadded_reports_missing_expansions_newest_first(parsed):
    """Catalog order is MSH, KHM, BLB; holding KHM anchors at position 1."""
    assert catalog.unadded(parsed, {"KHM"}) == ["BLB"]


def test_unadded_ignores_expansions_predating_the_collection(parsed):
    """BLB is last in this fixture, so holding it means nothing is newer."""
    assert catalog.unadded(parsed, {"BLB"}) == []


def test_unadded_never_offers_a_set_with_no_draft_data(parsed):
    """KHM sits at position 1, so it is in range but still must not be offered."""
    assert "KHM" not in catalog.unadded(parsed, {"MSH"})


def test_unadded_with_nothing_held_offers_everything_addable(parsed):
    assert catalog.unadded(parsed, set()) == ["BLB", "MSH"]


def test_unadded_is_empty_when_everything_is_held(parsed):
    assert catalog.unadded(parsed, {"MSH", "KHM", "BLB"}) == []


def test_unadded_ignores_held_sets_absent_from_the_catalog(parsed):
    """A local set 17Lands has retired must not break the anchor."""
    assert catalog.unadded(parsed, {"BLB", "NOTASET"}) == []


def test_unadded_orders_by_catalog_position_not_last_updated(parsed):
    """17Lands bumps last_updated when regenerating old files; array order is
    the stable release signal, so a regenerated old set must not look new."""
    stale_first = Catalog(
        datasets=tuple(
            d if d.expansion != "KHM" else replace(d, last_updated=datetime.date(2099, 1, 1))
            for d in parsed.datasets
        )
    )
    assert catalog.unadded(stale_first, set()) == ["BLB", "MSH"]
