"""分次回补、迟到证据与质量确认一致性的回归测试。"""
from __future__ import annotations

import shutil
import tempfile
import threading
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolated_data_root(monkeypatch):
    tmpdir = Path(tempfile.mkdtemp(prefix="geo_consistency_"))
    import app.services.base as base_mod
    monkeypatch.setattr(base_mod, "DATA_ROOT", tmpdir)
    for sub in ["geo_points", "quality_reports", "checkpoints"]:
        (tmpdir / sub).mkdir(parents=True, exist_ok=True)
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


def _batch(svc, key):
    from app.services.geo_source import GeoSourceService
    GeoSourceService().onboard_batch({
        "批次编号": key,
        "采集终端": "TERM-CONS",
        "定位方式": "GPS",
        "采样频率": "1Hz",
    }, point_count=8)


def test_split_writes_late_evidence_and_confirmation_scope():
    from app.services.geo_backfill import GeoBackfillService
    from app.services.geo_quality import GeoQualityService

    key = "CONS-SPLIT"
    _batch(None, key)
    backfill = GeoBackfillService()
    quality = GeoQualityService()
    backfill.register_gap(key, "GAP-4", required_point_ids=["p1", "p2", "p3", "p4"])

    first = backfill.write_coverage_manifest(key, [
        {"point_id": "p1", "lat": 39.91, "lon": 116.40, "timestamp": 1_000.0, "terminal": "T"},
        {"point_id": "p2", "lat": 39.9101, "lon": 116.4001, "timestamp": 1_010.0, "terminal": "T"},
    ], segment_id="GAP-4")
    report = quality.start_quality_report(key)
    assert report["consumed_manifest_ids"] == [first["manifest_id"]]

    second = backfill.write_coverage_manifest(key, [
        {"point_id": "p3", "lat": 39.9102, "lon": 116.4002, "timestamp": 1_020.0, "terminal": "T"},
        {"point_id": "p4", "lat": 39.9103, "lon": 116.4003, "timestamp": 1_030.0, "terminal": "T"},
    ], segment_id="GAP-4")
    verification = quality.verify_manifest(second["manifest_id"], inspector="qc-two", passed=True)
    assert verification["qualified_point_ids"] == ["p3", "p4"]

    confirmed = quality.confirm_quality_report(report["report_id"], inspector="qc-one")
    assert confirmed["closed_gap_ids"] == ["p1", "p2"]
    assert confirmed["late_manifest_ids"] == [second["manifest_id"]]
    assert len(confirmed["recheck_required"]) == 2
    assert len(confirmed["registered_rechecks"]) == 1
    assert confirmed["registered_rechecks"][0]["qualified"] is True
    assert [m["manifest_id"] for m in confirmed["claimable_manifests"]] == [second["manifest_id"]]

    stored = quality.get_quality_report(report["report_id"])
    assert len(stored["confirmations"]) == 1
    assert stored["evidence"][0]["manifest_id"] == first["manifest_id"]


def test_interrupted_manifest_only_exposes_complete_points():
    from app.services.geo_backfill import GeoBackfillService

    key = "CONS-INTERRUPT"
    _batch(None, key)
    svc = GeoBackfillService()
    first = svc.write_coverage_manifest(key, [{"point_id": "ok-1"}, {"point_id": "ok-2"}])
    svc.claim_coverage_manifest(first["manifest_id"], inspector="qc-old")
    svc.record_confirmation([first["manifest_id"]], "QCF-OLD", "qc-old")

    try:
        svc.write_coverage_manifest(
            key, [{"point_id": "complete-1"}, {"point_id": "missing-tail"}],
            segment_id="BROKEN", fail_after=1,
        )
        raise AssertionError("simulated interruption must surface")
    except IOError:
        pass

    broken = [m for m in svc.list_coverage_manifests(key) if m.get("segment_id") == "BROKEN"][0]
    reconciled = svc.reconcile_manifest(broken["manifest_id"])
    assert reconciled["status"] == "interrupted"
    assert reconciled["claimable_point_ids"] == ["complete-1"]

    ids = {m["manifest_id"] for m in svc.list_coverage_manifests(key)}
    assert {first["manifest_id"], broken["manifest_id"]} <= ids
    assert svc.get_coverage_manifest(first["manifest_id"])["confirmed"] is True


def test_next_segment_is_exclusively_dispatched():
    from app.services.geo_backfill import GeoBackfillService

    key = "CONS-LOCK"
    _batch(None, key)
    svc = GeoBackfillService()
    svc.register_gap(key, "SEG-1", missing_count=1, sequence=1)
    outcomes = []

    def claim(team):
        outcomes.append(svc.claim_next_segment(key, team=team))

    threads = [threading.Thread(target=claim, args=(team,)) for team in ("crew-a", "crew-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [item for item in outcomes if item.get("claimed")]
    assert len(winners) == 1
    assert winners[0]["segment_id"] == "SEG-1"
    loser = next(item for item in outcomes if not item.get("claimed"))
    assert loser["reason"] in {"already_claimed", "no_claimable_segment"}
