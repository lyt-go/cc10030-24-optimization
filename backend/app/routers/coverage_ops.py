"""分次写入覆盖清单与质量确认的辅助 HTTP 接口。

通用 CRUD 路由仍由 resource.py 提供；这里只暴露跨模块一致性动作。请求体使用宽松
字典，以便兼容不同调用方对 team/crew、inspector/reviewer 等字段命名。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Query

from app.services import service_for

router = APIRouter(prefix="/api", tags=["覆盖清单与质检一致性"])


def backfill_service():
    return service_for("geo_backfill")


def quality_service():
    return service_for("geo_quality")


# ---------- 覆盖清单 ----------

@router.post("/geo_backfill/manifests")
@router.post("/geo_backfill/coverage-manifests")
@router.post("/geo_backfill/coverage/manifests")
@router.post("/geo_backfill/write-manifests")
def write_manifest(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return backfill_service().write_coverage_manifest(**payload)


@router.get("/geo_backfill/manifests")
@router.get("/geo_backfill/coverage-manifests")
@router.get("/geo_backfill/coverage/manifests")
def list_manifests(batch_key: str | None = Query(default=None),
                   claimable_only: bool = Query(default=False)) -> Any:
    return backfill_service().list_coverage_manifests(
        batch_key, claimable_only=claimable_only)


@router.get("/geo_backfill/manifests/{manifest_id}")
@router.get("/geo_backfill/coverage-manifests/{manifest_id}")
@router.get("/geo_backfill/coverage/manifests/{manifest_id}")
def get_manifest(manifest_id: str) -> dict[str, Any]:
    result = backfill_service().get_coverage_manifest(manifest_id)
    return result or {"ok": False, "error": "manifest not found"}


@router.post("/geo_backfill/manifests/{manifest_id}/claim")
@router.post("/geo_backfill/coverage/manifests/{manifest_id}/claim")
def claim_manifest(manifest_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    payload.setdefault("manifest_id", manifest_id)
    return backfill_service().claim_coverage_manifest(**payload)


@router.post("/geo_backfill/manifests/{manifest_id}/verify")
@router.post("/geo_backfill/coverage/manifests/{manifest_id}/verify")
def verify_manifest(manifest_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    payload.setdefault("manifest_id", manifest_id)
    return backfill_service().record_verification(**payload)


@router.post("/geo_backfill/receipts")
@router.post("/geo_backfill/late-receipts")
def record_receipt(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return backfill_service().record_receipt(**payload)


@router.get("/geo_backfill/claimable-scope")
@router.get("/geo_backfill/coverage/claimable")
def claimable_scope(batch_key: str | None = Query(default=None)) -> dict[str, Any]:
    return backfill_service().get_claimable_backfill_scope(batch_key)


# ---------- 补采段独占派发 ----------

@router.post("/geo_backfill/segments")
@router.post("/geo_backfill/gaps")
def register_segment(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return backfill_service().register_segment(**payload)


@router.get("/geo_backfill/segments")
def list_segments(batch_key: str | None = Query(default=None),
                  claimable_only: bool = Query(default=False)) -> list[dict[str, Any]]:
    return backfill_service().list_segments(batch_key, claimable_only=claimable_only)


@router.get("/geo_backfill/segments/next")
def next_segment(batch_key: str | None = Query(default=None),
                 team: str | None = Query(default=None)) -> dict[str, Any] | None:
    return backfill_service().get_next_segment(batch_key, team=team)


@router.post("/geo_backfill/segments/next/claim")
@router.post("/geo_backfill/dispatch-next")
def claim_next_segment(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return backfill_service().claim_next_segment(**payload)


@router.post("/geo_backfill/segments/{segment_id}/claim")
def claim_segment(segment_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    payload.setdefault("segment_id", segment_id)
    return backfill_service().claim_segment(**payload)


# ---------- 质量报告计算/确认/复审 ----------

@router.post("/geo_quality/reports/start")
@router.post("/geo_quality/reports/calculate")
@router.post("/geo_quality/start-report")
def start_report(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return quality_service().start_quality_report(**payload)


@router.post("/geo_quality/{report_id}/confirm")
@router.post("/geo_quality/reports/{report_id}/confirm")
def confirm_report(report_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return quality_service().confirm_quality_report(report_id, **payload)


@router.get("/geo_quality/reports/{report_id}")
def get_report(report_id: str) -> dict[str, Any]:
    result = quality_service().get_quality_report(report_id)
    return result or {"ok": False, "error": "report not found"}


@router.get("/geo_quality/reports")
def list_reports(batch_key: str | None = Query(default=None)) -> list[dict[str, Any]]:
    return quality_service().list_reports(batch_key)


@router.post("/geo_quality/reports/{report_id}/review")
@router.post("/geo_quality/reports/{report_id}/recheck")
def review_report(report_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return quality_service().review_report(report_id, **payload)
