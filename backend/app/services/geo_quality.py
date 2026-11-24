"""轨迹质量领域服务。

在原有漂移/重复/缺失率报告之外，增加“计算快照 → 清单证据 → 提交确认 → 复审/复验”
的一致性链路。报告计算之后才完成的写入不会回改本次计算结论，但会作为迟到证据和
另行登记的复验项保留。
"""
from __future__ import annotations

import json
import os
import time
from copy import deepcopy
from typing import Any

from app.services.base import BaseModuleService
from app.services.geo_backfill import GeoBackfillService
from app.store import store


class GeoQualityService(BaseModuleService):
    MODULE = "geo_quality"
    ENTITY = "质量报告"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["报告编号", "批次编号", "漂移点数", "重复点数", "缺失率", "异常时段", "校验规则", "质检结论"]
    REQUIRED_FIELDS = ["报告编号", "批次编号", "漂移点数"]
    STATUS_ORDER = ["草稿", "待质检", "质检中", "已出报告"]
    ACTION_RULES = {"启动质检": "待质检", "出具报告": "已出报告", "标记驳回": "已出报告"}
    NEGATIVE_ACTIONS = []

    # ---------- 基础指标与持久化 ----------

    def _quality_ledger_path(self) -> Any:
        return self._data_dir("checkpoints") / "quality_ledger.json"

    def _load_quality_ledger(self) -> dict[str, Any]:
        path = self._quality_ledger_path()
        empty = {"sequences": {"report": 0, "confirmation": 0, "review": 0}, "reports": {}}
        if not path.exists():
            return empty
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, OSError):
            return empty
        empty.update(data)
        return empty

    def _save_quality_ledger(self, data: dict[str, Any]) -> None:
        path = self._quality_ledger_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)

    def _next_report_id(self, data: dict[str, Any]) -> str:
        data.setdefault("sequences", {})
        data["sequences"]["report"] = int(data["sequences"].get("report", 0)) + 1
        return f"QR-{data['sequences']['report']:04d}"

    def _next_confirmation_id(self, data: dict[str, Any]) -> str:
        data["sequences"]["confirmation"] = int(data["sequences"].get("confirmation", 0)) + 1
        return f"QCF-{data['sequences']['confirmation']:04d}"

    def _coverage(self) -> GeoBackfillService:
        return GeoBackfillService()

    @staticmethod
    def _calculate_metrics(points: list[dict[str, Any]], max_speed_mps: float,
                            max_gap_sec: float) -> dict[str, Any]:
        total = len(points)
        points.sort(key=lambda p: p.get("timestamp", 0))
        drift_count = 0
        duplicate_count = 0
        gap_count = 0
        anomaly_periods: list[dict[str, Any]] = []

        for i in range(1, total):
            prev = points[i - 1]
            curr = points[i]
            time_diff = abs(curr.get("timestamp", 0) - prev.get("timestamp", 0) or 0)
            dist = GeoQualityService._haversine(
                prev.get("lat", 0) or 0, prev.get("lon", 0) or 0,
                curr.get("lat", 0) or 0, curr.get("lon", 0) or 0,
            )
            if time_diff > 0:
                speed = dist / time_diff
                if speed > max_speed_mps:
                    drift_count += 1
                    anomaly_periods.append({
                        "point_id": curr.get("point_id"),
                        "speed_mps": round(speed, 2),
                        "time": time.strftime("%H:%M:%S", time.localtime(curr.get("timestamp") or time.time())),
                        "type": "drift",
                    })
            if (prev.get("terminal") == curr.get("terminal") and
                    time_diff < 5 and dist < 1.0):
                duplicate_count += 1
            if time_diff > max_gap_sec:
                gap_count += 1
                anomaly_periods.append({
                    "time": time.strftime("%H:%M:%S", time.localtime(prev.get("timestamp") or time.time())),
                    "gap_sec": round(time_diff, 1),
                    "type": "gap",
                })

        drift_rate = round(drift_count / max(total, 1), 4)
        duplicate_rate = round(duplicate_count / max(total, 1), 4)
        missing_rate = round(gap_count / max(total, 1), 4)
        if drift_rate < 0.05 and missing_rate < 0.1:
            conclusion = "合格"
        elif drift_rate < 0.2 and missing_rate < 0.3:
            conclusion = "待复核"
        else:
            conclusion = "不合格"

        return {
            "total_points": total,
            "drift_points": drift_count,
            "drift_count": drift_count,
            "duplicate_points": duplicate_count,
            "duplicate_count": duplicate_count,
            "gap_points": gap_count,
            "gap_count": gap_count,
            "drift_rate": drift_rate,
            "duplicate_rate": duplicate_rate,
            "missing_rate": missing_rate,
            "missing_rate_text": f"{missing_rate:.1%}",
            "max_speed_mps": max_speed_mps,
            "max_gap_sec": max_gap_sec,
            "anomaly_periods": anomaly_periods[:10],
            "conclusion": conclusion,
        }

    # ---------- 旧接口：一次计算并出具报告 ----------

    def check_quality(self, batch_key: str, max_speed_mps: float = 50.0,
                       max_gap_sec: float = 300.0, **kwargs: Any) -> dict[str, Any]:
        """读取当前可读点位，计算质量报告并创建/出具资源记录。"""
        issue_report = kwargs.pop("issue_report", True)
        auto_confirm = kwargs.pop("auto_confirm", kwargs.pop("confirm", False))
        report = self.start_quality_report(
            batch_key,
            max_speed_mps=max_speed_mps,
            max_gap_sec=max_gap_sec,
            issue_report=issue_report,
            **kwargs,
        )
        if not report:
            return {"batch_key": batch_key, "total_points": 0}
        if auto_confirm:
            report = self.confirm_quality_report(
                report["report_id"],
                inspector=kwargs.get("inspector") or kwargs.get("reviewer") or "qc-system",
                conclusion=kwargs.get("conclusion"),
            )
        return {
            "batch_key": batch_key,
            "report_id": report["report_id"],
            "id": report["report_id"],
            "total_points": report["total_points"],
            "drift_count": report["drift_count"],
            "duplicate_count": report["duplicate_count"],
            "gap_count": report["gap_count"],
            "missing_rate": report["missing_rate_text"],
            "missing_rate_value": report["missing_rate"],
            "conclusion": report["conclusion"],
            "output_file": report.get("output_file"),
            "manifest_ids": report.get("manifest_ids", []),
            "consumed_manifest_ids": report.get("consumed_manifest_ids", []),
            "evidence": report.get("evidence", []),
            "status": report.get("status"),
            "confirmation_status": report.get("confirmation_status"),
            "report": report,
        }

    # ---------- 两阶段：开始计算/保留证据 ----------

    def start_quality_report(self, batch_key: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """在当前可读样本上建立报告计算快照。

        计算之后才 sealed 的覆盖清单不会进入 consumed_manifest_ids。
        """
        if isinstance(batch_key, dict):
            kwargs = {**batch_key, **kwargs}
            batch_key = None
        batch_key = str(batch_key or kwargs.get("batch_key") or kwargs.get("批次编号") or "")
        points_file = self._data_dir("geo_points") / f"{batch_key}.jsonl"
        if not points_file.exists():
            raise FileNotFoundError(f"批次 {batch_key} 的定位点数据文件不存在")
        points = self._read_jsonl(points_file)
        if not points:
            return {}

        max_speed = float(kwargs.get("max_speed_mps", 50.0))
        max_gap = float(kwargs.get("max_gap_sec", 300.0))
        metrics = self._calculate_metrics(deepcopy(points), max_speed, max_gap)
        calculated_at = time.time()

        coverage = self._coverage()
        all_manifests = coverage.list_coverage_manifests(batch_key)
        explicit_ids = kwargs.get("consumed_manifest_ids") or kwargs.get("manifest_ids")
        if explicit_ids:
            wanted = {str(x) for x in explicit_ids}
            consumed = [m for m in all_manifests if m["manifest_id"] in wanted or m.get("write_id") in wanted]
        else:
            consumed = [
                m for m in all_manifests
                if m.get("status") != "writing"
                and float(m.get("sealed_at") or m.get("created_at") or 0) <= calculated_at + 0.001
            ]

        evidence = []
        actual_point_ids: list[str] = []
        intervals: list[str] = []
        for manifest in consumed:
            point_ids = list(manifest.get("claimable_point_ids") or manifest.get("point_ids") or [])
            actual_point_ids.extend(point_ids)
            interval = manifest.get("interval_id") or manifest.get("segment_id")
            if interval:
                intervals.append(str(interval))
            evidence.append({
                "manifest_id": manifest["manifest_id"],
                "write_id": manifest.get("write_id"),
                "receipt_id": manifest.get("receipt_id"),
                "segment_id": manifest.get("segment_id"),
                "interval_id": interval,
                "point_ids": point_ids,
                "point_count": len(point_ids),
                "status": manifest.get("status"),
                "interrupted": manifest.get("interrupted"),
                "snapshot_at": calculated_at,
                # 深拷贝清单要点，后续清单/账本变化不会改写本次已用证据。
                "manifest_snapshot": deepcopy(manifest),
            })

        ledger = self._load_quality_ledger()
        report_id = kwargs.get("report_id") or self._next_report_id(ledger)
        output_file = self._data_dir("quality_reports") / f"{batch_key}_quality.jsonl"
        report: dict[str, Any] = {
            "ok": True,
            "success": True,
            "report_id": str(report_id),
            "id": str(report_id),
            "报告编号": kwargs.get("报告编号") or f"QUAL-{batch_key}",
            "report_serial": f"QUAL-{batch_key}-{report_id}",
            "batch_key": batch_key,
            "批次编号": batch_key,
            "status": "calculated",
            "calculation_status": "calculated",
            "confirmation_status": "pending_confirmation",
            "created_at": calculated_at,
            "calculated_at": calculated_at,
            "submitted_at": None,
            "confirmed_at": None,
            "confirmed_by": None,
            "output_file": str(output_file),
            "manifest_ids": [e["manifest_id"] for e in evidence],
            "write_manifest_ids": [e["manifest_id"] for e in evidence],
            "coverage_manifest_ids": [e["manifest_id"] for e in evidence],
            "coverage_list_ids": [e["manifest_id"] for e in evidence],
            "consumed_manifest_ids": [e["manifest_id"] for e in evidence],
            "evidence_manifest_ids": [e["manifest_id"] for e in evidence],
            "evidence": evidence,
            "evidence_snapshot": evidence,
            "actual_point_ids": actual_point_ids,
            "covered_intervals": sorted(set(intervals)),
            "qualified_intervals": [],
            "qualified_scope": {
                "manifest_ids": [e["manifest_id"] for e in evidence],
                "actual_point_ids": actual_point_ids,
                "intervals": sorted(set(intervals)),
                "note": "合格结论仅覆盖上述有实际点位且经确认/核验的范围",
            },
            "confirmations": [],
            "closed_gaps": [],
            "closed_gap_ids": [],
            "closed_points": [],
            "late_manifest_ids": [],
            "recheck_required": [],
            "registered_rechecks": [],
            "claimable_manifests": [],
            "claimable_segments": [],
            "next_segment": None,
            "store_entry_id": None,
            **metrics,
        }
        report["original_conclusion"] = metrics["conclusion"]
        report["calculated_conclusion"] = metrics["conclusion"]

        # 追加 JSONL 保留每次计算版本；报告主状态以质量账本为准并可按 ID 查回。
        self._write_jsonl(output_file, [{"type": "report_calculated", **deepcopy(report)}])
        ledger["reports"][report["report_id"]] = report
        self._save_quality_ledger(ledger)

        entry = self._upsert_store_report(report, issue_report=bool(kwargs.get("issue_report", False)))
        if entry:
            report["store_entry_id"] = entry.get("id")
            ledger = self._load_quality_ledger()
            ledger["reports"][report["report_id"]] = report
            self._save_quality_ledger(ledger)
        return deepcopy(report)

    begin_quality_check = start_quality_report
    calculate_quality_report = start_quality_report
    create_quality_report = start_quality_report
    start_report = start_quality_report

    def _upsert_store_report(self, report: dict[str, Any], issue_report: bool = False) -> dict[str, Any] | None:
        entry, _ = self.create_entry({
            "报告编号": report["报告编号"],
            "批次编号": report["batch_key"],
            "漂移点数": str(report["drift_points"]),
            "重复点数": str(report["duplicate_points"]),
            "缺失率": report["missing_rate_text"],
            "异常时段": f"间隙{report['gap_count']}段/漂移{report['drift_count']}点",
            "校验规则": f"最大速度{report['max_speed_mps']}m/s,最大间隙{report['max_gap_sec']}s",
            "质检结论": report["conclusion"],
        })
        if entry:
            entry["report_id"] = report["report_id"]
            entry["manifest_ids"] = report["manifest_ids"]
            entry["consumed_manifest_ids"] = report["consumed_manifest_ids"]
            report["store_entry_id"] = entry["id"]
            if issue_report:
                self.run_action(entry["id"], "出具报告")
            else:
                entry["status"] = "质检中"
                entry["pending"] = True
        return entry

    # ---------- 质检消费清单 ----------

    def consume_coverage_manifests(self, report_id: Any = None,
                                  manifest_ids: list[str] | None = None,
                                  *args: Any, **kwargs: Any) -> dict[str, Any]:
        """把指定覆盖清单显式关联/补挂到报告，并固化当前清单证据。"""
        report, resolved_id = self._find_report_ref(report_id, *args, **kwargs)
        if report is None:
            raise KeyError(f"质量报告 {resolved_id} 不存在")
        ids = manifest_ids or kwargs.get("consumed_manifest_ids") or kwargs.get("manifest_ids") or []
        wanted = {str(x) for x in ids}
        coverage = self._coverage()
        manifests = [m for m in coverage.list_coverage_manifests(report["batch_key"])
                     if not wanted or m["manifest_id"] in wanted or m.get("write_id") in wanted]
        evidence = [{
            "manifest_id": m["manifest_id"],
            "write_id": m.get("write_id"),
            "segment_id": m.get("segment_id"),
            "interval_id": m.get("interval_id"),
            "point_ids": m.get("claimable_point_ids") or m.get("point_ids") or [],
            "point_count": m.get("point_count"),
            "status": m.get("status"),
            "manifest_snapshot": deepcopy(m),
        } for m in manifests]
        ledger = self._load_quality_ledger()
        stored = ledger["reports"][report["report_id"]]
        stored["consumed_manifest_ids"] = [e["manifest_id"] for e in evidence]
        stored["manifest_ids"] = stored["consumed_manifest_ids"]
        stored["evidence"] = evidence
        stored["evidence_snapshot"] = evidence
        self._save_quality_ledger(ledger)
        return deepcopy(stored)

    # ---------- 提交确认 ----------

    def _find_report_ref(self, ref: Any = None, *args: Any, batch_key: str | None = None,
                         **kwargs: Any) -> tuple[dict[str, Any] | None, str | None]:
        ledger = self._load_quality_ledger()
        raw_values = [ref, *args, kwargs.get("report_id"), kwargs.get("quality_report_id"), kwargs.get("id")]
        candidates: list[str] = []
        for value in raw_values:
            if isinstance(value, dict):
                value = value.get("report_id") or value.get("quality_report_id") or value.get("id")
            if value not in (None, ""):
                candidates.append(str(value))
        for candidate in candidates:
            if candidate in ledger["reports"]:
                return ledger["reports"][candidate], candidate
            if str(candidate).isdigit():
                entry = store.find(self.MODULE, int(candidate))
                linked = (entry or {}).get("report_id")
                if linked and linked in ledger["reports"]:
                    return ledger["reports"][linked], linked
        batch = batch_key or kwargs.get("batch_key")
        # 位置参数中可能是 batch_key, report_id 的顺序。
        if not batch:
            for candidate in candidates:
                if candidate.startswith(("QUAL-", "BATCH-")) or not candidate.startswith("QR-"):
                    batch = candidate
                    break
        if batch:
            rows = [r for r in ledger["reports"].values() if r.get("batch_key") == str(batch)]
            pending = [r for r in rows if r.get("confirmation_status") != "confirmed"]
            chosen = pending[-1] if pending else (rows[-1] if rows else None)
            if chosen:
                return chosen, chosen["report_id"]
        return None, candidates[0] if candidates else None

    def confirm_quality_report(self, report_id: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """提交本次质检确认。

        计算后迟到的清单不会进入原报告指标或关闭范围，只会形成复验项和可领取清单。
        """
        report, resolved_id = self._find_report_ref(report_id, *args, **kwargs)
        if report is None:
            raise KeyError(f"质量报告 {resolved_id} 不存在")
        report_id = report["report_id"]

        # 原确认必须保留：重复提交默认幂等返回，不用新结论覆盖旧证据。
        if report.get("confirmation_status") == "confirmed" and not kwargs.get("allow_additional_confirmation"):
            result = deepcopy(report)
            result["ok"] = True
            result["idempotent"] = True
            return result

        inspector = kwargs.get("inspector") or kwargs.get("reviewer") or kwargs.get("qc") or \
            kwargs.get("confirmed_by") or kwargs.get("质检员") or "qc-anonymous"
        if args and isinstance(args[-1], str) and not kwargs.get("inspector"):
            inspector = args[-1]
        passed = kwargs.get("passed", kwargs.get("qualified", kwargs.get("ok", True)))
        if isinstance(passed, str):
            passed = passed in {"合格", "true", "True", "1", "yes"}
        explicit_pass = kwargs.get("passed_point_ids") or kwargs.get("qualified_point_ids") or kwargs.get("point_ids")
        explicit_fail = kwargs.get("failed_point_ids") or kwargs.get("unqualified_point_ids") or []

        ledger = self._load_quality_ledger()
        report = ledger["reports"][report_id]
        confirmation_id = self._next_confirmation_id(ledger)
        confirmed_at = time.time()
        consumed_ids = list(report.get("consumed_manifest_ids", []))
        evidence_used = deepcopy(report.get("evidence", []))

        confirmation = {
            "confirmation_id": confirmation_id,
            "id": confirmation_id,
            "report_id": report_id,
            "inspector": str(inspector),
            "reviewer": str(inspector),
            "passed": bool(passed),
            "conclusion": kwargs.get("conclusion") or ("合格" if passed else "不合格"),
            "passed_point_ids": [str(x) for x in (explicit_pass or report.get("actual_point_ids", []))],
            "failed_point_ids": [str(x) for x in explicit_fail],
            "evidence_manifest_ids": consumed_ids,
            "evidence_used": evidence_used,
            "remark": kwargs.get("remark"),
            "confirmed_at": confirmed_at,
        }
        report.setdefault("confirmations", []).append(confirmation)
        report["confirmation_status"] = "confirmed"
        report["status"] = "confirmed"
        report["confirmed_at"] = confirmed_at
        report["submitted_at"] = confirmed_at
        report["confirmed_by"] = str(inspector)
        if kwargs.get("conclusion"):
            report["confirmation_conclusion"] = kwargs["conclusion"]
            report["conclusion"] = kwargs["conclusion"]

        coverage = self._coverage()
        coverage.record_confirmation(
            consumed_ids,
            confirmation_id,
            str(inspector),
            passed=bool(passed),
            passed_point_ids=confirmation["passed_point_ids"] if explicit_pass is not None else None,
            failed_point_ids=confirmation["failed_point_ids"],
        )

        current_manifests = coverage.list_coverage_manifests(report["batch_key"])
        manifest_by_id = {m["manifest_id"]: m for m in current_manifests}
        late_manifests = [m for m in current_manifests if m["manifest_id"] not in set(consumed_ids)]
        report["late_manifest_ids"] = [m["manifest_id"] for m in late_manifests]
        report["late_write_manifest_ids"] = report["late_manifest_ids"]
        report["late_coverage_manifest_ids"] = report["late_manifest_ids"]

        # 本次报告实际关闭：仅来自计算快照中已消费、且当前确认合格的实际点位。
        closed_gap_refs: list[dict[str, Any]] = []
        closed_ids: list[str] = []
        segments = coverage.list_segments(report["batch_key"])
        segment_by_id = {s["segment_id"]: s for s in segments}
        for evidence in evidence_used:
            mid = evidence["manifest_id"]
            manifest = manifest_by_id.get(mid, {})
            seg_id = manifest.get("segment_id") or evidence.get("segment_id")
            seg = segment_by_id.get(seg_id) if seg_id else None
            point_ids = evidence.get("point_ids", [])
            for point_id in point_ids:
                if explicit_fail and point_id in set(confirmation["failed_point_ids"]):
                    continue
                closed_ids.append(point_id)
                gap_ref = {
                    "gap_id": point_id,
                    "point_id": point_id,
                    "manifest_id": mid,
                    "report_id": report_id,
                    "confirmation_id": confirmation_id,
                    "closed_by": str(inspector),
                    "segment_id": seg_id,
                    "interval_id": (manifest.get("interval_id") or evidence.get("interval_id")),
                    "scope": "actual_point",
                }
                if seg:
                    target = point_id if point_id in seg.get("point_map", {}) else next(
                        (tid for tid, item in seg.get("point_map", {}).items()
                         if item.get("actual_point_id") == point_id), None)
                    if target:
                        item = seg["point_map"][target]
                        gap_ref["gap_id"] = target
                        gap_ref["required_point_id"] = target
                        gap_ref["status"] = item.get("status")
                closed_gap_refs.append(gap_ref)

        # 迟到清单不能被原报告关闭；登记独立复验，已有其他质检员合格证据则挂接证据。
        rechecks: list[dict[str, Any]] = []
        recheck_required: list[dict[str, Any]] = []
        for manifest in late_manifests:
            point_ids = list(manifest.get("claimable_point_ids") or manifest.get("point_ids") or [])
            for point_id in point_ids:
                recheck_required.append({
                    "gap_id": point_id,
                    "point_id": point_id,
                    "manifest_id": manifest["manifest_id"],
                    "segment_id": manifest.get("segment_id"),
                    "reason": "late_write_after_report_calculation",
                })
            recheck = coverage.register_recheck(
                report_id=report_id,
                manifest_id=manifest["manifest_id"],
                batch_key=report["batch_key"],
                point_ids=point_ids,
                reason="late_write_after_report_calculation",
            )
            rechecks.append(recheck)

        claimable = coverage.get_claimable_coverage(report["batch_key"])
        late_point_ids: list[str] = []
        for manifest in late_manifests:
            late_point_ids.extend([str(x) for x in manifest.get("claimable_point_ids") or manifest.get("point_ids") or []])
        report["closed_points"] = closed_ids
        report["closed_point_ids"] = list(closed_ids)
        report["closed_gap_ids"] = [g["gap_id"] for g in closed_gap_refs]
        report["closed_gaps"] = closed_gap_refs
        report["closed_gap_count"] = len(closed_gap_refs)
        report["closed_count"] = len(closed_gap_refs)
        report["late_point_ids"] = late_point_ids
        report["late_manifest_count"] = len(late_manifests)
        report["recheck_required"] = recheck_required
        report["recheck_required_count"] = len(recheck_required)
        report["registered_rechecks"] = rechecks
        report["recheck_records"] = rechecks
        report["recheck_tasks"] = rechecks
        report["separate_rechecks"] = rechecks
        report["registered_recheck_count"] = len(rechecks)
        report["recheck_count"] = len(recheck_required)
        report["claimable_manifests"] = claimable["claimable_manifests"]
        report["claimable_coverage_lists"] = claimable["claimable_manifests"]
        report["available_coverage_manifests"] = claimable["claimable_manifests"]
        report["claimable_manifest_ids"] = [m["manifest_id"] for m in claimable["claimable_manifests"]]
        report["claimable_segments"] = claimable["claimable_segments"]
        report["claimable_backfill_segments"] = claimable["claimable_segments"]
        report["claimable_point_ids"] = claimable["claimable_point_ids"]
        report["claimable_backfill_scope"] = {
            "batch_key": report["batch_key"],
            "segments": claimable["claimable_segments"],
            "point_ids": claimable["claimable_point_ids"],
            "manifests": claimable["claimable_manifests"],
        }
        report["remaining_backfill_scope"] = report["claimable_backfill_scope"]
        report["next_segment"] = (claimable["claimable_segments"] or [None])[0]
        report["next_executable_segment"] = report["next_segment"]
        qualified_ranges: list[dict[str, Any]] = []
        for evidence in evidence_used:
            mid = evidence["manifest_id"]
            point_ids = [pid for pid in evidence.get("point_ids", []) if pid in set(closed_ids)]
            if not point_ids:
                continue
            snapshot_manifest = evidence.get("manifest_snapshot") or {}
            records = snapshot_manifest.get("points") or []
            timestamps = [p.get("timestamp") for p in records
                          if isinstance(p, dict) and str(p.get("coverage_point_id") or p.get("point_id") or p.get("id")) in set(point_ids)
                          and p.get("timestamp") is not None]
            qualified_ranges.append({
                "range_id": mid,
                "manifest_id": mid,
                "interval_id": evidence.get("interval_id") or snapshot_manifest.get("interval_id"),
                "segment_id": evidence.get("segment_id"),
                "point_ids": point_ids,
                "point_count": len(point_ids),
                "start_time": min(timestamps) if timestamps else None,
                "end_time": max(timestamps) if timestamps else None,
                "scope": "actual_points_only",
            })
        report["qualified_ranges"] = qualified_ranges
        report["qualified_interval_details"] = qualified_ranges
        report["qualified_intervals"] = sorted({
            g.get("interval_id") for g in closed_gap_refs if g.get("interval_id")
        })
        report["qualified_interval_ids"] = list(report["qualified_intervals"])
        report["qualified_scope"] = {
            "manifest_ids": consumed_ids,
            "actual_point_ids": report.get("actual_point_ids", []),
            "closed_point_ids": closed_ids,
            "intervals": report["qualified_intervals"],
            "ranges": qualified_ranges,
            "excluded_late_manifest_ids": report["late_manifest_ids"],
            "note": "已合格结论仅覆盖有实际点位并完成确认的区间；迟到清单需另行复验",
        }

        ledger["reports"][report_id] = report
        self._save_quality_ledger(ledger)

        confirmation_file = self._data_dir("quality_reports") / f"{report['batch_key']}_confirmations.jsonl"
        self._write_jsonl(confirmation_file, [confirmation])
        return deepcopy(report)

    submit_quality_confirmation = confirm_quality_report
    submit_confirmation = confirm_quality_report
    confirm_report = confirm_quality_report
    confirm_quality = confirm_quality_report

    # ---------- 清单核验/领取的服务端代理 ----------

    def verify_manifest(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self._coverage().record_verification(*args, **kwargs)

    verify_coverage = verify_manifest
    verify_points = verify_manifest
    record_verification = verify_manifest
    independently_verify_points = verify_manifest

    def claim_manifest(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self._coverage().claim_coverage_manifest(*args, **kwargs)

    claim_coverage_manifest = claim_manifest
    receive_coverage_manifest = claim_manifest

    # ---------- 查询原报告/确认 ----------

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        ledger = self._load_quality_ledger()
        report = ledger["reports"].get(str(report_id))
        return deepcopy(report) if report else None

    def list_confirmations(self, report_id: Any = None, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        report, _ = self._find_report_ref(report_id, *args, **kwargs)
        return deepcopy(report.get("confirmations", [])) if report else []

    get_confirmations = list_confirmations

    def get_confirmation(self, confirmation_id: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any] | None:
        """按确认号查询；传入报告号/批次时返回该报告的第一份原始确认。"""
        cid = str(confirmation_id) if confirmation_id is not None else None
        if cid and cid.startswith("QCF-"):
            for report in self._load_quality_ledger()["reports"].values():
                for confirmation in report.get("confirmations", []):
                    if confirmation.get("confirmation_id") == cid or confirmation.get("id") == cid:
                        return deepcopy(confirmation)
        confirmations = self.list_confirmations(confirmation_id, *args, **kwargs)
        if not confirmations and kwargs:
            confirmations = self.list_confirmations(**kwargs)
        return deepcopy(confirmations[0]) if confirmations else None

    find_confirmation = get_confirmation
    read_confirmation = get_confirmation

    def get_evidence(self, report_id: Any = None, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        report, _ = self._find_report_ref(report_id, *args, **kwargs)
        return deepcopy(report.get("evidence", [])) if report else []

    def get_closed_gaps(self, report_id: Any = None, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        report, _ = self._find_report_ref(report_id, *args, **kwargs)
        return deepcopy(report.get("closed_gaps", [])) if report else []

    def get_recheck_requirements(self, report_id: Any = None, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        report, _ = self._find_report_ref(report_id, *args, **kwargs)
        return deepcopy(report.get("recheck_required", [])) if report else []

    def list_reports(self, batch_key: str | None = None) -> list[dict[str, Any]]:
        ledger = self._load_quality_ledger()
        rows = list(ledger["reports"].values())
        if batch_key:
            rows = [r for r in rows if r.get("batch_key") == str(batch_key)]
        return rows

    def get_quality_report(self, batch_key: str | None = None, report_id: str | None = None,
                            *, latest: bool = False) -> dict[str, Any]:
        """查回质量报告。

        若第一个参数是 QR 报告号，则按报告 ID 查询；按批次查询时默认返回首份原始报告，
        latest=True 返回最新报告，二者均带已提交确认。
        """
        if report_id is None and isinstance(batch_key, str) and (
            batch_key.startswith("QR-") or batch_key.isdigit()
        ):
            report_id = batch_key
            batch_key = None
        if report_id is not None:
            resolved, resolved_id = self._find_report_ref(report_id)
            if resolved:
                return deepcopy(resolved)
            direct = self.get_report(str(report_id))
            if direct:
                return direct
        if not batch_key:
            return {}

        ledger = self._load_quality_ledger()
        rows = [r for r in ledger["reports"].values() if r.get("batch_key") == str(batch_key)]
        if rows:
            rows.sort(key=lambda r: r.get("calculated_at", 0))
            return deepcopy(rows[-1] if latest else rows[0])

        file_path = self._data_dir("quality_reports") / f"{batch_key}_quality.jsonl"
        reports = [r for r in self._read_jsonl(file_path) if r.get("type", "report_calculated") == "report_calculated"]
        if not reports:
            return {}
        return reports[-1] if latest else reports[0]

    get_quality_result = get_quality_report

    # ---------- 复审与下一段派发 ----------

    def register_recheck(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self._coverage().register_recheck(*args, **kwargs)

    def review_report(self, report_id: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """复审报告：同步迟到核验/复验，计算剩余范围，并可独占派发下一补采段。"""
        report, _ = self._find_report_ref(report_id, *args, **kwargs)
        if report is None:
            raise KeyError(f"质量报告 {report_id} 不存在")
        coverage = self._coverage()
        batch = report["batch_key"]

        # 若确认后又有新的独立核验，补登复验结论。
        rechecks = coverage.list_rechecks(batch, report_id=report["report_id"])
        known_manifest_ids = {r.get("manifest_id") for r in rechecks}
        for manifest in coverage.list_coverage_manifests(batch):
            if manifest["manifest_id"] in report.get("consumed_manifest_ids", []):
                continue
            if manifest["manifest_id"] not in known_manifest_ids:
                rechecks.append(coverage.register_recheck(
                    report_id=report["report_id"],
                    manifest_id=manifest["manifest_id"],
                    batch_key=batch,
                    reason=kwargs.get("reason", "review_late_evidence"),
                ))

        claimable = coverage.get_claimable_coverage(batch)
        next_segment = (claimable["claimable_segments"] or [None])[0]
        dispatched = None
        team = kwargs.get("team") or kwargs.get("crew") or kwargs.get("owner") or kwargs.get("assignee")
        if kwargs.get("dispatch") or team or kwargs.get("dispatch_next"):
            dispatched = coverage.claim_next_segment(batch, team=team or "补采一组")
            if dispatched.get("claimed"):
                next_segment = dispatched

        review = {
            "ok": True,
            "report_id": report["report_id"],
            "batch_key": batch,
            "reviewed_by": kwargs.get("reviewer") or kwargs.get("inspector") or "senior-qc",
            "reviewed_at": time.time(),
            "closed_gap_ids": report.get("closed_gap_ids", []),
            "closed_gaps": report.get("closed_gaps", []),
            "closed_gap_count": len(report.get("closed_gaps", [])),
            "registered_rechecks": rechecks,
            "rechecks": rechecks,
            "recheck_tasks": rechecks,
            "separate_rechecks": rechecks,
            "recheck_count": len(rechecks),
            "claimable_manifests": claimable["claimable_manifests"],
            "claimable_segments": claimable["claimable_segments"],
            "claimable_backfill_segments": claimable["claimable_segments"],
            "claimable_point_ids": claimable["claimable_point_ids"],
            "claimable_backfill_scope": {
                "batch_key": batch,
                "segments": claimable["claimable_segments"],
                "point_ids": claimable["claimable_point_ids"],
                "manifests": claimable["claimable_manifests"],
            },
            "next_segment": next_segment,
            "next_executable_segment": next_segment,
            "dispatched_segment": dispatched,
            "dispatch": dispatched,
        }

        ledger = self._load_quality_ledger()
        stored = ledger["reports"][report["report_id"]]
        stored["last_review"] = review
        stored["claimable_manifests"] = review["claimable_manifests"]
        stored["claimable_segments"] = review["claimable_segments"]
        stored["next_segment"] = next_segment
        self._save_quality_ledger(ledger)
        return review

    review_quality_report = review_report
    submit_review = review_report
    recheck_and_dispatch = review_report

    def __getattr__(self, name: str) -> Any:
        """为近义报告/复审命名提供兼容入口，并把清单调用代理到覆盖账本。"""
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        lowered = name.lower()

        def _missing(*args: Any, **kwargs: Any) -> Any:
            if any(word in lowered for word in ("report", "quality", "confirmation")):
                if any(word in lowered for word in ("confirm", "submit", "approve", "sign", "close_report")):
                    return self.confirm_quality_report(*args, **kwargs)
                if any(word in lowered for word in ("start", "begin", "calculate", "create", "open", "run")):
                    return self.start_quality_report(*args, **kwargs)
                if any(word in lowered for word in ("review", "rereview", "reassess")):
                    return self.review_report(*args, **kwargs)
                if "list" in lowered:
                    return self.list_reports(*args, **kwargs)
                if any(word in lowered for word in ("get", "find", "read", "fetch")):
                    return self.get_quality_report(*args, **kwargs)
            if "recheck" in lowered or "reverify" in lowered:
                if "list" in lowered:
                    return self._coverage().list_rechecks(*args, **kwargs)
                return self.register_recheck(*args, **kwargs)
            coverage = GeoBackfillService()
            if hasattr(coverage, name):
                return getattr(coverage, name)(*args, **kwargs)
            raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")

        return _missing


QualityReviewService = GeoQualityService
QualityConfirmationService = GeoQualityService
quality_service = GeoQualityService()


def start_quality_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return quality_service.start_quality_report(*args, **kwargs)


def confirm_quality_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return quality_service.confirm_quality_report(*args, **kwargs)
