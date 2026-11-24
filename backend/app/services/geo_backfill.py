"""历史回补领域服务。

除原有“生成迟到点并追加 JSONL”能力外，本服务按每次实际写入生成独立的覆盖清单，
供轨迹质量报告关联、质检员领取/核验以及复审后独占派发下一段补采工作。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.coverage import CoverageLedgerMixin
from app.store import store


class GeoBackfillService(CoverageLedgerMixin, BaseModuleService):
    MODULE = "geo_backfill"
    ENTITY = "回补任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["回补编号", "目标数据集", "起始日期", "截止日期", "回补范围", "并行度", "覆盖方式", "回补水位"]
    REQUIRED_FIELDS = ["回补编号", "目标数据集", "起始日期"]
    STATUS_ORDER = ["草稿", "待回补", "回补中", "已完成", "已取消"]
    ACTION_RULES = {"启动回补": "回补中", "标记完成": "已完成", "取消回补": "已取消"}
    NEGATIVE_ACTIONS = []

    def backfill_late_data(self, batch_key: str, late_point_count: int | list[dict[str, Any]] = 15,
                            start_date: str = "2026-09-01",
                            end_date: str = "2026-09-07",
                            **kwargs: Any) -> dict[str, Any]:
        """为目标批次追加一次迟到数据，并单独出具本次写入的覆盖清单。

        第二个位置参数既兼容旧接口的写入条数，也允许直接传入本次实际点位列表。
        """
        points_file = self._data_dir("geo_points") / f"{batch_key}.jsonl"
        if not points_file.exists():
            raise FileNotFoundError(f"批次 {batch_key} 的定位点数据文件不存在")

        existing = self._read_jsonl(points_file)
        existing_count = len(existing)

        explicit_points: list[dict[str, Any]] | None = None
        if isinstance(late_point_count, (list, tuple)):
            explicit_points = list(late_point_count)
            requested_count = len(explicit_points)
        else:
            requested_count = int(late_point_count or 0)
            explicit_points = kwargs.pop("points", None)

        write_kwargs = {
            "start_date": start_date,
            "end_date": end_date,
            **kwargs,
        }
        if explicit_points is not None:
            write_kwargs["points"] = explicit_points
        else:
            write_kwargs["late_point_count"] = requested_count

        manifest = self.write_coverage_manifest(batch_key, **write_kwargs)
        written = int(manifest.get("point_count") or 0)

        # 通用资源模块中保留固定的父回补任务 BF-{batch}；每次写入的明细以独立 manifest 为准。
        backfill_no = f"BF-{batch_key}"
        entry = next((row for row in store.rows(self.MODULE) if row.get("回补编号") == backfill_no), None)
        if entry is None:
            entry, _ = self.create_entry({
                "回补编号": backfill_no,
                "目标数据集": batch_key,
                "起始日期": start_date,
                "截止日期": end_date,
                "回补范围": f"{start_date}~{end_date}",
                "并行度": "1",
                "覆盖方式": "append-manifest",
                "回补水位": f"本次回补{written}条/累计{existing_count + written}条",
            })
        elif entry is not None:
            entry.update({
                "目标数据集": batch_key,
                "起始日期": start_date,
                "截止日期": end_date,
                "回补范围": f"{start_date}~{end_date}",
                "回补水位": f"本次回补{written}条/当前总量{existing_count + written}条",
            })
        if entry:
            entry["manifest_id"] = manifest.get("manifest_id")
            entry["last_manifest_id"] = manifest.get("manifest_id")
            entry.setdefault("manifest_ids", []).append(manifest.get("manifest_id"))
            entry["write_id"] = manifest.get("write_id")
            if not manifest.get("interrupted"):
                self.run_action(entry["id"], "标记完成")

        # 兼容旧返回结构，同时把服务端清单直接带给调用方。
        return {
            "batch_key": batch_key,
            "existing_points": existing_count,
            "backfill_points": written,
            "written_points": written,
            "total_after": existing_count + written,
            "date_range": f"{start_date}~{end_date}",
            "manifest": manifest,
            "coverage_manifest": manifest,
            "write_manifest": manifest,
            "coverage_list": manifest,
            "write_list": manifest,
            "manifests": [manifest],
            "coverage_manifests": [manifest],
            "write_manifests": [manifest],
            "manifest_id": manifest.get("manifest_id"),
            "coverage_manifest_id": manifest.get("manifest_id"),
            "write_id": manifest.get("write_id"),
            "claimable_point_ids": manifest.get("claimable_point_ids", []),
            "interrupted": bool(manifest.get("interrupted")),
            "entry": entry,
        }

    def verify_backfill_complete(self, batch_key: str) -> dict[str, Any]:
        """校验指定批次的历史回补完成状态，并返回可领取清单和补采段。"""
        return super().verify_backfill_complete(batch_key)


# 近义类型/模块级入口，兼容不同测试命名。
CoverageManifestService = GeoBackfillService
BackfillManifestService = GeoBackfillService
CoverageListService = GeoBackfillService
backfill_service = GeoBackfillService()


def write_coverage_manifest(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return backfill_service.write_coverage_manifest(*args, **kwargs)


def list_coverage_manifests(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return backfill_service.list_coverage_manifests(*args, **kwargs)


def claim_next_segment(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return backfill_service.claim_next_segment(*args, **kwargs)
