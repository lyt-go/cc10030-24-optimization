"""业务路由汇总：一致性操作路由先注册，随后是通用资源路由。"""
from __future__ import annotations

from app.routers import coverage_ops, resource

ROUTERS = [coverage_ops, resource]
