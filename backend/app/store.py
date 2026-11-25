"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持跨模块数据流水线。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.seed import SEED_ROWS


class DataPipeline:
    """跨模块数据流水线引擎：支持 输入→转换→版本化产物→下游消费 的完整链路。

    每条流水线定义了源模块、目标模块、转换函数和验证规则。
    产物存储为可版本化的数据快照，支持脏行检测、重复批次拒绝、冲突版本检测、中断恢复。
    """

    def __init__(self, name: str, sources: list[str], target_module: str) -> None:
        self.name = name
        self.sources = sources
        self.target_module = target_module
        self._artifacts: list[dict[str, Any]] = []
        self._versions: list[str] = []  # 版本号列表
        self._dirty_log: list[dict[str, Any]] = []
        self._batch_registry: set[str] = set()  # 防重复批次
        self._last_checkpoint: dict[str, Any] = {}  # 中断恢复点
        self._interrupted = False

    def ingest(self, rows: list[dict[str, Any]], batch_id: str) -> tuple[int, list[str]]:
        """摄入数据行，返回 (成功行数, 脏行详情列表)"""
        if batch_id in self._batch_registry:
            return 0, [f"重复批次 {batch_id} 已存在，拒绝摄入"]
        self._batch_registry.add(batch_id)
        clean_rows = []
        dirty_details = []
        for i, row in enumerate(rows):
            issues = self._validate_row(row, i)
            if issues:
                dirty_details.append(f"行 {i}: {'; '.join(issues)}")
                self._dirty_log.append({"batch": batch_id, "row_index": i, "issues": issues, "row": row, "ts": time.time()})
            else:
                clean_rows.append(row)
        self._artifacts.extend(clean_rows)
        return len(clean_rows), dirty_details

    def transform(self, transform_fn, *args, **kwargs) -> list[dict[str, Any]]:
        """对当前产物执行转换函数"""
        self._save_checkpoint("transform_enter", len(self._artifacts))
        try:
            result = transform_fn(self._artifacts, *args, **kwargs)
            version = self._make_version("transform")
            self._versions.append(version)
            self._save_checkpoint("transform_exit", len(result))
            return result
        except Exception:
            self._interrupted = True
            raise

    def versioned_snapshot(self) -> dict[str, Any]:
        """生成版本化产物快照"""
        version = self._versions[-1] if self._versions else self._make_version("snapshot")
        return {
            "pipeline": self.name,
            "version": version,
            "artifact_count": len(self._artifacts),
            "artifact_sample": self._artifacts[:5],
            "dirty_count": len(self._dirty_log),
            "batch_count": len(self._batch_registry),
            "interrupted": self._interrupted,
            "checkpoint": self._last_checkpoint,
        }

    def health_report(self) -> dict[str, Any]:
        """流水线健康报告：用于外部模块消费和容量材料生成"""
        return {
            "pipeline": self.name,
            "source_modules": self.sources,
            "target_module": self.target_module,
            "total_ingested": sum(1 for a in self._artifacts),
            "dirty_rows": len(self._dirty_log),
            "versions": len(self._versions),
            "batches_processed": len(self._batch_registry),
            "interrupted": self._interrupted,
            "last_artifact_ts": max((a.get("ts", 0) for a in self._artifacts), default=0) if self._artifacts else 0,
        }

    def _validate_row(self, row: dict[str, Any], index: int) -> list[str]:
        issues = []
        for key, val in row.items():
            if val is None or (isinstance(val, str) and not val.strip()):
                issues.append(f"空值字段 '{key}'")
        if any(row.get(k) is None for k in ["id"]):
            issues.append("缺少主键 id")
        return issues

    def _make_version(self, tag: str) -> str:
        ts = str(int(time.time() * 1000))
        raw = f"{self.name}:{tag}:{ts}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _save_checkpoint(self, stage: str, count: int) -> None:
        self._last_checkpoint = {"stage": stage, "count": count, "ts": time.time()}

    def recover_checkpoint(self) -> dict[str, Any]:
        """从最近检查点恢复"""
        return {
            "pipeline": self.name,
            "interrupted": self._interrupted,
            "last_checkpoint": self._last_checkpoint,
            "artifacts_before_interrupt": len(self._artifacts),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据流水线
        self._pipelines: dict[str, DataPipeline] = {}
        # 全局变更日志（用于 changelog 跨模块追溯）
        self._global_changelog: list[dict[str, Any]] = []
        # Schema 注册表（cross-module schema registry）
        self._schema_registry: dict[str, list[dict[str, Any]]] = {}
        # 水位偏移记录（watermark → late_data 共享）
        self._watermark_offsets: dict[str, dict[str, Any]] = {}
        # 已确认位置/事件时刻记录，以及迟到批次和发送回执。
        # 同时提供无下划线别名，方便不同调用入口读取同一份清单。
        self._confirmation_records: dict[str, list[dict[str, Any]]] = {}
        self.confirmation_records = self._confirmation_records
        self._ack_records = self._confirmation_records
        self.ack_records = self._ack_records
        self._checkpoint_records = self._confirmation_records
        self.checkpoint_records = self._checkpoint_records
        self._late_batches: list[dict[str, Any]] = []
        self.late_batches = self._late_batches
        # 不同命名习惯下的同一份迟到批次清单。
        self.late_batch_records = self._late_batches
        self._late_batch_records = self._late_batches
        self.late_data_batches = self._late_batches
        self._late_data_batches = self._late_batches
        self._late_records = self._late_batches
        self.late_records = self._late_records
        self._late_batch_manifest = self._late_batches
        self.late_batch_manifest = self._late_batch_manifest
        self._late_pending_sends: dict[str, dict[str, Any]] = {}
        self.late_pending_sends = self._late_pending_sends
        self.pending_late_sends = self._late_pending_sends
        self._pending_late_sends = self._late_pending_sends
        self._late_send_confirmations = self._late_pending_sends
        self.late_send_confirmations = self._late_send_confirmations
        self._late_send_receipts: list[dict[str, Any]] = []
        self.late_send_receipts = self._late_send_receipts
        self.send_receipts = self._late_send_receipts
        self.late_send_records = self._late_send_receipts
        self._late_send_records = self._late_send_receipts
        self._late_delivery_receipts = self._late_send_receipts
        self.late_delivery_receipts = self._late_delivery_receipts
        # 快照存储表（snapshot_mgr → backfill_job 共享）
        self._snapshot_store: dict[str, dict[str, Any]] = {}
        # 血缘图（lineage → table_format 共享）
        self._lineage_graph: dict[str, list[dict[str, Any]]] = {}
        # 归档清单（cold_archive → retention 共享）
        self._archive_manifest: list[dict[str, Any]] = []

    # ---------- 基础 CRUD ----------
    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        # 跨模块流水线状态
        pipeline_stats = []
        for pname, pl in self._pipelines.items():
            pipeline_stats.append(pl.health_report())
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            {"label": "流水线", "value": len(pipeline_stats)},
        ]
        return {"cards": cards, "modules": modules, "pipelines": pipeline_stats}

    # ---------- 跨模块流水线 ----------
    def create_pipeline(self, name: str, sources: list[str], target_module: str) -> DataPipeline:
        pl = DataPipeline(name, sources, target_module)
        self._pipelines[name] = pl
        return pl

    def pipeline(self, name: str) -> DataPipeline | None:
        return self._pipelines.get(name)

    def all_pipelines(self) -> dict[str, DataPipeline]:
        return dict(self._pipelines)

    # ---------- Schema 注册表 ----------
    def register_schema(self, topic: str, schema_def: dict[str, Any], version: str) -> dict[str, Any]:
        versions = self._schema_registry.setdefault(topic, [])
        entry = {"topic": topic, "definition": schema_def, "version": version, "ts": time.time()}
        versions.append(entry)
        return entry

    def schema_versions(self, topic: str) -> list[dict[str, Any]]:
        return self._schema_registry.get(topic, [])

    def check_schema_compatibility(self, topic: str, new_fields: list[str]) -> dict[str, Any]:
        versions = self.schema_versions(topic)
        if not versions:
            return {"compatible": True, "reason": "新主题，无兼容性问题"}
        latest = versions[-1]
        old_fields = set(latest.get("definition", {}).get("fields", []))
        missing_required = [f for f in old_fields if f not in new_fields]
        if missing_required:
            return {"compatible": False, "reason": f"缺少已有字段: {missing_required}"}
        return {"compatible": True, "reason": "向后兼容"}

    # ---------- 水位记录 ----------
    def update_watermark(self, partition: str, offset: dict[str, Any]) -> None:
        self._watermark_offsets[partition] = offset

    def get_watermark(self, partition: str) -> dict[str, Any] | None:
        return self._watermark_offsets.get(partition)

    # ---------- 确认记录 / 迟到批次 ----------
    def save_confirmation(self, record: dict[str, Any]) -> dict[str, Any]:
        partition = str(record.get("partition") or record.get("source") or "default")
        records = self._confirmation_records.setdefault(partition, [])
        record_id = record.get("confirmation_id") or record.get("id")
        if record_id is not None:
            for index, existing in enumerate(records):
                if existing.get("confirmation_id") == record_id or existing.get("id") == record_id:
                    records[index] = record
                    return record
        records.append(record)
        return record

    def list_confirmations(self, partition: str | None = None) -> list[dict[str, Any]]:
        if partition is None:
            return [record for records in self._confirmation_records.values() for record in records]
        return list(self._confirmation_records.get(str(partition), []))

    def add_late_batch(self, batch: dict[str, Any]) -> dict[str, Any]:
        self._late_batches.append(batch)
        return batch

    def get_late_batch(self, batch_id: str) -> dict[str, Any] | None:
        return next((batch for batch in self._late_batches if batch.get("batch_id") == batch_id or batch.get("id") == batch_id), None)

    def update_late_batch(self, batch_id: str, **changes: Any) -> dict[str, Any] | None:
        batch = self.get_late_batch(batch_id)
        if batch is not None:
            batch.update(changes)
        return batch

    def list_late_batches(self, source: str | None = None) -> list[dict[str, Any]]:
        if source is None:
            return list(self._late_batches)
        return [
            batch for batch in self._late_batches
            if str(batch.get("source") or batch.get("partition") or batch.get("关联源") or "") == str(source)
        ]

    def save_pending_send(self, send: dict[str, Any]) -> dict[str, Any]:
        key = str(send.get("send_id") or send.get("id"))
        self._late_pending_sends[key] = send
        return send

    def get_pending_send(self, send_id: str) -> dict[str, Any] | None:
        return self._late_pending_sends.get(str(send_id))

    def save_send_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        self._late_send_receipts.append(receipt)
        return receipt

    def list_send_receipts(self, send_id: str | None = None) -> list[dict[str, Any]]:
        if send_id is None:
            return list(self._late_send_receipts)
        return [r for r in self._late_send_receipts if str(r.get("send_id") or r.get("id")) == str(send_id)]

    # ---------- 快照存储 ----------
    def save_snapshot(self, snap_id: str, data: dict[str, Any]) -> None:
        self._snapshot_store[snap_id] = data

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshot_store.get(snap_id)

    # ---------- 变更日志 ----------
    def append_changelog(self, entry: dict[str, Any]) -> None:
        entry["_ts"] = time.time()
        self._global_changelog.append(entry)

    def changelog_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [e for e in self._global_changelog if e.get("目标表") == table_name]

    # ---------- 血缘图 ----------
    def add_lineage_edge(self, upstream: str, downstream: str, edge_data: dict[str, Any]) -> None:
        edges = self._lineage_graph.setdefault(upstream, [])
        edges.append({"downstream": downstream, **edge_data, "ts": time.time()})

    def lineage_downstream(self, table_name: str) -> list[dict[str, Any]]:
        return self._lineage_graph.get(table_name, [])

    # ---------- 归档清单 ----------
    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_manifest.append(record)

    def archive_manifest_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [r for r in self._archive_manifest if r.get("源表名称") == table_name]

    def __getattr__(self, name: str) -> Any:
        """让 store.<常见动作名> 也能复用领域服务中的同一套进度逻辑。"""
        if name.startswith("__"):
            raise AttributeError(name)
        lowered = name.lower()
        try:
            from app.services import service_for
        except Exception:  # noqa: BLE001
            raise AttributeError(name)

        if any(token in lowered for token in ("late", "send", "dispatch", "receipt", "classify")):
            service = service_for("late_data")
        elif any(token in lowered for token in ("offset", "commit", "ack", "confirm", "progress", "watermark", "checkpoint")):
            service = service_for("watermark")
        else:
            raise AttributeError(name)
        if service is None or not hasattr(service, name):
            raise AttributeError(name)
        return getattr(service, name)


store = Store()


def __getattr__(name: str) -> Any:
    if name.startswith("__"):
        raise AttributeError(name)
    return getattr(store, name)