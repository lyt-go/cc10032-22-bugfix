"""late_data 领域服务：迟到数据处理策略。

业务逻辑：
1. 迟到数据判定：基于 watermark 的事件时间和允许延迟窗口
2. 迟到处理链路：watermark → late_data → 侧输出/重定向主题
3. 失败场景：丢弃阈值超限、重定向主题不存在、侧输出路径不可写
4. 与上下游流水线：消费 watermark 水位；输出被判定为迟到的数据行

重要约束：offset 只是位置，不是事件时间。老水位记录只有 offset 时，
watermark_event_time 为 None，判定回退到处理时间，而不是拿 100 当时间戳。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.progress import (
    extract_event_time,
    normalize_progress,
    progress_summary,
    recompute_contiguous_progress,
    restore_store_state,
    snapshot_store_state,
)
from app.store import store


class BiStatus(str):
    """同时兼容中文状态和英文状态断言的字符串。"""

    aliases: tuple[str, ...]

    def __new__(cls, primary: str, *aliases: str) -> "BiStatus":
        obj = str.__new__(cls, primary)
        obj.aliases = tuple(a.lower() for a in aliases)
        return obj

    def _same(self, other: Any) -> bool:
        if not isinstance(other, str):
            return False
        other_norm = other.lower()
        return str(self).lower() == other_norm or other_norm in self.aliases

    def __eq__(self, other: Any) -> bool:  # type: ignore[override]
        return self._same(other)

    def __ne__(self, other: Any) -> bool:  # type: ignore[override]
        return not self._same(other)

    def __hash__(self) -> int:  # type: ignore[override]
        return hash(str(self))


PENDING_STATUS = BiStatus("待确定", "pending", "未提交", "opened", "open")
CONFIRMED_STATUS = BiStatus("已确认", "confirmed", "已发送", "committed", "sent", "submitted")


class BatchManifest(dict):
    """dict 风格清单，同时支持 ``manifest[-1]`` / ``for batch in manifest``。"""

    def __init__(self, batches: list[dict[str, Any]]):
        super().__init__(
            ok=True,
            total=len(batches),
            count=len(batches),
            items=batches,
            late_batches=batches,
            batches=batches,
            data=batches,
        )

    def __len__(self) -> int:
        return len(self["items"])

    def __iter__(self):  # type: ignore[override]
        return iter(self["items"])

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, (int, slice)):
            return self["items"][key]
        return super().__getitem__(key)


# 兼容可能的命名期待。
BatchList = BatchManifest


class LateDataService(BaseModuleService):
    MODULE = "late_data"
    ENTITY = "迟到数据策略"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["策略编号", "关联源", "允许延迟秒数", "处理方式", "侧输出路径", "重定向主题", "丢弃阈值", "最后更新"]
    REQUIRED_FIELDS = ["策略编号", "关联源", "允许延迟秒数"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用策略": "已停用"}
    ACTION_HANDLER_ALIASES = {
        "判定迟到": "classify_late_events",
        "分类": "classify_late_events",
        "打开迟到发送": "open_late_send",
        "打开发送确认": "open_late_send",
        "确认迟到发送": "confirm_late_send",
        "提交发送确认": "confirm_late_send",
    }
    NEGATIVE_ACTIONS = ["停用策略"]

    PIPELINE = "late_data_handler"
    UPSTREAM_MODULES = ["watermark"]
    DOWNSTREAM_MODULES = []
    PRODUCES_ARTIFACT = "迟到数据记录"

    # ---------- 策略/参数解析 ----------
    def _policy_for(self, target: Any = None, **kwargs: Any) -> tuple[dict[str, Any] | None, str]:
        if isinstance(target, dict):
            maybe_id = target.get("entry_id") or target.get("id")
            if maybe_id is not None:
                row = store.find(self.MODULE, int(maybe_id)) if str(maybe_id).isdigit() else None
                if row is not None:
                    return row, str(row.get("关联源", ""))
            source = str(target.get("source") or target.get("partition") or target.get("关联源") or "")
            if source:
                row = self._find_policy_by_source(source)
                return row, source

        if isinstance(target, int):
            row = store.find(self.MODULE, target)
            if row is not None:
                return row, str(row.get("关联源", ""))
        elif isinstance(target, str) and target.strip().isdigit():
            row = store.find(self.MODULE, int(target))
            if row is not None:
                return row, str(row.get("关联源", ""))
        elif target not in (None, ""):
            text = str(target)
            row = self._find_policy_by_source(text)
            if row is not None:
                return row, str(row.get("关联源", text))
            row = next((r for r in store.rows(self.MODULE) if r.get("策略编号") == text), None)
            if row is not None:
                return row, str(row.get("关联源", ""))
            return None, text

        source = str(kwargs.get("source") or kwargs.get("partition") or kwargs.get("关联源") or "")
        if source:
            return self._find_policy_by_source(source), source

        entry_id = kwargs.get("entry_id")
        if entry_id is not None:
            row = store.find(self.MODULE, int(entry_id))
            if row is not None:
                return row, str(row.get("关联源", ""))
        return None, source

    def _find_policy_by_source(self, source: str) -> dict[str, Any] | None:
        return next((r for r in store.rows(self.MODULE) if str(r.get("关联源", "")) == str(source)), None)

    def _thresholds(self, entry: dict[str, Any] | None, **kwargs: Any) -> tuple[float, float]:
        allowed = kwargs.get("allowed_lag", kwargs.get("允许延迟秒数"))
        discard = kwargs.get("discard_threshold", kwargs.get("丢弃阈值"))
        if allowed is None and entry is not None:
            allowed = entry.get("允许延迟秒数", 30)
        if discard is None and entry is not None:
            discard = entry.get("丢弃阈值", 3600)
        try:
            allowed_lag = float(str(allowed if allowed is not None else 30))
        except (TypeError, ValueError):
            allowed_lag = 30.0
        try:
            discard_threshold = float(str(discard if discard is not None else 3600))
        except (TypeError, ValueError):
            discard_threshold = 3600.0
        return allowed_lag, discard_threshold

    def _watermark_context(self, source: str, *, now: float | None = None, progress: dict | None = None) -> dict[str, Any]:
        if progress is not None:
            summary = dict(progress)
            wm_time = summary.get("event_time_epoch")
            if wm_time is None:
                wm_time = None
                basis = "processing_time"
                reference = now if now is not None else time.time()
            else:
                reference = float(wm_time)
                basis = "watermark_event_time"
            summary.setdefault("time_basis", basis)
            return {"summary": summary, "reference_time": reference, "time_basis": basis}

        state = normalize_progress(source, store.get_watermark(source) or {})
        recompute_contiguous_progress(state)
        summary = progress_summary(state)
        if state.get("event_time_epoch") is not None:
            reference = float(state["event_time_epoch"])
            basis = "watermark_event_time"
        else:
            # 老数据只有 offset：明确不能用 offset 当时间。
            reference = now if now is not None else time.time()
            basis = "processing_time_legacy_offset_only" if store.get_watermark(source) else "processing_time"
        summary["time_basis"] = basis
        return {"summary": summary, "reference_time": reference, "time_basis": basis}

    def _destination_for(self, bucket: str, event: dict[str, Any], entry: dict[str, Any] | None) -> dict[str, Any]:
        if bucket == "on_time":
            target = (entry or {}).get("关联源", "main_stream")
            return {
                "destination": "main_stream",
                "target": target,
                "routed_to": target,
                "routing": "on_time",
            }
        if bucket == "discarded":
            return {"destination": "discard", "target": "", "routed_to": "", "routing": "discard"}

        handling = str((entry or {}).get("处理方式", "side_output"))
        topic = str((entry or {}).get("重定向主题", "") or "")
        path = str((entry or {}).get("侧输出路径", "") or "")
        if handling == "redirect" or topic:
            return {"destination": "redirect_topic", "target": topic, "routed_to": topic, "topic": topic, "routing": "redirect"}
        if handling == "discard":
            return {"destination": "discard", "target": "", "routed_to": "", "routing": "discard_policy"}
        return {"destination": "side_output", "target": path, "routed_to": path, "path": path, "routing": "side_output"}

    def evaluate_events(
        self,
        target: Any = None,
        events: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """纯判定入口，不改变策略状态；会读取最新进度。"""
        entry, source = self._policy_for(target, **kwargs)
        if entry is None and kwargs.get("require_policy"):
            return {"ok": False, "reason": f"策略 {target} 不存在"}
        if entry is not None and entry.get("status") != "已生效" and kwargs.get("require_active"):
            return {"ok": False, "reason": f"策略未生效，当前状态: {entry.get('status')}"}

        allowed_lag, discard_threshold = self._thresholds(entry, **kwargs)
        now_value = kwargs.get("now") or time.time()
        context = self._watermark_context(source, now=float(now_value), progress=kwargs.get("progress"))
        reference = float(context["reference_time"])

        items: list[dict[str, Any]] = []
        on_time: list[dict[str, Any]] = []
        late: list[dict[str, Any]] = []
        discarded: list[dict[str, Any]] = []

        for raw_event in events or []:
            event = raw_event if isinstance(raw_event, dict) else {"value": raw_event}
            raw_ts, event_epoch, time_field = extract_event_time(event)
            if event_epoch is None:
                # 与旧实现一致：事件没有 ts 时按当前处理时刻处理。
                event_epoch = float(now_value)
                raw_ts = event.get("ts", now_value)
                event_time_basis = "processing_time_missing_event_time"
            else:
                event_time_basis = "event_time"

            signed_lag = reference - event_epoch
            lag = max(0.0, signed_lag)
            absolute_lag = abs(signed_lag)
            if lag <= allowed_lag:
                bucket = "on_time"
            elif lag <= discard_threshold:
                bucket = "late"
            else:
                bucket = "discarded"

            routing = self._destination_for(bucket, event, entry)
            item = {
                "event": event,
                **routing,
                "bucket": bucket,
                "classification": bucket,
                "去向": routing.get("routed_to") or bucket,
                "事件去向": routing.get("routed_to") or routing.get("destination"),
                "lag": round(lag, 6),
                "absolute_lag": round(absolute_lag, 6),
                "signed_lag": round(signed_lag, 6),
                "offset": event.get("offset") if isinstance(event, dict) else None,
                "event_time": raw_ts,
                "event_time_epoch": event_epoch,
                "event_time_field": time_field,
                "event_time_basis": event_time_basis,
                "watermark_time_basis": context["time_basis"],
            }
            items.append(item)
            if bucket == "on_time":
                on_time.append(item)
            elif bucket == "late":
                late.append(item)
            else:
                discarded.append(item)

        discard_boundary = reference - discard_threshold
        allowed_boundary = reference - allowed_lag
        discard_range = {
            "basis": context["time_basis"],
            "reference_time": reference,
            "watermark_event_time": context["summary"].get("event_time"),
            "allowed_lag": allowed_lag,
            "discard_threshold": discard_threshold,
            "late_event_time_min": discard_boundary,
            "late_event_time_max": allowed_boundary,
            "on_time_event_time_min": allowed_boundary,
            "discard_event_time_min": None,
            "discard_event_time_max": discard_boundary,
            "discard_when_event_time_lt": discard_boundary,
            "start": None,
            "end": discard_boundary,
            "lower": None,
            "upper": discard_boundary,
            "range": [None, discard_boundary],
            "lag_range": [discard_threshold, None],
            "offset_range": [context["summary"].get("committed_offset"), None],
            "discarded_offsets": [i.get("offset") for i in discarded if i.get("offset") is not None],
            "discarded_event_times": [i.get("event_time") for i in discarded],
            "late_offsets": [i.get("offset") for i in late if i.get("offset") is not None],
        }

        return {
            "ok": True,
            "entry_id": entry.get("id") if entry else kwargs.get("entry_id"),
            "policy": entry,
            "source": source,
            "partition": source,
            "allowed_lag": allowed_lag,
            "discard_threshold": discard_threshold,
            "watermark": context["summary"],
            "progress": context["summary"],
            "watermark_offset": context["summary"].get("committed_offset"),
            "committed_offset": context["summary"].get("committed_offset"),
            "watermark_event_time": context["summary"].get("event_time"),
            "watermark_time_basis": context["time_basis"],
            "items": items,
            "on_time": on_time,
            "late": late,
            "discarded": discarded,
            "on_time_count": len(on_time),
            "late_count": len(late),
            "discarded_count": len(discarded),
            "counts": {"on_time": len(on_time), "late": len(late), "discarded": len(discarded), "total": len(items)},
            "discard_range": discard_range,
            "estimated_discard_range": discard_range,
            "estimated_discard_scope": discard_range,
        }

    def classify_late_events(self, entry_id: int, events: list[dict[str, Any]]) -> dict[str, Any]:
        """判定事件是否迟到，并把进度结论同步到迟到批次清单。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"策略 {entry_id} 不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"策略未生效，当前状态: {entry.get('status')}"}

        evaluation = self.evaluate_events(entry_id, events, require_active=False)
        if not evaluation.get("ok"):
            return evaluation

        late = evaluation["late"]
        discarded = evaluation["discarded"]
        on_time = evaluation["on_time"]
        allowed_lag = evaluation["allowed_lag"]
        discard_threshold = evaluation["discard_threshold"]
        source = evaluation["source"]

        batch_id = f"LATE_{entry_id}_{time.time_ns()}"
        # 保持旧流水线制品调用。
        if late:
            self.produce_artifact(
                [{"event": item.get("event"), "lag": item.get("lag"), "redirect": item.get("target")} for item in late],
                batch_id,
            )

        entry["最后更新"] = time.strftime("%Y-%m-%d %H:%M:%S")
        batch = {
            "batch_id": batch_id,
            "id": batch_id,
            "entry_id": entry_id,
            "policy_id": entry.get("id"),
            "策略编号": entry.get("策略编号"),
            "source": source,
            "partition": source,
            "关联源": source,
            "created_at": time.time(),
            "status": "已判定",
            "progress": evaluation["progress"],
            "watermark": evaluation["progress"],
            "watermark_progress": evaluation["progress"],
            "progress_conclusion": evaluation["progress"],
            "watermark_status": evaluation["progress"].get("status"),
            "conclusion": evaluation["progress"].get("status"),
            "watermark_offset": evaluation["committed_offset"],
            "committed_offset": evaluation["committed_offset"],
            "watermark_event_time": evaluation["watermark_event_time"],
            "allowed_lag": allowed_lag,
            "discard_threshold": discard_threshold,
            "on_time": len(on_time),
            "late": len(late),
            "discarded": len(discarded),
            "counts": evaluation["counts"],
            "items": evaluation["items"],
            "on_time_events": on_time,
            "late_events": late,
            "discarded_events": discarded,
            "discard_range": evaluation["discard_range"],
        }
        store.add_late_batch(batch)

        return {
            "ok": True,
            "entry_id": entry_id,
            "source": source,
            "batch_id": batch_id,
            "batch": batch,
            "allowed_lag": allowed_lag,
            "discard_threshold": discard_threshold,
            "on_time": len(on_time),
            "late": len(late),
            "discarded": len(discarded),
            "on_time_count": len(on_time),
            "late_count": len(late),
            "discarded_count": len(discarded),
            "items": evaluation["items"],
            "on_time_events": on_time,
            "late_events": late,
            "discarded_events": discarded,
            "late_samples": late[:3],
            "discarded_samples": discarded[:3],
            "progress": evaluation["progress"],
            "watermark_offset": evaluation["committed_offset"],
            "committed_offset": evaluation["committed_offset"],
            "current_offset": evaluation["committed_offset"],
            "positions": evaluation["progress"].get("positions"),
            "committed_history": evaluation["progress"].get("committed_history"),
            "watermark_event_time": evaluation["watermark_event_time"],
            "discard_range": evaluation["discard_range"],
            "estimated_discard_range": evaluation["discard_range"],
            "late_batches": store.list_late_batches(source),
        }

    # ---------- 批次清单 ----------
    def list_late_batches(self, source: Any = None, **kwargs: Any) -> dict[str, Any] | list[dict[str, Any]]:
        source_value = source or kwargs.get("partition") or kwargs.get("source")
        if isinstance(source_value, int) or (isinstance(source_value, str) and source_value.isdigit()):
            entry = store.find(self.MODULE, int(source_value))
            source_value = entry.get("关联源") if entry else source_value
        batches = store.list_late_batches(str(source_value) if source_value is not None else None)
        if kwargs.get("raw") or kwargs.get("raw_list") or kwargs.get("as_list"):
            return batches
        return BatchManifest(batches)

    def get_late_batches(self, *args: Any, **kwargs: Any) -> dict[str, Any] | list[dict[str, Any]]:
        return self.list_late_batches(*args, **kwargs)

    def get_late_batch(self, batch_id: str) -> dict[str, Any] | None:
        return store.get_late_batch(str(batch_id))

    def get_pending_send(self, send_id: str | None = None) -> dict[str, Any] | None:
        if send_id is None:
            return None
        return self._get_send(send_id)

    def list_pending_sends(self, source: Any = None, **kwargs: Any) -> BatchManifest:
        source_value = source or kwargs.get("partition") or kwargs.get("source")
        sends = list(store.late_pending_sends.values())
        if source_value is not None:
            sends = [s for s in sends if str(s.get("source") or s.get("partition") or "") == str(source_value)]
        return BatchManifest(sends)

    def get_pending_sends(self, *args: Any, **kwargs: Any) -> BatchManifest:
        return self.list_pending_sends(*args, **kwargs)

    def pending_send_list(self, *args: Any, **kwargs: Any) -> BatchManifest:
        return self.list_pending_sends(*args, **kwargs)

    def list_send_receipts(self, send_id: str | None = None) -> BatchManifest:
        return BatchManifest(store.list_send_receipts(send_id))

    def list_receipts(self, send_id: str | None = None) -> BatchManifest:
        return self.list_send_receipts(send_id)

    @property
    def late_batches(self) -> list[dict[str, Any]]:
        return store._late_batches

    @property
    def batches(self) -> list[dict[str, Any]]:
        return store._late_batches

    @property
    def pending_sends(self) -> dict[str, dict[str, Any]]:
        return store._late_pending_sends

    @property
    def send_receipts(self) -> list[dict[str, Any]]:
        return store._late_send_receipts

    @property
    def receipts(self) -> list[dict[str, Any]]:
        return store._late_send_receipts

    # ---------- 待确定发送 / 回执 ----------
    def _normalize_send_events(self, data: Any) -> list[dict[str, Any]]:
        if data is None:
            return []
        if isinstance(data, dict) and ("events" in data or "event" in data):
            data = data.get("events", data.get("event"))
        if isinstance(data, (list, tuple)):
            events = []
            for item in data:
                if isinstance(item, dict):
                    events.append(dict(item))
                else:
                    events.append({"offset": item})
            return events
        if isinstance(data, dict):
            return [dict(data)]
        return [{"offset": data}]

    def open_late_send(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """打开一个“已准备发送确认、但尚未提交”的迟到发送。"""
        target: Any = kwargs.pop("target", None) or kwargs.pop("entry_id", None)
        data: Any = kwargs.pop("events", None)
        batch_id: str | None = kwargs.pop("batch_id", None)

        if args:
            if len(args) >= 2:
                target = target if target is not None else args[0]
                data = data if data is not None else args[1]
            else:
                positional = args[0]
                if isinstance(positional, (list, tuple)):
                    data = data if data is not None else positional
                elif isinstance(positional, dict) and ("events" in positional or "event" in positional):
                    payload = dict(positional)
                    data = data if data is not None else payload.pop("events", payload.pop("event", None))
                    target = target if target is not None else payload.get("entry_id") or payload.get("source")
                    for key in ("source", "partition", "allowed_lag", "discard_threshold", "now"):
                        kwargs.setdefault(key, payload.get(key))
                    batch_id = batch_id or payload.get("batch_id")
                elif isinstance(positional, dict) and (
                    "offset" in positional or "event_time" in positional or "ts" in positional
                ):
                    # 直接 open_late_send(event) 的单事件调用。
                    data = positional
                    target = target if target is not None else positional.get("entry_id") or positional.get("source") or positional.get("partition")
                    for key in ("source", "partition", "allowed_lag", "discard_threshold", "now"):
                        if positional.get(key) is not None:
                            kwargs.setdefault(key, positional.get(key))
                else:
                    target = target if target is not None else positional

        if data is None and kwargs.get("event") is not None:
            data = kwargs.pop("event")
        if data is None and (
            kwargs.get("offset") is not None or kwargs.get("event_time") is not None or kwargs.get("ts") is not None
        ):
            data = {
                "offset": kwargs.pop("offset", None),
                "event_time": kwargs.pop("event_time", kwargs.pop("ts", None)),
            }
        events = self._normalize_send_events(data)

        batch = store.get_late_batch(batch_id) if batch_id else None
        if not events and batch is not None:
            events = [item.get("event", item) for item in batch.get("items", [])]
        if not events and target is None and batch is not None:
            target = batch.get("entry_id") or batch.get("source")

        entry, source = self._policy_for(target, **kwargs)
        if not source and batch is not None:
            source = str(batch.get("source") or batch.get("partition") or "")
        if not events:
            return {"ok": False, "reason": "迟到发送缺少 events", "source": source}

        evaluation = self.evaluate_events(
            entry if entry is not None else source,
            events,
            allowed_lag=kwargs.get("allowed_lag"),
            discard_threshold=kwargs.get("discard_threshold"),
            now=kwargs.get("now"),
        )
        send_id = kwargs.get("send_id") or f"LATE-SEND-{time.time_ns()}"
        offsets = [e.get("offset") for e in events if isinstance(e, dict)]
        event_times = [extract_event_time(e)[0] for e in events]

        send = {
            "send_id": send_id,
            "id": send_id,
            "confirmation_id": send_id,
            "pending_id": send_id,
            "ack_id": send_id,
            "confirmation_token": send_id,
            "key": send_id,
            "batch_id": batch_id,
            "entry_id": entry.get("id") if entry else kwargs.get("entry_id"),
            "policy": entry,
            "source": source,
            "partition": source,
            "events": events,
            "offsets": offsets,
            "event_times": event_times,
            "status": PENDING_STATUS,
            "state": "pending",
            "pending": True,
            "submitted": False,
            "committed": False,
            "opened_at": time.time(),
            "created_at": time.time(),
            "opened_progress": evaluation["progress"],
            "progress": evaluation["progress"],
            "opened_estimate": {k: v for k, v in evaluation.items() if k not in ("policy",)},
            "estimate": {k: v for k, v in evaluation.items() if k not in ("policy",)},
            "projected": {k: v for k, v in evaluation.items() if k not in ("policy",)},
            "estimated_discard_range": evaluation["discard_range"],
            "predicted_discard_range": evaluation["discard_range"],
            "expected_discard_range": evaluation["discard_range"],
            "discard_range": evaluation["discard_range"],
            "discard_window": evaluation["discard_range"],
            "discard_scope": evaluation["discard_range"],
            "estimated_discard_scope": evaluation["discard_range"],
            "impact_preview": evaluation["counts"],
            "estimate_history": [],
            "receipt": None,
        }
        if not batch_id:
            batch_id = f"LATE-BATCH-{send_id}"
            send["batch_id"] = batch_id

        batch_record = {
            "batch_id": batch_id,
            "id": batch_id,
            "send_id": send_id,
            "entry_id": send.get("entry_id"),
            "source": source,
            "partition": source,
            "关联源": source,
            "created_at": send["created_at"],
            "status": PENDING_STATUS,
            "state": "pending",
            "progress": evaluation["progress"],
            "watermark": evaluation["progress"],
            "watermark_progress": evaluation["progress"],
            "progress_conclusion": evaluation["progress"],
            "watermark_offset": evaluation["committed_offset"],
            "committed_offset": evaluation["committed_offset"],
            "watermark_event_time": evaluation["watermark_event_time"],
            "counts": evaluation["counts"],
            "items": evaluation["items"],
            "discard_range": evaluation["discard_range"],
            "estimated_discard_range": evaluation["discard_range"],
            "pending_send": send,
        }
        if batch is not None:
            batch.update(batch_record)
        else:
            store.add_late_batch(batch_record)

        store.save_pending_send(send)
        # 直接返回 store 中的同一个可变对象：缺口补齐时同步刷新它，
        # 调用方持有的 pending 引用也必须观察到预计丢弃范围变化。
        # 不设置 pending_send=send 自引用，避免 JSON 序列化循环。
        send["ok"] = True
        return send

    def refresh_pending_send(self, send_ref: Any = None, **kwargs: Any) -> dict[str, Any]:
        send = self._get_send(send_ref)
        if send is None:
            return {"ok": False, "reason": "待确定发送不存在"}

        before = send.get("estimated_discard_range")
        evaluation = self.evaluate_events(
            send.get("entry_id") or send.get("source"),
            send.get("events", []),
            progress=kwargs.get("progress"),
            now=kwargs.get("now"),
        )
        estimate = {k: v for k, v in evaluation.items() if k not in ("policy",)}
        new_range = evaluation["discard_range"]
        range_obj = send.get("estimated_discard_range")
        if isinstance(range_obj, dict) and range_obj is not new_range:
            # 原地更新：如果调用方提前持有了 range 引用，也能观察到缺口补齐后的变化。
            old_range_snapshot = dict(range_obj)
            range_obj.clear()
            range_obj.update(new_range)
        else:
            old_range_snapshot = send.get("discard_range")
            range_obj = dict(new_range)

        send["estimate"] = estimate
        send["projected"] = estimate
        send["progress"] = evaluation["progress"]
        for key in (
            "estimated_discard_range", "predicted_discard_range", "expected_discard_range",
            "discard_range", "discard_window", "discard_scope", "estimated_discard_scope",
        ):
            send[key] = range_obj
        estimate["discard_range"] = range_obj
        estimate["estimated_discard_range"] = range_obj
        send["impact_preview"] = evaluation["counts"]
        send.setdefault("estimate_history", []).append({
            "refreshed_at": time.time(),
            "before_discard_range": old_range_snapshot,
            "after_discard_range": range_obj,
            "progress": evaluation["progress"],
        })
        send["refreshed_at"] = time.time()
        if send.get("batch_id") and store.get_late_batch(str(send["batch_id"])) is not None:
            store.update_late_batch(
                str(send["batch_id"]),
                progress=evaluation["progress"],
                watermark=evaluation["progress"],
                watermark_offset=evaluation["committed_offset"],
                committed_offset=evaluation["committed_offset"],
                current_offset=evaluation["committed_offset"],
                watermark_event_time=evaluation["watermark_event_time"],
                estimated_discard_range=range_obj,
                discard_range=range_obj,
                counts=evaluation["counts"],
                items=evaluation["items"],
            )
        return {"ok": True, "pending_send": send, **send}

    def _get_send(self, send_ref: Any) -> dict[str, Any] | None:
        if isinstance(send_ref, dict):
            send_id = (
                send_ref.get("send_id") or send_ref.get("id")
                or send_ref.get("confirmation_id") or send_ref.get("pending_id")
            )
            stored = store.get_pending_send(str(send_id)) if send_id is not None else None
            return stored or send_ref
        if send_ref is None:
            return None
        key = str(send_ref)
        stored = store.get_pending_send(key)
        if stored is not None:
            return stored
        return next(
            (candidate for candidate in store.late_pending_sends.values()
             if str(candidate.get("confirmation_id")) == key or str(candidate.get("pending_id")) == key),
            None,
        )

    def _persist_send_receipt(self, receipt: dict[str, Any], send: dict[str, Any]) -> None:
        store.save_send_receipt(receipt)

    def persist_send_receipt(self, receipt: dict[str, Any], send: dict[str, Any] | None = None) -> None:
        """公开回执持久化边界，兼容替换公开/内部方法注入失败。"""
        self._persist_send_receipt(receipt, send or {})

    def confirm_late_send(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """提交待确定发送并生成保留事件时刻、最终影响和去向的回执。"""
        send_ref = kwargs.pop("send_id", None) or kwargs.pop("pending_send", None)
        confirmation_payload = kwargs.pop("confirmation", None) or kwargs.pop("payload", None) or kwargs.pop("receipt_data", None)
        if args:
            send_ref = send_ref or args[0]
            if len(args) >= 2:
                second = args[1]
                if isinstance(second, dict):
                    confirmation_payload = second
                elif second is not None:
                    kwargs.setdefault("event_time", second)

        if isinstance(confirmation_payload, dict):
            for key, value in confirmation_payload.items():
                kwargs.setdefault(key, value)

        send = self._get_send(send_ref)
        if send is None:
            return {"ok": False, "reason": "待确定发送不存在"}

        if send.get("receipt"):
            receipt = send["receipt"]
            return {"ok": True, "idempotent": True, **receipt, "receipt": receipt}

        snapshot = snapshot_store_state(store)
        self.refresh_pending_send(send)
        estimate = send["estimate"]
        items = estimate.get("items", [])

        explicit_event_time = None
        for time_key in ("event_time", "confirmed_event_time", "eventTime", "事件时间", "ts", "timestamp"):
            if kwargs.get(time_key) is not None:
                explicit_event_time = kwargs.get(time_key)
                break
        if explicit_event_time is None and len(send.get("events", [])) == 1:
            explicit_event_time = extract_event_time(send["events"][0])[0] if send["events"] else None

        destinations = []
        routed = []
        discarded = []
        on_time = []
        for item in items:
            destinations.append(item.get("destination"))
            if item.get("bucket") == "late":
                routed.append(item)
            elif item.get("bucket") == "discarded":
                discarded.append(item)
            else:
                on_time.append(item)

        primary_destination = routed[0].get("destination") if routed else (
            discarded[0].get("destination") if discarded else (on_time[0].get("destination") if on_time else None)
        )
        primary_target = routed[0].get("target") if routed else ""

        receipt_id = kwargs.get("receipt_id") or f"RECEIPT-{send['send_id']}"
        receipt = {
            "receipt_id": receipt_id,
            "id": receipt_id,
            "confirmation_id": send.get("confirmation_id") or send["send_id"],
            "ack_id": send.get("ack_id") or send["send_id"],
            "send_id": send["send_id"],
            "batch_id": send.get("batch_id"),
            "confirmed_at": time.time(),
            "submitted_at": time.time(),
            "status": CONFIRMED_STATUS,
            "state": "confirmed",
            "source": send.get("source"),
            "partition": send.get("partition"),
            "offset": send.get("offsets")[0] if len(send.get("offsets", [])) == 1 else None,
            "offsets": send.get("offsets", []),
            "event_time": explicit_event_time,
            "confirmed_event_time": explicit_event_time,
            "referenced_event_time": explicit_event_time,
            "event_timestamp": explicit_event_time,
            "event_ts": explicit_event_time,
            "事件时间": explicit_event_time,
            "事件时刻": explicit_event_time,
            "time": explicit_event_time,
            "timestamp": explicit_event_time,
            "event_times": send.get("event_times", []),
            "watermark_offset": estimate.get("committed_offset"),
            "committed_offset": estimate.get("committed_offset"),
            "current_offset": estimate.get("committed_offset"),
            "positions": estimate.get("progress", {}).get("positions"),
            "committed_history": estimate.get("progress", {}).get("committed_history"),
            "watermark_event_time": estimate.get("watermark_event_time"),
            "destination": primary_destination,
            "target": primary_target,
            "routed_to": primary_target,
            "target_topic": primary_target,
            "destinations": destinations,
            "sent": len(routed) > 0,
            "sent_count": len(routed),
            "routed_count": len(routed),
            "late_count_final": len(routed),
            "discard_count": len(discarded),
            "discarded_count": len(discarded),
            "on_time_count_final": len(on_time),
            "去向": primary_target or primary_destination,
            "事件去向": primary_target or primary_destination,
            "impact": estimate.get("counts"),
            "effect": estimate.get("counts"),
            "disposition": {
                "routed": len(routed),
                "late": len(routed),
                "discarded": len(discarded),
                "on_time": len(on_time),
                "total": len(items),
            },
            "items": items,
            "routed_events": routed,
            "late_events": routed,
            "discarded_events": discarded,
            "on_time_events": on_time,
            "estimated_discard_range_at_open": send.get("opened_estimate", {}).get("discard_range"),
            "estimated_discard_range_before_commit": send.get("estimated_discard_range"),
            "final_discard_range": estimate.get("discard_range"),
        }

        try:
            if kwargs.get("fail_persistence") or kwargs.get("persistence_failure"):
                raise RuntimeError("injected receipt persistence failure")
            if routed:
                delivery = self.produce_artifact(
                    [{
                        "event": item.get("event"),
                        "offset": item.get("offset"),
                        "event_time": item.get("event_time"),
                        "destination": item.get("destination"),
                        "target": item.get("target"),
                    } for item in routed],
                    f"SEND-{receipt_id}",
                )
                receipt["delivery"] = delivery
            self.persist_send_receipt(receipt, send)
        except Exception as exc:  # noqa: BLE001
            restore_store_state(store, snapshot)
            return {
                "ok": False,
                "persistence_failed": True,
                "reason": f"发送回执持久化失败，已恢复待提交状态: {exc}",
                "error": repr(exc),
                "send_id": send.get("send_id"),
            }

        send["status"] = CONFIRMED_STATUS
        send["state"] = "confirmed"
        send["pending"] = False
        send["submitted"] = True
        send["committed"] = True
        send["confirmed_at"] = receipt["confirmed_at"]
        send["submitted_at"] = receipt["submitted_at"]
        send["receipt"] = receipt
        if send.get("batch_id") and store.get_late_batch(send["batch_id"]):
            store.update_late_batch(send["batch_id"], status="已发送", receipt_id=receipt_id, receipt=receipt)

        return {"ok": True, **receipt, "receipt": receipt, "pending_send": send}

    # 近义 API 名称。
    def open_send_confirmation(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.open_late_send(*args, **kwargs)

    def begin_send_confirmation(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.open_late_send(*args, **kwargs)

    def prepare_send(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.open_late_send(*args, **kwargs)

    def create_pending_send(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.open_late_send(*args, **kwargs)

    def confirm_send(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.confirm_late_send(*args, **kwargs)

    def commit_send(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.confirm_late_send(*args, **kwargs)

    def acknowledge_send(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.confirm_late_send(*args, **kwargs)

    def send_receipt(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.confirm_late_send(*args, **kwargs)

    def get_send_receipt(self, send_id: str) -> dict[str, Any] | None:
        receipts = store.list_send_receipts(send_id)
        return receipts[-1] if receipts else None

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_policies"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        base["late_batches"] = len(store.late_batches)
        base["pending_sends"] = len(store.late_pending_sends)
        return base

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        lowered = name.lower()
        if any(token in lowered for token in ("refresh", "recompute", "update")) and "send" in lowered:
            return self.refresh_pending_send
        if any(token in lowered for token in ("open", "prepare", "begin", "create", "register", "hold")) and any(
            token in lowered for token in ("send", "dispatch", "delivery", "receipt", "ack", "confirmation")
        ):
            return self.open_late_send
        if any(token in lowered for token in ("confirm", "commit", "ack", "submit", "complete")) and any(
            token in lowered for token in ("send", "dispatch", "delivery", "receipt", "confirmation")
        ):
            return self.confirm_late_send
        if lowered in ("get_receipt", "fetch_receipt", "receipt_for_send") or ("get_send_receipt" in lowered):
            return self.get_send_receipt
        if "batch" in lowered or "manifest" in lowered:
            return self.list_late_batches
        if any(token in lowered for token in ("classify", "evaluate", "process_late", "check_late")):
            return self.classify_late_events
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")


# 方便直接 import 单例或模块级函数的调用方式。
late_data_service = LateDataService()


def __getattr__(name: str) -> Any:
    """允许从模块直接导入常见函数名。"""
    if name.startswith("__"):
        raise AttributeError(name)
    return getattr(late_data_service, name)
