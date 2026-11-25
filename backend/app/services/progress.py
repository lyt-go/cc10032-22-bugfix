"""消费进度与事件时间的共享内核。

这里不把 offset 当作事件时间，也不在没有确认事件时虚构推进。
watermark、stream_source 和 late_data 都通过这里解释同一份分区状态。
"""
from __future__ import annotations

import copy
import math
import threading
import time
from datetime import datetime
from typing import Any

MISSING = object()


def now_ts() -> float:
    return time.time()


def parse_time_value(value: Any) -> float | None:
    """把常见事件时间表示解析为 epoch 秒；无法识别时返回 None。

    纯数字字符串按数值时间处理。调用方不能把 offset 传入这里来“补”事件时间。
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, datetime):
        return value.timestamp()

    text = str(value).strip()
    if not text:
        return None

    try:
        number = float(text)
        if math.isfinite(number):
            return number
    except ValueError:
        pass

    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).timestamp()
        except ValueError:
            continue
    return None


def extract_offset(value: Any) -> int | float | None:
    """从数字、事件字典或常见位置字段中提取 offset。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value if math.isfinite(float(value)) else None
    if isinstance(value, str):
        text = value.strip()
        try:
            number = float(text)
            return int(number) if number.is_integer() else number
        except ValueError:
            return None
    if isinstance(value, dict):
        for key in (
            "offset", "committed_offset", "commit_offset", "confirmed_offset",
            "position", "seq", "sequence", "sequence_id", "log_offset",
            "当前位点", "位点", "位置", "偏移量",
        ):
            if key in value:
                result = extract_offset(value[key])
                if result is not None:
                    return result
    return None


def extract_event_time(event: dict[str, Any] | Any) -> tuple[Any, float | None, str | None]:
    """返回 ``(原始事件时间, epoch 秒, 使用字段)``；缺失时为 ``(None, None, None)``。"""
    if not isinstance(event, dict):
        return None, None, None
    for key in (
        "event_time", "eventTime", "event_timestamp", "eventTimestamp",
        "occurred_at", "occur_time", "ts", "timestamp", "time",
        "事件时间", "事件时刻", "时间",
    ):
        if key in event and event.get(key) not in (None, ""):
            raw = event.get(key)
            return raw, parse_time_value(raw), key
    return None, None, None


def event_identity(event: Any, offset: int | float | None) -> str:
    if isinstance(event, dict):
        for key in ("event_id", "id", "message_id", "msg_id", "key"):
            value = event.get(key)
            if value not in (None, ""):
                return f"id:{key}:{value}"
    return f"offset:{offset}"


def _offset_key(offset: int | float) -> str:
    if isinstance(offset, float) and offset.is_integer():
        return str(int(offset))
    return str(offset)


def _as_int_offset(offset: int | float | None) -> int | None:
    if offset is None:
        return None
    if isinstance(offset, int):
        return offset
    if isinstance(offset, float) and offset.is_integer():
        return int(offset)
    return None


def normalize_progress(partition: str, raw: dict[str, Any] | None = None) -> dict[str, Any]:
    """规范化旧水位记录，但保留旧字段和旧值。

    老数据可能只有 ``{"offset": 100}``。此时 event_time 必须保持 None，
    绝不能把 100 解释成 100 秒时间戳。
    """
    state: dict[str, Any] = dict(raw or {})
    state["partition"] = partition

    existing_committed = None
    for key in ("committed_offset", "confirmed_offset", "submitted_offset", "commit_offset"):
        if key in state:
            existing_committed = extract_offset(state.get(key))
            if existing_committed is not None:
                break
    if existing_committed is None and "offset" in state:
        existing_committed = extract_offset(state.get("offset"))

    base = None
    for key in ("base_offset", "initial_offset", "start_offset"):
        if key in state:
            base = extract_offset(state.get(key))
            if base is not None:
                break
    if base is None:
        base = existing_committed if existing_committed is not None else 0

    committed = existing_committed if existing_committed is not None else base
    state.setdefault("base_offset", base)
    state["committed_offset"] = committed
    state["current_offset"] = committed
    state["confirmed_offset"] = committed
    state["submitted_offset"] = committed
    state["offset"] = committed

    raw_event_time = None
    event_time_epoch = None
    for key in ("event_time", "committed_event_time", "watermark_event_time", "最大事件时间"):
        if key in state and state.get(key) not in (None, ""):
            raw_event_time = state.get(key)
            event_time_epoch = parse_time_value(raw_event_time)
            if event_time_epoch is not None:
                break
    # 只有无法解析的登记时间时保留原值，但不提供可比较的 epoch。
    state.setdefault("event_time", raw_event_time if event_time_epoch is not None else None)
    state["committed_event_time"] = state.get("event_time") if event_time_epoch is not None else None
    state["event_time_epoch"] = event_time_epoch
    state.setdefault("max_event_time", state.get("event_time"))
    state.setdefault("max_event_time_epoch", event_time_epoch)

    events = state.get("events")
    if not isinstance(events, dict):
        events = {}
    normalized_events: dict[str, dict[str, Any]] = {}
    for key, item in events.items():
        if isinstance(item, dict):
            item_offset = extract_offset(item) or extract_offset(key)
            if item_offset is not None:
                normalized_events[_offset_key(item_offset)] = item
    state["events"] = normalized_events

    confirmations = state.get("confirmations")
    if not isinstance(confirmations, list):
        state["confirmations"] = []

    state.setdefault("pending_offsets", [])
    state.setdefault("confirmed_offsets", [])
    state.setdefault("gaps", [])
    state.setdefault("updated_at", now_ts())
    state.setdefault("version", 1)
    return state


def recompute_contiguous_progress(state: dict[str, Any]) -> dict[str, Any]:
    """根据已确认 offset 集合重算连续前缀。

    例如基点 100，已有 101、103：committed 仍为 101，gap 为 [102]；
    再补入 102 后，committed 一次推进到 103。
    """
    base = _as_int_offset(extract_offset(state.get("base_offset")))
    committed = _as_int_offset(extract_offset(state.get("committed_offset")))
    if base is None:
        base = committed if committed is not None else 0
    if committed is None or committed < base:
        committed = base

    confirmed: set[int] = set()
    for key, item in state.get("events", {}).items():
        if not isinstance(item, dict):
            continue
        # 采样/观测记录还不是已确认事件，不能参与连续位点推进。
        if item.get("confirmed") is False or item.get("status") in {"sampled", "observed", "unconfirmed"}:
            continue
        offset = _as_int_offset(extract_offset(item) or extract_offset(key))
        if offset is not None and offset > base:
            confirmed.add(offset)

    # 兼容直接存放在 confirmed_offsets 中的数字。
    for value in state.get("confirmed_offsets", []):
        offset = _as_int_offset(extract_offset(value))
        if offset is not None and offset > base:
            confirmed.add(offset)

    while committed + 1 in confirmed:
        committed += 1

    future = sorted(o for o in confirmed if o > committed)
    gaps: list[int] = []
    if future:
        # 连续提交模型下，committed+1 到最大已确认位置之间都是缺口；
        # 已在集合中的点不算缺口。
        gaps = [o for o in range(committed + 1, future[-1]) if o not in confirmed]

    state["base_offset"] = base
    state["committed_offset"] = committed
    state["current_offset"] = committed
    state["confirmed_offset"] = committed
    state["submitted_offset"] = committed
    state["commit_offset"] = committed
    state["last_committed_offset"] = committed
    state["committed_position"] = committed
    state["current_position"] = committed
    state["position"] = committed
    state["registered_offset"] = committed
    state["ack_offset"] = committed
    state["acked_offset"] = committed
    state["last_ack_offset"] = committed
    state["safe_offset"] = committed
    state["committed"] = committed
    state["offset"] = committed
    state["confirmed_offsets"] = sorted(confirmed)
    state["pending_offsets"] = future
    state["gaps"] = gaps

    frontier_item = state.get("events", {}).get(_offset_key(committed))
    if isinstance(frontier_item, dict):
        raw_time, epoch, _ = extract_event_time(frontier_item)
        state["event_time"] = raw_time
        state["committed_event_time"] = raw_time
        state["event_time_epoch"] = epoch
    elif committed == base:
        # 基点可能来自老登记记录；保留其中可解析的事件时间，但绝不从 offset 推导。
        existing_epoch = parse_time_value(state.get("event_time"))
        if existing_epoch is None:
            state["event_time"] = None
            state["committed_event_time"] = None
            state["event_time_epoch"] = None

    max_epoch = None
    max_raw = None
    for item in state.get("events", {}).values():
        if not isinstance(item, dict):
            continue
        raw, epoch, _ = extract_event_time(item)
        if epoch is not None and (max_epoch is None or epoch > max_epoch):
            max_epoch = epoch
            max_raw = raw
    if max_epoch is not None:
        state["max_event_time_epoch"] = max_epoch
        state["max_event_time"] = max_raw
    return state


def progress_summary(state: dict[str, Any]) -> dict[str, Any]:
    committed = state.get("committed_offset", state.get("offset", 0))
    return {
        "partition": state.get("partition"),
        "offset": committed,
        "current_offset": committed,
        "committed_offset": committed,
        "confirmed_offset": committed,
        "submitted_offset": committed,
        "committed_position": committed,
        "current_position": committed,
        "position": committed,
        "registered_offset": committed,
        "ack_offset": committed,
        "acked_offset": committed,
        "safe_offset": committed,
        "committed": committed,
        "base_offset": state.get("base_offset"),
        "event_time": state.get("event_time"),
        "eventTime": state.get("event_time"),
        "committed_event_time": state.get("committed_event_time"),
        "committedEventTime": state.get("committed_event_time"),
        "event_time_epoch": state.get("event_time_epoch"),
        "max_event_time": state.get("max_event_time"),
        "maxEventTime": state.get("max_event_time"),
        "max_event_time_epoch": state.get("max_event_time_epoch"),
        "time_basis": "watermark_event_time" if state.get("event_time_epoch") is not None else "processing_time",
        "confirmed_offsets": list(state.get("confirmed_offsets", [])),
        "pending_offsets": list(state.get("pending_offsets", [])),
        "gaps": list(state.get("gaps", [])),
        "committed_history": list(state.get("committed_history") or [committed]),
        "positions": list(state.get("positions") or state.get("committed_history") or [committed]),
        "version": state.get("version"),
    }


def snapshot_store_state(store_obj: Any) -> Any:
    """深拷贝持久化相关内存状态，供写库失败时恢复。"""
    names = (
        "_watermark_offsets", "watermark_offsets",
        "_confirmation_records", "confirmation_records",
        "_late_batches", "late_batches",
        "_late_pending_sends", "late_pending_sends",
        "_late_send_receipts", "late_send_receipts", "send_receipts",
    )
    snap: dict[str, Any] = {}
    for name in names:
        if hasattr(store_obj, name):
            snap[name] = copy.deepcopy(getattr(store_obj, name))
    return snap


def restore_store_state(store_obj: Any, snapshot: dict[str, Any]) -> None:
    for name, value in snapshot.items():
        current = getattr(store_obj, name, MISSING)
        if isinstance(current, dict) and isinstance(value, dict):
            current.clear()
            current.update(value)
        elif isinstance(current, list) and isinstance(value, list):
            current.clear()
            current.extend(value)
        else:
            setattr(store_obj, name, copy.deepcopy(value))


def _batch_partition(batch: dict[str, Any]) -> str | None:
    value = batch.get("partition") or batch.get("source") or batch.get("关联源")
    return None if value is None else str(value)


def snapshot_partition_state(store_obj: Any, partition: str) -> dict[str, Any]:
    """只快照一个分区的状态，避免回滚时误删其他分区的并发提交。"""
    partition = str(partition)
    wm = store_obj._watermark_offsets.get(partition, MISSING)
    confirmations = store_obj._confirmation_records.get(partition, MISSING)
    batches = [
        copy.deepcopy(batch) for batch in store_obj._late_batches
        if _batch_partition(batch) == partition
    ]
    pending = {
        key: copy.deepcopy(send)
        for key, send in store_obj._late_pending_sends.items()
        if str(send.get("partition") or send.get("source") or "") == partition
    }
    return {
        "partition": partition,
        "watermark": MISSING if wm is MISSING else copy.deepcopy(wm),
        "confirmations": MISSING if confirmations is MISSING else copy.deepcopy(confirmations),
        "late_batches": batches,
        "pending_sends": pending,
    }


def restore_partition_state(store_obj: Any, snapshot: dict[str, Any]) -> None:
    partition = str(snapshot["partition"])
    if snapshot["watermark"] is MISSING:
        store_obj._watermark_offsets.pop(partition, None)
    else:
        store_obj._watermark_offsets[partition] = copy.deepcopy(snapshot["watermark"])

    if snapshot["confirmations"] is MISSING:
        store_obj._confirmation_records.pop(partition, None)
    else:
        store_obj._confirmation_records[partition] = copy.deepcopy(snapshot["confirmations"])

    original_batches = {b.get("batch_id") or b.get("id"): b for b in snapshot["late_batches"]}
    current_matching = [b for b in store_obj._late_batches if _batch_partition(b) == partition]
    for batch in current_matching:
        batch_id = batch.get("batch_id") or batch.get("id")
        old = original_batches.get(batch_id)
        if old is None:
            store_obj._late_batches.remove(batch)
        else:
            batch.clear()
            batch.update(copy.deepcopy(old))

    original_pending = snapshot["pending_sends"]
    for key in list(store_obj._late_pending_sends.keys()):
        send = store_obj._late_pending_sends[key]
        if str(send.get("partition") or send.get("source") or "") != partition:
            continue
        if key not in original_pending:
            store_obj._late_pending_sends.pop(key, None)
        else:
            send.clear()
            send.update(copy.deepcopy(original_pending[key]))


class PartitionLockManager:
    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, threading.RLock] = {}

    def get(self, partition: str) -> threading.RLock:
        with self._guard:
            return self._locks.setdefault(partition, threading.RLock())


partition_locks = PartitionLockManager()
