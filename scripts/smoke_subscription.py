#!/usr/bin/env python3
"""容器启动后的订阅链路回归冒烟测试（仅依赖标准库，可在容器外直接运行）。

覆盖 create→poll→ack 全链路：先制造可投递事件（提交 DAG 并跑完任务），
再创建订阅（断言 201 响应同时携带 id 与规范字段 subscription_id 且值相同），
随后立即用该值 poll 并 ack，验证游标推进；未知订阅仍返回
subscription_not_found。

用法（服务已启动，例如 docker compose up --build 之后）：

    python3 scripts/smoke_subscription.py                 # 默认 http://localhost:8000
    BASE_URL=http://localhost:8000 python3 scripts/smoke_subscription.py

退出码：全部断言通过为 0，任一失败为 1。
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
import uuid

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  ok: {message}")


def main() -> int:
    run = uuid.uuid4().hex[:8]
    dag_id = f"smoke-sub-{run}"
    print(f"smoke: subscription create->poll->ack against {BASE_URL} (dag {dag_id})")

    # 0. 服务可达
    status, body = call("GET", "/health")
    check(status == 200 and body.get("status") == "ok", "GET /health -> 200 ok")

    # 1. 制造可投递事件：提交 DAG 并跑完任务（dag.created/task.claimed/task.succeeded）
    status, dag = call("POST", "/dags", {"id": dag_id, "tasks": [{"key": "a"}]})
    check(status == 201, "POST /dags -> 201（制造事件）")
    status, claimed = call("POST", "/tasks/claim", {"dag_id": dag_id, "limit": 1})
    check(status == 200 and claimed["count"] == 1, "POST /tasks/claim -> 领到任务")
    task = claimed["claimed"][0]
    status, done = call(
        "POST",
        f"/tasks/{task['task_id']}/complete",
        {"completion_id": f"smoke-{run}", "lease_token": task["lease_token"], "success": True},
    )
    check(status == 200 and done["state"] == "succeeded", "POST /tasks/{id}/complete -> succeeded")

    # 2. 创建订阅：201 响应必须同时给出 id 与 subscription_id，且两者值完全相同
    status, sub = call("POST", "/subscriptions", {"dag_id": dag_id})
    check(status == 201, "POST /subscriptions -> 201")
    check(bool(sub.get("id")) and bool(sub.get("subscription_id")),
          "创建响应同时携带 id 与 subscription_id")
    check(sub["id"] == sub["subscription_id"], "id 与 subscription_id 值完全相同")
    sid = sub["subscription_id"]

    # 列表与详情的标识序列化与创建响应一致
    status, listing = call("GET", f"/subscriptions?dag_id={dag_id}")
    mine = [s for s in listing["subscriptions"] if s.get("subscription_id") == sid]
    check(status == 200 and len(mine) == 1 and mine[0]["id"] == sid,
          "列表响应的 id/subscription_id 与创建响应一致")
    status, detail = call("GET", f"/subscriptions/{sid}")
    check(status == 200 and detail["id"] == sid and detail["subscription_id"] == sid,
          "详情响应的 id/subscription_id 与创建响应一致")

    # 3. 立即用创建响应的 subscription_id poll：命中刚创建的记录并投出事件
    status, batch = call("POST", f"/subscriptions/{sid}/poll",
                         {"limit": 50, "lease_seconds": 30})
    check(status == 200, "POST /subscriptions/{subscription_id}/poll -> 200（不再 404）")
    check(batch["subscription_id"] == sid, "poll 响应回显同一 subscription_id")
    check(bool(batch["events"]) and batch["delivery_token"],
          "poll 投出事件并给出 delivery_token")
    check(all(e["dag_id"] == dag_id for e in batch["events"]), "dag_id 过滤生效")

    # 4. 凭 delivery_token ack：游标推进到该批最后一条 sequence
    status, acked = call("POST", f"/subscriptions/{sid}/ack",
                         {"delivery_token": batch["delivery_token"]})
    check(status == 200, "POST /subscriptions/{subscription_id}/ack -> 200")
    check(acked == {"subscription_id": sid, "committed_sequence": batch["last_sequence"]},
          "ack 推进游标到该批最后一条 sequence")
    status, detail = call("GET", f"/subscriptions/{sid}")
    check(detail["committed_sequence"] == batch["last_sequence"] and detail["in_flight"] is None,
          "详情确认游标已推进、在途批次已清空")

    # 5. 未知订阅仍是 subscription_not_found（语义不变）
    for method, path, payload in (
        ("GET", "/subscriptions/smoke-missing", None),
        ("POST", "/subscriptions/smoke-missing/poll", {"lease_seconds": 1}),
        ("POST", "/subscriptions/smoke-missing/ack", {"delivery_token": "x"}),
    ):
        status, err = call(method, path, payload)
        check(status == 404 and err["error"]["code"] == "subscription_not_found",
              f"{method} {path} -> 404 subscription_not_found")

    print("smoke: all checks passed")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        print(f"smoke: FAILED - {exc}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(f"smoke: FAILED - cannot reach {BASE_URL}: {exc}", file=sys.stderr)
        sys.exit(1)
