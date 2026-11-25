#!/usr/bin/env python3
"""End-to-end drill over the real HTTP API + real UDP sockets.

Covers:
  A. 普通会话公网端口共用(范围不重叠)与正文原样交付, 空闲端口不绑定
  B. 无合规端口时拒绝(409), 且旧分配完全不变, 拒绝不移动任何会话
  C. TFTP 首包换源端口: 暂扣 -> 任意端口范围 -> 锁定实收端口
  D. 并发试传: 不移动旧会话前提下尽量受理
Run: python3 -I tests/drill.py  (server must be listening on BASE)
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
import uuid

BASE = "http://127.0.0.1:8000"
PASS, FAIL = 0, 0


def call(method: str, path: str, body: dict | None = None,
         expect_error: int | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read())
            if expect_error is not None:
                boom(f"{method} {path} expected {expect_error}, got success")
            return payload
    except urllib.error.HTTPError as exc:
        if expect_error is None or exc.code != expect_error:
            boom(f"{method} {path} unexpected HTTP {exc.code}: {exc.read()!r}")
        return {"_status": exc.code, "_detail": exc.read().decode()}


def check(cond: bool, msg: str) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {msg}")
    else:
        FAIL += 1
        print(f"  [FAIL] {msg}")


def boom(msg: str) -> None:
    raise AssertionError(msg)


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return predicate()


def unique_port() -> int:
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def make_endpoint() -> dict:
    return call("POST", "/endpoints",
                {"host": "127.0.0.1", "port": unique_port()})


def main() -> int:
    echo = [unique_port() for _ in range(3)]
    tftp_p = unique_port()
    ep_ports = [unique_port() for _ in range(8)]
    pool = [unique_port() for _ in range(3)]
    tag = uuid.uuid4().hex[:6]

    print("== 环境准备: 上游 + 内网端点 + 端口池 ==")
    call("POST", "/upstream/start",
         {"host": "127.0.0.1", "echo_ports": echo, "tftp_port": tftp_p,
          "default_detain_ms": 400})
    eps = [call("POST", "/endpoints", {"host": "127.0.0.1", "port": p})
           for p in ep_ports]
    call("POST", "/round/setup",
         {"pool_ports": pool, "max_sessions_per_port": 2,
          "bind_host": "127.0.0.1"})
    check(call("GET", "/ports") and all(not p["bound"] for p in
          call("GET", "/ports")), "初始: 空闲公网端口全部未绑定")

    # -------------------------------------------------------------- Case A
    print("\n== A. 普通端口共用 + 原样交付 + 懒绑定 ==")
    s1 = call("POST", "/sessions", {"kind": "normal",
              "internal_endpoint_id": eps[0]["id"],
              "upstream_host": "127.0.0.1", "upstream_port": echo[0]})
    s2 = call("POST", "/sessions", {"kind": "normal",
              "internal_endpoint_id": eps[1]["id"],
              "upstream_host": "127.0.0.1", "upstream_port": echo[1]})
    check(s1["public_port"] == pool[0] and s2["public_port"] == pool[0],
          f"不同上游端口的两个普通会话共用公网端口 {pool[0]} "
          f"(got {s1['public_port']}, {s2['public_port']})")

    body1 = f"hello-A-{tag}"
    r1 = call("POST", f"/sessions/{s1['id']}/transmit", {"body": body1})
    check(r1["actual_send_port"] == pool[0],
          f"实际发包端口=公网端口 {pool[0]} (got {r1['actual_send_port']})")
    ports = {p["public_port"]: p for p in call("GET", "/ports")}
    check(ports[pool[0]]["bound"] and not ports[pool[1]]["bound"],
          "发包后仅占用端口被绑定, 空闲端口仍未绑定")

    body2 = f"hello-B-{tag}-二进制"
    r2 = call("POST", f"/sessions/{s2['id']}/transmit",
              {"body_hex": body2.encode().hex()})
    check(r2["bytes"] == len(body2.encode()), "十六进制正文按原始字节发出")

    got1 = wait_until(lambda: any(m["text"] == body1 for m in
                   call("GET", f"/endpoints/{eps[0]['id']}/messages")["messages"]))
    check(bool(got1), "会话1 正文原样回送到对应内网端点(UTF-8 一致)")
    got2 = wait_until(lambda: any(m["hex"] == body2.encode().hex() for m in
                   call("GET", f"/endpoints/{eps[1]['id']}/messages")["messages"]))
    check(bool(got2), "会话2 二进制正文原样交付(hex 一致)")

    disp = call("GET", "/dispatch")
    d0 = disp[0]
    check(set(d0) >= {"local_port", "src", "matched_session", "delivered"}
          and d0["local_port"] == pool[0] and d0["delivered"],
          "分发记录按 (来源地址, 源端口, 本地收包端口) 三元组可查")
    # 交叉验证: 两个会话的回包不会串到对方内网端点
    msgs1 = call("GET", f"/endpoints/{eps[0]['id']}/messages")["messages"]
    check(all(m["text"] != body2 or m["hex"] != body2.encode().hex()
              for m in msgs1), "回包严格按源端口分发, 不串会话")

    call("DELETE", f"/sessions/{s1['id']}")
    call("DELETE", f"/sessions/{s2['id']}")
    check(not any(p["bound"] for p in call("GET", "/ports")),
          "会话删除后公网端口解绑(空闲不绑定)")

    # -------------------------------------------------------------- Case B
    print("\n== B. 拒绝受理且不移动旧会话 ==")
    call("POST", "/round/setup",
         {"pool_ports": pool, "max_sessions_per_port": 1,
          "bind_host": "127.0.0.1"})
    a = call("POST", "/sessions", {"kind": "normal",
             "internal_endpoint_id": eps[2]["id"],
             "upstream_host": "127.0.0.1", "upstream_port": echo[0]})
    b = call("POST", "/sessions", {"kind": "normal",
             "internal_endpoint_id": eps[3]["id"],
             "upstream_host": "127.0.0.1", "upstream_port": echo[1]})
    c = call("POST", "/sessions", {"kind": "normal",
             "internal_endpoint_id": eps[4]["id"],
             "upstream_host": "127.0.0.1", "upstream_port": echo[2]})
    check({a["public_port"], b["public_port"], c["public_port"]} == set(pool),
          "容量 1 时三个会话各占一个公网端口")
    before = call("GET", "/ports")
    # 同上游端口 -> 与已有会话单点范围重叠; 端口池无合规位置
    rej = call("POST", "/sessions", {"kind": "normal",
               "internal_endpoint_id": eps[5]["id"],
               "upstream_host": "127.0.0.1", "upstream_port": echo[0]},
               expect_error=409)
    after = call("GET", "/ports")
    check(before == after, f"409 拒绝后端口分配完全不变 ({rej['_detail'][:60]}...)")
    # TFTP 未收首包前范围 1-65535, 与一切重叠, 同样无合规端口
    rej2 = call("POST", "/sessions", {"kind": "tftp",
                "internal_endpoint_id": eps[5]["id"],
                "upstream_host": "127.0.0.1", "upstream_port": tftp_p},
                expect_error=409)
    check(before == call("GET", "/ports"),
          f"TFTP 宽范围会话无合规端口时同样拒绝且不移动 ({rej2['_detail'][:50]}...)")

    # -------------------------------------------------------------- Case C
    print("\n== C. TFTP 首包暂扣 + 换源端口 + 锁定 ==")
    call("POST", "/round/setup",
         {"pool_ports": pool, "max_sessions_per_port": 2,
          "bind_host": "127.0.0.1"})
    t = call("POST", "/sessions", {"kind": "tftp",
             "internal_endpoint_id": eps[6]["id"],
             "upstream_host": "127.0.0.1", "upstream_port": tftp_p})
    check(t["accepted_source_port_range"] == "1-65535",
          "TFTP 首包前接受上游 IP 任意源端口 (1-65535)")
    check(t["public_port"] == pool[0], "宽范围会话独占一个公网端口")
    # 宽范围与一切重叠: 同端口不能再放第二个会话(即便上限是 2)
    blocked = call("POST", "/sessions", {"kind": "normal",
                   "internal_endpoint_id": eps[7]["id"],
                   "upstream_host": "127.0.0.1", "upstream_port": echo[0]})
    check(blocked["public_port"] != pool[0],
          f"未锁定的 TFTP 会话阻止同端口共用, 新会话落在 {blocked['public_port']}")

    tfb = f"tftp-read-{tag}".encode()
    sent_at = time.time()
    rr = call("POST", f"/sessions/{t['id']}/transmit",
              {"body_hex": tfb.hex()})
    check(rr["actual_send_port"] == pool[0], "TFTP 请求从公网端口真实发出")

    # 等待: 首包被暂扣 400ms, 然后上游从新 TID 端口回包
    def _pinned():
        s = call("GET", f"/sessions/{t['id']}")
        return s["pinned_source_port"]

    pin = wait_until(_pinned)
    sess_after = call("GET", f"/sessions/{t['id']}")
    check(sess_after["pinned_source_port"] is not None
          and sess_after["pinned_source_port"] != tftp_p,
          f"首包后锁定实收源端口 {sess_after['pinned_source_port']} "
          f"(!= 熟知端口 {tftp_p})")
    check(sess_after["accepted_source_port_range"]
          == str(sess_after["pinned_source_port"]),
          "接受范围收窄为锁定的单端口")
    check(time.time() - sent_at >= 0.35, "回包确实经历了上游暂扣")

    got = wait_until(lambda: any(m["hex"] == tfb.hex() for m in
                   call("GET", f"/endpoints/{eps[6]['id']}/messages")["messages"]))
    check(bool(got), "TFTP 换端口后的回包正文原样交付内网")

    up = call("GET", "/upstream/status")
    tid = [x["reply_source_port"] for x in up["tftp_tid_ports"]]
    check(sess_after["pinned_source_port"] in tid,
          f"引擎锁定端口与上游 TID {tid} 一致")

    # 锁定后范围收窄: 同公网端口可以再放一个不重叠的普通会话
    share = call("POST", "/sessions", {"kind": "normal",
                 "internal_endpoint_id": eps[7]["id"],
                 "upstream_host": "127.0.0.1", "upstream_port": echo[0]})
    check(share["public_port"] == sess_after["public_port"],
          "TFTP 锁定后新试传按收窄范围分配, 与普通会话同端口共用")

    # 锁定后, 从旧熟知端口/其他端口发来的杂包应被拒绝分发
    import socket as _s
    stray = _s.socket(_s.AF_INET, _s.SOCK_DGRAM)
    stray.sendto(b"stray-after-pin", ("127.0.0.1", sess_after["public_port"]))
    stray.close()
    time.sleep(0.3)
    last = call("GET", "/dispatch")[-1]
    check(last["matched_session"] is None and not last["delivered"],
          "锁定后非锁定端口的杂包无匹配会话, 不予分发")

    # -------------------------------------------------------------- Case D
    print("\n== D. 并发试传(不移动旧会话, 能受理则受理) ==")
    call("POST", "/round/setup",
         {"pool_ports": pool, "max_sessions_per_port": 2,
          "bind_host": "127.0.0.1"})
    import concurrent.futures
    def _concurrent_create(i):
        # 容量打满后 409 是预期行为; 受理与拒绝都视为合法返回
        data = json.dumps({
            "kind": "normal", "internal_endpoint_id": eps[i % 8]["id"],
            "upstream_host": "127.0.0.1",
            "upstream_port": echo[i % 3]}).encode()
        req = urllib.request.Request(
            BASE + "/sessions", data=data, method="POST",
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            if exc.code != 409:
                raise
            return {"_status": 409, "_detail": exc.read().decode()}

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        for i in range(10):
            futures.append(ex.submit(_concurrent_create, i))
    results = [f.result() for f in futures]
    accepted = [r for r in results if "_status" not in r]
    rejected = [r for r in results if r.get("_status") == 409]
    check(len(accepted) == 6 and len(rejected) == 4,
          f"3 端口 x 上限2 = 6 个受理, 4 个 409 (实得 {len(accepted)}/{len(rejected)})")
    occupancy = call("GET", "/ports")
    check(all(p["sessions"] <= 2 for p in occupancy), "无端口超出会话上限")
    check(all(not p["bound"] for p in occupancy),
          "会话只做分配、尚未试传: 公网端口全部保持未绑定(空闲不绑定)")
    # 任一受理会话试传后, 仅其所在端口懒绑定
    any_accepted = next(r for r in results if "_status" not in r)
    call("POST", f"/sessions/{any_accepted['id']}/transmit", {"body": "ping"})
    occupancy2 = call("GET", "/ports")
    used = any_accepted["public_port"]
    check(all(p["bound"] == (p["public_port"] == used) for p in occupancy2),
          f"试传后仅 {used} 懒绑定, 其余端口仍未绑定")

    print(f"\n==== 结果: {PASS} passed, {FAIL} failed ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
