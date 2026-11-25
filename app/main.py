"""FastAPI entry point for the local NAT drill."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .engine import NoCompliantPort, NatEngine
from .schemas import EndpointReq, SessionReq, SetupReq, TransmitReq, UpstreamStartReq
from .upstream import UpstreamSim

engine = NatEngine()
upstream = UpstreamSim()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    upstream.stop()
    engine.shutdown()


app = FastAPI(
    title="园区网关 UDP / TFTP 本地 NAT 演练",
    description=(
        "真实 UDP 收发的本地 NAT 演练。公网端口池 + 每端口会话上限; "
        "普通会话只认原上游端点, TFTP 首包后只认实收源端口; "
        "同端口会话回包范围不重叠; 不移动旧会话; 空闲公网端口不绑定。"
        "交互式文档见 /docs。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# --------------------------------------------------------------------- round
@app.post("/round/setup", tags=["1. 演练设置"])
def setup_round(req: SetupReq):
    """设置公网端口池与每端口会话上限(会重置本轮会话与绑定)。"""
    engine.configure(req.pool_ports, req.max_sessions_per_port, req.bind_host)
    return {"ok": True,
            "pool_ports": req.pool_ports,
            "max_sessions_per_port": req.max_sessions_per_port,
            "bind_host": req.bind_host}


@app.post("/round/reset", tags=["1. 演练设置"])
def reset_round():
    """重置一轮: 清会话、释放全部公网 UDP 绑定、清分发记录。"""
    engine.reset_round()
    return {"ok": True}


@app.get("/round/status", tags=["1. 演练设置"])
def round_status():
    return {
        "pool_ports": engine.pool,
        "max_sessions_per_port": engine.max_per_port,
        "bind_host": engine.bind_host,
        "ports": engine.port_status(),
    }


# ------------------------------------------------------------------ upstream
@app.post("/upstream/start", tags=["2. 样例上游"])
def start_upstream(req: UpstreamStartReq):
    """在同一上游 IP 启动三个回显端口和一个 TFTP 样例。"""
    try:
        upstream.start(req.host, req.echo_ports, req.tftp_port,
                       req.default_detain_ms)
    except OSError as exc:
        raise HTTPException(500, f"upstream bind failed: {exc}")
    return upstream.status()


@app.post("/upstream/stop", tags=["2. 样例上游"])
def stop_upstream():
    upstream.stop()
    return {"ok": True}


@app.get("/upstream/status", tags=["2. 样例上游"])
def upstream_status():
    """可查 TFTP 实际换用的回包源端口(TID)。"""
    return upstream.status()


# ----------------------------------------------------------------- endpoints
@app.post("/endpoints", tags=["3. 模拟内网端点"])
def add_endpoint(req: EndpointReq):
    """创建模拟内网端点(真实 UDP 套接字, 接收分发后的正文)。"""
    try:
        ep = engine.add_endpoint(req.host, req.port)
    except OSError as exc:
        raise HTTPException(500, f"endpoint bind failed: {exc}")
    return {"id": ep.id, "host": ep.host, "port": ep.port}


@app.get("/endpoints", tags=["3. 模拟内网端点"])
def list_endpoints():
    with engine.lock:
        return [
            {"id": ep.id, "host": ep.host, "port": ep.port,
             "bound": True,
             "messages": list(ep.messages),
             "events": ep.events}
            for ep in engine.endpoints.values()
        ]


@app.get("/endpoints/{endpoint_id}/messages", tags=["3. 模拟内网端点"])
def endpoint_messages(endpoint_id: str):
    with engine.lock:
        ep = engine.endpoints.get(endpoint_id)
        if ep is None:
            raise HTTPException(404, "unknown endpoint id")
        return {"id": ep.id, "messages": list(ep.messages)}


@app.delete("/endpoints/{endpoint_id}", tags=["3. 模拟内网端点"])
def delete_endpoint(endpoint_id: str):
    try:
        engine.remove_endpoint(endpoint_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    return {"ok": True}


# ------------------------------------------------------------------ sessions
@app.post("/sessions", tags=["4. 会话"], status_code=201)
def create_session(req: SessionReq):
    """受理新会话。无需移动旧会话且存在合规端口才受理, 否则 409, 原分配不变。"""
    before = engine.port_status()
    try:
        sess = engine.create_session(
            req.kind, req.internal_endpoint_id,
            req.upstream_host, req.upstream_port)
    except NoCompliantPort as exc:
        after = engine.port_status()
        assert before == after  # 拒绝时原分配照旧
        raise HTTPException(409, str(exc))
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    return sess.to_dict()


@app.get("/sessions", tags=["4. 会话"])
def list_sessions():
    return engine.list_sessions()


@app.get("/sessions/{session_id}", tags=["4. 会话"])
def get_session(session_id: str):
    try:
        return engine.get_session(session_id).to_dict()
    except ValueError as exc:
        raise HTTPException(404, str(exc))


@app.delete("/sessions/{session_id}", tags=["4. 会话"])
def delete_session(session_id: str):
    try:
        engine.delete_session(session_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    return {"ok": True}


# ----------------------------------------------------------------- transmit
@app.post("/sessions/{session_id}/transmit", tags=["5. 正文试传"])
def transmit(session_id: str, req: TransmitReq):
    """真实 UDP 发送。可查实际发包端口; 回包由引擎按三元组分发。"""
    if req.body_hex is not None:
        payload = bytes.fromhex(req.body_hex)
    else:
        payload = req.body.encode("utf-8")
    try:
        return engine.transmit(session_id, payload)
    except ValueError as exc:
        raise HTTPException(404, str(exc))


# ------------------------------------------------------------------ queries
@app.get("/ports", tags=["6. 查询"])
def port_occupancy():
    """各公网端口占用情况与是否已实际绑定(空闲端口不绑定)。"""
    return engine.port_status()


@app.get("/dispatch", tags=["6. 查询"])
def dispatch_results():
    """分发结果: 来源地址、源端口、本地收包端口、匹配会话、是否交付。"""
    return engine.dispatch_results()
