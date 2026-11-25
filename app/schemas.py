"""Pydantic request models for the NAT drill API."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

PORT_MIN, PORT_MAX = 1, 65535


def _check_port(value: int) -> int:
    if not (PORT_MIN <= value <= PORT_MAX):
        raise ValueError(f"port must be in [{PORT_MIN}, {PORT_MAX}]")
    return value


class SetupReq(BaseModel):
    pool_ports: list[int] = Field(..., min_length=1, description="公网 UDP 端口池")
    max_sessions_per_port: int = Field(..., ge=1, description="每个公网端口会话上限")
    bind_host: str = Field("127.0.0.1", description="公网/模拟套接字绑定地址")

    @field_validator("pool_ports")
    @classmethod
    def _ports(cls, v: list[int]) -> list[int]:
        if len(v) != len(set(v)):
            raise ValueError("pool_ports must be unique")
        return [_check_port(p) for p in v]


class EndpointReq(BaseModel):
    host: str = Field("127.0.0.1")
    port: int

    @field_validator("port")
    @classmethod
    def _p(cls, v: int) -> int:
        return _check_port(v)


class UpstreamStartReq(BaseModel):
    host: str = Field("127.0.0.1", description="同一上游 IP")
    echo_ports: list[int] = Field(..., min_length=3, max_length=3,
                                  description="三个普通回显端口")
    tftp_port: int = Field(..., description="TFTP 样例监听端口(如 69 的模拟端口)")
    default_detain_ms: int = Field(300, ge=0, description="TFTP 首包暂扣毫秒数")

    @field_validator("echo_ports")
    @classmethod
    def _echo(cls, v: list[int]) -> list[int]:
        if len(set(v)) != 3:
            raise ValueError("echo_ports must be 3 distinct ports")
        return [_check_port(p) for p in v]

    @field_validator("tftp_port")
    @classmethod
    def _tftp(cls, v: int) -> int:
        return _check_port(v)


class SessionReq(BaseModel):
    kind: Literal["normal", "tftp"]
    internal_endpoint_id: str
    upstream_host: str
    upstream_port: int

    @field_validator("upstream_port")
    @classmethod
    def _p(cls, v: int) -> int:
        return _check_port(v)


class TransmitReq(BaseModel):
    body: str = Field("", description="试传正文(UTF-8)")
    body_hex: Optional[str] = Field(None, description="可选: 十六进制正文, 优先于 body")

    @field_validator("body_hex")
    @classmethod
    def _hex(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.replace(" ", "").replace("0x", "")
        if len(v) % 2 or any(c not in "0123456789abcdefABCDEF" for c in v):
            raise ValueError("body_hex is not valid hexadecimal")
        return v
