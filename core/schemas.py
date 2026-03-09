"""Pydantic request/response schemas."""

from typing import List, Literal

from pydantic import BaseModel, Field


class WalletGenerateRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=100)


class PanicSellRequest(BaseModel):
    wallet_address: str
    token: str
    slippage: int = Field(default=3, ge=1, le=99)


class TransferRequest(BaseModel):
    to_address: str
    token: str = ""          # empty / "BNB" → native BNB; otherwise ERC-20 contract address
    amount: float = Field(gt=0)


class WalletRenameRequest(BaseModel):
    name: str = Field(min_length=1, max_length=64)


class ListenerTaskCreateRequest(BaseModel):
    target_address: str
    platforms: List[str] = Field(default_factory=lambda: ["fourmeme", "flap"])
    label: str = Field(default="", max_length=64)
    chain: Literal["bsc"] = "bsc"
    config: dict = Field(default_factory=dict)


class ListenerTaskRenameRequest(BaseModel):
    label: str = Field(min_length=1, max_length=64)


class SmartCondition(BaseModel):
    min_bnb: float = Field(gt=0)
    amount: float = Field(gt=0)


class CopyTaskCreateRequest(BaseModel):
    listener_task_id: int = Field(ge=1)
    wallet_id: int = Field(ge=1)
    buy_mode: Literal["fixed", "smart"]
    buy_config: dict = Field(default_factory=dict)
    sell_mode: Literal["copy_sell", "tp_sl", "both"]
    sell_config: dict = Field(default_factory=dict)
    gas_multiplier: float = Field(default=1.2, ge=1.0, le=3.0)


class StrategyTaskCreateRequest(BaseModel):
    wallet_id: int = Field(ge=1)
    token: str
    take_profit: float | None = None
    stop_loss: float | None = None
    config: dict = Field(default_factory=dict)


class TaskStatusUpdateRequest(BaseModel):
    status: Literal["pending", "running", "paused", "interrupted"]


class RpcConfigCreateRequest(BaseModel):
    chain: Literal["bsc"] = "bsc"
    label: str = ""
    rpc_url: str
    ws_url: str = ""
    chain_id: int = 56
