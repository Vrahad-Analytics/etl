"""
Airbyte Protocol Message Models & Specifications.

Implements the standard Airbyte data protocol:
- AirbyteMessage types: RECORD, STATE, LOG, SPEC, CONNECTION_STATUS
- Sync modes: full_refresh, incremental
- Destination sync modes: overwrite, append, deduped
- Standard serialization / deserialization for CLI & stream communication
"""

import json
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class SyncMode(str, Enum):
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


class DestinationSyncMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    DEDUPED = "deduped"


class MessageType(str, Enum):
    RECORD = "RECORD"
    STATE = "STATE"
    LOG = "LOG"
    SPEC = "SPEC"
    CONNECTION_STATUS = "CONNECTION_STATUS"


class Status(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass
class AirbyteRecordMessage:
    stream: str
    data: Dict[str, Any]
    emitted_at: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class AirbyteStateMessage:
    data: Dict[str, Any]
    stream: Optional[str] = None


@dataclass
class AirbyteLogMessage:
    level: str
    message: str


@dataclass
class AirbyteConnectionStatus:
    status: Status
    message: Optional[str] = None


@dataclass
class AirbyteMessage:
    type: MessageType
    record: Optional[AirbyteRecordMessage] = None
    state: Optional[AirbyteStateMessage] = None
    log: Optional[AirbyteLogMessage] = None
    connectionStatus: Optional[AirbyteConnectionStatus] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {"type": self.type.value if isinstance(self.type, MessageType) else str(self.type)}
        if self.record:
            result["record"] = asdict(self.record)
        if self.state:
            result["state"] = asdict(self.state)
        if self.log:
            result["log"] = asdict(self.log)
        if self.connectionStatus:
            result["connectionStatus"] = {
                "status": self.connectionStatus.status.value
                if isinstance(self.connectionStatus.status, Status)
                else str(self.connectionStatus.status),
                "message": self.connectionStatus.message,
            }
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AirbyteMessage":
        msg_type = MessageType(data["type"])
        record = None
        state = None
        log = None
        status = None

        if "record" in data and data["record"]:
            r = data["record"]
            record = AirbyteRecordMessage(
                stream=r["stream"],
                data=r["data"],
                emitted_at=r.get("emitted_at", int(time.time() * 1000)),
            )
        if "state" in data and data["state"]:
            s = data["state"]
            state = AirbyteStateMessage(data=s["data"], stream=s.get("stream"))
        if "log" in data and data["log"]:
            l = data["log"]
            log = AirbyteLogMessage(level=l.get("level", "INFO"), message=l.get("message", ""))
        if "connectionStatus" in data and data["connectionStatus"]:
            cs = data["connectionStatus"]
            status = AirbyteConnectionStatus(
                status=Status(cs["status"]),
                message=cs.get("message"),
            )

        return cls(
            type=msg_type,
            record=record,
            state=state,
            log=log,
            connectionStatus=status,
        )


def make_record(stream: str, data: Dict[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.RECORD,
        record=AirbyteRecordMessage(stream=stream, data=data),
    )


def make_state(state_data: Dict[str, Any], stream: Optional[str] = None) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.STATE,
        state=AirbyteStateMessage(data=state_data, stream=stream),
    )


def make_log(level: str, message: str) -> AirbyteMessage:
    return AirbyteMessage(
        type=MessageType.LOG,
        log=AirbyteLogMessage(level=level, message=message),
    )
