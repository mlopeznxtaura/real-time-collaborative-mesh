"""
NATS event bus — the backbone of the collaborative mesh.
Every user action publishes a message. All connected clients receive it.
SDKs: nats-py
"""
import asyncio
import json
import time
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass, asdict

import nats
from nats.aio.client import Client as NATS


@dataclass
class MeshEvent:
    event_id: str
    event_type: str      # "cursor_move", "doc_edit", "presence_join", "reaction", etc.
    user_id: str
    room_id: str
    payload: Dict[str, Any]
    timestamp: float = 0.0

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.time()

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @classmethod
    def from_json(cls, data: bytes) -> "MeshEvent":
        return cls(**json.loads(data))


class NATSEventBus:
    """
    NATS-backed event bus for the real-time collaborative mesh.
    Subject structure: mesh.<room_id>.<event_type>
    All subscribers to a room receive all events in that room.
    """

    def __init__(self, servers: str = "nats://localhost:4222"):
        self.servers = servers
        self.nc: Optional[NATS] = None
        self._subscriptions: Dict[str, Any] = {}
        self._handlers: Dict[str, List[Callable]] = {}

    async def connect(self):
        self.nc = await nats.connect(
            self.servers,
            reconnect_time_wait=2,
            max_reconnect_attempts=10,
            error_cb=self._on_error,
            closed_cb=self._on_closed,
            disconnected_cb=self._on_disconnected,
            reconnected_cb=self._on_reconnected,
        )
        print(f"[NATS] Connected to {self.servers}")

    async def disconnect(self):
        if self.nc:
            await self.nc.close()

    async def publish(self, event: MeshEvent):
        """Publish an event to the room channel."""
        subject = f"mesh.{event.room_id}.{event.event_type}"
        await self.nc.publish(subject, event.to_json())

    async def subscribe_room(
        self,
        room_id: str,
        handler: Callable[[MeshEvent], None],
        event_type: str = "*",
    ) -> Any:
        """
        Subscribe to all events in a room (or a specific event type).
        Uses NATS wildcard subjects: mesh.<room_id>.*
        """
        subject = f"mesh.{room_id}.{event_type}"
        sub_key = f"{room_id}:{event_type}"

        async def _handler(msg):
            event = MeshEvent.from_json(msg.data)
            await handler(event) if asyncio.iscoroutinefunction(handler) else handler(event)

        sub = await self.nc.subscribe(subject, cb=_handler)
        self._subscriptions[sub_key] = sub
        print(f"[NATS] Subscribed: {subject}")
        return sub

    async def request_reply(
        self, subject: str, payload: Dict, timeout: float = 2.0
    ) -> Optional[Dict]:
        """Request-reply pattern for synchronous operations over NATS."""
        msg = await self.nc.request(subject, json.dumps(payload).encode(), timeout=timeout)
        return json.loads(msg.data)

    async def unsubscribe_room(self, room_id: str, event_type: str = "*"):
        sub_key = f"{room_id}:{event_type}"
        if sub_key in self._subscriptions:
            await self._subscriptions[sub_key].unsubscribe()
            del self._subscriptions[sub_key]

    async def get_room_stats(self, room_id: str) -> Dict:
        """Query NATS JetStream for room message counts."""
        try:
            js = self.nc.jetstream()
            stream_info = await js.stream_info(f"MESH_{room_id.upper()}")
            return {
                "messages": stream_info.state.messages,
                "bytes": stream_info.state.bytes,
                "consumers": stream_info.state.consumer_count,
            }
        except Exception:
            return {}

    async def _on_error(self, e):
        print(f"[NATS] Error: {e}")

    async def _on_closed(self):
        print("[NATS] Connection closed")

    async def _on_disconnected(self):
        print("[NATS] Disconnected")

    async def _on_reconnected(self):
        print("[NATS] Reconnected")


class CollaborationRoom:
    """
    High-level room abstraction built on top of NATSEventBus.
    Tracks presence, broadcasts ops, and maintains event log.
    """

    def __init__(self, room_id: str, bus: NATSEventBus):
        self.room_id = room_id
        self.bus = bus
        self.members: Dict[str, Dict] = {}   # user_id -> {name, cursor, joined_at}
        self.event_log: List[MeshEvent] = []
        self._max_log = 1000

    async def join(self, user_id: str, display_name: str = ""):
        self.members[user_id] = {
            "name": display_name or user_id,
            "joined_at": time.time(),
            "cursor": None,
            "active": True,
        }
        event = MeshEvent(
            event_id=f"join_{user_id}_{int(time.time()*1000)}",
            event_type="presence_join",
            user_id=user_id,
            room_id=self.room_id,
            payload={"display_name": display_name, "member_count": len(self.members)},
        )
        await self.bus.publish(event)
        print(f"[Room {self.room_id}] {user_id} joined ({len(self.members)} members)")

    async def leave(self, user_id: str):
        if user_id in self.members:
            del self.members[user_id]
        event = MeshEvent(
            event_id=f"leave_{user_id}_{int(time.time()*1000)}",
            event_type="presence_leave",
            user_id=user_id,
            room_id=self.room_id,
            payload={"member_count": len(self.members)},
        )
        await self.bus.publish(event)

    async def broadcast(self, user_id: str, event_type: str, payload: Dict):
        """Send an event from a user to the entire room."""
        event = MeshEvent(
            event_id=f"{event_type}_{user_id}_{int(time.time()*1000)}",
            event_type=event_type,
            user_id=user_id,
            room_id=self.room_id,
            payload=payload,
        )
        self._log(event)
        await self.bus.publish(event)

    def _log(self, event: MeshEvent):
        self.event_log.append(event)
        if len(self.event_log) > self._max_log:
            self.event_log = self.event_log[-self._max_log:]
