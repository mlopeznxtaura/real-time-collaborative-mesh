"""
Upstash Redis presence tracking and edge cache.
Track who is online, in which room, and their last activity.
Sub-5ms globally via Upstash edge network.
SDKs: redis (Upstash compatible), FastAPI
"""
import os
import json
import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

import redis


@dataclass
class UserPresence:
    user_id: str
    room_id: str
    display_name: str
    last_seen: float
    cursor: Optional[Dict] = None
    status: str = "online"    # online, idle, offline


PRESENCE_TTL = 30       # seconds — user is "online" if seen within this window
IDLE_THRESHOLD = 10     # seconds without activity = idle


class RedisPresenceStore:
    """
    Edge presence store backed by Redis (Upstash compatible).
    All operations are O(1) or O(n_room_members).
    """

    def __init__(
        self,
        url: Optional[str] = None,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        decode_responses: bool = True,
    ):
        redis_url = url or os.environ.get("UPSTASH_REDIS_URL") or os.environ.get("REDIS_URL")
        if redis_url:
            self.r = redis.from_url(redis_url, decode_responses=decode_responses)
        else:
            self.r = redis.Redis(host=host, port=port, password=password,
                                  decode_responses=decode_responses)
        try:
            self.r.ping()
            print("[Redis] Presence store connected")
        except Exception as e:
            print(f"[Redis] Connection failed: {e}. Using in-memory fallback.")
            self._fallback: Dict[str, Any] = {}
            self.r = None

    def _key(self, namespace: str, *parts: str) -> str:
        return ":".join(["presence", namespace] + list(parts))

    def set_presence(self, presence: UserPresence):
        """Mark a user as online in a room."""
        key = self._key("user", presence.user_id)
        data = {
            "user_id": presence.user_id,
            "room_id": presence.room_id,
            "display_name": presence.display_name,
            "last_seen": presence.last_seen,
            "status": presence.status,
        }
        if presence.cursor:
            data["cursor"] = json.dumps(presence.cursor)

        if self.r:
            self.r.hset(key, mapping=data)
            self.r.expire(key, PRESENCE_TTL)
            self.r.sadd(self._key("room", presence.room_id, "members"), presence.user_id)
            self.r.expire(self._key("room", presence.room_id, "members"), PRESENCE_TTL)
        else:
            self._fallback[key] = data

    def get_presence(self, user_id: str) -> Optional[UserPresence]:
        """Get a user's current presence."""
        key = self._key("user", user_id)
        if self.r:
            data = self.r.hgetall(key)
        else:
            data = self._fallback.get(key, {})

        if not data:
            return None
        return UserPresence(
            user_id=data["user_id"],
            room_id=data.get("room_id", ""),
            display_name=data.get("display_name", user_id),
            last_seen=float(data.get("last_seen", 0)),
            status=data.get("status", "online"),
        )

    def get_room_members(self, room_id: str) -> List[UserPresence]:
        """Get all online members in a room."""
        if self.r:
            member_ids = self.r.smembers(self._key("room", room_id, "members"))
        else:
            member_ids = {uid for uid, data in self._fallback.items()
                         if data.get("room_id") == room_id}

        members = []
        for uid in member_ids:
            p = self.get_presence(uid)
            if p and time.time() - p.last_seen < PRESENCE_TTL:
                p.status = "idle" if time.time() - p.last_seen > IDLE_THRESHOLD else "online"
                members.append(p)
        return members

    def update_heartbeat(self, user_id: str):
        """Update last_seen for a user (call on any activity)."""
        key = self._key("user", user_id)
        if self.r:
            self.r.hset(key, "last_seen", time.time())
            self.r.expire(key, PRESENCE_TTL)
        elif key in self._fallback:
            self._fallback[key]["last_seen"] = time.time()

    def leave_room(self, user_id: str, room_id: str):
        """Remove user from room presence."""
        if self.r:
            self.r.srem(self._key("room", room_id, "members"), user_id)
            self.r.delete(self._key("user", user_id))
        else:
            self._fallback.pop(self._key("user", user_id), None)

    def get_active_rooms(self) -> List[str]:
        """List all rooms with active members."""
        if self.r:
            keys = self.r.keys(self._key("room", "*", "members"))
            return [k.split(":")[2] for k in keys]
        return list({data.get("room_id", "") for data in self._fallback.values() if data.get("room_id")})

    def set_edge_cache(self, key: str, value: Any, ttl: int = 60):
        """Store any value in Redis as edge cache."""
        cache_key = f"cache:{key}"
        serialized = json.dumps(value) if not isinstance(value, str) else value
        if self.r:
            self.r.set(cache_key, serialized, ex=ttl)
        else:
            self._fallback[cache_key] = serialized

    def get_edge_cache(self, key: str) -> Optional[Any]:
        """Retrieve from edge cache."""
        cache_key = f"cache:{key}"
        if self.r:
            val = self.r.get(cache_key)
        else:
            val = self._fallback.get(cache_key)
        if val is None:
            return None
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return val
