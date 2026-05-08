"""
LiveKit room management and JWT token generation.
WebRTC audio/video/screen share for collaborative sessions.
SDKs: livekit, livekit-api
"""
import os
import time
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

try:
    from livekit.api import LiveKitAPI, AccessToken, VideoGrants, ListRoomsRequest, DeleteRoomRequest
    LIVEKIT_AVAILABLE = True
except ImportError:
    LIVEKIT_AVAILABLE = False
    print("Warning: livekit not available. Install: pip install livekit livekit-api")


@dataclass
class RoomToken:
    token: str
    room_name: str
    participant_identity: str
    expires_at: float


class LiveKitRoomManager:
    """
    Manage LiveKit rooms for real-time video/audio collaboration.
    Issues JWT tokens, creates/deletes rooms, and lists participants.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        host: str = "ws://localhost:7880",
    ):
        self.api_key = api_key or os.environ.get("LIVEKIT_API_KEY", "devkey")
        self.api_secret = api_secret or os.environ.get("LIVEKIT_API_SECRET", "secret")
        self.host = host
        if LIVEKIT_AVAILABLE:
            self.api = LiveKitAPI(host, self.api_key, self.api_secret)
        print(f"[LiveKit] Manager initialized | host={host}")

    def create_token(
        self,
        room_name: str,
        participant_identity: str,
        participant_name: str = "",
        can_publish: bool = True,
        can_subscribe: bool = True,
        can_publish_data: bool = True,
        ttl_seconds: int = 3600,
    ) -> RoomToken:
        """
        Generate a LiveKit JWT token for a participant to join a room.
        Tokens are scoped to a specific room and identity.
        """
        if not LIVEKIT_AVAILABLE:
            # Stub token for dev without LiveKit
            import base64
            stub = base64.b64encode(
                f"{self.api_key}:{room_name}:{participant_identity}:{int(time.time()+ttl_seconds)}".encode()
            ).decode()
            return RoomToken(
                token=stub,
                room_name=room_name,
                participant_identity=participant_identity,
                expires_at=time.time() + ttl_seconds,
            )

        token = AccessToken(self.api_key, self.api_secret)
        token.with_identity(participant_identity)
        token.with_name(participant_name or participant_identity)
        token.with_grants(VideoGrants(
            room_join=True,
            room=room_name,
            can_publish=can_publish,
            can_subscribe=can_subscribe,
            can_publish_data=can_publish_data,
        ))
        token.with_ttl(ttl_seconds)

        jwt = token.to_jwt()
        return RoomToken(
            token=jwt,
            room_name=room_name,
            participant_identity=participant_identity,
            expires_at=time.time() + ttl_seconds,
        )

    async def list_rooms(self) -> List[Dict]:
        """List all active LiveKit rooms."""
        if not LIVEKIT_AVAILABLE:
            return []
        rooms = await self.api.room.list_rooms(ListRoomsRequest())
        return [
            {
                "name": r.name,
                "sid": r.sid,
                "num_participants": r.num_participants,
                "creation_time": r.creation_time,
                "active_recording": r.active_recording,
            }
            for r in rooms.rooms
        ]

    async def delete_room(self, room_name: str):
        """Delete a LiveKit room and disconnect all participants."""
        if not LIVEKIT_AVAILABLE:
            return
        await self.api.room.delete_room(DeleteRoomRequest(room=room_name))
        print(f"[LiveKit] Room deleted: {room_name}")

    async def get_participants(self, room_name: str) -> List[Dict]:
        """Get participants in a room."""
        if not LIVEKIT_AVAILABLE:
            return []
        from livekit.api import ListParticipantsRequest
        resp = await self.api.room.list_participants(
            ListParticipantsRequest(room=room_name)
        )
        return [
            {
                "identity": p.identity,
                "name": p.name,
                "state": p.state,
                "joined_at": p.joined_at,
                "is_publisher": p.is_publisher,
            }
            for p in resp.participants
        ]

    def create_collab_session(
        self,
        room_id: str,
        participants: List[str],
        viewer_only: bool = False,
    ) -> List[RoomToken]:
        """Issue tokens for all participants in a collaborative session."""
        tokens = []
        for uid in participants:
            token = self.create_token(
                room_name=room_id,
                participant_identity=uid,
                can_publish=not viewer_only,
                can_subscribe=True,
            )
            tokens.append(token)
        print(f"[LiveKit] Created {len(tokens)} tokens for room {room_id}")
        return tokens
