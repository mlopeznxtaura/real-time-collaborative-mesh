"""
Socket.io server with collaborative rooms and namespaces.
Handles real-time document editing, cursor sync, and presence.
SDKs: python-socketio, FastAPI
"""
import asyncio
import json
import time
import uuid
from typing import Dict, Any, Optional, Set
from dataclasses import dataclass, field

import socketio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


# Socket.io server with async support
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
)

app = FastAPI(title="Real-Time Collaborative Mesh")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ASGI app combining FastAPI + Socket.io
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

# In-memory state
rooms: Dict[str, Dict] = {}          # room_id -> {doc, cursors, members}
sessions: Dict[str, Dict] = {}       # sid -> {user_id, room_id, display_name}


def get_or_create_room(room_id: str) -> Dict:
    if room_id not in rooms:
        rooms[room_id] = {
            "doc": {"content": "", "version": 0},
            "cursors": {},
            "members": {},
            "created_at": time.time(),
        }
    return rooms[room_id]


# ---- Socket.io event handlers ----

@sio.event
async def connect(sid, environ, auth=None):
    user_id = auth.get("user_id", f"anon_{sid[:8]}") if auth else f"anon_{sid[:8]}"
    sessions[sid] = {"user_id": user_id, "room_id": None, "display_name": user_id}
    print(f"[Socket.io] Connect: {sid} ({user_id})")


@sio.event
async def disconnect(sid):
    session = sessions.pop(sid, {})
    room_id = session.get("room_id")
    user_id = session.get("user_id")
    if room_id and room_id in rooms:
        rooms[room_id]["members"].pop(user_id, None)
        rooms[room_id]["cursors"].pop(user_id, None)
        await sio.emit("presence_leave", {"user_id": user_id}, room=room_id, skip_sid=sid)
    print(f"[Socket.io] Disconnect: {sid} ({user_id})")


@sio.event
async def join_room(sid, data):
    """Client joins a collaborative room."""
    room_id = data.get("room_id", "default")
    display_name = data.get("display_name", sessions[sid]["user_id"])
    user_id = sessions[sid]["user_id"]

    room = get_or_create_room(room_id)
    sessions[sid]["room_id"] = room_id
    sessions[sid]["display_name"] = display_name
    room["members"][user_id] = {"display_name": display_name, "joined_at": time.time()}

    await sio.enter_room(sid, room_id)

    # Send current doc state to new joiner
    await sio.emit("room_state", {
        "doc": room["doc"],
        "cursors": room["cursors"],
        "members": room["members"],
        "member_count": len(room["members"]),
    }, to=sid)

    # Announce to others
    await sio.emit("presence_join", {
        "user_id": user_id,
        "display_name": display_name,
        "member_count": len(room["members"]),
    }, room=room_id, skip_sid=sid)

    print(f"[Socket.io] {user_id} joined room {room_id} ({len(room['members'])} members)")


@sio.event
async def doc_op(sid, data):
    """
    Operational transformation: apply a document operation.
    data: {op_type, position, content, version}
    """
    session = sessions.get(sid, {})
    room_id = session.get("room_id")
    user_id = session.get("user_id")
    if not room_id or room_id not in rooms:
        return

    room = rooms[room_id]
    op_type = data.get("op_type", "insert")  # "insert", "delete", "replace"
    position = data.get("position", 0)
    content = data.get("content", "")
    client_version = data.get("version", 0)

    # Simple last-write-wins OT (production would use real OT/CRDT)
    doc = room["doc"]
    if op_type == "insert":
        text = doc["content"]
        doc["content"] = text[:position] + content + text[position:]
    elif op_type == "delete":
        length = data.get("length", len(content))
        text = doc["content"]
        doc["content"] = text[:position] + text[position + length:]
    elif op_type == "replace":
        doc["content"] = content

    doc["version"] += 1

    # Broadcast op to all other room members
    await sio.emit("doc_op", {
        "user_id": user_id,
        "op_type": op_type,
        "position": position,
        "content": content,
        "version": doc["version"],
        "timestamp": time.time(),
    }, room=room_id, skip_sid=sid)


@sio.event
async def cursor_move(sid, data):
    """Sync cursor position across all clients in room."""
    session = sessions.get(sid, {})
    room_id = session.get("room_id")
    user_id = session.get("user_id")
    if not room_id or room_id not in rooms:
        return

    cursor = {"x": data.get("x", 0), "y": data.get("y", 0),
              "line": data.get("line"), "col": data.get("col")}
    rooms[room_id]["cursors"][user_id] = cursor

    await sio.emit("cursor_update", {
        "user_id": user_id,
        "cursor": cursor,
        "display_name": session.get("display_name", user_id),
    }, room=room_id, skip_sid=sid)


@sio.event
async def reaction(sid, data):
    """Broadcast emoji reaction to room."""
    session = sessions.get(sid, {})
    room_id = session.get("room_id")
    user_id = session.get("user_id")
    if not room_id:
        return
    await sio.emit("reaction", {
        "user_id": user_id,
        "emoji": data.get("emoji", "👍"),
        "x": data.get("x", 0.5),
        "y": data.get("y", 0.5),
        "timestamp": time.time(),
    }, room=room_id)


# ---- REST endpoints ----

@app.get("/rooms")
async def list_rooms():
    return {
        rid: {
            "member_count": len(r["members"]),
            "doc_version": r["doc"]["version"],
            "doc_length": len(r["doc"]["content"]),
        }
        for rid, r in rooms.items()
    }

@app.get("/rooms/{room_id}")
async def get_room(room_id: str):
    if room_id not in rooms:
        return {"error": "Room not found"}
    room = rooms[room_id]
    return {
        "room_id": room_id,
        "members": room["members"],
        "doc": room["doc"],
        "cursor_count": len(room["cursors"]),
    }

@app.get("/health")
async def health():
    return {"status": "ok", "rooms": len(rooms), "sessions": len(sessions)}
