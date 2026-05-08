"""
Virtual collaborative client simulator.
Simulates N users joining rooms, editing docs, and moving cursors.
Used for load testing and integration testing without a browser.
"""
import asyncio
import random
import time
import uuid
from typing import List, Optional, Dict
from dataclasses import dataclass

import socketio


@dataclass
class SimClient:
    user_id: str
    display_name: str
    room_id: str
    sio: socketio.AsyncClient
    connected: bool = False
    messages_sent: int = 0
    messages_received: int = 0


class MeshClientSimulator:
    """
    Simulate a fleet of collaborative clients connecting to the Socket.io server.
    """

    def __init__(
        self,
        server_url: str = "http://localhost:8000",
        n_clients: int = 10,
        room_id: str = "sim-room",
        seed: int = 42,
    ):
        self.server_url = server_url
        self.n_clients = n_clients
        self.room_id = room_id
        self.rng = random.Random(seed)
        self.clients: List[SimClient] = []
        self.stats = {"total_ops": 0, "cursor_moves": 0, "doc_edits": 0, "errors": 0}

    async def _make_client(self, i: int) -> SimClient:
        sio = socketio.AsyncClient(logger=False, engineio_logger=False)
        user_id = f"sim_user_{i:03d}"
        display_name = f"User {i}"
        client = SimClient(user_id=user_id, display_name=display_name,
                           room_id=self.room_id, sio=sio)

        @sio.event
        async def connect():
            client.connected = True

        @sio.event
        async def disconnect():
            client.connected = False

        @sio.on("doc_op")
        async def on_doc_op(data):
            client.messages_received += 1

        @sio.on("cursor_update")
        async def on_cursor(data):
            client.messages_received += 1

        @sio.on("presence_join")
        async def on_join(data):
            client.messages_received += 1

        return client

    async def _run_client(self, client: SimClient, n_steps: int):
        """Simulate a single client: connect, join room, emit ops, disconnect."""
        try:
            await client.sio.connect(
                self.server_url,
                auth={"user_id": client.user_id},
                transports=["websocket"],
            )
            await client.sio.emit("join_room", {
                "room_id": client.room_id,
                "display_name": client.display_name,
            })
            await asyncio.sleep(0.1)

            for step in range(n_steps):
                op = self.rng.choice(["cursor", "doc_edit", "reaction"])

                if op == "cursor":
                    await client.sio.emit("cursor_move", {
                        "x": self.rng.uniform(0, 1200),
                        "y": self.rng.uniform(0, 800),
                        "line": self.rng.randint(0, 50),
                        "col": self.rng.randint(0, 80),
                    })
                    self.stats["cursor_moves"] += 1

                elif op == "doc_edit":
                    chars = "abcdefghijklmnopqrstuvwxyz "
                    content = "".join(self.rng.choices(chars, k=self.rng.randint(1, 10)))
                    await client.sio.emit("doc_op", {
                        "op_type": "insert",
                        "position": self.rng.randint(0, 100),
                        "content": content,
                        "version": step,
                    })
                    self.stats["doc_edits"] += 1

                elif op == "reaction":
                    await client.sio.emit("reaction", {
                        "emoji": self.rng.choice(["👍", "❤️", "🎉", "🚀", "💯"]),
                        "x": self.rng.uniform(0, 1),
                        "y": self.rng.uniform(0, 1),
                    })

                client.messages_sent += 1
                self.stats["total_ops"] += 1
                await asyncio.sleep(self.rng.uniform(0.05, 0.2))

            await client.sio.disconnect()

        except Exception as e:
            self.stats["errors"] += 1
            print(f"[Sim] Client {client.user_id} error: {e}")

    async def run(self, n_steps: int = 50) -> Dict:
        """Run all simulated clients concurrently."""
        print(f"[Sim] Starting {self.n_clients} clients in room '{self.room_id}'")
        self.clients = [await self._make_client(i) for i in range(self.n_clients)]
        start = time.time()
        await asyncio.gather(*[self._run_client(c, n_steps) for c in self.clients])
        elapsed = time.time() - start
        total_msgs = sum(c.messages_sent for c in self.clients)
        self.stats["elapsed_sec"] = round(elapsed, 2)
        self.stats["msgs_per_sec"] = round(total_msgs / max(elapsed, 0.001), 1)
        print(f"[Sim] Done. {total_msgs} ops in {elapsed:.1f}s ({self.stats['msgs_per_sec']} ops/sec)")
        print(f"[Sim] Cursor moves: {self.stats['cursor_moves']} | Doc edits: {self.stats['doc_edits']} | Errors: {self.stats['errors']}")
        return self.stats
