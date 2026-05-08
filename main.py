"""
real-time-collaborative-mesh — Entry Point

Real-time collaborative infrastructure: Socket.io server, NATS bus,
presence tracking, LiveKit rooms, and client simulation.

Usage:
  python main.py --mode server              (start Socket.io + REST API)
  python main.py --mode simulate --clients 10 --steps 50
  python main.py --mode nats                (start NATS event bus demo)
"""
import argparse
import asyncio
import sys


def parse_args():
    parser = argparse.ArgumentParser(description="Real-Time Collaborative Mesh")
    parser.add_argument("--mode", required=True,
                        choices=["server", "simulate", "nats"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--steps", type=int, default=50)
    parser.add_argument("--room", default="main-room")
    parser.add_argument("--nats-url", default="nats://localhost:4222")
    return parser.parse_args()


def mode_server(args):
    import uvicorn
    from backend.socketio_server import socket_app
    print(f"[Server] Starting on {args.host}:{args.port}")
    uvicorn.run(socket_app, host=args.host, port=args.port, log_level="info")


async def mode_simulate(args):
    from simulation.client_simulator import MeshClientSimulator
    sim = MeshClientSimulator(
        server_url=f"http://{args.host}:{args.port}",
        n_clients=args.clients,
        room_id=args.room,
    )
    stats = await sim.run(n_steps=args.steps)
    print(f"
Simulation complete: {stats}")


async def mode_nats(args):
    from backend.nats_bus import NATSEventBus, CollaborationRoom
    bus = NATSEventBus(servers=args.nats_url)
    try:
        await bus.connect()
    except Exception as e:
        print(f"[NATS] Could not connect to {args.nats_url}: {e}")
        print("[NATS] Start NATS server: docker run -p 4222:4222 nats:latest")
        return

    room = CollaborationRoom(room_id=args.room, bus=bus)
    received = []

    async def handler(event):
        received.append(event)
        print(f"  [{event.event_type}] from {event.user_id}: {event.payload}")

    await bus.subscribe_room(args.room, handler)
    await room.join("alice", "Alice")
    await room.join("bob", "Bob")
    await room.broadcast("alice", "doc_edit", {"content": "Hello world", "position": 0})
    await room.broadcast("bob", "cursor_move", {"x": 150, "y": 200})
    await room.broadcast("alice", "reaction", {"emoji": "👍"})
    await asyncio.sleep(0.5)
    await room.leave("bob")
    await bus.disconnect()
    print(f"
[NATS] Demo complete. {len(received)} events received.")


def main():
    args = parse_args()
    print("=" * 60)
    print("  Real-Time Collaborative Mesh")
    print(f"  Mode: {args.mode.upper()}")
    print("=" * 60)

    if args.mode == "server":
        mode_server(args)
    elif args.mode == "simulate":
        asyncio.run(mode_simulate(args))
    elif args.mode == "nats":
        asyncio.run(mode_nats(args))


if __name__ == "__main__":
    main()
