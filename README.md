# Real-Time Collaborative Mesh

Cluster 13 of the NextAura 500 SDKs / 25 Clusters project.

Multiplayer-native application infrastructure — no backend required. Every user action is an event. State is synchronized across clients in real time.

## Architecture

- NATS for ultra-low-latency event pub/sub (every action is a message)
- Supabase Realtime for reactive database subscriptions
- LiveKit for WebRTC media rooms (audio/video/screen share)
- Socket.io for bidirectional event transport with fallbacks
- Convex for reactive database with server functions
- Upstash Redis for edge-cached session and presence state
- Next.js + React + Zustand for the reactive frontend
- tRPC for end-to-end typesafe APIs
- Zod for runtime schema validation
- Auth.js for multi-provider authentication
- Framer Motion for collaborative UI animations
- TanStack Query for server state synchronization

## SDKs Used

LiveKit SDK, Socket.io SDK, NATS SDK, Ably SDK, Pusher SDK, Supabase SDK, Convex SDK, Upstash Redis, Cloudflare Workers, Vercel SDK, Next.js SDK, React, Zustand, TanStack Query, Framer Motion, tRPC, Auth.js, Zod, WebRTC SDK, gRPC SDK

## Quickstart

```bash
pip install -r requirements.txt   # Python backend
cd frontend && npm install         # Next.js frontend

# Start Python backend
python main.py --mode server

# In separate terminal: start frontend
cd frontend && npm run dev

# Or run the full simulation
python main.py --mode simulate --clients 10
```

## Structure

```
backend/
  nats_bus.py        NATS event bus — publish/subscribe/request-reply
  supabase_sync.py   Supabase Realtime presence and DB sync
  livekit_rooms.py   LiveKit room management and token generation
  socketio_server.py Socket.io server with rooms and namespaces
  redis_presence.py  Upstash Redis presence tracking and edge cache
  trpc_router.py     tRPC-style typed API router (Python port)
  auth_manager.py    Auth.js compatible JWT + session management
frontend/
  pages/             Next.js pages
  components/        React collaborative components
  hooks/             Custom React hooks for real-time state
  store/             Zustand state stores
main.py              CLI entry point
```
