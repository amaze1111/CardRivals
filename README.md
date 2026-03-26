# Trio Clash Multiplayer Server

Authoritative WebSocket server for hosting multiplayer Trio Clash on Render.

## Why WebSocket for this game

- Hidden information must stay server-side
- Turn order and showdown resolution should be authoritative
- Realtime room updates are simpler with a persistent socket than with document sync alone

## Run locally

```bash
npm install
npm start
```

The server listens on `PORT` or `8080`.

## Render

- Create a new Web Service
- Root directory: repository root
- Build command: `npm install`
- Start command: `npm start`

You can also use the included `render.yaml`.

## Protocol summary

Client -> server:

- `create_room`
- `join_room`
- `start_game`
- `move_card_to_group`
- `discard_card`
- `confirm_ready`
- `reveal_next`
- `score_group`

Server -> client:

- `room_created`
- `state`
- `error`

Each `state` payload is sanitized per player so only that player sees their own hand.
