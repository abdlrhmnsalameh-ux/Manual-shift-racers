# Apex Loop Multiplayer

Apex Loop is a 3D browser racing game with rooms for up to four drivers.

## Run locally
1. Install Node.js 18 or newer.
2. Run `npm install`.
3. Run `npm start`.
4. Open `http://localhost:10000` in multiple browser windows/devices.

## Deploy on Render
1. Push the contents of this folder to a GitHub repository.
2. In Render, create a **New Web Service** and connect that repository.
3. Runtime: **Node**.
4. Build command: `npm install`.
5. Start command: `npm start`.
6. Deploy. Render provides the HTTPS URL; the client automatically uses secure WebSockets (`wss://`) on that same host.

## Features
- Create a room or join by 4-character room code.
- Add a tag to a room and filter the public room list by tag.
- Four starting slots, maximum four drivers.
- Host starts the race; each driver completes three laps.
- Live race position/lap board.
- Client-side vehicle collision response with momentum transfer.
- Original Apex Loop track, car, manual gearbox, audio, camera and touch controls retained.

## Notes
The browser clients simulate their own vehicle physics and exchange frequent position/velocity snapshots through the server. Car-to-car collision response is calculated by each client against interpolated opponent cars. This is a playable multiplayer prototype, not a fully authoritative anti-cheat racing simulation.
