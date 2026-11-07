const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const PUBLIC = path.join(__dirname, "public");

// Race length. The client reads this from the server, so this is the only place to change it.
const LAPS = 1;

const server = http.createServer((req, res) => {
  const pathname = new URL(
    req.url,
    `http://${req.headers.host}`
  ).pathname;

  const file = pathname === "/" ? "index.html" : path.basename(pathname);
  const target = path.join(PUBLIC, file);

  if (!target.startsWith(PUBLIC) || !fs.existsSync(target)) {
    res.writeHead(404);
    return res.end("Not found");
  }

  const type = target.endsWith(".html")
    ? "text/html; charset=utf-8"
    : "application/octet-stream";

  res.writeHead(200, {
    "Content-Type": type,
    "Cache-Control": "no-cache"
  });

  fs.createReadStream(target).pipe(res);
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();
const clients = new Map();

const MAX_PLAYERS = 4;

function code() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let s = "";

  do {
    s = Array.from(
      { length: 4 },
      () => chars[Math.floor(Math.random() * chars.length)]
    ).join("");
  } while (rooms.has(s));

  return s;
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function players(room) {
  return [...room.players.values()].map(p => ({
    id: p.id,
    name: p.name,
    slot: p.slot,
    lap: p.state.lap || 0,
    progress: p.state.progress || 0,
    finished: !!p.state.finished,
    finishedAt: p.state.finishedAt || 0,
    x: p.state.x || 0,
    z: p.state.z || 0,
    th: p.state.th || 0,
    vx: p.state.vx || 0,
    vy: p.state.vy || 0,
    r: p.state.r || 0,
    g: p.state.g || 1
  }));
}

function roomSummary(room) {
  return {
    code: room.code,
    name: room.name,
    tag: room.tag,
    players: room.players.size,
    started: room.started
  };
}

function broadcastRoom(room) {
  const payload = {
    t: "roomUpdate",
    room: roomSummary(room),
    hostId: room.hostId,
    players: players(room)
  };

  room.players.forEach(p => send(p.ws, payload));
}

function broadcastRooms() {
  const list = [...rooms.values()]
    .filter(r => r.players.size > 0)
    .map(roomSummary);

  clients.forEach(ws => send(ws, {
    t: "rooms",
    rooms: list
  }));
}

function leave(ws) {
  const p = clients.get(ws);
  if (!p) return;

  if (p.room) {
    const room = p.room;

    room.players.delete(p.id);

    room.players.forEach(other => {
      send(other.ws, {
        t: "leave",
        id: p.id
      });
    });

    if (room.players.size === 0) {
      rooms.delete(room.code);
    } else {
      if (room.hostId === p.id) {
        room.hostId = room.players.keys().next().value;
      }

      broadcastRoom(room);
    }
  }

  clients.delete(ws);
  broadcastRooms();
}

wss.on("connection", ws => {
  const p = {
    id: crypto.randomUUID(),
    ws,
    name: "Driver",
    slot: -1,
    room: null,
    state: {
      x: 0,
      z: 0,
      th: 0,
      vx: 0,
      vy: 0,
      lap: 0,
      progress: 0,
      finished: false,
      finishedAt: 0
    }
  };

  clients.set(ws, p);

  ws.on("message", raw => {
    let m;

    try {
      m = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (!m || typeof m.t !== "string") return;

    if (m.t === "list") {
      broadcastRooms();
      return;
    }

    if (m.t === "create") {
      if (p.room) {
        return send(ws, {
          t: "error",
          message: "Leave your current room before creating another."
        });
      }

      const room = {
        code: code(),
        name: String(m.roomName || "Apex race").slice(0, 32),
        tag: String(m.tag || "").slice(0, 18),
        hostId: p.id,
        players: new Map(),
        started: false,
        startAt: 0
      };

      rooms.set(room.code, room);

      p.room = room;
      p.name = String(m.name || "Driver").slice(0, 16);
      p.slot = 0;

      room.players.set(p.id, p);

      send(ws, {
        t: "joined",
        id: p.id,
        slot: 0,
        host: true,
        laps: LAPS,
        room: roomSummary(room),
        players: players(room),
        started: false
      });

      broadcastRoom(room);
      broadcastRooms();
      return;
    }

    if (m.t === "join") {
      if (p.room) {
        return send(ws, {
          t: "error",
          message: "You are already in a room."
        });
      }

      const room = rooms.get(
        String(m.code || "").toUpperCase()
      );

      if (!room) {
        return send(ws, {
          t: "error",
          message: "Room not found. Check the code."
        });
      }

      if (room.started) {
        return send(ws, {
          t: "error",
          message: "That race has already started."
        });
      }

      if (room.players.size >= MAX_PLAYERS) {
        return send(ws, {
          t: "error",
          message: "That room is full (4/4)."
        });
      }

      p.room = room;
      p.name = String(m.name || "Driver").slice(0, 16);

      const used = new Set(
        [...room.players.values()].map(q => q.slot)
      );

      let slot = 0;

      while (used.has(slot) && slot < MAX_PLAYERS) {
        slot++;
      }

      p.slot = slot;
      room.players.set(p.id, p);

      send(ws, {
        t: "joined",
        id: p.id,
        slot,
        host: false,
        laps: LAPS,
        room: roomSummary(room),
        players: players(room),
        started: false
      });

      broadcastRoom(room);
      broadcastRooms();
      return;
    }

    if (m.t === "start") {
      const room = p.room;

      if (!room || room.hostId !== p.id) {
        return send(ws, {
          t: "error",
          message: "Only the room host can start the race."
        });
      }

      if (room.started) return;

      room.started = true;
      room.startAt = Date.now() + 3500;

      room.players.forEach(q => {
        send(q.ws, {
          t: "race",
          laps: LAPS,
          startAt: room.startAt
        });
      });

      broadcastRoom(room);
      broadcastRooms();
      return;
    }

    if (m.t === "state" || m.t === "finish") {
      if (!p.room) return;

      // Car-to-car collisions: this server is a relay. It forwards every car's
      // position, heading and velocity (x, z, th, vx, vy) to the whole room 20x per
      // second, and each client resolves its own side of any contact against those
      // positions. Nothing extra is needed here as long as these fields keep flowing.
      const fields = [
        "x", "z", "th", "vx", "vy",
        "r", "g", "lap", "progress"
      ];

      if (m.t === "state") {
        for (const k of fields) {
          const n = Number(m[k]);

          if (Number.isFinite(n)) {
            p.state[k] = n;
          }
        }
      }

      if (m.t === "finish" && p.room.started) {
        p.state.finished = true;
      }

      if (m.t === "state" && m.finished === true && p.room.started) {
        p.state.finished = true;
      }

      if (p.state.finished) {
        p.state.lap = LAPS;

        // Remember who crossed the line first so finishing order stays fixed
        // even though finished cars keep driving.
        if (!p.state.finishedAt) {
          p.state.finishedAt = Date.now();
        }
      }

      return;
    }
  });

  ws.on("close", () => leave(ws));
  ws.on("error", () => leave(ws));
});

setInterval(() => {
  rooms.forEach(room => {
    if (!room.started) return;

    const snapshot = {
      t: "state",
      p: players(room)
    };

    room.players.forEach(p => send(p.ws, snapshot));
  });
}, 50);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Apex Loop server listening on ${PORT}`);
});
