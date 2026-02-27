from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import feedparser
import sqlite3
import httpx
import asyncio
import json
import os
import time
import jwt  # pip install PyJWT
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

# ─── Config ───────────────────────────────────────────────────────────────────

NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.nl",
]

# Path to your downloaded service account JSON file
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "newsnoti-d07e4-41fff0f86e6b.json")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # 5 min default
DB_PATH = os.getenv("DB_PATH", "notify.db")

# ─── FCM V1 Auth ──────────────────────────────────────────────────────────────

_fcm_token_cache = {"token": None, "expires_at": 0}

async def get_fcm_access_token() -> str:
    """Get a short-lived OAuth2 access token using the service account JSON."""
    now = time.time()
    if _fcm_token_cache["token"] and now < _fcm_token_cache["expires_at"] - 60:
        return _fcm_token_cache["token"]

    with open(GOOGLE_CREDENTIALS_FILE) as f:
        creds = json.load(f)

    # Build JWT assertion
    iat = int(now)
    exp = iat + 3600
    payload = {
        "iss": creds["client_email"],
        "sub": creds["client_email"],
        "aud": "https://oauth2.googleapis.com/token",
        "iat": iat,
        "exp": exp,
        "scope": "https://www.googleapis.com/auth/firebase.messaging",
    }
    private_key = creds["private_key"]
    assertion = jwt.encode(payload, private_key, algorithm="RS256")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            }
        )
        data = resp.json()
        token = data["access_token"]
        _fcm_token_cache["token"] = token
        _fcm_token_cache["expires_at"] = now + data.get("expires_in", 3600)
        return token

# ─── Database ─────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            display_name TEXT,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS seen_tweets (
            tweet_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            title TEXT,
            seen_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS fcm_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT,
            message TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()

def log(level: str, message: str):
    conn = get_db()
    conn.execute("INSERT INTO logs (level, message) VALUES (?, ?)", (level, message))
    conn.commit()
    conn.close()
    print(f"[{level.upper()}] {message}")

# ─── Nitter RSS Fetching ───────────────────────────────────────────────────────

async def fetch_rss(username: str) -> list[dict]:
    """Try each Nitter instance until one works."""
    for instance in NITTER_INSTANCES:
        url = f"{instance}/{username}/rss"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, follow_redirects=True)
                if resp.status_code == 200:
                    feed = feedparser.parse(resp.text)
                    tweets = []
                    for entry in feed.entries:
                        tweets.append({
                            "id": entry.get("id", entry.get("link", "")),
                            "title": entry.get("title", ""),
                            "link": entry.get("link", ""),
                            "published": entry.get("published", ""),
                        })
                    log("info", f"Fetched {len(tweets)} tweets from @{username} via {instance}")
                    return tweets
        except Exception as e:
            log("warn", f"Nitter instance {instance} failed for @{username}: {e}")
            continue
    log("error", f"All Nitter instances failed for @{username}")
    return []

# ─── FCM Notification ─────────────────────────────────────────────────────────

async def send_fcm_notification(title: str, body: str, link: str):
    conn = get_db()
    tokens = [row["token"] for row in conn.execute("SELECT token FROM fcm_tokens").fetchall()]
    conn.close()

    if not tokens:
        log("warn", "No FCM tokens registered, skipping notification")
        return

    with open(GOOGLE_CREDENTIALS_FILE) as f:
        creds = json.load(f)
    project_id = creds["project_id"]

    access_token = await get_fcm_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # FCM V1 sends one message per token
    for token in tokens:
        payload = {
            "message": {
                "token": token,
                "notification": {
                    "title": title,
                    "body": body,
                },
                "data": {
                    "link": link,
                },
                "android": {
                    "priority": "high",
                    "notification": {
                        "click_action": "OPEN_ARTICLE"
                    }
                }
            }
        }
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send",
                    json=payload,
                    headers=headers
                )
                if resp.status_code == 200:
                    log("info", f"FCM sent to token ...{token[-6:]}: {title}")
                else:
                    log("error", f"FCM failed for token ...{token[-6:]}: {resp.status_code} {resp.text}")
        except Exception as e:
            log("error", f"FCM exception: {e}")

# ─── Polling Loop ─────────────────────────────────────────────────────────────

async def poll_accounts():
    while True:
        try:
            conn = get_db()
            accounts = conn.execute(
                "SELECT username FROM accounts WHERE active = 1"
            ).fetchall()
            conn.close()

            for account in accounts:
                username = account["username"]
                tweets = await fetch_rss(username)

                conn = get_db()
                for tweet in tweets:
                    tweet_id = tweet["id"]
                    exists = conn.execute(
                        "SELECT 1 FROM seen_tweets WHERE tweet_id = ?", (tweet_id,)
                    ).fetchone()

                    if not exists:
                        conn.execute(
                            "INSERT INTO seen_tweets (tweet_id, username, title) VALUES (?, ?, ?)",
                            (tweet_id, username, tweet["title"])
                        )
                        conn.commit()
                        await send_fcm_notification(
                            title=f"@{username} tweeted",
                            body=tweet["title"][:200],
                            link=tweet["link"]
                        )
                        await asyncio.sleep(0.5)  # small delay between notifications

                conn.commit()
                conn.close()
                await asyncio.sleep(2)  # be nice between accounts

        except Exception as e:
            log("error", f"Polling error: {e}")

        await asyncio.sleep(POLL_INTERVAL)

# ─── App Lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    log("info", "Database initialized")
    task = asyncio.create_task(poll_accounts())
    log("info", f"Polling started (every {POLL_INTERVAL}s)")
    yield
    task.cancel()

app = FastAPI(title="TwitterNotify API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Models ───────────────────────────────────────────────────────────────────

class AccountIn(BaseModel):
    username: str
    display_name: Optional[str] = None

class TokenIn(BaseModel):
    token: str

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/accounts")
def list_accounts():
    conn = get_db()
    rows = conn.execute("SELECT * FROM accounts ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/accounts")
def add_account(body: AccountIn):
    username = body.username.lstrip("@").strip()
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO accounts (username, display_name) VALUES (?, ?)",
            (username, body.display_name or username)
        )
        conn.commit()
        log("info", f"Added account @{username}")
        return {"status": "ok", "username": username}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Account already exists")
    finally:
        conn.close()

@app.delete("/accounts/{username}")
def delete_account(username: str):
    conn = get_db()
    conn.execute("DELETE FROM accounts WHERE username = ?", (username,))
    conn.commit()
    conn.close()
    log("info", f"Removed account @{username}")
    return {"status": "ok"}

@app.patch("/accounts/{username}/toggle")
def toggle_account(username: str):
    conn = get_db()
    conn.execute(
        "UPDATE accounts SET active = 1 - active WHERE username = ?", (username,)
    )
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.post("/tokens")
def register_token(body: TokenIn):
    conn = get_db()
    try:
        conn.execute("INSERT OR IGNORE INTO fcm_tokens (token) VALUES (?)", (body.token,))
        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()

@app.get("/logs")
def get_logs(limit: int = 50):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM logs ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/stats")
def get_stats():
    conn = get_db()
    accounts = conn.execute("SELECT COUNT(*) as c FROM accounts WHERE active=1").fetchone()["c"]
    seen = conn.execute("SELECT COUNT(*) as c FROM seen_tweets").fetchone()["c"]
    tokens = conn.execute("SELECT COUNT(*) as c FROM fcm_tokens").fetchone()["c"]
    conn.close()
    return {"active_accounts": accounts, "tweets_tracked": seen, "devices": tokens}

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
