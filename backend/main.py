from fastapi import FastAPI
import asyncpg
import os

app = FastAPI()
DATABASE_URL = os.getenv("DATABASE_URL")

@app.on_event("startup")
async def startup():
    app.state.db = await asyncpg.connect(DATABASE_URL)

@app.on_event("shutdown")
async def shutdown():
    await app.state.db.close()

@app.get("/api/players")
async def get_players():
    rows = await app.state.db.fetch("SELECT * FROM players ORDER BY rating DESC LIMIT 5")
    return [dict(row) for row in rows]

@app.get("/api/price/{player_id}")
async def get_price(player_id: int):
    row = await app.state.db.fetchrow(
        "SELECT * FROM players WHERE player_id = $1", player_id
    )
    return dict(row) if row else {"error": "Player not found"}
