import asyncio
from futgg_auto_sync import sync_players

async def run_full_resync():
    print("🚀 Starting FULL FUT.GG player resync...")
    await sync_players()
    print("🎯 Full player resync complete!")

if __name__ == "__main__":
    asyncio.run(run_full_resync())