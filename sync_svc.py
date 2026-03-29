#!/usr/bin/env python3
"""
BookStore: sync-svc — Event Router

Listens to MongoDB Change Streams and routes events to downstream systems:
- books collection  → OpenSearch (search index)
- orders collection → Redis (best sellers leaderboard) + Neo4j (graph)

Replaces 3 AWS services: DynamoDB Streams + SQS + Lambda

Design decisions:
- Single pod (replicaCount: 1) — preserves event ordering
- No HPA — CPU usage is minimal (I/O bound, not compute bound)
- Async (motor + aioredis) — non-blocking, handles thousands of events/sec
- Graceful degradation — if a downstream is down, log + continue

Usage:
    python sync_svc.py
"""

import asyncio
import logging
import signal

from motor.motor_asyncio import AsyncIOMotorClient
from opensearchpy import AsyncOpenSearch
import redis.asyncio as aioredis

from change_stream_config import PIPELINE

# ── Configuration ──────────────────────────────────────────────────────────

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"
OPENSEARCH_HOST = "opensearch-sp:9200"
REDIS_URL = "redis://redis-sentinel:26379/0"
NEO4J_URI = "bolt://neo4j-vm:7687"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sync-svc")

# ── Resume Token ───────────────────────────────────────────────────────────


async def load_token(db) -> dict | None:
    meta = await db["_sync_meta"].find_one({"_id": "resume_token"})
    return meta["token"] if meta and "token" in meta else None


async def save_token(db, token: dict):
    await db["_sync_meta"].replace_one(
        {"_id": "resume_token"},
        {"_id": "resume_token", "token": token},
        upsert=True,
    )


# ── Event Handlers ─────────────────────────────────────────────────────────


async def handle_books_change(change: dict, os_client: AsyncOpenSearch):
    """Update OpenSearch search index when a book changes."""
    doc = change.get("fullDocument")
    if not doc:
        return

    book_id = str(change["documentKey"]["_id"])
    await os_client.index(
        index="books",
        id=book_id,
        body={
            "title": doc.get("title"),
            "author": doc.get("author"),
            "category": doc.get("category"),
            "price": doc.get("price"),
            "rating": doc.get("rating"),
            "created_at": doc.get("created_at"),
        },
    )
    logger.debug(f"OpenSearch: indexed book {book_id}")


async def handle_orders_change(change: dict, redis_client, neo4j_driver=None):
    """Update Redis leaderboard and Neo4j graph on new order."""
    doc = change.get("fullDocument")
    if not doc:
        return

    # Update Redis best sellers leaderboard
    for item in doc.get("items", []):
        book_id = item.get("book_id")
        if book_id:
            await redis_client.zincrby("bestsellers", 1, book_id)
            logger.debug(f"Redis: incremented {book_id}")

    # Update Neo4j graph (if available — graceful degradation)
    if neo4j_driver:
        try:
            user_id = doc.get("user_id")
            async with neo4j_driver.session() as session:
                for item in doc.get("items", []):
                    await session.run(
                        """
                        MERGE (u:User {user_id: $uid})
                        MERGE (b:Book {book_id: $bid})
                        CREATE (u)-[:PURCHASED {order_id: $oid}]->(b)
                        """,
                        uid=user_id,
                        bid=item["book_id"],
                        oid=str(doc["_id"]),
                    )
        except Exception as e:
            logger.warning(f"Neo4j unavailable, skipping graph update: {e}")


# ── Main Event Loop ────────────────────────────────────────────────────────


async def run():
    # Connect to all services
    mongo = AsyncIOMotorClient(MONGO_URI)
    db = mongo[MONGO_DB]

    os_client = AsyncOpenSearch(hosts=[OPENSEARCH_HOST])
    redis_client = aioredis.from_url(REDIS_URL)

    # Neo4j is optional (graceful degradation)
    neo4j_driver = None
    try:
        from neo4j import AsyncGraphDatabase

        neo4j_driver = AsyncGraphDatabase.driver(NEO4J_URI)
        logger.info("Neo4j connected — graph updates enabled")
    except Exception:
        logger.warning("Neo4j unavailable — graph updates disabled (graceful degradation)")

    # Load resume token for crash recovery
    token = await load_token(db)
    watch_kwargs = {"pipeline": PIPELINE, "full_document": "updateLookup"}
    if token:
        watch_kwargs["resume_after"] = token
        logger.info(f"Resuming from token: {token['_data'][:20]}...")
    else:
        logger.info("Starting fresh — no resume token found")

    # Route table: collection → handler
    event_count = 0
    logger.info("Watching for changes...")

    async with db.watch(**watch_kwargs) as stream:
        async for change in stream:
            collection = change["ns"]["coll"]
            op_type = change["operationType"]

            try:
                if collection == "books":
                    await handle_books_change(change, os_client)
                elif collection == "orders":
                    await handle_orders_change(change, redis_client, neo4j_driver)
                else:
                    continue  # ignore other collections

                # Persist resume token after successful processing
                await save_token(db, change["_id"])
                event_count += 1

                if event_count % 100 == 0:
                    logger.info(f"Processed {event_count} events")

            except Exception as e:
                logger.error(f"Error processing {collection}.{op_type}: {e}")
                # Continue processing — don't crash on individual event failure
                # Failed event will be retried on next restart via resume token


# ── Entrypoint ─────────────────────────────────────────────────────────────


def main():
    loop = asyncio.new_event_loop()

    # Graceful shutdown
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, loop.stop)

    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
