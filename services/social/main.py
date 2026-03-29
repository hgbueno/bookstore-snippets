"""
BookStore: social-svc

Recommendation service — graph-based "users who bought X also bought Y".
Replaces: AWS Lambda + Amazon Neptune (Gremlin queries)
Database: Neo4j Community (Cypher queries) with MongoDB fallback

Effort: Medium — replace Neptune Gremlin with Neo4j Cypher + add circuit breaker.

KEY PARADIGM SHIFT:
  Lambda + Neptune: Gremlin query language, TinkerPop traversal API
  FastAPI + Neo4j:  Cypher query language, native Python driver

  Also adds: graceful degradation — if Neo4j is down, API returns
  MongoDB best sellers instead of personalized recommendations.
  This pattern didn't exist in the Lambda version.
"""

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Query
from motor.motor_asyncio import AsyncIOMotorClient
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from pydantic import BaseModel

# ── Configuration ──────────────────────────────────────────────────────────

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"
NEO4J_URI = "bolt://neo4j-vm:7687"

# Circuit breaker settings
FAIL_THRESHOLD = 3
RECOVERY_TIMEOUT = 60  # seconds

logger = logging.getLogger("social-svc")

# ── State ──────────────────────────────────────────────────────────────────

db_client: AsyncIOMotorClient = None
neo4j_driver = None
_neo4j_available = True
_fail_count = 0
_circuit_opened_at = 0.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_client, neo4j_driver, _neo4j_available
    db_client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=20)

    try:
        neo4j_driver = AsyncGraphDatabase.driver(NEO4J_URI)
        await neo4j_driver.verify_connectivity()
        logger.info("Neo4j connected — personalized recommendations enabled")
    except Exception as e:
        logger.warning(f"Neo4j unavailable at startup: {e} — using fallback")
        _neo4j_available = False

    yield

    db_client.close()
    if neo4j_driver:
        await neo4j_driver.close()


app = FastAPI(title="social-svc", version="1.0.0", lifespan=lifespan)


def get_db():
    return db_client[MONGO_DB]


# ── Models ─────────────────────────────────────────────────────────────────


class Recommendation(BaseModel):
    book_id: str
    title: str
    score: int


class RecommendationResponse(BaseModel):
    source: str  # "neo4j" or "mongodb_fallback"
    user_id: str
    books: list[Recommendation]


# ── Neo4j: Personalized recommendations ───────────────────────────────────


async def _get_neo4j_recs(user_id: str, limit: int) -> list[dict]:
    """
    Graph traversal: friends-of-friends who bought same books.

    BEFORE (Lambda + Neptune — Gremlin):
        g.V().has('User', 'userId', user_id)
         .out('PURCHASED').in('PURCHASED')
         .out('PURCHASED').dedup()
         .limit(5).valueMap('bookId', 'title')

    AFTER (FastAPI + Neo4j — Cypher):
        MATCH (u:User {user_id: $uid})-[:PURCHASED]->(:Book)
              <-[:PURCHASED]-(friend)-[:PURCHASED]->(rec:Book)
        WHERE NOT (u)-[:PURCHASED]->(rec)
        RETURN rec.book_id, rec.title, count(*) AS score
        ORDER BY score DESC LIMIT $lim
    """
    async with neo4j_driver.session() as session:
        result = await session.run(
            """
            MATCH (u:User {user_id: $uid})-[:PURCHASED]->(:Book)
                  <-[:PURCHASED]-(friend:User)
                  -[:PURCHASED]->(rec:Book)
            WHERE NOT (u)-[:PURCHASED]->(rec)
            RETURN rec.book_id AS book_id,
                   rec.title AS title,
                   count(*) AS score
            ORDER BY score DESC
            LIMIT $lim
            """,
            uid=user_id,
            lim=limit,
        )
        return [dict(record) async for record in result]


# ── MongoDB: Best sellers fallback ─────────────────────────────────────────


async def _get_bestsellers(limit: int) -> list[dict]:
    """
    Fallback: aggregate best sellers by order count.
    This path didn't exist in the Lambda version — it's a new resilience feature.
    """
    db = get_db()
    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.book_id",
                "count": {"$sum": 1},
                "title": {"$first": "$items.title"},
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": limit},
    ]
    results = []
    async for doc in db["orders"].aggregate(pipeline):
        results.append(
            {"book_id": doc["_id"], "title": doc["title"], "score": doc["count"]}
        )
    return results


# ── Circuit breaker ────────────────────────────────────────────────────────


@app.get("/recommendations", response_model=RecommendationResponse)
async def get_recommendations(
    x_user_id: str = Header(..., alias="X-User-ID"),
    limit: int = Query(5, ge=1, le=20),
):
    """
    Get book recommendations with automatic fallback.

    Normal:   Neo4j graph → "Based on what your friends bought" (personalized)
    Degraded: MongoDB aggregation → "Trending on BookStore" (popular)

    The circuit breaker opens after 3 consecutive Neo4j failures and retries
    after 60 seconds. User experience is preserved in both states.
    """
    global _neo4j_available, _fail_count, _circuit_opened_at

    # Check if circuit breaker should attempt recovery
    if not _neo4j_available:
        elapsed = time.time() - _circuit_opened_at
        if elapsed > RECOVERY_TIMEOUT:
            logger.info("Circuit breaker: attempting recovery...")
            _neo4j_available = True
            _fail_count = 0

    # Try Neo4j if circuit is closed
    if _neo4j_available and neo4j_driver:
        try:
            books = await _get_neo4j_recs(x_user_id, limit)
            _fail_count = 0
            return RecommendationResponse(
                source="neo4j", user_id=x_user_id, books=books
            )
        except (ServiceUnavailable, SessionExpired, Exception) as e:
            _fail_count += 1
            logger.warning(f"Neo4j failed ({_fail_count}/{FAIL_THRESHOLD}): {e}")
            if _fail_count >= FAIL_THRESHOLD:
                _neo4j_available = False
                _circuit_opened_at = time.time()
                logger.error("Circuit breaker OPEN — switching to MongoDB fallback")

    # Fallback: best sellers from MongoDB
    books = await _get_bestsellers(limit)
    return RecommendationResponse(
        source="mongodb_fallback", user_id=x_user_id, books=books
    )


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "social-svc",
        "neo4j_available": _neo4j_available,
        "recommendation_source": "neo4j" if _neo4j_available else "mongodb_fallback",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
