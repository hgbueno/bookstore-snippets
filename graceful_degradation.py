#!/usr/bin/env python3
"""
BookStore: Graceful Degradation — Neo4j Circuit Breaker

Pattern: when Neo4j is unavailable, the recommendation API returns
best sellers from MongoDB instead of personalized graph-based results.

The user experience is preserved:
- Neo4j healthy → "Based on what your friends bought" (personalized)
- Neo4j down    → "Trending on BookStore" (popular by order count)

Core business flow is never interrupted:
- Browsing ✓  Search ✓  Cart ✓  Checkout ✓  Orders ✓
- Only the recommendation section changes behavior.

This module is used by social-svc (the recommendations microservice).

Usage:
    from graceful_degradation import get_recommendations

    # Returns personalized recs if Neo4j is up,
    # best sellers from MongoDB if Neo4j is down.
    books = await get_recommendations(user_id="user_123", limit=5)
"""

import logging
import time

import pymongo
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired

# ── Configuration ──────────────────────────────────────────────────────────

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"
NEO4J_URI = "bolt://neo4j-vm:7687"

# Circuit breaker settings
FAIL_THRESHOLD = 3          # consecutive failures to open circuit
RECOVERY_TIMEOUT = 60       # seconds before trying Neo4j again

logger = logging.getLogger("social-svc.degradation")

# ── Circuit Breaker State ──────────────────────────────────────────────────

_neo4j_available = True
_fail_count = 0
_circuit_opened_at = 0.0

# ── Connections ────────────────────────────────────────────────────────────

_neo4j_driver = None
_mongo_client = None
_mongo_db = None


def _get_neo4j():
    global _neo4j_driver
    if _neo4j_driver is None:
        _neo4j_driver = GraphDatabase.driver(NEO4J_URI)
    return _neo4j_driver


def _get_mongo():
    global _mongo_client, _mongo_db
    if _mongo_client is None:
        _mongo_client = pymongo.MongoClient(MONGO_URI)
        _mongo_db = _mongo_client[MONGO_DB]
    return _mongo_db


# ── Neo4j: Personalized Recommendations ───────────────────────────────────


def _get_neo4j_recommendations(user_id: str, limit: int = 5) -> list[dict]:
    """
    Graph-based recommendations: "Users who bought what you bought also bought..."

    Traversal: User → PURCHASED → Book ← PURCHASED ← Friend → PURCHASED → Recommendation
    Filters out books the user already purchased.
    """
    driver = _get_neo4j()
    with driver.session() as session:
        result = session.run(
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
        return [dict(record) for record in result]


# ── MongoDB: Best Sellers Fallback ─────────────────────────────────────────


def _get_bestsellers_fallback(limit: int = 5) -> list[dict]:
    """
    Fallback: aggregate best sellers by order count from MongoDB.

    Uses the orders collection to count how many times each book was purchased.
    Returns the top N most ordered books across all users.
    """
    db = _get_mongo()
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
    return [
        {"book_id": doc["_id"], "title": doc["title"], "score": doc["count"]}
        for doc in db["orders"].aggregate(pipeline)
    ]


# ── Circuit Breaker Logic ──────────────────────────────────────────────────


def get_recommendations(user_id: str, limit: int = 5) -> dict:
    """
    Get book recommendations with automatic fallback.

    Returns:
        {
            "source": "neo4j" | "mongodb_fallback",
            "books": [{"book_id": ..., "title": ..., "score": ...}]
        }
    """
    global _neo4j_available, _fail_count, _circuit_opened_at

    # Check if circuit breaker should attempt recovery
    if not _neo4j_available:
        elapsed = time.time() - _circuit_opened_at
        if elapsed > RECOVERY_TIMEOUT:
            logger.info("Circuit breaker: attempting recovery...")
            _neo4j_available = True  # half-open state
            _fail_count = 0

    # Try Neo4j if circuit is closed (or half-open)
    if _neo4j_available:
        try:
            books = _get_neo4j_recommendations(user_id, limit)
            _fail_count = 0  # reset on success
            return {"source": "neo4j", "books": books}

        except (ServiceUnavailable, SessionExpired, Exception) as e:
            _fail_count += 1
            logger.warning(f"Neo4j failed ({_fail_count}/{FAIL_THRESHOLD}): {e}")

            if _fail_count >= FAIL_THRESHOLD:
                _neo4j_available = False
                _circuit_opened_at = time.time()
                logger.error(
                    "Circuit breaker OPEN — switching to MongoDB fallback. "
                    f"Will retry in {RECOVERY_TIMEOUT}s."
                )

    # Fallback: best sellers from MongoDB
    books = _get_bestsellers_fallback(limit)
    return {"source": "mongodb_fallback", "books": books}
