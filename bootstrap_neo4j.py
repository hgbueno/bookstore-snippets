#!/usr/bin/env python3
"""
BookStore: Neo4j Graph Bootstrap from MongoDB

Rebuilds the recommendation graph from purchase history.
Reads orders from MongoDB and creates User/Book nodes + PURCHASED relationships.
Uses MERGE for idempotency — safe to re-run.

Same script used for:
- Phase 2 initial bootstrap (after DynamoDB → MongoDB migration)
- DR recovery (Neo4j VM failure — rebuild from scratch in ~15 min)

Usage:
    python bootstrap_neo4j.py
    python bootstrap_neo4j.py --batch-size 1000
"""

import argparse
import time

import pymongo
from neo4j import GraphDatabase

# ── Configuration ──────────────────────────────────────────────────────────

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"
NEO4J_URI = "bolt://neo4j-vm:7687"
NEO4J_AUTH = None  # Community edition, no auth by default

# ── Bootstrap Logic ────────────────────────────────────────────────────────


def bootstrap(batch_size: int = 500):
    mongo = pymongo.MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

    start = time.time()

    with driver.session() as session:
        # Step 1: Clear existing graph (clean start for idempotent re-runs)
        print("Clearing existing graph...")
        session.run("MATCH (n) DETACH DELETE n")

        # Step 2: Create uniqueness constraints (prevents duplicate nodes)
        print("Creating constraints...")
        session.run("""
            CREATE CONSTRAINT IF NOT EXISTS
            FOR (u:User) REQUIRE u.user_id IS UNIQUE
        """)
        session.run("""
            CREATE CONSTRAINT IF NOT EXISTS
            FOR (b:Book) REQUIRE b.book_id IS UNIQUE
        """)

        # Step 3: Read all orders from MongoDB
        total_orders = db["orders"].count_documents({})
        print(f"Processing {total_orders} orders...")

        orders = db["orders"].find({})
        edge_count = 0
        order_count = 0

        for order in orders:
            user_id = order["user_id"]

            # MERGE user node (create if not exists)
            session.run("MERGE (u:User {user_id: $uid})", uid=user_id)

            # Step 4: For each item in the order, MERGE book + relationship
            for item in order.get("items", []):
                book_id = item["book_id"]
                title = item.get("title", "")

                # MERGE book node
                session.run(
                    """
                    MERGE (b:Book {book_id: $bid})
                    SET b.title = $title
                    """,
                    bid=book_id,
                    title=title,
                )

                # CREATE purchase relationship
                session.run(
                    """
                    MATCH (u:User {user_id: $uid})
                    MATCH (b:Book {book_id: $bid})
                    CREATE (u)-[:PURCHASED {
                        order_id: $oid,
                        date: datetime($dt)
                    }]->(b)
                    """,
                    uid=user_id,
                    bid=book_id,
                    oid=str(order["_id"]),
                    dt=order.get("created_at", order.get("ordered_at")).isoformat(),
                )
                edge_count += 1

            order_count += 1
            if order_count % 1000 == 0:
                print(f"  Processed {order_count}/{total_orders} orders ({edge_count} edges)")

    elapsed = time.time() - start

    # Step 5: Report
    print(f"\nBootstrap complete:")
    print(f"  Orders processed: {order_count}")
    print(f"  Edges created:    {edge_count}")
    print(f"  Time elapsed:     {elapsed:.1f}s")

    driver.close()
    mongo.close()


# ── Main ───────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Neo4j graph bootstrap from MongoDB orders")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for processing")
    args = parser.parse_args()

    bootstrap(batch_size=args.batch_size)


if __name__ == "__main__":
    main()
