#!/usr/bin/env python3
"""
BookStore: MongoDB Change Streams Configuration

Demonstrates how to open a Change Stream on MongoDB replica set,
with resume token persistence for crash recovery.

This module is imported by sync_svc.py — not run standalone.
Replaces: DynamoDB Streams + SQS (no external broker needed).
"""

import pymongo

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"

# ── Change Stream Pipeline ─────────────────────────────────────────────────

# Only watch for data mutations (skip deletes for this use case)
PIPELINE = [
    {
        "$match": {
            "operationType": {"$in": ["insert", "update", "replace"]},
        }
    }
]

# ── Resume Token Persistence ───────────────────────────────────────────────

# Resume tokens are stored in a dedicated MongoDB collection (_sync_meta).
# On restart, sync-svc loads the last processed token and continues
# from exactly where it left off — no events are lost or duplicated.


def load_resume_token(db) -> dict | None:
    """Load the last successfully processed resume token."""
    meta = db["_sync_meta"].find_one({"_id": "resume_token"})
    if meta and "token" in meta:
        return meta["token"]
    return None


def save_resume_token(db, token: dict):
    """Persist resume token after successfully processing an event."""
    db["_sync_meta"].replace_one(
        {"_id": "resume_token"},
        {"_id": "resume_token", "token": token},
        upsert=True,
    )


# ── Open Change Stream ─────────────────────────────────────────────────────


def open_change_stream(db, collections: list[str] | None = None):
    """
    Open a Change Stream on the database.

    Uses database-level watch to capture changes across multiple collections
    (books, orders) in a single cursor.

    If a resume token exists, the stream continues from the last processed event.
    """
    token = load_resume_token(db)

    watch_kwargs = {
        "pipeline": PIPELINE,
        "full_document": "updateLookup",  # include full doc on updates
    }
    if token:
        watch_kwargs["resume_after"] = token
        print(f"Resuming from token: {token['_data'][:20]}...")
    else:
        print("Starting fresh — no resume token found")

    return db.watch(**watch_kwargs)


# ── Example: What a Change Event looks like ────────────────────────────────

"""
{
    "_id": {"_data": "826..."},           # resume token
    "operationType": "update",
    "ns": {"db": "bookstore", "coll": "books"},
    "documentKey": {"_id": "book_42"},
    "fullDocument": {                      # full doc (with updateLookup)
        "_id": "book_42",
        "title": "Dom Casmurro",
        "author": "Machado de Assis",
        "category": "fiction",
        "price": 29.90,
        "rating": 4.7
    },
    "updateDescription": {
        "updatedFields": {"price": 29.90, "rating": 4.7},
        "removedFields": []
    }
}
"""
