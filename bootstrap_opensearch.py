#!/usr/bin/env python3
"""
BookStore: OpenSearch Initial Bootstrap

First-time population of the search index from MongoDB.
Creates index with Portuguese analyzer (stemming + asciifolding),
bulk indexes all books, and validates count.

After bootstrap, sync-svc takes over via MongoDB Change Streams.

Usage:
    python bootstrap_opensearch.py
"""

import time

import pymongo
from opensearchpy import OpenSearch, helpers

# ── Configuration ──────────────────────────────────────────────────────────

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"
OPENSEARCH_HOST = "opensearch-sp"
OPENSEARCH_PORT = 9200
INDEX_NAME = "books"

# ── Index Mapping ──────────────────────────────────────────────────────────

INDEX_SETTINGS = {
    "settings": {
        "number_of_replicas": 1,
        "analysis": {
            "analyzer": {
                "book_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "asciifolding",       # ã→a, ç→c, é→e
                        "portuguese_stem",    # livros→livro
                    ],
                }
            }
        },
    },
    "mappings": {
        "properties": {
            "title": {"type": "text", "analyzer": "book_analyzer"},
            "author": {"type": "text", "analyzer": "book_analyzer"},
            "category": {"type": "keyword"},
            "price": {"type": "float"},
            "rating": {"type": "float"},
            "created_at": {"type": "date"},
        }
    },
}

# ── Bootstrap Logic ────────────────────────────────────────────────────────


def generate_actions(db):
    """Stream all books from MongoDB for bulk indexing."""
    for book in db["books"].find({}):
        yield {
            "_index": INDEX_NAME,
            "_id": str(book["_id"]),
            "title": book.get("title", ""),
            "author": book.get("author", ""),
            "category": book.get("category", ""),
            "price": book.get("price", 0.0),
            "rating": book.get("rating", 0.0),
            "created_at": book.get("created_at"),
        }


def bootstrap():
    mongo = pymongo.MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]

    os_client = OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        use_ssl=False,
    )

    start = time.time()

    # Step 1: Delete existing index (clean start, idempotent)
    if os_client.indices.exists(INDEX_NAME):
        print(f"Deleting existing index '{INDEX_NAME}'...")
        os_client.indices.delete(INDEX_NAME)

    # Step 2: Create index with custom mapping + Portuguese analyzer
    print(f"Creating index '{INDEX_NAME}' with book_analyzer...")
    os_client.indices.create(INDEX_NAME, body=INDEX_SETTINGS)

    # Step 3: Bulk index all books from MongoDB
    mongo_count = db["books"].count_documents({})
    print(f"Indexing {mongo_count} books from MongoDB...")

    success, errors = helpers.bulk(
        os_client,
        generate_actions(db),
        chunk_size=500,
        max_retries=3,
    )
    print(f"  Indexed: {success} documents, {len(errors)} errors")

    # Step 4: Validate count
    os_client.indices.refresh(INDEX_NAME)
    os_count = os_client.count(index=INDEX_NAME)["count"]
    elapsed = time.time() - start

    print(f"\nValidation:")
    print(f"  MongoDB count:    {mongo_count}")
    print(f"  OpenSearch count: {os_count}")
    print(f"  Time elapsed:     {elapsed:.1f}s")

    if os_count == mongo_count:
        print("\n✓ Bootstrap validated — counts match")
    else:
        print("\n✗ Count mismatch — review before proceeding")

    mongo.close()


if __name__ == "__main__":
    bootstrap()
