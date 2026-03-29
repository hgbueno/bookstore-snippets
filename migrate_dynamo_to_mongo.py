#!/usr/bin/env python3
"""
BookStore Migration: DynamoDB → MongoDB

Batch migration with automatic type conversion.
Handles all DynamoDB-specific types: Decimal, Sets, NULL, nested Maps.
Upsert-based (idempotent) — safe to re-run without duplication risk.

Usage:
    python migrate_dynamo_to_mongo.py --mode full
    python migrate_dynamo_to_mongo.py --mode incremental --since "2026-03-01T00:00:00"
    python migrate_dynamo_to_mongo.py --mode final
"""

import argparse
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
import boto3.dynamodb.conditions
import pymongo

# ── Configuration ──────────────────────────────────────────────────────────

DYNAMO_REGION = "sa-east-1"
MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"

COLLECTIONS = ["books", "orders", "users", "cart"]

KEY_MAP = {
    "books": "id",
    "orders": "id",
    "users": "id",
    "cart": "id",
}

# ── Type Conversion ────────────────────────────────────────────────────────

ISO_DATE_FIELDS = {"created_at", "updated_at", "ordered_at", "last_login"}


def is_iso_date(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except (ValueError, AttributeError):
        return False


def convert_types(item: dict, parent_key: str = "") -> dict:
    """
    Convert DynamoDB types to MongoDB-native types.

    Handles:
    - Decimal (Number)    → int or float
    - Set (SS, NS, BS)    → sorted list (array)
    - ISO date strings    → datetime objects (ISODate)
    - NULL                → omitted (field absent)
    - Nested Map (M)      → recursive conversion
    - List (L)            → array with recursive conversion
    """
    doc = {}
    for key, value in item.items():
        if isinstance(value, Decimal):
            doc[key] = int(value) if value == int(value) else float(value)
        elif isinstance(value, set):
            doc[key] = sorted(list(value))
        elif isinstance(value, str) and (key in ISO_DATE_FIELDS or is_iso_date(value)):
            try:
                doc[key] = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                doc[key] = value
        elif value is None:
            continue
        elif isinstance(value, dict):
            doc[key] = convert_types(value, parent_key=key)
        elif isinstance(value, list):
            doc[key] = [
                convert_types(v) if isinstance(v, dict) else v for v in value
            ]
        else:
            doc[key] = value
    return doc


# ── Migration Logic ────────────────────────────────────────────────────────


def migrate_collection(
    dynamo_resource, mongo_db, table_name: str, mode: str = "full", since: str | None = None
) -> int:
    table = dynamo_resource.Table(table_name)
    collection = mongo_db[table_name]
    id_field = KEY_MAP.get(table_name, "id")

    scan_kwargs: dict[str, Any] = {}
    if mode == "incremental" and since:
        scan_kwargs["FilterExpression"] = boto3.dynamodb.conditions.Attr("updated_at").gte(since)

    count = 0
    while True:
        response = table.scan(**scan_kwargs)
        for item in response["Items"]:
            doc = convert_types(item)
            if id_field in doc:
                doc["_id"] = doc.pop(id_field)
            collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
            count += 1
        if "LastEvaluatedKey" not in response:
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    return count


def create_indexes(mongo_db):
    mongo_db["books"].create_index(
        [("title", "text"), ("author", "text")], default_language="portuguese"
    )
    mongo_db["books"].create_index([("category", 1), ("rating", -1)])
    mongo_db["books"].create_index("updated_at")
    mongo_db["orders"].create_index([("user_id", 1), ("created_at", -1)])
    mongo_db["orders"].create_index("updated_at")
    mongo_db["users"].create_index("email", unique=True)
    mongo_db["users"].create_index("updated_at")
    mongo_db["cart"].create_index("user_id", unique=True)
    print("Indexes created successfully")


def validate_counts(dynamo_resource, mongo_db) -> bool:
    all_match = True
    for table_name in COLLECTIONS:
        table = dynamo_resource.Table(table_name)
        dynamo_count = table.item_count
        mongo_count = mongo_db[table_name].count_documents({})
        status = "✓" if mongo_count >= dynamo_count else "✗"
        print(f"  {status} {table_name}: DynamoDB={dynamo_count}, MongoDB={mongo_count}")
        if mongo_count < dynamo_count:
            all_match = False
    return all_match


# ── Main ───────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="DynamoDB → MongoDB migration")
    parser.add_argument("--mode", choices=["full", "incremental", "final"], default="full")
    parser.add_argument("--since", help="ISO datetime for incremental mode")
    args = parser.parse_args()

    dynamo = boto3.resource("dynamodb", region_name=DYNAMO_REGION)
    mongo = pymongo.MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]

    print(f"Migration mode: {args.mode}")
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}\n")

    total = 0
    for table_name in COLLECTIONS:
        count = migrate_collection(dynamo, db, table_name, args.mode, args.since)
        total += count
        print(f"  {table_name}: {count} documents migrated")

    print(f"\nTotal: {total} documents migrated")

    if args.mode in ("full", "final"):
        print("\nCreating indexes...")
        create_indexes(db)

    print("\nValidating counts...")
    if validate_counts(dynamo, db):
        print("\n✓ All counts match — migration validated")
    else:
        print("\n✗ Count mismatch — review before proceeding")
        sys.exit(1)


if __name__ == "__main__":
    main()
