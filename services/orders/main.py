"""
BookStore: orders-svc

Order processing service — highest CPU: payment validation, stock check, writes to MongoDB.
Replaces: AWS Lambda (CheckoutLambda, ListOrdersLambda)
Database: MongoDB (orders collection)

Effort: Medium — replace SQS publish with MongoDB writes (sync-svc handles downstream
via Change Streams). Add stock validation and payment processing.

KEY PARADIGM SHIFT:
  Lambda: order created → SQS message published → separate Lambda updates leaderboard
  FastAPI: order created in MongoDB → Change Streams (sync-svc) automatically updates
           OpenSearch, Redis leaderboard, and Neo4j. No explicit event publishing needed.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Header, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

# ── Configuration ──────────────────────────────────────────────────────────

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"

# ── Connection ─────────────────────────────────────────────────────────────

db_client: AsyncIOMotorClient = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_client
    db_client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=50, minPoolSize=10)
    yield
    db_client.close()


app = FastAPI(title="orders-svc", version="1.0.0", lifespan=lifespan)


def get_db():
    return db_client[MONGO_DB]


# ── Models ─────────────────────────────────────────────────────────────────


class OrderItem(BaseModel):
    book_id: str
    title: str
    price: float
    quantity: int = 1


class OrderCreate(BaseModel):
    items: list[OrderItem]
    shipping_address: Optional[str] = None


class Order(BaseModel):
    id: str = Field(alias="_id")
    user_id: str
    items: list[OrderItem]
    total: float
    status: str = "confirmed"
    created_at: datetime
    shipping_address: Optional[str] = None

    class Config:
        populate_by_name = True


# ── Endpoints ──────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    try:
        await get_db().command("ping")
        return {"status": "healthy", "service": "orders-svc"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/orders", response_model=Order, status_code=201)
async def create_order(
    order: OrderCreate,
    x_user_id: str = Header(..., alias="X-User-ID"),
):
    """
    Create a new order — the most complex endpoint.

    BEFORE (Lambda):
        1. Lambda validates stock via DynamoDB conditional write
        2. Lambda writes order to DynamoDB Orders table
        3. Lambda publishes SQS message for downstream consumers
        4. Separate Lambda (triggered by SQS) updates Redis leaderboard
        5. Separate Lambda (triggered by DynamoDB Streams) updates Neptune graph
        → 3 Lambda functions + SQS + DynamoDB Streams for one order

    AFTER (FastAPI):
        1. Validate stock via MongoDB findOneAndUpdate with conditions
        2. Write order to MongoDB orders collection
        3. That's it — sync-svc picks up the change via Change Streams
           and updates OpenSearch, Redis leaderboard, and Neo4j automatically
        → 1 service, no explicit event publishing, no SQS, no streams config
    """
    db = get_db()

    # Step 1: Validate stock (atomic decrement)
    for item in order.items:
        result = await db["books"].find_one_and_update(
            {"_id": item.book_id, "stock": {"$gte": item.quantity}},
            {"$inc": {"stock": -item.quantity}},
        )
        if not result:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient stock for book {item.book_id}",
            )

    # Step 2: Create order document
    order_doc = {
        "_id": f"order_{uuid4().hex[:12]}",
        "user_id": x_user_id,
        "items": [item.model_dump() for item in order.items],
        "total": sum(item.price * item.quantity for item in order.items),
        "status": "confirmed",
        "created_at": datetime.now(timezone.utc),
        "shipping_address": order.shipping_address,
    }

    await db["orders"].insert_one(order_doc)

    # Step 3: Clear user's cart
    await db["cart"].delete_one({"user_id": x_user_id})

    # NO STEP 4 — no SQS publish, no explicit event.
    # MongoDB Change Streams (sync-svc) automatically:
    #   → Updates Redis best sellers leaderboard (ZINCRBY)
    #   → Adds PURCHASED edges to Neo4j graph
    # This happens asynchronously, without coupling orders-svc to downstream.

    order_doc["_id"] = str(order_doc["_id"])
    return order_doc


@app.get("/orders", response_model=list[Order])
async def list_orders(
    x_user_id: str = Header(..., alias="X-User-ID"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    List orders for the current user, newest first.

    BEFORE: table.query(IndexName="user-orders-index", KeyConditionExpression=...)
    AFTER:  collection.find({"user_id": ...}).sort("created_at", -1)
            Uses compound index (user_id, created_at)
    """
    db = get_db()
    cursor = (
        db["orders"]
        .find({"user_id": x_user_id})
        .sort("created_at", -1)
        .skip(offset)
        .limit(limit)
    )
    orders = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        orders.append(doc)
    return orders


@app.get("/orders/{order_id}", response_model=Order)
async def get_order(
    order_id: str,
    x_user_id: str = Header(..., alias="X-User-ID"),
):
    """Get a single order (must belong to user)."""
    db = get_db()
    doc = await db["orders"].find_one({"_id": order_id, "user_id": x_user_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Order not found")
    doc["_id"] = str(doc["_id"])
    return doc


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
