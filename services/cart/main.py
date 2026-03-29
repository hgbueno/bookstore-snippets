"""
BookStore: cart-svc

Shopping cart service — session-based CRUD with Redis cache.
Replaces: AWS Lambda (ListItemsInCartLambda, AddToCartLambda, etc.)
Database: MongoDB (persistent) + Redis (session cache)

Effort: Low — replace DynamoDB put/get with MongoDB upsert, add Redis session layer.
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
import redis.asyncio as aioredis
import json

# ── Configuration ──────────────────────────────────────────────────────────

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"
REDIS_URL = "redis://redis-sentinel:26379/0"
CART_CACHE_TTL = 3600  # 1 hour

# ── Connections ────────────────────────────────────────────────────────────

db_client: AsyncIOMotorClient = None
redis_client: aioredis.Redis = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_client, redis_client
    db_client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=30)
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield
    db_client.close()
    await redis_client.close()


app = FastAPI(title="cart-svc", version="1.0.0", lifespan=lifespan)


def get_db():
    return db_client[MONGO_DB]


# ── Models ─────────────────────────────────────────────────────────────────


class CartItem(BaseModel):
    book_id: str
    title: str
    price: float
    quantity: int = 1


class Cart(BaseModel):
    user_id: str
    items: list[CartItem] = []
    total: float = 0.0


# ── Endpoints ──────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    try:
        await get_db().command("ping")
        await redis_client.ping()
        return {"status": "healthy", "service": "cart-svc"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/cart", response_model=Cart)
async def get_cart(x_user_id: str = Header(..., alias="X-User-ID")):
    """
    Get current user's cart.

    Flow: check Redis cache first → fallback to MongoDB → cache result.

    BEFORE (Lambda + DynamoDB):
        table.query(KeyConditionExpression=Key('userId').eq(user_id))
        - New DB connection per invocation
        - No caching layer

    AFTER (FastAPI + MongoDB + Redis):
        redis.get(cache_key) → mongo.find_one() → redis.setex()
        - Persistent connection pool
        - Redis cache reduces MongoDB load
    """
    cache_key = f"cart:{x_user_id}"

    # Try Redis cache first
    cached = await redis_client.get(cache_key)
    if cached:
        return Cart(**json.loads(cached))

    # Fallback to MongoDB
    db = get_db()
    doc = await db["cart"].find_one({"user_id": x_user_id})

    if not doc:
        return Cart(user_id=x_user_id, items=[], total=0.0)

    cart = Cart(
        user_id=doc["user_id"],
        items=doc.get("items", []),
        total=doc.get("total", 0.0),
    )

    # Cache for next request
    await redis_client.setex(cache_key, CART_CACHE_TTL, cart.model_dump_json())
    return cart


@app.post("/cart/items", response_model=Cart)
async def add_to_cart(
    item: CartItem,
    x_user_id: str = Header(..., alias="X-User-ID"),
):
    """
    Add item to cart (or increment quantity if exists).

    BEFORE: table.put_item(Item={...}) — full replace
    AFTER:  MongoDB $push + $inc with upsert — atomic update
    """
    db = get_db()

    # Check if item already in cart
    existing = await db["cart"].find_one(
        {"user_id": x_user_id, "items.book_id": item.book_id}
    )

    if existing:
        # Increment quantity
        await db["cart"].update_one(
            {"user_id": x_user_id, "items.book_id": item.book_id},
            {"$inc": {"items.$.quantity": item.quantity}},
        )
    else:
        # Add new item
        await db["cart"].update_one(
            {"user_id": x_user_id},
            {"$push": {"items": item.model_dump()}},
            upsert=True,
        )

    # Recalculate total
    doc = await db["cart"].find_one({"user_id": x_user_id})
    total = sum(i["price"] * i["quantity"] for i in doc.get("items", []))
    await db["cart"].update_one(
        {"user_id": x_user_id}, {"$set": {"total": total}}
    )

    # Invalidate cache
    await redis_client.delete(f"cart:{x_user_id}")

    return await get_cart(x_user_id)


@app.delete("/cart/items/{book_id}", response_model=Cart)
async def remove_from_cart(
    book_id: str,
    x_user_id: str = Header(..., alias="X-User-ID"),
):
    """Remove item from cart."""
    db = get_db()
    await db["cart"].update_one(
        {"user_id": x_user_id},
        {"$pull": {"items": {"book_id": book_id}}},
    )

    # Recalculate total
    doc = await db["cart"].find_one({"user_id": x_user_id})
    if doc:
        total = sum(i["price"] * i["quantity"] for i in doc.get("items", []))
        await db["cart"].update_one(
            {"user_id": x_user_id}, {"$set": {"total": total}}
        )

    await redis_client.delete(f"cart:{x_user_id}")
    return await get_cart(x_user_id)


@app.delete("/cart", response_model=Cart)
async def clear_cart(x_user_id: str = Header(..., alias="X-User-ID")):
    """Clear entire cart (after checkout)."""
    db = get_db()
    await db["cart"].delete_one({"user_id": x_user_id})
    await redis_client.delete(f"cart:{x_user_id}")
    return Cart(user_id=x_user_id, items=[], total=0.0)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
