"""
BookStore: books-svc

Book catalog service — CRUD operations on the books collection.
Replaces: AWS Lambda (ListBooksLambda, GetBookLambda)
Database: MongoDB (source of truth)

Effort: Low — straightforward CRUD, swap DynamoDB scan/get for MongoDB find/findOne.
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

# ── Configuration ──────────────────────────────────────────────────────────

MONGO_URI = "mongodb://mongo-rs0-1:27017,mongo-rs0-2:27017,mongo-rs0-3:27017/?replicaSet=rs0"
MONGO_DB = "bookstore"

# ── Database connection (persistent pool — NOT per-request like Lambda) ───

db_client: AsyncIOMotorClient = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: create connection pool. Shutdown: close it."""
    global db_client
    # KEY DIFFERENCE from Lambda: connection pool persists across all requests.
    # Lambda creates a new connection per invocation (or fragile reuse).
    db_client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=50, minPoolSize=10)
    yield
    db_client.close()


app = FastAPI(title="books-svc", version="1.0.0", lifespan=lifespan)


def get_db():
    return db_client[MONGO_DB]


# ── Models ─────────────────────────────────────────────────────────────────


class Book(BaseModel):
    id: str = Field(alias="_id")
    title: str
    author: str
    category: str
    price: float
    rating: float = 0.0
    description: Optional[str] = None
    cover_url: Optional[str] = None

    class Config:
        populate_by_name = True


class BookCreate(BaseModel):
    title: str
    author: str
    category: str
    price: float
    description: Optional[str] = None
    cover_url: Optional[str] = None


# ── Endpoints ──────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """Health check for Kubernetes liveness/readiness probes."""
    try:
        await get_db().command("ping")
        return {"status": "healthy", "service": "books-svc"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/books", response_model=list[Book])
async def list_books(
    category: Optional[str] = Query(None, description="Filter by category"),
    sort_by: str = Query("rating", enum=["rating", "price", "title"]),
    order: str = Query("desc", enum=["asc", "desc"]),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    List books with optional filtering and sorting.

    BEFORE (Lambda + DynamoDB):
        table.scan(FilterExpression=..., ExclusiveStartKey=...)
        - Full table scan, no native sorting
        - Pagination via LastEvaluatedKey (opaque cursor)

    AFTER (FastAPI + MongoDB):
        collection.find(filter).sort(field, direction).skip(offset).limit(limit)
        - Index-backed queries with compound indexes
        - Standard offset/limit pagination
    """
    db = get_db()
    query = {}
    if category:
        query["category"] = category

    sort_direction = -1 if order == "desc" else 1
    cursor = (
        db["books"]
        .find(query)
        .sort(sort_by, sort_direction)
        .skip(offset)
        .limit(limit)
    )

    books = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        books.append(doc)
    return books


@app.get("/books/{book_id}", response_model=Book)
async def get_book(book_id: str):
    """
    Get a single book by ID.

    BEFORE: table.get_item(Key={"id": book_id})
    AFTER:  collection.find_one({"_id": book_id})
    """
    db = get_db()
    doc = await db["books"].find_one({"_id": book_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Book not found")
    doc["_id"] = str(doc["_id"])
    return doc


@app.get("/books/category/{category}", response_model=list[Book])
async def list_by_category(
    category: str,
    limit: int = Query(20, ge=1, le=100),
):
    """
    List books by category — uses compound index (category, rating).

    BEFORE: table.query(IndexName="category-index", KeyConditionExpression=...)
    AFTER:  collection.find({"category": ...}).sort("rating", -1)
    """
    db = get_db()
    cursor = (
        db["books"]
        .find({"category": category})
        .sort("rating", -1)
        .limit(limit)
    )
    books = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        books.append(doc)
    return books


@app.post("/books", response_model=Book, status_code=201)
async def create_book(book: BookCreate):
    """Create a new book."""
    db = get_db()
    doc = book.model_dump()
    result = await db["books"].insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    return doc


@app.put("/books/{book_id}", response_model=Book)
async def update_book(book_id: str, book: BookCreate):
    """Update an existing book."""
    db = get_db()
    result = await db["books"].find_one_and_update(
        {"_id": book_id},
        {"$set": book.model_dump()},
        return_document=True,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Book not found")
    result["_id"] = str(result["_id"])
    return result


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
