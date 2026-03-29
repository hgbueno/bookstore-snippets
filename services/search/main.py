"""
BookStore: search-svc

Search service — proxies to OpenSearch for full-text search.
Replaces: AWS Lambda + Amazon Elasticsearch Service
Database: OpenSearch (read-only — populated by sync-svc)

Effort: Low — replace Elasticsearch client with OpenSearch client (API-compatible).
The OpenSearch query DSL is nearly identical to Elasticsearch.
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from opensearchpy import AsyncOpenSearch
from pydantic import BaseModel

# ── Configuration ──────────────────────────────────────────────────────────

OPENSEARCH_HOST = "opensearch-sp"
OPENSEARCH_PORT = 9200
INDEX_NAME = "books"

# ── Connection ─────────────────────────────────────────────────────────────

os_client: AsyncOpenSearch = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global os_client
    # Persistent connection — NOT per-request like Lambda
    os_client = AsyncOpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        use_ssl=False,
        connection_class=None,
    )
    yield
    await os_client.close()


app = FastAPI(title="search-svc", version="1.0.0", lifespan=lifespan)


# ── Models ─────────────────────────────────────────────────────────────────


class SearchResult(BaseModel):
    book_id: str
    title: str
    author: str
    category: str
    price: float
    rating: float
    score: float  # relevance score from OpenSearch


class SearchResponse(BaseModel):
    query: str
    total: int
    results: list[SearchResult]


# ── Endpoints ──────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    try:
        info = await os_client.cluster.health()
        return {
            "status": "healthy",
            "service": "search-svc",
            "cluster_status": info.get("status"),
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/search", response_model=SearchResponse)
async def search_books(
    q: str = Query(..., min_length=1, description="Search query"),
    category: Optional[str] = Query(None, description="Filter by category"),
    min_price: Optional[float] = Query(None, ge=0),
    max_price: Optional[float] = Query(None, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Full-text search across book titles and authors.

    BEFORE (Lambda + Elasticsearch):
        es_client = Elasticsearch(hosts=[...])  # new client per invocation
        es_client.search(index="books", body={...})
        - Cold start: ~500ms for VPC-attached Lambda
        - Connection created and destroyed per invocation

    AFTER (FastAPI + OpenSearch):
        os_client.search(index="books", body={...})  # persistent connection
        - No cold start — service is always running
        - Connection pool shared across requests
        - OpenSearch query DSL is API-compatible with Elasticsearch

    Search features:
        - Portuguese stemming: "livros" matches "livro"
        - Accent folding: "são" matches "sao"
        - Multi-field search: title + author
        - Category and price range filters
    """

    # Build query
    must_clauses = [
        {
            "multi_match": {
                "query": q,
                "fields": ["title^2", "author"],  # title weighted 2x
                "analyzer": "book_analyzer",
                "fuzziness": "AUTO",
            }
        }
    ]

    filter_clauses = []
    if category:
        filter_clauses.append({"term": {"category": category}})
    if min_price is not None or max_price is not None:
        range_filter = {}
        if min_price is not None:
            range_filter["gte"] = min_price
        if max_price is not None:
            range_filter["lte"] = max_price
        filter_clauses.append({"range": {"price": range_filter}})

    body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": filter_clauses,
            }
        },
        "size": limit,
        "_source": ["title", "author", "category", "price", "rating"],
    }

    try:
        response = await os_client.search(index=INDEX_NAME, body=body)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenSearch error: {e}")

    hits = response.get("hits", {})
    total = hits.get("total", {}).get("value", 0)

    results = []
    for hit in hits.get("hits", []):
        source = hit["_source"]
        results.append(
            SearchResult(
                book_id=hit["_id"],
                title=source.get("title", ""),
                author=source.get("author", ""),
                category=source.get("category", ""),
                price=source.get("price", 0.0),
                rating=source.get("rating", 0.0),
                score=hit["_score"],
            )
        )

    return SearchResponse(query=q, total=total, results=results)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
