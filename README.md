# BookStore Migration — Code Snippets

Reference implementation for the **BookStore AWS → Akamai Cloud** migration plan.

These scripts support the migration of a Brazilian digital bookstore (3M users, 180K orders/month, ~$35M annual revenue) from AWS managed services to open-source equivalents on Akamai Cloud Computing.

## Scripts

| File | Purpose | Phase |
|------|---------|-------|
| `migrate_dynamo_to_mongo.py` | DynamoDB → MongoDB batch migration with type conversion | Phase 2 / Phase 4 |
| `bootstrap_neo4j.py` | Rebuild recommendation graph from MongoDB orders | Phase 2 / DR recovery |
| `bootstrap_opensearch.py` | Initial search index population from MongoDB | Phase 2 |
| `change_stream_config.py` | MongoDB Change Streams configuration + resume token | Production |
| `sync_svc.py` | Event router: Change Streams → OpenSearch + Redis + Neo4j | Production |
| `sync_s3_to_akamai.sh` | S3 → Akamai Object Storage sync via rclone | Phase 2 → Phase 4 |
| `graceful_degradation.py` | Circuit breaker for Neo4j with MongoDB fallback | Production |

## Microservices (FastAPI on LKE)

The `services/` directory contains the 6 FastAPI microservices that replace the AWS Lambda functions. Each service is a standalone Python app with a shared Dockerfile.

| Service | Port | Replaces | Effort | Key Change |
|---------|------|----------|--------|------------|
| `services/books/main.py` | 8000 | ListBooksLambda, GetBookLambda | Low | DynamoDB scan/get → MongoDB find |
| `services/cart/main.py` | 8001 | CartLambda | Low | DynamoDB put/get → MongoDB upsert + Redis cache |
| `services/orders/main.py` | 8002 | CheckoutLambda | Medium | SQS publish removed — Change Streams handles downstream |
| `services/search/main.py` | 8003 | SearchLambda | Low | Elasticsearch → OpenSearch (API-compatible) |
| `services/social/main.py` | 8004 | RecommendationsLambda | Medium | Neptune Gremlin → Neo4j Cypher + circuit breaker |
| `services/sync/main.py` | — | DynamoDB Streams + SQS + Lambda | New | Replaces 3 AWS services with 1 Python service |

### Build & Run

```bash
# Build a specific service
docker build -t bookstore/books-svc --build-arg SERVICE=books -f services/Dockerfile .

# Run locally
docker run -p 8000:8000 -e MONGO_URI=mongodb://localhost:27017 bookstore/books-svc

# Build all services (CI/CD)
for svc in books cart orders search social sync; do
  docker build -t ghcr.io/hgbueno/bookstore-${svc}-svc --build-arg SERVICE=${svc} -f services/Dockerfile .
  docker push ghcr.io/hgbueno/bookstore-${svc}-svc
done
```

### Key Paradigm Shifts (Lambda → FastAPI)

| Before (Lambda) | After (FastAPI) |
|-----------------|-----------------|
| New DB connection per invocation | Persistent connection pool at startup |
| `def handler(event, context)` | `@app.get('/books/{id}')` |
| Cold start 50–500ms | Always warm, consistent latency |
| SQS/SNS for event routing | MongoDB Change Streams (sync-svc) |
| CloudWatch Logs | stdout → Loki, Prometheus → Grafana |
| Per-invocation billing | Flat monthly per node |

## Architecture Context

```
User → Cloudflare Pro → NodeBalancer → Kong OSS → Microservices → Databases
                                          ↓                         ↓
                                      Keycloak               MongoDB (primary)
                                     (JWT auth)              OpenSearch (search)
                                                             PostgreSQL (Kong/KC config)
                                                             Redis (sessions/leaderboard)
                                                             Neo4j (recommendations)
```

### Data Flow

1. **DynamoDB → MongoDB**: `migrate_dynamo_to_mongo.py` handles batch migration with DynamoDB-specific type conversions (Decimal, Sets, NULL, nested Maps)
2. **OpenSearch bootstrap**: `bootstrap_opensearch.py` populates the search index from MongoDB with Portuguese analyzer (stemming + asciifolding)
3. **Neo4j bootstrap**: `bootstrap_neo4j.py` rebuilds the recommendation graph from purchase history in MongoDB
4. **Production sync**: `sync_svc.py` uses MongoDB Change Streams to keep OpenSearch and Redis in sync in real time
5. **S3 sync**: `sync_s3_to_akamai.sh` copies static assets using rclone (idempotent, re-executable)

### Key Design Patterns

- **Upsert-based migration**: All scripts use upsert (insert-or-replace), making them idempotent and safe to re-run
- **Derived data rebuilt, not migrated**: OpenSearch and Neo4j are populated from MongoDB — not migrated from Elasticsearch/Neptune
- **Resume token persistence**: sync-svc survives pod restarts by persisting the Change Stream resume token in MongoDB
- **Graceful degradation**: Neo4j failure triggers a circuit breaker; recommendations fall back to MongoDB best sellers

## Stack

| Component | Technology |
|-----------|------------|
| Container Orchestration | LKE (Akamai Kubernetes) |
| API Gateway | Kong OSS |
| Identity Provider | Keycloak |
| Primary Database | MongoDB (replica set) |
| Search Engine | OpenSearch |
| Config Store | PostgreSQL |
| Cache / Leaderboard | Redis Sentinel |
| Graph Database | Neo4j Community |
| Object Storage | Akamai Object Storage (S3-compatible) |
| CDN / WAF | Cloudflare Pro |
| CI/CD | GitHub Actions + GHCR |

## Related

- Migration plan document: presented to BookStore stakeholders
- Infrastructure provisioned via Terraform (LKE, VLANs, WireGuard)
- DR strategy: Pilot Light in Miami (RTO ~1h, RPO ~seconds, $335/mo)
