# Delta Ingestion Service

A backend service that solves a deceptively common problem: you have a destination table with existing records, new data arrives periodically, and you want to insert only what's actually new — without duplicates, without re-processing records that already exist, and without blowing up on large payloads.

Feed it a batch of customer records. It diffs the batch against the database, inserts only the delta, and tells you exactly what was inserted, what was skipped, and what failed. Run the same payload twice — nothing extra gets written.

---

## Quick start (local)

You need Docker. That's it.

```bash
# 1. Clone and enter the project
git clone <repo-url>
cd delta-ingestion-service

# 2. Start Postgres + the app
docker compose up -d

# 3. Check it's up
curl http://localhost:8080/actuator/health
```

Flyway runs migrations and seeds the lookup tables automatically on first boot. No manual setup.

**Default local credentials:** `dev-user` / `dev-pass`

---

## All three APIs at a glance

| Method | Endpoint | What it does |
|---|---|---|
| `POST` | `/api/v1/customers/ingest` | Ingest a batch — inserts only net-new records |
| `POST` | `/api/v1/customers/ingest/dry-run` | Preview what would happen — no writes |
| `POST` | `/api/v1/admin/lookup/refresh` | Force-reload the in-memory lookup cache |

All endpoints require **Basic Auth**.

---

## API reference

### POST /api/v1/customers/ingest

The main endpoint. Accepts a JSON array of customer records, figures out which ones don't yet exist in the database, and inserts only those.

**Local:** `http://localhost:8080/api/v1/customers/ingest`  
**Deployed:** `https://delta-ingestion-service-production-2923.up.railway.app/api/v1/customers/ingest`

**Headers**
```
Content-Type: application/json
Authorization: Basic <base64(username:password)>
```

**Request body**
```json
[
  {
    "externalId": "cust_001",
    "name": "Alice Smith",
    "email": "alice@example.com",
    "countryCode": "US",
    "statusCode": "ACTIVE"
  },
  {
    "externalId": "cust_002",
    "name": "Bob Jones",
    "email": "bob@example.com",
    "countryCode": "IN",
    "statusCode": "INACTIVE"
  }
]
```

| Field | Required | Notes |
|---|---|---|
| `externalId` | yes | Unique ID from your source system — used for dedup |
| `name` | yes | Max 255 chars |
| `email` | yes | Must be a valid email address |
| `countryCode` | yes | Must match a seeded country (e.g. `US`, `IN`, `GB`) |
| `statusCode` | yes | Must match a seeded status (`ACTIVE`, `INACTIVE`, `SUSPENDED`, `PENDING`) |

**Response — 200 OK**
```json
{
  "received": 2,
  "inserted": 1,
  "skipped_existing": 1,
  "failed": 0
}
```

If any records fail (bad country code, unknown status, in-batch duplicate), they come back in a `failures` array alongside the counts. The rest of the batch still processes normally — one bad record doesn't block the others.

```json
{
  "received": 3,
  "inserted": 1,
  "skipped_existing": 1,
  "failed": 1,
  "failures": [
    {
      "externalId": "cust_003",
      "reason": "Unknown country_code: ZZ"
    }
  ]
}
```

**Other responses**

| Status | When |
|---|---|
| `400` | Empty batch or a field fails validation |
| `401` | Missing or wrong credentials |
| `413` | Batch exceeds 100,000 records |

---

### POST /api/v1/customers/ingest/dry-run

Runs the full ingestion logic — deduplication, lookup resolution, existence check — but writes nothing to the database. The response is identical in shape to the real ingest response, so you can use it to preview impact before committing a large batch.

**Local:** `http://localhost:8080/api/v1/customers/ingest/dry-run`  
**Deployed:** `https://delta-ingestion-service-production-2923.up.railway.app/api/v1/customers/ingest/dry-run`

Same request body and headers as `/customers/ingest`. Same response shape, with `inserted` representing *would-be-inserted* count.

---

### POST /api/v1/admin/lookup/refresh

Forces an immediate reload of the in-memory country and status caches from the database. Under normal operation you don't need this — the cache auto-refreshes every 5 minutes. Use it if you've manually added new lookup values and need them to take effect right away.

**Local:** `http://localhost:8080/api/v1/admin/lookup/refresh`  
**Deployed:** `https://delta-ingestion-service-production-2923.up.railway.app/api/v1/admin/lookup/refresh`

**Response — 200 OK**
```
Lookup caches refreshed successfully
```

---

## Swagger UI

Browse and test all three APIs interactively — no external tool needed.

**Local:** [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html)  
**Deployed:** [https://delta-ingestion-service-production-2923.up.railway.app/swagger-ui/index.html](https://delta-ingestion-service-production-2923.up.railway.app/swagger-ui/index.html)

Swagger UI is publicly accessible (no auth to browse). To actually call the endpoints from within Swagger, click the **Authorize** button (top right) and enter your Basic Auth credentials.

---

## Seeded lookup values

These are available out of the box after first boot:

**Country codes:** `US` `IN` `GB` `DE` `CA` `AU` `SG` `JP`

**Status codes:** `ACTIVE` `INACTIVE` `SUSPENDED` `PENDING`

---

## How it handles large batches

Incoming batches are split into chunks of 1,000 records (configurable). Each chunk runs its own bulk existence check and bulk insert in a single transaction. Memory usage stays flat regardless of batch size.

For each chunk:
1. Resolve `countryCode` / `statusCode` → database IDs via in-memory cache
2. `SELECT external_id FROM customers WHERE external_id = ANY(?)` — one query for the whole chunk
3. `INSERT ... ON CONFLICT (external_id) DO NOTHING` — one batched insert for net-new records only

No N+1 queries, no row-by-row processing.

---

## Health and metrics

The actuator runs on port 8081 locally. On Railway (which exposes a single port), `MANAGEMENT_SERVER_PORT` is set to `8080` so health and metrics share the same public URL.

**Local:**
```
GET http://localhost:8081/actuator/health
GET http://localhost:8081/actuator/metrics
GET http://localhost:8081/actuator/prometheus
```

**Deployed (Railway):**
```
GET https://delta-ingestion-service-production-2923.up.railway.app/actuator/health
GET https://delta-ingestion-service-production-2923.up.railway.app/actuator/prometheus
```

Key custom metrics:

| Metric | Type | Description |
|---|---|---|
| `ingestion.records.received` | Counter | Total records received across all batches |
| `ingestion.records.inserted` | Counter | Total records actually inserted |
| `ingestion.records.skipped` | Counter | Total records skipped (already existed) |
| `ingestion.records.failed` | Counter | Total records that failed (bad codes, dupes) |
| `ingestion.batch.duration` | Timer | End-to-end request processing time |
| `ingestion.chunk.duration` | Timer | Time per internal processing chunk |
| `ingestion.success.rate.percent` | Gauge | Success rate (%) of the most recent batch |
| `cache.country.hits/misses` | Counter | Country lookup cache efficiency |
| `cache.status.hits/misses` | Counter | Status lookup cache efficiency |

---

## Configuration

All values can be overridden via environment variables.

| Variable | Default | Description |
|---|---|---|
| `DB_URL` | `jdbc:postgresql://localhost:5432/delta_ingestion` | Postgres connection URL |
| `DB_USERNAME` | `postgres` | Database username |
| `DB_PASSWORD` | `postgres` | Database password |
| `INGESTION_API_USERNAME` | `dev-user` (dev only) | Basic Auth username |
| `INGESTION_API_PASSWORD` | `dev-pass` (dev only) | Basic Auth password |
| `SPRING_PROFILES_ACTIVE` | `dev` | Set to `prod` for production |

Tunable application settings:

| Key | Default | Description |
|---|---|---|
| `ingestion.chunk-size` | `1000` | Records per processing chunk |
| `ingestion.max-batch-size` | `100000` | Max records per request (413 if exceeded) |
| `ingestion.cache-refresh-interval-ms` | `300000` | Lookup cache auto-refresh interval (ms) |

---

## Running tests

No database needed — all repository calls are mocked.

```bash
./mvnw test
```

36 tests across the controller, service, chunk processor, lookup cache, and repository layers.

---

## Stack

- Java 17, Spring Boot 4
- PostgreSQL 15
- Flyway (migrations + seeding)
- JdbcTemplate (bulk operations, no ORM)
- Micrometer + Prometheus (metrics)
- SpringDoc / Swagger UI (API docs)
- Spring Security — Basic Auth, stateless
