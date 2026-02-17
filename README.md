# CockroachDB Best Practices & Anti-Patterns Demo

A hands-on Java project that serves as a **Center of Excellence** for CockroachDB usage. Each demo runs a realistic banking scenario and shows the anti-pattern first, then the correct approach -- with actual output you can see in your terminal.

Built on top of the [cockroachdb-jdbc-wrapper](https://github.com/viragtripathi/cockroachdb-jdbc-wrapper) library for automatic transaction retries.

---

## Best Practices Coverage Matrix

The table below maps every major CockroachDB best practice to the demo that covers it. This is the single reference for teams migrating from PostgreSQL, Oracle, or other monolithic databases.

| #  | Best Practice / Anti-Pattern                                          | Demo                                                                                            | Key Takeaway                                                                                              |
|----|-----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| 1  | **Savepoint-based retry does NOT work**                               | [Demo 1](#demo-1-savepoint-anti-pattern)                                                        | CockroachDB aborts the entire txn on 40001. SAVEPOINT rollback cannot fix it. Use full-transaction retry. |
| 2  | **Full-transaction retry with exponential backoff + jitter**          | [Demo 1](#demo-1-savepoint-anti-pattern), [Demo 2](#demo-2-concurrent-payments--40001-handling) | Wrap the entire BEGIN..COMMIT in a retry loop. The wrapper library does this automatically.               |
| 3  | **Serialization conflicts (40001) under contention**                  | [Demo 2](#demo-2-concurrent-payments--40001-handling)                                           | Hot rows cause 40001 errors. Retries with backoff handle them transparently.                              |
| 4  | **Read-write splitting with AS OF SYSTEM TIME**                       | [Demo 3](#demo-3-read-write-split)                                                              | Move heavy reads outside write transactions using historical snapshots. Reduces contention.               |
| 5  | **Batch operations vs row-by-row (N+1 anti-pattern)**                 | [Demo 4](#demo-4-batch-vs-row-by-row)                                                           | Replace per-row INSERTs with addBatch/executeBatch. Eliminates round-trip idle time.                      |
| 6  | **Session guardrails (transaction_rows_read/written_err)**            | [Demo 5](#demo-5-session-guardrails)                                                            | Catch runaway queries and missing WHERE clauses before they cause operational issues.                     |
| 7  | **Large payload / 16MB row limit**                                    | [Demo 6](#demo-6-large-payload--chunking)                                                       | Chunk large documents into smaller rows. Use external storage (S3) for very large payloads.               |
| 8  | **Sequential PK hotspots (SERIAL / auto-increment)**                  | [Demo 7](#demo-7-primary-key-anti-patterns)                                                     | NEVER use SERIAL as a single-column PK. Use UUID with gen_random_uuid().                                  |
| 9  | **Hash-sharded indexes for sequential keys**                          | [Demo 7](#demo-7-primary-key-anti-patterns)                                                     | When you MUST keep time ordering, hash-sharded indexes distribute writes across buckets.                  |
| 10 | **Composite primary keys**                                            | [Demo 7](#demo-7-primary-key-anti-patterns)                                                     | Use well-distributed first column (region, tenant_id) for data co-location + distribution.                |
| 11 | **Covering indexes with STORING clause**                              | [Demo 8](#demo-8-index-best-practices)                                                          | Embed non-key columns in the index to avoid expensive index joins back to the primary index.              |
| 12 | **Partial indexes**                                                   | [Demo 8](#demo-8-index-best-practices)                                                          | Index only rows matching a predicate (e.g., status='ACTIVE'). Reduces size and write amplification.       |
| 13 | **GIN (inverted) indexes on JSONB**                                   | [Demo 8](#demo-8-index-best-practices)                                                          | Enable fast containment queries (@>) on JSON data without full-table scans.                               |
| 14 | **Computed columns from JSONB**                                       | [Demo 8](#demo-8-index-best-practices)                                                          | Extract hot JSON fields into typed columns with B-tree indexes for fast range queries.                    |
| 15 | **Expression indexes**                                                | [Demo 8](#demo-8-index-best-practices)                                                          | Index the result of an expression without adding a stored column.                                         |
| 16 | **Online schema changes run as background JOBS**                      | [Demo 9](#demo-9-online-schema-changes)                                                         | DDL is NOT transactional like in PostgreSQL. Monitor via SHOW JOBS.                                       |
| 17 | **One DDL per transaction**                                           | [Demo 9](#demo-9-online-schema-changes)                                                         | NEVER wrap multiple ALTERs in BEGIN/COMMIT. Each DDL should be an implicit transaction.                   |
| 18 | **Prepared statement invalidation after schema changes**              | [Demo 9](#demo-9-online-schema-changes)                                                         | Always use explicit column lists, never SELECT *. Cached plans break after ALTER TABLE.                   |
| 19 | **autocommit_before_ddl for ORM/migration tool compatibility**        | [Demo 9](#demo-9-online-schema-changes)                                                         | Enables ORMs that wrap DDL in transactions to work correctly with CockroachDB.                            |
| 20 | **Migration tool best practices (Flyway, Liquibase)**                 | [Demo 9](#demo-9-online-schema-changes)                                                         | One DDL per migration step. Use migration tools, not client libraries, for schema changes.                |
| 21 | **Multi-region table localities (REGIONAL, GLOBAL, REGIONAL BY ROW)** | [Demo 10](#demo-10-multi-region-patterns)                                                       | Control data placement at the SQL level. No PostgreSQL/Oracle equivalent.                                 |
| 22 | **Survival goals (ZONE vs REGION failure)**                           | [Demo 10](#demo-10-multi-region-patterns)                                                       | Choose between low-latency writes (zone survival) and cross-region durability (region survival).          |
| 23 | **Multi-region migration checklist**                                  | [Demo 10](#demo-10-multi-region-patterns)                                                       | Step-by-step guide for going from single-region to multi-region CockroachDB.                              |

### CockroachDB Documentation References

| Topic                            | Official Docs                                                                            |
|----------------------------------|------------------------------------------------------------------------------------------|
| Primary key best practices       | https://www.cockroachlabs.com/docs/stable/schema-design-table#primary-key-best-practices |
| Secondary index best practices   | https://www.cockroachlabs.com/docs/stable/schema-design-indexes#best-practices           |
| Hash-sharded indexes             | https://www.cockroachlabs.com/docs/stable/hash-sharded-indexes                           |
| Partial indexes                  | https://www.cockroachlabs.com/docs/stable/partial-indexes                                |
| GIN (inverted) indexes           | https://www.cockroachlabs.com/docs/stable/inverted-indexes                               |
| JSONB data type                  | https://www.cockroachlabs.com/docs/stable/jsonb                                          |
| Online schema changes            | https://www.cockroachlabs.com/docs/stable/online-schema-changes                          |
| Multi-region overview            | https://www.cockroachlabs.com/docs/stable/multiregion-overview                           |
| Schema design overview           | https://www.cockroachlabs.com/docs/stable/schema-design-overview                         |
| Transaction retry best practices | https://www.cockroachlabs.com/docs/stable/transaction-retry-error-reference              |

---

## Prerequisites

- **Java 21+** (JDK)
- **Maven 3.9+**
- **CockroachDB** -- one of:
  - Docker (for local single-node)
  - CockroachDB Cloud cluster (Standard or Dedicated)
  - Self-hosted CockroachDB cluster

---

## Quick Start

### Option A: Local Single-Node (Docker -- quick start)

```bash
docker-compose up -d
mvn compile exec:java
```

This starts a single-node insecure CockroachDB at `localhost:26257`. Good for Demos 1-9. Demo 10 (Multi-Region) will run in informational mode since there's only one region.

### Option B: Local Multi-Region (Docker -- 5 nodes, 3 regions)

For testing Demo 10 and all multi-region patterns locally, use the [cockroach-collections](https://github.com/viragtripathi/cockroach-collections) multi-region Docker setups:

**Insecure (fastest setup):**

```bash
# Download and start 5-node multi-region cluster
wget -c https://github.com/viragtripathi/cockroach-collections/archive/main.zip && \
mkdir -p cockroach-mr-cluster && \
unzip main.zip "cockroach-collections-main/scripts/multi-region/*" -d cockroach-mr-cluster && \
cp -R cockroach-mr-cluster/cockroach-collections-main/scripts/multi-region/* cockroach-mr-cluster/ && \
rm -rf main.zip cockroach-mr-cluster/cockroach-collections-main && \
cd cockroach-mr-cluster && chmod +x *.sh && ./run-insecure.sh

# Run demos (HAProxy on localhost:26257)
cd /path/to/cockroachdb-best-practices-demo
mvn compile exec:java
```

**Secure (TLS + password auth, production-like):**

```bash
# Download and start 5-node secure multi-region cluster
wget -c https://github.com/viragtripathi/cockroach-collections/archive/main.zip && \
mkdir -p cockroach-mr-secure && \
unzip main.zip "cockroach-collections-main/scripts/multi-region-secure/*" -d cockroach-mr-secure && \
cp -R cockroach-mr-secure/cockroach-collections-main/scripts/multi-region-secure/* cockroach-mr-secure/ && \
rm -rf main.zip cockroach-mr-secure/cockroach-collections-main && \
cd cockroach-mr-secure && chmod +x *.sh && ./run-secure.sh

# Run demos with secure credentials (user: craig, password: cockroach)
cd /path/to/cockroachdb-best-practices-demo
export COCKROACH_URL="jdbc:postgresql://localhost:26257/defaultdb?sslmode=require&sslrootcert=/path/to/cockroach-mr-secure/certs/ca.crt"
export COCKROACH_USER="craig"
export COCKROACH_PASSWORD="cockroach"
mvn compile exec:java
```

Both setups provide:
- 5 nodes across 3 regions (us-east-1, us-west-2, us-central-1)
- HAProxy load balancer on port 26257
- Admin UI on ports 8080-8084
- Multi-region database auto-configured via init.sql

| Setup | Regions | Nodes | Auth | Best For |
|-------|---------|-------|------|----------|
| `docker-compose.yml` (included) | 1 | 1 | none | Quick start, Demos 1-9 |
| [multi-region](https://github.com/viragtripathi/cockroach-collections/tree/main/scripts/multi-region) | 3 | 5 | insecure | Demo 10, development |
| [multi-region-secure](https://github.com/viragtripathi/cockroach-collections/tree/main/scripts/multi-region-secure) | 3 | 5 | TLS + password | Demo 10, production-like |

### Option C: CockroachDB Cloud (no code changes needed)

Set environment variables and run:

```bash
export COCKROACH_URL="jdbc:postgresql://<host>:26257/defaultdb?sslmode=verify-full"
export COCKROACH_USER="<username>"
export COCKROACH_PASSWORD="<password>"

mvn compile exec:java
```

Or pass connection details as command-line arguments:

```bash
mvn compile exec:java -Dexec.args="all \
  --url 'jdbc:postgresql://<host>:26257/defaultdb?sslmode=verify-full' \
  --user <username> \
  --password <password>"
```

If your cluster requires a CA certificate:

```bash
export COCKROACH_URL="jdbc:postgresql://<host>:26257/defaultdb?sslmode=verify-full&sslrootcert=/path/to/ca.crt"
export COCKROACH_USER="<username>"
export COCKROACH_PASSWORD="<password>"

mvn compile exec:java
```

To enable multi-region on CockroachDB Cloud (required for Demo 10 full functionality):

```sql
ALTER DATABASE defaultdb PRIMARY REGION "<primary-region>";
ALTER DATABASE defaultdb ADD REGION "<region-2>";
ALTER DATABASE defaultdb ADD REGION "<region-3>";
```

### Connection Configuration Priority

The demos resolve connection settings in this order (first match wins):

1. **Command-line arguments**: `--url`, `--user`, `--password`
2. **Environment variables**: `COCKROACH_URL`, `COCKROACH_USER`, `COCKROACH_PASSWORD`
3. **Default**: `localhost:26257`, user `root`, no password, `sslmode=disable`

### Install the JDBC Wrapper

The demos depend on [cockroachdb-jdbc-wrapper](https://github.com/viragtripathi/cockroachdb-jdbc-wrapper). Install it to your local Maven repo:

```bash
cd /path/to/cockroachdb-jdbc-wrapper
./mvnw install -DskipTests
```

### Run the Demos

```bash
# Interactive menu
mvn compile exec:java

# Run a specific demo
mvn compile exec:java -Dexec.args="7"

# Run all demos
mvn compile exec:java -Dexec.args="all"
```

### Run Tests

```bash
# Against local CockroachDB
mvn test

# Against CockroachDB Cloud
COCKROACH_URL="jdbc:postgresql://..." COCKROACH_USER="..." COCKROACH_PASSWORD="..." mvn test
```

---

## What Each Demo Teaches

### Demo 1: Savepoint Anti-Pattern

**The problem:** Applications use `ROLLBACK TO SAVEPOINT` after a 40001 error, hoping to continue the transaction. CockroachDB aborts the entire transaction on serialization failure -- savepoint rollback cannot fix this.

**The fix:** Wrap the entire transaction in a retry loop with exponential backoff and jitter:

```java
RetryableExecutor executor = new RetryableExecutor();
executor.executeVoid(() -> {
    try (Connection conn = ds.getConnection()) {
        conn.setAutoCommit(false);
        // ... your SQL ...
        conn.commit();
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
});
```

### Demo 2: Concurrent Payments & 40001 Handling

**The problem:** A merchant account receives many concurrent deposits, each doing read-modify-write on the same balance row. Without retry logic, most fail with `TransactionRetryWithProtoRefreshError`.

**The fix:** Same full-transaction retry pattern. The demo shows 10 concurrent deposits with real 40001 errors in the logs and a correct final balance.

### Demo 3: Read-Write Split

**The problem:** Reading a large JSON document and updating status in the same transaction. The transaction holds write intents during the heavy read.

**The fix:** Three phases -- (1) read with `AS OF SYSTEM TIME`, (2) process in app, (3) short write transaction.

### Demo 4: Batch vs Row-by-Row

**The problem:** 500 row-by-row INSERTs = 500 network round trips. Idle time dominates.

**The fix:** Use `addBatch()`/`executeBatch()` or multi-row INSERT. Typical speedup: 2-4x.

### Demo 5: Session Guardrails

**The problem:** Missing WHERE clauses cause full-table scans inside transactions.

**The fix:** `SET transaction_rows_read_err = 200` and `transaction_rows_written_err = 1000` as circuit breakers.

### Demo 6: Large Payload & Chunking

**The problem:** CockroachDB has a ~16MB practical row limit. Large JSON stresses Raft replication.

**The fix:** Chunk into smaller rows, use external storage (S3), or extract hot fields into indexed columns.

### Demo 7: Primary Key Anti-Patterns

**The problem:** Using `SERIAL` or `SEQUENCE` for primary keys (standard in PostgreSQL/Oracle) creates write hotspots in CockroachDB because all inserts land in the same range.

**The fix:**
- **UUID with `gen_random_uuid()`** for most tables (random distribution)
- **Hash-sharded indexes** when you must keep sequential ordering (timestamps, counters)
- **Composite PKs** with a well-distributed first column (region, tenant_id)

### Demo 8: Index Best Practices

Covers five CockroachDB-specific indexing strategies:
1. **Covering indexes (STORING)** -- embed extra columns to avoid index joins
2. **Partial indexes** -- index only active rows to reduce write amplification
3. **GIN indexes on JSONB** -- fast containment queries without full-table scans
4. **Computed columns from JSONB** -- extract hot fields into typed, indexed columns
5. **Expression indexes** -- index an expression result without a stored column

### Demo 9: Online Schema Changes

**The problem:** In PostgreSQL, DDL is transactional. In CockroachDB, schema changes run as background **jobs** and lack full atomicity in multi-statement transactions.

**The fix:**
- One DDL per implicit transaction (no `BEGIN`/`COMMIT` wrapping)
- Use explicit column lists in prepared statements (never `SELECT *`)
- Use `autocommit_before_ddl = on` for ORM compatibility
- Run large backfill schema changes during off-peak hours

### Demo 10: Multi-Region Patterns

Covers CockroachDB's multi-region SQL abstractions (no PostgreSQL/Oracle equivalent):
- **REGIONAL BY TABLE** -- data homed in primary region (default)
- **REGIONAL BY ROW** -- each row pinned to a specific region
- **GLOBAL** -- low-latency reads from any region, higher write latency
- **Survival goals** -- survive zone failures vs region failures

Works on both single-region (shows expected limitations) and multi-region clusters.

---

## Project Structure

```
cockroachdb-best-practices-demo/
  src/main/java/demo/
    DemoRunner.java                       # Interactive menu + CLI entry point
    DemoUtils.java                        # DataSource, env vars, schema setup, logging
    Demo1_SavepointAntiPattern.java       # Savepoint failure + full-txn retry
    Demo2_RetryWithBackoff.java           # Concurrent 40001 conflicts + retry
    Demo3_ReadWriteSplit.java             # AS OF SYSTEM TIME for read-heavy workloads
    Demo4_BatchVsRowByRow.java            # Batching vs N+1 round trips
    Demo5_SessionGuardrails.java          # transaction_rows_read/written_err
    Demo6_LargePayloadAntiPattern.java    # 16MB limit + chunking strategy
    Demo7_PrimaryKeyAntiPatterns.java     # SERIAL hotspot vs UUID vs hash-sharded
    Demo8_IndexBestPractices.java         # STORING, partial, GIN, computed, expression
    Demo9_OnlineSchemaChanges.java        # DDL jobs, migration patterns, prepared stmts
    Demo10_MultiRegionPatterns.java       # REGIONAL, GLOBAL, survival goals
  src/test/java/demo/
    DemoIntegrationTest.java              # 11 integration tests against live CockroachDB
  docker-compose.yml                      # Single-node CockroachDB for local testing
  pom.xml
```

---

## Related Projects

- [cockroachdb-jdbc-wrapper](https://github.com/viragtripathi/cockroachdb-jdbc-wrapper) -- the retry library used by these demos
- [cockroachdb-jdbc-wrapper-demo](https://github.com/viragtripathi/cockroachdb-jdbc-wrapper-demo) -- a simpler "hello world" demo of the wrapper library

---

## License

MIT
