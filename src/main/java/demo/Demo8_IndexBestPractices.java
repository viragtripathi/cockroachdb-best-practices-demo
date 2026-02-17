package demo;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;

/**
 * DEMO 8: Index Best Practices
 * <p>
 * Covers CockroachDB-specific indexing strategies that differ from PostgreSQL/Oracle:
 *   1. Covering indexes with STORING clause (avoid index joins)
 *   2. Partial indexes (index only relevant rows)
 *   3. GIN (inverted) indexes on JSONB columns
 *   4. Computed columns extracted from JSONB for fast typed queries
 *   5. Expression indexes on JSONB fields
 * <p>
 * Scenario: A bank's customer accounts table with JSONB metadata. Demonstrates
 * how to go from slow full-table JSON scans to sub-millisecond indexed lookups.
 */
public class Demo8_IndexBestPractices {

    private static final int ROW_COUNT = 200;

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 8: Index Best Practices (Covering, Partial, GIN, Computed Columns)");

        System.out.println("""
            CockroachDB indexing differs from PostgreSQL and Oracle in important ways:

              - STORING clause: include non-indexed columns in the index to avoid
                expensive "index join" lookups back to the primary index
              - Partial indexes: index only a subset of rows (e.g., WHERE status = 'ACTIVE')
                to reduce storage and write amplification
              - GIN (inverted) indexes: enable fast containment queries on JSONB columns
              - Computed columns: extract hot JSON fields into typed, indexable columns
              - Expression indexes: index the result of an expression without a stored column

            In PostgreSQL you might get away with full-table scans on small datasets.
            In CockroachDB (distributed), every missing index means cross-node fan-out.
        """);

        // Seed data
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo8_accounts CASCADE");
            stmt.execute("""
                CREATE TABLE demo8_accounts (
                    account_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    customer_name STRING(200) NOT NULL,
                    account_type STRING(20) NOT NULL DEFAULT 'CHECKING',
                    balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                    status STRING(20) NOT NULL DEFAULT 'ACTIVE',
                    metadata JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """);

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO demo8_accounts (customer_name, account_type, balance, status, metadata) " +
                    "VALUES (?, ?, ?, ?, ?::JSONB)")) {
                for (int i = 1; i <= ROW_COUNT; i++) {
                    ps.setString(1, "Customer " + i);
                    ps.setString(2, i % 3 == 0 ? "SAVINGS" : "CHECKING");
                    ps.setBigDecimal(3, new BigDecimal("1000.00").add(BigDecimal.valueOf(i * 10)));
                    ps.setString(4, i % 5 == 0 ? "SUSPENDED" : "ACTIVE");
                    ps.setString(5, String.format(
                            "{\"risk_score\": %d, \"tier\": \"%s\", \"region\": \"%s\", \"tags\": [\"banking\", \"%s\"]}",
                            i % 100,
                            i % 10 < 3 ? "PLATINUM" : (i % 10 < 6 ? "GOLD" : "STANDARD"),
                            i % 4 == 0 ? "us-east" : (i % 4 == 1 ? "us-west" : (i % 4 == 2 ? "eu-west" : "ap-south")),
                            i % 2 == 0 ? "premium" : "basic"));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        DemoUtils.log("Seeded " + ROW_COUNT + " accounts with JSONB metadata");

        // -----------------------------------------------------------------
        // 1. Covering index with STORING clause
        // -----------------------------------------------------------------
        DemoUtils.section("1. Covering index with STORING clause");

        System.out.println("""
            Without STORING, CockroachDB does an "index join": it finds matching rows
            in the secondary index, then fetches the remaining columns from the primary
            index. With STORING, the extra columns are embedded in the secondary index.

              -- Without STORING (requires index join):
              CREATE INDEX idx_status ON demo8_accounts (status);

              -- With STORING (covering index, no index join needed):
              CREATE INDEX idx_status_covering ON demo8_accounts (status)
                  STORING (customer_name, balance);

            The STORING clause is the CockroachDB equivalent of PostgreSQL's INCLUDE.
        """);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE INDEX idx_status_covering ON demo8_accounts (status) STORING (customer_name, balance)");

            // EXPLAIN the query to show the covering index is used
            try (ResultSet rs = stmt.executeQuery(
                    "EXPLAIN SELECT customer_name, balance FROM demo8_accounts WHERE status = 'ACTIVE'")) {
                DemoUtils.log("EXPLAIN SELECT customer_name, balance WHERE status = 'ACTIVE':");
                while (rs.next()) {
                    DemoUtils.log("  " + rs.getString(1));
                }
            }
        }
        DemoUtils.log("Covering index avoids a round trip back to the primary index");

        // -----------------------------------------------------------------
        // 2. Partial index
        // -----------------------------------------------------------------
        DemoUtils.section("2. Partial index (index only a subset of rows)");

        System.out.println("""
            Partial indexes only include rows matching a WHERE predicate. This is ideal
            for tables where queries typically filter on a specific status:

              CREATE INDEX idx_active_only ON demo8_accounts (customer_name)
                  WHERE status = 'ACTIVE';

            Benefits:
              - Smaller index size (only ACTIVE rows, not SUSPENDED ones)
              - Less write amplification (inserts of SUSPENDED rows skip this index)
              - Faster queries that filter on status = 'ACTIVE'
        """);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE INDEX idx_active_only ON demo8_accounts (customer_name) WHERE status = 'ACTIVE'");

            // Count how many rows are in the partial index vs total
            int total, active;
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM demo8_accounts")) {
                rs.next();
                total = rs.getInt(1);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM demo8_accounts WHERE status = 'ACTIVE'")) {
                rs.next();
                active = rs.getInt(1);
            }
            DemoUtils.log("Total rows: " + total + ", Rows in partial index: " + active +
                    " (" + (100 * active / total) + "% of table)");

            try (ResultSet rs = stmt.executeQuery(
                    "EXPLAIN SELECT customer_name FROM demo8_accounts WHERE status = 'ACTIVE' AND customer_name LIKE 'Customer 1%'")) {
                DemoUtils.log("EXPLAIN SELECT ... WHERE status = 'ACTIVE' AND customer_name LIKE 'Customer 1%':");
                while (rs.next()) {
                    DemoUtils.log("  " + rs.getString(1));
                }
            }
        }

        // -----------------------------------------------------------------
        // 3. GIN (inverted) index on JSONB
        // -----------------------------------------------------------------
        DemoUtils.section("3. GIN (inverted) index on JSONB column");

        System.out.println("""
            GIN indexes allow fast containment queries (@>) on JSONB columns without
            scanning every row. This is critical when filtering on JSON fields in WHERE.

              CREATE INVERTED INDEX idx_metadata_gin ON demo8_accounts (metadata);

            Now queries like these use the GIN index:
              SELECT * FROM demo8_accounts WHERE metadata @> '{"tier": "PLATINUM"}';
              SELECT * FROM demo8_accounts WHERE metadata @> '{"tags": ["premium"]}';
        """);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE INVERTED INDEX idx_metadata_gin ON demo8_accounts (metadata)");

            // Query using containment operator
            long ginStart = System.currentTimeMillis();
            int platinumCount;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT count(*) FROM demo8_accounts WHERE metadata @> '{\"tier\": \"PLATINUM\"}'")) {
                rs.next();
                platinumCount = rs.getInt(1);
            }
            long ginMs = System.currentTimeMillis() - ginStart;
            DemoUtils.log("Found " + platinumCount + " PLATINUM accounts using GIN index in " + ginMs + "ms");

            try (ResultSet rs = stmt.executeQuery(
                    "EXPLAIN SELECT * FROM demo8_accounts WHERE metadata @> '{\"tier\": \"PLATINUM\"}'")) {
                DemoUtils.log("EXPLAIN (using GIN index):");
                while (rs.next()) {
                    DemoUtils.log("  " + rs.getString(1));
                }
            }
        }

        // -----------------------------------------------------------------
        // 4. Computed column from JSONB + standard index
        // -----------------------------------------------------------------
        DemoUtils.section("4. Computed column extracted from JSONB (stored + indexed)");

        System.out.println("""
            When you frequently query a specific JSON field (e.g., risk_score), extract
            it into a typed computed column. This gives you:
              - Strong typing (INT instead of parsing JSON every query)
              - Standard B-tree index (faster than GIN for range queries)
              - No application code changes needed

              ALTER TABLE demo8_accounts ADD COLUMN risk_score INT
                  AS ((metadata->>'risk_score')::INT) STORED;
              CREATE INDEX idx_risk_score ON demo8_accounts (risk_score);

            Now range queries on risk_score use a fast B-tree scan instead of
            parsing JSON for every row.
        """);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE demo8_accounts ADD COLUMN risk_score INT AS ((metadata->>'risk_score')::INT) STORED");
            stmt.execute("CREATE INDEX idx_risk_score ON demo8_accounts (risk_score)");

            // Range query on the computed column
            long compStart = System.currentTimeMillis();
            int highRiskCount;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT count(*) FROM demo8_accounts WHERE risk_score > 80")) {
                rs.next();
                highRiskCount = rs.getInt(1);
            }
            long compMs = System.currentTimeMillis() - compStart;
            DemoUtils.log("Found " + highRiskCount + " high-risk accounts (score > 80) in " + compMs + "ms");

            try (ResultSet rs = stmt.executeQuery(
                    "EXPLAIN SELECT customer_name, risk_score FROM demo8_accounts WHERE risk_score > 80")) {
                DemoUtils.log("EXPLAIN (using B-tree index on computed column):");
                while (rs.next()) {
                    DemoUtils.log("  " + rs.getString(1));
                }
            }
        }

        // -----------------------------------------------------------------
        // 5. Expression index (no stored column needed)
        // -----------------------------------------------------------------
        DemoUtils.section("5. Expression index on JSONB field (without stored column)");

        System.out.println("""
            If you don't want a stored computed column, use an expression index directly:

              CREATE INDEX idx_tier_expr ON demo8_accounts ((metadata->>'tier'));

            This indexes the expression result without adding a column to the table.
            Useful when you only need the index for queries, not for reading the value.
        """);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE INDEX idx_tier_expr ON demo8_accounts ((metadata->>'tier'))");

            try (ResultSet rs = stmt.executeQuery(
                    "EXPLAIN SELECT account_id FROM demo8_accounts WHERE metadata->>'tier' = 'GOLD'")) {
                DemoUtils.log("EXPLAIN SELECT ... WHERE metadata->>'tier' = 'GOLD':");
                while (rs.next()) {
                    DemoUtils.log("  " + rs.getString(1));
                }
            }
        }

        // -----------------------------------------------------------------
        // Summary
        // -----------------------------------------------------------------
        DemoUtils.section("INDEX STRATEGY DECISION TABLE");
        System.out.println("""
            | Use Case                              | Index Type          | Example                                           |
            |---------------------------------------|---------------------|----------------------------------------------------|
            | Query returns non-key columns         | STORING (covering)  | CREATE INDEX ... STORING (name, balance)           |
            | Query filters on specific status      | Partial index       | CREATE INDEX ... WHERE status = 'ACTIVE'           |
            | JSON containment queries (@>)         | GIN (inverted)      | CREATE INVERTED INDEX ... (metadata)               |
            | Frequent range queries on JSON field  | Computed + B-tree   | ADD COLUMN x AS (json->>'f')::INT STORED + INDEX   |
            | Equality queries on JSON field        | Expression index    | CREATE INDEX ... ((metadata->>'tier'))             |
            | Sequential key (timestamps, counters) | Hash-sharded        | PRIMARY KEY (...) USING HASH WITH (bucket_count=8) |

            IMPORTANT: Drop unused indexes! Every secondary index adds write overhead.
            Use SHOW INDEXES and crdb_internal.index_usage_statistics to audit.
        """);

        // Cleanup
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo8_accounts CASCADE");
        }
    }
}
