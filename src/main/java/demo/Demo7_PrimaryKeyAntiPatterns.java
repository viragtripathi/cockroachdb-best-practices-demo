package demo;

import com.cockroachdb.jdbc.RetryableExecutor;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * DEMO 7: Primary Key Anti-Patterns
 * <p>
 * The #1 mistake people make when migrating from PostgreSQL or Oracle to CockroachDB:
 * using sequential/auto-increment primary keys. In CockroachDB, data is distributed
 * across ranges based on primary key ordering. Sequential keys concentrate all inserts
 * into a single range, creating a "hotspot" that limits write throughput to one node.
 * <p>
 * This demo compares:
 *   1. SERIAL / auto-increment PK (hotspot anti-pattern)
 *   2. UUID with gen_random_uuid() (well-distributed)
 *   3. Hash-sharded index on sequential key (when you MUST keep ordering)
 *   4. Composite PK with well-distributed first column
 */
public class Demo7_PrimaryKeyAntiPatterns {

    private static final int ROW_COUNT = 500;

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 7: Primary Key Anti-Patterns (Sequential vs Distributed Keys)");

        System.out.println("""
            In PostgreSQL and Oracle, using SERIAL or SEQUENCE for primary keys is standard
            practice. In CockroachDB, this creates a dangerous hotspot:

              - CockroachDB distributes data across ranges sorted by primary key
              - Sequential keys (1, 2, 3, ...) all land in the SAME range
              - All inserts hit a single node, limiting throughput
              - This is called a "write hotspot" or "hot range"

            The fix depends on your needs:
              - Use UUID with gen_random_uuid() for random distribution
              - Use hash-sharded indexes when you MUST keep sequential ordering
              - Use composite PKs with a well-distributed first column
        """);

        RetryableExecutor executor = new RetryableExecutor();

        // -----------------------------------------------------------------
        // ANTI-PATTERN: SERIAL / auto-increment primary key
        // -----------------------------------------------------------------
        DemoUtils.section("ANTI-PATTERN: SERIAL (auto-increment) primary key");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo7_serial CASCADE");
            stmt.execute("""
                CREATE TABLE demo7_serial (
                    id INT PRIMARY KEY DEFAULT unique_rowid(),
                    customer_name STRING NOT NULL,
                    balance DECIMAL(15,2) NOT NULL
                )
            """);
        }

        System.out.println("""
            CREATE TABLE demo7_serial (
                id INT PRIMARY KEY DEFAULT unique_rowid(),  -- sequential!
                customer_name STRING, balance DECIMAL
            );

            With unique_rowid() or SERIAL, every new row gets a key larger than the
            previous one. All inserts are appended to the end of the primary index.
            In a multi-node cluster, this means ONE node handles ALL inserts.
        """);

        long serialStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO demo7_serial (customer_name, balance) VALUES (?, ?)")) {
                    for (int i = 1; i <= ROW_COUNT; i++) {
                        ps.setString(1, "Customer " + i);
                        ps.setBigDecimal(2, new java.math.BigDecimal("1000.00"));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long serialMs = System.currentTimeMillis() - serialStart;

        // Show the range distribution
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            DemoUtils.log("Inserted " + ROW_COUNT + " rows with SERIAL PK in " + serialMs + "ms");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT count(DISTINCT range_id) FROM [SHOW RANGES FROM TABLE demo7_serial]")) {
                if (rs.next()) {
                    DemoUtils.log("Data spread across " + rs.getInt(1) + " range(s) -- likely 1 (hotspot!)");
                }
            }
        }

        // -----------------------------------------------------------------
        // CORRECT: UUID primary key with gen_random_uuid()
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT: UUID primary key with gen_random_uuid()");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo7_uuid CASCADE");
            stmt.execute("""
                CREATE TABLE demo7_uuid (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    customer_name STRING NOT NULL,
                    balance DECIMAL(15,2) NOT NULL
                )
            """);
        }

        System.out.println("""
            CREATE TABLE demo7_uuid (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- random!
                customer_name STRING, balance DECIMAL
            );

            UUIDs are randomly generated and uniformly distributed. Inserts spread
            evenly across all ranges and all nodes. This is the RECOMMENDED approach
            for most tables in CockroachDB.
        """);

        long uuidStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO demo7_uuid (customer_name, balance) VALUES (?, ?)")) {
                    for (int i = 1; i <= ROW_COUNT; i++) {
                        ps.setString(1, "Customer " + i);
                        ps.setBigDecimal(2, new java.math.BigDecimal("1000.00"));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long uuidMs = System.currentTimeMillis() - uuidStart;
        DemoUtils.log("Inserted " + ROW_COUNT + " rows with UUID PK in " + uuidMs + "ms");

        // -----------------------------------------------------------------
        // CORRECT: Hash-sharded index on sequential key
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT: Hash-sharded index (when you MUST keep sequential ordering)");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo7_hash_sharded CASCADE");
            stmt.execute("""
                CREATE TABLE demo7_hash_sharded (
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    event_id UUID NOT NULL DEFAULT gen_random_uuid(),
                    event_type STRING NOT NULL,
                    payload JSONB,
                    PRIMARY KEY (created_at, event_id) USING HASH WITH (bucket_count = 8)
                )
            """);
        }

        System.out.println("""
            CREATE TABLE demo7_hash_sharded (
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                event_id UUID NOT NULL DEFAULT gen_random_uuid(),
                event_type STRING NOT NULL,
                payload JSONB,
                PRIMARY KEY (created_at, event_id) USING HASH WITH (bucket_count = 8)
            );

            When you MUST order by timestamp (e.g., event logs, audit trails), use
            hash-sharded indexes. CockroachDB prepends a hidden hash bucket column
            to distribute writes across 8 ranges instead of 1.

            The trade-off: range scans for ORDER BY created_at must fan out across
            all 8 buckets. But write throughput scales linearly with nodes.
        """);

        long hashStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO demo7_hash_sharded (event_type, payload) VALUES (?, ?::JSONB)")) {
                    for (int i = 1; i <= ROW_COUNT; i++) {
                        ps.setString(1, "ACCOUNT_CREATED");
                        ps.setString(2, "{\"customer\": \"Customer " + i + "\"}");
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long hashMs = System.currentTimeMillis() - hashStart;
        DemoUtils.log("Inserted " + ROW_COUNT + " rows with hash-sharded PK in " + hashMs + "ms");

        // -----------------------------------------------------------------
        // CORRECT: Composite PK with well-distributed first column
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT: Composite primary key (well-distributed first column)");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo7_composite CASCADE");
            stmt.execute("""
                CREATE TABLE demo7_composite (
                    region STRING NOT NULL,
                    account_id UUID NOT NULL DEFAULT gen_random_uuid(),
                    customer_name STRING NOT NULL,
                    balance DECIMAL(15,2) NOT NULL,
                    PRIMARY KEY (region, account_id)
                )
            """);
        }

        System.out.println("""
            CREATE TABLE demo7_composite (
                region STRING NOT NULL,
                account_id UUID NOT NULL DEFAULT gen_random_uuid(),
                customer_name STRING, balance DECIMAL,
                PRIMARY KEY (region, account_id)
            );

            Composite PKs are recommended when your queries naturally filter by a
            well-distributed prefix column (e.g., region, tenant_id). The first
            column of the PK determines data distribution across ranges.

            This pattern is ideal for multi-tenant applications: each tenant's data
            is co-located for fast reads, while writes are distributed across regions.
        """);

        String[] regions = {"us-east", "us-west", "eu-west", "ap-south"};
        long compositeStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO demo7_composite (region, customer_name, balance) VALUES (?, ?, ?)")) {
                    for (int i = 1; i <= ROW_COUNT; i++) {
                        ps.setString(1, regions[i % regions.length]);
                        ps.setString(2, "Customer " + i);
                        ps.setBigDecimal(3, new java.math.BigDecimal("1000.00"));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long compositeMs = System.currentTimeMillis() - compositeStart;
        DemoUtils.log("Inserted " + ROW_COUNT + " rows with composite PK in " + compositeMs + "ms");

        // Summary
        DemoUtils.section("COMPARISON");
        System.out.printf("  SERIAL (anti-pattern):  %,5d ms  (all rows in 1 range = hotspot)%n", serialMs);
        System.out.printf("  UUID (recommended):     %,5d ms  (random distribution)%n", uuidMs);
        System.out.printf("  Hash-sharded:           %,5d ms  (8 buckets for sequential keys)%n", hashMs);
        System.out.printf("  Composite PK:           %,5d ms  (distributed by region prefix)%n", compositeMs);

        // -----------------------------------------------------------------
        // WHY one DB node has elevated CPU
        // -----------------------------------------------------------------
        DemoUtils.section("WHY SERIAL PKs cause elevated CPU on ONE node");

        System.out.println("""
            A common observation in production: one CockroachDB node shows 80-90%%
            CPU utilization while the other nodes are idle at 10-20%%. This is almost
            always caused by a write hotspot from sequential primary keys.

            Here's the chain:

            1. SERIAL/SEQUENCE PKs generate monotonically increasing values
            2. All new rows have keys LARGER than existing rows
            3. CockroachDB sorts data by primary key into RANGES
            4. All new inserts land in the LAST range (the one with the highest keys)
            5. That range lives on ONE specific node (the leaseholder)
            6. That ONE node handles ALL inserts -> elevated CPU

            Meanwhile, other nodes are idle because no writes are routed to them.
            The cluster has 5 nodes but only 1 is doing work -- you're paying for
            5x the hardware but getting 1x the throughput.

            Diagnosis in DB Console:
              - Hot Ranges page: one range receiving all QPS
              - Hardware dashboard: CPU skew across nodes
              - SQL Activity: all writes hitting same range_id

            Fix: Switch to UUID with gen_random_uuid() and the writes will distribute
            evenly across ALL nodes. CPU utilization becomes uniform.
        """);

        System.out.println("""

            TAKEAWAY: Coming from PostgreSQL/Oracle, STOP using SERIAL or SEQUENCE
            for primary keys. Instead:
              - Use UUID with gen_random_uuid() for most tables
              - Use hash-sharded indexes when you MUST keep time ordering
              - Use composite PKs for multi-tenant or regionally partitioned data
              - NEVER use monotonically increasing INT as a single-column PK

            To require explicit PKs in your cluster:
              SET CLUSTER SETTING sql.defaults.require_explicit_primary_keys.enabled = true;
        """);

        // Cleanup
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo7_serial CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo7_uuid CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo7_hash_sharded CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo7_composite CASCADE");
        }
    }
}
