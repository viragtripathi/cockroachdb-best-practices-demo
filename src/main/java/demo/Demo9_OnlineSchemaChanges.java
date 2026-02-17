package demo;

import javax.sql.DataSource;
import java.sql.*;

/**
 * DEMO 9: Online Schema Changes
 * <p>
 * Critical differences from PostgreSQL/Oracle that break migration workflows:
 *   1. Schema changes run as background JOBS, not inline DDL
 *   2. Schema changes inside multi-statement transactions lack full atomicity
 *   3. One DDL per transaction is the recommended pattern
 *   4. Prepared statements can break after schema changes (cached plan invalidation)
 *   5. autocommit_before_ddl setting for ORM/migration tool compatibility
 * <p>
 * In PostgreSQL, you can wrap multiple ALTER TABLEs in a transaction and they either
 * all succeed or all roll back. CockroachDB does NOT guarantee this -- each DDL may
 * commit independently. This demo shows the correct patterns.
 */
public class Demo9_OnlineSchemaChanges {

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 9: Online Schema Changes (DDL != PostgreSQL)");

        System.out.println("""
            In PostgreSQL and Oracle, DDL statements (CREATE, ALTER, DROP) run inside
            transactions with full ACID guarantees. You can wrap multiple schema changes
            in BEGIN/COMMIT and roll them all back on failure.

            CockroachDB is DIFFERENT:
              - DDL runs as background JOBS, not inline operations
              - Schema changes in multi-statement transactions lack full atomicity
              - Each DDL should be in its own implicit transaction (one DDL per statement)
              - Migration tools (Flyway, Liquibase) must run one DDL per migration step

            This is the #2 surprise after sequential PKs for people migrating from
            monolithic databases.
        """);

        // -----------------------------------------------------------------
        // 1. DDL runs as background jobs
        // -----------------------------------------------------------------
        DemoUtils.section("1. Schema changes run as background JOBS");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo9_events CASCADE");
            stmt.execute("""
                CREATE TABLE demo9_events (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    event_type STRING NOT NULL,
                    payload JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """);

            // Add a column -- this runs as a background job
            DemoUtils.log("Running: ALTER TABLE demo9_events ADD COLUMN severity INT DEFAULT 0");
            stmt.execute("ALTER TABLE demo9_events ADD COLUMN severity INT DEFAULT 0");
            DemoUtils.log("ALTER TABLE completed (ran as a background schema change job)");

            // Show the schema change job
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT job_id, job_type, status, description " +
                    "FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE' " +
                    "ORDER BY created DESC LIMIT 3")) {
                DemoUtils.log("Recent schema change jobs:");
                while (rs.next()) {
                    DemoUtils.log(String.format("  Job %s [%s] %s: %.80s",
                            rs.getString("job_id"),
                            rs.getString("status"),
                            rs.getString("job_type"),
                            rs.getString("description")));
                }
            }
        }

        System.out.println("""

            Unlike PostgreSQL where ALTER TABLE blocks and completes inline,
            CockroachDB schema changes:
              - Run as background jobs you can PAUSE, RESUME, or CANCEL
              - Don't block reads or writes while running
              - Can be monitored via SHOW JOBS or the DB Console Jobs page
              - May temporarily use up to 3x storage during backfill
        """);

        // -----------------------------------------------------------------
        // 2. Multi-DDL transactions: lack of atomicity
        // -----------------------------------------------------------------
        DemoUtils.section("2. Multi-DDL transactions lack full atomicity");

        System.out.println("""
            In PostgreSQL, this is safe:
              BEGIN;
              ALTER TABLE events ADD COLUMN col_a INT;
              ALTER TABLE events ADD COLUMN col_b INT;
              COMMIT;  -- both succeed or both roll back

            In CockroachDB, this is RISKY. If col_b fails, col_a may already be
            committed. CockroachDB does NOT guarantee atomicity of schema changes
            within multi-statement transactions.

            CORRECT PATTERN: One DDL per implicit transaction (no BEGIN/COMMIT):
              ALTER TABLE events ADD COLUMN col_a INT;
              ALTER TABLE events ADD COLUMN col_b INT;

            Each runs independently. If the second fails, the first is already done,
            and you can fix and retry the second.
        """);

        // Demonstrate the correct pattern: individual DDL statements
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            DemoUtils.log("CORRECT: Running DDL as individual statements (one DDL per implicit txn):");
            stmt.execute("ALTER TABLE demo9_events ADD COLUMN region STRING DEFAULT 'us-east'");
            DemoUtils.log("  Added column 'region'");
            stmt.execute("ALTER TABLE demo9_events ADD COLUMN priority INT DEFAULT 1");
            DemoUtils.log("  Added column 'priority'");
            stmt.execute("CREATE INDEX idx_events_type ON demo9_events (event_type)");
            DemoUtils.log("  Created index idx_events_type");

            // Verify the schema
            try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM demo9_events")) {
                DemoUtils.log("Current schema of demo9_events:");
                while (rs.next()) {
                    DemoUtils.log(String.format("  %-15s %-15s default=%-20s",
                            rs.getString("column_name"),
                            rs.getString("data_type"),
                            rs.getString("column_default")));
                }
            }
        }

        // -----------------------------------------------------------------
        // 3. CREATE TABLE + DDL in same transaction (exception)
        // -----------------------------------------------------------------
        DemoUtils.section("3. Exception: DDL in same transaction as CREATE TABLE");

        System.out.println("""
            There IS one exception: you CAN run schema changes in the same transaction
            as the CREATE TABLE that creates the table. This works because the table
            doesn't exist yet, so there's no concurrent access concern:

              BEGIN;
              CREATE TABLE new_table (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name STRING);
              ALTER TABLE new_table ADD COLUMN status STRING DEFAULT 'ACTIVE';
              CREATE INDEX idx_name ON new_table (name);
              INSERT INTO new_table (name) VALUES ('test');
              COMMIT;

            This pattern is useful for migration tools that create tables with initial
            indexes and seed data in one step.
        """);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS demo9_new_table CASCADE");
                stmt.execute("""
                    CREATE TABLE demo9_new_table (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name STRING NOT NULL
                    )
                """);
                stmt.execute("ALTER TABLE demo9_new_table ADD COLUMN status STRING DEFAULT 'ACTIVE'");
                stmt.execute("CREATE INDEX idx_new_name ON demo9_new_table (name)");
                stmt.execute("INSERT INTO demo9_new_table (name) VALUES ('test-row')");
                conn.commit();
                DemoUtils.log("CREATE TABLE + ALTER + CREATE INDEX + INSERT all committed in one transaction");
            }
        }

        // -----------------------------------------------------------------
        // 4. Prepared statement invalidation after schema change
        // -----------------------------------------------------------------
        DemoUtils.section("4. Prepared statement invalidation after schema changes");

        System.out.println("""
            When you use prepared statements with SELECT *, adding a column to the
            table invalidates the cached plan:

              PREPARE stmt AS SELECT * FROM my_table;
              ALTER TABLE my_table ADD COLUMN new_col INT;
              EXECUTE stmt;  -- ERROR: cached plan must not change result type

            FIX: Always list explicit columns in prepared statements, not SELECT *.
            This is a best practice in any database, but it's REQUIRED in CockroachDB
            when schema changes happen while the app is running.
        """);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo9_prep_test CASCADE");
            stmt.execute("CREATE TABLE demo9_prep_test (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name STRING)");
            stmt.execute("INSERT INTO demo9_prep_test (name) VALUES ('Alice')");
        }

        // Demonstrate the problem with SELECT *
        try (Connection conn = ds.getConnection()) {
            // Prepare with SELECT * -- this caches the result column set
            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM demo9_prep_test")) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    DemoUtils.log("Before schema change: SELECT * returns " +
                            rs.getMetaData().getColumnCount() + " columns");
                }

                // Now add a column (changes the table schema)
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ALTER TABLE demo9_prep_test ADD COLUMN age INT DEFAULT 0");
                }

                // Re-execute the same prepared statement
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    DemoUtils.log("After schema change: SELECT * still works on same connection " +
                            "(" + rs.getMetaData().getColumnCount() + " columns)");
                    DemoUtils.log("  NOTE: On a different connection or after reconnect, cached plans");
                    DemoUtils.log("  with SELECT * may fail with 'cached plan must not change result type'");
                }
            }
        }

        DemoUtils.log("FIX: Use explicit column lists: SELECT id, name FROM demo9_prep_test");

        // -----------------------------------------------------------------
        // 5. autocommit_before_ddl for ORM compatibility
        // -----------------------------------------------------------------
        DemoUtils.section("5. autocommit_before_ddl for migration tool compatibility");

        System.out.println("""
            Many ORMs and migration tools (Hibernate, Django, some Flyway configs) wrap
            DDL in explicit transactions. Since CockroachDB doesn't support transactional
            DDL with full atomicity, this can cause issues.

            The autocommit_before_ddl session setting fixes this:

              SET autocommit_before_ddl = on;

            When enabled, any DDL inside an explicit transaction automatically commits
            the current transaction first, then runs the DDL as an implicit transaction.

            To enable for all users:
              ALTER ROLE ALL SET autocommit_before_ddl = on;

            Or add to your connection string:
              jdbc:postgresql://host:26257/db?options=-c autocommit_before_ddl=on
        """);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("SET autocommit_before_ddl = on");
            DemoUtils.log("SET autocommit_before_ddl = on");
            DemoUtils.log("Now DDL inside explicit transactions will auto-commit first");
        }

        // -----------------------------------------------------------------
        // Migration tool recommendations
        // -----------------------------------------------------------------
        DemoUtils.section("MIGRATION TOOL BEST PRACTICES");
        System.out.println("""
            | Tool       | Recommendation                                                    |
            |------------|-------------------------------------------------------------------|
            | Flyway     | One DDL statement per migration file (V1__create_table.sql, etc.) |
            | Liquibase  | One changeset per DDL operation                                   |
            | Hibernate  | Use Flyway/Liquibase for DDL; disable hibernate.ddl-auto          |
            | Django     | Use --run-syncdb=false; manage migrations manually                |
            | Custom     | Execute each DDL as a standalone statement, no wrapping txn       |

            General rules:
              1. NEVER wrap multiple DDL in BEGIN/COMMIT (except with CREATE TABLE)
              2. Run large backfill schema changes during off-peak hours
              3. Monitor schema change progress via SHOW JOBS
              4. Use explicit column lists in prepared statements (never SELECT *)
              5. Schema changes on large tables may need 3x storage temporarily
              6. Don't use client libraries for DDL -- use migration tools or cockroach sql
        """);

        // Cleanup
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo9_events CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo9_new_table CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo9_prep_test CASCADE");
        }
    }
}
