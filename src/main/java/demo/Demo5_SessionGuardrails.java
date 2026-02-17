package demo;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;

/**
 * DEMO 5: Session Guardrails
 * <p>
 * Addresses transaction size limits and preventing runaway queries.
 * <p>
 * Scenario: A bank's operations team wants to prevent runaway queries in production.
 * CockroachDB provides session-level settings that abort transactions exceeding
 * configured row read/write limits.
 * <p>
 * This catches anti-patterns like:
 *   - Full table scans inside transactions (missing WHERE clauses)
 *   - Bulk updates that should be chunked
 *   - Queries missing indexes that scan entire tables
 */
public class Demo5_SessionGuardrails {

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 5: Session Guardrails (Preventing Runaway Transactions)");

        System.out.println("""
            CockroachDB provides session-level guardrails that abort transactions
            when they exceed configured limits:

              SET transaction_rows_read_err = <N>;
              SET transaction_rows_written_err = <N>;

            These act as circuit breakers: if a query accidentally does a full table
            scan or a bulk update without a proper WHERE clause, CockroachDB stops
            it before it causes operational problems.

            Use these in development and testing to catch bad queries early.
        """);

        // Seed 100 accounts
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM payments WHERE true");
            stmt.execute("DELETE FROM customer_documents WHERE true");
            stmt.execute("DELETE FROM accounts WHERE true");

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance, currency) " +
                    "VALUES (gen_random_uuid(), ?, 'CHECKING', ?, 'USD')")) {
                for (int i = 1; i <= 100; i++) {
                    ps.setString(1, "Customer " + i);
                    ps.setBigDecimal(2, new BigDecimal("1000.00").add(BigDecimal.valueOf(i)));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        DemoUtils.log("Seeded 100 customer accounts");

        // -----------------------------------------------------------------
        // Guardrail: transaction_rows_read_err
        // -----------------------------------------------------------------
        DemoUtils.section("Guardrail: transaction_rows_read_err");

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET transaction_rows_read_err = 20");
                DemoUtils.log("SET transaction_rows_read_err = 20");
                DemoUtils.log("Attempting to SELECT all 100 accounts (no WHERE clause)...");

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM accounts")) {
                    int count = 0;
                    while (rs.next()) count++;
                    DemoUtils.log("Read " + count + " rows -- should have been blocked");
                } catch (SQLException e) {
                    DemoUtils.log("BLOCKED: " + e.getMessage());
                    System.out.println("""

                  The transaction was aborted because it read more than 20 rows.
                  This catches missing WHERE clauses and full-table-scan anti-patterns.
                  In production, this prevents a single bad query from scanning millions of rows.
                   \s""");
                }
            }
            conn.rollback();
        }

        // -----------------------------------------------------------------
        // Guardrail: transaction_rows_written_err
        // -----------------------------------------------------------------
        DemoUtils.section("Guardrail: transaction_rows_written_err");

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET transaction_rows_written_err = 10");
                DemoUtils.log("SET transaction_rows_written_err = 10");
                DemoUtils.log("Attempting bulk UPDATE on all 100 accounts...");

                try {
                    stmt.executeUpdate("UPDATE accounts SET balance = balance + 1.00");
                    DemoUtils.log("Updated all rows -- should have been blocked");
                } catch (SQLException e) {
                    DemoUtils.log("BLOCKED: " + e.getMessage());
                    System.out.println("""

                  The transaction was aborted because it wrote more than 10 rows.
                  This catches bulk updates that should be chunked into smaller batches.
                   \s""");
                }
            }
            conn.rollback();
        }

        // -----------------------------------------------------------------
        // Within limits: targeted query succeeds
        // -----------------------------------------------------------------
        DemoUtils.section("Within limits: targeted query succeeds");

        // Create an index so targeted queries don't need a full table scan
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_accounts_name ON accounts (customer_name)");
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET transaction_rows_read_err = 50");
                stmt.execute("SET transaction_rows_written_err = 50");
                DemoUtils.log("Both guardrails set to 50");
                DemoUtils.log("(Note: CockroachDB counts MVCC-level KV reads, not just logical rows.");
                DemoUtils.log(" An UPDATE of 3 rows may internally read index entries + primary rows");
                DemoUtils.log(" + MVCC versions, so the actual read count is higher than 3.)");

                int count = 0;
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT customer_name, balance FROM accounts " +
                        "WHERE customer_name IN ('Customer 1', 'Customer 2', 'Customer 3')")) {
                    while (rs.next()) {
                        if (count == 0) DemoUtils.log("Selected 3 accounts by indexed name:");
                        DemoUtils.log("  " + rs.getString(1) + ": $" + rs.getBigDecimal(2));
                        count++;
                    }
                }

                // Targeted update using the same indexed column
                stmt.executeUpdate(
                        "UPDATE accounts SET balance = balance + 10.00 " +
                        "WHERE customer_name IN ('Customer 1', 'Customer 2', 'Customer 3')");
                DemoUtils.log("Updated 3 accounts (within limit)");

                conn.commit();
                DemoUtils.log("Transaction committed successfully");
            }
        }

        System.out.println("""

            TAKEAWAY: Use session guardrails during development and testing:

              SET transaction_rows_read_err = 200;
              SET transaction_rows_written_err = 1000;

            Start strict and loosen as you verify queries are optimized.
            This forces developers to:
              - Add proper WHERE clauses and LIMIT
              - Create indexes for frequently-queried columns
              - Chunk large bulk operations into smaller transactions
        """);
    }
}
