package demo;

import com.cockroachdb.jdbc.RetryableExecutor;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;

/**
 * DEMO 4: Batch Operations vs Row-by-Row
 * <p>
 * Addresses keeping transactions short, reducing idle time from per-statement
 * round trips, and avoiding bad SQL patterns.
 * <p>
 * Scenario: End-of-day settlement -- a payment processor needs to create 500
 * customer accounts for a batch onboarding. Compares:
 *   - Row-by-row INSERTs (500 network round trips)
 *   - JDBC addBatch/executeBatch (1 batch round trip)
 *   - Single multi-row INSERT statement (1 round trip)
 * <p>
 * Chatty workloads with many statements per transaction see significant idle
 * time from network round trips -- often ~50% of total transaction latency.
 */
public class Demo4_BatchVsRowByRow {

    private static final int ROW_COUNT = 500;

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 4: Batch Operations vs Row-by-Row (N+1 Anti-Pattern)");

        System.out.printf("""
            Each SQL statement inside a transaction is a network round trip to the
            CockroachDB cluster. More statements = more idle time waiting for responses.

            Chatty workloads with ~90 statements per transaction (e.g. ~34 SELECTs
            and ~18 DMLs) lose about half their latency to idle/round-trip overhead
            -- not actual database work.

            This demo creates %d customer accounts three different ways
            and compares the time.
        %n""", ROW_COUNT);

        RetryableExecutor executor = new RetryableExecutor();

        // -----------------------------------------------------------------
        // ANTI-PATTERN: Row-by-row inserts
        // -----------------------------------------------------------------
        DemoUtils.section("ANTI-PATTERN: " + ROW_COUNT + " separate INSERT statements");

        resetAccounts(ds);

        long rowByRowStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO accounts (account_id, customer_name, account_type, balance, currency) " +
                        "VALUES (gen_random_uuid(), ?, 'CHECKING', ?, 'USD')")) {
                    for (int i = 1; i <= ROW_COUNT; i++) {
                        ps.setString(1, "Customer " + i);
                        ps.setBigDecimal(2, new BigDecimal("1000.00"));
                        ps.executeUpdate();
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long rowByRowMs = System.currentTimeMillis() - rowByRowStart;
        DemoUtils.log("Row-by-row: " + ROW_COUNT + " inserts in " + rowByRowMs + "ms (" + ROW_COUNT + " round trips + COMMIT)");

        // -----------------------------------------------------------------
        // CORRECT: JDBC batch
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT: JDBC addBatch/executeBatch");

        resetAccounts(ds);

        long batchStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO accounts (account_id, customer_name, account_type, balance, currency) " +
                        "VALUES (gen_random_uuid(), ?, 'CHECKING', ?, 'USD')")) {
                    for (int i = 1; i <= ROW_COUNT; i++) {
                        ps.setString(1, "Customer " + i);
                        ps.setBigDecimal(2, new BigDecimal("1000.00"));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long batchMs = System.currentTimeMillis() - batchStart;
        DemoUtils.log("Batched: " + ROW_COUNT + " inserts in " + batchMs + "ms (1 batch + COMMIT)");

        // -----------------------------------------------------------------
        // ALSO CORRECT: Multi-row INSERT
        // -----------------------------------------------------------------
        DemoUtils.section("ALSO CORRECT: Single multi-row INSERT");

        resetAccounts(ds);

        long multiRowStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);

                StringBuilder sql = new StringBuilder(
                        "INSERT INTO accounts (account_id, customer_name, account_type, balance, currency) VALUES ");
                for (int i = 1; i <= ROW_COUNT; i++) {
                    if (i > 1) sql.append(", ");
                    sql.append("(gen_random_uuid(), 'Customer ").append(i).append("', 'CHECKING', 1000.00, 'USD')");
                }

                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(sql.toString());
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long multiRowMs = System.currentTimeMillis() - multiRowStart;
        DemoUtils.log("Multi-row: " + ROW_COUNT + " inserts in " + multiRowMs + "ms (1 statement + COMMIT)");

        // Summary
        DemoUtils.section("COMPARISON");
        System.out.printf("  Row-by-row:  %,5d ms  (%d round trips)%n", rowByRowMs, ROW_COUNT);
        System.out.printf("  Batched:     %,5d ms  (1 batch)%n", batchMs);
        System.out.printf("  Multi-row:   %,5d ms  (1 statement)%n", multiRowMs);
        float speedup = (float) rowByRowMs / Math.max(1, batchMs);
        System.out.printf("  Batch is ~%.1fx faster than row-by-row%n", speedup);

        System.out.println("""

            TAKEAWAY: Replace row-by-row INSERT/UPDATE loops with batched or
            multi-row statements. This directly reduces the "idle time" that
            observed in chatty workloads (round trips can be ~50%% of txn latency).

            For batch UPDATEs, use:
              UPDATE accounts SET balance = CASE account_id
                WHEN '...' THEN 100.00
                WHEN '...' THEN 200.00
              END WHERE account_id IN ('...', '...')
        """);
    }

    private static void resetAccounts(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM payments WHERE true");
            stmt.execute("DELETE FROM customer_documents WHERE true");
            stmt.execute("DELETE FROM accounts WHERE true");
        }
    }
}
