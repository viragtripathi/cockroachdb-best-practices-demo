package demo;

import com.cockroachdb.jdbc.RetryableExecutor;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DEMO 1: Savepoint Anti-Pattern vs Full-Transaction Retry
 * <p>
 * Why SAVEPOINTs don't work as a retry mechanism in CockroachDB.
 * <p>
 * Shows why using SAVEPOINTs to "recover" from a retryable 40001 error does NOT
 * work in CockroachDB. The server aborts the entire transaction on a serialization
 * failure -- partial rollback via SAVEPOINT cannot fix this.
 * <p>
 * Then demonstrates the correct pattern: wrapping the entire transaction in a
 * retry loop using RetryableExecutor with exponential backoff + jitter.
 * <p>
 * Scenario: Two bank customers, Alice and Bob, transfer money back and forth.
 * Under contention, savepoint-based recovery fails while full-txn retry succeeds
 * and preserves the total balance (conservation of money).
 */
public class Demo1_SavepointAntiPattern {

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 1: Savepoint Anti-Pattern vs Full-Transaction Retry");

        // Seed two customer accounts
        String aliceId, bobId;
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM payments WHERE true");
            stmt.execute("DELETE FROM accounts WHERE true");

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), ?, 'CHECKING', ?) RETURNING account_id")) {
                ps.setString(1, "Alice Johnson");
                ps.setBigDecimal(2, new BigDecimal("10000.00"));
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    aliceId = rs.getString(1);
                }

                ps.setString(1, "Bob Martinez");
                ps.setBigDecimal(2, new BigDecimal("10000.00"));
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    bobId = rs.getString(1);
                }
            }
        }

        DemoUtils.log("Created accounts: Alice=" + aliceId + ", Bob=" + bobId);
        DemoUtils.log("Starting balance: Alice=$10,000.00, Bob=$10,000.00, Total=$20,000.00");

        // -----------------------------------------------------------------
        // ANTI-PATTERN: Savepoint-based retry
        // -----------------------------------------------------------------
        DemoUtils.section("ANTI-PATTERN: Savepoint-based retry (DO NOT USE)");
        System.out.println("""
            Many applications use SAVEPOINTs inside transactions,
            hoping to ROLLBACK TO SAVEPOINT after a 40001 error and continue:

              BEGIN;
              SAVEPOINT before_debit;
              UPDATE accounts SET balance = balance - 500 WHERE account_id = '...';
              -- 40001 occurs here under contention
              ROLLBACK TO SAVEPOINT before_debit;   -- DOES NOT WORK
              -- Transaction is in ABORTED state, no further statements succeed
              COMMIT;                                -- FAILS

            CockroachDB requires the ENTIRE transaction to restart on a 40001 error.
            SAVEPOINT rollback does NOT replay server-side MVCC state. The transaction
            remains in an aborted state and all subsequent statements will fail.

            In practice, heavy SAVEPOINT usage (e.g. ~34 per txn) adds a network
            round trip for each one while providing zero retry benefit.
        """);

        demonstrateSavepointFailure(ds, aliceId);

        // -----------------------------------------------------------------
        // CORRECT: Full-transaction retry with RetryableExecutor
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT: Full-transaction retry with RetryableExecutor");
        System.out.println("""
            The correct pattern wraps the ENTIRE transaction (BEGIN through COMMIT) in
            a retry loop with exponential backoff + jitter. On a retryable error, the
            whole transaction is rolled back and re-executed from scratch.

            This is the ONLY correct approach for CockroachDB's optimistic concurrency.
        """);

        // Reset balances
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE accounts SET balance = 10000.00 WHERE account_id IN ('" + aliceId + "', '" + bobId + "')");
        }

        demonstrateCorrectRetry(ds, aliceId, bobId);
    }

    private static void demonstrateSavepointFailure(DataSource ds, String aliceId) {
        DemoUtils.log("Attempting a transfer using SAVEPOINT-based error handling...");

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            Savepoint sp = conn.setSavepoint("before_debit");
            DemoUtils.log("  SAVEPOINT before_debit created");

            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE accounts SET balance = balance - 500.00 WHERE account_id = ?")) {
                ps.setObject(1, UUID.fromString(aliceId));
                ps.executeUpdate();
                DemoUtils.log("  Debited Alice $500 (no contention in this single-thread demo)");
            }

            // Simulate what happens when a 40001 arrives and you try SAVEPOINT recovery
            DemoUtils.log("  Simulating a 40001 error scenario...");
            try {
                conn.rollback(sp);
                DemoUtils.log("  ROLLBACK TO SAVEPOINT succeeded -- but this is MISLEADING.");
                DemoUtils.log("  In a real 40001 scenario, the server aborts the entire txn.");
                DemoUtils.log("  The savepoint rollback either fails or leaves the txn unusable.");
            } catch (SQLException e) {
                DemoUtils.log("  ROLLBACK TO SAVEPOINT failed: " + e.getMessage());
            }

            conn.rollback();
            DemoUtils.log("  Full ROLLBACK to clean up");

        } catch (SQLException e) {
            DemoUtils.log("  Error: " + e.getMessage());
        }

        System.out.println("""

            TAKEAWAY: Even if ROLLBACK TO SAVEPOINT doesn't error in a non-conflict
            scenario, under real contention the transaction is ABORTED by the server.
            You CANNOT continue -- you must restart the entire transaction.
        """);
    }

    private static void demonstrateCorrectRetry(DataSource ds, String aliceId, String bobId) throws Exception {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(10)
                .waitDuration(Duration.ofMillis(50))
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(50, 2.0, 0.5))
                .retryOnException(ex -> {
                    boolean retryable = RetryableExecutor.isRetryable(ex);
                    if (retryable) {
                        DemoUtils.log("  -> Retryable error, will retry: " + rootCause(ex));
                    }
                    return retryable;
                })
                .build();
        RetryableExecutor executor = new RetryableExecutor(config);

        int numTransfers = 10;
        CountDownLatch latch = new CountDownLatch(numTransfers);
        AtomicInteger successCount = new AtomicInteger(0);

        DemoUtils.log("Starting " + numTransfers + " concurrent $50 transfers (Alice <-> Bob)...");

        try (var pool = Executors.newFixedThreadPool(3)) {
            for (int i = 0; i < numTransfers; i++) {
                final String fromId = (i % 2 == 0) ? aliceId : bobId;
                final String toId = fromId.equals(aliceId) ? bobId : aliceId;

                pool.submit(() -> {
                    try {
                        executor.executeVoid(() -> {
                            try (Connection conn = ds.getConnection()) {
                                conn.setAutoCommit(false);
                                try (PreparedStatement debit = conn.prepareStatement(
                                        "UPDATE accounts SET balance = balance - 50.00, updated_at = now() WHERE account_id = ?");
                                     PreparedStatement credit = conn.prepareStatement(
                                        "UPDATE accounts SET balance = balance + 50.00, updated_at = now() WHERE account_id = ?")) {
                                    debit.setObject(1, UUID.fromString(fromId));
                                    debit.executeUpdate();
                                    credit.setObject(1, UUID.fromString(toId));
                                    credit.executeUpdate();
                                }
                                conn.commit();
                                successCount.incrementAndGet();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } catch (Exception e) {
                        DemoUtils.log("  Transfer FAILED after retries: " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
        }

        // Verify money conservation
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT customer_name, balance FROM accounts ORDER BY customer_name")) {
            System.out.println();
            DemoUtils.log("Results: " + successCount.get() + "/" + numTransfers + " transfers completed");
            BigDecimal total = BigDecimal.ZERO;
            while (rs.next()) {
                BigDecimal bal = rs.getBigDecimal("balance");
                total = total.add(bal);
                DemoUtils.log("  " + rs.getString("customer_name") + ": $" + bal);
            }
            DemoUtils.log("  Total: $" + total + " (must equal $20,000.00 -- conservation of money)");
        }

        System.out.println("""

            TAKEAWAY: Full-transaction retry with exponential backoff + jitter handles
            contention gracefully. All transfers complete, total balance is conserved.
            No savepoints needed. This is the pattern CockroachDB requires.
        """);
    }

    private static String rootCause(Throwable t) {
        while (t.getCause() != null) t = t.getCause();
        return t.getMessage();
    }
}
