package demo;

import com.cockroachdb.jdbc.RetryableExecutor;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DEMO 2: Handling TransactionRetryWithProtoRefreshError (40001)
 * <p>
 * Concurrent writers cause serialization conflicts that need proper retry handling.
 * <p>
 * Scenario: A high-traffic merchant account receives hundreds of incoming payments
 * simultaneously (think payroll deposits or marketplace settlements). Every payment
 * does a read-modify-write on the same account balance row, causing heavy contention.
 * <p>
 * Without retry logic, most of these payments would fail with 40001.
 * With RetryableExecutor, every single payment succeeds and the final balance is correct.
 */
public class Demo2_RetryWithBackoff {

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 2: Concurrent Payments & Serialization Conflicts (40001)");

        System.out.println("""
            CockroachDB uses optimistic concurrency control (MVCC with serializable
            isolation). When two transactions read and modify the same row, one gets
            aborted with a 40001 error:

              ERROR: restart transaction: TransactionRetryWithProtoRefreshError

            The application MUST retry the entire transaction. This demo simulates
            a realistic hot-account scenario: a merchant account receiving many
            concurrent payment deposits. Every deposit reads the current balance,
            adds the payment amount, and writes it back -- classic read-modify-write
            contention.
        """);

        // Create a merchant account
        String merchantId;
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM payments WHERE true");
            stmt.execute("DELETE FROM accounts WHERE true");

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), 'Acme Corp Merchant', 'MERCHANT', 0.00) RETURNING account_id");
                 ResultSet rs = ps.executeQuery()) {
                rs.next();
                merchantId = rs.getString(1);
            }
        }
        DemoUtils.log("Merchant account created: " + merchantId);
        DemoUtils.log("Starting balance: $0.00");

        RetryConfig config = RetryConfig.custom()
                .maxAttempts(15)
                .waitDuration(Duration.ofMillis(50))
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(50, 2.0, 0.5))
                .retryOnException(ex -> {
                    boolean retryable = RetryableExecutor.isRetryable(ex);
                    if (retryable) {
                        DemoUtils.log("  RETRY -> " + rootCause(ex));
                    }
                    return retryable;
                })
                .build();
        RetryableExecutor executor = new RetryableExecutor(config);

        int numPayments = 10;
        BigDecimal paymentAmount = new BigDecimal("75.00");
        BigDecimal expectedTotal = paymentAmount.multiply(BigDecimal.valueOf(numPayments));

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numPayments);
        AtomicInteger successCount = new AtomicInteger(0);
        final String mId = merchantId;

        DemoUtils.log("Launching " + numPayments + " concurrent $" + paymentAmount + " deposits...");
        System.out.println();

        try (var pool = Executors.newFixedThreadPool(3)) {
            for (int i = 0; i < numPayments; i++) {
                final int paymentNum = i + 1;
                pool.submit(() -> {
                    try {
                        startGate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    try {
                        executor.executeVoid(() -> {
                            try (Connection conn = ds.getConnection()) {
                                conn.setAutoCommit(false);

                                BigDecimal currentBalance;
                                try (PreparedStatement read = conn.prepareStatement(
                                        "SELECT balance FROM accounts WHERE account_id = ?")) {
                                    read.setObject(1, UUID.fromString(mId));
                                    try (ResultSet rs = read.executeQuery()) {
                                        rs.next();
                                        currentBalance = rs.getBigDecimal(1);
                                    }
                                }

                                try (PreparedStatement write = conn.prepareStatement(
                                        "UPDATE accounts SET balance = ?, updated_at = now() WHERE account_id = ?")) {
                                    write.setBigDecimal(1, currentBalance.add(paymentAmount));
                                    write.setObject(2, UUID.fromString(mId));
                                    write.executeUpdate();
                                }

                                conn.commit();
                                successCount.incrementAndGet();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } catch (Exception e) {
                        DemoUtils.log("Payment #" + paymentNum + " FAILED: " + e.getMessage());
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Release all threads at once for maximum contention
            startGate.countDown();
            doneLatch.await();
        }

        // Verify
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT balance FROM accounts WHERE account_id = ?")) {
            ps.setObject(1, UUID.fromString(merchantId));
            BigDecimal finalBalance;
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                finalBalance = rs.getBigDecimal(1);
            }

            System.out.println();
            DemoUtils.log("Results:");
            DemoUtils.log("  Successful deposits: " + successCount.get() + "/" + numPayments);
            DemoUtils.log("  Final balance:    $" + finalBalance);
            DemoUtils.log("  Expected balance: $" + expectedTotal);
            DemoUtils.log("  Correct: " + (finalBalance.compareTo(expectedTotal) == 0));
        }

        System.out.println("""

            TAKEAWAY: Even under heavy contention on a single row, every transaction
            eventually succeeds with full-transaction retry + exponential backoff.
            The final balance is exact -- no lost updates, no double-counts.

            Without retry logic, most of these deposits would fail with 40001 and
            the merchant would see missing payments.
        """);
    }

    private static String rootCause(Throwable t) {
        while (t.getCause() != null) t = t.getCause();
        return t.getMessage();
    }
}
