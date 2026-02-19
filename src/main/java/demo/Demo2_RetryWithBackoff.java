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

        // -----------------------------------------------------------------
        // WHY more pods/threads on hot rows LOWERS throughput
        // -----------------------------------------------------------------
        DemoUtils.section("WHY more pods + more transactions = LOWER TPS on hot rows");

        System.out.println("""
            A common surprise: adding more application pods (or threads) to a workload
            that writes to the SAME hot rows actually DECREASES total throughput.

            Here's why:

            1. CockroachDB uses optimistic concurrency (serializable MVCC).
               Two transactions touching the same row CANNOT commit in parallel --
               one will be aborted with 40001 and must retry.

            2. With N concurrent writers on the same row:
               - Only 1 can commit per "round" (the winner)
               - The other N-1 are aborted and must retry
               - Each retry adds latency (backoff delay + re-execution)
               - Wasted work = (N-1)/N of all attempts

            3. Scaling from 5 pods to 20 pods on a hot-row workload:
               - 5 pods:  ~4 out of 5 attempts wasted per round (~80%% waste)
               - 20 pods: ~19 out of 20 attempts wasted per round (~95%% waste)
               - TPS goes DOWN because more time is spent retrying, not committing

            This is fundamentally different from scaling reads (which do scale linearly).
        """);

        // Demonstrate: 3 threads vs 6 threads on the same hot row
        for (int threadCount : new int[]{3, 6}) {
            // Reset balance
            try (Connection conn = ds.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "UPDATE accounts SET balance = 0.00 WHERE account_id = ?")) {
                ps.setObject(1, UUID.fromString(merchantId));
                ps.executeUpdate();
            }

            int ops = 10;
            CountDownLatch gate = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(ops);
            AtomicInteger retryCount = new AtomicInteger(0);
            AtomicInteger okCount = new AtomicInteger(0);

            RetryConfig scalingConfig = RetryConfig.custom()
                    .maxAttempts(20)
                    .waitDuration(Duration.ofMillis(50))
                    .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(50, 2.0, 0.5))
                    .retryOnException(ex -> {
                        boolean retryable = RetryableExecutor.isRetryable(ex);
                        if (retryable) retryCount.incrementAndGet();
                        return retryable;
                    })
                    .build();
            RetryableExecutor scalingExecutor = new RetryableExecutor(scalingConfig);
            final String mid = merchantId;

            long start = System.currentTimeMillis();
            try (var pool = Executors.newFixedThreadPool(threadCount)) {
                for (int i = 0; i < ops; i++) {
                    pool.submit(() -> {
                        try { gate.await(); } catch (InterruptedException e) { return; }
                        try {
                            scalingExecutor.executeVoid(() -> {
                                try (Connection conn = ds.getConnection()) {
                                    conn.setAutoCommit(false);
                                    BigDecimal bal;
                                    try (PreparedStatement read = conn.prepareStatement(
                                            "SELECT balance FROM accounts WHERE account_id = ?")) {
                                        read.setObject(1, UUID.fromString(mid));
                                        try (ResultSet rs = read.executeQuery()) {
                                            rs.next(); bal = rs.getBigDecimal(1);
                                        }
                                    }
                                    try (PreparedStatement write = conn.prepareStatement(
                                            "UPDATE accounts SET balance = ? WHERE account_id = ?")) {
                                        write.setBigDecimal(1, bal.add(paymentAmount));
                                        write.setObject(2, UUID.fromString(mid));
                                        write.executeUpdate();
                                    }
                                    conn.commit();
                                    okCount.incrementAndGet();
                                } catch (SQLException e) { throw new RuntimeException(e); }
                            });
                        } catch (Exception ignored) {
                        } finally { done.countDown(); }
                    });
                }
                gate.countDown();
                done.await();
            }
            long elapsed = System.currentTimeMillis() - start;
            double tps = (double) okCount.get() / elapsed * 1000;

            DemoUtils.log(String.format(
                    "  %d threads: %d/%d succeeded, %d retries, %d ms, ~%.1f TPS",
                    threadCount, okCount.get(), ops, retryCount.get(), elapsed, tps));
        }

        System.out.println("""

            Notice: more threads = more retries = often LOWER TPS on the same hot row.

            SOLUTIONS to improve throughput under contention:
              1. Eliminate the hot row: use SELECT FOR UPDATE or atomic UPDATE SET
                 balance = balance + amount (single statement, no read-modify-write)
              2. Distribute writes: shard the account across multiple rows
              3. Queue and batch: collect payments, apply in periodic single-writer batch
              4. Use FOR UPDATE locking hints to serialize access explicitly
              5. Reduce transaction scope: fewer statements = shorter conflict window
        """);
    }

    private static String rootCause(Throwable t) {
        while (t.getCause() != null) t = t.getCause();
        return t.getMessage();
    }
}
