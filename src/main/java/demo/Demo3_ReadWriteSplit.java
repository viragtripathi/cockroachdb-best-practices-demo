package demo;

import com.cockroachdb.jdbc.RetryableExecutor;

import javax.sql.DataSource;
import java.sql.*;
import java.util.UUID;

/**
 * DEMO 3: Read-Write Split and AS OF SYSTEM TIME
 * <p>
 * Addresses idle time, long-running transactions, and JSON-heavy reads
 * inside write transactions.
 * <p>
 * Scenario: A KYC (Know Your Customer) review workflow in a banking platform.
 * A compliance officer opens a customer's KYC document (large JSON payload with
 * identity details, risk scores, address history, etc.), reviews it, then updates
 * the status to APPROVED or REJECTED.
 * <p>
 * Anti-pattern: read the large KYC JSON and update the status in one long transaction.
 * Correct: read the KYC document outside the write txn (using AS OF SYSTEM TIME for
 * a conflict-free snapshot), process it, then do a short write-only txn.
 */
public class Demo3_ReadWriteSplit {

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 3: Read-Write Split & AS OF SYSTEM TIME");

        // Seed an account and a KYC document with realistic JSON
        String accountId, docId;
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM customer_documents WHERE true");
            stmt.execute("DELETE FROM payments WHERE true");
            stmt.execute("DELETE FROM accounts WHERE true");

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), 'Maria Chen', 'SAVINGS', 25000.00) RETURNING account_id");
                 ResultSet rs = ps.executeQuery()) {
                rs.next();
                accountId = rs.getString(1);
            }

            // Realistic KYC payload
            String kycJson = """
                {
                    "customer_id": "%s",
                    "full_name": "Maria Chen",
                    "date_of_birth": "1985-03-14",
                    "nationality": "US",
                    "tax_id": "XXX-XX-1234",
                    "risk_score": 23,
                    "risk_category": "LOW",
                    "pep_status": false,
                    "sanctions_check": "CLEAR",
                    "source_of_funds": "EMPLOYMENT",
                    "employer": "TechNova Inc.",
                    "annual_income_range": "100000-250000",
                    "addresses": [
                        {"type": "PRIMARY", "street": "742 Evergreen Terrace", "city": "Springfield", "state": "IL", "zip": "62704", "country": "US", "since": "2019-06-01"},
                        {"type": "PREVIOUS", "street": "123 Oak Street", "city": "Chicago", "state": "IL", "zip": "60601", "country": "US", "since": "2015-01-15", "until": "2019-05-31"}
                    ],
                    "identity_documents": [
                        {"type": "PASSPORT", "number": "XXXXXXXXX", "issuing_country": "US", "expiry": "2029-07-22", "verified": true},
                        {"type": "DRIVERS_LICENSE", "number": "XXXXXXXXX", "issuing_state": "IL", "expiry": "2027-03-14", "verified": true}
                    ],
                    "account_purpose": "PERSONAL_SAVINGS",
                    "expected_monthly_volume": 5000,
                    "review_history": [
                        {"date": "2024-01-15", "reviewer": "jsmith", "action": "INITIAL_REVIEW", "notes": "Standard onboarding, all documents verified"},
                        {"date": "2024-07-20", "reviewer": "kpatel", "action": "PERIODIC_REVIEW", "notes": "No changes, risk score stable"}
                    ]
                }
                """.formatted(accountId);

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO customer_documents (doc_id, account_id, doc_type, payload, status) " +
                    "VALUES (gen_random_uuid(), ?, 'KYC', ?::JSONB, 'PENDING_REVIEW') RETURNING doc_id")) {
                ps.setObject(1, UUID.fromString(accountId));
                ps.setString(2, kycJson);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    docId = rs.getString(1);
                }
            }
        }

        DemoUtils.log("Account: " + accountId + " (Maria Chen)");
        DemoUtils.log("KYC Document: " + docId);

        RetryableExecutor executor = new RetryableExecutor();

        // -----------------------------------------------------------------
        // ANTI-PATTERN: Read JSON + Write in the SAME transaction
        // -----------------------------------------------------------------
        DemoUtils.section("ANTI-PATTERN: KYC read + status update in same transaction");
        System.out.println("""
            This pattern reads a large KYC JSON payload and updates the review status
            in the SAME transaction. The transaction holds write intents while parsing
            the heavy JSON, increasing duration, idle time, and conflict probability.

            In real-world evaluations, transactions with ~90 statements, ~34 SELECTs,
            and ~18 DMLs showed about half the txn time as idle/round-trip overhead.
        """);

        // Reset status
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(
                "UPDATE customer_documents SET status = 'PENDING_REVIEW' WHERE doc_id = ?")) {
            ps.setObject(1, UUID.fromString(docId));
            ps.executeUpdate();
        }

        long antiPatternStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);

                String payload;
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT payload FROM customer_documents WHERE doc_id = ?")) {
                    ps.setObject(1, UUID.fromString(docId));
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        payload = rs.getString(1);
                    }
                }
                DemoUtils.log("Read KYC payload inside write txn (" + payload.length() + " chars)");

                // Simulate compliance officer reviewing the document (app-side processing)
                boolean approved = payload.contains("\"risk_category\": \"LOW\"");

                // Update status in the same transaction
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE customer_documents SET status = ?, reviewer = 'demo_user', updated_at = now() WHERE doc_id = ?")) {
                    ps.setString(1, approved ? "APPROVED" : "REJECTED");
                    ps.setObject(2, UUID.fromString(docId));
                    ps.executeUpdate();
                }

                conn.commit();
                DemoUtils.log("Committed (read + review + write all in one txn)");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long antiPatternMs = System.currentTimeMillis() - antiPatternStart;
        DemoUtils.log("Anti-pattern took " + antiPatternMs + "ms");

        // -----------------------------------------------------------------
        // CORRECT: Read outside txn, then short write txn
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT: Read outside txn (AS OF SYSTEM TIME), short write txn");
        System.out.println("""
            Phase 1: Read the KYC document OUTSIDE the write transaction using
                     AS OF SYSTEM TIME for a consistent, conflict-free snapshot.
                     This read holds NO write intents.

            Phase 2: Process the document in the application layer (parse JSON,
                     run compliance checks, compute risk scores).

            Phase 3: Start a SHORT write-only transaction to update the status.
                     Fast, narrow, and unlikely to conflict.
        """);

        // Reset status
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(
                "UPDATE customer_documents SET status = 'PENDING_REVIEW' WHERE doc_id = ?")) {
            ps.setObject(1, UUID.fromString(docId));
            ps.executeUpdate();
        }

        // Brief pause so AS OF SYSTEM TIME '-1s' can see the data
        DemoUtils.log("Waiting briefly so the historical snapshot can see existing data...");
        Thread.sleep(1500);

        long correctStart = System.currentTimeMillis();

        // Phase 1: Read outside any write transaction
        String payload;
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT payload FROM customer_documents AS OF SYSTEM TIME '-1s' WHERE doc_id = ?")) {
            ps.setObject(1, UUID.fromString(docId));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                payload = rs.getString(1);
            }
        }
        DemoUtils.log("Phase 1: Read KYC payload outside txn (" + payload.length() + " chars)");

        // Phase 2: App-side processing
        boolean approved = payload.contains("\"risk_category\": \"LOW\"");
        String decision = approved ? "APPROVED" : "REJECTED";
        DemoUtils.log("Phase 2: Compliance review complete -> " + decision);

        // Phase 3: Short write-only transaction
        final String finalDocId = docId;
        final String finalDecision = decision;
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE customer_documents SET status = ?, reviewer = 'demo_user', updated_at = now() WHERE doc_id = ?")) {
                    ps.setString(1, finalDecision);
                    ps.setObject(2, UUID.fromString(finalDocId));
                    ps.executeUpdate();
                }
                conn.commit();
                DemoUtils.log("Phase 3: Short write txn committed");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long correctMs = System.currentTimeMillis() - correctStart;
        DemoUtils.log("Correct pattern took " + correctMs + "ms");

        System.out.println("""

            TAKEAWAY: Splitting a long read+write transaction into:
              1. A conflict-free read (AS OF SYSTEM TIME)
              2. App-side processing
              3. A short write-only transaction

            ...reduces transaction duration, idle time, write intent lifetime,
            and 40001 retry probability. This is the recommended refactoring for
            JSON-heavy transaction patterns common in banking applications.
        """);
    }
}
