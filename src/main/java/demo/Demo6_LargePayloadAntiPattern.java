package demo;

import com.cockroachdb.jdbc.RetryableExecutor;

import javax.sql.DataSource;
import java.sql.*;
import java.util.UUID;

/**
 * DEMO 6: Large Payload Anti-Pattern and Chunking
 * <p>
 * Addresses the practical ~16MB row/transaction payload limit.
 * <p>
 * Scenario: A bank stores customer onboarding packets -- these include scanned
 * documents, signed agreements, and extensive KYC data as JSON. Some packets
 * approach or exceed the practical 16MB row size limit.
 * <p>
 * Shows:
 *   - Large inline payloads stress Raft replication and increase compaction CPU
 *   - Chunking strategy: split payloads into smaller rows
 *   - Reference strategy: store payloads in object storage with a DB pointer
 *   - Hot field extraction: pull queried fields into indexed columns
 */
public class Demo6_LargePayloadAntiPattern {

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 6: Large Payload Anti-Pattern & Chunking Strategy");

        System.out.println("""
            CockroachDB has a practical limit of ~16MB per row/transaction payload.
            Large rows stress Raft replication, increase compaction CPU, and can
            trigger "split failed while applying backpressure" errors.

            This is common in banking: KYC onboarding packets, signed PDFs stored
            as base64, or large audit trail JSON can easily approach this limit.
        """);

        // Seed an account
        String accountId;
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM customer_document_chunks WHERE true");
            stmt.execute("DELETE FROM customer_documents WHERE true");
            stmt.execute("DELETE FROM payments WHERE true");
            stmt.execute("DELETE FROM accounts WHERE true");

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), 'Global Industries Ltd', 'CORPORATE', 500000.00) RETURNING account_id");
                 ResultSet rs = ps.executeQuery()) {
                rs.next();
                accountId = rs.getString(1);
            }
        }

        RetryableExecutor executor = new RetryableExecutor();

        // Build a ~2MB JSON payload simulating a large onboarding packet
        // (We use 2MB instead of 16MB to keep the demo fast on local instances)
        StringBuilder json = new StringBuilder();
        json.append("{\"customer_id\": \"").append(accountId).append("\", ");
        json.append("\"doc_type\": \"ONBOARDING_PACKET\", ");
        json.append("\"sections\": [");
        int sectionCount = 2000;
        for (int i = 0; i < sectionCount; i++) {
            if (i > 0) json.append(",");
            json.append("{\"section\": \"").append(i).append("\", ");
            json.append("\"title\": \"Due Diligence Section ").append(i).append("\", ");
            json.append("\"content\": \"").append("X".repeat(900)).append("\", ");
            json.append("\"verified\": ").append(i % 3 == 0).append("}");
        }
        json.append("]}");
        int payloadBytes = json.length();
        DemoUtils.log("Built onboarding packet: " + String.format("%,.0f", (double) payloadBytes / 1024) + " KB");

        // -----------------------------------------------------------------
        // ANTI-PATTERN: Store the entire payload in one row
        // -----------------------------------------------------------------
        DemoUtils.section("ANTI-PATTERN: Store entire " + String.format("%,.0f", (double) payloadBytes / 1024) + " KB payload in one row");
        System.out.println("""
            Storing a multi-MB JSON document as a single JSONB row works for small
            payloads, but at scale this causes:
              - Slow Raft replication (entire payload replicated on every write)
              - High compaction CPU
              - Range split backpressure under load
              - Increased 40001 retry probability (larger txn = more conflict window)
        """);

        long inlineStart = System.currentTimeMillis();
        try {
            executor.executeVoid(() -> {
                try (Connection conn = ds.getConnection()) {
                    conn.setAutoCommit(false);
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO customer_documents (doc_id, account_id, doc_type, payload, status) " +
                            "VALUES (gen_random_uuid(), ?, 'ONBOARDING', ?::JSONB, 'PENDING_REVIEW')")) {
                        ps.setObject(1, UUID.fromString(accountId));
                        ps.setString(2, json.toString());
                        ps.executeUpdate();
                    }
                    conn.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            long inlineMs = System.currentTimeMillis() - inlineStart;
            DemoUtils.log("Inline insert took " + inlineMs + "ms");
            DemoUtils.log("This works at 2MB but degrades badly approaching 16MB");
        } catch (Exception e) {
            DemoUtils.log("Inline insert FAILED: " + e.getMessage());
        }

        // -----------------------------------------------------------------
        // CORRECT STRATEGY 1: Chunked storage
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT STRATEGY 1: Chunk payload into smaller rows");
        System.out.println("""
            Split the large document into fixed-size chunks stored as separate rows.
            Each chunk is well under the limit. Reassemble on read by ordering chunks.
        """);

        int chunkSize = 256 * 1024; // 256 KB per chunk
        String rawPayload = json.toString();
        int numChunks = (int) Math.ceil((double) rawPayload.length() / chunkSize);

        // First, create a document record (small, just metadata)
        String chunkedDocId;
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO customer_documents (doc_id, account_id, doc_type, status) " +
                     "VALUES (gen_random_uuid(), ?, 'ONBOARDING', 'PENDING_REVIEW') RETURNING doc_id")) {
            ps.setObject(1, UUID.fromString(accountId));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                chunkedDocId = rs.getString(1);
            }
        }

        long chunkStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO customer_document_chunks (doc_id, chunk_index, data) VALUES (?, ?, ?)")) {
                    for (int i = 0; i < numChunks; i++) {
                        int start = i * chunkSize;
                        int end = Math.min(start + chunkSize, rawPayload.length());
                        ps.setObject(1, UUID.fromString(chunkedDocId));
                        ps.setInt(2, i);
                        ps.setString(3, rawPayload.substring(start, end));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long chunkMs = System.currentTimeMillis() - chunkStart;
        DemoUtils.log("Chunked insert: " + numChunks + " chunks in " + chunkMs + "ms");

        // Verify reassembly
        StringBuilder reassembled = new StringBuilder();
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT data FROM customer_document_chunks WHERE doc_id = ? ORDER BY chunk_index")) {
            ps.setObject(1, UUID.fromString(chunkedDocId));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    reassembled.append(rs.getString(1));
                }
            }
        }
        DemoUtils.log("Reassembled: " + String.format("%,.0f", (double) reassembled.length() / 1024) +
                " KB (matches original: " + rawPayload.contentEquals(reassembled) + ")");

        // -----------------------------------------------------------------
        // CORRECT STRATEGY 2: External storage reference
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT STRATEGY 2: Store in object storage, keep DB reference");
        System.out.println("""
            For very large documents (10MB+), store the actual content in object
            storage (S3, GCS, Azure Blob) and keep only a reference in the database:

              INSERT INTO customer_documents (doc_id, account_id, doc_type, payload, status)
              VALUES (gen_random_uuid(), '...', 'ONBOARDING',
                      '{"storage": "s3", "bucket": "bank-docs", "key": "onboarding/12345.json"}'::JSONB,
                      'PENDING_REVIEW');

            The database row stays tiny. The app fetches the actual content from
            object storage when needed.
        """);

        // -----------------------------------------------------------------
        // CORRECT STRATEGY 3: Extract hot fields
        // -----------------------------------------------------------------
        DemoUtils.section("CORRECT STRATEGY 3: Extract hot JSON fields into indexed columns");
        System.out.println("""
            If queries frequently filter on JSON fields (e.g., "find all KYC docs
            for customer X" or "find all docs with risk_score > 50"), extract those
            fields into typed columns with B-tree indexes:

              ALTER TABLE customer_documents ADD COLUMN customer_id_indexed UUID;
              CREATE INDEX idx_docs_customer ON customer_documents (customer_id_indexed);

            Or add an inverted index on the JSONB column for flexible queries:

              CREATE INVERTED INDEX idx_docs_payload ON customer_documents (payload);

            This converts expensive full-table JSON scans into fast index lookups,
            dramatically reducing transaction duration and retry probability.
        """);

        System.out.println("""
            TAKEAWAY: Keep row sizes well under 16MB. For large payloads:
              1. Chunk into multiple rows (shown above)
              2. Store in object storage with a DB reference
              3. Extract frequently-queried JSON fields into indexed columns

            Audit which documents exceed 1MB and apply one of these strategies.
            Add inverted indexes on JSONB columns used in WHERE clauses to
            eliminate full-table scans inside transactions.
        """);
    }
}
