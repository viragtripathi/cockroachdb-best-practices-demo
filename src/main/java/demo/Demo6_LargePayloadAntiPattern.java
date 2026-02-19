package demo;

import com.cockroachdb.jdbc.RetryableExecutor;

import javax.sql.DataSource;
import java.sql.*;
import java.util.UUID;

/**
 * DEMO 6: Large Payload Anti-Pattern and Chunking
 * <p>
 * Addresses the practical ~16MB row AND transaction payload limit.
 * <p>
 * The 16MB limit is not just about single-row size. A transaction with 20+ SQL
 * statements, each inserting/updating modest KB-sized rows, can accumulate a
 * total transaction payload that hits the limit. This demo shows both scenarios.
 * <p>
 * Also explains "split failed while applying backpressure to Put" errors.
 * <p>
 * Shows:
 *   - Single large row approaching limits
 *   - Multi-statement transaction accumulating KB-sized rows toward 16MB
 *   - Chunking strategy: split payloads into smaller rows
 *   - Reference strategy: store payloads in object storage with a DB pointer
 *   - Hot field extraction: pull queried fields into indexed columns
 */
public class Demo6_LargePayloadAntiPattern {

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 6: Large Payload Anti-Pattern & Chunking Strategy");

        System.out.println("""
            CockroachDB has a practical limit of ~16MB per row AND per transaction
            payload. This limit applies to the TOTAL data written in a single
            transaction, not just individual rows.

            Two ways to hit this limit:
              1. ONE large row (e.g., a 15MB JSON document in a single INSERT)
              2. MANY moderate rows in one transaction (e.g., 25 INSERTs of 500KB
                 each = 12.5MB total, plus overhead = close to the limit)

            When exceeded, you get errors like:
              ERROR: split failed while applying backpressure to Put[/Table/...]

            This error means a range has grown too large for CockroachDB to split
            efficiently. The Raft replication pipeline stalls because:
              - Each Raft proposal must fit in memory on every replica
              - Large proposals slow down consensus and block other writes
              - The range can't split while a large write is in flight
              - Compaction (LSM merge) falls behind under sustained large writes

            Banking systems hit this with: KYC onboarding packets, signed PDFs
            stored as base64, audit trail JSON, or batch settlement transactions
            that write hundreds of rows per commit.
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

        // -----------------------------------------------------------------
        // SCENARIO 1: Multi-statement transaction accumulating toward 16MB
        // -----------------------------------------------------------------
        DemoUtils.section("SCENARIO 1: Multi-statement transaction accumulating KB-sized rows");
        System.out.println("""
            Real banking transactions often have 20-30+ SQL statements:
              - INSERT audit log entry (~50 KB JSON each)
              - UPDATE account balances
              - INSERT payment records with metadata
              - INSERT compliance check results

            Each statement writes a modest amount, but the TOTAL transaction payload
            is the sum of ALL writes. Here's the math:

              25 statements x 500 KB each = 12.5 MB total transaction payload
              30 statements x 600 KB each = 18 MB -- EXCEEDS THE LIMIT

            This is the scenario most teams miss: no single row is large, but the
            transaction as a whole is too big.
        """);

        // Demonstrate accumulation with KB-sized inserts
        int stmtCount = 25;
        int perStmtKB = 50;  // 50 KB per statement (realistic audit log size)
        String filler = "X".repeat(perStmtKB * 1024);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo6_audit_log CASCADE");
            stmt.execute("""
                CREATE TABLE demo6_audit_log (
                    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    txn_ref STRING NOT NULL,
                    action STRING NOT NULL,
                    details STRING NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """);
        }

        long accumulatedKB = 0;
        long multiStmtStart = System.currentTimeMillis();
        executor.executeVoid(() -> {
            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO demo6_audit_log (txn_ref, action, details) VALUES (?, ?, ?)")) {
                    for (int i = 1; i <= stmtCount; i++) {
                        ps.setString(1, "TXN-2024-" + i);
                        ps.setString(2, "SETTLEMENT_STEP_" + i);
                        ps.setString(3, filler);
                        ps.executeUpdate();
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        long multiStmtMs = System.currentTimeMillis() - multiStmtStart;
        accumulatedKB = (long) stmtCount * perStmtKB;

        DemoUtils.log(stmtCount + " statements x " + perStmtKB + " KB each = " +
                String.format("%,d", accumulatedKB) + " KB total transaction payload");
        DemoUtils.log("Completed in " + multiStmtMs + "ms");
        DemoUtils.log("At this size it works, but scale it up:");
        System.out.println("""

            Transaction size projections:
              25 stmts x  50 KB =  1,250 KB (  1.2 MB) -- OK
              25 stmts x 200 KB =  5,000 KB (  4.9 MB) -- OK but slow Raft
              25 stmts x 500 KB = 12,500 KB ( 12.2 MB) -- DANGER ZONE
              30 stmts x 600 KB = 18,000 KB ( 17.6 MB) -- EXCEEDS 16MB LIMIT
              40 stmts x 500 KB = 20,000 KB ( 19.5 MB) -- EXCEEDS 16MB LIMIT

            FIX: Break multi-statement transactions into smaller batches:
              - Commit every 5-10 statements instead of 25-30
              - Use separate transactions for audit logs vs balance updates
              - Move large payloads (JSON blobs, base64 docs) to separate writes
        """);

        // Show the batched approach
        DemoUtils.section("FIX: Break into smaller transaction batches");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM demo6_audit_log WHERE true");
        }

        int batchSize = 5;
        long batchedStart = System.currentTimeMillis();
        for (int batch = 0; batch < stmtCount / batchSize; batch++) {
            final int batchNum = batch;
            executor.executeVoid(() -> {
                try (Connection conn = ds.getConnection()) {
                    conn.setAutoCommit(false);
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO demo6_audit_log (txn_ref, action, details) VALUES (?, ?, ?)")) {
                        for (int i = 1; i <= batchSize; i++) {
                            int stmtIdx = batchNum * batchSize + i;
                            ps.setString(1, "TXN-2024-" + stmtIdx);
                            ps.setString(2, "SETTLEMENT_STEP_" + stmtIdx);
                            ps.setString(3, filler);
                            ps.executeUpdate();
                        }
                    }
                    conn.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        long batchedMs = System.currentTimeMillis() - batchedStart;
        DemoUtils.log("Batched: " + stmtCount + " statements in " + (stmtCount / batchSize) +
                " batches of " + batchSize + " = " + batchedMs + "ms");
        DemoUtils.log("Each batch: " + (batchSize * perStmtKB) + " KB (well under 16MB limit)");

        // -----------------------------------------------------------------
        // SCENARIO 2: Single large row
        // -----------------------------------------------------------------
        DemoUtils.section("SCENARIO 2: Single large row approaching the limit");

        // Build a ~500KB JSON payload (realistic KYC packet)
        StringBuilder json = new StringBuilder();
        json.append("{\"customer_id\": \"").append(accountId).append("\", ");
        json.append("\"doc_type\": \"ONBOARDING_PACKET\", ");
        json.append("\"sections\": [");
        int sectionCount = 500;
        for (int i = 0; i < sectionCount; i++) {
            if (i > 0) json.append(",");
            json.append("{\"section\": \"").append(i).append("\", ");
            json.append("\"title\": \"Due Diligence Section ").append(i).append("\", ");
            json.append("\"content\": \"").append("X".repeat(900)).append("\", ");
            json.append("\"verified\": ").append(i % 3 == 0).append("}");
        }
        json.append("]}");
        int payloadKB = json.length() / 1024;
        DemoUtils.log("Built onboarding packet: " + String.format("%,d", payloadKB) + " KB");

        System.out.println("""
            Even a single 500KB row causes overhead:
              - Raft replicates the ENTIRE row to all replicas on every write
              - Updating one field in the JSON rewrites the whole row
              - Multiple such rows in one transaction compound the problem

            At 16MB you get hard failures. But even at 1-5MB, performance degrades:
              - Raft proposals take longer to replicate
              - Range splits are blocked during large writes
              - Compaction CPU increases (larger SSTable entries to merge)
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
            DemoUtils.log("Inline insert of " + payloadKB + " KB took " + inlineMs + "ms");
        } catch (Exception e) {
            DemoUtils.log("Inline insert FAILED: " + e.getMessage());
        }

        // -----------------------------------------------------------------
        // CORRECT STRATEGY 1: Chunked storage
        // -----------------------------------------------------------------
        DemoUtils.section("FIX STRATEGY 1: Chunk large payloads into smaller rows");
        System.out.println("""
            Split the large document into fixed-size chunks stored as separate rows.
            Each chunk is well under the limit. Reassemble on read by ordering chunks.

            Recommended chunk size: 64-256 KB (well under range split threshold).
        """);

        int chunkSize = 64 * 1024; // 64 KB per chunk
        String rawPayload = json.toString();
        int numChunks = (int) Math.ceil((double) rawPayload.length() / chunkSize);

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
        DemoUtils.log("Chunked insert: " + numChunks + " chunks of 64 KB in " + chunkMs + "ms");

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
        DemoUtils.log("Reassembled: " + String.format("%,d", reassembled.length() / 1024) +
                " KB (matches original: " + rawPayload.contentEquals(reassembled) + ")");

        // -----------------------------------------------------------------
        // CORRECT STRATEGY 2: External storage reference
        // -----------------------------------------------------------------
        DemoUtils.section("FIX STRATEGY 2: Store in object storage, keep DB reference");
        System.out.println("""
            For documents over ~1MB, store the content in object storage (S3, GCS,
            Azure Blob) and keep only a reference in the database:

              INSERT INTO customer_documents (doc_id, account_id, doc_type, payload, status)
              VALUES (gen_random_uuid(), '...', 'ONBOARDING',
                      '{"storage": "s3", "bucket": "bank-docs", "key": "onboarding/12345.json"}'::JSONB,
                      'PENDING_REVIEW');

            The database row stays tiny (<1 KB). The app fetches the actual content
            from object storage when needed. This completely eliminates the 16MB concern.
        """);

        // -----------------------------------------------------------------
        // CORRECT STRATEGY 3: Extract hot fields
        // -----------------------------------------------------------------
        DemoUtils.section("FIX STRATEGY 3: Extract hot JSON fields into indexed columns");
        System.out.println("""
            If queries frequently filter on JSON fields, extract them into typed
            columns with B-tree indexes:

              ALTER TABLE customer_documents ADD COLUMN customer_id_indexed UUID;
              CREATE INDEX idx_docs_customer ON customer_documents (customer_id_indexed);

            Or add an inverted index on the JSONB column:

              CREATE INVERTED INDEX idx_docs_payload ON customer_documents (payload);

            This converts expensive full-table JSON scans into fast index lookups.
        """);

        // -----------------------------------------------------------------
        // "split failed while applying backpressure" deep dive
        // -----------------------------------------------------------------
        DemoUtils.section("DEEP DIVE: 'split failed while applying backpressure to Put'");
        System.out.println("""
            This error occurs when CockroachDB cannot split a range fast enough to
            accommodate incoming writes. Here's the chain of events:

            1. Data in CockroachDB is divided into RANGES (default ~512 MB each)
            2. When a range grows too large, CockroachDB splits it into two ranges
            3. During a split, the range must pause writes momentarily
            4. If writes are too large or too fast, splits can't keep up
            5. The system applies BACKPRESSURE -- slowing or rejecting writes

            Common causes:
              - Large row values (multi-MB JSON, base64 blobs)
              - High write throughput to a narrow key range (hotspot)
              - Many large rows written in a single transaction
              - Sequential primary keys concentrating all writes in one range

            Solutions:
              1. Keep individual row sizes under 1 MB
              2. Keep total transaction payload well under 16 MB
              3. Break large transactions into smaller commits (5-10 statements)
              4. Use UUID primary keys to distribute writes across ranges
              5. Move large blobs to object storage (S3/GCS)
              6. Use hash-sharded indexes for sequential write patterns
        """);

        System.out.println("""
            SUMMARY: The 16MB limit applies to TOTAL transaction payload, not just
            individual rows. A transaction with 20-30 modest SQL statements can hit
            it. Audit your transaction patterns:

              1. Count statements per transaction (target: <10)
              2. Measure total payload per transaction (target: <4 MB)
              3. Keep individual row sizes under 1 MB
              4. Chunk large documents into 64-256 KB pieces
              5. Store blobs >1 MB in object storage with DB references
              6. Separate audit/logging writes from business logic transactions
        """);

        // Cleanup
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo6_audit_log CASCADE");
        }
    }
}
