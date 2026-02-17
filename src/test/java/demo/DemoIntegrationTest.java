package demo;

import org.junit.jupiter.api.*;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that run against a live CockroachDB instance.
 *
 * Requires CockroachDB running at localhost:26257 (docker-compose up -d).
 * These tests verify the schema setup and core behavior that each demo relies on.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DemoIntegrationTest {

    private static DataSource ds;

    @BeforeAll
    static void setup() throws Exception {
        // Wait for CockroachDB to be available
        ds = DemoUtils.getDataSource();
        int retries = 10;
        while (retries-- > 0) {
            try (Connection conn = ds.getConnection()) {
                break;
            } catch (SQLException e) {
                if (retries == 0) throw new RuntimeException("CockroachDB not available at localhost:26257", e);
                Thread.sleep(1000);
            }
        }
        DemoUtils.setupSchema(ds);
    }

    @Test
    @Order(1)
    void schemaCreatedSuccessfully() throws SQLException {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            // Verify all tables exist
            for (String table : new String[]{"accounts", "payments", "payment_audit_log",
                    "customer_documents", "customer_document_chunks"}) {
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT count(*) FROM information_schema.tables WHERE table_name = '" + table + "'")) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), "Table '" + table + "' should exist");
                }
            }
        }
    }

    @Test
    @Order(2)
    void canInsertAndReadAccounts() throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), ?, 'CHECKING', ?) RETURNING account_id")) {
                ps.setString(1, "Test User");
                ps.setBigDecimal(2, new BigDecimal("5000.00"));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertNotNull(rs.getString(1));
                }
            }
            conn.commit();

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT balance FROM accounts WHERE customer_name = ?")) {
                ps.setString(1, "Test User");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(0, new BigDecimal("5000.00").compareTo(rs.getBigDecimal(1)));
                }
            }
        }
    }

    @Test
    @Order(3)
    void canInsertPayments() throws SQLException {
        String fromId, toId;
        try (Connection conn = ds.getConnection()) {
            // Create two accounts
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), ?, 'CHECKING', 1000.00) RETURNING account_id")) {
                ps.setString(1, "Sender");
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    fromId = rs.getString(1);
                }

                ps.setString(1, "Receiver");
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    toId = rs.getString(1);
                }
            }

            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO payments (from_account_id, to_account_id, amount, status, reference) " +
                    "VALUES (?, ?, 250.00, 'COMPLETED', 'TEST-PAY-001') RETURNING payment_id")) {
                ps.setObject(1, UUID.fromString(fromId));
                ps.setObject(2, UUID.fromString(toId));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertNotNull(rs.getString(1));
                }
            }
            conn.commit();
        }
    }

    @Test
    @Order(4)
    void canInsertAndReadDocuments() throws SQLException {
        String accountId;
        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), 'DocTestUser', 'SAVINGS', 100.00) RETURNING account_id");
                 ResultSet rs = ps.executeQuery()) {
                rs.next();
                accountId = rs.getString(1);
            }

            String json = "{\"name\": \"DocTestUser\", \"risk_score\": 15}";
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO customer_documents (doc_id, account_id, doc_type, payload, status) " +
                    "VALUES (gen_random_uuid(), ?, 'KYC', ?::JSONB, 'PENDING_REVIEW') RETURNING doc_id")) {
                ps.setObject(1, UUID.fromString(accountId));
                ps.setString(2, json);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT payload->>'risk_score' FROM customer_documents WHERE account_id = ?")) {
                ps.setObject(1, UUID.fromString(accountId));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("15", rs.getString(1));
                }
            }
        }
    }

    @Test
    @Order(5)
    void canChunkAndReassembleDocuments() throws SQLException {
        String docId;
        try (Connection conn = ds.getConnection()) {
            // Create an account and doc record
            String accountId;
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), 'ChunkTestUser', 'CHECKING', 100.00) RETURNING account_id");
                 ResultSet rs = ps.executeQuery()) {
                rs.next();
                accountId = rs.getString(1);
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO customer_documents (doc_id, account_id, doc_type, status) " +
                    "VALUES (gen_random_uuid(), ?, 'ONBOARDING', 'PENDING_REVIEW') RETURNING doc_id")) {
                ps.setObject(1, UUID.fromString(accountId));
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    docId = rs.getString(1);
                }
            }

            // Write chunks
            String original = "A".repeat(1000) + "B".repeat(1000) + "C".repeat(500);
            int chunkSize = 1000;
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO customer_document_chunks (doc_id, chunk_index, data) VALUES (?, ?, ?)")) {
                for (int i = 0; i * chunkSize < original.length(); i++) {
                    int start = i * chunkSize;
                    int end = Math.min(start + chunkSize, original.length());
                    ps.setObject(1, UUID.fromString(docId));
                    ps.setInt(2, i);
                    ps.setString(3, original.substring(start, end));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.commit();

            StringBuilder reassembled = new StringBuilder();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT data FROM customer_document_chunks WHERE doc_id = ? ORDER BY chunk_index")) {
                ps.setObject(1, UUID.fromString(docId));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        reassembled.append(rs.getString(1));
                    }
                }
            }
            assertEquals(original, reassembled.toString());
        }
    }

    @Test
    @Order(6)
    void sessionGuardrailBlocksLargeRead() throws SQLException {
        // Seed some rows
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 30; i++) {
                stmt.execute("INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                        "VALUES (gen_random_uuid(), 'GuardrailUser" + i + "', 'CHECKING', 100.00)");
            }
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET transaction_rows_read_err = 5");
                SQLException ex = assertThrows(SQLException.class, () -> {
                    ResultSet rs = stmt.executeQuery("SELECT * FROM accounts");
                    while (rs.next()) {} // drain the result set to trigger the limit
                });
                assertTrue(ex.getMessage().contains("transaction_rows_read_err") ||
                           ex.getMessage().toLowerCase().contains("row count") ||
                           ex.getMessage().contains("above the limit"),
                        "Expected guardrail error but got: " + ex.getMessage());
            }
            conn.rollback();
        }
    }

    @Test
    @Order(7)
    void hashShardedIndexWorks() throws Exception {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_hash_sharded CASCADE");
            stmt.execute("""
                CREATE TABLE test_hash_sharded (
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    event_id UUID NOT NULL DEFAULT gen_random_uuid(),
                    event_type STRING NOT NULL,
                    PRIMARY KEY (created_at, event_id) USING HASH WITH (bucket_count = 4)
                )
            """);
            stmt.execute("INSERT INTO test_hash_sharded (event_type) VALUES ('TEST')");

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test_hash_sharded")) {
                rs.next();
                assertEquals(1, rs.getInt(1));
            }

            // Verify the hash-sharded index created a hidden bucket column
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_name = 'test_hash_sharded' AND is_hidden = 'YES'")) {
                assertTrue(rs.next(), "Hash-sharded index should create a hidden bucket column");
            }
            stmt.execute("DROP TABLE IF EXISTS test_hash_sharded CASCADE");
        }
    }

    @Test
    @Order(8)
    void ginIndexOnJsonbWorks() throws Exception {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_gin CASCADE");
            stmt.execute("""
                CREATE TABLE test_gin (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    metadata JSONB
                )
            """);
            stmt.execute("CREATE INVERTED INDEX idx_test_gin ON test_gin (metadata)");
            stmt.execute("INSERT INTO test_gin (metadata) VALUES ('{\"tier\": \"GOLD\", \"score\": 85}')");
            stmt.execute("INSERT INTO test_gin (metadata) VALUES ('{\"tier\": \"STANDARD\", \"score\": 40}')");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT count(*) FROM test_gin WHERE metadata @> '{\"tier\": \"GOLD\"}'")) {
                rs.next();
                assertEquals(1, rs.getInt(1), "GIN index should find 1 GOLD row");
            }
            stmt.execute("DROP TABLE IF EXISTS test_gin CASCADE");
        }
    }

    @Test
    @Order(9)
    void computedColumnFromJsonbWorks() throws Exception {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_computed CASCADE");
            stmt.execute("""
                CREATE TABLE test_computed (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    data JSONB,
                    score INT AS ((data->>'score')::INT) STORED
                )
            """);
            stmt.execute("INSERT INTO test_computed (data) VALUES ('{\"score\": 95, \"name\": \"Alice\"}')");
            stmt.execute("INSERT INTO test_computed (data) VALUES ('{\"score\": 30, \"name\": \"Bob\"}')");

            try (ResultSet rs = stmt.executeQuery("SELECT score FROM test_computed WHERE score > 50")) {
                assertTrue(rs.next());
                assertEquals(95, rs.getInt(1));
                assertFalse(rs.next(), "Only one row should match score > 50");
            }
            stmt.execute("DROP TABLE IF EXISTS test_computed CASCADE");
        }
    }

    @Test
    @Order(10)
    void onlineSchemaChangeRunsAsJob() throws Exception {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_schema_change CASCADE");
            stmt.execute("CREATE TABLE test_schema_change (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name STRING)");
            stmt.execute("ALTER TABLE test_schema_change ADD COLUMN status STRING DEFAULT 'ACTIVE'");

            // Verify the column was added
            boolean found = false;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_name = 'test_schema_change' AND column_name = 'status'")) {
                found = rs.next();
            }
            assertTrue(found, "ALTER TABLE ADD COLUMN should have added 'status'");
            stmt.execute("DROP TABLE IF EXISTS test_schema_change CASCADE");
        }
    }

    @Test
    @Order(11)
    void asOfSystemTimeReadsWork() throws Exception {
        // Insert a row, wait, then read with AS OF SYSTEM TIME
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO accounts (account_id, customer_name, account_type, balance) " +
                    "VALUES (gen_random_uuid(), 'TimeReadUser', 'CHECKING', 999.99)");
        }

        Thread.sleep(1500); // wait so AS OF SYSTEM TIME '-1s' can see the row

        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT balance FROM accounts AS OF SYSTEM TIME '-1s' WHERE customer_name = 'TimeReadUser'")) {
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal("999.99").compareTo(rs.getBigDecimal(1)));
        }
    }
}
