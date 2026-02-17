package demo;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class DemoUtils {

    static final String LOCAL_JDBC_URL = "jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable";
    static final String LOCAL_USER = "root";
    static final String LOCAL_PASSWORD = "";

    static final String DEFAULT_JDBC_URL = env("COCKROACH_URL", LOCAL_JDBC_URL);
    static final String DEFAULT_USER = env("COCKROACH_USER", LOCAL_USER);
    static final String DEFAULT_PASSWORD = env("COCKROACH_PASSWORD", LOCAL_PASSWORD);

    private DemoUtils() {}

    private static String env(String name, String fallback) {
        String val = System.getenv(name);
        return (val != null && !val.isEmpty()) ? val : fallback;
    }

    public static DataSource getDataSource() {
        return getDataSource(DEFAULT_JDBC_URL, DEFAULT_USER, DEFAULT_PASSWORD);
    }

    public static DataSource getDataSource(String url, String user, String password) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(url);
        ds.setUser(user);
        if (password != null && !password.isEmpty()) {
            ds.setPassword(password);
        }
        return ds;
    }

    public static void banner(String title) {
        String line = "=".repeat(72);
        System.out.println();
        System.out.println(line);
        System.out.println("  " + title);
        System.out.println(line);
        System.out.println();
    }

    public static void section(String label) {
        System.out.println();
        System.out.println("--- " + label + " ---");
        System.out.println();
    }

    public static void log(String msg) {
        System.out.printf("[%s] %s%n", Thread.currentThread().getName(), msg);
    }

    public static void setupSchema(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS payment_audit_log CASCADE");
            stmt.execute("DROP TABLE IF EXISTS payments CASCADE");
            stmt.execute("DROP TABLE IF EXISTS customer_documents CASCADE");
            stmt.execute("DROP TABLE IF EXISTS customer_document_chunks CASCADE");
            stmt.execute("DROP TABLE IF EXISTS accounts CASCADE");

            stmt.execute("""
                CREATE TABLE accounts (
                    account_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    customer_name STRING NOT NULL,
                    account_type STRING NOT NULL DEFAULT 'CHECKING',
                    currency STRING NOT NULL DEFAULT 'USD',
                    balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                    status STRING NOT NULL DEFAULT 'ACTIVE',
                    metadata JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """);

            stmt.execute("""
                CREATE TABLE payments (
                    payment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    from_account_id UUID NOT NULL REFERENCES accounts(account_id),
                    to_account_id UUID NOT NULL REFERENCES accounts(account_id),
                    amount DECIMAL(15,2) NOT NULL,
                    currency STRING NOT NULL DEFAULT 'USD',
                    payment_type STRING NOT NULL DEFAULT 'TRANSFER',
                    status STRING NOT NULL DEFAULT 'PENDING',
                    reference STRING,
                    payload JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """);

            stmt.execute("""
                CREATE TABLE payment_audit_log (
                    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    payment_id UUID NOT NULL,
                    action STRING NOT NULL,
                    details JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """);

            stmt.execute("""
                CREATE TABLE customer_documents (
                    doc_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    account_id UUID NOT NULL REFERENCES accounts(account_id),
                    doc_type STRING NOT NULL DEFAULT 'KYC',
                    payload JSONB,
                    status STRING NOT NULL DEFAULT 'PENDING_REVIEW',
                    reviewer STRING,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """);

            stmt.execute("""
                CREATE TABLE customer_document_chunks (
                    doc_id UUID NOT NULL,
                    chunk_index INT NOT NULL,
                    data STRING NOT NULL,
                    PRIMARY KEY (doc_id, chunk_index)
                )
            """);
        }
    }
}
