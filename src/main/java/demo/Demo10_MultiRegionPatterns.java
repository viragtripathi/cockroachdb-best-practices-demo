package demo;

import javax.sql.DataSource;
import java.sql.*;

/**
 * DEMO 10: Multi-Region Patterns
 * <p>
 * Covers CockroachDB's multi-region SQL abstractions that have no equivalent in
 * PostgreSQL or Oracle. These allow you to control data placement, latency, and
 * survival guarantees at the SQL level.
 * <p>
 * This demo shows the SQL patterns and explains when to use each. The DDL
 * executes successfully on multi-region clusters and shows informative errors
 * on single-region clusters so you can see exactly what's needed.
 * <p>
 * Topics:
 *   1. Database regions and survival goals
 *   2. REGIONAL tables (data homed in one region)
 *   3. REGIONAL BY ROW tables (row-level region pinning)
 *   4. GLOBAL tables (low-latency reads everywhere, higher write latency)
 *   5. Choosing the right locality for each table
 */
public class Demo10_MultiRegionPatterns {

    public static void run(DataSource ds) throws Exception {
        DemoUtils.banner("DEMO 10: Multi-Region Patterns (Data Placement & Survival Goals)");

        System.out.println("""
            CockroachDB is the only SQL database that lets you control data placement,
            read/write latency, and failure survival at the TABLE and ROW level using
            standard SQL. There is no equivalent in PostgreSQL or Oracle.

            Key concepts:
              - CLUSTER REGIONS: set at node startup (--locality=region=us-east-1)
              - DATABASE REGIONS: which cluster regions this database uses
              - SURVIVAL GOALS: survive zone failures (default) or region failures
              - TABLE LOCALITY: REGIONAL (default), REGIONAL BY ROW, or GLOBAL

            This demo walks through the SQL patterns. On a single-region cluster,
            some commands will show expected errors -- this helps you understand
            what a multi-region setup requires.
        """);

        // -----------------------------------------------------------------
        // 1. Check current cluster regions
        // -----------------------------------------------------------------
        DemoUtils.section("1. Current cluster regions");

        boolean isMultiRegion = false;
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SHOW REGIONS FROM CLUSTER")) {
                DemoUtils.log("Cluster regions:");
                int regionCount = 0;
                while (rs.next()) {
                    DemoUtils.log("  " + rs.getString("region") + " (zones: " + rs.getString("zones") + ")");
                    regionCount++;
                }
                if (regionCount <= 1) {
                    DemoUtils.log("  Single-region cluster detected.");
                }
            }

            // The real check: does the current database have a primary region?
            try (ResultSet rs = stmt.executeQuery("SHOW REGIONS FROM DATABASE")) {
                while (rs.next()) {
                    String isPrimary = rs.getString("primary");
                    if ("t".equalsIgnoreCase(isPrimary) || "true".equalsIgnoreCase(isPrimary)) {
                        isMultiRegion = true;
                    }
                }
            } catch (SQLException e) {
                // SHOW REGIONS FROM DATABASE may error if no primary region is set
            }

            if (!isMultiRegion) {
                DemoUtils.log("  Database is NOT multi-region enabled. Multi-region DDL will be skipped.");
                DemoUtils.log("  To test: deploy a 3+ region cluster or use cockroach demo --geo-partitioned-replicas");
            } else {
                DemoUtils.log("  Database IS multi-region enabled.");
            }
        }

        // -----------------------------------------------------------------
        // 2. Database regions and survival goals
        // -----------------------------------------------------------------
        DemoUtils.section("2. Database regions and survival goals");

        System.out.println("""
            To make a database multi-region, add a primary region:

              ALTER DATABASE mydb PRIMARY REGION "us-east-1";
              ALTER DATABASE mydb ADD REGION "us-west-2";
              ALTER DATABASE mydb ADD REGION "eu-west-1";

            Then set the survival goal:

              -- Survive any single zone failure (default, 3+ zones required):
              ALTER DATABASE mydb SURVIVE ZONE FAILURE;

              -- Survive an entire region going down (3+ regions required):
              ALTER DATABASE mydb SURVIVE REGION FAILURE;

            SURVIVE REGION FAILURE requires 3+ regions and adds write latency
            (cross-region Raft consensus) but guarantees zero data loss even if
            an entire cloud region goes offline.
        """);

        if (isMultiRegion) {
            try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SHOW REGIONS FROM DATABASE")) {
                    DemoUtils.log("Current database regions:");
                    while (rs.next()) {
                        DemoUtils.log("  " + rs.getString("region") +
                                " (primary: " + rs.getString("primary") + ")");
                    }
                }
                try (ResultSet rs = stmt.executeQuery("SHOW SURVIVAL GOAL FROM DATABASE")) {
                    if (rs.next()) {
                        DemoUtils.log("Survival goal: " + rs.getString(1));
                    }
                }
            }
        } else {
            DemoUtils.log("Skipping database region commands (database not multi-region enabled)");
        }

        // -----------------------------------------------------------------
        // 3. Table localities
        // -----------------------------------------------------------------
        DemoUtils.section("3. Table locality patterns");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo10_config CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo10_user_profiles CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo10_transactions CASCADE");
        }

        // 3a. REGIONAL TABLE (default)
        DemoUtils.section("3a. REGIONAL TABLE (default -- data homed in primary region)");

        System.out.println("""
            By default, all tables in a multi-region database are REGIONAL tables,
            homed in the database's primary region. Reads and writes from the primary
            region are fast; cross-region access adds latency.

              CREATE TABLE transactions (...);
              -- Implicitly: ALTER TABLE transactions SET LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;

            Use REGIONAL tables for data that is mostly accessed from one region:
              - Configuration tables
              - Reference data
              - Region-specific operational data
        """);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE demo10_transactions (
                    txn_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    account_id UUID NOT NULL,
                    amount DECIMAL(15,2) NOT NULL,
                    region STRING NOT NULL DEFAULT 'us-east',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """);
            DemoUtils.log("Created demo10_transactions (REGIONAL BY TABLE by default)");

            if (isMultiRegion) {
                try (ResultSet rs = stmt.executeQuery(
                        "SHOW CREATE TABLE demo10_transactions")) {
                    while (rs.next()) {
                        DemoUtils.log("  " + rs.getString(2));
                    }
                }
            }
        }

        // 3b. REGIONAL BY ROW
        DemoUtils.section("3b. REGIONAL BY ROW (row-level region pinning)");

        System.out.println("""
            REGIONAL BY ROW lets each row live in a specific region based on a column
            value. CockroachDB adds a hidden 'crdb_region' column and uses it to
            co-locate rows with the users who access them.

              CREATE TABLE user_profiles (
                  user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                  name STRING,
                  email STRING
              ) LOCALITY REGIONAL BY ROW;

            Or pin rows to regions explicitly:

              INSERT INTO user_profiles (crdb_region, name, email)
              VALUES ('us-east-1', 'Alice', 'alice@example.com');

            Use REGIONAL BY ROW for:
              - User profile tables in global apps
              - Any table where different rows are accessed from different regions
              - Multi-tenant tables where tenants are in different regions
        """);

        if (isMultiRegion) {
            try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("""
                    CREATE TABLE demo10_user_profiles (
                        user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name STRING NOT NULL,
                        email STRING NOT NULL
                    ) LOCALITY REGIONAL BY ROW
                """);
                DemoUtils.log("Created demo10_user_profiles with REGIONAL BY ROW");
            }
        } else {
            DemoUtils.log("Cannot create REGIONAL BY ROW table on single-region cluster");
            DemoUtils.log("Required: ALTER DATABASE <db> PRIMARY REGION 'region-name'");

            try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("""
                    CREATE TABLE demo10_user_profiles (
                        user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name STRING NOT NULL,
                        email STRING NOT NULL,
                        region STRING NOT NULL DEFAULT 'us-east'
                    )
                """);
                DemoUtils.log("Created demo10_user_profiles with manual 'region' column as workaround");
                DemoUtils.log("On a multi-region cluster, CockroachDB manages this automatically");
            }
        }

        // 3c. GLOBAL TABLE
        DemoUtils.section("3c. GLOBAL TABLE (low-latency reads from any region)");

        System.out.println("""
            GLOBAL tables use a special "non-blocking transaction" protocol to serve
            reads from ANY region with local latency. Writes are slightly slower
            because they must propagate to all regions.

              CREATE TABLE config (
                  key STRING PRIMARY KEY,
                  value STRING
              ) LOCALITY GLOBAL;

            Use GLOBAL tables for:
              - Reference/lookup data (currency codes, country lists)
              - Configuration tables read by all regions
              - Any table with high read:write ratio accessed globally

            Trade-off: reads are fast everywhere, but writes have higher latency
            (cross-region consensus). Perfect for data that changes rarely.
        """);

        if (isMultiRegion) {
            try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("""
                    CREATE TABLE demo10_config (
                        key STRING PRIMARY KEY,
                        value STRING NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    ) LOCALITY GLOBAL
                """);
                DemoUtils.log("Created demo10_config as GLOBAL table");
            }
        } else {
            DemoUtils.log("Cannot create GLOBAL table on single-region cluster");
            try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("""
                    CREATE TABLE demo10_config (
                        key STRING PRIMARY KEY,
                        value STRING NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """);
                DemoUtils.log("Created demo10_config as standard table (GLOBAL requires multi-region)");
            }
        }

        // -----------------------------------------------------------------
        // 4. Decision matrix
        // -----------------------------------------------------------------
        DemoUtils.section("TABLE LOCALITY DECISION MATRIX");
        System.out.println("""
            | Access Pattern                          | Locality          | Example Table            |
            |-----------------------------------------|-------------------|--------------------------|
            | Mostly accessed from one region          | REGIONAL BY TABLE | orders, transactions     |
            | Different rows accessed from diff regions| REGIONAL BY ROW   | user_profiles, sessions  |
            | Read frequently from all regions         | GLOBAL            | config, currency_rates   |
            | Write-heavy, single region               | REGIONAL BY TABLE | audit_logs               |
            | Multi-tenant, tenants in diff regions    | REGIONAL BY ROW   | tenant_data              |

            SURVIVAL GOALS:
            | Goal                  | Requirement    | Write Latency  | Data Safety              |
            |-----------------------|----------------|----------------|--------------------------|
            | SURVIVE ZONE FAILURE  | 3+ zones       | Low (local)    | Survives 1 zone outage   |
            | SURVIVE REGION FAILURE| 3+ regions     | Higher (x-reg) | Survives 1 region outage |
        """);

        // -----------------------------------------------------------------
        // 5. Multi-region migration checklist
        // -----------------------------------------------------------------
        DemoUtils.section("MULTI-REGION MIGRATION CHECKLIST");
        System.out.println("""
            For teams migrating from single-region PostgreSQL/Oracle to multi-region CockroachDB:

            1. DEPLOY: Start nodes with --locality=region=<region>,zone=<zone>
            2. CONFIGURE: ALTER DATABASE <db> PRIMARY REGION '<region>'
            3. ADD REGIONS: ALTER DATABASE <db> ADD REGION '<region>' (for each)
            4. SET SURVIVAL: ALTER DATABASE <db> SURVIVE ZONE|REGION FAILURE
            5. CLASSIFY TABLES:
               - High-read, low-write, global access -> GLOBAL
               - Row-level regional affinity        -> REGIONAL BY ROW
               - Single-region access               -> REGIONAL BY TABLE (default)
            6. SET LOCALITIES: ALTER TABLE <t> SET LOCALITY <locality>
            7. MONITOR: Use DB Console to verify leaseholder distribution
            8. TEST FAILOVER: Kill a zone/region and verify survival goal holds
            9. SCHEMA CHANGES: Pin system DB lease preferences to one region
               ALTER DATABASE system CONFIGURE ZONE USING
                 lease_preferences = '[[+region=us-east-1]]';

            CockroachDB handles replication, consensus, and data placement automatically
            based on these SQL-level declarations. No application code changes needed
            for data placement -- it's all in the schema.
        """);

        // Cleanup
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS demo10_config CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo10_user_profiles CASCADE");
            stmt.execute("DROP TABLE IF EXISTS demo10_transactions CASCADE");
        }
    }
}
