package demo;

import javax.sql.DataSource;
import java.util.Scanner;

/**
 * Main entry point. Run all demos or pick one interactively.
 * <p>
 * Usage:
 *   mvn compile exec:java                       -- interactive menu (local CockroachDB)
 *   mvn compile exec:java -Dexec.args="all"     -- run all demos
 *   mvn compile exec:java -Dexec.args="1"       -- run demo 1 only
 *   mvn compile exec:java -Dexec.args="1 --url jdbc:postgresql://host:26257/db --user root --password secret"
 */
public class DemoRunner {

    public static void main(String[] args) throws Exception {
        String url = DemoUtils.DEFAULT_JDBC_URL;
        String user = DemoUtils.DEFAULT_USER;
        String password = DemoUtils.DEFAULT_PASSWORD;
        String demoChoice = null;

        // Parse args
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--url" -> url = args[++i];
                case "--user" -> user = args[++i];
                case "--password" -> password = args[++i];
                default -> { if (demoChoice == null) demoChoice = args[i]; }
            }
        }

        DataSource ds = DemoUtils.getDataSource(url, user, password);

        System.out.println("""

            ╔══════════════════════════════════════════════════════════════════╗
            ║        CockroachDB Best Practices & Anti-Patterns Demo           ║
            ║                                                                  ║
            ║  Demonstrates correct vs incorrect usage patterns for            ║
            ║  CockroachDB with real-world banking scenarios.                  ║
            ╚══════════════════════════════════════════════════════════════════╝
        """);

        DemoUtils.log("Connecting to: " + url);

        // Setup schema
        DemoUtils.log("Setting up schema...");
        DemoUtils.setupSchema(ds);
        DemoUtils.log("Schema ready.");

        if (demoChoice == null) {
            // Interactive menu
            try (Scanner scanner = new Scanner(System.in)) {
                while (true) {
                    printMenu();
                    System.out.print("  Choice: ");
                    String choice = scanner.nextLine().trim();
                    if (choice.equalsIgnoreCase("q")) break;
                    if (choice.equalsIgnoreCase("all")) {
                        runAll(ds);
                        continue;
                    }
                    runDemo(ds, choice);
                }
            }
        } else if (demoChoice.equalsIgnoreCase("all")) {
            runAll(ds);
        } else {
            runDemo(ds, demoChoice);
        }
    }

    private static void printMenu() {
        System.out.println("""

            Available demos:
              1   Savepoint Anti-Pattern vs Full-Transaction Retry
              2   Concurrent Payments & Serialization Conflicts (40001)
              3   Read-Write Split & AS OF SYSTEM TIME
              4   Batch Operations vs Row-by-Row
              5   Session Guardrails
              6   Large Payload Anti-Pattern & Chunking
              7   Primary Key Anti-Patterns (Sequential vs UUID vs Hash-Sharded)
              8   Index Best Practices (Covering, Partial, GIN, Computed Columns)
              9   Online Schema Changes (DDL Jobs, Migration Patterns)
              10  Multi-Region Patterns (REGIONAL, GLOBAL, Survival Goals)

              all  Run all demos in sequence
              q    Quit
        """);
    }

    private static void runDemo(DataSource ds, String choice) throws Exception {
        // Re-setup schema for a clean state before each demo
        DemoUtils.setupSchema(ds);

        switch (choice) {
            case "1" -> Demo1_SavepointAntiPattern.run(ds);
            case "2" -> Demo2_RetryWithBackoff.run(ds);
            case "3" -> Demo3_ReadWriteSplit.run(ds);
            case "4" -> Demo4_BatchVsRowByRow.run(ds);
            case "5" -> Demo5_SessionGuardrails.run(ds);
            case "6" -> Demo6_LargePayloadAntiPattern.run(ds);
            case "7" -> Demo7_PrimaryKeyAntiPatterns.run(ds);
            case "8" -> Demo8_IndexBestPractices.run(ds);
            case "9" -> Demo9_OnlineSchemaChanges.run(ds);
            case "10" -> Demo10_MultiRegionPatterns.run(ds);
            default -> System.out.println("  Unknown demo: " + choice);
        }
    }

    private static void runAll(DataSource ds) throws Exception {
        for (String demo : new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}) {
            runDemo(ds, demo);
        }
        printBadSqlPatterns();
        DemoUtils.banner("ALL DEMOS COMPLETE");
    }

    static void printBadSqlPatterns() {
        DemoUtils.banner("CONSOLIDATED: Bad SQL Patterns to Avoid in CockroachDB");
        System.out.println("""
            This is the single-page reference of SQL anti-patterns that cause
            production issues in CockroachDB. Each is demonstrated in the demos above.

            ┌──────────────────────────────────────────────────────────────────────────┐
            │  BAD SQL PATTERN                        │  WHY IT'S BAD       │  DEMO    │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  SAVEPOINT + ROLLBACK TO SAVEPOINT      │  40001 aborts entire│  Demo 1  │
            │  as retry mechanism                     │  txn, not fixable   │          │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  Read-modify-write on hot rows           │  40001 retries,     │  Demo 2 │
            │  without retry logic                     │  lost updates       │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  SELECT large JSON + UPDATE in same txn  │  Long txn duration, │  Demo 3 │
            │                                          │  idle time, 40001   │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  Row-by-row INSERT/UPDATE in a loop      │  N round trips,     │  Demo 4 │
            │  (N+1 pattern)                           │  ~50%% idle time    │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  SELECT * without WHERE or LIMIT         │  Full table scan,   │  Demo 5 │
            │  inside a transaction                    │  blocks other txns  │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  UPDATE without WHERE clause             │  Bulk write, can    │  Demo 5 │
            │                                          │  exceed row limits  │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  20+ statements in one transaction       │  Accumulates toward │  Demo 6 │
            │  (each writing KB-sized payloads)        │  16MB txn limit     │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  Storing multi-MB JSON blobs inline      │  Slow Raft, range   │  Demo 6 │
            │                                          │  split backpressure │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  SERIAL/SEQUENCE primary keys            │  Write hotspot on   │  Demo 7 │
            │                                          │  one node, CPU skew │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  Missing STORING clause on indexes       │  Index join penalty │  Demo 8 │
            │  that return non-key columns             │  (extra round trip) │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  Full-table scan on JSONB columns        │  No GIN index =     │  Demo 8 │
            │  without inverted index                  │  scan every row     │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  Multiple DDL in BEGIN/COMMIT            │  Not atomic in CRDB │  Demo 9 │
            │                                          │  partial commit risk│         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  SELECT * in prepared statements         │  Schema change      │  Demo 9 │
            │                                          │  breaks cached plan │         │
            ├──────────────────────────────────────────────────────────────────────────┤
            │  Mixing reads, writes, and DDL in        │  Long duration,     │  Demo 9 │
            │  one logical transaction                 │  contention, errors │         │
            └──────────────────────────────────────────────────────────────────────────┘

            GENERAL RULES:
              1. Keep transactions SHORT: <10 statements, <4 MB total payload
              2. Keep rows SMALL: <1 MB per row, use chunking or object storage
              3. Use UUID primary keys (gen_random_uuid()), never SERIAL
              4. Add STORING clause to indexes that serve SELECT queries
              5. Use AS OF SYSTEM TIME for read-heavy operations outside write txns
              6. Use addBatch/executeBatch instead of row-by-row loops
              7. One DDL per implicit transaction (no BEGIN/COMMIT wrapping)
              8. Always list explicit columns in prepared statements (no SELECT *)
              9. Set session guardrails (transaction_rows_read/written_err) in dev/test
             10. Monitor SHOW JOBS for schema change progress
        """);
    }
}
