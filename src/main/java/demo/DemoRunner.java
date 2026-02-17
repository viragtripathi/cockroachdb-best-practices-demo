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
        DemoUtils.banner("ALL DEMOS COMPLETE");
    }
}
