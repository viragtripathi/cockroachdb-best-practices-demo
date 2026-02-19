package demo;

import org.postgresql.ds.PGSimpleDataSource;
import java.sql.*;

/**
 * One-time utility to enable multi-region on the defaultdb database.
 * Run with: mvn compile exec:java -Dexec.mainClass=demo.EnableMultiRegion -Dexec.args="..."
 */
public class EnableMultiRegion {

    public static void main(String[] args) throws Exception {
        String url = DemoUtils.DEFAULT_JDBC_URL;
        String user = DemoUtils.DEFAULT_USER;
        String password = DemoUtils.DEFAULT_PASSWORD;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--url" -> url = args[++i];
                case "--user" -> user = args[++i];
                case "--password" -> password = args[++i];
            }
        }

        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(url);
        ds.setUser(user);
        if (password != null && !password.isEmpty()) ds.setPassword(password);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            // Discover cluster regions
            System.out.println("Cluster regions:");
            var regions = new java.util.ArrayList<String>();
            try (ResultSet rs = stmt.executeQuery("SHOW REGIONS FROM CLUSTER")) {
                while (rs.next()) {
                    String region = rs.getString("region");
                    regions.add(region);
                    System.out.println("  " + region);
                }
            }

            if (regions.isEmpty()) {
                System.out.println("No regions found. Is this a CockroachDB cluster?");
                return;
            }

            // Check if already multi-region
            boolean alreadyMultiRegion = false;
            try (ResultSet rs = stmt.executeQuery("SHOW REGIONS FROM DATABASE")) {
                while (rs.next()) {
                    if ("true".equalsIgnoreCase(rs.getString("primary"))) {
                        alreadyMultiRegion = true;
                        System.out.println("Database is already multi-region (primary: " + rs.getString("region") + ")");
                    }
                }
            } catch (SQLException e) {
                // Expected if not multi-region
            }

            if (alreadyMultiRegion) {
                System.out.println("Nothing to do.");
                return;
            }

            // Set primary region (first one)
            String primary = regions.getFirst();
            System.out.println("Setting primary region: " + primary);
            stmt.execute("ALTER DATABASE defaultdb PRIMARY REGION \"" + primary + "\"");
            System.out.println("  Done.");

            // Add remaining regions
            for (int i = 1; i < regions.size(); i++) {
                String region = regions.get(i);
                System.out.println("Adding region: " + region);
                stmt.execute("ALTER DATABASE defaultdb ADD REGION \"" + region + "\"");
                System.out.println("  Done.");
            }

            System.out.println("Multi-region enabled successfully!");
        }
    }
}
