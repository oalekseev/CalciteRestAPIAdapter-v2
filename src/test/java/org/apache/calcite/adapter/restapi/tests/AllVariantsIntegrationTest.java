package org.apache.calcite.adapter.restapi.tests;

import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.apache.calcite.adapter.restapi.tests.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simplified test for all 5 filter specification variants from README.
 *
 * This test shows how to verify different filter formats in HTTP requests:
 * - Variant 1: Simple flat filters (field=value as direct properties)
 * - Variant 2: DNF with 'where' (AND) + 'or' (OR groups)
 * - Variant 3: CNF with 'where' (OR) + 'and' (AND groups)
 * - Variant 4: DNF with 'filters' only
 * - Variant 5: CNF with 'filters' only
 *
 * NO dynamic filtering - uses static responses and verifies request format.
 */
public class AllVariantsIntegrationTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "AllVariantsIntegrationTest";
    }

    @Test
    @DisplayName("Variant 1: Simple flat filters - name=Alice AND last_name=Smith AND department=IT")
    public void testVariant1_SimpleFlatFilters() throws Exception {
        String specFile = "variant1-simple-flat.yaml";

        // Setup: return Alice Smith from IT when filters match
        setupStaticJsonResponse("/users", "users-alice-smith-it.json");

        setupConnection(specFile);

        System.out.println("=== Variant 1: Simple flat filters ===");
        System.out.println("SQL: WHERE name='Alice' AND last_name='Smith' AND department='IT'");

        String sql = """
            SELECT *
            FROM users_schema.users
            WHERE name = 'Alice'
              AND last_name = 'Smith'
              AND department = 'IT'
            """;

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String lastName = rs.getString("last_name");
                String department = rs.getString("department");

                ids.add(id);
                System.out.printf("Result: id=%d, name=%s %s, department=%s%n",
                    id, name, lastName, department);
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST FORMAT
        // ========================================

        System.out.println("\n=== Verifying HTTP request format ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        assertTrue(!requests.isEmpty(), "Should have made request to /users");
        String requestBody = requests.get(0).getBodyAsString();

        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        // Variant 1: Simple flat filters as direct properties
        // Should be: { "page": 1, "limit": 100, "name": "Alice", "department": "IT" }
        // NOT nested in "filters" or "where" structures

        assertTrue(requestBody.contains("\"name\":\"Alice\"") ||
                   requestBody.contains("\"name\": \"Alice\""),
            "Should have flat filter: name=Alice");

        assertTrue(requestBody.contains("\"department\":\"IT\"") ||
                   requestBody.contains("\"department\": \"IT\""),
            "Should have flat filter: department=IT");

        // Should NOT have nested filter structures
        assertFalse(requestBody.contains("\"filters\""),
            "Variant 1 should NOT use 'filters' array");
        assertFalse(requestBody.contains("\"where\""),
            "Variant 1 should NOT use 'where' structure");

        System.out.println("✓ Verified: Filters are flat properties (Variant 1 format)");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Expected: Alice Smith, 28, IT (id=5)
        assertEquals(List.of(5), ids, "Should return user with id=5");
        System.out.println("✓ Result verified: Alice Smith from IT");
    }

    @Test
    @DisplayName("Variant 2: DNF with 'where' + 'or' - Complex nested query")
    public void testVariant2_DNF_WhereAndOr() throws Exception {
        String specFile = "variant2-dnf-where-or.yaml";

        // For complex query, return pre-filtered result
        setupStaticJsonResponse("/users", "users-complex-query-result.json");

        setupConnection(specFile);

        System.out.println("=== Variant 2: DNF with 'where' (AND) + 'or' (OR groups) ===");

        // Complex query from README:
        // (name='Bob' AND age>=21) OR (name='Alice' AND (department='IT' OR age>=25))
        // AND
        // (last_name='Smith' OR (department='IT' AND age<30))
        String sql = """
            SELECT * FROM users_schema.users
            WHERE (
                (name = 'Bob' AND age >= 21)
                OR
                (name = 'Alice' AND (department = 'IT' OR age >= 25))
              )
              AND
              (last_name = 'Smith' OR (department = 'IT' AND age < 30))
            """;

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String lastName = rs.getString("last_name");
                int age = rs.getInt("age");
                String department = rs.getString("department");

                ids.add(id);
                System.out.printf("Result: id=%d, name=%s %s, age=%d, department=%s%n",
                    id, name, lastName, age, department);
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST FORMAT
        // ========================================

        System.out.println("\n=== Verifying HTTP request format ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        // Variant 2: DNF with 'where' (first AND group) + 'or' (additional AND groups)
        // Should have structure:
        // {
        //   "where": [ {...}, {...} ],  // First AND group
        //   "or": [ [{...}, {...}], [{...}] ]  // Additional AND groups (OR of ANDs)
        // }

        assertTrue(requestBody.contains("\"where\""),
            "Variant 2 should use 'where' for first AND group");

        assertTrue(requestBody.contains("\"or\""),
            "Variant 2 should use 'or' for additional AND groups (DNF structure)");

        System.out.println("✓ Verified: Has 'where' (AND) + 'or' (OR groups) - DNF Variant 2 format");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Expected: Bob Smith (id=1) and Alice Smith (id=5)
        assertEquals(List.of(1, 5), ids,
            "Should return users with id=1 (Bob Smith) and id=5 (Alice Smith)");
        System.out.println("✓ Results verified: Bob Smith and Alice Smith");
    }
    
    @Test
    @DisplayName("Variant 3: CNF with 'where' + 'and' - Complex nested query")
    public void testVariant3_CNF_WhereAndOr() throws Exception {
        String specFile = "variant3-cnf-where-and.yaml";

        // For complex query, return pre-filtered result
        setupStaticJsonResponse("/users", "users-complex-query-result.json");

        setupConnection(specFile);

        System.out.println("=== Variant 3: CNF with 'where' (OR) + 'and' (AND groups) ===");

        // Complex query from README:
        // (name='Bob' AND age>=21) OR (name='Alice' AND (department='IT' OR age>=25))
        // AND
        // (last_name='Smith' OR (department='IT' AND age<30))
        String sql = """
            SELECT * FROM users_schema.users
            WHERE (
                (name = 'Bob' AND age >= 21)
                OR
                (name = 'Alice' AND (department = 'IT' OR age >= 25))
              )
              AND
              (last_name = 'Smith' OR (department = 'IT' AND age < 30))
            """;

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String lastName = rs.getString("last_name");
                int age = rs.getInt("age");
                String department = rs.getString("department");

                ids.add(id);
                System.out.printf("Result: id=%d, name=%s %s, age=%d, department=%s%n",
                    id, name, lastName, age, department);
            }
        }

        System.out.println("\n=== Verifying HTTP request format ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);
        
        assertTrue(requestBody.contains("\"where\""),
            "Variant 3 should use 'where' for first OR group");

        assertTrue(requestBody.contains("\"and\""),
            "Variant 3 should use 'and' for additional OR groups (CNF structure)");

        System.out.println("✓ Verified: Has 'where' (OR) + 'and' (AND groups) - CNF Variant 3 format");

        System.out.println("\n=== Verifying results ===");

        // Expected: Bob Smith (id=1) and Alice Smith (id=5)
        assertEquals(List.of(1, 5), ids,
            "Should return users with id=1 (Bob Smith) and id=5 (Alice Smith)");
        System.out.println("✓ Results verified: Bob Smith and Alice Smith");
    }

    @Test
    @DisplayName("Variant 4: DNF with 'filters' only - Complex nested query")
    public void testVariant4_DNF_Filters() throws Exception {
        String specFile = "variant4-dnf-filters.yaml";

        setupStaticJsonResponse("/users", "users-complex-query-result.json");
        setupConnection(specFile);

        System.out.println("=== Variant 4: DNF with 'filters' only ===");
        
        String sql = """
            SELECT * FROM users_schema.users
            WHERE (
                (name = 'Bob' AND age >= 21)
                OR
                (name = 'Alice' AND (department = 'IT' OR age >= 25))
              )
              AND
              (last_name = 'Smith' OR (department = 'IT' AND age < 30))
            """;

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String lastName = rs.getString("last_name");
                int age = rs.getInt("age");
                String department = rs.getString("department");

                ids.add(id);
                System.out.printf("Result: id=%d, name=%s %s, age=%d, department=%s%n",
                    id, name, lastName, age, department);
            }
        }

        System.out.println("\n=== Verifying HTTP request format ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        assertTrue(requestBody.contains("\"filters\""),
            "Variant 4 should use 'filters'");

        System.out.println("✓ Verified: Has 'filters' - DNF Variant 4 format");

        System.out.println("\n=== Verifying results ===");

        // Expected: Bob Smith (id=1) and Alice Smith (id=5)
        assertEquals(List.of(1, 5), ids,
            "Should return users with id=1 (Bob Smith) and id=5 (Alice Smith)");
        System.out.println("✓ Results verified: Bob Smith and Alice Smith");
    }

    @Test
    @DisplayName("Variant 5: CNF with 'filters' only - Complex nested query")
    public void testVariant5_CNF_Filters() throws Exception {
        String specFile = "variant5-cnf-filters.yaml";

        setupStaticJsonResponse("/users", "users-complex-query-result.json");
        setupConnection(specFile);

        System.out.println("=== Variant 5: CNF with 'filters' only ===");

        String sql = """
            SELECT * FROM users_schema.users
            WHERE (
                (name = 'Bob' AND age >= 21)
                OR
                (name = 'Alice' AND (department = 'IT' OR age >= 25))
              )
              AND
              (last_name = 'Smith' OR (department = 'IT' AND age < 30))
            """;

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String lastName = rs.getString("last_name");
                int age = rs.getInt("age");
                String department = rs.getString("department");

                ids.add(id);
                System.out.printf("Result: id=%d, name=%s %s, age=%d, department=%s%n",
                    id, name, lastName, age, department);
            }
        }

        System.out.println("\n=== Verifying HTTP request format ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        assertTrue(requestBody.contains("\"filters\""),
            "Variant 5 should use 'filters'");

        System.out.println("✓ Verified: Has 'filters' - CNF Variant 5 format");

        System.out.println("\n=== Verifying results ===");

        // Expected: Bob Smith (id=1) and Alice Smith (id=5)
        assertEquals(List.of(1, 5), ids,
            "Should return users with id=1 (Bob Smith) and id=5 (Alice Smith)");
        System.out.println("✓ Results verified: Bob Smith and Alice Smith");
    }
}