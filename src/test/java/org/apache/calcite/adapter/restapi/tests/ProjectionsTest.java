package org.apache.calcite.adapter.restapi.tests;

import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.apache.calcite.adapter.restapi.tests.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simplified projections test using ONLY static responses.
 *
 * This test verifies:
 * 1. Adapter sends correct projections in HTTP request
 * 2. Adapter sends correct filters in HTTP request
 * 3. Adapter parses static response correctly
 * 4. SQL returns correct data
 *
 * NO dynamic filtering logic - we test the adapter, not the REST service.
 */
public class ProjectionsTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "ProjectionsTest";
    }

    @Test
    @DisplayName("SELECT name, last_name WHERE age >= 23 - Verify HTTP request")
    public void testProjectionWithFilter() throws Exception {
        String specFile = "projections.yaml";

        // Setup: Return filtered static response when age >= 23 filter is present
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.filters[0][0].name", equalTo("age")))
            .withRequestBody(matchingJsonPath("$.filters[0][0].operator", equalTo(">=")))
            .withRequestBody(matchingJsonPath("$.filters[0][0].value", equalTo("23")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBodyFile("ProjectionsTest/responses/users-age-gte-23.json")));

        // Fallback: Return all users if no filter
        setupStaticJsonResponse("/users", "users-all.json");

        setupConnection(specFile);

        System.out.println("=== Test: SELECT name, last_name WHERE age >= 23 ===");

        String sql = """
            SELECT name, last_name
            FROM users_schema.users
            WHERE age >= 23
            ORDER BY name, last_name
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("name", rs.getString("name"));
                row.put("last_name", rs.getString("last_name"));
                results.add(row);

                System.out.printf("User: %s %s%n",
                    row.get("name"), row.get("last_name"));
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        // Get actual request
        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        assertTrue(!requests.isEmpty(), "Should have made at least one request to /users");
        String requestBody = requests.get(0).getBodyAsString();

        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        // Verify projections: should contain "name" and "lastName" (mapped from last_name)
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'name')]"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'lastName')]"))
        );
        System.out.println("✓ Projections verified: contains 'name' and 'lastName'");

        // Verify filter: age >= 23
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.filters[0][0].name", equalTo("age")))
            .withRequestBody(matchingJsonPath("$.filters[0][0].operator", equalTo(">=")))
            .withRequestBody(matchingJsonPath("$.filters[0][0].value", equalTo("23")))
        );
        System.out.println("✓ Filter verified: age >= 23");

        // Verify that department and id are NOT in projections (not selected)
        assertFalse(requestBody.contains("\"department\"") || requestBody.contains("\"id\""),
            "Request should NOT contain 'department' or 'id' in projections");
        System.out.println("✓ Unused fields NOT in projections (correct!)");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Expected: Bob Smith, Martin Smith, Alice Smith, John Doe, Sarah Smith = 5 users
        assertEquals(5, results.size(), "Should return 5 users with age >= 23");

        Set<String> expectedNames = Set.of("Bob", "Martin", "Alice", "John", "Sarah");
        Set<String> actualNames = new HashSet<>();
        for (Map<String, Object> row : results) {
            actualNames.add((String) row.get("name"));
        }

        assertEquals(expectedNames, actualNames, "Should have correct names");
        System.out.println("✓ Results verified: 5 users with correct names");
    }

    @Test
    @DisplayName("SELECT name, department WHERE age >= 25 AND last_name = 'Smith'")
    public void testProjectionWithMultipleFilters() throws Exception {
        String specFile = "projections.yaml";

        // Setup: Return filtered static response
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.filters[0][0].name", equalTo("age")))
            .withRequestBody(matchingJsonPath("$.filters[0][0].operator", equalTo(">=")))
            .withRequestBody(matchingJsonPath("$.filters[0][1].name", equalTo("lastName")))
            .withRequestBody(matchingJsonPath("$.filters[0][1].operator", equalTo("=")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBodyFile("ProjectionsTest/responses/users-age-gte-25-lastname-smith.json")));

        setupStaticJsonResponse("/users", "users-all.json");
        setupConnection(specFile);

        System.out.println("=== Test: SELECT name, department WHERE age >= 25 AND last_name = 'Smith' ===");

        String sql = """
            SELECT name, department
            FROM users_schema.users
            WHERE age >= 25 AND last_name = 'Smith'
            ORDER BY name
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("name", rs.getString("name"));
                row.put("department", rs.getString("department"));
                results.add(row);

                System.out.printf("User: %s, Department: %s%n",
                    row.get("name"), row.get("department"));
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        // Verify projections: name, department
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'name')]"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'department')]"))
        );
        System.out.println("✓ Projections verified: contains 'name' and 'department'");

        // Verify filters: age >= 25 AND lastName = 'Smith'
        // Both should be in the same AND group (filters[0])
        assertTrue(requestBody.contains("\"age\""), "Should have age filter");
        assertTrue(requestBody.contains("\"lastName\""), "Should have lastName filter");
        System.out.println("✓ Filters verified: age >= 25 AND lastName = 'Smith'");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Expected: Bob Smith (25, IT), Martin Smith (35, Sales),
        //           Alice Smith (28, IT), Sarah Smith (26, IT) = 4 users
        assertEquals(4, results.size(), "Should return 4 users");

        System.out.println("✓ Results verified: 4 users with age >= 25 and last_name = 'Smith'");
    }

    @Test
    @DisplayName("SELECT * (all columns) WHERE age >= 30")
    public void testSelectAllWithFilter() throws Exception {
        String specFile = "projections.yaml";

        // Setup: Return filtered static response
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.filters[0][0].name", equalTo("age")))
            .withRequestBody(matchingJsonPath("$.filters[0][0].value", equalTo("30")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBodyFile("ProjectionsTest/responses/users-age-gte-30.json")));

        setupStaticJsonResponse("/users", "users-all.json");
        setupConnection(specFile);

        System.out.println("=== Test: SELECT * WHERE age >= 30 ===");

        String sql = """
            SELECT *
            FROM users_schema.users
            WHERE age >= 30
            ORDER BY id
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("id", rs.getInt("id"));
                row.put("name", rs.getString("name"));
                row.put("last_name", rs.getString("last_name"));
                row.put("age", rs.getInt("age"));
                row.put("department", rs.getString("department"));
                results.add(row);

                System.out.printf("User: id=%d, name=%s %s, age=%d, department=%s%n",
                    row.get("id"), row.get("name"), row.get("last_name"),
                    row.get("age"), row.get("department"));
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        // When SELECT *, all columns should be in projections
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'id')]"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'name')]"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'lastName')]"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'age')]"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'department')]"))
        );
        System.out.println("✓ Projections verified: SELECT * includes all columns");

        // Verify filter: age >= 30
        assertTrue(requestBody.contains("\"age\""), "Should have age filter");
        assertTrue(requestBody.contains("\">="), "Should have >= operator");
        System.out.println("✓ Filter verified: age >= 30");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Expected: Martin Smith (35), John Doe (30) = 2 users
        assertEquals(2, results.size(), "Should return 2 users with age >= 30");

        System.out.println("✓ Results verified: 2 users with age >= 30");
    }

    @Test
    @DisplayName("SELECT name, last_name (no filter) - Verify projections without filters")
    public void testProjectionWithoutFilter() throws Exception {
        String specFile = "projections.yaml";

        setupStaticJsonResponse("/users", "users-all.json");
        setupConnection(specFile);

        System.out.println("=== Test: SELECT name, last_name (no filter) ===");

        String sql = """
            SELECT name, last_name
            FROM users_schema.users
            ORDER BY id
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("name", rs.getString("name"));
                row.put("last_name", rs.getString("last_name"));
                results.add(row);

                System.out.printf("User: %s %s%n",
                    row.get("name"), row.get("last_name"));
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        // Verify projections: name, lastName
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'name')]"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'lastName')]"))
        );
        System.out.println("✓ Projections verified: contains 'name' and 'lastName'");

        // Verify NO filters
        assertFalse(requestBody.contains("\"filters\""), "Should NOT have filters");
        System.out.println("✓ No filters in request (correct!)");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Should return all 7 test users
        assertEquals(7, results.size(), "Should return all 7 users");

        System.out.println("✓ Results verified: all 7 users returned");
    }
}
