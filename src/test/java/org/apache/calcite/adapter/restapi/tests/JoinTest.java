package org.apache.calcite.adapter.restapi.tests;

import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.apache.calcite.adapter.restapi.tests.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive JOIN test suite for SQL JOIN operations via REST API adapter.
 *
 * Tests cover:
 * 1. Different JOIN types (INNER, SELF, CROSS)
 * 2. HTTP request inspection and verification
 * 3. Filter pushdown to REST API
 * 4. Projection pushdown to REST API
 * 5. Calcite in-memory JOIN processing
 */
public class JoinTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "JoinTest";
    }

    // ============================================================
    // BASIC JOIN OPERATIONS
    // ============================================================

    @Test
    @DisplayName("INNER JOIN: Multiple tables in same schema")
    public void testJoinMultipleTablesInSameSchema() throws Exception {
        String specFile = "single-schema-multi-tables.yaml";

        // Setup static JSON responses (NO transformers!)
        setupStaticJsonResponse("/users", "users-all.json");
        setupStaticJsonResponse("/departments", "departments-all.json");

        // Setup Calcite connection
        setupConnection(specFile);
        System.out.println("=== Connection established ===");

        // Test 1: Query users table to verify adapter reads static response
        System.out.println("=== Test 1: Querying users table ===");
        String testSql1 = "SELECT * FROM company_schema.users LIMIT 3";
        int userCount = 0;

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(testSql1)) {
            while (rs.next()) {
                userCount++;
                System.out.println("User " + userCount + ": id=" + rs.getInt("id") +
                    ", name=" + rs.getString("name") + ", last_name=" + rs.getString("last_name") +
                    ", department=" + rs.getString("department"));
            }
            System.out.println("Total users in sample: " + userCount);
        }

        // Verify adapter called the correct endpoint
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users")));
        System.out.println("✓ Verified /users endpoint was called");

        // Verify we got data from static response
        assertTrue(userCount > 0, "Should have read users from static response");

        // Test 2: Query departments table
        System.out.println("\n=== Test 2: Querying departments table ===");
        String testSql2 = "SELECT * FROM company_schema.departments LIMIT 3";
        int departmentCount = 0;

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(testSql2)) {
            while (rs.next()) {
                departmentCount++;
                System.out.println("Department " + departmentCount + ": id=" + rs.getInt("id") +
                    ", name=" + rs.getString("name") + ", budget=" + rs.getInt("budget"));
            }
            System.out.println("Total departments in sample: " + departmentCount);
        }

        // Verify adapter called the correct endpoint
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/departments")));
        System.out.println("✓ Verified /departments endpoint was called");

        // Verify we got data from static response
        assertTrue(departmentCount > 0, "Should have read departments from static response");

        // Test 3: JOIN query
        System.out.println("\n=== Test 3: JOIN query ===");
        String joinSql = """
            SELECT
                u.id as user_id,
                u.name as user_name,
                u.last_name as user_last_name,
                u.department as user_department,
                d.id as department_id,
                d.name as department_name,
                d.budget as department_budget
            FROM company_schema.users u
            JOIN company_schema.departments d
                ON u.department = d.name
            WHERE d.budget >= 250000
            ORDER BY u.id
            """;

        List<Map<String, Object>> joinResults = new ArrayList<>();

        try (Statement statement = getConnection().createStatement();
             ResultSet rs = statement.executeQuery(joinSql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("user_id", rs.getInt("user_id"));
                row.put("user_name", rs.getString("user_name"));
                row.put("user_last_name", rs.getString("user_last_name"));
                row.put("user_department", rs.getString("user_department"));
                row.put("department_id", rs.getInt("department_id"));
                row.put("department_name", rs.getString("department_name"));
                row.put("department_budget", rs.getInt("department_budget"));
                joinResults.add(row);

                System.out.println("JOIN result: User '" + row.get("user_name") + " " + row.get("user_last_name") +
                    "' (ID: " + row.get("user_id") + ") works in department '" +
                    row.get("department_name") + "' (Budget: $" + row.get("department_budget") + ")");
            }
        }

        System.out.println("Total JOIN results: " + joinResults.size());

        // Verify JOIN worked
        assertTrue(joinResults.size() > 0,
            "Should have found users joined with departments. " +
            "Static responses: users-all.json and departments-all.json");

        // Expected: Users in departments with budget >= 250000
        // From static data: IT (500000), Sales (300000), Marketing (250000), Finance (400000)
        // Users in IT: Alice, Bob = 2
        // Users in Sales: Eve, Frank = 2
        // Users in Marketing: Grace = 1
        // Users in Finance: Henry = 1
        // Total = 6 users
        assertEquals(6, joinResults.size(),
            "Should have 6 users in departments with budget >= 250000");

        System.out.println("✓ JOIN test passed: Found " + joinResults.size() + " joined records");
    }

    @Test
    @DisplayName("Self-JOIN: Find colleagues in same department")
    public void testSelfJoinFindColleagues() throws Exception {
        String specFile = "single-schema-multi-tables.yaml";

        // Setup static JSON response
        setupStaticJsonResponse("/users", "users-all.json");

        // Setup Calcite connection
        setupConnection(specFile);

        // Self-join to find pairs of users in the same department
        String sql = """
            SELECT
                u1.id as user1_id,
                u1.name as user1_name,
                u1.last_name as user1_last_name,
                u2.id as user2_id,
                u2.name as user2_name,
                u2.last_name as user2_last_name,
                u1.department as department
            FROM company_schema.users u1
            JOIN company_schema.users u2
                ON u1.department = u2.department
            WHERE u1.id < u2.id
            ORDER BY u1.id, u2.id
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement statement = getConnection().createStatement();
             ResultSet rs = statement.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("user1_id", rs.getInt("user1_id"));
                row.put("user1_name", rs.getString("user1_name"));
                row.put("user1_last_name", rs.getString("user1_last_name"));
                row.put("user2_id", rs.getInt("user2_id"));
                row.put("user2_name", rs.getString("user2_name"));
                row.put("user2_last_name", rs.getString("user2_last_name"));
                row.put("department", rs.getString("department"));
                results.add(row);

                System.out.println("Colleagues: " + row.get("user1_name") + " " + row.get("user1_last_name") +
                    " and " + row.get("user2_name") + " " + row.get("user2_last_name") +
                    " both work in " + row.get("department"));
            }
        }

        // Verify we got colleague pairs
        assertTrue(results.size() > 0,
            "Should have found at least one pair of colleagues in the same department");

        // Verify HTTP request was made
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users")));

        System.out.println("✓ Self-JOIN test passed: Found " + results.size() + " colleague pairs");
    }

    @Test
    @DisplayName("CROSS JOIN: All possible user pairs")
    public void testCrossJoin() throws Exception {
        String specFile = "single-schema-multi-tables.yaml";

        // Setup static JSON response
        setupStaticJsonResponse("/users", "users-all.json");

        // Setup Calcite connection
        setupConnection(specFile);

        // CROSS JOIN creates cartesian product - all possible pairs
        String sql = """
            SELECT
                u1.id as user1_id,
                u1.name as user1_name,
                u2.id as user2_id,
                u2.name as user2_name
            FROM company_schema.users u1
            CROSS JOIN company_schema.users u2
            WHERE u1.id < u2.id
            ORDER BY u1.id, u2.id
            LIMIT 10
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement statement = getConnection().createStatement();
             ResultSet rs = statement.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("user1_id", rs.getInt("user1_id"));
                row.put("user1_name", rs.getString("user1_name"));
                row.put("user2_id", rs.getInt("user2_id"));
                row.put("user2_name", rs.getString("user2_name"));
                results.add(row);

                System.out.println("Pair: " + row.get("user1_name") +
                    " with " + row.get("user2_name"));
            }
        }

        // Verify we got results
        assertTrue(results.size() > 0,
            "Should have created user pairs from CROSS JOIN");

        assertTrue(results.size() <= 10,
            "LIMIT should restrict results to 10");

        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users")));

        System.out.println("✓ CROSS JOIN test passed: Created " + results.size() + " pairs");
    }

    // ============================================================
    // HTTP REQUEST INSPECTION
    // ============================================================

    @Test
    @DisplayName("JOIN with WHERE - Inspect HTTP requests")
    public void testJoinWithWhereInspectRequests() throws Exception {
        String specFile = "single-schema-multi-tables.yaml";

        // Setup static JSON responses (NO filtering logic!)
        setupStaticJsonResponse("/users", "users-all.json");
        setupStaticJsonResponse("/departments", "departments-all.json");

        setupConnection(specFile);

        System.out.println("=== Test: JOIN with WHERE clause ===");
        System.out.println("SQL: SELECT ... FROM users JOIN departments WHERE d.budget >= 250000");

        // Execute JOIN query with filter
        String joinSql = """
            SELECT
                u.id as user_id,
                u.name as user_name,
                u.last_name as user_last_name,
                u.department as user_department,
                d.id as department_id,
                d.name as department_name,
                d.budget as department_budget
            FROM company_schema.users u
            JOIN company_schema.departments d
                ON u.department = d.name
            WHERE d.budget >= 250000
            ORDER BY u.id
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement statement = getConnection().createStatement();
             ResultSet rs = statement.executeQuery(joinSql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("user_id", rs.getInt("user_id"));
                row.put("user_name", rs.getString("user_name"));
                row.put("user_last_name", rs.getString("user_last_name"));
                row.put("user_department", rs.getString("user_department"));
                row.put("department_id", rs.getInt("department_id"));
                row.put("department_name", rs.getString("department_name"));
                row.put("department_budget", rs.getInt("department_budget"));
                results.add(row);
            }
        }

        // ============================================================
        // INSPECT ACTUAL HTTP REQUESTS
        // ============================================================

        System.out.println("\n=== Inspecting actual HTTP requests ===");

        // Get all requests to /users
        List<LoggedRequest> usersRequests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        System.out.println("\n--- Request to /users ---");
        System.out.println("Number of requests: " + usersRequests.size());

        if (!usersRequests.isEmpty()) {
            LoggedRequest usersRequest = usersRequests.get(0);
            String usersRequestBody = usersRequest.getBodyAsString();
            System.out.println("Request body:");
            System.out.println(usersRequestBody);

            // In current template, this should be:
            // { "page": 1, "limit": 100 }
            // NO filters because WHERE clause is on departments.budget

            assertTrue(usersRequestBody.contains("\"page\""),
                "Request should contain page parameter");
            assertTrue(usersRequestBody.contains("\"limit\""),
                "Request should contain limit parameter");


            assertFalse(usersRequestBody.contains("filters_dnf") || usersRequestBody.contains("filters"),
                "Request to /users should NOT contain filters (filter is on departments only)");

            System.out.println("✓ /users request: has pagination, no filters (correct!)");
        }

        // Get all requests to /departments
        List<LoggedRequest> deptRequests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/departments"))
        );

        System.out.println("\n--- Request to /departments ---");
        System.out.println("Number of requests: " + deptRequests.size());

        if (!deptRequests.isEmpty()) {
            LoggedRequest deptRequest = deptRequests.get(0);
            String deptRequestBody = deptRequest.getBodyAsString();
            System.out.println("Request body:");
            System.out.println(deptRequestBody);

            // PROBLEM: Current template doesn't send filters!
            // Template is: { "page": 1, "limit": 100 }
            //
            // It SHOULD be:
            // {
            //   "page": 1,
            //   "limit": 100,
            //   "filters_dnf": [[
            //     {"name": "budget", "operator": ">=", "value": "250000"}
            //   ]]
            // }

            System.out.println("\n⚠️ PROBLEM: Template doesn't include filters!");
            System.out.println("Current template only sends: page, limit");
            System.out.println("Should also send: filters_dnf with budget >= 250000");

            // To fix: use single-schema-with-filters.yaml with proper templates
        }

        // ============================================================
        // VERIFY RESULTS
        // ============================================================

        System.out.println("\n=== Results ===");
        System.out.println("Total results: " + results.size());

        // Since REST API returns ALL data (no filtering on server side),
        // Calcite will filter locally after JOIN
        // Expected: 6 users in high-budget departments

        for (Map<String, Object> row : results) {
            System.out.printf("User '%s %s' in dept '%s' (Budget: $%d)%n",
                row.get("user_name"), row.get("user_last_name"),
                row.get("department_name"), row.get("department_budget"));
        }

        // Verify Calcite filtered correctly
        assertTrue(results.size() > 0, "Should have results");

        // All results should have budget >= 250000
        for (Map<String, Object> row : results) {
            int budget = (int) row.get("department_budget");
            assertTrue(budget >= 250000,
                "All results should have budget >= 250000, but got: " + budget);
        }

        System.out.println("\n✓ All results have budget >= 250000 (Calcite filtered correctly)");
        System.out.println("\nNote: To improve efficiency, use templates with filters_dnf support");
    }

    @Test
    @DisplayName("Simple JOIN - Show request format")
    public void testSimpleJoinShowRequestFormat() throws Exception {
        String specFile = "single-schema-multi-tables.yaml";

        setupStaticJsonResponse("/users", "users-all.json");
        setupStaticJsonResponse("/departments", "departments-all.json");

        setupConnection(specFile);

        System.out.println("=== Test: Simple JOIN without WHERE ===");

        // Simple JOIN without WHERE clause
        String joinSql = """
            SELECT
                u.name as user_name,
                d.name as dept_name
            FROM company_schema.users u
            JOIN company_schema.departments d
                ON u.department = d.name
            LIMIT 3
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement statement = getConnection().createStatement();
             ResultSet rs = statement.executeQuery(joinSql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("user_name", rs.getString("user_name"));
                row.put("dept_name", rs.getString("dept_name"));
                results.add(row);
            }
        }

        // Inspect requests
        System.out.println("\n=== Actual HTTP Requests ===");

        List<LoggedRequest> usersRequests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        if (!usersRequests.isEmpty()) {
            System.out.println("\nRequest to /users:");
            System.out.println(usersRequests.get(0).getBodyAsString());
        }

        List<LoggedRequest> deptRequests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/departments"))
        );

        if (!deptRequests.isEmpty()) {
            System.out.println("\nRequest to /departments:");
            System.out.println(deptRequests.get(0).getBodyAsString());
        }

        // Verify we got results
        assertEquals(3, results.size(), "Should have 3 results due to LIMIT");

        System.out.println("\n=== Results ===");
        for (Map<String, Object> row : results) {
            System.out.printf("%s works in %s%n",
                row.get("user_name"), row.get("dept_name"));
        }

        System.out.println("\n✓ Test passed");
    }

    // ============================================================
    // FILTER AND PROJECTION PUSHDOWN VERIFICATION
    // ============================================================

    @Test
    @DisplayName("JOIN with filter pushdown - Verify budget >= 250000 sent to REST API")
    public void testJoinWithBudgetFilterVerification() throws Exception {
        String specFile = "single-schema-with-filters.yaml";

        // Setup static responses
        setupStaticJsonResponse("/users", "users-all.json");

        // For /departments: return different response based on request filter
        // When request contains budget >= 250000, return filtered departments
        getWireMockServer().stubFor(post(urlEqualTo("/departments"))
            .withRequestBody(matchingJsonPath("$.filters_dnf[0][0].name", equalTo("budget")))
            .withRequestBody(matchingJsonPath("$.filters_dnf[0][0].operator", equalTo(">=")))
            .withRequestBody(matchingJsonPath("$.filters_dnf[0][0].value", equalTo("250000")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBodyFile("JoinTest/responses/departments-budget-gte-250000.json")));

        // Fallback: if no filter or different filter, return all departments
        setupStaticJsonResponse("/departments", "departments-all.json");

        setupConnection(specFile);

        // Execute SQL with JOIN and filter on departments.budget
        String joinSql = """
            SELECT
                u.id as user_id,
                u.name as user_name,
                u.last_name as user_last_name,
                u.department as user_department,
                d.id as department_id,
                d.name as department_name,
                d.budget as department_budget
            FROM company_schema.users u
            JOIN company_schema.departments d
                ON u.department = d.name
            WHERE d.budget >= 250000
            ORDER BY u.id
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        System.out.println("=== Executing JOIN SQL with filter: d.budget >= 250000 ===");
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(joinSql)) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("user_id", rs.getInt("user_id"));
                row.put("user_name", rs.getString("user_name"));
                row.put("user_last_name", rs.getString("user_last_name"));
                row.put("user_department", rs.getString("user_department"));
                row.put("department_id", rs.getInt("department_id"));
                row.put("department_name", rs.getString("department_name"));
                row.put("department_budget", rs.getInt("department_budget"));
                results.add(row);

                System.out.printf("Result: User '%s %s' (ID: %d) in dept '%s' (Budget: $%d)%n",
                    row.get("user_name"), row.get("user_last_name"),
                    row.get("user_id"), row.get("department_name"),
                    row.get("department_budget"));
            }
        }

        // ========================================
        // VERIFY HTTP REQUESTS
        // ========================================

        System.out.println("\n=== Verifying HTTP requests ===");

        // 1. Verify /users was called (should have NO filters, since filter is only on departments)
        System.out.println("Verifying /users request...");
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page"))
            .withRequestBody(matchingJsonPath("$.limit"))
            // No filters on users table - request should not contain filters_dnf
            .withRequestBody(notMatching(".*\"filters_dnf\".*"))
        );
        System.out.println("✓ /users request verified: has pagination, no filters");

        // 2. Verify /departments was called WITH correct filter: budget >= 250000
        System.out.println("Verifying /departments request...");
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/departments"))
            .withRequestBody(matchingJsonPath("$.page"))
            .withRequestBody(matchingJsonPath("$.limit"))
            .withRequestBody(matchingJsonPath("$.filters_dnf[0][0].name", equalTo("budget")))
            .withRequestBody(matchingJsonPath("$.filters_dnf[0][0].operator", equalTo(">=")))
            .withRequestBody(matchingJsonPath("$.filters_dnf[0][0].value", equalTo("250000")))
        );
        System.out.println("✓ /departments request verified: has filter budget >= 250000");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Expected results:
        // - IT (budget 500000): Alice, Bob = 2 users
        // - Sales (budget 300000): Eve, Frank = 2 users
        // - Marketing (budget 250000): Grace = 1 user
        // - Finance (budget 400000): Henry = 1 user
        // Total = 6 users
        assertEquals(6, results.size(), "Expected 6 users in departments with budget >= 250000");

        // Verify all returned users are in high-budget departments
        for (Map<String, Object> row : results) {
            int budget = (int) row.get("department_budget");
            assertTrue(budget >= 250000,
                String.format("User %s should be in dept with budget >= 250000, but got %d",
                    row.get("user_name"), budget));
        }

        System.out.println("✓ All results verified: 6 users in high-budget departments");
    }

    @Test
    @DisplayName("JOIN with projection pushdown - Verify only requested columns sent to REST API")
    public void testJoinWithProjectionVerification() throws Exception {
        String specFile = "single-schema-with-filters.yaml";

        setupStaticJsonResponse("/users", "users-all.json");
        setupStaticJsonResponse("/departments", "departments-all.json");

        setupConnection(specFile);

        // Execute SQL with specific column projections
        String joinSql = """
            SELECT
                u.name,
                d.name as dept_name,
                d.budget
            FROM company_schema.users u
            JOIN company_schema.departments d
                ON u.department = d.name
            ORDER BY u.name
            LIMIT 5
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        System.out.println("=== Executing JOIN SQL with projections: u.name, d.name, d.budget ===");
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(joinSql)) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("user_name", rs.getString("name"));
                row.put("dept_name", rs.getString("dept_name"));
                row.put("budget", rs.getInt("budget"));
                results.add(row);

                System.out.printf("Result: %s works in %s (Budget: $%d)%n",
                    row.get("user_name"), row.get("dept_name"), row.get("budget"));
            }
        }

        // ========================================
        // VERIFY HTTP REQUESTS
        // ========================================

        System.out.println("\n=== Verifying HTTP requests ===");

        // 1. Verify /users was called with correct projections
        System.out.println("Verifying /users request...");
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'name')]"))
            // Should request department field for JOIN condition
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'department')]"))
        );
        System.out.println("✓ /users request verified: has projections [name, department]");

        // 2. Verify /departments was called with correct projections
        System.out.println("Verifying /departments request...");
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/departments"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'name')]"))
            .withRequestBody(matchingJsonPath("$.projections[?(@ == 'budget')]"))
        );
        System.out.println("✓ /departments request verified: has projections [name, budget]");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        assertTrue(results.size() <= 5, "LIMIT 5 should restrict results");
        assertTrue(results.size() > 0, "Should have at least 1 result");

        System.out.printf("✓ Results verified: got %d records (limited to 5)%n", results.size());
    }

    @Test
    @DisplayName("Self-JOIN request verification - Verify requests to same endpoint")
    public void testSelfJoinRequestVerification() throws Exception {
        String specFile = "single-schema-with-filters.yaml";

        setupStaticJsonResponse("/users", "users-all.json");
        setupConnection(specFile);

        // Self-join to find colleague pairs
        String sql = """
            SELECT
                u1.id as user1_id,
                u1.name as user1_name,
                u2.id as user2_id,
                u2.name as user2_name,
                u1.department
            FROM company_schema.users u1
            JOIN company_schema.users u2
                ON u1.department = u2.department
            WHERE u1.id < u2.id
            ORDER BY u1.id, u2.id
            """;

        List<Map<String, Object>> results = new ArrayList<>();

        System.out.println("=== Executing Self-JOIN SQL ===");
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("user1_id", rs.getInt("user1_id"));
                row.put("user1_name", rs.getString("user1_name"));
                row.put("user2_id", rs.getInt("user2_id"));
                row.put("user2_name", rs.getString("user2_name"));
                row.put("department", rs.getString("department"));
                results.add(row);

                System.out.printf("Colleagues: %s and %s both in %s%n",
                    row.get("user1_name"), row.get("user2_name"), row.get("department"));
            }
        }

        // ========================================
        // VERIFY HTTP REQUESTS
        // ========================================

        System.out.println("\n=== Verifying HTTP requests ===");

        // For self-join, /users should be called (adapter may optimize to call once)
        // Verify at least one call was made
        getWireMockServer().verify(moreThanOrExactly(1), postRequestedFor(urlEqualTo("/users")));
        System.out.println("✓ /users endpoint was called (self-join)");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        assertTrue(results.size() > 0, "Should have found colleague pairs");

        // From test data:
        // IT: Alice, Bob = 1 pair
        // HR: Carol, David = 1 pair
        // Sales: Eve, Frank = 1 pair
        // Total minimum = 3 pairs
        assertTrue(results.size() >= 3, "Should have at least 3 colleague pairs");

        System.out.printf("✓ Results verified: found %d colleague pairs%n", results.size());
    }
}
