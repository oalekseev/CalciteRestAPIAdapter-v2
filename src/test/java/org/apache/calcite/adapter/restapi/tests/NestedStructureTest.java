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
 * Simplified nested structure test using static JSON responses.
 *
 * This test demonstrates:
 * 1. How adapter flattens deeply nested JSON (Departments → Employees → Tasks)
 * 2. How to query flattened data with SQL
 * 3. How x-calcite-restapi-mappings works for nested structures
 * 4. How to verify requests for nested data
 *
 * Example nested structure:
 * departments [
 *   {
 *     id, name, location, budget,
 *     employees: [
 *       {
 *         id, name, position, salary,
 *         tasks: [
 *           {id, title, status, priority}
 *         ]
 *       }
 *     ]
 *   }
 * ]
 *
 * Flattened to SQL table with columns:
 * department_id, department_name, department_location, department_budget,
 * employee_id, employee_name, employee_position, employee_salary,
 * task_id, task_title, task_status, task_priority
 */
public class NestedStructureTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "NestedStructureTest";
    }

    @Test
    @DisplayName("Nested Structure: Query flattened departments → employees → tasks")
    public void testQueryFlattenedNestedStructure() throws Exception {
        String specFile = "neested-json-response.yaml";

        // Setup: Return nested JSON response
        setupStaticJsonResponse("/departments", "departments-with-employees-and-tasks.json");

        setupConnection(specFile);

        System.out.println("=== Test: Query flattened nested structure ===");
        System.out.println("Departments → Employees → Tasks");
        System.out.println();

        String sql = """
            SELECT
                department_name,
                employee_name,
                employee_position,
                task_title,
                task_status,
                task_priority
            FROM departments_schema.departments
            ORDER BY department_name, employee_name, task_priority
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("department_name", rs.getString("department_name"));
                row.put("employee_name", rs.getString("employee_name"));
                row.put("employee_position", rs.getString("employee_position"));
                row.put("task_title", rs.getString("task_title"));
                row.put("task_status", rs.getString("task_status"));
                row.put("task_priority", rs.getInt("task_priority"));
                rows.add(row);

                System.out.printf("%-10s | %-8s (%-18s) | %-25s | %-12s | Priority %d%n",
                    row.get("department_name"),
                    row.get("employee_name"),
                    row.get("employee_position"),
                    row.get("task_title"),
                    row.get("task_status"),
                    row.get("task_priority"));
            }
        }

        // ========================================
        // VERIFY FLATTENING
        // ========================================

        System.out.println("\n=== Verifying flattening ===");

        // Original nested structure has:
        // - 2 departments (IT, HR)
        // - 3 employees total (Alice, Bob in IT; Carol in HR)
        // - 5 tasks total (2 for Alice, 2 for Bob, 1 for Carol)
        //
        // Flattened table should have 5 rows (one per task)

        assertEquals(5, rows.size(),
            "Flattened table should have 5 rows (one per task)");

        System.out.println("✓ Flattening verified: 5 rows from nested structure");

        // Verify we have data from all 3 levels
        assertTrue(rows.stream().anyMatch(r -> "IT".equals(r.get("department_name"))),
            "Should have department data");

        assertTrue(rows.stream().anyMatch(r -> "Alice".equals(r.get("employee_name"))),
            "Should have employee data");

        assertTrue(rows.stream().anyMatch(r -> "Fix bug #123".equals(r.get("task_title"))),
            "Should have task data");

        System.out.println("✓ All 3 levels present: Department → Employee → Task");

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/departments"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body: " + requestBody);

        System.out.println("✓ Request made to /departments endpoint");
    }

    @Test
    @DisplayName("Nested Structure: Filter on nested field - WHERE employee_name = 'Alice'")
    public void testFilterOnNestedField() throws Exception {
        String specFile = "neested-json-response.yaml";

        setupStaticJsonResponse("/departments", "departments-with-employees-and-tasks.json");
        setupConnection(specFile);

        System.out.println("=== Test: Filter on nested field (employee_name) ===");

        String sql = """
            SELECT
                department_name,
                employee_name,
                task_title
            FROM departments_schema.departments
            WHERE employee_name = 'Alice'
            ORDER BY task_title
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("department_name", rs.getString("department_name"));
                row.put("employee_name", rs.getString("employee_name"));
                row.put("task_title", rs.getString("task_title"));
                rows.add(row);

                System.out.printf("%s: %s - %s%n",
                    row.get("department_name"),
                    row.get("employee_name"),
                    row.get("task_title"));
            }
        }

        // ========================================
        // VERIFY FILTERING
        // ========================================

        System.out.println("\n=== Verifying filtering ===");

        // Alice has 2 tasks
        assertEquals(2, rows.size(), "Alice should have 2 tasks");

        // All rows should be for Alice
        assertTrue(rows.stream().allMatch(r -> "Alice".equals(r.get("employee_name"))),
            "All rows should be for Alice");

        System.out.println("✓ Filtering verified: Only Alice's tasks returned");
    }

    @Test
    @DisplayName("Nested Structure: Filter on deepest level - WHERE task_status = 'In Progress'")
    public void testFilterOnDeepestLevel() throws Exception {
        String specFile = "neested-json-response.yaml";

        setupStaticJsonResponse("/departments", "departments-with-employees-and-tasks.json");
        setupConnection(specFile);

        System.out.println("=== Test: Filter on deepest level (task_status) ===");

        String sql = """
            SELECT
                department_name,
                employee_name,
                task_title,
                task_status
            FROM departments_schema.departments
            WHERE task_status = 'In Progress'
            ORDER BY department_name, employee_name
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("department_name", rs.getString("department_name"));
                row.put("employee_name", rs.getString("employee_name"));
                row.put("task_title", rs.getString("task_title"));
                row.put("task_status", rs.getString("task_status"));
                rows.add(row);

                System.out.printf("%s - %s: %s (%s)%n",
                    row.get("department_name"),
                    row.get("employee_name"),
                    row.get("task_title"),
                    row.get("task_status"));
            }
        }

        // ========================================
        // VERIFY FILTERING
        // ========================================

        System.out.println("\n=== Verifying filtering ===");

        // Tasks "In Progress": Fix bug #123 (Alice), Design API (Bob), Interview candidates (Carol) = 3
        assertEquals(3, rows.size(), "Should have 3 'In Progress' tasks");

        // All should be "In Progress"
        assertTrue(rows.stream().allMatch(r -> "In Progress".equals(r.get("task_status"))),
            "All tasks should have status 'In Progress'");

        System.out.println("✓ Filtering verified: Only 'In Progress' tasks returned");
    }

    @Test
    @DisplayName("Nested Structure: Aggregation - COUNT tasks per department")
    public void testAggregationOnNestedData() throws Exception {
        String specFile = "neested-json-response.yaml";

        setupStaticJsonResponse("/departments", "departments-with-employees-and-tasks.json");
        setupConnection(specFile);

        System.out.println("=== Test: Aggregation on nested data ===");

        String sql = """
            SELECT
                department_name,
                COUNT(*) as task_count
            FROM departments_schema.departments
            GROUP BY department_name
            ORDER BY task_count DESC
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("department_name", rs.getString("department_name"));
                row.put("task_count", rs.getInt("task_count"));
                rows.add(row);

                System.out.printf("%s: %d tasks%n",
                    row.get("department_name"),
                    row.get("task_count"));
            }
        }

        // ========================================
        // VERIFY AGGREGATION
        // ========================================

        System.out.println("\n=== Verifying aggregation ===");

        // IT department: 4 tasks (2 for Alice, 2 for Bob)
        // HR department: 1 task (1 for Carol)

        assertEquals(2, rows.size(), "Should have 2 departments");

        Map<String, Object> itRow = rows.stream()
            .filter(r -> "IT".equals(r.get("department_name")))
            .findFirst()
            .orElseThrow();

        assertEquals(4, itRow.get("task_count"), "IT department should have 4 tasks");

        Map<String, Object> hrRow = rows.stream()
            .filter(r -> "HR".equals(r.get("department_name")))
            .findFirst()
            .orElseThrow();

        assertEquals(1, hrRow.get("task_count"), "HR department should have 1 task");

        System.out.println("✓ Aggregation verified: Correct task counts per department");
    }

    @Test
    @DisplayName("Nested Structure: Inspect request for nested endpoint")
    public void testInspectNestedRequest() throws Exception {
        String specFile = "neested-json-response.yaml";

        setupStaticJsonResponse("/departments", "departments-with-employees-and-tasks.json");
        setupConnection(specFile);

        System.out.println("=== Inspecting request for nested endpoint ===");

        String sql = "SELECT * FROM departments_schema.departments WHERE department_name = 'IT'";

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) { /* consume */ }
        }

        // ========================================
        // INSPECT REQUEST
        // ========================================

        System.out.println("\n=== Actual HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/departments"))
        );

        if (!requests.isEmpty()) {
            String requestBody = requests.get(0).getBodyAsString();
            System.out.println("Request body:");
            System.out.println(requestBody);

            System.out.println("\n=== Analysis ===");

            if (requestBody.contains("department_name") || requestBody.contains("name")) {
                System.out.println("✓ Contains filter on department_name");
                System.out.println("  Note: SQL field 'department_name' is mapped to REST field 'name'");
                System.out.println("  Adapter automatically converts filter field names");
            }

            System.out.println("\nFor nested structures:");
            System.out.println("  - Request goes to single endpoint (/departments)");
            System.out.println("  - Response contains full nested structure");
            System.out.println("  - Adapter flattens it into SQL table");
            System.out.println("  - Filters can be on any level (department, employee, task)");
        }

        System.out.println("\n✓ Test completed");
    }
}
