package org.apache.calcite.adapter.restapi.tests;

import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.apache.calcite.adapter.restapi.rest.exception.ConvertFiltersException;
import org.apache.calcite.adapter.restapi.tests.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for REQUEST-only parameters (parameters used in request but not returned in response).
 *
 * Scenario: Company lookup API where rating parameter (1-10) is used to filter
 * but is NOT returned in the response (only company details: id, name, industry, revenue, employees).
 *
 * Tests verify:
 * 1. REQUEST-only parameter with '=' operator works and value is substituted in result rows
 * 2. REQUEST-only parameter with other operators (>, <, !=, etc.) throws validation error
 */
public class RequestOnlyParametersTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "RequestOnlyParametersTest";
    }

    @Test
    @DisplayName("REQUEST-only parameter with '=' operator should work and substitute value in results")
    public void testRequestOnlyParameterWithEqualsOperator() throws Exception {
        // Setup static response
        setupStaticJsonResponse("/companies", "companies-response.json");

        setupConnection("request-only-params.yaml");

        System.out.println("=== Test: REQUEST-only parameter with '=' operator ===");

        // Query with rating = 10 (rating is REQUEST-only parameter)
        String sql = "SELECT rating, id, name, industry, revenue, employee_count " +
                     "FROM company_schema.companies " +
                     "WHERE rating = 10";

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;

                // ids should be substituted from WHERE clause (not from response)
                int rating = rs.getInt("rating");
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String industry = rs.getString("industry");
                double revenue = rs.getDouble("revenue");
                int employeeCount = rs.getInt("employee_count");

                System.out.printf("Row %d: id=%d, name=%s, industry=%s, rating=%d, revenue=%.2f, employees=%d%n",
                    rowCount, id, name, industry, rating, revenue, employeeCount);

                // All rows should have rating = 10 (substituted from WHERE)
                assertEquals(10, rating,
                    "REQUEST-only parameter should be substituted from WHERE clause");

                // Verify response fields are present
                assertTrue(id > 0, "ID should be present from response");
                assertNotNull(name, "Name should be present from response");
                assertNotNull(industry, "Industry should be present from response");
                assertTrue(revenue > 0, "Revenue should be present from response");
                assertTrue(employeeCount > 0, "Employee count should be present from response");
            }

            // Should have 3 rows (all companies from response with substituted ids)
            assertEquals(3, rowCount, "Should return all companies from response");
        }
//
//        // Verify request was made with ids filter
//        getWireMockServer().verify(postRequestedFor(urlEqualTo("/companies"))
//            .withRequestBody(containing("\"rating\": 10")));
//
//

        // ========================================
        // VERIFY HTTP REQUEST FORMAT
        // ========================================

        System.out.println("\n=== Verifying HTTP request format ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
                postRequestedFor(urlEqualTo("/companies"))
        );

        assertTrue(!requests.isEmpty(), "Should have made request to /companies");
        String requestBody = requests.get(0).getBodyAsString();

        System.out.println("Actual HTTP request body:");
        System.out.println(requestBody);

        // Verify request was made with ids filter
        assertTrue(requestBody.contains("\"rating\": 10"), "Should have flat filter: rating=10");

        System.out.println("✓ REQUEST-only parameter with '=' operator test passed");

    }

    @Test
    @DisplayName("REQUEST-only parameter with '>' operator should throw error")
    public void testRequestOnlyParameterWithGreaterThanOperatorShouldFail() throws Exception {
        // Setup static response
        setupStaticJsonResponse("/companies", "companies-response.json");

        setupConnection("request-only-params.yaml");

        System.out.println("=== Test: REQUEST-only parameter with '>' operator (should fail) ===");

        // Query with ids > '1'
        // Should throw error because:
        // - rating is REQUEST-only (not in response)
        // - '>' operator requires actual values for each row
        // - We cannot correctly filter without actual values
        String sql = "SELECT rating, id, name, revenue, employee_count " +
                     "FROM company_schema.companies " +
                     "WHERE rating > 5";

        try (Statement stmt = getConnection().createStatement()) {
            // Should throw either AssertionError directly or SQLException wrapping it
            Throwable exception = assertThrows(Throwable.class, () -> {
                stmt.executeQuery(sql);
            });

            String errorMessage = exception.getMessage();
            if (exception.getCause() != null) {
                errorMessage = exception.getCause().getMessage();
            }
            System.out.println("Expected error message: " + errorMessage);

            // Verify error message contains key information
            assertTrue(errorMessage.contains("rating"),
                "Error should mention field name 'rating'");
            assertTrue(errorMessage.contains("REQUEST-only") || errorMessage.contains("not returned in REST API response"),
                "Error should explain field is REQUEST-only");
            assertTrue(errorMessage.contains(">") || errorMessage.contains("operator"),
                "Error should mention the problematic operator");
            assertTrue(errorMessage.contains("'=' operator") || errorMessage.contains("Only '='"),
                "Error should suggest using '=' operator");

            System.out.println("✓ Correctly prevented REQUEST-only parameter with '>' operator");
        }
    }

    @Test
    @DisplayName("REQUEST-only parameter with '<' operator should throw error")
    public void testRequestOnlyParameterWithLessThanOperatorShouldFail() throws Exception {
        // Setup static response
        setupStaticJsonResponse("/companies", "companies-response.json");

        setupConnection("request-only-params.yaml");

        System.out.println("=== Test: REQUEST-only parameter with '<' operator (should fail) ===");

        String sql = "SELECT rating, id, name, revenue, employee_count " +
                     "FROM company_schema.companies " +
                     "WHERE rating < '5'";

        try (Statement stmt = getConnection().createStatement()) {
            Throwable exception = assertThrows(Throwable.class, () -> {
                stmt.executeQuery(sql);
            });

            String errorMessage = exception.getMessage();
            if (exception.getCause() != null) {
                errorMessage = exception.getCause().getMessage();
            }
            System.out.println("Expected error: " + errorMessage);

            assertTrue(errorMessage.contains("REQUEST-only") || errorMessage.contains("not returned"),
                "Error should explain field is REQUEST-only");

            System.out.println("✓ Correctly prevented REQUEST-only parameter with '<' operator");
        }
    }

    @Test
    @DisplayName("REQUEST-only parameter with '<>' operator should throw error")
    public void testRequestOnlyParameterWithNotEqualsOperatorShouldFail() throws Exception {
        // Setup static response
        setupStaticJsonResponse("/companies", "companies-response.json");

        setupConnection("request-only-params.yaml");

        System.out.println("=== Test: REQUEST-only parameter with '<>' operator (should fail) ===");

        String sql = "SELECT rating, id, name, revenue, employee_count " +
                     "FROM company_schema.companies " +
                     "WHERE rating <> 10";

        try (Statement stmt = getConnection().createStatement()) {
            Throwable exception = assertThrows(Throwable.class, () -> {
                stmt.executeQuery(sql);
            });

            String errorMessage = exception.getMessage();
            if (exception.getCause() != null) {
                errorMessage = exception.getCause().getMessage();
            }
            System.out.println("Expected error: " + errorMessage);

            assertTrue(errorMessage.contains("REQUEST-only") || errorMessage.contains("not returned"),
                "Error should explain field is REQUEST-only");

            System.out.println("✓ Correctly prevented REQUEST-only parameter with '<>' operator");
        }
    }
}
