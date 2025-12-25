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
 * Simplified pagination test using static responses for each page.
 *
 * This test demonstrates:
 * 1. How to verify pagination parameters (page, limit) in HTTP requests
 * 2. How to return different static responses for different pages
 * 3. How adapter fetches multiple pages until data is exhausted
 * 4. How SQL LIMIT affects pagination behavior
 */
public class PaginationTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "PaginationTest";
    }

    @Test
    @DisplayName("Pagination: Fetch all data with pageSize=2 - Verify multiple requests")
    public void testFetchAllDataWithPagination() throws Exception {
        String specFile = "pagination.yaml";

        // Setup: Return different pages based on page parameter
        // Page 1: users 1-2
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("1")))
            .withRequestBody(matchingJsonPath("$.limit", equalTo("2")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page1-limit2.json"))));

        // Page 2: users 3-4
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("2")))
            .withRequestBody(matchingJsonPath("$.limit", equalTo("2")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page2-limit2.json"))));

        // Page 3: users 5-6
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("3")))
            .withRequestBody(matchingJsonPath("$.limit", equalTo("2")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page3-limit2.json"))));

        // Page 4: user 7 (last page, only 1 user - stops pagination)
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("4")))
            .withRequestBody(matchingJsonPath("$.limit", equalTo("2")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page4-limit2.json"))));

        setupConnection(specFile);

        System.out.println("=== Test: Fetch all data with pageSize=2 ===");
        System.out.println("SQL: SELECT * FROM users (no LIMIT)");

        String sql = "SELECT * FROM users_schema.users ORDER BY id";

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                ids.add(id);

                System.out.printf("User: id=%d, name=%s%n", id, name);
            }
        }

        // ========================================
        // VERIFY PAGINATION REQUESTS
        // ========================================

        System.out.println("\n=== Verifying pagination requests ===");

        List<LoggedRequest> allRequests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        System.out.printf("Total requests made: %d%n", allRequests.size());

        // Should have made 4 requests (pages 1-4)
        assertEquals(4, allRequests.size(), "Should fetch 4 pages");

        // Verify each request
        for (int i = 0; i < allRequests.size(); i++) {
            LoggedRequest req = allRequests.get(i);
            String body = req.getBodyAsString();
            System.out.printf("\nRequest %d body: %s%n", i + 1, body);

            assertTrue(body.contains("\"page\":" + (i + 1)) ||
                       body.contains("\"page\": " + (i + 1)),
                "Request " + (i + 1) + " should have page=" + (i + 1));

            assertTrue(body.contains("\"limit\":2") ||
                       body.contains("\"limit\": 2"),
                "Request " + (i + 1) + " should have limit=2");
        }

        System.out.println("\n✓ Pagination verified: 4 requests with page=1,2,3,4 and limit=2");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Should have fetched all 7 users
        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), ids,
            "Should have fetched all 7 users across 4 pages");

        System.out.println("✓ Results verified: All 7 users fetched");
    }

    @Test
    @DisplayName("Pagination with SQL LIMIT 3 - Should stop after fetching enough data")
    public void testPaginationWithSqlLimit() throws Exception {
        String specFile = "pagination.yaml";

        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("1")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page1-limit2.json"))));

        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("2")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page2-limit2.json"))));

        setupConnection(specFile);

        System.out.println("=== Test: SQL LIMIT 3 with pageSize=2 ===");
        System.out.println("SQL: SELECT * FROM users LIMIT 3");

        String sql = "SELECT * FROM users_schema.users LIMIT 3";

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                ids.add(id);

                System.out.printf("User: id=%d, name=%s%n", id, name);
            }
        }

        // ========================================
        // VERIFY PAGINATION BEHAVIOR
        // ========================================

        System.out.println("\n=== Verifying pagination behavior ===");

        List<LoggedRequest> allRequests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        System.out.printf("Total requests made: %d%n", allRequests.size());

        // Note: Depending on Calcite implementation, it may:
        // - Fetch 2 pages (page1: 2 users, page2: 2 users, then apply LIMIT 3 locally)
        // - Or optimize to fetch only needed pages
        //
        // Current implementation typically fetches until LIMIT is satisfied
        assertTrue(allRequests.size() >= 2,
            "Should fetch at least 2 pages to get 3 users (pageSize=2)");

        for (LoggedRequest req : allRequests) {
            System.out.println("Request: " + req.getBodyAsString());
        }

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Should return exactly 3 users due to LIMIT
        assertEquals(List.of(1, 2, 3), ids,
            "Should return exactly 3 users due to SQL LIMIT 3");

        System.out.println("✓ Results verified: LIMIT 3 applied correctly");
    }

    @Test
    @DisplayName("Inspect pagination parameters in request")
    public void testInspectPaginationParameters() throws Exception {
        String specFile = "pagination.yaml";

        // Simple stub
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page1-limit2.json"))));

        setupConnection(specFile);

        System.out.println("=== Inspecting pagination parameters ===");

        String sql = "SELECT * FROM users_schema.users LIMIT 1";

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) { /* consume */ }
        }

        // ========================================
        // INSPECT REQUEST
        // ========================================

        System.out.println("\n=== Actual pagination request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        if (!requests.isEmpty()) {
            String requestBody = requests.get(0).getBodyAsString();
            System.out.println("First request body:");
            System.out.println(requestBody);

            // Pretty print
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = 
                    new com.fasterxml.jackson.databind.ObjectMapper();
                Object json = mapper.readValue(requestBody, Object.class);
                String prettyJson = mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(json);

                System.out.println("\nPretty printed:");
                System.out.println(prettyJson);
            } catch (Exception e) {
                // Ignore
            }

            System.out.println("\n=== Analysis ===");

            if (requestBody.contains("\"page\"")) {
                System.out.println("✓ Contains 'page' parameter");
                // Extract page number
                if (requestBody.contains("\"page\":1") || requestBody.contains("\"page\": 1")) {
                    System.out.println("  Page number: 1");
                }
            }

            if (requestBody.contains("\"limit\"")) {
                System.out.println("✓ Contains 'limit' parameter");
                // Extract limit value
                if (requestBody.contains("\"limit\":2") || requestBody.contains("\"limit\": 2")) {
                    System.out.println("  Limit value: 2");
                }
            }

            if (requestBody.contains("\"offset\"")) {
                System.out.println("✓ Contains 'offset' parameter (alternative to page)");
            }
        }

        System.out.println("\n✓ Test completed");
    }

    @Test
    @DisplayName("Empty page - Pagination stops when receiving less than pageSize")
    public void testEmptyPageStopsPagination() throws Exception {
        String specFile = "pagination.yaml";

        // Page 1: 2 users (full page)
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("1")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page1-limit2.json"))));

        // Page 2: 1 user (less than pageSize=2, should stop)
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("2")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-page4-limit2.json")))); // Only 1 user

        // Page 3: should NOT be requested (pagination stopped)
        getWireMockServer().stubFor(post(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("3")))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(loadStaticResponse("users-empty.json"))));

        setupConnection(specFile);

        System.out.println("=== Test: Pagination stops when page has less than pageSize ===");

        String sql = "SELECT * FROM users_schema.users ORDER BY id";

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                ids.add(id);
                System.out.printf("User: id=%d%n", id);
            }
        }

        // ========================================
        // VERIFY PAGINATION STOPPED
        // ========================================

        System.out.println("\n=== Verifying pagination stopped ===");

        List<LoggedRequest> allRequests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        System.out.printf("Total requests made: %d%n", allRequests.size());

        // Should have made only 2 requests (page 1 and 2)
        // Page 2 returned only 1 user (< pageSize=2), so pagination stops
        assertEquals(2, allRequests.size(),
            "Should stop after page 2 (which has less than pageSize items)");

        // Verify page 3 was NOT requested
        getWireMockServer().verify(0, postRequestedFor(urlEqualTo("/users"))
            .withRequestBody(matchingJsonPath("$.page", equalTo("3")))
        );

        System.out.println("✓ Pagination stopped correctly after page 2");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Should have 3 users (2 from page1, 1 from page2)
        assertEquals(3, ids.size(), "Should have 3 users total");

        System.out.println("✓ Results verified: 3 users fetched before pagination stopped");
    }
}