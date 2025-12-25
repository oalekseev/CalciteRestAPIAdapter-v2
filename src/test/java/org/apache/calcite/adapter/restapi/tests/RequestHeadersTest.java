package org.apache.calcite.adapter.restapi.tests;

import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.apache.calcite.adapter.restapi.tests.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simplified request headers test using static responses.
 *
 * This test demonstrates:
 * 1. How to use x-calcite-restapi-request-headers-template for custom headers
 * 2. How to verify HTTP requests contain the correct headers
 * 3. How FreeMarker templates work for headers with variables
 *
 * Template variables (from Properties):
 * - jwtToken: JWT authentication token
 * - restApiVersion: API version header
 * - contentType: Content-Type header
 */
public class RequestHeadersTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "RequestHeadersTest";
    }

    /**
     * Override to provide template variables for FreeMarker headers template.
     */
    @Override
    protected Properties getConnectionProperties() {
        Properties props = super.getConnectionProperties();
        props.setProperty("restApiVersion", "v2.0");
        props.setProperty("contentType", "application/json");
        props.setProperty("jwtToken", "SOME_JWT_TOKEN_HERE");
        return props;
    }

    @Test
    @DisplayName("Custom Headers: Verify Authorization and API-Version headers")
    public void testCustomHeadersInRequest() throws Exception {
        String specFile = "request-headers.yaml";

        // Setup: Return static response (headers are tested, not filtering)
        setupStaticJsonResponse("/users", "users-alice.json");

        setupConnection(specFile);

        System.out.println("=== Test: Custom Headers (Authorization, API-Version) ===");

        String sql = """
            SELECT *
            FROM users_schema.users
            WHERE name = 'Alice'
            """;

        List<String> names = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String name = rs.getString("name");
                names.add(name);
                System.out.printf("User: %s, age: %d, department: %s%n",
                    name, rs.getInt("age"), rs.getString("department"));
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST HEADERS
        // ========================================

        System.out.println("\n=== Verifying HTTP request headers ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        assertTrue(!requests.isEmpty(), "Should have made request to /users");
        LoggedRequest request = requests.get(0);

        System.out.println("Headers:");
        request.getHeaders().all().forEach(header -> {
            System.out.printf("  %s: %s%n", header.key(), header.firstValue());
        });

        // Verify Authorization header
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withHeader("Authorization", equalTo("Bearer SOME_JWT_TOKEN_HERE"))
        );
        System.out.println("✓ Authorization header verified: Bearer SOME_JWT_TOKEN_HERE");

        // Verify API-Version header
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withHeader("API-Version", equalTo("v2.0"))
        );
        System.out.println("✓ API-Version header verified: v2.0");

        // Verify Content-Type header
        getWireMockServer().verify(postRequestedFor(urlEqualTo("/users"))
            .withHeader("Content-Type", equalTo("application/json"))
        );
        System.out.println("✓ Content-Type header verified: application/json");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        assertTrue(names.size() > 0, "Should return Alice");
        System.out.println("✓ Results verified: Found " + names.size() + " user(s)");
    }

    @Test
    @DisplayName("Custom Headers: Inspect all headers sent")
    public void testInspectAllHeaders() throws Exception {
        String specFile = "request-headers.yaml";

        setupStaticJsonResponse("/users", "users-alice.json");
        setupConnection(specFile);

        System.out.println("=== Test: Inspect all HTTP headers ===");

        String sql = "SELECT name FROM users_schema.users LIMIT 1";

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) { /* consume */ }
        }

        // ========================================
        // INSPECT ALL HEADERS
        // ========================================

        System.out.println("\n=== All HTTP headers sent ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/users"))
        );

        LoggedRequest request = requests.get(0);

        request.getHeaders().all().forEach(header -> {
            System.out.printf("%-20s: %s%n", header.key(), header.firstValue());
        });

        System.out.println("\n=== Analysis ===");

        if (request.getHeader("Authorization") != null) {
            System.out.println("✓ Contains Authorization header (from template)");
        }

        if (request.getHeader("API-Version") != null) {
            System.out.println("✓ Contains API-Version header (from template)");
        }

        if (request.getHeader("Content-Type") != null) {
            System.out.println("✓ Contains Content-Type header (from template)");
        }

        System.out.println("\nThese headers are generated by the FreeMarker template:");
        System.out.println("  headers-template.ftl with variables from Properties");
        System.out.println("  jwtToken = \"SOME_JWT_TOKEN_HERE\"");
        System.out.println("  restApiVersion = \"v2.0\"");
        System.out.println("  contentType = \"application/json\"");
    }
}
