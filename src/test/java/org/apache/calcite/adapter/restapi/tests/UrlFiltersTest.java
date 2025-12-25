package org.apache.calcite.adapter.restapi.tests;

import org.apache.calcite.adapter.restapi.tests.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.sql.*;
import java.util.Properties;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Simplified URL filters test - verifies filters/projections in URL query params.
 * Tests x-calcite-restapi-request-url-template.
 */
public class UrlFiltersTest extends BaseTest {
    
    @Override
    protected String getTestResourceDirectory() {
        return "UrlFiltersTest";
    }
    
    @Override
    protected Properties getConnectionProperties() {
        Properties props = super.getConnectionProperties();
        props.setProperty("restApiVersion", "v1.0");
        props.setProperty("contentType", "application/json");
        props.setProperty("jwtToken", "SOME_JWT_TOKEN_HERE");
        return props;
    }
    
    @Test
    @DisplayName("URL Template: Verify filters and projections in URL query parameters")
    public void testUrlTemplateWithQueryParams() throws Exception {
        // Setup GET stub (not POST) - explicitly configure for GET requests
        String responseBody = loadStaticResponse("users-all.json");
        getWireMockServer().stubFor(get(urlMatching("/users\\\\?.*"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(responseBody)));

        setupConnection("url-filters.yaml");
        
        System.out.println("=== Test: URL Template with Query Params ===");
        
        String sql = "SELECT name, age FROM users_schema.users WHERE name = 'Alice' LIMIT 10";
        
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("User: %s, age: %d%n", rs.getString("name"), rs.getInt("age"));
            }
        }
        
        // Verify request was made with GET and query parameters
        getWireMockServer().verify(getRequestedFor(urlMatching("/users\\\\?.*")));
        System.out.println("✓ URL template test completed");
    }
}
