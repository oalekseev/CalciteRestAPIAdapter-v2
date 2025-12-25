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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test different JSON and XML response structures:
 * 1. Anonymous JSON/XML array - response is a direct array without wrapping object
 * 2. Single row response - response is a single object, not in an array (interpreted as one-row table)
 * 3. Fields with array - response has fields and an array, where fields repeat for each array item
 */
public class ResponseStructureTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "ResponseStructureTest";
    }

    @Test
    @DisplayName("Anonymous JSON Array: Direct array response without wrapping object")
    public void testAnonymousJsonArrayResponse() throws Exception {
        String specFile = "json-response-structure.yaml";

        // Setup: Return anonymous JSON array response (no wrapping object)
        setupStaticJsonResponse("/data", "anonymous-array-response.json");

        setupConnection(specFile);

        System.out.println("=== Test: Anonymous JSON Array Response ===");
        System.out.println("Response contains direct array without wrapping object");
        System.out.println();

        String sql = """
            SELECT id, name, item_value
            FROM anonymous_array_schema.data
            ORDER BY id
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("id", rs.getInt("id"));
                row.put("name", rs.getString("name"));
                row.put("item_value", rs.getString("item_value"));
                rows.add(row);

                System.out.printf("ID: %d | Name: %s | Value: %s%n",
                    row.get("id"),
                    row.get("name"),
                    row.get("item_value"));
            }
        }

        // ========================================
        // VERIFY ANONYMOUS ARRAY PROCESSING
        // ========================================

        System.out.println("\n=== Verifying anonymous array processing ===");

        // The anonymous array should result in 3 rows
        assertEquals(3, rows.size(), "Anonymous array should produce 3 rows");

        // Check first row
        assertEquals(1, rows.get(0).get("id"));
        assertEquals("Item 1", rows.get(0).get("name"));
        assertEquals("Value 1", rows.get(0).get("item_value"));

        // Check second row
        assertEquals(2, rows.get(1).get("id"));
        assertEquals("Item 2", rows.get(1).get("name"));
        assertEquals("Value 2", rows.get(1).get("item_value"));

        // Check third row
        assertEquals(3, rows.get(2).get("id"));
        assertEquals("Item 3", rows.get(2).get("name"));
        assertEquals("Value 3", rows.get(2).get("item_value"));

        System.out.println("✓ Anonymous array processing verified: 3 rows from direct array");

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/data")
            )
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body: " + requestBody);

        System.out.println("✓ Request made to /data endpoint");
    }

    @Test
    @DisplayName("Anonymous XML Array: XML response with items array")
    public void testAnonymousXmlArrayResponse() throws Exception {
        String specFile = "xml-response-structure.yaml";

        // Setup: Return XML response with items array
        setupStaticXmlResponse("/data-xml", "anonymous-array-response.xml");

        setupConnection(specFile);

        System.out.println("=== Test: Anonymous XML Array Response ===");
        System.out.println("Response contains XML with items array");
        System.out.println();

        String sql = """
            SELECT id, name, item_value
            FROM anonymous_array_xml_schema.data_xml
            ORDER BY id
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("id", rs.getInt("id"));
                row.put("name", rs.getString("name"));
                row.put("item_value", rs.getString("item_value"));
                rows.add(row);

                System.out.printf("ID: %d | Name: %s | Value: %s%n",
                    row.get("id"),
                    row.get("name"),
                    row.get("item_value"));
            }
        }

        // ========================================
        // VERIFY ANONYMOUS XML ARRAY PROCESSING
        // ========================================

        System.out.println("\n=== Verifying anonymous XML array processing ===");

        // The XML array should result in 3 rows
        assertEquals(3, rows.size(), "Anonymous XML array should produce 3 rows");

        // Check first row
        assertEquals(1, rows.get(0).get("id"));
        assertEquals("Item 1", rows.get(0).get("name"));
        assertEquals("Value 1", rows.get(0).get("item_value"));

        // Check second row
        assertEquals(2, rows.get(1).get("id"));
        assertEquals("Item 2", rows.get(1).get("name"));
        assertEquals("Value 2", rows.get(1).get("item_value"));

        // Check third row
        assertEquals(3, rows.get(2).get("id"));
        assertEquals("Item 3", rows.get(2).get("name"));
        assertEquals("Value 3", rows.get(2).get("item_value"));

        System.out.println("✓ Anonymous XML array processing verified: 3 rows from XML array");

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/data-xml")
            )
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body: " + requestBody);

        System.out.println("✓ Request made to /data-xml endpoint");
    }

    @Test
    @DisplayName("Single XML Object: Single XML object (interpreted as one-row table)")
    public void testSingleXmlObjectResponse() throws Exception {
        String specFile = "xml-response-structure.yaml";

        // Setup: Return single XML object (not in an array)
        setupStaticXmlResponse("/single-xml", "single-row-response.xml");

        setupConnection(specFile);

        System.out.println("=== Test: Single XML Object Response ===");
        System.out.println("Response contains single XML object, interpreted as one-row table");
        System.out.println();

        String sql = """
            SELECT id, name, description, status
            FROM single_row_xml_schema.single_xml
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("id", rs.getInt("id"));
                row.put("name", rs.getString("name"));
                row.put("description", rs.getString("description"));
                row.put("status", rs.getString("status"));
                rows.add(row);

                System.out.printf("ID: %d | Name: %s | Description: %s | Status: %s%n",
                    row.get("id"),
                    row.get("name"),
                    row.get("description"),
                    row.get("status"));
            }
        }

        // ========================================
        // VERIFY SINGLE XML OBJECT PROCESSING
        // ========================================

        System.out.println("\n=== Verifying single XML object processing ===");

        // The single XML object should result in 1 row
        assertEquals(1, rows.size(), "Single XML object should produce 1 row");

        // Check the single row
        assertEquals(50, rows.get(0).get("id"));
        assertEquals("Standalone Item", rows.get(0).get("name"));
        assertEquals("This is a standalone item in a single object response", rows.get(0).get("description"));
        assertEquals("active", rows.get(0).get("status"));

        System.out.println("✓ Single XML object processing verified: 1 row from single XML object");

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/single-xml")
            )
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body: " + requestBody);

        System.out.println("✓ Request made to /single-xml endpoint");
    }

    @Test
    @DisplayName("XML Fields with Array: XML response with root fields and nested array, root fields repeat for each array item")
    public void testXmlFieldsWithArrayResponse() throws Exception {
        String specFile = "xml-response-structure.yaml";

        // Setup: Return XML response with root fields and nested array
        setupStaticXmlResponse("/complex-xml", "fields-with-array-response.xml");

        setupConnection(specFile);

        System.out.println("=== Test: XML Fields with Array Response ===");
        System.out.println("Response contains root fields and nested array, root fields repeat for each array item");
        System.out.println();

        String sql = """
            SELECT response_id, response_timestamp, response_source, category_id, category_name, item_id, item_name, item_price
            FROM fields_array_xml_schema.complex_xml
            ORDER BY category_id, item_id
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("response_id", rs.getInt("response_id"));
                row.put("response_timestamp", rs.getString("response_timestamp"));
                row.put("response_source", rs.getString("response_source"));
                row.put("category_id", rs.getInt("category_id"));
                row.put("category_name", rs.getString("category_name"));
                row.put("item_id", rs.getInt("item_id"));
                row.put("item_name", rs.getString("item_name"));
                row.put("item_price", rs.getDouble("item_price"));
                rows.add(row);

                System.out.printf("Response: %d (%s) | Category: %d (%s) | Item: %d (%s) | Price: $%.2f%n",
                    row.get("response_id"),
                    row.get("response_source"),
                    row.get("category_id"),
                    row.get("category_name"),
                    row.get("item_id"),
                    row.get("item_name"),
                    row.get("item_price"));
            }
        }

        // ========================================
        // VERIFY XML FIELDS WITH ARRAY PROCESSING
        // ========================================

        System.out.println("\n=== Verifying XML fields with array processing ===");

        // The response has 2 categories, each with 2 items = 4 rows total
        assertEquals(4, rows.size(), "XML fields with array should produce 4 rows");

        // Check that root-level fields are repeated for each row
        for (int i = 0; i < 4; i++) {
            assertEquals(12345, rows.get(i).get("response_id"));
            String timestamp = (String) rows.get(i).get("response_timestamp");
            assertTrue(timestamp != null && timestamp.contains("2023-12-20") && timestamp.contains("10:00:00"),
                "Timestamp should contain expected date and time: " + timestamp);
            assertEquals("product_catalog_api", rows.get(i).get("response_source"));
        }

        // Check first category's items
        assertEquals(1, rows.get(0).get("category_id"));
        assertEquals("Electronics", rows.get(0).get("category_name"));
        assertEquals(101, rows.get(0).get("item_id"));
        assertEquals("Laptop", rows.get(0).get("item_name"));
        assertEquals(999.99, rows.get(0).get("item_price"));

        assertEquals(1, rows.get(1).get("category_id"));
        assertEquals("Electronics", rows.get(1).get("category_name"));
        assertEquals(102, rows.get(1).get("item_id"));
        assertEquals("Mouse", rows.get(1).get("item_name"));
        assertEquals(25.99, rows.get(1).get("item_price"));

        // Check second category's items
        assertEquals(2, rows.get(2).get("category_id"));
        assertEquals("Books", rows.get(2).get("category_name"));
        assertEquals(201, rows.get(2).get("item_id"));
        assertEquals("Java Guide", rows.get(2).get("item_name"));
        assertEquals(49.99, rows.get(2).get("item_price"));

        assertEquals(2, rows.get(3).get("category_id"));
        assertEquals("Books", rows.get(3).get("category_name"));
        assertEquals(202, rows.get(3).get("item_id"));
        assertEquals("Python Basics", rows.get(3).get("item_name"));
        assertEquals(39.99, rows.get(3).get("item_price"));

        System.out.println("✓ XML fields with array processing verified: 4 rows with repeated root fields");

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/complex-xml")
            )
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body: " + requestBody);

        System.out.println("✓ Request made to /complex-xml endpoint");
    }

    @Test
    @DisplayName("Single Object Response: Single JSON object (interpreted as one-row table)")
    public void testSingleObjectResponse() throws Exception {
        String specFile = "json-response-structure.yaml";

        // Setup: Return single JSON object (not in an array)
        setupStaticJsonResponse("/single", "single-row-response.json");

        setupConnection(specFile);

        System.out.println("=== Test: Single Object Response ===");
        System.out.println("Response contains single JSON object, interpreted as one-row table");
        System.out.println();

        String sql = """
            SELECT id, name, description, status
            FROM single_row_schema.single
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("id", rs.getInt("id"));
                row.put("name", rs.getString("name"));
                row.put("description", rs.getString("description"));
                row.put("status", rs.getString("status"));
                rows.add(row);

                System.out.printf("ID: %d | Name: %s | Description: %s | Status: %s%n",
                    row.get("id"),
                    row.get("name"),
                    row.get("description"),
                    row.get("status"));
            }
        }

        // ========================================
        // VERIFY SINGLE OBJECT PROCESSING
        // ========================================

        System.out.println("\n=== Verifying single object processing ===");

        // The single object should result in 1 row
        assertEquals(1, rows.size(), "Single object should produce 1 row");

        // Check the single row
        assertEquals(50, rows.get(0).get("id"));
        assertEquals("Standalone Item", rows.get(0).get("name"));
        assertEquals("This is a standalone item in a single object response", rows.get(0).get("description"));
        assertEquals("active", rows.get(0).get("status"));

        System.out.println("✓ Single object processing verified: 1 row from single object");

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/single")
            )
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body: " + requestBody);

        System.out.println("✓ Request made to /single endpoint");
    }

    @Test
    @DisplayName("Fields with Array: Response with fields and array, fields repeat for each array item")
    public void testFieldsWithArrayResponse() throws Exception {
        String specFile = "json-response-structure.yaml";

        // Setup: Return response with fields and array
        setupStaticJsonResponse("/complex", "fields-with-array-response.json");

        setupConnection(specFile);

        System.out.println("=== Test: Fields with Array Response ===");
        System.out.println("Response contains fields and array, fields repeat for each array item");
        System.out.println();

        String sql = """
            SELECT response_id, response_timestamp, response_source, category_id, category_name, item_id, item_name, item_price
            FROM fields_array_schema.complex
            ORDER BY category_id, item_id
            """;

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("response_id", rs.getInt("response_id"));
                row.put("response_timestamp", rs.getString("response_timestamp"));
                row.put("response_source", rs.getString("response_source"));
                row.put("category_id", rs.getInt("category_id"));
                row.put("category_name", rs.getString("category_name"));
                row.put("item_id", rs.getInt("item_id"));
                row.put("item_name", rs.getString("item_name"));
                row.put("item_price", rs.getDouble("item_price"));
                rows.add(row);

                System.out.printf("Response: %d (%s) | Category: %d (%s) | Item: %d (%s) | Price: $%.2f%n",
                    row.get("response_id"),
                    row.get("response_source"),
                    row.get("category_id"),
                    row.get("category_name"),
                    row.get("item_id"),
                    row.get("item_name"),
                    row.get("item_price"));
            }
        }

        // ========================================
        // VERIFY FIELDS WITH ARRAY PROCESSING
        // ========================================

        System.out.println("\n=== Verifying fields with array processing ===");

        // The response has 2 categories, each with 2 items = 4 rows total
        assertEquals(4, rows.size(), "Fields with array should produce 4 rows");

        // Check that root-level fields are repeated for each row
        for (int i = 0; i < 4; i++) {
            assertEquals(12345, rows.get(i).get("response_id"));
            // The timestamp format might be processed differently by the system
            // Check that it contains the expected date/time components
            String timestamp = (String) rows.get(i).get("response_timestamp");
            assertTrue(timestamp != null && timestamp.contains("2023-12-20") && timestamp.contains("10:00:00"),
                "Timestamp should contain expected date and time: " + timestamp);
            assertEquals("product_catalog_api", rows.get(i).get("response_source"));
        }

        // Check first category's items
        assertEquals(1, rows.get(0).get("category_id"));
        assertEquals("Electronics", rows.get(0).get("category_name"));
        assertEquals(101, rows.get(0).get("item_id"));
        assertEquals("Laptop", rows.get(0).get("item_name"));
        assertEquals(999.99, rows.get(0).get("item_price"));

        assertEquals(1, rows.get(1).get("category_id"));
        assertEquals("Electronics", rows.get(1).get("category_name"));
        assertEquals(102, rows.get(1).get("item_id"));
        assertEquals("Mouse", rows.get(1).get("item_name"));
        assertEquals(25.99, rows.get(1).get("item_price"));

        // Check second category's items
        assertEquals(2, rows.get(2).get("category_id"));
        assertEquals("Books", rows.get(2).get("category_name"));
        assertEquals(201, rows.get(2).get("item_id"));
        assertEquals("Java Guide", rows.get(2).get("item_name"));
        assertEquals(49.99, rows.get(2).get("item_price"));

        assertEquals(2, rows.get(3).get("category_id"));
        assertEquals("Books", rows.get(3).get("category_name"));
        assertEquals(202, rows.get(3).get("item_id"));
        assertEquals("Python Basics", rows.get(3).get("item_name"));
        assertEquals(39.99, rows.get(3).get("item_price"));

        System.out.println("✓ Fields with array processing verified: 4 rows with repeated root fields");

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor(
                com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/complex")
            )
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body: " + requestBody);

        System.out.println("✓ Request made to /complex endpoint");
    }
}