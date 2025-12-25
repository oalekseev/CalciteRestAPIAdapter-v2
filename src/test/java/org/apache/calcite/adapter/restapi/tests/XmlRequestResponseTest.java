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
 * Simplified XML request/response test using static XML files.
 *
 * This test demonstrates:
 * 1. How adapter parses XML responses
 * 2. How adapter sends XML request bodies
 * 3. How to verify XML request format
 * 4. How filters work with XML format
 */
public class XmlRequestResponseTest extends BaseTest {

    @Override
    protected String getTestResourceDirectory() {
        return "XmlRequestResponseTest";
    }

    /**
     * Override to provide template variables for FreeMarker templates.
     */
    @Override
    protected Properties getConnectionProperties() {
        Properties props = super.getConnectionProperties();
        props.setProperty("jwtToken", "TEST_XML_JWT_TOKEN");
        return props;
    }

    @Test
    @DisplayName("XML Response: SELECT * - All books from XML")
    public void testSelectAllFromXml() throws Exception {
        String specFile = "xml-request-response.yaml";

        // Setup: Return XML response
        setupStaticXmlResponse("/books-xml", "books-all.xml");

        setupConnection(specFile);

        System.out.println("=== Test: SELECT * from XML response ===");

        String sql = "SELECT * FROM books_schema.books ORDER BY book_id";

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("book_id");
                String title = rs.getString("book_title");
                String author = rs.getString("book_author");
                int year = rs.getInt("book_year");
                String genre = rs.getString("book_genre");

                ids.add(id);

                System.out.printf("Book: id=%d, title='%s', author='%s', year=%d, genre=%s%n",
                    id, title, author, year, genre);
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/books-xml"))
        );

        assertTrue(!requests.isEmpty(), "Should have made request to /books");

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body:");
        System.out.println(requestBody);

        // Verify XML request format
        assertTrue(requestBody.startsWith("<?xml") || requestBody.contains("<request>"),
            "Request body should be in XML format");

        System.out.println("✓ Request verified: XML format");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Should have all 5 books from XML
        assertEquals(List.of(1, 2, 3, 4, 5), ids,
            "Should parse all 5 books from XML response");

        System.out.println("✓ Results verified: All 5 books parsed from XML");
    }

    @Test
    @DisplayName("XML Response: SELECT with filter - WHERE year >= 1950")
    public void testSelectWithYearFilterFromXml() throws Exception {
        String specFile = "xml-request-response.yaml";

        // Setup: Return filtered XML when year filter is present
        getWireMockServer().stubFor(post(urlEqualTo("/books-xml"))
            .withRequestBody(containing("year"))
            .withRequestBody(containing(">="))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/xml")
                .withBodyFile("XmlRequestResponseTest/responses/books-year-gte-1950.xml")));

        // Fallback
        setupStaticXmlResponse("/books-xml", "books-all.xml");

        setupConnection(specFile);

        System.out.println("=== Test: SELECT WHERE year >= 1950 from XML ===");

        String sql = "SELECT * FROM books_schema.books WHERE book_year >= 1950 ORDER BY book_id";

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("book_id");
                String title = rs.getString("book_title");
                int year = rs.getInt("book_year");

                ids.add(id);

                System.out.printf("Book: id=%d, title='%s', year=%d%n", id, title, year);
            }
        }

        // ========================================
        // VERIFY HTTP REQUEST
        // ========================================

        System.out.println("\n=== Verifying HTTP request ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/books-xml"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body:");
        System.out.println(requestBody);

        // Verify filter is in request (XML format)
        assertTrue(requestBody.contains("year"), "Request should contain year filter");

        System.out.println("✓ Request verified: contains year filter in XML");

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Expected: To Kill a Mockingbird (1960, id=3), The Catcher in the Rye (1951, id=5)
        assertEquals(List.of(3, 5), ids,
            "Should return 2 books with year >= 1950");

        System.out.println("✓ Results verified: 2 books with year >= 1950 from XML");
    }

    @Test
    @DisplayName("XML Response: SELECT with genre filter")
    public void testSelectWithGenreFilterFromXml() throws Exception {
        String specFile = "xml-request-response.yaml";

        // Setup: Return Fiction books XML
        getWireMockServer().stubFor(post(urlEqualTo("/books-xml"))
            .withRequestBody(containing("genre"))
            .withRequestBody(containing("Fiction"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/xml")
                .withBodyFile("XmlRequestResponseTest/responses/books-fiction-genre.xml")));

        setupStaticXmlResponse("/books-xml", "books-all.xml");

        setupConnection(specFile);

        System.out.println("=== Test: SELECT WHERE genre = 'Fiction' from XML ===");

        String sql = "SELECT * FROM books_schema.books WHERE book_genre = 'Fiction' ORDER BY book_id";

        List<Integer> ids = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("book_id");
                String title = rs.getString("book_title");
                String genre = rs.getString("book_genre");

                ids.add(id);

                System.out.printf("Book: id=%d, title='%s', genre=%s%n", id, title, genre);
            }
        }

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        // Expected: The Great Gatsby (id=1), To Kill a Mockingbird (id=3),
        //           The Catcher in the Rye (id=5)
        assertEquals(List.of(1, 3, 5), ids,
            "Should return 3 Fiction books");

        // Verify all are Fiction
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String genre = rs.getString("book_genre");
                assertEquals("Fiction", genre, "All books should be Fiction genre");
            }
        }

        System.out.println("✓ Results verified: 3 Fiction books from XML");
    }


    @Test
    @DisplayName("XML Response: SELECT with projections")
    public void testSelectWithProjectionsFromXml() throws Exception {
        String specFile = "xml-request-response.yaml";

        setupStaticXmlResponse("/books-xml", "books-all.xml");
        setupConnection(specFile);

        System.out.println("=== Test: SELECT id, title (projections) from XML ===");

        String sql = "SELECT book_id, book_title FROM books_schema.books ORDER BY book_id LIMIT 3";

        List<String> titles = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                int id = rs.getInt("book_id");
                String title = rs.getString("book_title");

                titles.add(title);

                System.out.printf("Book: id=%d, title='%s'%n", id, title);
            }
        }

        // ========================================
        // VERIFY REQUEST WITH PROJECTIONS
        // ========================================

        System.out.println("\n=== Verifying request with projections ===");

        List<LoggedRequest> requests = getWireMockServer().findAll(
            postRequestedFor(urlEqualTo("/books-xml"))
        );

        String requestBody = requests.get(0).getBodyAsString();
        System.out.println("Request body:");
        System.out.println(requestBody);

        // Check if projections are in request (if template supports it)
        if (requestBody.contains("projections") || requestBody.contains("<projections>")) {
            System.out.println("✓ Request contains projections");
        } else {
            System.out.println("ℹ Template might not include projections (check template file)");
        }

        // ========================================
        // VERIFY RESULTS
        // ========================================

        System.out.println("\n=== Verifying results ===");

        assertEquals(3, titles.size(), "Should return 3 books");

        List<String> expectedTitles = List.of(
            "The Great Gatsby",
            "1984",
            "To Kill a Mockingbird"
        );

        assertEquals(expectedTitles, titles, "Should have correct titles");

        System.out.println("✓ Results verified: 3 books with projections from XML");
    }
}
