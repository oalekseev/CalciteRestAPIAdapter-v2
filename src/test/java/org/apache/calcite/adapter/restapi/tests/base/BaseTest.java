package org.apache.calcite.adapter.restapi.tests.base;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Simple base test class WITHOUT any transformers or filtering logic.
 * Uses only static JSON/XML/CSV responses from files.
 *
 * Tests verify:
 * 1. Adapter forms correct HTTP request (using WireMock.verify())
 * 2. Adapter parses static response correctly
 * 3. SQL returns correct data
 */
public abstract class BaseTest {

    protected static WireMockServer wireMockServer;
    protected Connection connection;

    /**
     * Returns the test resource directory name (e.g., "JoinTest", "ProjectionsTest")
     */
    protected abstract String getTestResourceDirectory();

    @BeforeEach
    public void setupWireMock() {
        if (wireMockServer == null) {
            // NO transformers! Simple WireMock server that only returns static responses
            wireMockServer = new WireMockServer(
                WireMockConfiguration.wireMockConfig().dynamicPort()
            );
            wireMockServer.start();
            System.out.println("[BaseTest] WireMock server started on port: " + wireMockServer.port());
        }
        // Reset WireMock before each test to ensure isolation
        configureFor("localhost", wireMockServer.port());
        wireMockServer.resetAll();
    }

    /**
     * Setup a static JSON response for an endpoint
     * @param endpoint REST endpoint (e.g., "/users")
     * @param responseFile JSON file name in test resources (e.g., "users-all.json")
     */
    protected void setupStaticJsonResponse(String endpoint, String responseFile) throws IOException {
        setupStaticJsonResponse(urlEqualTo(endpoint), responseFile);
    }

    protected void setupStaticJsonResponse(UrlPattern urlPattern, String responseFile) throws IOException {
        String responseBody = loadStaticResponse(responseFile);

        wireMockServer.stubFor(post(urlPattern)
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody(responseBody)));

        System.out.println("[BaseTest] Configured static JSON response for endpoint: " + urlPattern.toString() +
            " from file: " + responseFile);
    }

    /**
     * Setup a static XML response for an endpoint
     * @param endpoint REST endpoint (e.g., "/users")
     * @param responseFile XML file name in test resources (e.g., "users-all.xml")
     */
    protected void setupStaticXmlResponse(String endpoint, String responseFile) throws IOException {
        String responseBody = loadStaticResponse(responseFile);

        wireMockServer.stubFor(post(urlEqualTo(endpoint))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/xml")
                .withBody(responseBody)));

        System.out.println("[BaseTest] Configured static XML response for endpoint: " + endpoint +
            " from file: " + responseFile);
    }

    /**
     * Setup a static CSV response for an endpoint
     * @param endpoint REST endpoint (e.g., "/users")
     * @param responseFile CSV file name in test resources (e.g., "users-all.csv")
     */
    protected void setupStaticCsvResponse(String endpoint, String responseFile) throws IOException {
        String responseBody = loadStaticResponse(responseFile);

        wireMockServer.stubFor(post(urlEqualTo(endpoint))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "text/csv")
                .withBody(responseBody)));

        System.out.println("[BaseTest] Configured static CSV response for endpoint: " + endpoint +
            " from file: " + responseFile);
    }

    /**
     * Load static response file from test resources
     */
    protected String loadStaticResponse(String responseFile) throws IOException {
        Path responsePath = Paths.get("src", "test", "resources", "rest",
            getTestResourceDirectory(), "responses", responseFile);

        if (!Files.exists(responsePath)) {
            throw new IOException("Static response file not found: " + responsePath.toAbsolutePath());
        }

        return Files.readString(responsePath);
    }

    /**
     * Override this to provide custom connection properties (e.g., FreeMarker template variables).
     * Default implementation returns basic properties.
     */
    protected Properties getConnectionProperties() {
        Properties properties = new Properties();
        properties.setProperty("lex", "JAVA");
        return properties;
    }

    /**
     * Setup Calcite connection with OpenAPI spec
     * @param openApiSpecFile OpenAPI YAML file name (e.g., "single-schema-multi-tables.yaml")
     */
    protected void setupConnection(String openApiSpecFile) throws Exception {
        // Create temp directory for test resources
        String tempDir = Files.createTempDirectory("calcite-simple-test-" + openApiSpecFile)
            .toAbsolutePath()
            .toString();

        // Read and modify OpenAPI spec to point to WireMock
        String originalPath = Paths.get("src", "test", "resources", "rest",
            getTestResourceDirectory(), openApiSpecFile).toAbsolutePath().toString();
        String openapiContent = Files.readString(Paths.get(originalPath));

        // Replace server URL with WireMock URL
        openapiContent = openapiContent.replaceAll(
            "- url: http://localhost:8080",
            "- url: http://localhost:" + wireMockServer.port()
        );

        Files.writeString(Paths.get(tempDir, openApiSpecFile), openapiContent);

        // Copy all .ftl template files to temp directory (both from templates/ and root)
        // First try templates/ subdirectory
        Path templateSourceDir = Paths.get("src", "test", "resources", "rest",
            getTestResourceDirectory(), "templates");
        if (Files.exists(templateSourceDir)) {
            try (var stream = Files.list(templateSourceDir)) {
                stream.filter(p -> p.toString().endsWith(".ftl"))
                    .forEach(ftlFile -> {
                        try {
                            Files.copy(ftlFile,
                                Paths.get(tempDir, ftlFile.getFileName().toString()),
                                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to copy template file: " + ftlFile, e);
                        }
                    });
            }
        }

        // Also copy .ftl files from root test directory
        Path testRootDir = Paths.get("src", "test", "resources", "rest", getTestResourceDirectory());
        if (Files.exists(testRootDir)) {
            try (var stream = Files.list(testRootDir)) {
                stream.filter(p -> p.toString().endsWith(".ftl"))
                    .forEach(ftlFile -> {
                        try {
                            Files.copy(ftlFile,
                                Paths.get(tempDir, ftlFile.getFileName().toString()),
                                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to copy template file: " + ftlFile, e);
                        }
                    });
            }
        }

        // Set system property for Calcite to find specs
        System.setProperty("calcite.rest", tempDir);

        // Build Calcite connection properties (with support for template variables)
        Properties properties = getConnectionProperties();

        // Create Calcite connection
        DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
        org.apache.calcite.jdbc.CalciteConnection calciteConnection =
            DriverManager.getConnection("jdbc:calcite:", properties)
                .unwrap(org.apache.calcite.jdbc.CalciteConnection.class);

        // Register REST schema - this will load all OpenAPI specs from the directory
        Map<String, Object> contextMap = new HashMap<>();
        org.apache.calcite.schema.SchemaPlus rootSchema = calciteConnection.getRootSchema();
        org.apache.calcite.schema.SchemaFactory restSchemaFactory = new org.apache.calcite.adapter.restapi.rest.RestSchemaFactory();
        org.apache.calcite.adapter.restapi.rest.RestSchema restSchema =
            (org.apache.calcite.adapter.restapi.rest.RestSchema) restSchemaFactory.create(rootSchema, "", contextMap);
        restSchema.registerSubSchemas(rootSchema);

        connection = calciteConnection;

        System.out.println("[BaseTest] Calcite connection established with spec: " + openApiSpecFile);
        System.out.println("[BaseTest] Temp directory: " + tempDir);
    }

    @AfterEach
    public void tearDown() {
        if (connection != null) {
            try {
                connection.close();
                System.out.println("[BaseTest] Connection closed");
            } catch (SQLException e) {
                System.err.println("[BaseTest] Error closing connection: " + e.getMessage());
            }
            connection = null;
        }
    }

    @AfterAll
    public static void tearDownWiremock() {
        if (wireMockServer != null) {
            wireMockServer.stop();
            System.out.println("[BaseTest] WireMock server stopped");
            wireMockServer = null;
        }
    }

    protected WireMockServer getWireMockServer() {
        return wireMockServer;
    }

    protected Connection getConnection() {
        return connection;
    }
}