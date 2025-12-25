package org.apache.calcite.adapter.restapi.rest;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.parser.OpenAPIV3Parser;
import org.apache.calcite.adapter.restapi.model.ApiRequestConfig;
import org.apache.calcite.adapter.restapi.model.ApiService;
import org.apache.calcite.adapter.restapi.model.ApiTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import freemarker.template.TemplateModel;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/**
 * {@code RestApiSchema} represents a Calcite schema for a single OpenAPI specification file.
 * It creates tables from all endpoints defined in that spec.
 *
 * <p>This schema is used by {@link RestSchema} to create separate schemas for each OpenAPI file.
 */
public class RestApiSchema extends AbstractSchema {
    private final Logger logger = LoggerFactory.getLogger(RestApiSchema.class);

    /** Path to the OpenAPI specification file */
    private final String openapiFilePath;
    /** Schema name for this specific schema */
    private final String schemaName;
    /** Global template context, available to all Freemarker templates. */
    private final Map<String, TemplateModel> context;
    /** Logical group name for configuration (optional). */
    private final String group;
    /** Directory containing OpenAPI specs and templates */
    private final String restDirectory;
    /** Calcite table mapping, lazily initialized. */
    private Map<String, Table> tableMap;

    /**
     * Constructs a REST API schema for a single OpenAPI file and a specific schema name.
     *
     * @param openapiFilePath Path to the OpenAPI specification file
     * @param schemaName Name of the specific schema this instance represents
     * @param restDirectory Directory containing templates
     * @param group Logical group name (optional)
     * @param context Freemarker context variables
     */
    public RestApiSchema(String openapiFilePath, String schemaName, String restDirectory, String group, Map<String, TemplateModel> context) {
        this.openapiFilePath = openapiFilePath;
        this.schemaName = schemaName;
        this.restDirectory = restDirectory;
        this.group = group;
        this.context = context != null ? new HashMap<>(context) : new HashMap<>();
    }

    /**
     * Returns the complete Calcite table map for this OpenAPI spec and specific schema.
     * Tables are created for endpoints that belong to this schema.
     *
     * @return Table map: table name -> Table implementation.
     */
    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap != null) {
            return tableMap;
        }
        tableMap = new HashMap<>();

        // Load all Freemarker request templates from the directory
        Map<String, String> requestTemplates = loadTemplates(restDirectory);

        // Parse this OpenAPI specification - now returns a list of services
        List<ApiService> apiServices = OpenApiSchemaConverter.fromOpenApi(openapiFilePath, requestTemplates);

        // Process services to find tables for this specific schema
        for (ApiService apiService : apiServices) {
            if (schemaName.equals(apiService.getSchemaName())) {
                logger.info("Processing service with schema '{}', adding {} tables", schemaName, apiService.getTables().size());
                for (ApiTable apiTable : apiService.getTables()) {
                    // Use table-level requestConfig if available, otherwise fall back to service-level
                    ApiRequestConfig requestConfig = apiTable.getRequestConfig() != null
                            ? apiTable.getRequestConfig()
                            : apiService.getRequestConfig();

                    tableMap.put(
                            apiTable.getName(),
                            new RestTable(
                                    apiService.getDataSourceName() != null ? apiService.getDataSourceName() : group,
                                    requestConfig,  // Use table-specific config
                                    apiTable,
                                    context));
                }
            }
        }

        // If no tables were found for the specific schema name, try to find tables that might belong to it
        // by checking if there's a schema with a generic name fallback
        if (tableMap.isEmpty()) {
            // Try to find if there are any tables from the same OpenAPI file that should belong to this schema
            // by re-parsing and checking table configurations
            OpenAPI openAPI = new OpenAPIV3Parser().read(openapiFilePath);
            String fileName = java.nio.file.Paths.get(openapiFilePath).getFileName().toString();
            String fallbackSchemaName = fileName.replaceFirst("\\.(yaml|yml|json)$", "");

            if (schemaName.equals(fallbackSchemaName)) {
                // If this schema name matches the default file-based name,
                // get tables from all services in the file (fallback for single schema per file)
                for (ApiService apiService : apiServices) {
                    for (ApiTable apiTable : apiService.getTables()) {
                        tableMap.put(
                                apiTable.getName(),
                                new RestTable(
                                        apiService.getDataSourceName() != null ? apiService.getDataSourceName() : group,
                                        apiService.getRequestConfig(),
                                        apiTable,
                                        context));
                    }
                }
            }
        }

        return tableMap;
    }

    /**
     * Loads Freemarker request body templates from a given directory.
     * Templates must have a ".ftl" extension, and are mapped to tables using the file name (without the extension).
     * Also stores templates with full file name (including .ftl extension) for x-calcite-restapi-request-body-template support.
     *
     * @param directory Directory containing .ftl files.
     * @return Map of table name (or full file name) to template content.
     */
    private Map<String, String> loadTemplates(String directory) {
        Map<String, String> templates = new HashMap<>();
        Path dirPath = Paths.get(directory);
        if (Files.exists(dirPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, "*.ftl")) {
                for (Path path : stream) {
                    String fileName = path.getFileName().toString();
                    String tableName = fileName.replaceFirst("\\.ftl$", "");
                    String content = Files.readString(path);
                    // Store by table name (backward compatibility)
                    templates.put(tableName, content);
                    // Also store by full file name (for x-calcite-restapi-request-body-template extension)
                    templates.put(fileName, content);
                }
            } catch (IOException e) {
                logger.warn("Could not load Freemarker templates: {}", e.getMessage());
            }
        }
        return templates;
    }
}
