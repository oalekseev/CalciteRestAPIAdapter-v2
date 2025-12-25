package org.apache.calcite.adapter.restapi.rest;

import org.apache.calcite.adapter.restapi.model.ApiService;
import org.apache.calcite.adapter.restapi.rest.config.AdapterConfiguration;
import org.apache.calcite.adapter.restapi.rest.config.SystemPropertyConfiguration;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import freemarker.template.TemplateModel;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * {@code RestSchema} is the root schema that creates separate sub-schemas for each OpenAPI specification file.
 * Each OpenAPI file becomes its own schema, allowing SQL queries like: SELECT * FROM schemaName.tableName
 *
 * <p>
 * Features:
 * <ul>
 *   <li>Auto-discovers OpenAPI files (.yaml/.yml/.json) from a specified directory (configured via system property).</li>
 *   <li>Creates a separate Calcite sub-schema for each OpenAPI file (schema name = file name without extension).</li>
 *   <li>Each sub-schema contains tables from its corresponding OpenAPI spec.</li>
 * </ul>
 *
 * <p><b>Usage:</b> Place your OpenAPI specs and corresponding Freemarker templates into a directory.
 * The directory path is determined by the {@code calcite.rest} system property or defaults to {@code ${catalina.base}/calcite/rest}.
 * The schema will automatically create SQL schemas for every OpenAPI file, with tables for each endpoint.
 *
 * <p><b>Example:</b>
 * <pre>
 * Directory: /path/to/rest/
 *   - users.yaml      -> creates schema "users" with tables from users.yaml
 *   - orders.yaml     -> creates schema "orders" with tables from orders.yaml
 *
 * SQL: SELECT * FROM users.getUsers
 * SQL: SELECT * FROM orders.getOrders
 * </pre>
 */
public class RestSchema extends AbstractSchema {
    private final Logger logger = LoggerFactory.getLogger(RestSchema.class);

    /** Global template context, available to all Freemarker templates. */
    private final Map<String, TemplateModel> context;
    /** Logical group name for configuration (optional). */
    private final String group;
    /** Map of sub-schemas (one per OpenAPI file), lazily initialized. */
    private Map<String, org.apache.calcite.schema.Schema> subSchemaMap;
    /** Configuration provider for adapter settings. */
    private final AdapterConfiguration configuration;

    /**
     * Constructs the REST schema using configuration options provided via a map.
     * The map may contain "group" for logical grouping and "context" for exposing Freemarker context variables.
     *
     * @param map Configuration map containing group/context settings.
     */
    @SuppressWarnings("unchecked")
    public RestSchema(Map<String, Object> map) {
        if (map != null) {
            this.group = (String) map.get("group");
            Object contextObject = map.get("context");
            if (contextObject != null)
                this.context = new HashMap<>((Map<String, TemplateModel>) contextObject);
            else
                this.context = new HashMap<>();

            // Allow custom configuration injection via map
            Object configObject = map.get("configuration");
            if (configObject instanceof AdapterConfiguration) {
                this.configuration = (AdapterConfiguration) configObject;
            } else {
                this.configuration = new SystemPropertyConfiguration();
            }
        } else {
            this.group = "";
            this.context = new HashMap<>();
            this.configuration = new SystemPropertyConfiguration();
        }
    }

    /**
     * Returns sub-schemas for each OpenAPI file found in the configured directory.
     * Each OpenAPI file becomes a separate schema with its own tables.
     *
     * @return Map of schema name -> Schema instance
     */
    @Override
    protected Map<String, org.apache.calcite.schema.Schema> getSubSchemaMap() {
        if (subSchemaMap != null) {
            return subSchemaMap;
        }
        subSchemaMap = new HashMap<>();
        String restDirectory = configuration.get("calcite.rest");
        if (restDirectory == null) {
            String catalinaBase = configuration.get("catalina.base");
            if (catalinaBase != null) {
                restDirectory = catalinaBase + File.separator + "calcite" + File.separator + "rest";
            }
        }

        final String finalRestDirectory = restDirectory;

        // For each OpenAPI specification in the directory (.yaml, .yml, .json)
        try (Stream<Path> stream = Files.walk(Paths.get(restDirectory))) {
            stream.filter(Files::isRegularFile)
                    .filter(p -> {
                        String name = p.getFileName().toString().toLowerCase();
                        return name.endsWith(".yaml") || name.endsWith(".yml") || name.endsWith(".json");
                    })
                    .forEach(openApiPath -> {
                        String fileName = openApiPath.getFileName().toString();

                        // Load all Freemarker request templates from the directory
                        Map<String, String> requestTemplates = loadTemplates(finalRestDirectory);

                        // Parse the OpenAPI specification - this now returns multiple services if schema names differ
                        List<ApiService> apiServices = OpenApiSchemaConverter.fromOpenApi(openApiPath.toString(), requestTemplates);

                        // Group services by schema name to avoid duplicate schema creation
                        Map<String, List<ApiService>> servicesBySchema = new HashMap<>();
                        for (ApiService apiService : apiServices) {
                            String schemaName = apiService.getSchemaName();
                            servicesBySchema.computeIfAbsent(schemaName, k -> new ArrayList<>()).add(apiService);
                        }

                        // Create a separate schema for each unique schema name
                        // RestApiSchema will automatically collect all tables from all services with the same schema name
                        for (Map.Entry<String, List<ApiService>> entry : servicesBySchema.entrySet()) {
                            String schemaName = entry.getKey();
                            List<ApiService> services = entry.getValue();

                            // Only create schema if it doesn't exist yet
                            if (!subSchemaMap.containsKey(schemaName)) {
                                // Create a sub-schema for this specific schema name
                                // It will automatically collect all tables from all services with this schema name
                                RestApiSchema apiSchema = new RestApiSchema(
                                        openApiPath.toString(),
                                        schemaName,
                                        finalRestDirectory,
                                        group,
                                        context
                                );

                                subSchemaMap.put(schemaName, apiSchema);

                                // Count total tables across all services with this schema name
                                int totalTables = services.stream().mapToInt(s -> s.getTables().size()).sum();
                                logger.info("Created REST API schema '{}' from file: {} with {} tables from {} service(s)",
                                    schemaName, fileName, totalTables, services.size());
                            } else {
                                logger.warn("Schema '{}' already exists (from a previous file). Skipping duplicate schema creation from file: {}",
                                    schemaName, fileName);
                            }
                        }
                    });
        } catch (IOException e) {
            logger.error("Error processing OpenAPI files: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        return subSchemaMap;
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

    /**
     * Registers all sub-schemas (one per OpenAPI file) directly into the provided root schema.
     * This allows SQL queries to use schema names directly: SELECT * FROM schemaName.tableName
     *
     * @param rootSchema The root schema to register sub-schemas into
     */
    public void registerSubSchemas(org.apache.calcite.schema.SchemaPlus rootSchema) {
        Map<String, org.apache.calcite.schema.Schema> subSchemas = getSubSchemaMap();
        for (Map.Entry<String, org.apache.calcite.schema.Schema> entry : subSchemas.entrySet()) {
            rootSchema.add(entry.getKey(), entry.getValue());
            logger.info("Registered sub-schema '{}' in root schema", entry.getKey());
        }
    }

    /**
     * Extracts schema name from OpenAPI specification.
     * First checks for x-calcite-restapi-schema-name extension in info section.
     * Falls back to file name without extension if not specified.
     *
     * @param openApiPath Path to OpenAPI file
     * @return Schema name to use
     */
    private String extractSchemaName(Path openApiPath) {
        try {
            // Parse OpenAPI to check for custom schema name
            io.swagger.v3.oas.models.OpenAPI openAPI = new io.swagger.v3.parser.OpenAPIV3Parser().read(openApiPath.toString());

            if (openAPI != null && openAPI.getInfo() != null) {
                // Check for x-calcite-restapi-schema-name extension
                Object schemaNameExt = openAPI.getInfo().getExtensions() != null
                    ? openAPI.getInfo().getExtensions().get("x-calcite-restapi-schema-name")
                    : null;

                if (schemaNameExt != null) {
                    return schemaNameExt.toString();
                }
            }
        } catch (Exception e) {
            logger.warn("Could not parse OpenAPI file for schema name extraction: {}", e.getMessage());
        }

        // Fallback: use file name without extension
        String fileName = openApiPath.getFileName().toString();
        return fileName.replaceFirst("\\.(yaml|yml|json)$", "");
    }

}
