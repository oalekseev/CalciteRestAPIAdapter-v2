package org.apache.calcite.adapter.restapi.rest;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.parameters.*;
import io.swagger.v3.oas.models.media.*;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.parser.OpenAPIV3Parser;

import org.apache.calcite.adapter.restapi.model.*;
import org.apache.calcite.adapter.restapi.rest.typemapper.*;

import java.util.*;

/**
 * Converts an OpenAPI schema into domain models for the REST SQL adapter.
 * <p>
 * Responsibilities:
 *  <ul>
 *    <li>Maps request and response parameters for each REST endpoint.</li>
 *    <li>Auto-detects the tabular root (JSONPath) for the response.</li>
 *    <li>Optionally loads Freemarker request templates for each table.</li>
 *    <li>Analyzes filter capabilities (DNF/CNF, operators) based on OpenAPI schema.</li>
 *  </ul>
 * <p>
 * Usage: Call {@code fromOpenApi()} with the path to an OpenAPI spec and an optional map of Freemarker templates.
 */
public class OpenApiSchemaConverter {

    public static List<ApiService> fromOpenApi(String openapiPath, Map<String, String> requestTemplates) {
        OpenAPI openAPI = new OpenAPIV3Parser().read(openapiPath);

        // Check for x-calcite-restapi-schema-name extension in info section (global schema name)
        String globalSchemaName = null;
        if (openAPI.getInfo() != null && openAPI.getInfo().getExtensions() != null) {
            Object schemaNameExt = openAPI.getInfo().getExtensions().get("x-calcite-restapi-schema-name");
            if (schemaNameExt != null) {
                globalSchemaName = schemaNameExt.toString();
            }
        }

        // Group operations by schema name
        Map<String, ApiService> serviceMap = new HashMap<>();

        for (Map.Entry<String, PathItem> pathEntry : openAPI.getPaths().entrySet()) {
            String endpoint = pathEntry.getKey();
            PathItem pathItem = pathEntry.getValue();

            // For each HTTP method on this path, construct ApiTable
            Map<PathItem.HttpMethod, Operation> operationMap = pathItem.readOperationsMap();
            for (Map.Entry<PathItem.HttpMethod, Operation> entry : operationMap.entrySet()) {
                PathItem.HttpMethod method = entry.getKey();
                Operation operation = entry.getValue();
                if (operation == null) continue;

                // Check for x-calcite-restapi-schema-name extension at operation level first
                String schemaName = null;
                if (operation.getExtensions() != null && operation.getExtensions().containsKey("x-calcite-restapi-schema-name")) {
                    schemaName = operation.getExtensions().get("x-calcite-restapi-schema-name").toString();
                }

                // If no operation-level schema name, check for global schema name
                if (schemaName == null) {
                    schemaName = globalSchemaName;
                }

                // If still no schema name, fall back to filename only (no title fallback)
                if (schemaName == null) {
                    // Generate a default schema name from the file name
                    String fileName = java.nio.file.Paths.get(openapiPath).getFileName().toString();
                    schemaName = fileName.replaceFirst("\\.(yaml|yml|json)$", "");
                }

                // Get or create the service for this schema name
                ApiService service = serviceMap.computeIfAbsent(schemaName, k -> {
                    ApiService newService = new ApiService();
                    newService.setSchemaName(k);
                    newService.setVersion(openAPI.getInfo().getVersion());
                    newService.setDescription(openAPI.getInfo().getDescription());
                    newService.setTables(new ArrayList<>());
                    return newService;
                });

                ApiTable table = new ApiTable();

                // Check for x-calcite-restapi-table-name extension
                String tableName = null;
                if (operation.getExtensions() != null && operation.getExtensions().containsKey("x-calcite-restapi-table-name")) {
                    tableName = operation.getExtensions().get("x-calcite-restapi-table-name").toString();
                }
                if (tableName == null) {
                    tableName = operation.getOperationId() != null ? operation.getOperationId() : endpoint;
                }
                table.setName(tableName);

                // --- RESPONSE schema flattening ---
                ApiResponse response = operation.getResponses().get("200");
                if (response == null || response.getContent() == null) continue;

                // Determine content type from response schema
                String contentType = response.getContent().keySet().iterator().next();
                MediaType mediaType = response.getContent().get(contentType);
                Schema<?> schema = mediaType.getSchema();

                // Resolve $ref if schema.type is null
                schema = resolveSchema(schema, openAPI);

                List<ApiParameter> parameters = new ArrayList<>();

                // Calculate the deepest array path to determine nesting depth
                String deepestArrayPath = findDeepestArrayPath(schema, openAPI);
                int nestingDepth = deepestArrayPath.split("\\.").length - 1; // Count dots in path ($.a.b.c has depth 3)

                // Extract field mappings from schema (x-calcite-restapi-mappings)
                // --- Flatten all fields including nested arrays ---
                // Field mappings will be extracted per-schema during traversal
                flattenSchemaFieldsWithArrayHandling(schema, new ArrayList<>(), ApiParamDirection.RESPONSE, parameters, openAPI, new ArrayList<>(), nestingDepth);

                // --- REQUEST parameters ---
                // Option 1: Standard OpenAPI parameters (typically for GET operations)
                if (operation.getParameters() != null) {
                    for (Parameter param : operation.getParameters()) {
                        // Check if this parameter already exists in response parameters
                        boolean exists = parameters.stream().anyMatch(p -> p.getName().equals(param.getName()));

                        if (exists) {
                            // Mark existing parameter as BOTH (request and response)
                            parameters.stream()
                                .filter(p -> p.getName().equals(param.getName()))
                                .forEach(p -> {
                                    p.setDirection(ApiParamDirection.BOTH);
                                });
                        } else {
                            // Add new REQUEST-only parameter
                            ApiParameter p = new ApiParameter();
                            p.setName(param.getName());
                            p.setDbType(mapSchemaType(param.getSchema() != null ? param.getSchema().getType() : "string",
                                    param.getSchema() != null ? param.getSchema().getFormat() : null));
                            p.setJsonpath(param.getName());
                            p.setDirection(ApiParamDirection.REQUEST);
                            parameters.add(p);
                        }
                    }
                }

                // Option 2: x-calcite-restapi-filterable-fields extension (for POST with JSON body)
                if (operation.getExtensions() != null && operation.getExtensions().containsKey("x-calcite-restapi-filterable-fields")) {
                    Object filterableFieldsObj = operation.getExtensions().get("x-calcite-restapi-filterable-fields");
                    if (filterableFieldsObj instanceof List) {
                        List<?> filterableFieldsList = (List<?>) filterableFieldsObj;
                        for (Object fieldObj : filterableFieldsList) {
                            if (fieldObj instanceof Map) {
                                Map<?, ?> fieldMap = (Map<?, ?>) fieldObj;
                                String fieldName = fieldMap.get("name") != null ? fieldMap.get("name").toString() : null;
                                String fieldType = fieldMap.get("type") != null ? fieldMap.get("type").toString() : "string";
                                String fieldFormat = fieldMap.get("format") != null ? fieldMap.get("format").toString() : null;

                                if (fieldName != null) {
                                    // Check if this parameter already exists in response parameters
                                    boolean exists = parameters.stream().anyMatch(p -> p.getName().equals(fieldName));

                                    if (exists) {
                                        // Mark existing parameter as BOTH (request and response)
                                        parameters.stream()
                                            .filter(p -> p.getName().equals(fieldName))
                                            .forEach(p -> {
                                                p.setDirection(ApiParamDirection.BOTH);
                                            });
                                    } else {
                                        // Add new REQUEST-only parameter
                                        ApiParameter p = new ApiParameter();
                                        p.setName(fieldName);
                                        p.setDbType(mapSchemaType(fieldType, fieldFormat));
                                        p.setJsonpath(fieldName);
                                        p.setDirection(ApiParamDirection.REQUEST);
                                        parameters.add(p);
                                    }
                                }
                            }
                        }
                    }
                }

                // Request body schema is NOT parsed for table columns
                // But we extract filterable field names from FilterCriterion schema
                Set<String> filterableRestFields = extractFilterableFields(operation, openAPI);
                if (!filterableRestFields.isEmpty()) {
                    // Convert REST field names to SQL column names using field mappings
                    // We need NON-inverted mappings (REST→SQL) for this
                    // IMPORTANT: Multiple SQL columns can map to the same REST field name (e.g., department_name, employee_name, task_name all map to "name")
                    // So we need to collect ALL SQL columns that map to each filterable REST field
                    Set<String> filterableSqlColumns = new HashSet<>();
                    Map<String, Object> invertedMappings = extractFieldMappingsFromSchema(schema, openAPI);

                    // Invert back: SQL→REST becomes REST→SQL
                    // Use a MultiMap to handle multiple SQL columns mapping to the same REST field
                    Map<String, Set<String>> restToSqlMappings = new HashMap<>();
                    for (Map.Entry<String, Object> mappingEntry : invertedMappings.entrySet()) {
                        String sqlColumnName = mappingEntry.getKey();
                        Object value = mappingEntry.getValue();
                        String restFieldName = null;
                        if (value instanceof String) {
                            restFieldName = (String) value;
                        } else if (value instanceof Map) {
                            Map<?, ?> complexMapping = (Map<?, ?>) value;
                            if (complexMapping.containsKey("restName")) {
                                restFieldName = complexMapping.get("restName").toString();
                            }
                        }
                        if (restFieldName != null) {
                            restToSqlMappings.computeIfAbsent(restFieldName, k -> new HashSet<>()).add(sqlColumnName);
                        }
                    }

                    // Convert filterable REST field names to ALL corresponding SQL column names
                    for (String restFieldName : filterableRestFields) {
                        Set<String> sqlColumnNames = restToSqlMappings.get(restFieldName);
                        if (sqlColumnNames != null) {
                            filterableSqlColumns.addAll(sqlColumnNames);
                        }
                    }

                    // Mark response parameters as BOTH if their SQL column name is filterable
                    for (ApiParameter param : parameters) {
                        if (param.getDirection() == ApiParamDirection.RESPONSE && filterableSqlColumns.contains(param.getName())) {
                            param.setDirection(ApiParamDirection.BOTH);
                        }
                    }
                }

                table.setParameters(parameters);

                table.setDeepestArrayPath(findDeepestArrayPath(schema, openAPI));

                String httpMethod = method.name().toLowerCase();

                service.getTables().add(table);

                // --- Build ApiRequestConfig as usual ---
                ApiRequestConfig requestConfig = new ApiRequestConfig();
                requestConfig.setAddresses(openAPI.getServers() != null && !openAPI.getServers().isEmpty()
                        ? openAPI.getServers().get(0).getUrl() : "http://localhost");
                requestConfig.setMethod(httpMethod.toUpperCase());
                requestConfig.setUrl(endpoint);
                requestConfig.setConnectionTimeout(10);
                requestConfig.setResponseTimeout(30);
                requestConfig.setDefaultContentType(contentType);

                // Check for x-calcite-restapi-pagination extension
                if (operation.getExtensions() != null && operation.getExtensions().containsKey("x-calcite-restapi-pagination")) {
                    Map<String, Object> pagination = (Map<String, Object>) operation.getExtensions().get("x-calcite-restapi-pagination");
                    if (pagination != null) {
                        if (pagination.containsKey("pageStart")) {
                            requestConfig.setPageStart(Integer.parseInt(pagination.get("pageStart").toString()));
                        }
                        if (pagination.containsKey("pageSize")) {
                            requestConfig.setPageSize(Integer.parseInt(pagination.get("pageSize").toString()));
                        }
                    }
                }

                // Check for x-calcite-restapi-request-body-template extension first
                String templateFile = null;
                if (operation.getExtensions() != null && operation.getExtensions().containsKey("x-calcite-restapi-request-body-template")) {
                    templateFile = operation.getExtensions().get("x-calcite-restapi-request-body-template").toString();
                }
                if (templateFile != null && requestTemplates != null && requestTemplates.containsKey(templateFile)) {
                    requestConfig.setBody(requestTemplates.get(templateFile));
                }

                // Check for x-calcite-restapi-request-headers-template extension
                String headersTemplateFile = null;
                if (operation.getExtensions() != null && operation.getExtensions().containsKey("x-calcite-restapi-request-headers-template")) {
                    headersTemplateFile = operation.getExtensions().get("x-calcite-restapi-request-headers-template").toString();
                }
                if (headersTemplateFile != null && requestTemplates != null && requestTemplates.containsKey(headersTemplateFile)) {
                    requestConfig.setHeaderTemplate(requestTemplates.get(headersTemplateFile));
                }

                // Check for x-calcite-restapi-request-url-template extension
                String urlTemplateFile = null;
                if (operation.getExtensions() != null && operation.getExtensions().containsKey("x-calcite-restapi-request-url-template")) {
                    urlTemplateFile = operation.getExtensions().get("x-calcite-restapi-request-url-template").toString();
                }
                if (urlTemplateFile != null && requestTemplates != null && requestTemplates.containsKey(urlTemplateFile)) {
                    requestConfig.setUrlTemplate(requestTemplates.get(urlTemplateFile));
                }

                // Extract filter field mappings from unified x-calcite-restapi-mappings
                // extractFieldMappingsFromSchema now returns ALREADY INVERTED mappings:
                // format: { "sql_column_name": "restFieldName", ... }
                // This is exactly what filterFieldMappings needs!
                Map<String, Object> invertedMappings = extractFieldMappingsFromSchema(schema, openAPI);
                if (!invertedMappings.isEmpty()) {
                    Map<String, String> filterMappings = new HashMap<>();
                    for (Map.Entry<String, Object> invertedEntry : invertedMappings.entrySet()) {
                        String sqlColumnName = invertedEntry.getKey();
                        Object value = invertedEntry.getValue();

                        if (value instanceof String) {
                            // Simple inverted mapping: sql_column → restField
                            filterMappings.put(sqlColumnName, (String) value);
                        } else if (value instanceof Map) {
                            // Complex inverted mapping: sql_column → {restName: "...", sqlType: "..."}
                            Map<?, ?> complexMapping = (Map<?, ?>) value;
                            if (complexMapping.containsKey("restName")) {
                                filterMappings.put(sqlColumnName, complexMapping.get("restName").toString());
                            }
                        }
                    }
                    requestConfig.setFilterFieldMappings(filterMappings);
                }

                // Assign requestConfig to the table (not to service!)
                // Each table has its own endpoint and configuration
                table.setRequestConfig(requestConfig);

                // Keep for backward compatibility (some tests may rely on service-level requestConfig)
                // but table-level requestConfig takes precedence
                service.setRequestConfig(requestConfig);
            }
        }

        return new ArrayList<>(serviceMap.values());
    }

    /**
     * Resolves a schema $ref to its actual definition
     */
    private static Schema<?> resolveSchema(Schema<?> schema, OpenAPI openAPI) {
        if (schema == null) return null;
        if (schema.get$ref() != null) {
            String ref = schema.get$ref();
            String schemaName = ref.substring(ref.lastIndexOf('/') + 1);
            if (openAPI.getComponents() != null && openAPI.getComponents().getSchemas() != null) {
                Schema<?> resolved = openAPI.getComponents().getSchemas().get(schemaName);
                return resolved != null ? resolved : schema;
            }
        }
        return schema;
    }

    /**
     * Extracts and inverts field mappings from x-calcite-restapi-mappings extension.
     * Recursively collects mappings from all nested schemas and INVERTS them immediately.
     *
     * Input format in YAML:
     * 1. Simple: { "restFieldName": "sql_column_name" }
     * 2. Extended: { "restFieldName": { "sqlName": "sql_column_name", "sqlType": "VARCHAR" } }
     *
     * Output (INVERTED for filter usage):
     * 1. Simple: { "sql_column_name": "restFieldName" }
     * 2. Extended: { "sql_column_name": { "restName": "restFieldName", "sqlType": "VARCHAR" } }
     *
     * This inversion prevents collisions since SQL column names are unique across nested schemas
     * (department_id, employee_id, task_id) while REST field names repeat (id, id, id).
     *
     * @param schema Response schema
     * @param openAPI OpenAPI spec
     * @return Map of SQL column names to REST field names (inverted mappings)
     */
    private static Map<String, Object> extractFieldMappingsFromSchema(Schema<?> schema, OpenAPI openAPI) {
        Map<String, Object> invertedMappings = new HashMap<>();
        extractFieldMappingsRecursively(schema, openAPI, invertedMappings);
        return invertedMappings;
    }

    /**
     * Recursively extracts field mappings from a schema and all nested schemas.
     * Since SQL column names are unique (department_id, employee_id, task_id) but REST field names
     * can repeat (id, name), we immediately invert mappings to avoid collisions.
     */
    private static void extractFieldMappingsRecursively(Schema<?> schema, OpenAPI openAPI, Map<String, Object> allMappings) {
        if (schema == null) return;
        schema = resolveSchema(schema, openAPI);

        // Extract mappings from current schema and IMMEDIATELY invert them
        // This avoids collisions since SQL column names are unique
        if (schema.getExtensions() != null && schema.getExtensions().containsKey("x-calcite-restapi-mappings")) {
            Object mappingsObj = schema.getExtensions().get("x-calcite-restapi-mappings");
            if (mappingsObj instanceof Map) {
                Map<String, Object> schemaMappings = (Map<String, Object>) mappingsObj;
                // Invert immediately: restFieldName → sqlColumnName becomes sqlColumnName → restFieldName
                for (Map.Entry<String, Object> entry : schemaMappings.entrySet()) {
                    String restFieldName = entry.getKey();
                    Object value = entry.getValue();

                    if (value instanceof String) {
                        String sqlColumnName = (String) value;
                        // Use SQL column name as key (unique!) and REST field name as value
                        allMappings.put(sqlColumnName, restFieldName);
                    } else if (value instanceof Map) {
                        Map<?, ?> complexMapping = (Map<?, ?>) value;
                        if (complexMapping.containsKey("sqlName")) {
                            String sqlColumnName = complexMapping.get("sqlName").toString();
                            // Store inverted mapping with full complex object but keyed by SQL name
                            Map<String, Object> invertedComplexMapping = new HashMap<>();
                            invertedComplexMapping.put("restName", restFieldName);
                            if (complexMapping.containsKey("sqlType")) {
                                invertedComplexMapping.put("sqlType", complexMapping.get("sqlType").toString());
                            }
                            allMappings.put(sqlColumnName, invertedComplexMapping);
                        }
                    }
                }
            }
        }

        // Navigate into nested structures
        if ("array".equals(schema.getType())) {
            Schema<?> itemSchema = resolveSchema(schema.getItems(), openAPI);
            extractFieldMappingsRecursively(itemSchema, openAPI, allMappings);
        } else if ("object".equals(schema.getType()) && schema.getProperties() != null) {
            // Recursively extract from all properties
            for (Schema<?> prop : schema.getProperties().values()) {
                extractFieldMappingsRecursively(prop, openAPI, allMappings);
            }
        }
    }

    /**
     * Extracts filterable field names from FilterCriterion schema in request body.
     * Looks at the enum values in the "name" field of FilterCriterion.
     * Returns REST API field names that can be used in filters.
     */
    private static Set<String> extractFilterableFields(Operation operation, OpenAPI openAPI) {
        Set<String> fields = new HashSet<>();
        if (operation.getRequestBody() == null || operation.getRequestBody().getContent() == null) {
            return fields;
        }

        for (MediaType mediaType : operation.getRequestBody().getContent().values()) {
            Schema<?> reqSchema = resolveSchema(mediaType.getSchema(), openAPI);
            if (reqSchema == null || reqSchema.getProperties() == null) continue;

            // Look for filter-related fields (or, and, where, filters, etc.)
            for (Map.Entry<String, Schema> prop : reqSchema.getProperties().entrySet()) {
                Schema<?> filterSchema = resolveSchema(prop.getValue(), openAPI);
                if ("array".equals(filterSchema.getType())) {
                    // Recursively check array items for FilterCriterion
                    Schema<?> itemSchema = resolveSchema(filterSchema.getItems(), openAPI);
                    extractFieldNamesFromFilterCriterion(itemSchema, openAPI, fields);
                }
            }
        }
        return fields;
    }

    /**
     * Recursively extract field names from FilterCriterion-like schemas
     */
    private static void extractFieldNamesFromFilterCriterion(Schema<?> schema, OpenAPI openAPI, Set<String> fields) {
        if (schema == null) return;
        schema = resolveSchema(schema, openAPI);

        if ("array".equals(schema.getType())) {
            Schema<?> itemSchema = resolveSchema(schema.getItems(), openAPI);
            extractFieldNamesFromFilterCriterion(itemSchema, openAPI, fields);
        } else if ("object".equals(schema.getType()) && schema.getProperties() != null) {
            // Look for a "name" or "field" property with enum values
            for (String propName : new String[]{"name", "field", "fieldName", "column"}) {
                Schema<?> fieldProp = (Schema<?>) schema.getProperties().get(propName);
                if (fieldProp != null) {
                    fieldProp = resolveSchema(fieldProp, openAPI);
                    if (fieldProp.getEnum() != null) {
                        for (Object enumValue : fieldProp.getEnum()) {
                            if (enumValue != null) {
                                fields.add(enumValue.toString());
                            }
                        }
                    }
                }
            }
        }
    }


    /**
     * Flattens schema fields handling nested arrays properly.
     * For nested arrays (departments -> employees -> tasks), creates columns like:
     * - department_id, department_name, department_location, department_budget
     * - employee_id, employee_name, employee_position, employee_salary
     * - task_id, task_title, task_status, task_priority
     *
     * @param schema Current schema node
     * @param parentPath Path of field names from root (includes array names)
     * @param dir Parameter direction
     * @param params Output list of parameters
     * @param openAPI OpenAPI spec for resolving $refs
     * @param arrayPrefixes List of array names encountered (used for column name prefixing)
     */
    private static void flattenSchemaFieldsWithArrayHandling(Schema<?> schema, List<String> parentPath,
                                                              ApiParamDirection dir, List<ApiParameter> params,
                                                              OpenAPI openAPI, List<String> arrayPrefixes, int nestingDepth) {
        if (schema == null) return;
        schema = resolveSchema(schema, openAPI);
        String type = schema.getType();

        if ("array".equals(type)) {
            Schema<?> itemSchema = schema instanceof ArraySchema ? ((ArraySchema)schema).getItems() : schema.getItems();
            itemSchema = resolveSchema(itemSchema, openAPI);

            // Track this array name for prefixing all fields inside it
            String arrayName = parentPath.isEmpty() ? "item" : parentPath.get(parentPath.size() - 1);
            List<String> newArrayPrefixes = new ArrayList<>(arrayPrefixes);
            newArrayPrefixes.add(arrayName);

            // Continue into array items, keeping the parentPath but passing new arrayPrefixes
            flattenSchemaFieldsWithArrayHandling(itemSchema, parentPath, dir, params, openAPI, newArrayPrefixes, nestingDepth);

        } else if ("object".equals(type) && schema.getProperties() != null) {
            // Extract field mappings from THIS schema (x-calcite-restapi-mappings)
            Map<String, Object> fieldMappings = new HashMap<>();
            if (schema.getExtensions() != null && schema.getExtensions().containsKey("x-calcite-restapi-mappings")) {
                Object mappingsObj = schema.getExtensions().get("x-calcite-restapi-mappings");
                if (mappingsObj instanceof Map) {
                    fieldMappings = (Map<String, Object>) mappingsObj;
                }
            }

            for (Map.Entry<String, Schema> prop : schema.getProperties().entrySet()) {
                List<String> childPath = new ArrayList<>(parentPath);
                childPath.add(prop.getKey());
                Schema<?> propSchema = resolveSchema(prop.getValue(), openAPI);

                // If this property is a leaf field (not object/array), extract mapping
                if (propSchema.getType() != null && !propSchema.getType().equals("object") && !propSchema.getType().equals("array")) {
                    // Leaf field - get REST field name
                    String restFieldName = prop.getKey();

                    // Try to find SQL column name and type from field mappings
                    String sqlColumnName = null;
                    String explicitSqlType = null;

                    if (fieldMappings.containsKey(restFieldName)) {
                        Object mapping = fieldMappings.get(restFieldName);
                        if (mapping instanceof String) {
                            sqlColumnName = (String) mapping;
                        } else if (mapping instanceof Map) {
                            Map<?, ?> complexMapping = (Map<?, ?>) mapping;
                            if (complexMapping.containsKey("sqlName")) {
                                sqlColumnName = complexMapping.get("sqlName").toString();
                            }
                            if (complexMapping.containsKey("sqlType")) {
                                explicitSqlType = complexMapping.get("sqlType").toString();
                            }
                        }
                    }

                    // Only add this field if SQL column name is defined
                    if (sqlColumnName != null && !sqlColumnName.isEmpty()) {
                        ApiParameter param = new ApiParameter();
                        param.setName(sqlColumnName);

                        // Use explicit SQL type if provided, otherwise auto-detect
                        if (explicitSqlType != null && !explicitSqlType.isEmpty()) {
                            param.setDbType(explicitSqlType.toLowerCase());
                        } else {
                            param.setDbType(mapSchemaType(propSchema.getType(), propSchema.getFormat()));
                        }

                        // JSONPath for nested arrays
                        String jsonPath;
                        if (!arrayPrefixes.isEmpty() && nestingDepth > 1) {
                            String arrayName = arrayPrefixes.get(arrayPrefixes.size() - 1);
                            jsonPath = arrayName + "." + restFieldName;
                        } else {
                            jsonPath = restFieldName;
                        }
                        param.setJsonpath(jsonPath);
                        param.setDirection(dir);
                        params.add(param);
                    }
                } else {
                    // Recurse into nested object/array
                    flattenSchemaFieldsWithArrayHandling(propSchema, childPath, dir, params, openAPI, arrayPrefixes, nestingDepth);
                }
            }
        }
    }

    // --- Helper: Find the path to the deepest (last) array in the schema ---
    private static String findDeepestArrayPath(Schema<?> schema, OpenAPI openAPI) {
        List<String> path = new ArrayList<>();
        deepestArrayPath(schema, new ArrayList<>(), path, openAPI);
        if (path.isEmpty()) return "$";
        return "$." + String.join(".", path);
    }
    private static void deepestArrayPath(Schema<?> schema, List<String> current, List<String> result, OpenAPI openAPI) {
        if (schema == null) return;
        if (openAPI != null) {
            schema = resolveSchema(schema, openAPI);  // Resolve $ref if any
        }
        String type = schema.getType();
        if ("array".equals(type)) {
            // Found an array - update result with current path (which should include the array field name)
            result.clear();
            result.addAll(current);
            Schema<?> item = schema instanceof ArraySchema ? ((ArraySchema)schema).getItems() : schema.getItems();
            // Continue recursively - items might contain more nested arrays
            deepestArrayPath(item, current, result, openAPI);
        } else if ("object".equals(type) && schema.getProperties() != null) {
            for (Map.Entry<String, Schema> field : schema.getProperties().entrySet()) {
                List<String> sub = new ArrayList<>(current);
                sub.add(field.getKey());
                deepestArrayPath(field.getValue(), sub, result, openAPI);
            }
        }
    }

    /**
     * Maps OpenAPI type and format to the correct dbType string for application/Calcite/Java mapping.
     *
     * JSON Type Mappings:
     * - integer           → int (default), long (format: int64)
     * - number            → double (default), float (format: float)
     * - boolean           → boolean
     * - string            → string (default), date, time, timestamp, uuid, byte (based on format)
     *
     * Supported SQL Types (when specified explicitly in x-calcite-restapi-mappings):
     * - INTEGER, BIGINT, SMALLINT, TINYINT
     * - DECIMAL, NUMERIC
     * - FLOAT, DOUBLE, REAL
     * - BOOLEAN, BIT
     * - VARCHAR, CHAR, TEXT
     * - DATE, TIME, TIMESTAMP
     * - UUID
     * - BINARY, VARBINARY, BLOB
     *
     * @param type   JSON schema type (e.g. "string", "integer", "number").
     * @param format Format string (e.g. "date-time", "uuid", "int64").
     * @return The standardized dbType string used in internal models.
     */
    /**
     * Shared type mapper chain for schema type conversion.
     * Initialized once and reused across all conversions.
     */
    private static final OpenApiTypeMapperChain TYPE_MAPPER_CHAIN = initializeTypeMapperChain();

    /**
     * Initializes the type mapper chain with all supported mappers.
     *
     * @return Configured type mapper chain
     */
    private static OpenApiTypeMapperChain initializeTypeMapperChain() {
        OpenApiTypeMapperChain chain = new OpenApiTypeMapperChain();
        chain.addMapper(new IntegerTypeMapper())
             .addMapper(new NumberTypeMapper())
             .addMapper(new BooleanTypeMapper())
             .addMapper(new StringTypeMapper())
             .addMapper(new DefaultTypeMapper()); // Fallback
        return chain;
    }

    public static String mapSchemaType(String type, String format) {
        if (type == null) {
            return "string";
        }
        return TYPE_MAPPER_CHAIN.mapType(type, format);
    }
}
