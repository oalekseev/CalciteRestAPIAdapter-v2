package org.apache.calcite.adapter.restapi.rest.service;

import freemarker.template.SimpleNumber;
import freemarker.template.SimpleScalar;
import freemarker.template.TemplateModel;
import org.apache.calcite.adapter.restapi.freemarker.FreeMarkerEngine;
import org.apache.calcite.adapter.restapi.freemarker.exception.ConvertException;
import org.apache.calcite.adapter.restapi.model.ApiRequestConfig;
import org.apache.calcite.adapter.restapi.rest.Field;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.rex.RexNode;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Builds FreeMarker template context for REST API request rendering.
 *
 * <p>Constructs context variables including:</p>
 * <ul>
 *   <li>Pagination: offset, limit, pageStart</li>
 *   <li>Table metadata: name</li>
 *   <li>Connection properties: user-defined variables</li>
 *   <li>SQL projections: selected fields</li>
 *   <li>SQL filters: DNF and CNF formats</li>
 * </ul>
 *
 * <p><b>Example context:</b></p>
 * <pre>{@code
 * {
 *   "offset": 0,
 *   "limit": 100,
 *   "projects": ["id", "name", "age"],
 *   "filters_dnf": [[{"name": "age", "operator": ">=", "value": 25}]]
 * }
 * }</pre>
 */
public class TemplateContextBuilder {

    private final FilterConverter filterConverter;

    public TemplateContextBuilder() {
        this.filterConverter = new FilterConverter();
    }

    /**
     * Builds complete FreeMarker template context for request rendering.
     *
     * @param baseContext Base context to populate
     * @param connectionData API connection configuration
     * @param fieldsMap Field definitions for schema
     * @param tableName Table name for context
     * @param dnfFilters DNF filter groups
     * @param cnfFilters CNF filter groups
     * @param offset Pagination offset
     * @param properties Connection properties
     * @param selectedProjectFields SQL projection fields
     * @throws ConvertException If context building fails
     */
    public void fillContext(
            Map<String, TemplateModel> baseContext,
            ApiRequestConfig connectionData,
            Map<String, Field> fieldsMap,
            String tableName,
            List<List<RexNode>> dnfFilters,
            List<List<RexNode>> cnfFilters,
            int offset,
            Properties properties,
            Set<String> selectedProjectFields) throws ConvertException {

        // Add pagination and metadata
        baseContext.put("offset", new SimpleNumber(offset));
        baseContext.put("limit", new SimpleNumber(connectionData.getPageSize()));
        baseContext.put("pageStart", new SimpleNumber(connectionData.getPageStart()));
        baseContext.put("name", new SimpleScalar(tableName));

        // Add user properties
        addPropertiesToContext(baseContext, properties);

        // Add selected fields if present
        if (!selectedProjectFields.isEmpty()) {
            addSelectedFieldsToContext(baseContext, connectionData, selectedProjectFields);
        }

        // Add both DNF and CNF filters to template context
        // User templates can choose which format to use based on their REST API requirements
        if (!dnfFilters.isEmpty()) {
            addFiltersToContext(baseContext, connectionData, fieldsMap, "dnf", dnfFilters);
        }
        if (!cnfFilters.isEmpty()) {
            addFiltersToContext(baseContext, connectionData, fieldsMap, "cnf", cnfFilters);
        }
    }

    /**
     * Adds connection/environment properties to FreeMarker context.
     * Excludes Calcite-internal configuration keys.
     *
     * @param context Context to populate
     * @param properties Full property map to expose to context
     */
    private void addPropertiesToContext(Map<String, TemplateModel> context, Properties properties) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();

            // Skip Calcite internal properties
            if (!key.equals(CalciteConnectionProperty.MODEL.camelName()) &&
                    !key.equals(CalciteConnectionProperty.FUN.camelName()) &&
                    !key.equals(CalciteConnectionProperty.CASE_SENSITIVE.camelName()) &&
                    !key.equals(CalciteConnectionProperty.QUOTED_CASING.camelName()) &&
                    !key.equals(CalciteConnectionProperty.UNQUOTED_CASING.camelName())) {
                context.put(key, new SimpleScalar(entry.getValue().toString()));
            }
        }
    }

    /**
     * Adds SQL projection fields (selected columns) to context for optimal REST pushdown.
     * Maps SQL column names to REST field names using filterFieldMappings.
     *
     * @param context Context to populate
     * @param connectionData Connection config with field mappings
     * @param selectedProjectFields Set of projected SQL column names
     * @throws ConvertException If conversion fails
     */
    private void addSelectedFieldsToContext(
            Map<String, TemplateModel> context,
            ApiRequestConfig connectionData,
            Set<String> selectedProjectFields) throws ConvertException {

        // Map SQL column names to REST field names
        List<String> restFieldNames = new ArrayList<>();
        for (String sqlColumnName : selectedProjectFields) {
            String restFieldName = sqlColumnName;

            // Use filterFieldMappings to translate SQL column name to REST field name
            if (connectionData != null && connectionData.getFilterFieldMappings() != null) {
                String mappedName = connectionData.getFilterFieldMappings().get(sqlColumnName);
                if (mappedName != null) {
                    restFieldName = mappedName;
                }
            }

            restFieldNames.add(restFieldName);
        }

        // Convert list to FreeMarker template model
        context.put("projects", FreeMarkerEngine.convert(restFieldNames));
    }

    /**
     * Converts filter structures to FreeMarker template models and adds them to context.
     * Both DNF and CNF filters are added to context regardless of API capabilities.
     * The choice of which format to use is made in the FreeMarker template.
     *
     * @param context Context to populate
     * @param connectionData Connection config with field mappings
     * @param fieldsMap Field definitions for schema
     * @param form "dnf" or "cnf"
     * @param filters Filter groups in RexNode form
     * @throws ConvertException If conversion fails
     */
    private void addFiltersToContext(
            Map<String, TemplateModel> context,
            ApiRequestConfig connectionData,
            Map<String, Field> fieldsMap,
            String form,
            List<List<RexNode>> filters) throws ConvertException {

        List<List<Map<String, TemplateModel>>> filterModel = filters.stream().map(group ->
                        group.stream()
                                .map(node -> {
                                    try {
                                        return filterConverter.convertToMap(node, fieldsMap, connectionData, context);
                                    } catch (Exception e) {
                                        return null;
                                    }
                                })
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()))
                .filter(group -> !group.isEmpty())
                .collect(Collectors.toList());

        context.put("filters_" + form, FreeMarkerEngine.convert(filterModel));
    }
}
