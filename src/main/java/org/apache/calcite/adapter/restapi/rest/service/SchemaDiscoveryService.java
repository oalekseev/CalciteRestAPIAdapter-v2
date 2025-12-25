package org.apache.calcite.adapter.restapi.rest.service;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.restapi.model.ApiParameter;
import org.apache.calcite.adapter.restapi.model.ApiTable;
import org.apache.calcite.adapter.restapi.rest.Field;
import org.apache.calcite.adapter.restapi.rest.RestFieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Discovers and builds SQL schema from OpenAPI table definitions.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Initialize field mappings from OpenAPI parameters</li>
 *   <li>Build Calcite RelDataType for table schema</li>
 *   <li>Extract selected projection fields from SQL queries</li>
 *   <li>Map fields by index for filter conversion</li>
 * </ul>
 *
 * <p><b>Field types:</b></p>
 * <ul>
 *   <li>Request parameters: Fields used in SQL WHERE clauses</li>
 *   <li>Response parameters: Fields returned in SQL SELECT results</li>
 * </ul>
 */
public class SchemaDiscoveryService {

    /**
     * Initializes field mappings from OpenAPI table definition.
     * Fields can be both request parameters (filters) and response parameters (projections).
     *
     * @param apiTable OpenAPI table definition
     * @param typeFactory Calcite type factory for schema building
     * @return Map of field name to Field definition
     */
    public Map<String, Field> initializeFieldsMap(ApiTable apiTable, RelDataTypeFactory typeFactory) {
        Map<String, Field> fieldsMap = new LinkedHashMap<>();

        if (apiTable.getParameters() != null) {
            for (ApiParameter apiParameter : apiTable.getParameters()) {
                if (apiParameter.getDirection() != null) {
                    Field field = null;

                    // Initialize response parameter field
                    if (apiParameter.getDirection().isResponseParam()) {
                        RestFieldType restFieldType = RestFieldType.of(apiParameter.getDbType());
                        field = new Field(
                                apiParameter.getName(),
                                restFieldType,
                                restFieldType.toType((JavaTypeFactory) typeFactory));
                        field.setJsonpath(apiParameter.getJsonpath());
                        field.setResponseParameter(true);
                    }

                    // Mark as request parameter field (can be both request and response)
                    if (apiParameter.getDirection().isRequestParam()) {
                        if (field == null) {
                            RestFieldType restFieldType = RestFieldType.of(apiParameter.getDbType());
                            field = new Field(
                                    apiParameter.getName(),
                                    restFieldType,
                                    restFieldType.toType((JavaTypeFactory) typeFactory));
                        }
                        field.setRequestParameter(true);
                    }

                    // Store direction in field for validation purposes
                    field.setDirection(apiParameter.getDirection());

                    fieldsMap.put(apiParameter.getName(), field);
                }
            }
        }

        return fieldsMap;
    }

    /**
     * Builds Calcite RelDataType from field mappings.
     *
     * @param fieldsMap Map of field definitions
     * @param typeFactory Calcite type factory
     * @return Struct type representing table schema
     */
    public RelDataType buildRelDataType(Map<String, Field> fieldsMap, RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(fieldsMap.values()
                .stream()
                .map(field -> new Pair<>(field.getName(), field.getRelDataType()))
                .collect(Collectors.toList()));
    }

    /**
     * Gets selected projection field names from SQL query.
     * Enables projection pushdown to REST API.
     *
     * @param fieldsMap Map of field definitions
     * @param typeFactory Calcite type factory
     * @param projects Array of projection column indices (null = all columns)
     * @return Set of projected column names
     */
    public Set<String> getSelectedProjectFields(
            Map<String, Field> fieldsMap,
            RelDataTypeFactory typeFactory,
            int[] projects) {

        RelDataType rowType = buildRelDataType(fieldsMap, typeFactory);

        if (projects != null) {
            return Arrays.stream(projects)
                    .mapToObj(rowType.getFieldList()::get)
                    .map(RelDataTypeField::getName)
                    .collect(Collectors.toUnmodifiableSet());
        } else {
            return rowType.getFieldList().stream()
                    .map(RelDataTypeField::getName)
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    /**
     * Gets field by index from field map.
     * Used for filter conversion when processing RexInputRef nodes.
     *
     * @param index Field index in table schema
     * @param fieldsMap Map of field definitions
     * @return Field at given index
     */
    public Field getFieldByIndex(int index, Map<String, Field> fieldsMap) {
        return new ArrayList<>(fieldsMap.values()).get(index);
    }
}
