package org.apache.calcite.adapter.restapi.rest.service;

import freemarker.template.*;
import org.apache.calcite.adapter.restapi.freemarker.CalendarDate;
import org.apache.calcite.adapter.restapi.model.ApiParamDirection;
import org.apache.calcite.adapter.restapi.model.ApiRequestConfig;
import org.apache.calcite.adapter.restapi.rest.Field;
import org.apache.calcite.adapter.restapi.rest.exception.ConvertFiltersException;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Converts Calcite RexNode filter expressions to FreeMarker TemplateModel format.
 *
 * <p>Handles conversion of SQL filter predicates into REST API filter structures
 * suitable for template expansion in HTTP request bodies.</p>
 *
 * <p><b>Example conversion:</b></p>
 * <pre>
 * SQL: WHERE name = 'Alice' AND age >= 25
 * →
 * TemplateModel: {
 *   "name": "name",
 *   "operator": "=",
 *   "value": "Alice"
 * }
 * </pre>
 */
public class FilterConverter {

    /**
     * Converts a RexNode filter expression to a TemplateModel map.
     *
     * @param node RexNode filter expression (e.g., name = 'Alice')
     * @param fieldsMap Map of field definitions for schema lookup
     * @param connectionData Connection config with field mappings
     * @param commonContext Shared FreeMarker context to update with request values
     * @return Map containing filter structure (name, operator, value) or null if not convertible
     * @throws ConvertFiltersException If conversion fails
     */
    public Map<String, TemplateModel> convertToMap(
            RexNode node,
            Map<String, Field> fieldsMap,
            ApiRequestConfig connectionData,
            Map<String, TemplateModel> commonContext) throws ConvertFiltersException {

        if (!(node instanceof RexCall)) {
            return null;
        }

        RexCall call = (RexCall) node;
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();

        if (operands.size() != 2) {
            return null;
        }

        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        if (!(left instanceof RexInputRef)) {
            return null;
        }

        Field field = getField(((RexInputRef) left).getIndex(), fieldsMap);
        if (!field.isRequestParameter()) {
            return null;
        }

        // Validate: REQUEST-only parameters (not in response) can only use '=' operator
        // For other operators (>, <, !=, LIKE, etc.) we need actual values from the response
        // Throw AssertionError (extends Error, not Exception) to ensure it's not caught by Calcite's exception handlers
        if (field.getDirection() == ApiParamDirection.REQUEST && !"=".equals(operator.getName())) {
            throw new AssertionError(
                String.format(
                    "Cannot execute query: Field '%s' is a REQUEST-only parameter (not returned in REST API response). " +
                    "Only '=' operator is supported for REQUEST-only fields. " +
                    "Current query uses '%s' operator which requires actual field values from response for each row. " +
                    "Possible solutions:\n" +
                    "  1) Change query to use '=' operator: WHERE %s = <value>\n" +
                    "  2) Mark field '%s' as RESPONSE or BOTH direction in OpenAPI specification",
                    field.getName(),
                    operator.getName(),
                    field.getName(),
                    field.getName()
                )
            );
        }

        if (!(right instanceof RexLiteral)) {
            return null;
        }

        RexLiteral literal = (RexLiteral) right;
        try {
            TemplateModel value = convertLiteralToTemplateModel(literal);
            setRequestValueField(field, value, commonContext);

            // Map SQL column name to REST API parameter name using filterFieldMappings
            String fieldName = field.getName();
            if (connectionData != null && connectionData.getFilterFieldMappings() != null) {
                String mappedName = connectionData.getFilterFieldMappings().get(fieldName);
                if (mappedName != null) {
                    fieldName = mappedName;
                }
            }

            return Map.of(
                    "name", new SimpleScalar(fieldName),
                    "operator", new SimpleScalar(operator.getName()),
                    "value", value
            );
        } catch (TemplateModelException e) {
            throw ConvertFiltersException.buildConvertFiltersException(e);
        }
    }

    /**
     * Converts a RexLiteral value to the proper TemplateModel for FreeMarker context.
     *
     * @param rexLiteral Literal value from filter expression
     * @return FreeMarker value wrapper for template expansion
     * @throws TemplateModelException If conversion fails
     */
    public static TemplateModel convertLiteralToTemplateModel(RexLiteral rexLiteral) throws TemplateModelException {
        switch (rexLiteral.getTypeName()) {
            case BOOLEAN:
                return Boolean.TRUE.equals(rexLiteral.getValueAs(Boolean.class))
                        ? TemplateBooleanModel.TRUE
                        : TemplateBooleanModel.FALSE;
            case CHAR:
            case VARCHAR:
                return new SimpleScalar(rexLiteral.getValueAs(String.class));
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return new SimpleNumber(rexLiteral.getValueAs(Number.class));
            case DATE:
                return new CalendarDate(rexLiteral.getValueAs(Calendar.class), TemplateDateModel.DATE);
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return new CalendarDate(rexLiteral.getValueAs(Calendar.class), TemplateDateModel.TIME);
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new CalendarDate(rexLiteral.getValueAs(Calendar.class), TemplateDateModel.DATETIME);
            default:
                throw new IllegalStateException("Unexpected type: " + rexLiteral.getTypeName());
        }
    }

    /**
     * Sets request value in field and updates FreeMarker context.
     *
     * @param field Field to update
     * @param value Template model value
     * @param commonContext Shared FreeMarker context
     */
    private void setRequestValueField(Field field, TemplateModel value, Map<String, TemplateModel> commonContext) {
        if (field == null || !field.isRequestParameter()) {
            return;
        }
        field.setRequestValue(value);
        commonContext.put(field.getName(), value);
    }

    /**
     * Gets field by index from field map.
     *
     * @param index Field index
     * @param fieldsMap Map of field definitions
     * @return Field at given index
     */
    private Field getField(int index, Map<String, Field> fieldsMap) {
        return new java.util.ArrayList<>(fieldsMap.values()).get(index);
    }
}
