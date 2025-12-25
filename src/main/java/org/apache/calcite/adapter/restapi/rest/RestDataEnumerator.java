package org.apache.calcite.adapter.restapi.rest;

import org.apache.calcite.adapter.restapi.rest.interfaces.RestIterator;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.calcite.adapter.restapi.rest.RestFieldType.STRING;

/**
 * Enumerator implementation for scanning and iterating server-side REST API data as SQL table rows.
 * <p>
 * Supports paging, dynamic typing, and conversion between REST data and Calcite field values
 * (including integration with Freemarker and custom mapping).
 * </p>
 *
 * - Reads paginated sets of records (ResponseRowReader) and projects them as rows.
 * - Handles conversion from JSON, Freemarker TemplateModel, and REST primitives to Calcite SQL types.
 * - Supports selection of only specific projected fields (for SQL SELECT ...).
 */
public class RestDataEnumerator implements Enumerator<Object[]> {

    private static final Logger logger = LoggerFactory.getLogger(RestDataEnumerator.class);

    /** Current page of rows */
    private List<ResponseRowReader> rows;
    /** Current index in the rows list */
    private int index = -1;
    /** Current row being processed */
    private ResponseRowReader current;
    /** True if enumerator is closed */
    private boolean isClosed = false;
    /** Map: field name -> Field meta (includes type info for conversion) */
    private final Map<String, Field> fieldsMap;

    /** Callback for retrieving next page/rowset from the REST source */
    RestIterator restIterator;

    /**
     * Constructs a new enumerator for REST SQL table data.
     *
     * @param results    Initial set of ResponseRowReader (records).
     * @param fieldsMap  All field definitions (name/type) for row projection.
     * @param projects   Array of field indices to include in projection; if null, includes all.
     * @param restIterator Callback for paging: retrieves more rows when available.
     */
    public RestDataEnumerator(List<ResponseRowReader> results, Map<String, Field> fieldsMap, int[] projects, RestIterator restIterator) {
        this.rows = results;
        this.restIterator = restIterator;
        this.fieldsMap = getProjectFields(projects, fieldsMap);
    }

    /**
     * Returns the next row from the current page.
     */
    private ResponseRowReader getNextRow() {
        return rows.get(++index);
    }

    /**
     * Retrieves the current row, converting each field to proper SQL type as an Object.
     * Fields can be set from REST response (response parameters) or from request context/parameter (request parameters).
     * @return Object array representing SQL row values for current record.
     */
    @Override
    public Object[] current() {
        final Object[] objects = new Object[fieldsMap.size()];
        int i = 0;

        for (Field field : fieldsMap.values()) {
            if (field.isResponseParameter()) {
                Object value = current.getArrayParamReader().read(index, field.getJsonpath());
                objects[i++] = convert(field.getRestFieldType(), value);
            } else if (field.isRequestParameter()) { // Since this is not a response parameter, a specific value is not received, so we take what was requested
                objects[i++] = convert(field.getRestFieldType(), field.getRequestValue());
            }
        }

        return objects;
    }

    /**
     * Advances enumeration to the next row, fetching additional data from the REST server if needed (uses RestIterator).
     * @return true if more data available, false if finished.
     */
    @Override
    public boolean moveNext() {
        if ((index + 1) == rows.size()) {
            rows = restIterator.getMore();
            if (rows.isEmpty()) {
                return false;
            }
            index = -1;
        }
        current = getNextRow();
        return true;
    }

    /**
     * Resets enumeration (does not fetch new data).
     */
    @Override
    public void reset() {
        current = null;
    }

    /**
     * Closes the enumerator and releases resources.
     */
    @Override
    public void close() {
        isClosed = true;
    }

    /**
     * Converts value from REST/TemplateModel/primitive to appropriate target SQL type.
     *
     * @param fieldType SQL field type to convert to
     * @param object  Original object value from REST/TemplateModel
     * @return Converted value for returned SQL row, or null if conversion failed.
     */
    protected Object convert(RestFieldType fieldType, Object object) {
        if (object == null) {
            return null;
        }
        if (fieldType == null) {
            return object.toString();
        }
        if (!fieldType.equals(STRING) && object.toString().isEmpty()) {
            return null;
        }

        return ValueConverter.convert(object, fieldType);
    }

    /**
     * Builds projection field set. If projection indices are specified, returns only selected fields. Otherwise returns all.
     *
     * @param projects  Array of indices for fields to include in projection.
     * @param fieldsMap Map of all available fields.
     * @return Map containing only fields specified in projection.
     */
    private Map<String, Field> getProjectFields(int[] projects, Map<String, Field> fieldsMap) {
        if (projects == null) return fieldsMap;

        List<Field> arrayFields = new ArrayList<>(fieldsMap.values());
        Map<String, Field> projectFieldsMap = new LinkedHashMap<>(arrayFields.size());
        for (int index : projects) {
            Field field = arrayFields.get(index);
            projectFieldsMap.put(field.getName(), field);
        }
        return projectFieldsMap;
    }
}