package org.apache.calcite.adapter.restapi.rest.csv;

import org.apache.calcite.adapter.restapi.rest.interfaces.ArrayParamReader;

import java.util.List;
import java.util.Map;

/**
 * Reads field value from a CSV row represented as Map<String, Object>.
 * Used for field-by-field access in table row mapping.
 */
public class CsvArrayParamReader implements ArrayParamReader {
    private final List<Map<String, Object>> rows;

    public CsvArrayParamReader(List<Map<String, Object>> rows) {
        this.rows = rows;
    }

    @Override
    public Object read(int index, String path) {
        if (index < 0 || index >= rows.size()) return null;
        Map<String, Object> row = rows.get(index);
        // path is the column name
        return row.get(path);
    }
}
