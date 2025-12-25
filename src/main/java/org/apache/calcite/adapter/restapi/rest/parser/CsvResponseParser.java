package org.apache.calcite.adapter.restapi.rest.parser;

import net.minidev.json.JSONArray;
import org.apache.calcite.adapter.restapi.rest.ResponseRowReader;
import org.apache.calcite.adapter.restapi.rest.csv.CsvArrayParamReader;
import org.apache.calcite.adapter.restapi.rest.csv.CsvArrayReader;
import org.apache.calcite.adapter.restapi.rest.interfaces.ArrayParamReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses CSV format HTTP responses.
 *
 * <p>Handles content types:</p>
 * <ul>
 *   <li>text/csv</li>
 *   <li>text/plain (when CSV data is detected)</li>
 * </ul>
 *
 * <p><b>CSV format:</b></p>
 * <pre>
 * id,name,age
 * 1,Alice,30
 * 2,Bob,25
 * </pre>
 *
 * <p><b>Note:</b> CSV parsing assumes first row contains column headers.</p>
 */
public class CsvResponseParser implements ResponseParser {

    @Override
    public boolean canHandle(String contentType) {
        return contentType != null && contentType.contains("csv");
    }

    @Override
    public List<ResponseRowReader> parse(String httpResponse, String arrayPath) throws ParseException {
        try {
            List<ResponseRowReader> result = new ArrayList<>();

            CsvArrayReader csvReader = new CsvArrayReader(httpResponse);
            JSONArray dataArray = csvReader.read("");

            if (dataArray == null) {
                return result;
            }

            List<Map<String, Object>> csvRows = new ArrayList<>();
            for (Object o : dataArray) {
                csvRows.add((Map<String, Object>) o);
            }

            ArrayParamReader paramReader = new CsvArrayParamReader(csvRows);
            for (int i = 0; i < csvRows.size(); i++) {
                result.add(new ResponseRowReader(paramReader));
            }

            return result;
        } catch (Exception e) {
            throw new ParseException("Failed to parse CSV response", e);
        }
    }

    @Override
    public int getPriority() {
        return 10; // Check CSV before JSON (specific format)
    }

    @Override
    public String getName() {
        return "CsvResponseParser";
    }
}
