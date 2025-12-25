package org.apache.calcite.adapter.restapi.rest.csv;

import com.opencsv.CSVReader;
import net.minidev.json.JSONArray;
import org.apache.calcite.adapter.restapi.rest.interfaces.ArrayReader;

import java.io.StringReader;
import java.util.*;

/**
 * Reads CSV response using OpenCSV as a JSON-like array structure for Calcite REST integration.
 * Each CSV row is mapped as a Map<String, Object> where keys are column names.
 */
public class CsvArrayReader implements ArrayReader {
    private final String csvText;

    public CsvArrayReader(String csvText) {
        this.csvText = csvText;
    }

    @Override
    public JSONArray read(String path) {
        JSONArray result = new JSONArray();
        try (CSVReader csvReader = new CSVReader(new StringReader(csvText))) {
            String[] columns = csvReader.readNext(); // Read header row
            if (columns == null) return result;
            String[] rowValues;
            while ((rowValues = csvReader.readNext()) != null) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 0; i < columns.length; i++) {
                    row.put(columns[i].trim(), i < rowValues.length ? rowValues[i].trim() : null);
                }
                result.add(row);
            }
        } catch (Exception e) {
            // Logging or error handling here if needed
        }
        return result;
    }
}
