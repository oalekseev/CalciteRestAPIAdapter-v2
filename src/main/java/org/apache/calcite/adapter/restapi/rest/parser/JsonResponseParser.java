package org.apache.calcite.adapter.restapi.rest.parser;

import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.apache.calcite.adapter.restapi.rest.ResponseRowReader;
import org.apache.calcite.adapter.restapi.rest.json.JsonArrayParamReader;
import org.apache.calcite.adapter.restapi.rest.json.JsonArrayReader;

import java.util.*;

/**
 * Parses JSON format HTTP responses.
 *
 * <p>Handles content types:</p>
 * <ul>
 *   <li>application/json</li>
 *   <li>text/json</li>
 *   <li>Default fallback for unrecognized types</li>
 * </ul>
 *
 * <p><b>JSON format example:</b></p>
 * <pre>
 * {
 *   "data": [
 *     {"id": 1, "name": "Alice"},
 *     {"id": 2, "name": "Bob"}
 *   ]
 * }
 * </pre>
 *
 * <p><b>Nested arrays:</b> Supports flattening of nested JSON arrays
 * (e.g., $.departments[].employees[].tasks[]) into flat row structure.</p>
 */
public class JsonResponseParser implements ResponseParser {

    @Override
    public boolean canHandle(String contentType) {
        // JSON is the default format, handles any content type not matched by others
        return true;
    }

    @Override
    public List<ResponseRowReader> parse(String httpResponse, String arrayPath) throws ParseException {
        try {
            List<ResponseRowReader> result = new ArrayList<>();
            JsonArrayReader jsonReader = new JsonArrayReader(httpResponse);

            // Check if we have nested arrays by analyzing arrayPath
            String deepestArrayPath = arrayPath;
            String[] pathParts = deepestArrayPath.replace("$.", "").replace("$", "").split("\\.");

            // If we have multiple array levels (e.g., $.departments.employees.tasks)
            // we need to flatten them
            if (pathParts.length > 1 && !pathParts[0].isEmpty()) {
                result = flattenNestedJsonArrays(httpResponse, pathParts);
            } else {
                // Simple case: single array level
                JSONArray jsonArray = jsonReader.read(deepestArrayPath);
                if (jsonArray != null) {
                    // For paths like $.categories.items, we need to extract root-level fields
                    // that are not part of the path hierarchy but still need to be included
                    Map<String, Object> rootFields = extractRootFields(httpResponse, deepestArrayPath);

                    for (Object o : jsonArray) {
                        // Create a merged object that includes both the array item and root fields
                        Map<String, Object> mergedObject = mergeRootFieldsWithItem(o, rootFields);
                        result.add(new ResponseRowReader(new JsonArrayParamReader(mergedObject)));
                    }
                }
            }

            return result;
        } catch (Exception e) {
            throw new ParseException("Failed to parse JSON response", e);
        }
    }

    /**
     * Extracts root-level fields from the response, excluding the path to the deepest array
     */
    private Map<String, Object> extractRootFields(String httpResponse, String deepestArrayPath) {
        Map<String, Object> rootFields = new HashMap<>();
        try {
            // Parse the full response to get the root object
            Object fullResponse = JsonPath.parse(httpResponse).json();

            if (fullResponse instanceof Map) {
                Map<String, Object> responseMap = (Map<String, Object>) fullResponse;

                // Split the path to understand the hierarchy
                // For "categories.items", we want fields like response_id, response_timestamp, response_source
                // but not the categories array itself or its sub-arrays
                String[] pathParts = deepestArrayPath.replace("$.", "").split("\\.");

                // Add all fields from the root that are not part of the path hierarchy
                for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();

                    // Don't include the first element of path hierarchy as a root field
                    // since that's where our target array starts
                    if (!key.equals(pathParts[0]) && !(value instanceof java.util.List)) {
                        // Include non-array fields that are not part of the path to the deepest array
                        rootFields.put(key, value);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rootFields;
    }

    /**
     * Merges root-level fields with an item object
     */
    private Map<String, Object> mergeRootFieldsWithItem(Object item, Map<String, Object> rootFields) {
        Map<String, Object> merged = new HashMap<>();

        // Add root fields first
        merged.putAll(rootFields);

        // Then add item fields, potentially overriding root fields with the same name
        if (item instanceof Map) {
            merged.putAll((Map<String, Object>) item);
        } else {
            // If item is not a map, add it as a special field
            merged.put("item_value", item);
        }

        return merged;
    }

    @Override
    public int getPriority() {
        return 100; // Default/fallback parser - check last
    }

    @Override
    public String getName() {
        return "JsonResponseParser";
    }

    /**
     * Flattens nested JSON arrays into a list of ResponseRowReaders.
     * For structure like departments[].employees[].tasks[], creates one row per task
     * with all parent data merged in.
     *
     * @param httpResponse The JSON response string
     * @param pathParts Array path components (e.g., ["departments", "employees", "tasks"])
     * @return List of ResponseRowReaders, one per deepest array element
     */
    private List<ResponseRowReader> flattenNestedJsonArrays(String httpResponse, String[] pathParts) {
        List<ResponseRowReader> result = new ArrayList<>();
        try {
            // Parse the JSON response
            Object jsonObject = JsonPath.parse(httpResponse).json();

            // Extract root-level fields that should be included in every row
            Map<String, Object> rootFields = extractRootFields(httpResponse, String.join(".", pathParts));

            // Recursively flatten nested arrays, passing root fields to be included in each row
            flattenNestedJsonArraysRecursiveWithRootFields(jsonObject, pathParts, 0, new HashMap<>(), result, rootFields);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Recursive helper to flatten nested arrays with root fields.
     * Accumulates parent object data as it traverses down the nesting levels.
     *
     * @param currentObject Current JSON object/array
     * @param pathParts Remaining array path components
     * @param level Current nesting level
     * @param accumulatedData Data accumulated from parent levels
     * @param result Output list to add flattened rows
     * @param rootFields Root-level fields to include in every row
     */
    private void flattenNestedJsonArraysRecursiveWithRootFields(Object currentObject, String[] pathParts, int level,
                                                  Map<String, Object> accumulatedData, List<ResponseRowReader> result,
                                                  Map<String, Object> rootFields) {
        if (level >= pathParts.length) {
            // Reached deepest level - the currentObject is the final element
            // Merge root fields with accumulated data
            Map<String, Object> finalData = new HashMap<>(rootFields);
            finalData.putAll(accumulatedData);
            result.add(new ResponseRowReader(new JsonArrayParamReader(finalData)));
            return;
        }

        String currentArrayName = pathParts[level];

        if (currentObject instanceof List) {
            // Current level is an array - iterate through elements
            List<?> array = (List<?>) currentObject;
            for (Object element : array) {
                if (element instanceof Map) {
                    Map<String, Object> elementMap = (Map<String, Object>) element;

                    // Create nested structure preserving array names as intermediate keys
                    Map<String, Object> newAccumulatedData = new HashMap<>(accumulatedData);

                    // Create a map for this level's data
                    Map<String, Object> currentLevelData = new HashMap<>();
                    for (Map.Entry<String, Object> entry : elementMap.entrySet()) {
                        if (!(entry.getValue() instanceof List)) {
                            currentLevelData.put(entry.getKey(), entry.getValue());
                        }
                    }

                    // Store this level's data under the array name
                    newAccumulatedData.put(currentArrayName, currentLevelData);

                    // If there's a next level, recurse into it
                    if (level + 1 < pathParts.length) {
                        String nextArrayName = pathParts[level + 1];
                        Object nextArray = elementMap.get(nextArrayName);
                        if (nextArray != null) {
                            flattenNestedJsonArraysRecursiveWithRootFields(nextArray, pathParts, level + 1, newAccumulatedData, result, rootFields);
                        }
                    } else {
                        // This is the last array level - merge root fields and create the response row
                        Map<String, Object> finalData = new HashMap<>(rootFields);
                        finalData.putAll(newAccumulatedData);
                        result.add(new ResponseRowReader(new JsonArrayParamReader(finalData)));
                    }
                }
            }
        } else if (currentObject instanceof Map) {
            // Current object is a map - extract the array at this level
            Map<String, Object> map = (Map<String, Object>) currentObject;
            Object arrayAtThisLevel = map.get(currentArrayName);
            if (arrayAtThisLevel != null) {
                flattenNestedJsonArraysRecursiveWithRootFields(arrayAtThisLevel, pathParts, level, accumulatedData, result, rootFields);
            }
        }
    }

    /**
     * Original recursive method for backward compatibility (without root fields)
     */
    private void flattenNestedJsonArraysRecursive(Object currentObject, String[] pathParts, int level,
                                                  Map<String, Object> accumulatedData, List<ResponseRowReader> result) {
        // For backward compatibility, call the new method with empty root fields
        flattenNestedJsonArraysRecursiveWithRootFields(currentObject, pathParts, level, accumulatedData, result, new HashMap<>());
    }
}
