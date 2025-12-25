package org.apache.calcite.adapter.restapi.rest.parser;

import net.minidev.json.JSONArray;
import org.apache.calcite.adapter.restapi.rest.ResponseRowReader;
import org.apache.calcite.adapter.restapi.rest.xml.XmlArrayParamReader;
import org.apache.calcite.adapter.restapi.rest.xml.XmlArrayReader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.*;

/**
 * Parses XML format HTTP responses.
 *
 * <p>Handles content types:</p>
 * <ul>
 *   <li>application/xml</li>
 *   <li>text/xml</li>
 * </ul>
 */
public class XmlResponseParser implements ResponseParser {

    @Override
    public boolean canHandle(String contentType) {
        return contentType != null && contentType.contains("xml");
    }

    @Override
    public List<ResponseRowReader> parse(String httpResponse, String arrayPath) throws ParseException {
        try {
            List<ResponseRowReader> result = new ArrayList<>();
            XmlArrayReader xmlReader = new XmlArrayReader(httpResponse);

            // Check if we have nested arrays by analyzing arrayPath
            String[] pathParts = arrayPath.replace("$.", "").replace("$", "").split("\\.");

            // If we have multiple array levels (e.g., departments.employees.tasks)
            // we need to flatten them
            if (pathParts.length > 1 && !pathParts[0].isEmpty()) {
                result = flattenNestedXmlArrays(httpResponse, pathParts);
            } else {
                // Simple case: single array level
                JSONArray arr = xmlReader.read(arrayPath);
                if (arr != null) {
                    // For XML, extract root-level fields that should be included in every row
                    Map<String, Object> rootFields = extractRootFieldsFromXml(httpResponse, arrayPath);

                    for (Object o : arr) {
                        // Create a merged object that includes both the array item and root fields
                        Map<String, Object> mergedObject = mergeRootFieldsWithItem(o, rootFields);
                        result.add(new ResponseRowReader(new XmlArrayParamReader(mergedObject)));
                    }
                }
            }

            return result;
        } catch (Exception e) {
            throw new ParseException("Failed to parse XML response", e);
        }
    }

    @Override
    public int getPriority() {
        return 20; // Check after CSV but before JSON
    }

    @Override
    public String getName() {
        return "XmlResponseParser";
    }

    /**
     * Flattens nested XML arrays into a list of ResponseRowReaders.
     *
     * @param httpResponse Raw XML response
     * @param pathParts Array path components (e.g., ["departments", "employees", "tasks"])
     * @return Flattened list of row readers
     */
    private List<ResponseRowReader> flattenNestedXmlArrays(String httpResponse, String[] pathParts) {
        List<ResponseRowReader> result = new ArrayList<>();
        try {
            // Use DOM parser to correctly handle repeating XML elements
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature(javax.xml.XMLConstants.FEATURE_SECURE_PROCESSING, true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(httpResponse.getBytes()));

            // Convert DOM to Map structure for recursive processing
            Element root = doc.getDocumentElement();
            Map<String, Object> rootMap = convertDomElementToMap(root);

            // Extract root-level fields that should be included in every row
            Map<String, Object> rootFields = extractRootFieldsFromXml(httpResponse, String.join(".", pathParts));

            // Recursively flatten nested arrays, passing root fields to be included in each row
            flattenNestedXmlArraysRecursiveWithRootFields(rootMap, pathParts, 0, new HashMap<>(), result, rootFields);
        } catch (Exception e) {
            // Silently continue, result will be empty
        }
        return result;
    }

    /**
     * Recursive helper to flatten nested XML arrays with root fields.
     * Accumulates parent object data as it traverses down the nesting levels.
     *
     * @param currentObject Current XML object/array
     * @param pathParts Remaining array path components
     * @param level Current nesting level
     * @param accumulatedData Data accumulated from parent levels
     * @param result Output list to add flattened rows
     * @param rootFields Root-level fields to include in every row
     */
    private void flattenNestedXmlArraysRecursiveWithRootFields(Object currentObject, String[] pathParts, int level,
                                                 Map<String, Object> accumulatedData, List<ResponseRowReader> result,
                                                 Map<String, Object> rootFields) {
        if (level >= pathParts.length) {
            // Reached deepest level - process the current object
            // If it's a list, iterate through its elements
            if (currentObject instanceof List) {
                List<?> list = (List<?>) currentObject;
                for (Object item : list) {
                    Map<String, Object> finalData = new HashMap<>(rootFields);
                    finalData.putAll(accumulatedData);
                    if (item instanceof Map) {
                        finalData.putAll((Map<String, Object>) item);
                    } else {
                        finalData.put("item_value", item);
                    }
                    result.add(new ResponseRowReader(new XmlArrayParamReader(finalData)));
                }
            } else if (currentObject instanceof Map) {
                // Process single map object
                Map<String, Object> finalData = new HashMap<>(rootFields);
                finalData.putAll(accumulatedData);
                finalData.putAll((Map<String, Object>) currentObject);
                result.add(new ResponseRowReader(new XmlArrayParamReader(finalData)));
            } else {
                // Process single non-map object
                Map<String, Object> finalData = new HashMap<>(rootFields);
                finalData.putAll(accumulatedData);
                finalData.put("value", currentObject);
                result.add(new ResponseRowReader(new XmlArrayParamReader(finalData)));
            }
            return;
        }

        String currentArrayName = pathParts[level];

        if (currentObject instanceof List) {
            // Current level is an array - iterate through elements
            List<?> array = (List<?>) currentObject;
            for (Object element : array) {
                if (element instanceof Map) {
                    Map<String, Object> elementMap = (Map<String, Object>) element;

                    // Create nested structure preserving parent data
                    // Match JsonResponseParser behavior: store each level's data under the array name
                    Map<String, Object> newAccumulatedData = new HashMap<>(accumulatedData);

                    // Create a map for this level's data
                    Map<String, Object> currentLevelData = new HashMap<>();
                    for (Map.Entry<String, Object> entry : elementMap.entrySet()) {
                        if (!(entry.getValue() instanceof List)) {
                            currentLevelData.put(entry.getKey(), entry.getValue());
                        }
                    }

                    // Store this level's data under the array name (similar to JsonResponseParser)
                    if (!currentLevelData.isEmpty()) {
                        newAccumulatedData.put(currentArrayName, currentLevelData);
                    }

                    // If there's a next level, recurse into it
                    if (level + 1 < pathParts.length) {
                        String nextArrayName = pathParts[level + 1];
                        Object nextArray = elementMap.get(nextArrayName);
                        if (nextArray != null) {
                            flattenNestedXmlArraysRecursiveWithRootFields(nextArray, pathParts, level + 1, newAccumulatedData, result, rootFields);
                        } else {
                            // For XML, check if the next array might be nested inside another field
                            // For example, if path is categories.items, but items might be nested in category.items
                            boolean foundNextArray = false;
                            for (Map.Entry<String, Object> nestedEntry : elementMap.entrySet()) {
                                Object nestedValue = nestedEntry.getValue();
                                if (nestedValue instanceof List) {
                                    for (Object nestedElement : (List<?>) nestedValue) {
                                        if (nestedElement instanceof Map && ((Map<?, ?>) nestedElement).containsKey(nextArrayName)) {
                                            Object foundArray = ((Map<?, Object>) nestedElement).get(nextArrayName);
                                            if (foundArray != null) {
                                                flattenNestedXmlArraysRecursiveWithRootFields(foundArray, pathParts, level + 1, newAccumulatedData, result, rootFields);
                                                foundNextArray = true;
                                                break;
                                            }
                                        }
                                    }
                                    if (foundNextArray) break;
                                }
                            }
                            
                            if (!foundNextArray) {
                                // If we can't find the next array, process this as the final level
                                Object targetArray = elementMap.get(currentArrayName);
                                if (targetArray != null) {
                                    flattenNestedXmlArraysRecursiveWithRootFields(targetArray, pathParts, level + 1, newAccumulatedData, result, rootFields);
                                }
                            }
                        }
                    } else {
                        // This is the last array level - find the target array and process its items
                        Object targetArray = elementMap.get(currentArrayName);

                        // For XML, if the targetArray is not found directly, check if it's nested
                        // Need to handle the case where there are multiple parent objects (categories) each with the target array (items)
                        boolean foundAnyTargetArrays = false;  // Track if we found and processed any nested target arrays
                        if (targetArray == null) {

                            // Look for the target array in nested fields
                            for (Map.Entry<String, Object> entry : elementMap.entrySet()) {
                                String fieldName = entry.getKey();
                                Object fieldValue = entry.getValue();

                                if (fieldValue instanceof List) {
                                    List<?> fieldList = (List<?>) fieldValue;

                                    // Process each element in the field that might contain the target array
                                    for (Object listElement : fieldList) {
                                        if (listElement instanceof Map) {
                                            Map<String, Object> listElementMap = (Map<String, Object>) listElement;

                                            if (listElementMap.containsKey(currentArrayName)) {
                                                Object nestedTargetArray = listElementMap.get(currentArrayName);

                                                // Create accumulated data that includes this specific element's data
                                                // Match JsonResponseParser behavior: store each level's data under the field name
                                                Map<String, Object> elementSpecificAccumulatedData = new HashMap<>(newAccumulatedData);
                                                Map<String, Object> nestedLevelData = new HashMap<>();
                                                for (Map.Entry<String, Object> elemEntry : listElementMap.entrySet()) {
                                                    String elemKey = elemEntry.getKey();
                                                    Object elemValue = elemEntry.getValue();
                                                    if (!elemKey.equals(currentArrayName) && !(elemValue instanceof List)) {
                                                        nestedLevelData.put(elemKey, elemValue);
                                                    }
                                                }
                                                // Store this level's data under the path array name (not XML element name)
                                                // For path $.categories.items at level=1, use pathParts[level-1]="categories"
                                                if (!nestedLevelData.isEmpty() && level > 0) {
                                                    String parentArrayName = pathParts[level - 1];
                                                    elementSpecificAccumulatedData.put(parentArrayName, nestedLevelData);
                                                }

                                                // Process this specific target array with its associated element data
                                                List<?> itemsToProcess = null;
                                                if (nestedTargetArray instanceof List) {
                                                    List<?> nestedTargetList = (List<?>) nestedTargetArray;

                                                    // If the list contains Maps and each map has a field that is itself a list of items,
                                                    // then we need to extract from that sub-field
                                                    if (!nestedTargetList.isEmpty() && nestedTargetList.get(0) instanceof Map) {
                                                        Map<String, Object> firstElement = (Map<String, Object>) nestedTargetList.get(0);

                                                        // Look for a field that contains the actual items (often named after the child elements)
                                                        for (Map.Entry<String, Object> itemEntry : firstElement.entrySet()) {
                                                            String itemFieldName = itemEntry.getKey();
                                                            Object itemFieldValue = itemEntry.getValue();

                                                            // If this field contains a list of items, use that
                                                            if (itemFieldValue instanceof List) {
                                                                List<?> potentialItems = (List<?>) itemFieldValue;
                                                                // Heuristic: if the first element in this potential list is a Map (not a simple value),
                                                                // it's likely the actual items array
                                                                if (!potentialItems.isEmpty() && potentialItems.get(0) instanceof Map) {
                                                                    itemsToProcess = potentialItems;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }

                                                    // If we didn't find a sub-field with items, process the original list
                                                    if (itemsToProcess == null) {
                                                        itemsToProcess = nestedTargetList;
                                                    }
                                                }

                                                if (itemsToProcess != null) {
                                                    // Process each item in this target array
                                                    for (Object item : itemsToProcess) {
                                                        Map<String, Object> finalData = new HashMap<>(rootFields);
                                                        finalData.putAll(elementSpecificAccumulatedData);
                                                        // Store item data under the target array name (similar to JsonResponseParser)
                                                        if (item instanceof Map) {
                                                            finalData.put(currentArrayName, item);
                                                        } else {
                                                            finalData.put("item_value", item);
                                                        }
                                                        result.add(new ResponseRowReader(new XmlArrayParamReader(finalData)));
                                                    }
                                                } else {
                                                    // Process as single item
                                                    Map<String, Object> finalData = new HashMap<>(rootFields);
                                                    finalData.putAll(elementSpecificAccumulatedData);
                                                    if (nestedTargetArray instanceof Map) {
                                                        finalData.putAll((Map<String, Object>) nestedTargetArray);
                                                    } else if (nestedTargetArray != null) {
                                                        finalData.put("value", nestedTargetArray);
                                                    }
                                                    result.add(new ResponseRowReader(new XmlArrayParamReader(finalData)));
                                                }

                                                foundAnyTargetArrays = true;
                                            }
                                        }
                                    }

                                    // Don't break here - continue to process all elements in this field that might have target arrays
                                    // (e.g., all categories in the category array might have items)
                                }
                            }
                        }

                        // Only process the default targetArray if we didn't find any nested target arrays
                        // This prevents duplicate processing when nested arrays were already handled
                        if (!foundAnyTargetArrays) {
                            // Handle the case where targetArray might be a wrapper element that contains the actual items
                            List<?> itemsToProcess = null;

                            if (targetArray instanceof List) {
                                List<?> targetList = (List<?>) targetArray;

                                // If the list contains Maps and each map has a field that is itself a list of items,
                                // then we need to extract from that sub-field (common in XML where container elements wrap content)
                                if (!targetList.isEmpty() && targetList.get(0) instanceof Map) {
                                    Map<String, Object> firstElement = (Map<String, Object>) targetList.get(0);

                                    // Look for a field that contains the actual items (often named after the child elements)
                                    for (Map.Entry<String, Object> entry : firstElement.entrySet()) {
                                        String fieldName = entry.getKey();
                                        Object fieldValue = entry.getValue();

                                        // If this field contains a list of items, use that
                                        if (fieldValue instanceof List) {
                                            List<?> potentialItems = (List<?>) fieldValue;
                                            // Heuristic: if the first element in this potential list is a Map (not a simple value),
                                            // it's likely the actual items array
                                            if (!potentialItems.isEmpty() && potentialItems.get(0) instanceof Map) {
                                                itemsToProcess = potentialItems;
                                                break;
                                            }
                                        }
                                    }
                                }

                                // If we didn't find a sub-field with items, process the original list
                                if (itemsToProcess == null) {
                                    itemsToProcess = targetList;
                                }
                            }


                            if (itemsToProcess != null) {
                                // Process each item in the target array
                                for (Object item : itemsToProcess) {
                                    Map<String, Object> finalData = new HashMap<>(rootFields);
                                    finalData.putAll(newAccumulatedData);
                                    // Store item data under the target array name (similar to JsonResponseParser)
                                    if (item instanceof Map) {
                                        finalData.put(currentArrayName, item);
                                    } else {
                                        finalData.put("item_value", item);
                                    }
                                    result.add(new ResponseRowReader(new XmlArrayParamReader(finalData)));
                                }
                            } else {
                                // Process as single item
                                Map<String, Object> finalData = new HashMap<>(rootFields);
                                finalData.putAll(newAccumulatedData);
                                if (targetArray instanceof Map) {
                                    finalData.putAll((Map<String, Object>) targetArray);
                                } else if (targetArray != null) {
                                    finalData.put("value", targetArray);
                                }
                                result.add(new ResponseRowReader(new XmlArrayParamReader(finalData)));
                            }
                        }
                    }
                }
            }
        } else if (currentObject instanceof Map) {
            // Current object is a map - extract the array at this level
            Map<String, Object> map = (Map<String, Object>) currentObject;
            Object arrayAtThisLevel = map.get(currentArrayName);
            if (arrayAtThisLevel != null) {
                flattenNestedXmlArraysRecursiveWithRootFields(arrayAtThisLevel, pathParts, level + 1, accumulatedData, result, rootFields);
            }
        }
    }

//    /**
//     * Original recursive method for backward compatibility (without root fields)
//     */
//    private void flattenNestedXmlArraysRecursive(Object currentObject, String[] pathParts, int level,
//                                                 Map<String, Object> accumulatedData, List<ResponseRowReader> result) {
//        // For backward compatibility, call the new method with empty root fields
//        flattenNestedXmlArraysRecursiveWithRootFields(currentObject, pathParts, level, accumulatedData, result, new HashMap<>());
//    }

    /**
     * Convert DOM Element to Map structure, handling repeating elements as arrays.
     * Container elements (elements that contain multiple child elements of the same name)
     * are treated as arrays of those child elements.
     */
    private Map<String, Object> convertDomElementToMap(Element element) {
        Map<String, Object> map = new LinkedHashMap<>();
        NodeList children = element.getChildNodes();

        // Group children by tag name to detect arrays
        Map<String, List<Node>> childGroups = new LinkedHashMap<>();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                String tagName = child.getNodeName();
                childGroups.computeIfAbsent(tagName, k -> new ArrayList<>()).add(child);
            }
        }

        // Convert each group
        for (Map.Entry<String, List<Node>> entry : childGroups.entrySet()) {
            String tagName = entry.getKey();
            List<Node> nodes = entry.getValue();

            if (nodes.size() == 1) {
                // Single element of this type
                Node node = nodes.get(0);
                if (isSimpleDomElement((Element) node)) {
                    // Simple value (text only) - store as scalar
                    map.put(tagName, getDomElementTextValue((Element) node));
                } else {
                    // Complex element (has nested elements) - ALWAYS treat as array
                    // This ensures consistent structure for nested arrays navigation
                    // (e.g., employees[].tasks[] works even if employee has only 1 task)
                    List<Object> array = new ArrayList<>();
                    array.add(convertDomElementToMap((Element) node));
                    map.put(tagName, array);
                }
            } else {
                // Multiple elements with same name - create array
                List<Object> array = new ArrayList<>();
                for (Node node : nodes) {
                    if (isSimpleDomElement((Element) node)) {
                        array.add(getDomElementTextValue((Element) node));
                    } else {
                        array.add(convertDomElementToMap((Element) node));
                    }
                }
                map.put(tagName, array);
            }
        }

        return map;
    }

    private boolean isSimpleDomElement(Element element) {
        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
                return false;
            }
        }
        return true;
    }

    private Object getDomElementTextValue(Element element) {
        String text = element.getTextContent().trim();
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e1) {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException e2) {
                try {
                    return Double.parseDouble(text);
                } catch (NumberFormatException e3) {
                    return text;
                }
            }
        }
    }

    /**
     * Extracts root-level fields from the XML response, excluding the path to the deepest array.
     */
    private Map<String, Object> extractRootFieldsFromXml(String httpResponse, String deepestArrayPath) {
        Map<String, Object> rootFields = new HashMap<>();
        try {
            // Use DOM parser to handle the XML
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature(javax.xml.XMLConstants.FEATURE_SECURE_PROCESSING, true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(httpResponse.getBytes()));

            // Get the root element
            Element root = doc.getDocumentElement();

            // Convert the root element to a map structure
            Map<String, Object> rootMap = convertDomElementToMap(root);

            // Parse the path to understand the hierarchy
            String[] pathParts = deepestArrayPath.replace("$.", "").split("\\.");

            // Add all fields from the root that are not part of the path hierarchy
            for (Map.Entry<String, Object> entry : rootMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // Don't include the first element of path hierarchy as a root field
                // since that's where our target array starts
                if (!key.equals(pathParts[0]) && !(value instanceof List)) {
                    // Include non-array fields that are not part of the path to the deepest array
                    rootFields.put(key, value);
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
}