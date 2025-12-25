package org.apache.calcite.adapter.restapi.rest.xml;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import net.minidev.json.JSONArray;
import org.apache.calcite.adapter.restapi.rest.interfaces.ArrayReader;

import java.util.*;

public class XmlArrayReader implements ArrayReader {
    private final String xml;

    public XmlArrayReader(String xml) {
        this.xml = xml;
    }

    @Override
    public JSONArray read(String path) {
        try {
            // Use DOM parser to handle repeating elements correctly
            javax.xml.parsers.DocumentBuilderFactory factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
            javax.xml.parsers.DocumentBuilder builder = factory.newDocumentBuilder();
            org.w3c.dom.Document doc = builder.parse(new java.io.ByteArrayInputStream(xml.getBytes()));

            // Navigate to the target path
            String[] pathParts = path.replace("$.", "").replace("$", "").split("\\.");

            if (pathParts.length == 0 || (pathParts.length == 1 && pathParts[0].isEmpty())) {
                // Root level - return document root element
                return convertNodeListToJsonArray(doc.getChildNodes());
            }

            // Start from document element (not childNodes which includes text nodes)
            org.w3c.dom.Element root = doc.getDocumentElement();

            // If first path part matches root element name, skip it for navigation
            // but remember that we started from the root element
            boolean startedFromRoot = false;
            int startIndex = 0;
            if (pathParts[0].equals(root.getNodeName())) {
                startIndex = 1;
                startedFromRoot = true;
            }

            // Navigate through path starting from root
            org.w3c.dom.NodeList currentNodes = null;
            org.w3c.dom.Node currentNode = root;

            for (int i = startIndex; i < pathParts.length; i++) {
                String part = pathParts[i];
                if (part.isEmpty()) continue;

                // Find children with this tag name
                List<org.w3c.dom.Node> matches = new ArrayList<>();
                org.w3c.dom.NodeList children = currentNode.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    org.w3c.dom.Node child = children.item(j);
                    if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE && child.getNodeName().equals(part)) {
                        matches.add(child);
                    }
                }

                if (matches.isEmpty()) {
                    return new JSONArray();
                }

                // If this is the last part of the path, return all matches as array
                if (i == pathParts.length - 1) {
                    currentNodes = createNodeList(matches);
                    break;
                } else {
                    // Otherwise, continue navigation from first match
                    currentNode = matches.get(0);
                }
            }

            // If we started from root and no further navigation was needed,
            // return the children of the root element that are ELEMENT nodes
            if (currentNodes == null && startedFromRoot && startIndex > 0) {
                // Get all direct child elements of the root (which matched the path)
                List<org.w3c.dom.Node> rootChildren = new ArrayList<>();
                org.w3c.dom.NodeList children = root.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    org.w3c.dom.Node child = children.item(j);
                    if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
                        rootChildren.add(child);
                    }
                }
                currentNodes = createNodeList(rootChildren);
            }

            if (currentNodes == null) {
                return new JSONArray();
            }

            return convertNodeListToJsonArray(currentNodes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Create a NodeList from a List of Nodes
     */
    private org.w3c.dom.NodeList createNodeList(List<org.w3c.dom.Node> nodes) {
        return new org.w3c.dom.NodeList() {
            public org.w3c.dom.Node item(int index) { return nodes.get(index); }
            public int getLength() { return nodes.size(); }
        };
    }

    /**
     * Find all children with the given tag name from a NodeList
     */
    private org.w3c.dom.NodeList findChildrenByName(org.w3c.dom.NodeList nodes, String tagName) {
        List<org.w3c.dom.Node> result = new ArrayList<>();
        for (int i = 0; i < nodes.getLength(); i++) {
            org.w3c.dom.Node node = nodes.item(i);
            if (node.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
                org.w3c.dom.NodeList children = node.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    org.w3c.dom.Node child = children.item(j);
                    if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE && child.getNodeName().equals(tagName)) {
                        result.add(child);
                    }
                }
            }
        }
        return new org.w3c.dom.NodeList() {
            public org.w3c.dom.Node item(int index) { return result.get(index); }
            public int getLength() { return result.size(); }
        };
    }

    /**
     * Convert DOM NodeList to JSONArray of Maps
     */
    private JSONArray convertNodeListToJsonArray(org.w3c.dom.NodeList nodes) {
        JSONArray result = new JSONArray();
        for (int i = 0; i < nodes.getLength(); i++) {
            org.w3c.dom.Node node = nodes.item(i);
            if (node.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
                Map<String, Object> map = convertNodeToMap((org.w3c.dom.Element) node);
                result.add(map);
            }
        }
        return result;
    }

    /**
     * Convert a DOM Element to a Map, handling nested elements and arrays
     */
    private Map<String, Object> convertNodeToMap(org.w3c.dom.Element element) {
        Map<String, Object> map = new HashMap<>();
        org.w3c.dom.NodeList children = element.getChildNodes();

        // Group children by tag name to detect arrays (multiple elements with same name)
        Map<String, List<org.w3c.dom.Node>> childGroups = new LinkedHashMap<>();
        for (int i = 0; i < children.getLength(); i++) {
            org.w3c.dom.Node child = children.item(i);
            if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
                String tagName = child.getNodeName();
                childGroups.computeIfAbsent(tagName, k -> new ArrayList<>()).add(child);
            }
        }

        // Convert each group
        for (Map.Entry<String, List<org.w3c.dom.Node>> entry : childGroups.entrySet()) {
            String tagName = entry.getKey();
            List<org.w3c.dom.Node> nodes = entry.getValue();

            if (nodes.size() == 1) {
                // Single element - check if it's a simple value or nested object
                org.w3c.dom.Node node = nodes.get(0);
                if (isSimpleElement((org.w3c.dom.Element) node)) {
                    map.put(tagName, getElementTextValue((org.w3c.dom.Element) node));
                } else {
                    map.put(tagName, convertNodeToMap((org.w3c.dom.Element) node));
                }
            } else {
                // Multiple elements with same name - create array
                List<Object> array = new ArrayList<>();
                for (org.w3c.dom.Node node : nodes) {
                    if (isSimpleElement((org.w3c.dom.Element) node)) {
                        array.add(getElementTextValue((org.w3c.dom.Element) node));
                    } else {
                        array.add(convertNodeToMap((org.w3c.dom.Element) node));
                    }
                }
                map.put(tagName, array);
            }
        }

        return map;
    }

    /**
     * Check if element contains only text (no child elements)
     */
    private boolean isSimpleElement(org.w3c.dom.Element element) {
        org.w3c.dom.NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i).getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get text content of element, trying to parse as number if possible
     */
    private Object getElementTextValue(org.w3c.dom.Element element) {
        String text = element.getTextContent().trim();

        // Try to parse as integer
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e1) {
            // Try to parse as long
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException e2) {
                // Try to parse as double
                try {
                    return Double.parseDouble(text);
                } catch (NumberFormatException e3) {
                    // Return as string
                    return text;
                }
            }
        }
    }
}
