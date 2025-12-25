package org.apache.calcite.adapter.restapi.rest.interfaces;

/**
 * Interface for reading data fields from array or object structures in a REST API response.
 * <p>
 * Used to abstract access to single fields within elements of a JSON array, XML node-set,
 * or other complex REST-returned collections in Calcite adapter integrations.
 * </p>
 */
public interface ArrayParamReader {

    /**
     * Reads a field value from the given element, using a relative path expression within the array/object.
     *
     * @param index Index of the array or collection element to read from.
     * @param path  Path or field identifier (e.g., JSONPath, XPath, or property name).
     * @return      The value found at the given path in the specified element, or {@code null} if not present.
     */
    Object read(int index, String path);

}
