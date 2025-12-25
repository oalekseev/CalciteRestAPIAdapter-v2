package org.apache.calcite.adapter.restapi.rest.interfaces;

import net.minidev.json.JSONArray;

/**
 * Interface for reading array structures from REST API responses.
 * <p>
 * Provides abstraction for extracting a JSON array (or analogous list structure)
 * from the raw response, given a path expression.
 * </p>
 *
 * Implementations may support JSONPath, XPath, or other syntax depending on the content type.
 */
public interface ArrayReader {

    /**
     * Reads an array from the response using the given path expression.
     *
     * @param path Path expression to the array in the response (e.g., JSONPath or equivalent).
     * @return     The extracted {@link JSONArray} at the given path, or {@code null} if not found.
     */
    JSONArray read(String path);

}
