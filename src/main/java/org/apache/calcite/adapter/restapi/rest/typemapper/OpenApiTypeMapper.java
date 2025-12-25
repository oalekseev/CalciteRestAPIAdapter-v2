package org.apache.calcite.adapter.restapi.rest.typemapper;

/**
 * Interface for mapping OpenAPI types to Java/SQL types.
 *
 * <p>Converts OpenAPI schema type definitions (type + format) to
 * Java type names used in Calcite schema.</p>
 *
 * <p><b>Example mappings:</b></p>
 * <ul>
 *   <li>integer + int64 → "long"</li>
 *   <li>string + date-time → "timestamp"</li>
 *   <li>boolean → "boolean"</li>
 * </ul>
 */
public interface OpenApiTypeMapper {

    /**
     * Checks if this mapper can handle the given OpenAPI type/format combination.
     *
     * @param type OpenAPI type (e.g., "integer", "string", "boolean")
     * @param format OpenAPI format (e.g., "int64", "date-time", null)
     * @return true if this mapper can handle this type/format
     */
    boolean canHandle(String type, String format);

    /**
     * Maps OpenAPI type/format to Java type name.
     *
     * @param type OpenAPI type
     * @param format OpenAPI format (can be null)
     * @return Java type name (e.g., "int", "long", "string", "timestamp")
     */
    String mapType(String type, String format);

    /**
     * Returns mapper priority for chain ordering.
     * Lower values are checked first (specific mappers before generic).
     *
     * @return priority value (default: 50)
     */
    default int getPriority() {
        return 50;
    }

    /**
     * Returns mapper name for debugging.
     *
     * @return mapper name
     */
    String getName();
}
