package org.apache.calcite.adapter.restapi.rest.typemapper;

/**
 * Maps OpenAPI integer type to Java integer types.
 *
 * <p><b>Supported mappings:</b></p>
 * <ul>
 *   <li>integer + int64 → "long"</li>
 *   <li>integer + int32 → "int"</li>
 *   <li>integer (no format) → "int" (default)</li>
 * </ul>
 */
public class IntegerTypeMapper implements OpenApiTypeMapper {

    @Override
    public boolean canHandle(String type, String format) {
        return "integer".equals(type);
    }

    @Override
    public String mapType(String type, String format) {
        if ("int64".equals(format)) {
            return "long";
        }
        if ("int32".equals(format)) {
            return "int";
        }
        return "int"; // default
    }

    @Override
    public int getPriority() {
        return 10; // Specific type, check early
    }

    @Override
    public String getName() {
        return "IntegerTypeMapper";
    }
}
