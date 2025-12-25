package org.apache.calcite.adapter.restapi.rest.typemapper;

/**
 * Maps OpenAPI number type to Java floating-point types.
 *
 * <p><b>Supported mappings:</b></p>
 * <ul>
 *   <li>number + float → "float"</li>
 *   <li>number + double → "double"</li>
 *   <li>number (no format) → "double" (default)</li>
 * </ul>
 */
public class NumberTypeMapper implements OpenApiTypeMapper {

    @Override
    public boolean canHandle(String type, String format) {
        return "number".equals(type);
    }

    @Override
    public String mapType(String type, String format) {
        if ("float".equals(format)) {
            return "float";
        }
        if ("double".equals(format)) {
            return "double";
        }
        return "double"; // default
    }

    @Override
    public int getPriority() {
        return 10; // Specific type, check early
    }

    @Override
    public String getName() {
        return "NumberTypeMapper";
    }
}
