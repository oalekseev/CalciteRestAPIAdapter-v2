package org.apache.calcite.adapter.restapi.rest.typemapper;

/**
 * Maps OpenAPI boolean type to Java boolean.
 *
 * <p><b>Supported mappings:</b></p>
 * <ul>
 *   <li>boolean → "boolean"</li>
 * </ul>
 */
public class BooleanTypeMapper implements OpenApiTypeMapper {

    @Override
    public boolean canHandle(String type, String format) {
        return "boolean".equals(type);
    }

    @Override
    public String mapType(String type, String format) {
        return "boolean";
    }

    @Override
    public int getPriority() {
        return 10; // Specific type, check early
    }

    @Override
    public String getName() {
        return "BooleanTypeMapper";
    }
}
