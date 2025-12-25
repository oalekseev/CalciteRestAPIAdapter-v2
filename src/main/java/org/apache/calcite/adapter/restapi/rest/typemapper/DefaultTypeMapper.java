package org.apache.calcite.adapter.restapi.rest.typemapper;

/**
 * Default fallback mapper for unsupported OpenAPI types.
 *
 * <p>Maps all unrecognized types to "string" as a safe default.</p>
 *
 * <p><b>Priority:</b> 100 (checked last)</p>
 */
public class DefaultTypeMapper implements OpenApiTypeMapper {

    @Override
    public boolean canHandle(String type, String format) {
        return true; // Handles everything as fallback
    }

    @Override
    public String mapType(String type, String format) {
        return "string"; // Safe default for unknown types
    }

    @Override
    public int getPriority() {
        return 100; // Check last (default fallback)
    }

    @Override
    public String getName() {
        return "DefaultTypeMapper";
    }
}
