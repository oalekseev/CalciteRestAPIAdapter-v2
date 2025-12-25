package org.apache.calcite.adapter.restapi.rest.typemapper;

/**
 * Maps OpenAPI string type to Java string and date/time types.
 *
 * <p><b>Supported mappings:</b></p>
 * <ul>
 *   <li>string + date-time → "timestamp"</li>
 *   <li>string + date → "date"</li>
 *   <li>string + time → "time"</li>
 *   <li>string + uuid → "uuid"</li>
 *   <li>string + byte → "byte"</li>
 *   <li>string + binary → "byte"</li>
 *   <li>string (no format) → "string" (default)</li>
 * </ul>
 */
public class StringTypeMapper implements OpenApiTypeMapper {

    @Override
    public boolean canHandle(String type, String format) {
        return "string".equals(type);
    }

    @Override
    public String mapType(String type, String format) {
        if (format != null) {
            switch (format) {
                case "date-time":
                    return "timestamp";
                case "date":
                    return "date";
                case "time":
                    return "time";
                case "uuid":
                    return "uuid";
                case "byte":
                case "binary":
                    return "byte";
            }
        }
        return "string"; // default
    }

    @Override
    public int getPriority() {
        return 10; // Specific type, check early
    }

    @Override
    public String getName() {
        return "StringTypeMapper";
    }
}
