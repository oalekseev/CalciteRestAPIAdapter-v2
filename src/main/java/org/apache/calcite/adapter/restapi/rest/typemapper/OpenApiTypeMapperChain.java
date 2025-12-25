package org.apache.calcite.adapter.restapi.rest.typemapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Selects appropriate OpenAPI type mapper based on type and format.
 *
 * <p>Mappers are checked in priority order (lower priority values first).
 * The first mapper that can handle the type/format is used.</p>
 *
 * <p><b>Priority guidelines:</b></p>
 * <ul>
 *   <li>0-19: Specific types (integer, number, boolean, string)</li>
 *   <li>100+: Default/fallback mappers</li>
 * </ul>
 *
 * <p><b>Example usage:</b></p>
 * <pre>{@code
 * OpenApiTypeMapperChain chain = new OpenApiTypeMapperChain();
 * chain.addMapper(new IntegerTypeMapper())
 *      .addMapper(new StringTypeMapper())
 *      .addMapper(new DefaultTypeMapper());
 *
 * String javaType = chain.mapType("integer", "int64"); // Returns "long"
 * }</pre>
 */
public class OpenApiTypeMapperChain {

    private final List<OpenApiTypeMapper> mappers = new ArrayList<>();

    /**
     * Adds a mapper to the chain.
     * Mappers are automatically sorted by priority after adding.
     *
     * @param mapper Mapper to add
     * @return This chain instance for method chaining
     */
    public OpenApiTypeMapperChain addMapper(OpenApiTypeMapper mapper) {
        mappers.add(mapper);
        // Sort by priority (lower values first)
        mappers.sort(Comparator.comparingInt(OpenApiTypeMapper::getPriority));
        return this;
    }

    /**
     * Maps OpenAPI type/format to Java type name using first matching mapper.
     *
     * @param type OpenAPI type (e.g., "integer", "string")
     * @param format OpenAPI format (e.g., "int64", "date-time", can be null)
     * @return Java type name (e.g., "long", "timestamp", "string")
     */
    public String mapType(String type, String format) {
        for (OpenApiTypeMapper mapper : mappers) {
            if (mapper.canHandle(type, format)) {
                return mapper.mapType(type, format);
            }
        }

        // Should not happen if DefaultTypeMapper is registered
        return "string"; // Safe fallback
    }

    /**
     * Gets the mapper that would handle the given type/format.
     *
     * @param type OpenAPI type
     * @param format OpenAPI format (can be null)
     * @return The mapper that can handle this type/format, or null if none found
     */
    public OpenApiTypeMapper getMapperForType(String type, String format) {
        return mappers.stream()
                .filter(m -> m.canHandle(type, format))
                .findFirst()
                .orElse(null);
    }

    /**
     * Returns all registered mappers in priority order.
     *
     * @return Unmodifiable list of mappers
     */
    public List<OpenApiTypeMapper> getMappers() {
        return new ArrayList<>(mappers);
    }

    /**
     * Clears all registered mappers.
     */
    public void clear() {
        mappers.clear();
    }
}
