package org.apache.calcite.adapter.restapi.rest.config;

/**
 * Configuration interface for REST adapter settings.
 *
 * <p>Abstracts configuration sources (system properties, environment variables, files, etc.)
 * to enable testability and flexibility.</p>
 *
 * <p><b>Configuration keys:</b></p>
 * <ul>
 *   <li>defaultOpenApiFile - Default OpenAPI specification file path</li>
 *   <li>defaultSpecFile - Default spec file path (deprecated)</li>
 * </ul>
 *
 * <p><b>Example usage:</b></p>
 * <pre>{@code
 * AdapterConfiguration config = new SystemPropertyConfiguration();
 * String openApiFile = config.get("org.apache.calcite.adapter.restapi.defaultOpenApiFile");
 * }</pre>
 */
public interface AdapterConfiguration {

    /**
     * Gets configuration value by key.
     *
     * @param key Configuration key
     * @return Configuration value or null if not found
     */
    String get(String key);

    /**
     * Gets configuration value by key with default fallback.
     *
     * @param key Configuration key
     * @param defaultValue Default value if key not found
     * @return Configuration value or default value
     */
    default String get(String key, String defaultValue) {
        String value = get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Checks if configuration key exists.
     *
     * @param key Configuration key
     * @return true if key exists, false otherwise
     */
    default boolean has(String key) {
        return get(key) != null;
    }
}
