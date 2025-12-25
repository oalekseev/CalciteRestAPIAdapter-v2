package org.apache.calcite.adapter.restapi.rest.config;

/**
 * Configuration implementation that reads from Java system properties.
 *
 * <p>Uses {@link System#getProperty(String)} to retrieve configuration values.</p>
 *
 * <p><b>Example:</b></p>
 * <pre>{@code
 * // Set system property
 * System.setProperty("org.apache.calcite.adapter.restapi.defaultOpenApiFile", "spec.yaml");
 *
 * // Read via configuration
 * AdapterConfiguration config = new SystemPropertyConfiguration();
 * String file = config.get("org.apache.calcite.adapter.restapi.defaultOpenApiFile");
 * }</pre>
 */
public class SystemPropertyConfiguration implements AdapterConfiguration {

    @Override
    public String get(String key) {
        return System.getProperty(key);
    }
}
