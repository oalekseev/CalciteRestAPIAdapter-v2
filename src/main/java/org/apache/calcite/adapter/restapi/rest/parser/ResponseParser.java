package org.apache.calcite.adapter.restapi.rest.parser;

import org.apache.calcite.adapter.restapi.rest.ResponseRowReader;

import java.util.List;

/**
 * Interface for parsing HTTP responses in different formats.
 *
 * <p>Implementations handle specific content types (CSV, XML, JSON, etc.)
 * and convert raw HTTP response strings into structured row readers.</p>
 *
 * <p><b>Supported formats:</b></p>
 * <ul>
 *   <li>CSV: text/csv, text/plain</li>
 *   <li>XML: application/xml, text/xml</li>
 *   <li>JSON: application/json (default)</li>
 * </ul>
 *
 * <p>Use {@link ResponseParserChain} to automatically select the appropriate parser.</p>
 *
 * <p><b>Example usage:</b></p>
 * <pre>{@code
 * ResponseParser parser = new JsonResponseParser();
 * if (parser.canHandle("application/json")) {
 *     List<ResponseRowReader> rows = parser.parse(httpResponse, "data.items");
 * }
 * }</pre>
 */
public interface ResponseParser {

    /**
     * Checks if this parser can handle the given content type.
     *
     * @param contentType HTTP Content-Type header value (e.g., "application/json")
     * @return true if this parser supports the format
     */
    boolean canHandle(String contentType);

    /**
     * Parses HTTP response into structured row readers.
     *
     * @param httpResponse raw HTTP response body as string
     * @param arrayPath path to nested arrays
     * @return list of row readers for iteration
     * @throws ParseException if parsing fails
     */
    List<ResponseRowReader> parse(String httpResponse, String arrayPath) throws ParseException;

    /**
     * Returns parser priority for chain ordering.
     * Lower values are checked first.
     *
     * <p><b>Priority guidelines:</b></p>
     * <ul>
     *   <li>0-19: Specific formats (CSV, XML)</li>
     *   <li>20-99: Common formats (JSON)</li>
     *   <li>100+: Default/fallback parsers</li>
     * </ul>
     *
     * @return priority value (default: 50)
     */
    default int getPriority() {
        return 50;
    }

    /**
     * Returns a human-readable name for this parser.
     * Used for logging and debugging.
     *
     * @return parser name (e.g., "CsvResponseParser")
     */
    String getName();

    /**
     * Exception thrown when response parsing fails.
     */
    class ParseException extends Exception {
        public ParseException(String message) {
            super(message);
        }

        public ParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
