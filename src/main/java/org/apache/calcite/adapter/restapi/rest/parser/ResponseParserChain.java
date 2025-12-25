package org.apache.calcite.adapter.restapi.rest.parser;

import org.apache.calcite.adapter.restapi.rest.ResponseRowReader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Selects the appropriate ResponseParser based on content type.
 *
 * <p>Parsers are checked in priority order (lower priority values first).
 * The first parser that can handle the content type is used.</p>
 *
 * <p><b>Priority guidelines:</b></p>
 * <ul>
 *   <li>0-19: Specific formats (CSV, XML)</li>
 *   <li>20-99: Common formats</li>
 *   <li>100+: Default/fallback parsers (JSON)</li>
 * </ul>
 *
 * <p><b>Example usage:</b></p>
 * <pre>{@code
 * ResponseParserChain chain = new ResponseParserChain();
 * chain.addParser(new CsvResponseParser())
 *      .addParser(new XmlResponseParser())
 *      .addParser(new JsonResponseParser());
 *
 * List<ResponseRowReader> rows = chain.parse(httpResponse, contentType, arrayPath);
 * }</pre>
 */
public class ResponseParserChain {

    private final List<ResponseParser> parsers = new ArrayList<>();

    /**
     * Adds a parser to the chain.
     * Parsers are automatically sorted by priority after adding.
     *
     * @param parser Parser to add
     * @return This chain instance for method chaining
     */
    public ResponseParserChain addParser(ResponseParser parser) {
        parsers.add(parser);
        // Sort by priority (lower values first)
        parsers.sort(Comparator.comparingInt(ResponseParser::getPriority));
        return this;
    }

    /**
     * Parses HTTP response using the first matching parser.
     *
     * @param httpResponse Raw HTTP response body
     * @param contentType HTTP Content-Type header value
     * @param arrayPath Path to nested arrays
     * @return List of row readers for iteration
     * @throws ResponseParser.ParseException If no parser can handle the content type or parsing fails
     */
    public List<ResponseRowReader> parse(String httpResponse, String contentType, String arrayPath)
            throws ResponseParser.ParseException {

        // Find first parser that can handle this content type
        for (ResponseParser parser : parsers) {
            if (parser.canHandle(contentType)) {
                return parser.parse(httpResponse, arrayPath);
            }
        }

        // No parser found - this should not happen if JsonResponseParser is registered as fallback
        throw new ResponseParser.ParseException(
                "No parser found for content type: " + contentType);
    }

    /**
     * Gets the parser that would handle the given content type.
     *
     * @param contentType HTTP Content-Type header value
     * @return The parser that can handle this content type, or null if none found
     */
    public ResponseParser getParserForContentType(String contentType) {
        return parsers.stream()
                .filter(p -> p.canHandle(contentType))
                .findFirst()
                .orElse(null);
    }

    /**
     * Returns all registered parsers in priority order.
     *
     * @return Unmodifiable list of parsers
     */
    public List<ResponseParser> getParsers() {
        return new ArrayList<>(parsers);
    }

    /**
     * Clears all registered parsers.
     */
    public void clear() {
        parsers.clear();
    }
}
