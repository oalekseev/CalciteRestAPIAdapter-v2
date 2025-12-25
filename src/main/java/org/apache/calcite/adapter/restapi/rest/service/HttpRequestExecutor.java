package org.apache.calcite.adapter.restapi.rest.service;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Executes HTTP requests and handles responses for REST API communication.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Execute HTTP requests (GET, POST, etc.)</li>
 *   <li>Log request/response details for debugging</li>
 *   <li>Validate response status codes</li>
 *   <li>Extract response body as string</li>
 * </ul>
 *
 * <p><b>Success criteria:</b> HTTP status codes 200-299</p>
 *
 * <p><b>Note:</b> Uses a shared HttpClient instance for connection pooling
 * and resource efficiency.</p>
 */
public class HttpRequestExecutor {

    private static final Logger logger = LoggerFactory.getLogger(HttpRequestExecutor.class);

    /**
     * Shared HttpClient instance for connection pooling and reuse.
     * Creating a new HttpClient for each request is expensive;
     * this singleton improves performance.
     */
    private static final org.apache.hc.client5.http.impl.classic.CloseableHttpClient SHARED_HTTP_CLIENT =
            HttpClientBuilder.create().build();

    /**
     * Executes an HTTP request and returns the response body as string.
     *
     * @param request Configured HTTP request to execute
     * @return Response body as UTF-8 string
     * @throws IOException If request execution fails or response is unsuccessful
     */
    public String executeRequest(HttpUriRequestBase request) throws IOException {
        if (logger.isDebugEnabled()) {
            logRequest(request);
        }

        return SHARED_HTTP_CLIENT.execute(request, response -> {
                int statusCode = response.getCode();

                if (logger.isDebugEnabled()) {
                    logResponse(response, statusCode);
                }

                if (!isSuccessfulResponse(statusCode)) {
                    throw new RuntimeException("Request Failed, status code (" + statusCode + ")");
                }

                HttpEntity entity = response.getEntity();
                if (entity == null) {
                    throw new IOException("Empty response entity");
                }

                try (InputStream is = entity.getContent()) {
                    String responseBody = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Response Body:");
                        logger.debug("{}", responseBody);
                    }
                    return responseBody;
                }
            });
    }

    /**
     * Checks if HTTP status code indicates success (200-299).
     *
     * @param statusCode HTTP status code
     * @return true if successful, false otherwise
     */
    private boolean isSuccessfulResponse(int statusCode) {
        return (statusCode - 200 >= 0) && (statusCode - 200 < 100);
    }

    /**
     * Logs HTTP request details for debugging.
     *
     * @param request HTTP request to log
     */
    private void logRequest(HttpUriRequestBase request) {
        logger.debug("=== HTTP REQUEST ===");
        logger.debug("Method: {}", request.getMethod());
        logger.debug("URI: {}", request.getRequestUri());
        logger.debug("Headers:");
        for (var header : request.getHeaders()) {
            logger.debug("  {}: {}", header.getName(), header.getValue());
        }

        // Log request body if present
        if (request.getEntity() != null) {
            try {
                String requestBody = EntityUtils.toString(request.getEntity(), StandardCharsets.UTF_8);
                logger.debug("Request Body:");
                logger.debug("{}", requestBody);
                // Recreate entity after reading (since InputStream is consumed)
                request.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));
            } catch (Exception e) {
                logger.warn("Could not log request body: {}", e.getMessage());
            }
        } else {
            logger.debug("Request Body: <none>");
        }
    }

    /**
     * Logs HTTP response details for debugging.
     *
     * @param response HTTP response
     * @param statusCode HTTP status code
     */
    private void logResponse(org.apache.hc.core5.http.HttpResponse response, int statusCode) {
        logger.debug("=== HTTP RESPONSE ===");
        logger.debug("Status Code: {}", statusCode);
        logger.debug("Response Headers:");
        for (var header : response.getHeaders()) {
            logger.debug("  {}: {}", header.getName(), header.getValue());
        }
    }
}
