package org.apache.calcite.adapter.restapi.rest.service;

import com.jayway.jsonpath.JsonPath;
import freemarker.template.TemplateModel;
import org.apache.calcite.adapter.restapi.freemarker.FreeMarkerEngine;
import org.apache.calcite.adapter.restapi.freemarker.exception.ConvertException;
import org.apache.calcite.adapter.restapi.model.ApiHeader;
import org.apache.calcite.adapter.restapi.model.ApiRequestConfig;
import org.apache.calcite.adapter.restapi.rest.Field;
import org.apache.calcite.rex.RexNode;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Builds HTTP requests for REST API communication with FreeMarker template processing.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Process URL templates with FreeMarker context</li>
 *   <li>Create HTTP GET/POST requests</li>
 *   <li>Process request body templates</li>
 *   <li>Configure request timeouts</li>
 *   <li>Add headers from templates or configuration</li>
 * </ul>
 *
 * <p><b>Example usage:</b></p>
 * <pre>{@code
 * HttpRequestBuilder builder = new HttpRequestBuilder(contextBuilder);
 * HttpUriRequestBase request = builder.buildRequest(
 *     "http://api.example.com",
 *     connectionData,
 *     fieldsMap,
 *     commonContext,
 *     tableName,
 *     dnfFilters,
 *     cnfFilters,
 *     0,
 *     properties,
 *     selectedFields
 * );
 * }</pre>
 */
public class HttpRequestBuilder {

    private static final Logger logger = LoggerFactory.getLogger(HttpRequestBuilder.class);

    private final TemplateContextBuilder contextBuilder;

    public HttpRequestBuilder(TemplateContextBuilder contextBuilder) {
        this.contextBuilder = contextBuilder;
    }

    /**
     * Builds an HTTP request for the REST endpoint with filters, projections, and paging.
     *
     * @param address Base URL of REST service
     * @param connectionData API connection configuration
     * @param fieldsMap Field definitions for schema
     * @param commonContext Shared FreeMarker context
     * @param tableName Table name for context
     * @param dnfFilters Filters in DNF (OR-of-ANDs)
     * @param cnfFilters Filters in CNF (AND-of-ORs)
     * @param offset Pagination offset
     * @param properties Connection/config properties
     * @param selectedProjectFields Selected SQL projection fields
     * @return Fully configured HTTP request
     * @throws ConvertException If context building or template processing fails
     */
    public HttpUriRequestBase buildRequest(
            String address,
            ApiRequestConfig connectionData,
            Map<String, Field> fieldsMap,
            Map<String, TemplateModel> commonContext,
            String tableName,
            List<List<RexNode>> dnfFilters,
            List<List<RexNode>> cnfFilters,
            int offset,
            Properties properties,
            Set<String> selectedProjectFields) throws ConvertException {

        // Fill template context with filters, projections, etc.
        contextBuilder.fillContext(
                commonContext,
                connectionData,
                fieldsMap,
                tableName,
                dnfFilters,
                cnfFilters,
                offset,
                properties,
                selectedProjectFields);

        // Process URL template
        String processedUrl;
        if (connectionData.getUrlTemplate() != null) {
            processedUrl = FreeMarkerEngine.getInstance().process(connectionData.getUrlTemplate(), commonContext);
        } else {
            processedUrl = FreeMarkerEngine.getInstance().process(connectionData.getUrl(), commonContext);
        }

        String URI = address + processedUrl;

        // Create appropriate HTTP method
        HttpUriRequestBase request;
        if (Objects.requireNonNull(Method.normalizedValueOf(connectionData.getMethod())) == Method.POST) {
            request = new HttpPost(URI);
        } else {
            request = new HttpGet(URI);
        }

        // Apply request body template (if present)
        if (connectionData.getBody() != null) {
            String processedBody = FreeMarkerEngine.getInstance().process(connectionData.getBody(), commonContext);
            request.setEntity(new StringEntity(processedBody, StandardCharsets.UTF_8));
        }

        // Configure timeouts
        request.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(connectionData.getConnectionTimeout(), TimeUnit.SECONDS)
                .setResponseTimeout(connectionData.getResponseTimeout(), TimeUnit.SECONDS)
                .build());

        // Add headers
        addHeaders(request, connectionData, commonContext);

        return request;
    }

    /**
     * Adds headers to the HTTP request from template or configuration.
     *
     * @param request HTTP request to add headers to
     * @param connectionData API connection configuration
     * @param commonContext FreeMarker context for template processing
     */
    private void addHeaders(
            HttpUriRequestBase request,
            ApiRequestConfig connectionData,
            Map<String, TemplateModel> commonContext) {

        // Apply header template if available, otherwise use default headers
        if (connectionData.getHeaderTemplate() != null) {
            // Process header template to generate dynamic headers
            String processedHeaderTemplate = FreeMarkerEngine.getInstance().process(
                    connectionData.getHeaderTemplate(),
                    commonContext);
            addTemplateHeaders(request, processedHeaderTemplate);
        } else if (connectionData.getApiHeaders() != null) {
            for (ApiHeader apiHeader : connectionData.getApiHeaders()) {
                String processedValue = FreeMarkerEngine.getInstance().process(apiHeader.getValue(), commonContext);
                request.setHeader(apiHeader.getKey(), processedValue);
            }
        }
    }

    /**
     * Adds headers from a processed FreeMarker template.
     * Supports JSON format ({"key": "value"}) or simple format (key:value\nkey:value).
     *
     * @param request HTTP request to add headers to
     * @param processedHeaderTemplate Processed template string
     */
    private void addTemplateHeaders(HttpUriRequestBase request, String processedHeaderTemplate) {
        // Try to parse as JSON first (most common format for headers)
        try {
            // If the processed template is in JSON format like: {"headerName": "headerValue", ...}
            Object parsed = JsonPath.parse(processedHeaderTemplate).json();
            if (parsed instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> headers = (Map<String, Object>) parsed;
                for (Map.Entry<String, Object> header : headers.entrySet()) {
                    String headerName = header.getKey();
                    String headerValue = header.getValue() != null ? header.getValue().toString() : "";
                    request.setHeader(headerName, headerValue);
                }
            } else {
                logger.warn("Header template did not produce a JSON object: {}", processedHeaderTemplate);
            }
        } catch (Exception e) {
            // If JSON parsing fails, treat as simple format "key:value\nkey:value"
            logger.warn("Could not parse header template as JSON, treating as simple format. Error: {}", e.getMessage());
            parseSimpleHeaderFormat(request, processedHeaderTemplate);
        }
    }

    /**
     * Parses simple header format (key:value\nkey:value) and adds to request.
     *
     * @param request HTTP request to add headers to
     * @param processedHeaderTemplate Template string in simple format
     */
    private void parseSimpleHeaderFormat(HttpUriRequestBase request, String processedHeaderTemplate) {
        String[] lines = processedHeaderTemplate.split("\n");
        for (String line : lines) {
            line = line.trim();
            int colonIndex = line.indexOf(':');
            if (colonIndex > 0) {
                String headerName = line.substring(0, colonIndex).trim();
                String headerValue = line.substring(colonIndex + 1).trim();
                if (!headerName.isEmpty() && !headerValue.isEmpty()) {
                    request.setHeader(headerName, headerValue);
                }
            }
        }
    }
}
