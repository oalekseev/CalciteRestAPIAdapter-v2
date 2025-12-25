package org.apache.calcite.adapter.restapi.rest;

import org.apache.calcite.adapter.restapi.freemarker.FreeMarkerEngine;
import org.apache.calcite.adapter.restapi.freemarker.exception.ConvertException;
import org.apache.calcite.adapter.restapi.model.ApiTable;
import org.apache.calcite.adapter.restapi.model.ApiRequestConfig;
import org.apache.calcite.adapter.restapi.rest.parser.CsvResponseParser;
import org.apache.calcite.adapter.restapi.rest.parser.JsonResponseParser;
import org.apache.calcite.adapter.restapi.rest.parser.ResponseParserChain;
import org.apache.calcite.adapter.restapi.rest.parser.XmlResponseParser;
import org.apache.calcite.adapter.restapi.rest.service.*;
import com.google.common.base.Joiner;
import freemarker.template.*;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
/**
 * {@code RestTable} represents a Calcite table mapped to a REST API endpoint.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Dynamically discovers schema (fields, types) from OpenAPI definitions.</li>
 *   <li>Pushes down SQL filter and projection logic to REST requests.</li>
 *   <li>Supports flexible request body generation via Freemarker templates or automatic JSON construction.</li>
 *   <li>Handles paging, filter normalization (DNF/CNF), and multi-format response parsing (JSON, XML, CSV).</li>
 * </ul>
 *
 * Designed for seamless integration of RESTful data sources with SQL queries.
 */
public class RestTable extends AbstractTable implements ProjectableFilterableTable {
    private final Logger logger = LoggerFactory.getLogger(RestTable.class);

    /** Logical group identifier for source grouping (e.g., multi-source REST). */
    private final String group;

    /** Macro context for Freemarker expansion: filters, projections, paging, etc. */
    private final Map<String, TemplateModel> commonContext;

    /** REST API endpoint/table definition from OpenAPI. */
    private final ApiTable apiTable;

    /** Connection configuration: addresses, headers, method, URL template, timeouts. */
    private final ApiRequestConfig connectionData;

    /** Cached field mapping for schema discovery (name/type/jsonpath). */
    private Map<String, Field> fieldsMap;

    /** Response format parser for CSV/XML/JSON. */
    private final ResponseParserChain responseParserChain;

    /** Service layer dependencies. */
    private final FilterConverter filterConverter;
    private final TemplateContextBuilder templateContextBuilder;
    private final HttpRequestBuilder httpRequestBuilder;
    private final HttpRequestExecutor httpRequestExecutor;
    private final SchemaDiscoveryService schemaDiscoveryService;

    /**
     * Constructs a REST SQL table from endpoint configuration and schema mapping.
     *
     * @param group Logical group name for source management
     * @param connectionData REST connection configuration
     * @param apiTable API table definition from OpenAPI
     * @param context Freemarker macro/context values
     */
    public RestTable(String group, ApiRequestConfig connectionData, ApiTable apiTable, Map<String, TemplateModel> context) {
        this.group = group;
        this.connectionData = connectionData;
        this.apiTable = apiTable;
        this.commonContext = context;
        FreeMarkerEngine.init(); // Initialize Freemarker for template handling

        // Initialize service layer
        this.responseParserChain = initializeResponseParsers();
        this.filterConverter = new FilterConverter();
        this.templateContextBuilder = new TemplateContextBuilder();
        this.httpRequestExecutor = new HttpRequestExecutor();
        this.httpRequestBuilder = new HttpRequestBuilder(templateContextBuilder);
        this.schemaDiscoveryService = new SchemaDiscoveryService();
    }

    /**
     * Initializes the response parser chain with all supported formats.
     * Parsers are registered in priority order (lower priority values first).
     *
     * @return Configured ResponseParserChain
     */
    private ResponseParserChain initializeResponseParsers() {
        ResponseParserChain chain = new ResponseParserChain();
        chain.addParser(new CsvResponseParser())
             .addParser(new XmlResponseParser())
             .addParser(new JsonResponseParser()); // Default/fallback parser
        return chain;
    }

    /**
     * Dynamically builds the SQL row type (schema) from OpenAPI model and internal mapping.
     *
     * @param typeFactory JavaTypeFactory from execution context
     * @return Struct type representing table columns
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        // Lazily initialize fields from OpenAPI parameters
        if (fieldsMap == null) {
            fieldsMap = schemaDiscoveryService.initializeFieldsMap(apiTable, typeFactory);
        }
        return schemaDiscoveryService.buildRelDataType(fieldsMap, typeFactory);
    }

    /**
     * Main entry point for SQL scan queries. Handles database-level filtering and projection.
     *
     * @param root     Calcite DataContext for execution
     * @param filters  SQL WHERE filter nodes
     * @param projects SQL SELECT field projection indices
     * @return Enumerable of result rows
     */
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        RexBuilder rexBuilder = new RexBuilder(root.getTypeFactory());

        // Disjunctive Normal Form (DNF): OR-of-ANDs; for RESTs supporting DNF-style filters
        RexNode combinedFilter = RexUtil.composeConjunction(rexBuilder, filters);
        RexNode dnfFilter = RexUtil.toDnf(rexBuilder, combinedFilter);
        List<List<RexNode>> dnfFilters = RelOptUtil.disjunctions(dnfFilter)
                .stream().map(RelOptUtil::conjunctions).collect(Collectors.toList());

        // Conjunctive Normal Form (CNF): AND-of-ORs; for RESTs supporting CNF-style filters
        RexNode cnfFilter = RexUtil.toCnf(rexBuilder, combinedFilter);
        List<List<RexNode>> cnfFilters = RelOptUtil.conjunctions(cnfFilter)
                .stream().map(RelOptUtil::disjunctions).collect(Collectors.toList());

        Set<String> selectedProjectFields = schemaDiscoveryService.getSelectedProjectFields(fieldsMap, root.getTypeFactory(), projects);
        Properties properties = ((CalciteConnection) root.getQueryProvider()).getProperties();

        int pageSize = connectionData.getPageSize();
        AtomicInteger offset = new AtomicInteger(0);

        // Triggers REST data loading and paging, uses filters/projections for endpoint query
        Map.Entry<String, List<ResponseRowReader>> restResult = getRestResult(null, dnfFilters, cnfFilters, offset.get(), properties, selectedProjectFields);

        AtomicBoolean hasMore = new AtomicBoolean(pageSize > 0 && restResult.getValue().size() == pageSize);

        return new AbstractEnumerable<>() {
            public Enumerator<Object[]> enumerator() {
                return new RestDataEnumerator(restResult.getValue(), fieldsMap, projects, () -> {
                    if (hasMore.get()) {
                        Map.Entry<String, List<ResponseRowReader>> restResultMore = getRestResult(
                                restResult.getKey(), dnfFilters, cnfFilters, offset.addAndGet(pageSize), properties, selectedProjectFields);
                        hasMore.set(pageSize > 0 && restResultMore.getValue().size() == pageSize);
                        return restResultMore.getValue();
                    } else {
                        return Collections.emptyList();
                    }
                });
            }
        };
    }

    /** Returns the API table name. */
    public String getTableName() {
        return this.apiTable.getName();
    }

    /**
     * Loads and maps REST API data as a result set, supporting fallback addresses and exception aggregation.
     *
     * @param address Preferred address or null to auto-select/switch
     * @param dnfFilters DNF-form filters
     * @param cnfFilters CNF-form filters
     * @param offset Paging offset for data requests
     * @param properties Connection properties/config
     * @param selectedProjectFields Projected SQL fields
     * @return REST response mapping: address -> rows list
     */
    public Map.Entry<String, List<ResponseRowReader>> getRestResult(
            String address,
            List<List<RexNode>> dnfFilters,
            List<List<RexNode>> cnfFilters,
            int offset,
            Properties properties,
            Set<String> selectedProjectFields) {
        if (address == null) {
            List<String> errors = new ArrayList<>();
            for (String tryingAddress : connectionData.getAddresses().split(",")) {
                try {
                    return doRequest(tryingAddress.trim(), dnfFilters, cnfFilters, offset, properties, selectedProjectFields);
                } catch (IOException e) {
                    errors.add(e.getMessage());
                    logger.warn(e.getMessage());
                }
            }
            throw new RuntimeException("All requests attempts failed: " + Joiner.on(", \n").join(errors));
        } else {
            try {
                return doRequest(address, dnfFilters, cnfFilters, offset, properties, selectedProjectFields);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Executes a single REST request and maps the response based on the detected format (CSV, XML, JSON).
     *
     * @param address REST endpoint address
     * @param dnfFilters DNF-form filters for request parameterization
     * @param cnfFilters CNF-form filters for request parameterization
     * @param offset Data paging offset
     * @param properties Connection/query properties
     * @param selectedProjectFields Projected SQL fields
     * @return Response address and mapped rows
     */
    private Map.Entry<String, List<ResponseRowReader>> doRequest(
            String address,
            List<List<RexNode>> dnfFilters,
            List<List<RexNode>> cnfFilters,
            int offset,
            Properties properties,
            Set<String> selectedProjectFields) throws ConvertException, IOException {


        // Build the request using HttpRequestBuilder service
        HttpUriRequestBase request = httpRequestBuilder.buildRequest(
                address,
                connectionData,
                fieldsMap,
                commonContext,
                getTableName(),
                dnfFilters,
                cnfFilters,
                offset,
                properties,
                selectedProjectFields);

        // Execute the request using HttpRequestExecutor service
        String httpResponse = httpRequestExecutor.executeRequest(request);
        String contentType = resolveContentType(properties);

        List<ResponseRowReader> result = new ArrayList<>();
        if (apiTable.getParameters() != null && !apiTable.getParameters().isEmpty()) {
            try {
                // Use ResponseParserChain to automatically select the appropriate parser
                result = responseParserChain.parse(httpResponse, contentType, apiTable.getDeepestArrayPath());
            } catch (Exception e) {
                logger.error("Failed to parse response with content type: {}\n", contentType, e);
                throw new IOException("Failed to parse HTTP response", e);
            }
        }
        return new AbstractMap.SimpleImmutableEntry<>(address, result);
    }


    /**
     * Determines the correct content type for REST API requests based on configuration and client-provided parameters.
     * Prioritizes OpenAPI/config settings, then client property, then defaults to JSON.
     *
     * @param properties Calcite connection/query properties
     * @return The selected MIME type for REST communication
     */
    private String resolveContentType(Properties properties) {
        if (connectionData.getDefaultContentType() != null && !connectionData.getDefaultContentType().isEmpty()) {
            return connectionData.getDefaultContentType();
        }
        if (properties != null && properties.get("contentType") != null) {
            return properties.get("contentType").toString();
        }
        return "application/json";
    }
}
