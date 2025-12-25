package org.apache.calcite.adapter.restapi.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ApiRequestConfig {
    private String addresses;
    private int connectionTimeout;
    private int responseTimeout;

    private String defaultContentType; // e.g. "application/json", "text/csv", "application/xml"
    private String method;
    private String url;
    private String body;
    private String headerTemplate;
    private String urlTemplate;
    private int pageStart;
    private int pageSize;
    private List<ApiHeader> apiHeaders;

    // Maps SQL table column names to REST API filter parameter names
    // Example: {"department_name": "name", "department_id": "id"}
    private Map<String, String> filterFieldMappings;

}
