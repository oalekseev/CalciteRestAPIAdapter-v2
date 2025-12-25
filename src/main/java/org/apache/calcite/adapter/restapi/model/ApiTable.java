package org.apache.calcite.adapter.restapi.model;

import lombok.Data;

import java.util.List;

@Data
public class ApiTable {
    private String name;
    private String deepestArrayPath;
    private List<ApiParameter> parameters;
    private ApiRequestConfig requestConfig;  // Each table has its own request configuration
}

