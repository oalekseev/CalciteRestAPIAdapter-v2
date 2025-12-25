package org.apache.calcite.adapter.restapi.model;

import lombok.Data;

import java.util.List;

@Data
public class ApiService {
    private String dataSourceName;
    private String schemaName;
    private String version;
    private String description;
    private ApiRequestConfig requestConfig;
    private List<ApiTable> tables;

}
