package org.apache.calcite.adapter.restapi.model;

import lombok.Data;

@Data
public class ApiParameter {
    private String name;
    private ApiParamDirection direction;
    private String dbType;
    private String jsonpath;
}
