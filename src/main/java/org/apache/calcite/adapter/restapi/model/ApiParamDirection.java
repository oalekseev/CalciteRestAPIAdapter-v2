package org.apache.calcite.adapter.restapi.model;

public enum ApiParamDirection {

    REQUEST,
    RESPONSE,
    BOTH; //REQUEST & RESPONSE

    public boolean isRequestParam() {
        return this == REQUEST || this == BOTH;
    }

    public boolean isResponseParam() {
        return this == RESPONSE || this == BOTH;
    }

}
