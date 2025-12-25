package org.apache.calcite.adapter.restapi.rest;

import freemarker.template.TemplateModel;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.adapter.restapi.model.ApiParamDirection;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Metadata for a field/column in the SQL table abstraction mapped to the REST API response.
 * <p>
 * Describes name, type mapping, SQL type reference, path within REST response,
 * direction (request/response param), and optionally a value for outbound requests.
 * </p>
 *
 * Used for dynamic schema construction and proper conversions between REST, Freemarker, and Calcite.
 */
@Getter
@Setter
public class Field {

    /** Logical name for SQL column / REST field */
    private String name;
    /** Field type mapping between REST/db/OpenAPI and Java/Calcite */
    private RestFieldType restFieldType;
    /** Calcite type definition for SQL-level query planning and projection */
    private RelDataType relDataType;
    /** True if this field is a request (input/parameter) field */
    private boolean isRequestParameter;
    /** True if this field is a response (output) field */
    private boolean isResponseParameter;
    /** Parameter direction: REQUEST (input only), RESPONSE (output only), or BOTH */
    private ApiParamDirection direction;
    /** JSONPath or equivalent for locating value within response object/array */
    private String jsonpath;
    /** Value for request parameter, as wrapped for Freemarker template usage */
    private TemplateModel requestValue;

    /**
     * Constructs a field metadata object for schema mapping.
     *
     * @param name           Logical name of the field.
     * @param restFieldType  REST/database/OpenAPI-mapped type.
     * @param relDataType    Calcite SQL type for this field.
     */
    public Field(String name, RestFieldType restFieldType, RelDataType relDataType) {
        this.name = name;
        this.restFieldType = restFieldType;
        this.relDataType = relDataType;
    }

}
