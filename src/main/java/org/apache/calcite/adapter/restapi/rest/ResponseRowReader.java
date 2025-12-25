package org.apache.calcite.adapter.restapi.rest;

import org.apache.calcite.adapter.restapi.rest.interfaces.ArrayParamReader;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Wraps a REST API response row for tabular access.
 * <p>
 * Used as an abstraction layer to enable reading fields from
 * a single row/object (or element in a list) returned by the REST service,
 * supporting projection and type conversion.
 * </p>
 *
 * Typically, {@code ArrayParamReader} provides access to individual fields
 * using relative JSON paths.
 */
@AllArgsConstructor
@Getter
public class ResponseRowReader {

    /** Underlying array/JSON/object reader for this row */
    private ArrayParamReader arrayParamReader;

}
