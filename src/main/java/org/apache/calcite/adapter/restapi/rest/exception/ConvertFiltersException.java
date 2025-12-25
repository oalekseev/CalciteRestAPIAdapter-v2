package org.apache.calcite.adapter.restapi.rest.exception;

import org.apache.calcite.adapter.restapi.freemarker.exception.ConvertException;

/**
 * Exception indicating failure during filter conversion or translation
 * (e.g., when processing SQL WHERE filters into REST API filter structures).
 * <p>
 * Extends {@link ConvertException} for unified exception handling and reporting
 * within the REST adapter's filter and query construction logic.
 * </p>
 */
public class ConvertFiltersException extends ConvertException {

    /**
     * Constructs a new ConvertFiltersException with a cause.
     *
     * @param cause The cause of this exception (e.g., parsing error).
     */
    ConvertFiltersException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new ConvertFiltersException with a message.
     *
     * @param message Human-readable error message describing the failure.
     */
    ConvertFiltersException(String message) {
        super(message);
    }

    /**
     * Factory method to wrap an existing throwable as a ConvertFiltersException.
     *
     * @param cause The original throwable.
     * @return Wraps and returns a ConvertFiltersException.
     */
    public static ConvertFiltersException buildConvertFiltersException(Throwable cause) {
        return new ConvertFiltersException(cause);
    }

    /**
     * Factory method to create a ConvertFiltersException with a message.
     *
     * @param message Human-readable error message describing the failure.
     * @return A new ConvertFiltersException with the given message.
     */
    public static ConvertFiltersException buildConvertFiltersException(String message) {
        return new ConvertFiltersException(message);
    }

}
