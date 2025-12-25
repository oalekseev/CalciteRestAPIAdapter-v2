package org.apache.calcite.adapter.restapi.freemarker.exception;

/**
 * Specific exception indicating errors in Freemarker template formatting or evaluation.
 * <p>
 * Used to signal failures in template construction, rendering, or when template expressions
 * produce invalid or non-conformant output (e.g., syntax errors, rendering failures, or
 * output not matching expected format).
 * </p>
 * <p>
 * Extends {@link FreeMarkerException} for unified Freemarker exception handling.
 * </p>
 */
public class FreeMarkerFormatException extends FreeMarkerException {
    /** Error message describing the formatting problem. */
    private String message;

    /**
     * Constructs a new FreeMarkerFormatException with message and cause.
     *
     * @param message Human-readable error description.
     * @param cause   The underlying cause of the formatting error.
     */
    public FreeMarkerFormatException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
    }

    /**
     * Constructs a new FreeMarkerFormatException with a message only.
     *
     * @param message Error description.
     */
    public FreeMarkerFormatException(String message) {
        super(message);
        this.message = message;
    }

    /**
     * Constructs a new FreeMarkerFormatException that wraps an underlying exception.
     *
     * @param ex The original exception that caused the failure.
     */
    public FreeMarkerFormatException(Exception ex) {
        super(ex);
    }

    /**
     * Returns the error message for this exception.
     */
    @Override
    public String toString() {
        return message;
    }
}
