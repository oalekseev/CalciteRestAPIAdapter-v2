package org.apache.calcite.adapter.restapi.freemarker.exception;

/**
 * Exception type representing failures and errors within Freemarker template creation,
 * processing, or macro logic in Calcite REST adapter integrations.
 * <p>
 * Used to report template compilation or evaluation problems, variable expansion failures,
 * or incompatible input types in engine logic.
 * </p>
 */
public class FreeMarkerException extends RuntimeException {
    /** Human-readable error message for the exception. */
    private String message;

    /**
     * Constructs a new FreeMarkerException with message and cause.
     *
     * @param message Error description for context and debugging.
     * @param cause   The underlying reason or stack-trace root.
     */
    public FreeMarkerException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
    }

    /**
     * Constructs a new FreeMarkerException with a message only.
     *
     * @param message Reason for the exception.
     */
    public FreeMarkerException(String message) {
        super(message);
        this.message = message;
    }

    /**
     * Constructs a new FreeMarkerException wrapping the originating cause.
     *
     * @param cause The raw cause of the failure.
     */
    public FreeMarkerException(Throwable cause) {
        super(cause);
    }

    /**
     * Returns the custom error message if set; otherwise, standard string representation.
     */
    @Override
    public String toString() {
        return message;
    }
}
