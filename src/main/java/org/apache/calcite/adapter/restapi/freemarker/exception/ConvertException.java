package org.apache.calcite.adapter.restapi.freemarker.exception;

/**
 * General exception type for errors occurring during object-to-TemplateModel conversion in Freemarker adapter logic.
 * <p>
 * Used when wrapping failures in type adaptation, expression processing, or value wrapping for templates.
 * </p>
 */
public class ConvertException extends RuntimeException {

    /**
     * Constructs a new ConvertException with a detailed message.
     *
     * @param message Reason for the exception.
     */
    public ConvertException(String message) {
        super(message);
    }

    /**
     * Constructs a new ConvertException wrapping the originating cause.
     *
     * @param cause The underlying exception or error.
     */
    public ConvertException(Throwable cause) {
        super(cause);
    }

    /**
     * Static factory for consistent exception wrapping.
     *
     * @param cause The root cause.
     * @return      New instance of ConvertException wrapping the cause.
     */
    public static ConvertException buildConvertException(Throwable cause) {
        return new ConvertException(cause);
    }

}
