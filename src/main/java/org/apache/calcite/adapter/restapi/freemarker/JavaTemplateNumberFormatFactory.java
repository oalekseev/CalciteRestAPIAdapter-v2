package org.apache.calcite.adapter.restapi.freemarker;

import freemarker.core.*;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateNumberModel;

import java.util.Locale;

/**
 * Custom Freemarker TemplateNumberFormatFactory that delegates to a format
 * producing Java's standard string representation for numbers.
 * <p>
 * Used when numeric values must be output in their canonical Java string
 * form (with no locale-dependent formatting).
 * </p>
 *
 * This factory registers a singleton instance for simple, stateless formatting.
 */
public class JavaTemplateNumberFormatFactory extends TemplateNumberFormatFactory {

    /** Singleton instance for global reuse. */
    public static final JavaTemplateNumberFormatFactory INSTANCE =
            new JavaTemplateNumberFormatFactory();

    /**
     * Private constructor for singleton.
     */
    private JavaTemplateNumberFormatFactory() {
        // Prevent outside instantiation
    }

    /**
     * Returns a {@link TemplateNumberFormat} that outputs the Java string representation of a number.
     * Ignores any format parameters.
     *
     * @param params format string, must be empty
     * @param locale locale for formatting (ignored)
     * @param env    current Freemarker environment
     * @return Singleton instance of HexTemplateNumberFormat
     * @throws InvalidFormatParametersException if parameters are supplied
     */
    @Override
    public TemplateNumberFormat get(String params, Locale locale, Environment env)
            throws InvalidFormatParametersException {
        TemplateFormatUtil.checkHasNoParameters(params);
        return HexTemplateNumberFormat.INSTANCE;
    }

    /**
     * Singleton TemplateNumberFormat that returns Java's string form for a number.
     */
    private static class HexTemplateNumberFormat extends TemplateNumberFormat {

        private static final HexTemplateNumberFormat INSTANCE = new HexTemplateNumberFormat();

        private HexTemplateNumberFormat() { }

        /**
         * Formats a {@link TemplateNumberModel} using Java number toString().
         *
         * @param numberModel input number model
         * @return Java string representation of the number
         * @throws UnformattableValueException if number cannot be represented
         * @throws TemplateModelException      if extraction fails
         */
        @Override
        public String formatToPlainText(TemplateNumberModel numberModel)
                throws UnformattableValueException, TemplateModelException {
            Number n = TemplateFormatUtil.getNonNullNumber(numberModel);
            try {
                return n.toString();
            } catch (ArithmeticException e) {
                throw new UnformattableValueException(n + " doesn't fit into a number");
            }
        }

        /**
         * Indicates that this format is independent of locale.
         * @return false (not locale dependent)
         */
        @Override
        public boolean isLocaleBound() {
            return false;
        }

        /**
         * Returns a brief description of this format's purpose.
         * @return format description
         */
        @Override
        public String getDescription() {
            return "java string representation of number";
        }
    }

}
