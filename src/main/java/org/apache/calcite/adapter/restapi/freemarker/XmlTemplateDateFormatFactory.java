package org.apache.calcite.adapter.restapi.freemarker;

import freemarker.core.*;
import freemarker.template.TemplateDateModel;

import java.text.ParseException;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Freemarker TemplateDateFormatFactory for producing "XML" date representations.
 * <p>
 * This implementation serializes dates using Java's {@code toString()} (typically ISO-8601-compatible)
 * and enables parsing using the custom {@link CalendarDate} utility if needed.
 * </p>
 *
 * Usage: Register this factory to configure Freemarker's date/time output for XML integration.
 */
public class XmlTemplateDateFormatFactory extends TemplateDateFormatFactory {

    /** Singleton instance for reuse across Freemarker environments. */
    public static final XmlTemplateDateFormatFactory INSTANCE = new XmlTemplateDateFormatFactory();

    /** Private constructor for singleton instantiation only. */
    private XmlTemplateDateFormatFactory() { }

    /**
     * Returns the singleton date format for XML-style output.
     *
     * @param params           Additional parameters/suffix in Freemarker format string (ignored, must be empty)
     * @param dateType         Type of date: date, time, or datetime
     * @param locale           Locale (ignored in XML format)
     * @param timeZone         Time zone (ignored in XML format)
     * @param zoneLessInput    Whether the date/time string has no time zone info
     * @param env              Current Freemarker environment
     * @return                 Singleton XmlTemplateDateFormat instance
     * @throws InvalidFormatParametersException if unsupported parameters are present
     */
    @Override
    public TemplateDateFormat get(String params, int dateType,
                                  Locale locale, TimeZone timeZone, boolean zoneLessInput,
                                  Environment env)
            throws InvalidFormatParametersException {
        TemplateFormatUtil.checkHasNoParameters(params);
        return XmlTemplateDateFormat.INSTANCE;
    }

    /**
     * Implementation of Freemarker TemplateDateFormat for XML compatibility.
     * <p>
     * Serializes dates as simple strings (e.g. ISO-8601 or Java {@linkplain Date#toString()} output),
     * typically used for XML and SOAP data.
     * </p>
     */
    private static class XmlTemplateDateFormat extends TemplateDateFormat {

        private static final XmlTemplateDateFormat INSTANCE = new XmlTemplateDateFormat();

        private XmlTemplateDateFormat() { }

        /**
         * Formats the given Freemarker date model to plain text for XML.
         *
         * @param dateModel Input date model (date, time, or datetime).
         * @return String representation for XML (uses Date.toString()).
         */
        @Override
        public String formatToPlainText(TemplateDateModel dateModel) {
            return dateModel.toString();
        }

        /** This format is not locale dependent. */
        @Override
        public boolean isLocaleBound() {
            return false;
        }

        /** This format is not time zone dependent. */
        @Override
        public boolean isTimeZoneBound() {
            return false;
        }

        /**
         * Parses a date/time string according to XML conventions using {@link CalendarDate#parse}.
         *
         * @param s        Source date/time string
         * @param dateType Freemarker date type constant
         * @return Java Date representation
         * @throws UnparsableValueException if parsing fails
         */
        @Override
        public Date parse(String s, int dateType) throws UnparsableValueException {
            try {
                return CalendarDate.parse(s, dateType);
            } catch (ParseException ex) {
                throw new UnparsableValueException("", ex);
            }
        }

        /** @return Description for registry or debugging. */
        @Override
        public String getDescription() {
            return "xml format";
        }

    }

}
