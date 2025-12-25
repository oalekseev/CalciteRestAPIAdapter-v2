package org.apache.calcite.adapter.restapi.freemarker;

import freemarker.template.TemplateDateModel;
import freemarker.template.TemplateModelException;
import lombok.Getter;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * Adapter for working with date and time values in Freemarker templates in a way suitable for XML/REST output.
 * <p>
 * Supports formatting and parsing for:
 * <ul>
 *   <li>Date (yyyy-MM-dd)</li>
 *   <li>Time (HH:mm:ss)</li>
 *   <li>DateTime with timezone (ISO 8601 style)</li>
 * </ul>
 * Respects locale (configurable via system property "freemarker_datetime_functions_locale").
 * </p>
 *
 * Implements Freemarker's {@link TemplateDateModel} for compatibility with template expressions/macros.
 */
public class CalendarDate implements TemplateDateModel {

    private static final String XML_DATETIME_WITH_TIMEZONE_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ssZZ";
    private static final String XML_DATE_FORMAT_PATTERN = "yyyy-MM-dd";
    private static final String XML_TIME_FORMAT_PATTERN = "HH:mm:ss";
    private static final FastDateFormat XmlDateTimeFormatWithTimeZone = FastDateFormat.getInstance(XML_DATETIME_WITH_TIMEZONE_FORMAT_PATTERN);
    private static final FastDateFormat XmlDateFormat = FastDateFormat.getInstance(XML_DATE_FORMAT_PATTERN);
    private static final FastDateFormat TimeFormatWithTimeZone = FastDateFormat.getInstance(XML_TIME_FORMAT_PATTERN);

    /**
     * Locale for formatting. Default is ru-RU, but can be overridden via system property.
     */
    private static Locale LOCALE = Locale.forLanguageTag("ru-RU");

    static {
        String prop = System.getProperty("freemarker_datetime_functions_locale");
        if (prop != null && !prop.isEmpty()) {
            Locale locale = Locale.forLanguageTag(prop);
            if (locale != null) {
                LOCALE = locale;
            }
        }
    }

    @Getter
    private final Calendar calendar;
    private final int dataType;

    /**
     * Constructs a CalendarDate from given calendar and data type.
     *
     * @param date     Source calendar instance (time, date, or datetime).
     * @param dataType Freemarker date type constant (DATE, TIME, DATETIME).
     * @throws TemplateModelException if type is unsupported.
     */
    public CalendarDate(Calendar date, int dataType) throws TemplateModelException {
        switch (dataType) {
            case DATETIME:
                // Deep copy with timezone for full datetime
                Calendar newDateTime = Calendar.getInstance(date.getTimeZone(), LOCALE);
                newDateTime.setTimeInMillis(date.getTimeInMillis());
                newDateTime.getTime(); // Force update
                this.calendar = newDateTime;
                break;
            case DATE:
                // Only date part, reset time
                Calendar newDate = Calendar.getInstance(LOCALE);
                newDate.set(Calendar.YEAR, date.get(Calendar.YEAR));
                newDate.set(Calendar.MONTH, date.get(Calendar.MONTH));
                newDate.set(Calendar.DAY_OF_MONTH, date.get(Calendar.DAY_OF_MONTH));
                newDate.set(Calendar.HOUR_OF_DAY, 0);
                newDate.set(Calendar.MINUTE, 0);
                newDate.set(Calendar.SECOND, 0);
                newDate.set(Calendar.MILLISECOND, 0);
                newDate.getTime();
                this.calendar = newDate;
                break;
            case TIME:
                // Time part only, set arbitrary date
                Calendar newTime = Calendar.getInstance(LOCALE);
                newTime.set(Calendar.YEAR, 1970);
                newTime.set(Calendar.MONTH, 0);
                newTime.set(Calendar.DAY_OF_MONTH, 1);
                newTime.set(Calendar.HOUR_OF_DAY, date.get(Calendar.HOUR_OF_DAY));
                newTime.set(Calendar.MINUTE, date.get(Calendar.MINUTE));
                newTime.set(Calendar.SECOND, date.get(Calendar.SECOND));
                newTime.set(Calendar.MILLISECOND, 0);
                newTime.getTime();
                this.calendar = newTime;
                break;
            default:
                throw new TemplateModelException(String.format("Unsupported date and time type '%s'",
                        TemplateDateModel.TYPE_NAMES.get(dataType)));
        }
        this.dataType = dataType;
    }

    /**
     * Returns underlying Date value for use in Freemarker or formatting.
     */
    @Override
    public Date getAsDate() {
        return calendar.getTime();
    }

    /**
     * Returns Freemarker's date type constant (DATE, TIME, DATETIME).
     */
    @Override
    public int getDateType() {
        return dataType;
    }

    /**
     * Formats this date as text suitable for XML or REST API output.
     * Selects pattern based on type: date, time, or datetime.
     */
    @Override
    public String toString() {
        switch (dataType) {
            case DATE:
                return XmlDateFormat.format(calendar);
            case TIME:
                return TimeFormatWithTimeZone.format(calendar);
        }
        return FastDateFormat.getInstance(XML_DATETIME_WITH_TIMEZONE_FORMAT_PATTERN, calendar.getTimeZone()).format(calendar);
    }

    /**
     * Parses a date/time string according to data type and XML/ISO conventions.
     *
     * @param date     Source string to parse.
     * @param dataType Freemarker date type (DATE, TIME, DATETIME).
     * @return Date object.
     * @throws ParseException if format does not match.
     */
    public static Date parse(String date, int dataType) throws ParseException {
        switch (dataType) {
            case DATE:
                return XmlDateFormat.parse(date);
            case TIME:
                return TimeFormatWithTimeZone.parse(date);
        }
        return XmlDateTimeFormatWithTimeZone.parse(date);
    }
}
