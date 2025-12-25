package org.apache.calcite.adapter.restapi.rest;

import freemarker.template.TemplateBooleanModel;
import freemarker.template.TemplateModel;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Converts Object values to typed values based on RestFieldType.
 *
 * <p>Handles conversion from:</p>
 * <ul>
 *   <li>String representations to primitive types (int, long, float, etc.)</li>
 *   <li>FreeMarker TemplateModel to Java types</li>
 *   <li>Date/time string formats to Calcite internal representations</li>
 * </ul>
 *
 * <p><b>Supported types:</b></p>
 * <ul>
 *   <li>BOOLEAN, BYTE, SHORT, INT, LONG</li>
 *   <li>FLOAT, DOUBLE</li>
 *   <li>DATE, TIME, TIMESTAMP</li>
 *   <li>UUID, STRING</li>
 * </ul>
 */
public class ValueConverter {

    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    private static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd", GMT);
    private static final FastDateFormat TIME_FORMAT = FastDateFormat.getInstance("HH:mm:ss", GMT);
    private static final FastDateFormat TIMESTAMP_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss", GMT);

    /**
     * Converts object to typed value based on field type.
     *
     * @param object Object to convert (can be String, TemplateModel, or typed value)
     * @param fieldType Target field type
     * @return Converted typed value
     * @throws RuntimeException If parsing fails
     */
    public static Object convert(Object object, RestFieldType fieldType) {
        if (object == null) {
            return null;
        }

        try {
            switch (fieldType) {
                case BOOLEAN:
                    if (object instanceof TemplateModel) {
                        return object.equals(TemplateBooleanModel.TRUE);
                    }
                    return Boolean.parseBoolean(object.toString());

                case BYTE:
                    return Byte.parseByte(object.toString());

                case SHORT:
                    return Short.parseShort(object.toString());

                case INT:
                    return Integer.parseInt(object.toString());

                case LONG:
                    return Long.parseLong(object.toString());

                case FLOAT:
                    return Float.parseFloat(object.toString());

                case DOUBLE:
                    return Double.parseDouble(object.toString());

                case DATE:
                    return (int) (DATE_FORMAT.parse(object.toString()).getTime() / DateTimeUtils.MILLIS_PER_DAY);

                case TIME:
                    return (int) TIME_FORMAT.parse(object.toString()).getTime();

                case TIMESTAMP:
                    return TIMESTAMP_FORMAT.parse(object.toString()).getTime();

                case UUID:
                    return UUID.fromString(object.toString());

                case STRING:
                default:
                    return object.toString();
            }
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse value: " + object + " as " + fieldType, e);
        }
    }
}
