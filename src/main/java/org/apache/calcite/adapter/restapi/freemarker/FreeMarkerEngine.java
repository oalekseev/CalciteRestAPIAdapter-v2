package org.apache.calcite.adapter.restapi.freemarker;

import org.apache.calcite.adapter.restapi.freemarker.exception.ConvertException;
import org.apache.calcite.adapter.restapi.freemarker.exception.FreeMarkerException;
import org.apache.calcite.adapter.restapi.freemarker.exception.FreeMarkerFormatException;
import freemarker.core.TemplateDateFormatFactory;
import freemarker.core.TemplateNumberFormatFactory;
import freemarker.template.*;
import lombok.Getter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Application-wide singleton for Freemarker template engine configuration and execution.
 * <p>
 * - Configures date and number formatting for integration with REST APIs (XML/Java conventions).
 * - Allows thread-safe creation and evaluation of dynamic templates from strings.
 * - Provides conversion between Java objects and Freemarker {@code TemplateModel}s.
 * </p>
 */
public class FreeMarkerEngine {

    /** Singleton instance of engine for global access. */
    @Getter
    private static final FreeMarkerEngine instance = new FreeMarkerEngine();

    /** Freemarker configuration: thread-safe, global per application. */
    private static final Configuration cfg = new Configuration(new Version("2.3.28"));

    private String fmFunctions;

    /**
     * Initializes Freemarker configuration with custom date/number formats and error handlers.
     * Should be called once at application startup.
     */
    public static void init() {
        cfg.setBooleanFormat("c");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);
        cfg.setWrapUncheckedExceptions(true);

        // Register custom date formatters (e.g., XML)
        Map<String, TemplateDateFormatFactory> templateDateFormatFactoryMap = new HashMap<>();
        templateDateFormatFactoryMap.put("xml", XmlTemplateDateFormatFactory.INSTANCE);
        cfg.setCustomDateFormats(templateDateFormatFactoryMap);
        cfg.setDateFormat("@xml");
        cfg.setDateTimeFormat("@xml");
        cfg.setTimeFormat("@xml");

        // Register custom number formatter (e.g., Java style)
        Map<String, TemplateNumberFormatFactory> templateNumberFormatFactoryMap = new HashMap<>();
        templateNumberFormatFactoryMap.put("java", JavaTemplateNumberFormatFactory.INSTANCE);
        cfg.setCustomNumberFormats(templateNumberFormatFactoryMap);
        cfg.setNumberFormat("@java");
    }

    /**
     * Registers a static/global variable for Freemarker templates.
     * Useful for functions or reusable constants.
     *
     * @param name Name under which the variable will be made available.
     * @param tm   The TemplateModel instance to share.
     */
    public static void setSharedVariable(String name, TemplateModel tm) {
        cfg.setSharedVariable(name, tm);
    }

    /**
     * Renders a Freemarker template string with provided variable bindings.
     *
     * @param template  The template text as a string.
     * @param variables Bindings (macro/variable name -> TemplateModel).
     * @return The rendered template result as a trimmed string.
     * @throws FreeMarkerFormatException If the template fails to compile or renders with error.
     */
    public String process(String template, Map<String, TemplateModel> variables)
            throws FreeMarkerFormatException {
        StringWriter stringWriter = new StringWriter();
        try {
            getTemplate(template).process(variables, stringWriter);
        } catch (IOException | TemplateException ex) {
            throw new FreeMarkerFormatException(ex.getMessage());
        }
        return stringWriter.toString().trim();
    }

    /**
     * Compiles a Freemarker template from the provided string.
     *
     * @param templateText Plain template source code.
     * @return The compiled Freemarker Template object.
     * @throws FreeMarkerException If the template cannot be parsed.
     */
    public Template getTemplate(String templateText) {
        try {
            return new Template("freemarker", templateText, cfg);
        } catch (IOException e) {
            throw new FreeMarkerException(e.getMessage(), e);
        }
    }

    /**
     * Converts a Java object to a Freemarker TemplateModel, for use as a variable in templates.
     *
     * @param value Arbitrary Java object (primitives, collections, beans, etc).
     * @return Corresponding TemplateModel for Freemarker binding.
     * @throws ConvertException If wrapping fails or the value type is not supported.
     */
    public static TemplateModel convert(Object value) throws ConvertException {
        try {
            return cfg.getObjectWrapper().wrap(value);
        } catch (TemplateModelException e) {
            throw ConvertException.buildConvertException(e);
        }
    }

}
