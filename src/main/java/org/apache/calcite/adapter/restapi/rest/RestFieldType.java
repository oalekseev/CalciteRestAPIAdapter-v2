package org.apache.calcite.adapter.restapi.rest;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps logical REST/JSON field types to Java and SQL types used for Calcite integration.
 * <p>
 * Enables translating OpenAPI/dbType conventions and Java primitive/wrapper types
 * into Calcite {@code RelDataType} for query planning and field conversion.
 * </p>
 */
public enum RestFieldType {

    STRING(String.class, "string"),
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.SHORT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.FLOAT),
    DOUBLE(Primitive.DOUBLE),
    DATE(java.sql.Date.class, "date"),
    TIME(java.sql.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp"),
    UUID(java.util.UUID.class, "uuid");

    /** Java class corresponding to the field type (boxed or reference) */
    private final Class<?> clazz;
    /** Simple field type identifier used in mapping and config */
    private final String simpleName;

    /** Fast lookup: string/dbType name -> RestFieldType */
    private static final Map<String, RestFieldType> MAP = new HashMap<>();

    static {
        for (RestFieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    /**
     * Constructs a RestFieldType from a primitive type.
     */
    RestFieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveName);
    }

    /**
     * Constructs a RestFieldType from Java Class and type name.
     */
    RestFieldType(Class<?> clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    /**
     * Returns Calcite's RelDataType for this REST field type.
     * Used for building table schemas and SQL projection.
     *
     * @param typeFactory Calcite JavaTypeFactory for type creation.
     * @return Nullable SQL type associated with this enum constant.
     */
    public RelDataType toType(JavaTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
        return typeFactory.createTypeWithNullability(sqlType, true);
    }

    /**
     * Looks up the field type enum by dbType or string name.
     * @param typeString Lowercase type string (e.g., "int", "string", "timestamp")
     * @return Matching RestFieldType, or null if not found.
     */
    public static RestFieldType of(String typeString) {
        return MAP.get(typeString);
    }
}
