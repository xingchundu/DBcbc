package org.dbcbc.connector.dm.schema;

import org.dbcbc.connector.dm.DmException;
import org.dbcbc.connector.dm.schema.support.DmBytesType;
import org.dbcbc.connector.dm.schema.support.DmDateType;
import org.dbcbc.connector.dm.schema.support.DmDecimalType;
import org.dbcbc.connector.dm.schema.support.DmDoubleType;
import org.dbcbc.connector.dm.schema.support.DmFloatType;
import org.dbcbc.connector.dm.schema.support.DmIntType;
import org.dbcbc.connector.dm.schema.support.DmStringType;
import org.dbcbc.connector.dm.schema.support.DmTimestampType;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.AbstractSchemaResolver;
import org.dbcbc.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

public final class DmSchemaResolver extends AbstractSchemaResolver {

    /**
     * 达梦 JDBC 元数据类型名常为全小写（如 clob、varchar2），且常带精度后缀（如 TIMESTAMP(6)），需归一化后再匹配。
     */
    @Override
    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        if (field == null || field.getTypeName() == null) {
            return null;
        }
        String typeName = normalizeTypeName(field.getTypeName());
        if (typeName.isEmpty()) {
            return null;
        }
        DataType dataType = mapping.get(typeName);
        if (dataType == null) {
            dataType = mapping.get(typeName.toUpperCase());
        }
        if (dataType == null) {
            dataType = mapping.get(typeName.toLowerCase());
        }
        if (dataType != null) {
            return dataType;
        }
        String upper = typeName.toUpperCase();
        if (upper.startsWith("TIMESTAMP")) {
            return mapping.get("TIMESTAMP");
        }
        if (upper.startsWith("NUMBER") || upper.startsWith("NUMERIC") || upper.startsWith("DECIMAL")) {
            DataType numberType = mapping.get("NUMBER");
            return numberType != null ? numberType : mapping.get("NUMERIC");
        }
        if (upper.startsWith("NVARCHAR2") || upper.startsWith("NVARCHAR")) {
            DataType nvarchar = mapping.get("NVARCHAR2");
            return nvarchar != null ? nvarchar : mapping.get("VARCHAR");
        }
        if (upper.startsWith("VARCHAR2") || upper.startsWith("VARCHAR")) {
            return mapping.get("VARCHAR");
        }
        if (upper.startsWith("NCHAR")) {
            return mapping.get("NCHAR");
        }
        if (upper.startsWith("CHAR")) {
            return mapping.get("CHAR");
        }
        if (upper.startsWith("CLOB") || upper.equals("TEXT")) {
            return mapping.get("CLOB");
        }
        if (upper.startsWith("NCLOB")) {
            return mapping.get("NCLOB");
        }
        if (upper.startsWith("BLOB") || upper.startsWith("BINARY") || upper.startsWith("VARBINARY") || upper.equals("IMAGE")) {
            DataType blobType = mapping.get("BLOB");
            return blobType != null ? blobType : mapping.get("VARBINARY");
        }
        if (upper.startsWith("BINARY_FLOAT") || upper.equals("REAL")) {
            return mapping.get("FLOAT");
        }
        if (upper.startsWith("BINARY_DOUBLE") || upper.startsWith("DOUBLE")) {
            return mapping.get("DOUBLE");
        }
        if (upper.equals("BOOLEAN") || upper.equals("BIT")) {
            DataType intType = mapping.get("INT");
            return intType != null ? intType : mapping.get("INTEGER");
        }
        if (upper.equals("BIGINT") || upper.equals("LONG")) {
            return mapping.get("NUMBER");
        }
        if (upper.equals("JSON") || upper.startsWith("JSON") || upper.equals("XML") || upper.startsWith("XML")) {
            return mapping.get("CLOB");
        }
        return null;
    }

    /**
     * 目标字段类型无法识别且值为 null 时，基类 convert 会在构造异常信息时对 null 取 class 导致 NPE。
     * Oracle 源端 CLOB 已合并为 String，写入达梦 CLOB 列时直接透传字符串。
     */
    @Override
    public Object convert(Object val, Field field) {
        if (val == null) {
            return null;
        }
        if (val instanceof String && isStringLikeTarget(field)) {
            return val;
        }
        return super.convert(val, field);
    }

    static String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return "";
        }
        String normalized = typeName.trim().replace("\"", "").replace("'", "");
        int dot = normalized.lastIndexOf('.');
        if (dot >= 0 && dot < normalized.length() - 1) {
            normalized = normalized.substring(dot + 1);
        }
        int paren = normalized.indexOf('(');
        if (paren > 0) {
            normalized = normalized.substring(0, paren);
        }
        return normalized.trim();
    }

    private boolean isStringLikeTarget(Field field) {
        if (field == null || field.getTypeName() == null) {
            return false;
        }
        String upper = normalizeTypeName(field.getTypeName()).toUpperCase();
        return upper.contains("CLOB")
                || upper.contains("NCLOB")
                || upper.contains("VARCHAR")
                || upper.equals("TEXT")
                || upper.equals("LONG")
                || upper.startsWith("CHAR")
                || upper.startsWith("NCHAR")
                || upper.contains("JSON")
                || upper.contains("XML");
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
            new DmBytesType(),
            new DmDateType(),
            new DmDecimalType(),
            new DmDoubleType(),
            new DmFloatType(),
            new DmIntType(),
            new DmStringType(),
            new DmTimestampType())
        .forEach(t -> t.getSupportedTypeName().forEach(typeName -> {
            if (mapping.containsKey(typeName)) {
                throw new DmException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
            String lower = typeName.toLowerCase();
            if (!mapping.containsKey(lower)) {
                mapping.put(lower, t);
            }
        }));
    }
}
