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
     * 达梦/Oracle 兼容库元数据中类型常带精度后缀（如 TIMESTAMP(6)、NUMBER(20)），需归一化后再匹配。
     */
    @Override
    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        DataType dataType = super.getDataType(mapping, field);
        if (dataType != null || field == null || field.getTypeName() == null) {
            return dataType;
        }
        String upper = field.getTypeName().toUpperCase();
        if (upper.startsWith("TIMESTAMP")) {
            return mapping.get("TIMESTAMP");
        }
        if (upper.startsWith("NUMBER") || upper.startsWith("NUMERIC") || upper.startsWith("DECIMAL")) {
            DataType numberType = mapping.get("NUMBER");
            return numberType != null ? numberType : mapping.get("NUMERIC");
        }
        if (upper.startsWith("NVARCHAR2")) {
            return mapping.get("NVARCHAR2");
        }
        if (upper.startsWith("VARCHAR2")) {
            return mapping.get("VARCHAR2");
        }
        if (upper.startsWith("VARCHAR")) {
            return mapping.get("VARCHAR");
        }
        if (upper.startsWith("NCHAR")) {
            return mapping.get("NCHAR");
        }
        if (upper.startsWith("CHAR")) {
            return mapping.get("CHAR");
        }
        return null;
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
        }));
    }
}
