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
import org.dbcbc.sdk.schema.AbstractSchemaResolver;
import org.dbcbc.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

public final class DmSchemaResolver extends AbstractSchemaResolver {

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
