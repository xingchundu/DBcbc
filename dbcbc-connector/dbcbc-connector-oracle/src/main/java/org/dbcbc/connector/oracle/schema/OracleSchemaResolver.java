/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.oracle.schema;

import org.dbcbc.connector.oracle.OracleException;
import org.dbcbc.connector.oracle.schema.support.OracleBytesType;
import org.dbcbc.connector.oracle.schema.support.OracleDateType;
import org.dbcbc.connector.oracle.schema.support.OracleDecimalType;
import org.dbcbc.connector.oracle.schema.support.OracleDoubleType;
import org.dbcbc.connector.oracle.schema.support.OracleFloatType;
import org.dbcbc.connector.oracle.schema.support.OracleIntType;
import org.dbcbc.connector.oracle.schema.support.OracleLongType;
import org.dbcbc.connector.oracle.schema.support.OracleStringType;
import org.dbcbc.connector.oracle.schema.support.OracleTimestampType;
import org.dbcbc.sdk.schema.AbstractSchemaResolver;
import org.dbcbc.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-24 23:45
 */
public final class OracleSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
            new OracleBytesType(),
            new OracleDateType(),
            new OracleDecimalType(),
            new OracleDoubleType(),
            new OracleFloatType(),
            new OracleIntType(),
            new OracleLongType(),
            new OracleStringType(),
            new OracleTimestampType())
        .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
            if (mapping.containsKey(typeName)) {
                throw new OracleException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }
}
