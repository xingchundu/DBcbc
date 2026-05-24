/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbcbc.connector.postgresql.schema;

import org.dbcbc.connector.postgresql.PostgreSQLException;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLBooleanType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLBytesType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLDateType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLDecimalType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLDoubleType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLFloatType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLIntType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLLongType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLStringType;
import org.dbcbc.connector.postgresql.schema.support.PostgreSQLTimestampType;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.AbstractSchemaResolver;
import org.dbcbc.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * PostgreSQL标准数据类型解析器
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:01
 */
public final class PostgreSQLSchemaResolver extends AbstractSchemaResolver {

    /**
     * 规范化 PostgreSQL 类型名：去除 schema 前缀（如 "public"."geometry" -> geometry）和引号，并转小写。
     *
     * @param typeName 原始类型名
     * @return 规范化后的类型名
     */
    public static String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return null;
        }
        // 处理带 schema 前缀的类型，如 "public"."geometry" 或 public.geometry
        int dotIndex = typeName.lastIndexOf('.');
        if (dotIndex >= 0) {
            typeName = typeName.substring(dotIndex + 1);
        }
        // 去除引号
        typeName = typeName.replace("\"", "").replace("'", "").trim();
        return typeName.toLowerCase();
    }

    @Override
    protected DataType getDataType(Map<String, DataType> mapping, Field field) {
        // 先用原始类型名查找，找不到则规范化后再查
        DataType dataType = mapping.get(field.getTypeName());
        if (dataType == null) {
            dataType = mapping.get(normalizeTypeName(field.getTypeName()));
        }
        return dataType;
    }

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(new PostgreSQLStringType(), new PostgreSQLIntType(), new PostgreSQLLongType(), new PostgreSQLDecimalType(), new PostgreSQLFloatType(), new PostgreSQLDoubleType(), new PostgreSQLDateType(), new PostgreSQLTimestampType(), new PostgreSQLBooleanType(), new PostgreSQLBytesType())
                .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
                    if (mapping.containsKey(typeName)) {
                        throw new PostgreSQLException("Duplicate type name: " + typeName);
                    }
                    mapping.put(typeName, t);
                }));
    }
}