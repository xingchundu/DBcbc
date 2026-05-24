/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.mysql.schema;

import org.dbcbc.connector.mysql.MySQLException;
import org.dbcbc.connector.mysql.schema.support.MySQLByteType;
import org.dbcbc.connector.mysql.schema.support.MySQLBytesType;
import org.dbcbc.connector.mysql.schema.support.MySQLDateType;
import org.dbcbc.connector.mysql.schema.support.MySQLDecimalType;
import org.dbcbc.connector.mysql.schema.support.MySQLDoubleType;
import org.dbcbc.connector.mysql.schema.support.MySQLFloatType;
import org.dbcbc.connector.mysql.schema.support.MySQLIntType;
import org.dbcbc.connector.mysql.schema.support.MySQLLongType;
import org.dbcbc.connector.mysql.schema.support.MySQLShortType;
import org.dbcbc.connector.mysql.schema.support.MySQLStringType;
import org.dbcbc.connector.mysql.schema.support.MySQLTimeType;
import org.dbcbc.connector.mysql.schema.support.MySQLTimestampType;
import org.dbcbc.sdk.schema.AbstractSchemaResolver;
import org.dbcbc.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * MySQL标准数据类型解析器
 * <p>https://gitee.com/ghi/dbcbc/wikis/%E9%A1%B9%E7%9B%AE%E8%AE%BE%E8%AE%A1/%E6%A0%87%E5%87%86%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B/MySQL</p>
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-25 22:08
 */
public final class MySQLSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(
            new MySQLBytesType(),
            new MySQLByteType(),
            new MySQLDateType(),
            new MySQLDecimalType(),
            new MySQLDoubleType(),
            new MySQLFloatType(),
            new MySQLIntType(),
            new MySQLLongType(),
            new MySQLShortType(),
            new MySQLStringType(),
            new MySQLTimestampType(),
            new MySQLTimeType())
        .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
            if (mapping.containsKey(typeName)) {
                throw new MySQLException("Duplicate type name: " + typeName);
            }
            mapping.put(typeName, t);
        }));
    }

}