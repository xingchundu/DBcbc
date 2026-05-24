/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.connector.kafka.schema;

import org.dbcbc.connector.kafka.KafkaException;
import org.dbcbc.connector.kafka.schema.support.KafkaBooleanType;
import org.dbcbc.connector.kafka.schema.support.KafkaByteType;
import org.dbcbc.connector.kafka.schema.support.KafkaBytesType;
import org.dbcbc.connector.kafka.schema.support.KafkaDateType;
import org.dbcbc.connector.kafka.schema.support.KafkaDecimalType;
import org.dbcbc.connector.kafka.schema.support.KafkaDoubleType;
import org.dbcbc.connector.kafka.schema.support.KafkaFloatType;
import org.dbcbc.connector.kafka.schema.support.KafkaIntType;
import org.dbcbc.connector.kafka.schema.support.KafkaLongType;
import org.dbcbc.connector.kafka.schema.support.KafkaShortType;
import org.dbcbc.connector.kafka.schema.support.KafkaStringType;
import org.dbcbc.connector.kafka.schema.support.KafkaTimeType;
import org.dbcbc.connector.kafka.schema.support.KafkaTimestampType;
import org.dbcbc.sdk.schema.AbstractSchemaResolver;
import org.dbcbc.sdk.schema.DataType;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:18
 */
public final class KafkaSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypeMapping(Map<String, DataType> mapping) {
        Stream.of(new KafkaStringType(), new KafkaIntType(), new KafkaShortType(), new KafkaLongType(), new KafkaDecimalType(), new KafkaFloatType(), new KafkaDoubleType(), new KafkaDateType(), new KafkaTimestampType(), new KafkaTimeType(), new KafkaBooleanType(), new KafkaBytesType(), new KafkaByteType())
                .forEach(t->t.getSupportedTypeName().forEach(typeName-> {
                    if (mapping.containsKey(typeName)) {
                        throw new KafkaException("Duplicate type name: " + typeName);
                    }
                    mapping.put(typeName, t);
                }));
    }
}