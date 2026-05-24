/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.connector.sqlserver.schema.support;

import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.ByteType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-11 22:21
 */
public final class SqlServerByteType extends ByteType {

    private enum TypeEnum {

        BIT("bit"),

        TINYINT("tinyint");

        private final String value;

        TypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected Byte merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }

}
