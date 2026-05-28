/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbcbc.connector.postgresql.schema.support;

import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.DateType;

import java.sql.Date;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 23:26
 */
public final class PostgreSQLDateType extends DateType {

    private enum TypeEnum {

        DATE("date");

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
    protected Date merge(Object val, Field field) {
        if (val instanceof String) {
            String str = (String) val;
            if ("0000-00-00".equals(str) || "0000-00-00 00:00:00".equals(str)) {
                return null;
            }
            return Date.valueOf(str);
        }
        if (val instanceof java.util.Date) {
            return new Date(((java.util.Date) val).getTime());
        }
        return throwUnsupportedException(val, field);
    }
}
