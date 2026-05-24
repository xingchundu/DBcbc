package org.dbcbc.connector.dm.schema.support;

import org.dbcbc.common.util.DateFormatUtil;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.DateType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class DmDateType extends DateType {

    private enum TypeEnum {
        DATE("DATE");

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
        if (val instanceof Date) {
            return (Date) val;
        }
        if (val instanceof Timestamp) {
            return new Date(((Timestamp) val).getTime());
        }
        if (val instanceof java.util.Date) {
            return new Date(((java.util.Date) val).getTime());
        }
        if (val instanceof LocalDateTime) {
            return new Date(Timestamp.valueOf((LocalDateTime) val).getTime());
        }
        if (val instanceof String) {
            Timestamp timestamp = DateFormatUtil.stringToTimestamp((String) val);
            if (timestamp != null) {
                return new Date(timestamp.getTime());
            }
            return DateFormatUtil.stringToDate((String) val);
        }
        return throwUnsupportedException(val, field);
    }
}
