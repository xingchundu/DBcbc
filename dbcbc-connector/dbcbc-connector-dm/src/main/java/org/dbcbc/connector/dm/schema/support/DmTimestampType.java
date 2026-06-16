package org.dbcbc.connector.dm.schema.support;

import org.dbcbc.common.util.DateFormatUtil;
import org.dbcbc.connector.dm.schema.support.DmDateParseUtil;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.TimestampType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class DmTimestampType extends TimestampType {

    private enum TypeEnum {
        TIMESTAMP("TIMESTAMP"), TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP WITH TIME ZONE"),
        TIMESTAMP_WITH_LOCAL_TIME_ZONE("TIMESTAMP WITH LOCAL TIME ZONE");

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
    protected Timestamp merge(Object val, Field field) {
        if (val instanceof Timestamp) {
            return (Timestamp) val;
        }
        if (val instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) val).getTime());
        }
        if (val instanceof String) {
            Timestamp timestamp = DmDateParseUtil.parseTimestamp((String) val);
            if (timestamp != null) {
                return timestamp;
            }
            try {
                return DateFormatUtil.stringToTimestamp((String) val);
            } catch (RuntimeException e) {
                return throwUnsupportedException(val, field);
            }
        }
        return throwUnsupportedException(val, field);
    }
}
