package org.dbcbc.connector.dm.schema.support;

import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.IntType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class DmIntType extends IntType {

    private enum TypeEnum {
        INT("INT"), INTEGER("INTEGER"), TINYINT("TINYINT"), SMALLINT("SMALLINT");

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
    protected Integer merge(Object val, Field field) {
        if (val instanceof Integer) {
            return (Integer) val;
        }
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        if (val instanceof BigDecimal) {
            return ((BigDecimal) val).intValue();
        }
        if (val instanceof String) {
            return Integer.parseInt(((String) val).trim());
        }
        return throwUnsupportedException(val, field);
    }
}
