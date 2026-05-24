package org.dbcbc.connector.dm.schema.support;

import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.DecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class DmDecimalType extends DecimalType {

    private enum TypeEnum {
        NUMBER("NUMBER"), NUMERIC("NUMERIC"), DECIMAL("DECIMAL"), DEC("DEC");

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
    protected BigDecimal merge(Object val, Field field) {
        if (val instanceof BigDecimal) {
            return (BigDecimal) val;
        }
        if (val instanceof Number) {
            return new BigDecimal(val.toString());
        }
        if (val instanceof String) {
            String str = (String) val;
            if (str.trim().isEmpty()) {
                return BigDecimal.ZERO;
            }
            try {
                return new BigDecimal(str.trim());
            } catch (NumberFormatException e) {
                return throwUnsupportedException(val, field);
            }
        }
        if (val instanceof Boolean) {
            return new BigDecimal((Boolean) val ? 1 : 0);
        }
        return throwUnsupportedException(val, field);
    }
}
