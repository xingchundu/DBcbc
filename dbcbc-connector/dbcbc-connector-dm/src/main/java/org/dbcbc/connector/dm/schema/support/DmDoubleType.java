package org.dbcbc.connector.dm.schema.support;

import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.DoubleType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class DmDoubleType extends DoubleType {

    private enum TypeEnum {
        DOUBLE("DOUBLE"), DOUBLE_PRECISION("DOUBLE PRECISION");

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
    protected Double merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }
}
