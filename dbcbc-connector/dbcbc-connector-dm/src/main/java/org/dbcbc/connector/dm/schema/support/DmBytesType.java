package org.dbcbc.connector.dm.schema.support;

import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.BytesType;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class DmBytesType extends BytesType {

    private enum TypeEnum {
        BLOB("BLOB"), BINARY("BINARY"), VARBINARY("VARBINARY"), IMAGE("IMAGE");

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
    protected byte[] getDefaultMergedVal(Field field) {
        return new byte[0];
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        if (val instanceof Blob) {
            try {
                Blob blob = (Blob) val;
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new org.dbcbc.connector.dm.DmException(e);
            }
        }
        if (val instanceof String) {
            return ((String) val).getBytes();
        }
        return throwUnsupportedException(val, field);
    }
}
