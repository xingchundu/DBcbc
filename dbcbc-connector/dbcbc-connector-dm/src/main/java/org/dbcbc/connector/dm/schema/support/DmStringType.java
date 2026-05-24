package org.dbcbc.connector.dm.schema.support;

import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.StringType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class DmStringType extends StringType {

    private enum TypeEnum {
        CHAR("CHAR"), NCHAR("NCHAR"), VARCHAR("VARCHAR"), VARCHAR2("VARCHAR2"),
        NVARCHAR2("NVARCHAR2"), TEXT("TEXT"), CLOB("CLOB"), NCLOB("NCLOB"), LONG("LONG");

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
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            return val;
        }
        return super.convert(val, field);
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return new String((byte[]) val, StandardCharsets.UTF_8);
        }
        if (val instanceof Clob) {
            return clobToString((Clob) val);
        }
        return throwUnsupportedException(val, field);
    }

    private String clobToString(Clob clob) {
        try (Reader is = clob.getCharacterStream(); BufferedReader br = new BufferedReader(is)) {
            StringBuilder sb = new StringBuilder();
            String s = br.readLine();
            while (s != null) {
                sb.append(s);
                s = br.readLine();
            }
            return sb.toString();
        } catch (SQLException | IOException e) {
            throw new org.dbcbc.connector.dm.DmException(e);
        }
    }
}
