/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.mysql.schema.support;

import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.support.IntType;

import java.sql.Date;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-26 22:59
 */
public final class MySQLIntType extends IntType {

    private enum TypeEnum {

        SMALLINT_UNSIGNED("SMALLINT UNSIGNED"), MEDIUMINT("MEDIUMINT"), MEDIUMINT_UNSIGNED("MEDIUMINT UNSIGNED"), INT("INT"), INTEGER("INTEGER"),
        YEAR("YEAR"), MONTH("MONTH"), DAY("DAY");

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
        if (val instanceof Date) {
            Date d = (Date) val;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d);
            String typeName = field.getTypeName();
            if (typeName != null && "MONTH".equalsIgnoreCase(typeName)) {
                return calendar.get(Calendar.MONTH) + 1;
            }
            if (typeName != null && "DAY".equalsIgnoreCase(typeName)) {
                return calendar.get(Calendar.DAY_OF_MONTH);
            }
            return calendar.get(Calendar.YEAR);
        }
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        if (val instanceof String) {
            String s = ((String) val).trim();
            if (s.isEmpty()) {
                return null;
            }
            return Integer.parseInt(s);
        }
        return throwUnsupportedException(val, field);
    }

}