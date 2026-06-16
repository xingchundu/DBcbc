/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.logminer.parser;

import org.dbcbc.connector.dm.schema.support.DmDateParseUtil;
import org.dbcbc.connector.oracle.logminer.parser.OracleColumnValue;
import org.dbcbc.sdk.model.Field;

import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import oracle.jdbc.OracleTypes;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * 达梦 LogMiner 字段值解析：补齐 DATE 列及 redo 中常见日期字面量。
 */
public class DmColumnValue extends OracleColumnValue {

    public DmColumnValue(Expression value) {
        super(value);
    }

    public Object asFieldValue(Field field) {
        if (isNull()) {
            return null;
        }
        switch (field.getType()) {
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return asBigDecimal();
            case Types.DATE:
                return asSqlDate();
            case Types.TIME:
            case Types.TIMESTAMP:
                return asSqlTimestamp();
            case OracleTypes.TIMESTAMPTZ:
                return asOffsetDateTime();
            default:
                return asString();
        }
    }

    public Date asSqlDate() {
        Timestamp timestamp = asSqlTimestamp();
        return timestamp == null ? null : new Date(timestamp.getTime());
    }

    public Timestamp asSqlTimestamp() {
        Timestamp timestamp = asTimestamp();
        if (timestamp != null) {
            return timestamp;
        }
        return DmDateParseUtil.parseTimestamp(asString());
    }

    @Override
    public Timestamp asTimestamp() {
        Expression expression = getValue();
        if (expression instanceof DateTimeLiteralExpression) {
            return parseDateTimeLiteral((DateTimeLiteralExpression) expression);
        }
        if (expression instanceof Function) {
            Timestamp timestamp = super.asTimestamp();
            if (timestamp != null) {
                return timestamp;
            }
        }
        if (expression instanceof StringValue) {
            return DmDateParseUtil.parseTimestamp(((StringValue) expression).getValue());
        }
        return DmDateParseUtil.parseTimestamp(asString());
    }

    private Timestamp parseDateTimeLiteral(DateTimeLiteralExpression literal) {
        String value = literal.getValue();
        if (value != null && value.length() >= 2 && value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
            value = value.substring(1, value.length() - 1);
        }
        return DmDateParseUtil.parseTimestamp(value);
    }
}
