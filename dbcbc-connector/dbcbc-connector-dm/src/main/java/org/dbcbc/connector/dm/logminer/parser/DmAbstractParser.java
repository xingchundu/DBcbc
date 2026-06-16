/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.logminer.parser;

import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.oracle.logminer.parser.AbstractParser;
import org.dbcbc.sdk.model.Field;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 达梦 LogMiner 解析基类：列名大小写不敏感匹配。
 */
public abstract class DmAbstractParser extends AbstractParser {

    @Override
    public void findColumn(Expression expression) {
        if (expression == null) {
            return;
        }
        if (expression instanceof Parenthesis) {
            findColumn(((Parenthesis) expression).getExpression());
            return;
        }
        if (expression instanceof InExpression) {
            collectInExpression((InExpression) expression);
            return;
        }
        if (expression instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) expression;
            Column column = (Column) isNullExpression.getLeftExpression();
            columnMap.put(normalizeColumnName(column.getColumnName()), expression);
            return;
        }

        if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            if (binaryExpression.getLeftExpression() instanceof Column) {
                Column column = (Column) binaryExpression.getLeftExpression();
                columnMap.put(normalizeColumnName(column.getColumnName()), binaryExpression.getRightExpression());
                return;
            }
            findColumn(binaryExpression.getLeftExpression());
            findColumn(binaryExpression.getRightExpression());
        }
    }

    private void collectInExpression(InExpression inExpression) {
        if (!(inExpression.getLeftExpression() instanceof Column)) {
            return;
        }
        Column column = (Column) inExpression.getLeftExpression();
        Expression rightExpression = inExpression.getRightExpression();
        if (rightExpression instanceof ExpressionList) {
            ExpressionList<?> values = (ExpressionList<?>) rightExpression;
            if (!values.isEmpty()) {
                columnMap.put(normalizeColumnName(column.getColumnName()), values.get(0));
            }
            return;
        }
        columnMap.put(normalizeColumnName(column.getColumnName()), rightExpression);
    }

    protected List<Object> columnMapToDataIgnoreCase() {
        List<Object> data = new LinkedList<>();
        for (Field field : fields) {
            DmColumnValue columnValue = new DmColumnValue(findExpression(field.getName()));
            data.add(columnValue.asFieldValue(field));
        }
        return data;
    }

    protected String normalizeColumnName(String name) {
        if (name == null) {
            return StringUtil.EMPTY;
        }
        return StringUtil.replace(name, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY).toUpperCase();
    }

    private Expression findExpression(String fieldName) {
        if (fieldName == null) {
            return null;
        }
        Expression expression = columnMap.get(fieldName);
        if (expression != null) {
            return expression;
        }
        for (Map.Entry<String, Expression> entry : columnMap.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(fieldName)) {
                return entry.getValue();
            }
        }
        return null;
    }
}
