/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.logminer.parser;

import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.sdk.model.Field;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Values;

import java.util.List;

/**
 * 达梦 LogMiner INSERT 解析：支持无列名 VALUES 写法及带 schema 的表名。
 */
public class DmInsertSql extends DmAbstractParser {

    private final Insert insert;

    public DmInsertSql(Insert insert, List<Field> fields) {
        this.insert = insert;
        setFields(fields);
    }

    @Override
    public List<Object> parseColumns() {
        ExpressionList<?> values = resolveExpressions(insert);
        if (values == null || values.isEmpty()) {
            throw new IllegalStateException("Unsupported DM insert redo sql: " + insert);
        }

        ExpressionList<Column> columns = insert.getColumns();
        if (CollectionUtils.isEmpty(columns)) {
            for (int i = 0; i < fields.size() && i < values.size(); i++) {
                columnMap.put(normalizeColumnName(fields.get(i).getName()), values.get(i));
            }
        } else {
            for (int i = 0; i < columns.size() && i < values.size(); i++) {
                columnMap.put(normalizeColumnName(columns.get(i).getColumnName()), values.get(i));
            }
        }
        return columnMapToDataIgnoreCase();
    }

    private ExpressionList<?> resolveExpressions(Insert insert) {
        if (insert.getValues() != null) {
            return insert.getValues().getExpressions();
        }
        if (insert.getSelect() instanceof Values) {
            return ((Values) insert.getSelect()).getExpressions();
        }
        return null;
    }
}
