/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.logminer.parser;

import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.sdk.model.Field;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.util.List;

/**
 * 达梦 LogMiner UPDATE 解析：列名大小写不敏感，兼容 SET/WHERE 及 legacy columns 写法。
 */
public class DmUpdateSql extends DmAbstractParser {

    private final Update update;

    public DmUpdateSql(Update update, List<Field> fields) {
        this.update = update;
        setFields(fields);
    }

    @Override
    public List<Object> parseColumns() {
        findColumn(update.getWhere());
        collectUpdateSets();
        return columnMapToDataIgnoreCase();
    }

    private void collectUpdateSets() {
        List<UpdateSet> updateSets = update.getUpdateSets();
        if (!CollectionUtils.isEmpty(updateSets)) {
            for (UpdateSet updateSet : updateSets) {
                if (updateSet.getColumns() != null && updateSet.getValues() != null) {
                    for (int i = 0; i < updateSet.getColumns().size() && i < updateSet.getValues().size(); i++) {
                        putExpression(updateSet.getColumns().get(i), updateSet.getValues().get(i));
                    }
                } else if (updateSet.getColumns() != null && !updateSet.getColumns().isEmpty()) {
                    putExpression(updateSet.getColumns().get(0), updateSet.getValue(0));
                }
            }
            return;
        }
        List<Column> columns = update.getColumns();
        List<Expression> expressions = update.getExpressions();
        if (CollectionUtils.isEmpty(columns) || CollectionUtils.isEmpty(expressions)) {
            return;
        }
        for (int i = 0; i < columns.size() && i < expressions.size(); i++) {
            putExpression(columns.get(i), expressions.get(i));
        }
    }

    private void putExpression(Column column, Expression value) {
        if (column == null) {
            return;
        }
        columnMap.put(normalizeColumnName(column.getColumnName()), value);
    }
}
