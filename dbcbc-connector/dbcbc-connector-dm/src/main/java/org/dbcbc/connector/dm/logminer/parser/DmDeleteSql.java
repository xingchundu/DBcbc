/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.logminer.parser;

import org.dbcbc.sdk.model.Field;

import net.sf.jsqlparser.statement.delete.Delete;

import java.util.List;

/**
 * 达梦 LogMiner DELETE 解析：WHERE 列名大小写不敏感，支持括号与 DATE 字面量。
 */
public class DmDeleteSql extends DmAbstractParser {

    private final Delete delete;

    public DmDeleteSql(Delete delete, List<Field> fields) {
        this.delete = delete;
        setFields(fields);
    }

    @Override
    public List<Object> parseColumns() {
        findColumn(delete.getWhere());
        return columnMapToDataIgnoreCase();
    }
}
