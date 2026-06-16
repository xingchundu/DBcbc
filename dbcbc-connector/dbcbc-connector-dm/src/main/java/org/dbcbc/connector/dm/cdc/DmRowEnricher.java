/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.cdc;

import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.sdk.connector.database.Database;
import org.dbcbc.sdk.connector.database.DatabaseConnectorInstance;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.util.PrimaryKeyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 达梦 LogMiner UPDATE redo 常只包含变更列，按主键回源库补全未出现在 redo 中的字段。
 */
final class DmRowEnricher {

    private static final Logger logger = LoggerFactory.getLogger(DmRowEnricher.class);

    private DmRowEnricher() {
    }

    static void fillMissingColumns(DatabaseConnectorInstance instance, Database database, String schema, String tableName, List<Field> fields,
            List<Object> row) {
        if (instance == null || database == null || CollectionUtils.isEmpty(fields) || CollectionUtils.isEmpty(row) || !containsNull(row)) {
            return;
        }
        List<Field> pkFields = PrimaryKeyUtil.findPrimaryKeyFields(fields);
        if (CollectionUtils.isEmpty(pkFields)) {
            return;
        }
        List<Object> pkArgs = new ArrayList<>(pkFields.size());
        for (Field pkField : pkFields) {
            Object pkValue = getValue(fields, row, pkField.getName());
            if (pkValue == null) {
                return;
            }
            pkArgs.add(pkValue);
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append(database.buildWithQuotation(fields.get(i).getName()));
        }
        sql.append(" FROM ").append(qualifyTable(database, schema, tableName)).append(" WHERE ");
        database.appendPrimaryKeys(sql, pkFields.stream().map(Field::getName).collect(Collectors.toList()));

        try {
            Map<String, Object> dbRow = instance.execute(template -> template.queryForMap(sql.toString(), pkArgs.toArray()));
            if (CollectionUtils.isEmpty(dbRow)) {
                logger.warn("Cannot enrich DM row, source row not found: {} pk={}", qualifyTable(database, schema, tableName), pkArgs);
                return;
            }
            for (int i = 0; i < fields.size(); i++) {
                if (row.get(i) != null) {
                    continue;
                }
                Object value = resolveColumnValue(dbRow, fields.get(i).getName());
                if (value != null) {
                    row.set(i, value);
                }
            }
        } catch (Exception e) {
            logger.warn("Enrich DM row failed: {} pk={}, {}", qualifyTable(database, schema, tableName), pkArgs, e.getMessage());
        }
    }

    private static Object resolveColumnValue(Map<String, Object> row, String columnName) {
        if (row.containsKey(columnName)) {
            return row.get(columnName);
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(columnName)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static Object getValue(List<Field> fields, List<Object> row, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equalsIgnoreCase(name)) {
                return row.get(i);
            }
        }
        return null;
    }

    private static boolean containsNull(List<Object> row) {
        for (Object value : row) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }

    private static String qualifyTable(Database database, String schema, String tableName) {
        if (StringUtil.isNotBlank(schema)) {
            return database.buildWithQuotation(schema) + "." + database.buildWithQuotation(tableName);
        }
        return database.buildWithQuotation(tableName);
    }
}
