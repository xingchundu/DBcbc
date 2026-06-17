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
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        List<Field> lookupFields = resolveLookupFields(fields, row);
        if (CollectionUtils.isEmpty(lookupFields)) {
            logger.debug("Skip enrich, no lookup key available: {} pk={}", qualifyTable(database, schema, tableName), row);
            return;
        }
        List<Object> lookupArgs = new ArrayList<>(lookupFields.size());
        for (Field lookupField : lookupFields) {
            lookupArgs.add(getValue(fields, row, lookupField.getName()));
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append(database.buildWithQuotation(fields.get(i).getName()));
        }
        sql.append(" FROM ").append(qualifyTable(database, schema, tableName)).append(" WHERE ");
        appendLookupCondition(database, sql, lookupFields);

        try {
            Map<String, Object> dbRow = instance.execute(template -> template.queryForMap(sql.toString(), lookupArgs.toArray()));
            if (CollectionUtils.isEmpty(dbRow)) {
                logger.warn("Cannot enrich DM row, source row not found: {} lookup={}", qualifyTable(database, schema, tableName), lookupArgs);
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
            logger.warn("Enrich DM row failed: {} lookup={}, {}", qualifyTable(database, schema, tableName), lookupArgs, e.getMessage());
        }
    }

    /**
     * 达梦 redo 可能只带部分主键列，优先用已有主键值回源补全；无主键标记时退化为首个非空列。
     */
    private static List<Field> resolveLookupFields(List<Field> fields, List<Object> row) {
        List<Field> pkFields = PrimaryKeyUtil.findPrimaryKeyFields(fields);
        if (!CollectionUtils.isEmpty(pkFields)) {
            List<Field> available = new ArrayList<>();
            for (Field pkField : pkFields) {
                Object pkValue = getValue(fields, row, pkField.getName());
                if (pkValue != null) {
                    available.add(pkField);
                }
            }
            return available;
        }
        for (Field field : fields) {
            if (getValue(fields, row, field.getName()) != null) {
                return Collections.singletonList(field);
            }
        }
        return Collections.emptyList();
    }

    private static void appendLookupCondition(Database database, StringBuilder sql, List<Field> lookupFields) {
        for (int i = 0; i < lookupFields.size(); i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append(database.buildWithQuotation(lookupFields.get(i).getName())).append("=?");
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
