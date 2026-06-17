/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.connector.postgresql;

import org.dbcbc.common.model.Result;
import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.postgresql.cdc.PostgreSQLListener;
import org.dbcbc.connector.postgresql.schema.PostgreSQLSchemaResolver;
import org.dbcbc.connector.postgresql.validator.PostgreSQLConfigValidator;
import org.dbcbc.sdk.SdkException;
import org.dbcbc.sdk.config.DatabaseConfig;
import org.dbcbc.sdk.config.SqlBuilderConfig;
import org.dbcbc.sdk.connector.ConfigValidator;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.connector.ConnectorServiceContext;
import org.dbcbc.sdk.connector.database.AbstractDatabaseConnector;
import org.dbcbc.sdk.connector.database.Database;
import org.dbcbc.sdk.connector.database.DatabaseConnectorInstance;
import org.dbcbc.sdk.constant.ConnectorConstant;
import org.dbcbc.sdk.constant.DatabaseConstant;
import org.dbcbc.sdk.enums.ListenerTypeEnum;
import org.dbcbc.sdk.listener.DatabaseQuartzListener;
import org.dbcbc.sdk.listener.Listener;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.model.PageSql;
import org.dbcbc.sdk.plugin.PluginContext;
import org.dbcbc.sdk.plugin.ReaderContext;
import org.dbcbc.sdk.schema.CustomData;
import org.dbcbc.sdk.schema.SchemaResolver;
import org.dbcbc.sdk.util.PrimaryKeyUtil;

import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PostgreSQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class PostgreSQLConnector extends AbstractDatabaseConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String QUERY_DATABASE = "SELECT datname FROM pg_database WHERE datistemplate = FALSE order by datname";
    private final String QUERY_SCHEMA = "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '#' and schema_name NOT LIKE 'pg_%' AND schema_name not in('information_schema') order by schema_name";

    private final PostgreSQLConfigValidator configValidator = new PostgreSQLConfigValidator();
    private final PostgreSQLSchemaResolver schemaResolver = new PostgreSQLSchemaResolver();

    @Override
    public String getConnectorType() {
        return "PostgreSQL";
    }

    @Override
    public ConnectorInstance connect(DatabaseConfig config, ConnectorServiceContext context) {
        String catalog = context.getCatalog();
        String schema = context.getSchema();
        if (StringUtil.isNotBlank(catalog)) {
            DatabaseConfig effectiveConfig = copyDatabaseConfig(config);
            effectiveConfig.setDatabase(catalog);
            effectiveConfig.setUrl(resolveJdbcUrl(config, catalog));
            return new DatabaseConnectorInstance(effectiveConfig, catalog, schema);
        }
        return new DatabaseConnectorInstance(config, catalog, schema);
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new PostgreSQLListener();
        }
        return null;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate->databaseTemplate.queryForList(QUERY_DATABASE, String.class));
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return connectorInstance.execute(databaseTemplate->databaseTemplate.queryForList(QUERY_SCHEMA.replace("#", catalog), String.class));
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getPageSql(PageSql config) {
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类方法添加ORDER BY（按主键排序，保证分页一致性）
        appendOrderByPrimaryKeys(sql, config);
        sql.append(DatabaseConstant.POSTGRESQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public String getPageCursorSql(PageSql config) {
        // 不支持游标查询
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }

        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类的公共方法构建WHERE条件和ORDER BY
        buildCursorConditionAndOrderBy(sql, config);
        sql.append(DatabaseConstant.POSTGRESQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors || cursors.length == 0) {
            return new Object[]{pageSize, 0};
        }
        // 使用基类的公共方法构建游标条件参数
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{pageSize, 0};
        }

        // PostgreSQL使用 LIMIT ? OFFSET ?，参数顺序为 [游标参数..., pageSize, 0]
        Object[] newCursors = new Object[cursorArgs.length + 2];
        System.arraycopy(cursorArgs, 0, newCursors, 0, cursorArgs.length);
        newCursors[cursorArgs.length] = pageSize; // LIMIT
        newCursors[cursorArgs.length + 1] = 0; // OFFSET
        return newCursors;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    protected String getSchema(String schema, Connection connection) {
        return StringUtil.isNotBlank(schema) ? schema : "public";
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // jdbc:postgresql://127.0.0.1:5432/postgres
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://").append(config.getHost()).append(":").append(config.getPort()).append("/");
        if (StringUtil.isNotBlank(database)) {
            url.append(database);
        }
        return url.toString();
    }

    /**
     * PostgreSQL 需通过 JDBC URL 中的库名连接目标库，setCatalog 无法可靠切换物理库。
     */
    private String resolveJdbcUrl(DatabaseConfig config, String database) {
        if (StringUtil.isNotBlank(config.getHost()) && config.getPort() > 0) {
            return buildJdbcUrl(config, database);
        }
        String url = config.getUrl();
        if (StringUtil.isBlank(url) || StringUtil.isBlank(database)) {
            return url;
        }
        int slash = url.lastIndexOf('/');
        if (slash < 0) {
            return url;
        }
        int queryIndex = url.indexOf('?', slash);
        String suffix = queryIndex > 0 ? url.substring(queryIndex) : "";
        return url.substring(0, slash + 1) + database + suffix;
    }

    private DatabaseConfig copyDatabaseConfig(DatabaseConfig config) {
        DatabaseConfig copy = new DatabaseConfig();
        copy.setDriverClassName(config.getDriverClassName());
        copy.setHost(config.getHost());
        copy.setPort(config.getPort());
        copy.setUsername(config.getUsername());
        copy.setPassword(config.getPassword());
        copy.setMaxActive(config.getMaxActive());
        copy.setKeepAlive(config.getKeepAlive());
        copy.setDatabase(config.getDatabase());
        copy.setServiceName(config.getServiceName());
        copy.setUrl(config.getUrl());
        copy.setConnectorType(config.getConnectorType());
        copy.setProperties(config.getProperties());
        copy.setExtInfo(config.getExtInfo());
        return copy;
    }

    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        // PostgreSQL 使用 ON CONFLICT DO NOTHING 实现 INSERT IGNORE 效果
        UpsertContext context = buildUpsertContext(config);
        StringBuilder sql = new StringBuilder(config.getDatabase().generateUniqueCode());

        // 构建 INSERT INTO ... VALUES (...)
        buildInsertIntoClause(sql, config, context);

        // 构建 ON CONFLICT (...) DO NOTHING
        buildOnConflictClause(sql, context);
        sql.append(" DO NOTHING");

        return sql.toString();
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        UpsertContext context = buildUpsertContext(config);
        StringBuilder sql = new StringBuilder(config.getDatabase().generateUniqueCode());

        // 构建 INSERT INTO ... VALUES (...)
        buildInsertIntoClause(sql, config, context);

        // 构建 ON CONFLICT：若有非主键列则 DO UPDATE SET，否则 DO NOTHING（仅主键表如关联表无法用空 SET）
        buildOnConflictClause(sql, context);
        if (context.updateSets.isEmpty()) {
            sql.append(" DO NOTHING");
        } else {
            sql.append(" DO UPDATE SET ");
            sql.append(StringUtil.join(context.updateSets, StringUtil.COMMA));
        }

        return sql.toString();
    }

    /**
     * 开启覆盖同步时，UPDATE 事件若仅包含变更列，UPSERT 的 INSERT 阶段会因 NOT NULL 约束失败。
     * 对含空值的 UPDATE 行改为只更新非空列，避免把未变更列写成 NULL。
     */
    @Override
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        Result result;
        if (context.isForceUpdate() && isUpdate(context.getEvent()) && containsPartialUpdateRow(context)) {
            result = writePartialUpdate(connectorInstance, context);
        } else {
            result = super.writer(connectorInstance, context);
        }
        normalizeResultData(result);
        return result;
    }

    /**
     * 写入成功后持久化增量记录时，PGobject/PGpoint 等 JDBC 类型需转为可序列化值。
     */
    @SuppressWarnings("unchecked")
    private void normalizeResultData(Result result) {
        if (result == null) {
            return;
        }
        for (Object item : result.getSuccessData()) {
            if (item instanceof Map) {
                normalizeRowForStorage((Map) item);
            }
        }
        for (Object item : result.getFailData()) {
            if (item instanceof Map) {
                normalizeRowForStorage((Map) item);
            }
        }
    }

    private void normalizeRowForStorage(Map row) {
        if (CollectionUtils.isEmpty(row)) {
            return;
        }
        row.replaceAll((key, value) -> toStorableValue(value));
    }

    private Object toStorableValue(Object value) {
        if (value instanceof PGobject) {
            String text = ((PGobject) value).getValue();
            return text != null ? text : StringUtil.EMPTY;
        }
        if (value instanceof PGpoint) {
            PGpoint point = (PGpoint) value;
            return String.format("(%s,%s)", point.x, point.y);
        }
        return value;
    }

    private boolean containsPartialUpdateRow(PluginContext context) {
        List<Field> targetFields = context.getTargetFields();
        if (CollectionUtils.isEmpty(targetFields) || CollectionUtils.isEmpty(context.getTargetList())) {
            return false;
        }
        Set<String> pkNames = PrimaryKeyUtil.findPrimaryKeyFields(targetFields).stream().map(Field::getName).collect(Collectors.toSet());
        for (Map row : context.getTargetList()) {
            for (Field field : targetFields) {
                if (!isPrimaryKeyField(pkNames, field.getName()) && getRowValue(row, field.getName()) == null) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isPrimaryKeyField(Set<String> pkNames, String fieldName) {
        for (String pkName : pkNames) {
            if (pkName != null && pkName.equalsIgnoreCase(fieldName)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private Object getRowValue(Map row, String fieldName) {
        if (CollectionUtils.isEmpty(row) || StringUtil.isBlank(fieldName)) {
            return null;
        }
        if (row.containsKey(fieldName)) {
            return row.get(fieldName);
        }
        for (Object key : row.keySet()) {
            if (key instanceof String && ((String) key).equalsIgnoreCase(fieldName)) {
                return row.get(key);
            }
        }
        return null;
    }

    private Result writePartialUpdate(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        List<Map> data = context.getTargetList();
        List<Field> targetFields = context.getTargetFields();
        List<Field> pkFields = PrimaryKeyUtil.findExistPrimaryKeyFields(targetFields);
        Set<String> pkNames = pkFields.stream().map(Field::getName).collect(Collectors.toSet());
        String tableQualifier = resolveTableQualifier(context);
        String event = context.getEvent();
        Result result = new Result();

        for (Map row : data) {
            List<Field> setFields = new ArrayList<>();
            for (Field field : targetFields) {
                if (!isPrimaryKeyField(pkNames, field.getName()) && getRowValue(row, field.getName()) != null) {
                    setFields.add(field);
                }
            }
            if (setFields.isEmpty()) {
                result.getSuccessData().add(row);
                continue;
            }
            String executeSql = buildPartialUpdateSql(tableQualifier, setFields, pkFields);
            Object[] args = buildPartialUpdateArgs(row, setFields, pkFields);
            try {
                int affected = connectorInstance.execute(databaseTemplate->databaseTemplate.update(executeSql, args));
                if (affected == 0) {
                    throw new SdkException("数据不存在或执行异常");
                }
                result.getSuccessData().add(row);
                logPartialUpdate(context, event, row, true, null);
            } catch (Exception e) {
                result.getFailData().add(row);
                result.getError().append(context.getTraceId()).append(" SQL:").append(executeSql).append(System.lineSeparator()).append("ERROR:")
                        .append(e.getMessage()).append(System.lineSeparator());
                logPartialUpdate(context, event, row, false, e.getMessage());
            }
        }
        return result;
    }

    private String resolveTableQualifier(PluginContext context) {
        String upsertSql = context.getCommand().get(ConnectorConstant.OPERTION_UPSERT);
        if (StringUtil.isNotBlank(upsertSql)) {
            int start = upsertSql.indexOf("INSERT INTO ");
            if (start >= 0) {
                start += "INSERT INTO ".length();
                int end = upsertSql.indexOf('(', start);
                if (end > start) {
                    return upsertSql.substring(start, end).trim();
                }
            }
        }
        return buildWithQuotation(context.getTargetTable().getName());
    }

    private String buildPartialUpdateSql(String tableQualifier, List<Field> setFields, List<Field> pkFields) {
        StringBuilder sql = new StringBuilder(generateUniqueCode());
        sql.append("UPDATE ").append(tableQualifier).append(" SET ");
        List<String> sets = new ArrayList<>();
        for (Field field : setFields) {
            String fieldName = buildWithQuotation(field.getName());
            List<String> values = new ArrayList<>();
            if (buildCustomValue(values, field)) {
                sets.add(fieldName + "=" + values.get(0));
            } else {
                sets.add(fieldName + "=?");
            }
        }
        sql.append(StringUtil.join(sets, StringUtil.COMMA));
        sql.append(" WHERE ");
        appendPrimaryKeys(sql, pkFields.stream().map(Field::getName).collect(Collectors.toList()));
        return sql.toString();
    }

    private Object[] buildPartialUpdateArgs(Map row, List<Field> setFields, List<Field> pkFields) {
        List<Object> args = new ArrayList<>();
        for (Field field : setFields) {
            appendFieldArg(args, row, field);
        }
        for (Field field : pkFields) {
            appendFieldArg(args, row, field);
        }
        return args.toArray();
    }

    private void appendFieldArg(List<Object> args, Map row, Field field) {
        Object value = getRowValue(row, field.getName());
        if (value instanceof CustomData) {
            args.addAll(((CustomData) value).apply());
            return;
        }
        args.add(value);
    }

    private void logPartialUpdate(PluginContext context, String event, Map row, boolean success, String message) {
        if (success) {
            if (context.isEnablePrintTraceInfo()) {
                logger.info("{} {}表事件{}, 执行{}成功, {}", context.getTraceId(), context.getTargetTable().getName(), context.getEvent(), event, row);
            }
            return;
        }
        logger.error("{} {}表事件{}, 执行{}失败:{}, DATA:{}", context.getTraceId(), context.getTargetTable().getName(), context.getEvent(), event, message,
                row);
    }

    /**
     * 构建 INSERT INTO ... VALUES (...) 子句
     */
    private void buildInsertIntoClause(StringBuilder sql, SqlBuilderConfig config, UpsertContext context) {
        sql.append("INSERT INTO ").append(config.getSchema());
        sql.append(config.getDatabase().buildWithQuotation(config.getTableName()));
        sql.append("(").append(StringUtil.join(context.fieldNames, StringUtil.COMMA)).append(") ");
        sql.append("OVERRIDING SYSTEM VALUE VALUES (").append(StringUtil.join(context.valuePlaceholders, StringUtil.COMMA)).append(")");
    }

    /**
     * 构建 ON CONFLICT (...) 子句
     */
    private void buildOnConflictClause(StringBuilder sql, UpsertContext context) {
        sql.append(" ON CONFLICT (");
        sql.append(StringUtil.join(context.pkFieldNames, StringUtil.COMMA));
        sql.append(")");
    }

    /**
     * 构建 UPSERT 上下文（字段、主键等信息）
     */
    private UpsertContext buildUpsertContext(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        UpsertContext context = new UpsertContext();

        config.getFields().forEach(f-> {
            String fieldName = database.buildWithQuotation(f.getName());
            context.fieldNames.add(fieldName);

            // 构建 VALUES 占位符
            List<String> fieldVs = new ArrayList<>();
            if (database.buildCustomValue(fieldVs, f)) {
                // 自定义值表达式（如 geometry 类型）
                context.valuePlaceholders.add(fieldVs.get(0));
            } else {
                context.valuePlaceholders.add("?");
            }

            if (f.isPk()) {
                context.pkFieldNames.add(fieldName);
            } else {
                // UPDATE SET fieldName = EXCLUDED.fieldName
                context.updateSets.add(String.format("%s = EXCLUDED.%s", fieldName, fieldName));
            }
        });

        return context;
    }

    /**
     * UPSERT 语句构建上下文
     */
    private static class UpsertContext {

        List<String> fieldNames = new ArrayList<>();
        List<String> valuePlaceholders = new ArrayList<>();
        List<String> pkFieldNames = new ArrayList<>();
        List<String> updateSets = new ArrayList<>();
    }

}