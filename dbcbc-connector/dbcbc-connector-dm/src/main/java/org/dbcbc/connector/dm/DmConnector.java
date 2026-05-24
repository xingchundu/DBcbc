package org.dbcbc.connector.dm;

import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.dm.cdc.DmListener;
import org.dbcbc.connector.dm.schema.DmSchemaResolver;
import org.dbcbc.connector.dm.validator.DmConfigValidator;
import org.dbcbc.sdk.config.DatabaseConfig;
import org.dbcbc.sdk.config.SqlBuilderConfig;
import org.dbcbc.sdk.enums.SqlBuilderEnum;
import org.dbcbc.sdk.connector.ConfigValidator;
import org.dbcbc.sdk.connector.database.AbstractDatabaseConnector;
import org.dbcbc.sdk.connector.database.Database;
import org.dbcbc.sdk.connector.database.DatabaseConnectorInstance;
import org.dbcbc.sdk.constant.DatabaseConstant;
import org.dbcbc.sdk.enums.ListenerTypeEnum;
import org.dbcbc.sdk.listener.DatabaseQuartzListener;
import org.dbcbc.sdk.listener.Listener;
import org.dbcbc.sdk.model.PageSql;
import org.dbcbc.sdk.plugin.ReaderContext;
import org.dbcbc.sdk.schema.SchemaResolver;
import org.dbcbc.sdk.util.PrimaryKeyUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class DmConnector extends AbstractDatabaseConnector {

    private final String QUERY_SCHEMA = "SELECT USERNAME FROM ALL_USERS WHERE USERNAME NOT IN ('SYS','SYSDBA','SYSAUDITOR','SYSDBO','SYSJOB','SYSJOBMAIN','SYSSSO','SYSTS','CTISYS','DBMS_JOB') ORDER BY USERNAME";

    private final DmConfigValidator configValidator = new DmConfigValidator();
    private final DmSchemaResolver schemaResolver = new DmSchemaResolver();

    @Override
    public String getConnectorType() {
        return "DM";
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
            return new DmListener();
        }
        return null;
    }

    @Override
    public List<String> getSchemas(DatabaseConnectorInstance connectorInstance, String catalog) {
        return connectorInstance.execute(databaseTemplate -> databaseTemplate.queryForList(QUERY_SCHEMA, String.class));
    }

    @Override
    public String buildSqlWithQuotation() {
        return "\"";
    }

    @Override
    public String getQueryCountSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        String queryFilter = config.getQueryFilter();
        String query = "SELECT COUNT(*) FROM %s%s t %s";
        return String.format(query, config.getSchema(), database.buildWithQuotation(config.getTableName()), queryFilter);
    }

    @Override
    public String getPageSql(PageSql config) {
        StringBuilder sql = new StringBuilder();
        sql.append(config.getQuerySql());
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
        if (!PrimaryKeyUtil.isSupportedCursor(config.getFields())) {
            return StringUtil.EMPTY;
        }
        StringBuilder sql = new StringBuilder();
        sql.append(config.getQuerySql());
        buildCursorConditionAndOrderBy(sql, config);
        sql.append(" limit ?");
        return sql.toString();
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors || cursors.length == 0) {
            return new Object[]{pageSize};
        }
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{pageSize};
        }
        Object[] newCursors = new Object[cursorArgs.length + 1];
        System.arraycopy(cursorArgs, 0, newCursors, 0, cursorArgs.length);
        newCursors[cursorArgs.length] = pageSize;
        return newCursors;
    }

    @Override
    public String getValidationQuery() {
        return "select 1";
    }

    @Override
    protected String getCatalog(String database, Connection connection) {
        return null;
    }

    @Override
    protected String getSchema(String schema, Connection connection) throws SQLException {
        if (StringUtil.isBlank(schema)) {
            schema = connection.getSchema();
        }
        if (StringUtil.isNotBlank(schema)) {
            schema = schema.toUpperCase();
        }
        return schema;
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        StringBuilder url = new StringBuilder();
        url.append("jdbc:dm://").append(config.getHost()).append(":").append(config.getPort()).append("/").append(database);
        return url.toString();
    }

    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        // 达梦 JDBC 对 MERGE 批写入常返回影响行数 0，导致全量同步误判失败；全量 INSERT 使用标准 INSERT 语句
        return SqlBuilderEnum.INSERT.getSqlBuilder().buildSql(config);
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        MergeContext context = buildMergeContext(config);

        StringBuilder sql = new StringBuilder(config.getDatabase().generateUniqueCode());
        buildMergeHeader(sql, config, context);

        sql.append("WHEN MATCHED THEN UPDATE SET ");
        sql.append(StringUtil.join(context.updateSets, StringUtil.COMMA)).append(" ");

        buildInsertClause(sql, context);

        return sql.toString();
    }

    private MergeContext buildMergeContext(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        MergeContext context = new MergeContext();

        config.getFields().forEach(f -> {
            String fieldName = database.buildWithQuotation(f.getName());
            context.fieldNames.add(fieldName);

            List<String> fieldVs = new ArrayList<>();
            if (database.buildCustomValue(fieldVs, f)) {
                context.selectFields.add(fieldVs.get(0) + " AS " + fieldName);
            } else {
                context.selectFields.add("? AS " + fieldName);
            }

            if (f.isPk()) {
                context.pkFieldNames.add(fieldName);
            } else {
                context.updateSets.add(String.format("t.%s = s.%s", fieldName, fieldName));
            }
        });

        return context;
    }

    private void buildMergeHeader(StringBuilder sql, SqlBuilderConfig config, MergeContext context) {
        Database database = config.getDatabase();

        sql.append("MERGE INTO ").append(config.getSchema());
        sql.append(database.buildWithQuotation(config.getTableName())).append(" t ");

        sql.append("USING (SELECT ");
        sql.append(StringUtil.join(context.selectFields, StringUtil.COMMA));
        sql.append(" FROM DUAL) s ");

        sql.append("ON (");
        for (int i = 0; i < context.pkFieldNames.size(); i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            String pkFieldName = context.pkFieldNames.get(i);
            sql.append("t.").append(pkFieldName).append(" = s.").append(pkFieldName);
        }
        sql.append(") ");
    }

    private void buildInsertClause(StringBuilder sql, MergeContext context) {
        sql.append("WHEN NOT MATCHED THEN INSERT (");
        sql.append(StringUtil.join(context.fieldNames, StringUtil.COMMA)).append(") VALUES (");

        List<String> sFieldNames = new ArrayList<>();
        context.fieldNames.forEach(f -> sFieldNames.add("s." + f));
        sql.append(StringUtil.join(sFieldNames, StringUtil.COMMA)).append(")");
    }

    private static class MergeContext {
        List<String> fieldNames = new ArrayList<>();
        List<String> selectFields = new ArrayList<>();
        List<String> pkFieldNames = new ArrayList<>();
        List<String> updateSets = new ArrayList<>();
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }
}
