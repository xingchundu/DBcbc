/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.connector.mysql;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbcbc.common.model.Result;
import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.mysql.cdc.MySQLListener;
import org.dbcbc.connector.mysql.schema.MySQLSchemaResolver;
import org.dbcbc.connector.mysql.storage.MySQLStorageService;
import org.dbcbc.connector.mysql.validator.MySQLConfigValidator;
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
import org.dbcbc.sdk.schema.SchemaResolver;
import org.dbcbc.sdk.storage.StorageService;
import org.dbcbc.sdk.util.PrimaryKeyUtil;

import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MySQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public final class MySQLConnector extends AbstractDatabaseConnector {

    private final MySQLConfigValidator configValidator = new MySQLConfigValidator();
    private final MySQLSchemaResolver schemaResolver = new MySQLSchemaResolver();
    private final Set<String> SYSTEM_DATABASES = Stream.of("information_schema", "mysql", "performance_schema", "sys").collect(Collectors.toSet());

    /**
     * 连接 extInfo 置为 true 时保留 Unicode 补充平面（emoji 等），不剥离；表列为 utf8mb4 时建议设 true。未置 true 时写入前会去掉 U+10000 以上码点，避免目标列为 MySQL 旧版 utf8(utf8mb3) 时 1366。
     */
    private static final String EXT_PRESERVE_4BYTE_CHARS = "preserve4byteChars";

    private static final String ZERO_DATE_TIME_BEHAVIOR = "zeroDateTimeBehavior";

    private static final String ZERO_DATE_TIME_CONVERT_TO_NULL = "convertToNull";

    @Override
    public String getConnectorType() {
        return "MySQL";
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
            return new MySQLListener();
        }
        return null;
    }

    @Override
    public List<String> getDatabases(DatabaseConnectorInstance connectorInstance) {
        return connectorInstance.execute(databaseTemplate-> {
            List<String> databases = databaseTemplate.queryForList("SHOW DATABASES", String.class);
            if (!CollectionUtils.isEmpty(databases)) {
                return databases.stream().filter(name->!SYSTEM_DATABASES.contains(name.toLowerCase())).collect(Collectors.toList());
            }
            return Collections.emptyList();
        });
    }

    @Override
    public StorageService getStorageService() {
        return new MySQLStorageService();
    }

    /**
     * 为未显式配置 utf8mb4 的持久化连接补齐会话字符集/排序规则，与 emoji 等 4 字节 UTF-8 兼容；目标列仍须为 utf8mb4 字符集，否则 1366 由库表侧解决。
     */
    @Override
    public ConnectorInstance<DatabaseConfig, Connection> connect(DatabaseConfig config, ConnectorServiceContext context) {
        return new DatabaseConnectorInstance(applyUtf8mb4ConnectionParams(config), context.getCatalog(), context.getSchema());
    }

    private static DatabaseConfig applyUtf8mb4ConnectionParams(DatabaseConfig config) {
        if (config == null) {
            return null;
        }
        String url = config.getUrl();
        Properties p = config.getProperties();
        boolean urlNeed = StringUtil.isNotBlank(url) && !url.contains("connectionCollation=");
        boolean propNeed = p == null || p.getProperty("connectionCollation") == null;
        if (!urlNeed && !propNeed) {
            return config;
        }
        Properties p2 = new Properties();
        if (p != null) {
            p2.putAll(p);
        }
        if (p2.getProperty("connectionCollation") == null) {
            p2.setProperty("connectionCollation", "utf8mb4_unicode_ci");
        }
        if (p2.getProperty(ZERO_DATE_TIME_BEHAVIOR) == null) {
            p2.setProperty(ZERO_DATE_TIME_BEHAVIOR, ZERO_DATE_TIME_CONVERT_TO_NULL);
        }
        String u = url;
        if (StringUtil.isNotBlank(u) && !u.contains("connectionCollation=")) {
            u = u + (u.contains("?") ? "&" : "?") + "useUnicode=true&connectionCollation=utf8mb4_unicode_ci";
        }
        if (StringUtil.isNotBlank(u) && !u.contains(ZERO_DATE_TIME_BEHAVIOR + "=")) {
            u = u + (u.contains("?") ? "&" : "?") + ZERO_DATE_TIME_BEHAVIOR + "=" + ZERO_DATE_TIME_CONVERT_TO_NULL;
        }
        DatabaseConfig c = new DatabaseConfig();
        c.setConnectorType(config.getConnectorType());
        c.setUrl(u);
        c.setProperties(p2);
        c.setExtInfo(config.getExtInfo());
        c.setDriverClassName(config.getDriverClassName());
        c.setHost(config.getHost());
        c.setPort(config.getPort());
        c.setUsername(config.getUsername());
        c.setPassword(config.getPassword());
        c.setMaxActive(config.getMaxActive());
        c.setKeepAlive(config.getKeepAlive());
        c.setDatabase(config.getDatabase());
        c.setServiceName(config.getServiceName());
        return c;
    }

    @Override
    public Result reader(DatabaseConnectorInstance connectorInstance, ReaderContext context) {
        boolean supportedCursor = context.isSupportedCursor() && context.getCursors() != null && context.getCursors().length > 0;
        String queryKey = supportedCursor ? ConnectorConstant.OPERTION_QUERY_CURSOR : ConnectorConstant.OPERTION_QUERY;
        final String querySql = context.getCommand().get(queryKey);
        Assert.hasText(querySql, "查询语句不能为空.");
        Collections.addAll(context.getArgs(), supportedCursor ? getPageCursorArgs(context) : getPageArgs(context));

        List<Map<String, Object>> list = connectorInstance.execute(databaseTemplate -> databaseTemplate.query(querySql,
                context.getArgs().toArray(), (rs, rowNum) -> mapRow(rs)));
        return new Result(list);
    }

    private Map<String, Object> mapRow(ResultSet rs) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String label = meta.getColumnLabel(i);
            row.put(label, readColumnValue(rs, i, meta.getColumnTypeName(i)));
        }
        return row;
    }

    /**
     * 非法日期（如 2022-00-03）经 JDBC 转 Date 时会抛 SQLException: MONTH，回退为字符串供后续映射处理。
     */
    private Object readColumnValue(ResultSet rs, int columnIndex, String typeName) throws SQLException {
        if (typeName == null) {
            return rs.getObject(columnIndex);
        }
        String upper = typeName.toUpperCase(Locale.ROOT);
        if (upper.contains("DATE") || upper.contains("TIME") || upper.equals("YEAR") || upper.equals("MONTH") || upper.equals("DAY")) {
            try {
                return rs.getObject(columnIndex);
            } catch (SQLException e) {
                return rs.getString(columnIndex);
            }
        }
        return rs.getObject(columnIndex);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Result writer(DatabaseConnectorInstance connectorInstance, PluginContext context) {
        if (!isPreserve4ByteChars(connectorInstance) && !CollectionUtils.isEmpty(context.getTargetList())) {
            for (Map row : context.getTargetList()) {
                removeSupplementaryPlaneStringsInRow(row);
            }
        }
        return super.writer(connectorInstance, context);
    }

    private static boolean isPreserve4ByteChars(DatabaseConnectorInstance connectorInstance) {
        DatabaseConfig c = connectorInstance == null ? null : connectorInstance.getConfig();
        if (c == null || c.getExtInfo() == null) {
            return false;
        }
        return "true".equalsIgnoreCase(c.getExtInfo().getProperty(EXT_PRESERVE_4BYTE_CHARS, "").trim());
    }

    @SuppressWarnings("rawtypes")
    private static void removeSupplementaryPlaneStringsInRow(Map row) {
        if (row == null || row.isEmpty()) {
            return;
        }
        for (Object k : row.keySet()) {
            Object v = row.get(k);
            if (v instanceof String) {
                String t = toBmpCodePointsOnly((String) v);
                if (!Objects.equals(t, v)) {
                    row.put(k, t);
                }
            }
        }
    }

    /**
     * 仅保留 U+0000..U+FFFF，避免写入 MySQL 旧版 utf8(utf8mb3) 列时 1366；目标列为 utf8mb4 时请在连接 extInfo 中设置 preserve4byteChars=true
     */
    private static String toBmpCodePointsOnly(String s) {
        if (s == null) {
            return null;
        }
        if (s.isEmpty() || s.length() == s.codePointCount(0, s.length())) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        s.codePoints().forEach(cp-> {
            if (cp <= 0xFFFF) {
                sb.appendCodePoint(cp);
            }
        });
        return sb.toString();
    }

    @Override
    public String generateUniqueCode() {
        return DatabaseConstant.DBS_UNIQUE_CODE;
    }

    @Override
    public String buildSqlWithQuotation() {
        return "`";
    }

    @Override
    public String getPageSql(PageSql config) {
        StringBuilder sql = new StringBuilder(config.getQuerySql());
        // 使用基类方法添加ORDER BY（按主键排序，保证分页一致性）
        appendOrderByPrimaryKeys(sql, config);
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
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
        sql.append(DatabaseConstant.MYSQL_PAGE_SQL);
        return sql.toString();
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors || cursors.length == 0) {
            return new Object[]{0, pageSize};
        }
        // 使用基类的公共方法构建游标条件参数
        Object[] cursorArgs = buildCursorArgs(cursors);
        if (cursorArgs == null) {
            return new Object[]{0, pageSize};
        }
        // MySQL需要OFFSET=0和LIMIT=pageSize参数
        Object[] newCursors = new Object[cursorArgs.length + 2];
        System.arraycopy(cursorArgs, 0, newCursors, 0, cursorArgs.length);
        newCursors[cursorArgs.length] = 0; // OFFSET
        newCursors[cursorArgs.length + 1] = pageSize; // LIMIT
        return newCursors;
    }

    @Override
    public String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();
        List<String> fs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        List<String> dfs = new ArrayList<>();
        fields.forEach(f-> {
            String name = database.buildWithQuotation(f.getName());
            fs.add(name);
            vs.add("?");
            if (!f.isPk()) {
                dfs.add(String.format("%s = VALUES(%s)", name, name));
            }
        });
        // 仅主键/唯一列时，非主键列集合为空，MySQL 不允许 ON DUPLICATE KEY UPDATE 后无列；用主键自赋值占位，语义等价于无业务更新
        if (CollectionUtils.isEmpty(dfs) && !fields.isEmpty()) {
            for (Field f : fields) {
                if (f.isPk()) {
                    String name = database.buildWithQuotation(f.getName());
                    dfs.add(String.format("%s = VALUES(%s)", name, name));
                }
            }
        }
        // Field#isPk 与元数据不一致时，以 SqlBuilderConfig 中的主键名为准
        if (CollectionUtils.isEmpty(dfs) && !fields.isEmpty() && !CollectionUtils.isEmpty(config.getPrimaryKeys())) {
            for (String pkName : config.getPrimaryKeys()) {
                for (Field f : fields) {
                    if (pkName != null && pkName.equalsIgnoreCase(f.getName())) {
                        String name = database.buildWithQuotation(f.getName());
                        dfs.add(String.format("%s = VALUES(%s)", name, name));
                    }
                }
            }
        }
        // 仍为空则对全部列作 VALUES 自赋（极端映射场景兜底）
        if (CollectionUtils.isEmpty(dfs) && !fields.isEmpty()) {
            for (Field f : fields) {
                String name = database.buildWithQuotation(f.getName());
                dfs.add(String.format("%s = VALUES(%s)", name, name));
            }
        }

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = buildTableName(config);
        String fieldNames = StringUtil.join(fs, StringUtil.COMMA);
        String values = StringUtil.join(vs, StringUtil.COMMA);
        String dupNames = StringUtil.join(dfs, StringUtil.COMMA);
        // 基于主键或唯一索引冲突时更新
        return String.format("%sINSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;", uniqueCode, table, fieldNames, values, dupNames);
    }

    @Override
    public String buildInsertSql(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        List<Field> fields = config.getFields();

        List<String> fs = new ArrayList<>();
        List<String> vs = new ArrayList<>();
        fields.forEach(f-> {
            fs.add(database.buildWithQuotation(f.getName()));
            vs.add("?");
        });

        String uniqueCode = database.generateUniqueCode();
        StringBuilder table = buildTableName(config);
        String fieldNames = StringUtil.join(fs, StringUtil.COMMA);
        String values = StringUtil.join(vs, StringUtil.COMMA);

        // 冲突时忽略插入，不进行任何操作
        return String.format("%sINSERT IGNORE INTO %s (%s) VALUES (%s)", uniqueCode, table, fieldNames, values);
    }

    private StringBuilder buildTableName(SqlBuilderConfig config) {
        Database database = config.getDatabase();
        StringBuilder table = new StringBuilder();
        table.append(config.getSchema());
        table.append(database.buildWithQuotation(config.getTableName()));
        return table;
    }

    @Override
    public SchemaResolver getSchemaResolver() {
        return schemaResolver;
    }

    @Override
    protected String getSchema(String schema, Connection connection) {
        return null;
    }

    @Override
    public String buildJdbcUrl(DatabaseConfig config, String database) {
        // 连接参数字段在 Properties，此处固定追加 utf8mb4，与 emoji 等 4 字节字符兼容
        StringBuilder url = new StringBuilder();
        url.append("jdbc:mysql://").append(config.getHost()).append(":").append(config.getPort());
        if (database != null && !database.trim().isEmpty()) {
            url.append("/").append(database);
        }
        url.append("?useUnicode=true&characterEncoding=utf-8&connectionCollation=utf8mb4_unicode_ci&")
                .append(ZERO_DATE_TIME_BEHAVIOR).append("=").append(ZERO_DATE_TIME_CONVERT_TO_NULL);
        return url.toString();
    }

    @Override
    public String buildAlterCatalog(DatabaseConnectorInstance connectorInstance, Alter alter) {
        // 目标数据库名
        String catalog = connectorInstance.getCatalog();
        catalog = buildWithQuotation(catalog);
        // 1. 生成基础 SQL
        String sql = alter.toString();

        // 如果目标库名不为空且当前 SQL 未包含该库名
        if (catalog != null && !sql.contains(catalog + ".")) {
            String tableName = alter.getTable().getName();

            // 正则解释：
            // (?i) : 忽略大小写
            // (ALTER\s+TABLE\s+) : 捕获组1，匹配 "ALTER TABLE " 及其后的空格
            // (?:`[^`]+`\.)? : 非捕获组，匹配可选的 "旧库名." (例如 `test`.)
            // (?:`)? : 匹配可选的起始反引号
            // \\Q...\\E : 匹配纯表名
            // (?:`)? : 匹配可选的结束反引号
            String regex = "(?i)(ALTER\\s+TABLE\\s+)(?:`[^`]+`\\.)?(?:`)?" + java.util.regex.Pattern.quote(tableName) + "(?:`)?";

            // 替换为：捕获组1 + 新库名 + . + 表名
            String replacement = "$1" + catalog + "." + tableName;
            return sql.replaceFirst(regex, replacement);
        }
        return sql;
    }
}