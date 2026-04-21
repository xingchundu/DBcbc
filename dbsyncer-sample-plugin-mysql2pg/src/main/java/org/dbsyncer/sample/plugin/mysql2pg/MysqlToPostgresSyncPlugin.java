/**
 * Sample plugin: optional row cleanup when replicating MySQL → PostgreSQL.
 * DBSyncer already reads/writes via connectors; this hook adjusts target rows before write.
 */
package org.dbsyncer.sample.plugin.mysql2pg;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.PluginService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 上传到 DBSyncer {@code plugins/} 目录的独立 JAR（需含 META-INF/services SPI）。
 * <p>
 * 使用步骤：1）{@code mvn -pl dbsyncer-sample-plugin-mysql2pg package}；2）将生成的 jar 上传到「插件管理」；
 * 3）在 MySQL→PG 的驱动「高级配置」中绑定本插件。
 * <p>
 * 默认只做通用规范化（空串转 NULL、整型 0/1 转布尔等）；业务字段请在 {@link #transformRow(Map, List)} 中扩展。
 */
public class MysqlToPostgresSyncPlugin implements PluginService {

    private static final Logger log = LoggerFactory.getLogger(MysqlToPostgresSyncPlugin.class);

    @Override
    public void init() {
        log.info("MysqlToPostgresSyncPlugin loaded, version {}", getVersion());
    }

    @Override
    public void postProcessBefore(PluginContext context) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "before convert: model={} event={} source={} target={} batchSize={}",
                    context.getModelEnum(),
                    context.getEvent(),
                    context.getSourceTable() != null ? context.getSourceTable().getName() : "",
                    context.getTargetTable() != null ? context.getTargetTable().getName() : "",
                    context.getBatchSize());
        }
    }

    @Override
    public void convert(PluginContext context) {
        // 必须为 false，否则本批数据不会写入目标库（与内置 Demo 不同）
        context.setTerminated(false);

        List<Map> targetRows = context.getTargetList();
        if (targetRows == null || targetRows.isEmpty()) {
            return;
        }

        List<Field> targetFields = context.getTargetFields();
        for (Map row : targetRows) {
            normalizeRowForPostgres(row, targetFields);
            transformRow(row, targetFields);
        }
        context.setTargetList(targetRows);
    }

    @Override
    public void postProcessAfter(PluginContext context) {
        List<Map> list = context.getTargetList();
        int n = list != null ? list.size() : 0;
        log.info(
                "after convert: targetTable={} event={} rows={}",
                context.getTargetTable() != null ? context.getTargetTable().getName() : "",
                context.getEvent(),
                n);
    }

    @Override
    public void close() {
        log.info("MysqlToPostgresSyncPlugin closed");
    }

    @Override
    public String getName() {
        return "MySQL-PostgreSQL-Sample";
    }

    @Override
    public String getVersion() {
        return "2.0.9";
    }

    /**
     * 按目标列（PostgreSQL）类型做常见兼容处理。
     */
    protected void normalizeRowForPostgres(Map<String, Object> row, List<Field> targetFields) {
        if (row == null || targetFields == null) {
            return;
        }
        for (Field f : targetFields) {
            if (f == null) {
                continue;
            }
            String col = f.getName();
            if (!row.containsKey(col)) {
                continue;
            }
            Object v = row.get(col);
            String type = f.getTypeName() != null ? f.getTypeName().toUpperCase(Locale.ROOT) : "";

            if (v instanceof String) {
                String s = (String) v;
                if (s.isEmpty()) {
                    if (isNumericOrMoney(type) || isBooleanType(type)) {
                        row.put(col, null);
                    }
                    continue;
                }
                if (isBooleanType(type) && isZeroOneString(s)) {
                    row.put(col, "1".equals(s) || "true".equalsIgnoreCase(s));
                }
                continue;
            }

            if (v instanceof Number && isBooleanType(type)) {
                int n = ((Number) v).intValue();
                if (n == 0 || n == 1) {
                    row.put(col, n != 0);
                }
            }
        }
    }

    /**
     * 子类或复制此类后在此编写业务：改值、删字段、按源行 context.getSourceList() 对应下标处理等。
     */
    protected void transformRow(Map<String, Object> row, List<Field> targetFields) {
        // 示例：给某列加前缀（取消注释并改成你的列名）
        // row.put("your_column", "PG:" + row.get("your_column"));
    }

    private static boolean isNumericOrMoney(String type) {
        return type.contains("INT")
                || type.contains("NUMERIC")
                || type.contains("DECIMAL")
                || type.contains("FLOAT")
                || type.contains("DOUBLE")
                || type.contains("REAL")
                || type.contains("MONEY")
                || type.contains("SERIAL")
                || "SMALLINT".equals(type);
    }

    private static boolean isBooleanType(String type) {
        return type.contains("BOOL") || type.contains("BIT");
    }

    private static boolean isZeroOneString(String s) {
        return "0".equals(s) || "1".equals(s) || "true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s);
    }
}
