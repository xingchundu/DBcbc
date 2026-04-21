/**
 * Sample plugin: optional row cleanup when replicating Oracle → Oracle.
 * DBSyncer connectors perform read/write; this hook adjusts target rows before write.
 */
package org.dbsyncer.sample.plugin.oracle2oracle;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.PluginService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 上传到 DBSyncer {@code plugins/} 的独立 JAR（含 META-INF/services SPI）。
 * <p>
 * 构建：{@code mvn -pl dbsyncer-sample-plugin-oracle2oracle -am package -Dmaven.test.skip=true}<br>
 * 使用：上传 jar → 插件管理；在 Oracle→Oracle 驱动高级配置中绑定本插件。
 * <p>
 * 默认做 Oracle 侧常见规范化（空白、空串对数值/日期列转 null、0/1 标志转整数等）；业务逻辑在 {@link #transformRow(Map, List)} 扩展。
 */
public class OracleToOracleSyncPlugin implements PluginService {

    private static final Logger log = LoggerFactory.getLogger(OracleToOracleSyncPlugin.class);

    @Override
    public void init() {
        log.info("OracleToOracleSyncPlugin loaded, version {}", getVersion());
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
        context.setTerminated(false);

        List<Map> targetRows = context.getTargetList();
        if (targetRows == null || targetRows.isEmpty()) {
            return;
        }

        List<Field> targetFields = context.getTargetFields();
        for (Map row : targetRows) {
            normalizeRowForOracle(row, targetFields);
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
        log.info("OracleToOracleSyncPlugin closed");
    }

    @Override
    public String getName() {
        return "Oracle-Oracle-Sample";
    }

    @Override
    public String getVersion() {
        return "2.0.9";
    }

    /**
     * 按目标列（目标库 Oracle）类型做常见兼容处理。
     */
    protected void normalizeRowForOracle(Map<String, Object> row, List<Field> targetFields) {
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
                if (isVarcharFamily(type)) {
                    s = s.trim();
                    row.put(col, s);
                }
                // 映射异常或驱动把数字列读成空串时，按 NULL 写入 Oracle
                if (s.isEmpty() && isNumericOrDateType(type)) {
                    row.put(col, null);
                }
                continue;
            }

            if (v instanceof Boolean && isFlagNumericType(type)) {
                row.put(col, ((Boolean) v) ? 1 : 0);
            }
        }
    }

    /**
     * 在此编写业务：字段映射补偿、脱敏、默认值等。key 为映射中目标列 {@link Field#getName()}。
     */
    protected void transformRow(Map<String, Object> row, List<Field> targetFields) {
        // 示例：目标列统一大写（若目标表列为大写标识）
        // Object x = row.get("MY_COL");
    }

    private static boolean isVarcharFamily(String type) {
        return type.contains("VARCHAR")
                || type.contains("NVARCHAR")
                || type.contains("LONG")
                || "CLOB".equals(type)
                || "NCLOB".equals(type);
    }

    private static boolean isNumericOrDateType(String type) {
        return type.contains("NUMBER")
                || type.contains("NUMERIC")
                || type.contains("INTEGER")
                || type.contains("INT")
                || type.contains("FLOAT")
                || type.contains("DOUBLE")
                || type.contains("BINARY_FLOAT")
                || type.contains("BINARY_DOUBLE")
                || type.contains("DATE")
                || type.contains("TIMESTAMP")
                || type.contains("INTERVAL");
    }

    private static boolean isFlagNumericType(String type) {
        return type.contains("NUMBER") || type.contains("INTEGER") || type.contains("INT");
    }
}
