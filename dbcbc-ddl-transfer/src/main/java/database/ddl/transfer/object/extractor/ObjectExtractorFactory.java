package database.ddl.transfer.object.extractor;

import database.ddl.transfer.object.extractor.impl.OracleObjectExtractor;
import database.ddl.transfer.object.extractor.impl.SqlServerObjectExtractor;

import java.sql.Connection;
import java.util.Locale;

/**
 * 根据数据库类型返回对应的 ObjectExtractor
 */
public final class ObjectExtractorFactory {

    private ObjectExtractorFactory() {}

    public static AbstractObjectExtractor getInstance(Connection connection) {
        try {
            String dbName = connection.getMetaData().getDatabaseProductName();
            String lower  = dbName == null ? "" : dbName.toLowerCase(Locale.ROOT);
            if (lower.contains("oracle")) {
                return new OracleObjectExtractor(connection);
            }
            if (lower.contains("microsoft sql server") || lower.contains("sql server")) {
                return new SqlServerObjectExtractor(connection);
            }
            // DM 高度兼容 Oracle，复用 Oracle 提取器
            if (lower.contains("dm") || lower.contains("dameng")) {
                return new OracleObjectExtractor(connection);
            }
            throw new IllegalArgumentException(
                    "暂不支持从该数据库提取对象（仅支持 Oracle/DM/SqlServer 作为源库）：" + dbName);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("创建 ObjectExtractor 失败", e);
        }
    }
}
