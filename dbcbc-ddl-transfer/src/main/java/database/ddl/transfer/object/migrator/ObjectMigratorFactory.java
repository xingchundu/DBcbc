package database.ddl.transfer.object.migrator;

import database.ddl.transfer.object.migrator.impl.Oracle2DmObjectMigrator;
import database.ddl.transfer.object.migrator.impl.Oracle2MySQLObjectMigrator;
import database.ddl.transfer.object.migrator.impl.Oracle2PostgreSQLObjectMigrator;
import database.ddl.transfer.object.migrator.impl.SqlServer2PostgreSQLObjectMigrator;

import java.sql.Connection;
import java.util.Locale;

/**
 * 根据源库 → 目标库类型选择正确的 ObjectMigrator
 *
 * 支持路径：
 * <ul>
 *   <li>Oracle  → PostgreSQL
 *   <li>Oracle  → DM (达梦)
 *   <li>Oracle  → MySQL
 *   <li>SqlServer → PostgreSQL
 * </ul>
 */
public final class ObjectMigratorFactory {

    private ObjectMigratorFactory() {}

    public static AbstractObjectMigrator getInstance(
            Connection sourceConn, Connection targetConn) {
        try {
            String srcType = dbType(sourceConn);
            String tgtType = dbType(targetConn);

            if (srcType.contains("oracle")) {
                if (tgtType.contains("postgres")) {
                    return new Oracle2PostgreSQLObjectMigrator(targetConn);
                }
                if (tgtType.contains("dm") || tgtType.contains("dameng")) {
                    return new Oracle2DmObjectMigrator(targetConn);
                }
                if (tgtType.contains("mysql")) {
                    return new Oracle2MySQLObjectMigrator(targetConn);
                }
            }
            if (srcType.contains("microsoft sql server") || srcType.contains("sql server")) {
                if (tgtType.contains("postgres")) {
                    return new SqlServer2PostgreSQLObjectMigrator(targetConn);
                }
            }
            // DM 作为源库：与 Oracle 完全兼容，目标为 Oracle/DM 则直接执行
            if (srcType.contains("dm") || srcType.contains("dameng")) {
                if (tgtType.contains("postgres")) {
                    return new Oracle2PostgreSQLObjectMigrator(targetConn);
                }
                if (tgtType.contains("mysql")) {
                    return new Oracle2MySQLObjectMigrator(targetConn);
                }
                return new Oracle2DmObjectMigrator(targetConn); // 同库或到 Oracle
            }
            throw new IllegalArgumentException(
                    "暂不支持的对象迁移路径：" + srcType + " → " + tgtType);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("创建 ObjectMigrator 失败", e);
        }
    }

    private static String dbType(Connection conn) {
        try {
            String n = conn.getMetaData().getDatabaseProductName();
            return n == null ? "" : n.toLowerCase(Locale.ROOT);
        } catch (Exception e) {
            return "";
        }
    }
}
