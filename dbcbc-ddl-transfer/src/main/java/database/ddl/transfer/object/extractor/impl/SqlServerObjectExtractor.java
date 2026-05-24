package database.ddl.transfer.object.extractor.impl;

import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.DbObjectType;
import database.ddl.transfer.object.extractor.AbstractObjectExtractor;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL Server 对象提取器 —— 使用 sys 视图 + OBJECT_DEFINITION()
 */
public class SqlServerObjectExtractor extends AbstractObjectExtractor {

    public SqlServerObjectExtractor(Connection connection) {
        super(connection);
    }

    /** 通用：通过 OBJECT_DEFINITION 提取指定 type 的对象 */
    private List<DbObject> extractByObjectType(DbObjectType dbObjType, String... sqlServerTypes) {
        List<DbObject> result = new ArrayList<>();
        for (String sysType : sqlServerTypes) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                String sql = "SELECT o.name, OBJECT_DEFINITION(o.object_id) AS ddl " +
                             "FROM sys.objects o " +
                             "WHERE o.type = ? AND SCHEMA_NAME(o.schema_id) = SCHEMA_NAME() " +
                             "ORDER BY o.name";
                ps = connection.prepareStatement(sql);
                ps.setString(1, sysType);
                rs = ps.executeQuery();
                while (rs.next()) {
                    String name = rs.getString("name");
                    String ddl  = rs.getString("ddl");
                    if (ddl != null && !ddl.trim().isEmpty()) {
                        result.add(new DbObject(dbObjType, name, ddl.trim()));
                    }
                }
            } catch (Exception e) {
                logger.warn("提取 SQL Server 对象类型 {} 失败：{}", sysType, e.getMessage());
            } finally {
                close(ps, rs);
            }
        }
        return result;
    }

    @Override
    protected List<DbObject> extractProcedures() {
        return extractByObjectType(DbObjectType.PROCEDURE, "P");
    }

    @Override
    protected List<DbObject> extractFunctions() {
        // FN=标量函数, TF=表值函数, IF=内联表值函数
        return extractByObjectType(DbObjectType.FUNCTION, "FN", "TF", "IF");
    }

    @Override
    protected List<DbObject> extractTriggers() {
        return extractByObjectType(DbObjectType.TRIGGER, "TR");
    }

    @Override
    protected List<DbObject> extractViews() {
        return extractByObjectType(DbObjectType.VIEW, "V");
    }

    @Override
    protected List<DbObject> extractSequences() {
        List<DbObject> result = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(
                    "SELECT name, start_value, increment, minimum_value, maximum_value, is_cycling, cache_size " +
                    "FROM sys.sequences WHERE schema_id = SCHEMA_ID() ORDER BY name");
            rs = ps.executeQuery();
            while (rs.next()) {
                String name  = rs.getString("name");
                long   start = rs.getLong("start_value");
                long   incr  = rs.getLong("increment");
                long   minV  = rs.getLong("minimum_value");
                long   maxV  = rs.getLong("maximum_value");
                boolean cycle = rs.getBoolean("is_cycling");
                long   cache = rs.getLong("cache_size");
                String ddl = String.format(
                        "CREATE SEQUENCE [%s]\n  AS BIGINT\n  START WITH %d\n  INCREMENT BY %d" +
                        "\n  MINVALUE %d\n  MAXVALUE %d\n  %s\n  CACHE %d",
                        name, start, incr, minV, maxV, cycle ? "CYCLE" : "NO CYCLE", cache);
                result.add(new DbObject(DbObjectType.SEQUENCE, name, ddl));
            }
        } catch (Exception e) {
            logger.warn("提取 SQL Server 序列失败：{}", e.getMessage());
        } finally {
            close(ps, rs);
        }
        return result;
    }

    @Override
    protected List<DbObject> extractJobs() {
        List<DbObject> result = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(
                    "SELECT j.name, j.description, j.enabled " +
                    "FROM msdb.dbo.sysjobs j ORDER BY j.name");
            rs = ps.executeQuery();
            while (rs.next()) {
                String name = rs.getString("name");
                String ddl = "-- [SQL Server Agent Job] " + name + "\n" +
                             "-- Description: " + rs.getString("description") + "\n" +
                             "-- Enabled: " + rs.getInt("enabled") + "\n" +
                             "-- NOTE: 请在目标库中手动创建对应调度任务";
                result.add(new DbObject(DbObjectType.JOB, name, ddl));
            }
        } catch (Exception e) {
            logger.warn("提取 SQL Server Job 失败（可能无 msdb 权限）：{}", e.getMessage());
        } finally {
            close(ps, rs);
        }
        return result;
    }

    @Override
    protected List<DbObject> extractRoleGrants() {
        List<DbObject> result = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(
                    "SELECT dp.name AS role_name " +
                    "FROM sys.database_role_members drm " +
                    "JOIN sys.database_principals dp ON dp.principal_id = drm.role_principal_id " +
                    "JOIN sys.database_principals mp ON mp.principal_id = drm.member_principal_id " +
                    "WHERE mp.name = USER_NAME() ORDER BY dp.name");
            rs = ps.executeQuery();
            StringBuilder ddl = new StringBuilder("-- [SQL Server 角色授权]\n");
            while (rs.next()) {
                ddl.append("-- EXEC sp_addrolemember '")
                   .append(rs.getString("role_name")).append("', '<目标用户>';\n");
            }
            ddl.append("-- NOTE: 请在目标库中手动执行上述授权语句");
            result.add(new DbObject(DbObjectType.ROLE_GRANT, "user_roles", ddl.toString()));
        } catch (Exception e) {
            logger.warn("提取 SQL Server 权限/角色失败：{}", e.getMessage());
        } finally {
            close(ps, rs);
        }
        return result;
    }
}
