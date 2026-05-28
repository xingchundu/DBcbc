package database.ddl.transfer.object.extractor.impl;

import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.DbObjectType;
import database.ddl.transfer.object.extractor.AbstractObjectExtractor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Oracle 对象提取器 —— 利用 DBMS_METADATA.GET_DDL 和数据字典视图提取 DDL
 */
public class OracleObjectExtractor extends AbstractObjectExtractor {

    public OracleObjectExtractor(Connection connection) {
        super(connection);
    }

    // ─── 通用：通过 DBMS_METADATA 提取 ───

    private List<DbObject> extractViaMetadata(DbObjectType type, String oracleTypeName, String listSql) {
        List<DbObject> result = new ArrayList<>();
        List<String> names = queryNames(listSql);
        for (String name : names) {
            String ddl = querySingleString(
                    "SELECT DBMS_METADATA.GET_DDL(?,?) FROM DUAL", oracleTypeName, name);
            if (ddl != null && !ddl.trim().isEmpty()) {
                result.add(new DbObject(type, name.toLowerCase(), ddl.trim()));
            } else {
                logger.warn("    DBMS_METADATA 返回空 DDL：[{}] {}", oracleTypeName, name);
            }
        }
        return result;
    }

    @Override
    protected List<DbObject> extractProcedures() {
        return extractViaMetadata(DbObjectType.PROCEDURE, "PROCEDURE",
                "SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE='PROCEDURE' ORDER BY OBJECT_NAME");
    }

    @Override
    protected List<DbObject> extractFunctions() {
        return extractViaMetadata(DbObjectType.FUNCTION, "FUNCTION",
                "SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE='FUNCTION' ORDER BY OBJECT_NAME");
    }

    @Override
    protected List<DbObject> extractTriggers() {
        return extractViaMetadata(DbObjectType.TRIGGER, "TRIGGER",
                "SELECT TRIGGER_NAME FROM USER_TRIGGERS ORDER BY TRIGGER_NAME");
    }

    @Override
    protected List<DbObject> extractPackages() {
        // 提取 PACKAGE 规格 + PACKAGE BODY
        List<DbObject> specs = extractViaMetadata(DbObjectType.PACKAGE, "PACKAGE",
                "SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE='PACKAGE' ORDER BY OBJECT_NAME");
        List<DbObject> bodies = extractViaMetadata(DbObjectType.PACKAGE, "PACKAGE_BODY",
                "SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE='PACKAGE BODY' ORDER BY OBJECT_NAME");
        // 将 BODY 附加到对应 SPEC 的 extra 字段
        for (DbObject body : bodies) {
            String specName = body.getName();
            boolean matched = false;
            for (DbObject spec : specs) {
                if (spec.getName().equalsIgnoreCase(specName)) {
                    spec.setExtra(body.getDdl());
                    matched = true;
                    break;
                }
            }
            if (!matched) specs.add(body); // 独立的 BODY
        }
        return specs;
    }

    @Override
    protected List<DbObject> extractViews() {
        List<DbObject> result = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(
                    "SELECT VIEW_NAME, TEXT FROM USER_VIEWS ORDER BY VIEW_NAME");
            rs = ps.executeQuery();
            while (rs.next()) {
                String name = rs.getString("VIEW_NAME");
                String text = rs.getString("TEXT");
                if (text != null && !text.trim().isEmpty()) {
                    String ddl = "CREATE OR REPLACE VIEW " + name + " AS\n" + text.trim();
                    result.add(new DbObject(DbObjectType.VIEW, name.toLowerCase(), ddl));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("提取 Oracle 视图失败", e);
        } finally {
            close(ps, rs);
        }
        return result;
    }

    @Override
    protected List<DbObject> extractSynonyms() {
        List<DbObject> result = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(
                    "SELECT SYNONYM_NAME, TABLE_OWNER, TABLE_NAME, DB_LINK FROM USER_SYNONYMS ORDER BY SYNONYM_NAME");
            rs = ps.executeQuery();
            while (rs.next()) {
                String synName  = rs.getString("SYNONYM_NAME");
                String owner    = rs.getString("TABLE_OWNER");
                String tblName  = rs.getString("TABLE_NAME");
                String dbLink   = rs.getString("DB_LINK");
                StringBuilder ddl = new StringBuilder("CREATE OR REPLACE SYNONYM ")
                        .append(synName).append(" FOR ");
                if (owner != null && !owner.trim().isEmpty()) {
                    ddl.append(owner.trim()).append(".");
                }
                ddl.append(tblName);
                if (dbLink != null && !dbLink.trim().isEmpty()) {
                    ddl.append("@").append(dbLink.trim());
                }
                result.add(new DbObject(DbObjectType.SYNONYM, synName.toLowerCase(), ddl.toString()));
            }
        } catch (Exception e) {
            throw new RuntimeException("提取 Oracle 同义词失败", e);
        } finally {
            close(ps, rs);
        }
        return result;
    }

    @Override
    protected List<DbObject> extractSequences() {
        List<DbObject> result = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(
                    "SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, " +
                    "CYCLE_FLAG, ORDER_FLAG, CACHE_SIZE, LAST_NUMBER FROM USER_SEQUENCES ORDER BY SEQUENCE_NAME");
            rs = ps.executeQuery();
            while (rs.next()) {
                String name  = rs.getString("SEQUENCE_NAME");
                long   minV  = rs.getLong("MIN_VALUE");
                long   maxV  = rs.getLong("MAX_VALUE");
                long   incr  = rs.getLong("INCREMENT_BY");
                String cycle = rs.getString("CYCLE_FLAG");
                long   cache = rs.getLong("CACHE_SIZE");
                long   lastN = rs.getLong("LAST_NUMBER");
                long   start = lastN > minV ? lastN : minV;
                StringBuilder ddl = new StringBuilder("CREATE SEQUENCE ")
                        .append(name)
                        .append("\n  START WITH ").append(start)
                        .append("\n  INCREMENT BY ").append(incr)
                        .append("\n  MINVALUE ").append(minV)
                        .append("\n  MAXVALUE ").append(maxV)
                        .append("\n  ").append("Y".equalsIgnoreCase(cycle) ? "CYCLE" : "NOCYCLE")
                        .append("\n  ").append(cache > 0 ? "CACHE " + cache : "NOCACHE");
                result.add(new DbObject(DbObjectType.SEQUENCE, name.toLowerCase(), ddl.toString()));
            }
        } catch (Exception e) {
            throw new RuntimeException("提取 Oracle 序列失败", e);
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
                    "SELECT JOB_NAME, JOB_TYPE, JOB_ACTION, START_DATE, " +
                    "REPEAT_INTERVAL, ENABLED FROM USER_SCHEDULER_JOBS ORDER BY JOB_NAME");
            rs = ps.executeQuery();
            while (rs.next()) {
                String jobName = rs.getString("JOB_NAME");
                String ddl = "-- [Oracle Scheduler Job] " + jobName + "\n" +
                             "-- Type: " + rs.getString("JOB_TYPE") + "\n" +
                             "-- Action: " + rs.getString("JOB_ACTION") + "\n" +
                             "-- Repeat: " + rs.getString("REPEAT_INTERVAL") + "\n" +
                             "-- Enabled: " + rs.getString("ENABLED") + "\n" +
                             "-- NOTE: 请在目标库中手动创建对应调度任务";
                result.add(new DbObject(DbObjectType.JOB, jobName.toLowerCase(), ddl));
            }
        } catch (Exception e) {
            logger.warn("提取 Oracle Job 失败（可能权限不足）：{}", e.getMessage());
        } finally {
            close(ps, rs);
        }
        return result;
    }

    @Override
    protected List<DbObject> extractRoleGrants() {
        List<DbObject> result = new ArrayList<>();
        StringBuilder ddl = new StringBuilder("-- [Oracle 用户权限/角色]\n");
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // 系统权限
            ps = connection.prepareStatement("SELECT PRIVILEGE FROM USER_SYS_PRIVS ORDER BY PRIVILEGE");
            rs = ps.executeQuery();
            ddl.append("-- 系统权限:\n");
            while (rs.next()) {
                ddl.append("-- GRANT ").append(rs.getString(1)).append(" TO <目标用户>;\n");
            }
            close(ps, rs);
            // 角色授权
            ps = connection.prepareStatement("SELECT GRANTED_ROLE, ADMIN_OPTION FROM USER_ROLE_PRIVS ORDER BY GRANTED_ROLE");
            rs = ps.executeQuery();
            ddl.append("-- 角色授权:\n");
            while (rs.next()) {
                ddl.append("-- GRANT ").append(rs.getString("GRANTED_ROLE"))
                   .append(" TO <目标用户>")
                   .append("YES".equalsIgnoreCase(rs.getString("ADMIN_OPTION")) ? " WITH ADMIN OPTION" : "")
                   .append(";\n");
            }
            ddl.append("-- NOTE: 请在目标库中手动执行上述授权语句");
            result.add(new DbObject(DbObjectType.ROLE_GRANT, "user_grants", ddl.toString()));
        } catch (Exception e) {
            logger.warn("提取 Oracle 权限/角色失败：{}", e.getMessage());
        } finally {
            close(ps, rs);
        }
        return result;
    }
}
