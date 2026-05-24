/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.connector.oracle.logminer;

import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-09 20:23
 */
public class LogMinerHelper {

    private static final Logger logger = LoggerFactory.getLogger(LogMinerHelper.class);
    public static final int LOG_MINER_OC_INSERT = 1;
    public static final int LOG_MINER_OC_DELETE = 2;
    public static final int LOG_MINER_OC_UPDATE = 3;
    public static final int LOG_MINER_OC_DDL = 5;
    public static final int LOG_MINER_OC_COMMIT = 7;
    public static final int LOG_MINER_OC_MISSING_SCN = 34;
    public static final int LOG_MINER_OC_ROLLBACK = 36;
    private static final String LOG_MINER_SQL_QUERY_ROLES = "SELECT * FROM USER_ROLE_PRIVS";
    private static final String LOG_MINER_SQL_QUERY_PRIVILEGES = "SELECT * FROM SESSION_PRIVS";
    private static final List<String> LOG_MINER_PRIVILEGES_NEEDED = Arrays.asList("CREATE SESSION", "SELECT ANY TRANSACTION", "SELECT ANY DICTIONARY", "LOGMINING");
    private static final List<String> LOG_MINER_ORACLE_11_PRIVILEGES_NEEDED = Arrays.asList("CREATE SESSION", "SELECT ANY TRANSACTION", "SELECT ANY DICTIONARY");
    private static final String LOG_MINER_SELECT_CATALOG_ROLE_ROLE = "SELECT_CATALOG_ROLE";
    private static final String LOG_MINER_SQL_GET_CURRENT_SCN = "select CURRENT_SCN from V$DATABASE";
    private static final String LOG_MINER_SQL_GET_CURRENT_SCN_FALLBACK = "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM DUAL";
    /** 12c+ 可用；11g 无此 USERENV 参数会报 ORA-02003 */
    private static final String LOG_MINER_SQL_IS_CDB = "SELECT SYS_CONTEXT('USERENV','CDB') FROM DUAL";
    private static final String LOG_MINER_SQL_IS_CDB_VDATABASE = "SELECT CDB FROM V$DATABASE";
    private static final String LOG_MINER_SQL_ALTER_SESSION_CONTAINER = "alter session set container=CDB$ROOT";
    private static final String LOG_MINER_SQL_ALTER_NLS_SESSION_PARAMETERS = "ALTER SESSION SET " + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'" + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'" + "  NLS_NUMERIC_CHARACTERS = '.,'" + "  TIME_ZONE = '00:00'";
    private static final String LOG_MINER_SQL_CURRENT_REDO_SEQUENCE = "SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'";
    private static final String LOG_MINER_SQL_END_LOG_MINER = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";
    private static final String LOG_MINER_SQL_ADD_LOG_FILES = "DECLARE\n" + "start_scn NUMBER := ?; end_scn NUMBER := ?; first_file BOOLEAN := true; \n" + "BEGIN \n" + "FOR log_file IN\n" + " (\n"
            + "  SELECT MIN(name) name, first_change# FROM \n" + "  (\n"
            + "   SELECT member AS name, first_change# FROM v$log l INNER JOIN v$logfile f ON l.group# = f.group# WHERE (l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE') AND first_change# < end_scn\n"
            + "   UNION\n" + "   SELECT name, first_change# FROM v$archived_log WHERE name IS NOT NULL AND STANDBY_DEST='NO' AND first_change# < end_scn AND next_change# > start_scn \n"
            + "  ) group by first_change# ORDER BY first_change# \n" + " ) LOOP \n" + " IF first_file THEN\n" + "  SYS.DBMS_LOGMNR.add_logfile(log_file.name, SYS.DBMS_LOGMNR.NEW);\n"
            + "  first_file := false;\n" + " ELSE\n" + "  SYS.DBMS_LOGMNR.add_logfile(log_file.name, SYS.DBMS_LOGMNR.ADDFILE);\n" + " END IF;\n" + "END LOOP;\n" + "\n"
            + "END;";

    private static final String LOG_MINER_SQL_START_LOG_MINER = "BEGIN \n"
            + "SYS.DBMS_LOGMNR.start_logmnr( options => SYS.DBMS_LOGMNR.SKIP_CORRUPTION + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);\n"
            + "END;";

    public static void executeCallableStatement(Connection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.prepareCall(statement)) {
            s.execute();
        }
    }

    public static List<BigInteger> getCurrentRedoLogSequences(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(LOG_MINER_SQL_CURRENT_REDO_SEQUENCE)) {
                List<BigInteger> sequences = new ArrayList<>();
                if (rs.next()) {
                    sequences.add(new BigInteger(rs.getString(1)));
                }
                // 如果是RAC则会返回多个SEQUENCE
                return sequences;
            }
        }
    }

    public static void endLogMiner(Connection connection) {
        if (connection != null) {
            try {
                executeCallableStatement(connection, LOG_MINER_SQL_END_LOG_MINER);
            } catch (Exception e) {
                if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                    logger.info("LogMiner session was already closed", e);
                } else {
                    logger.warn("Cannot close log miner session gracefully", e);
                }
            }
        }
    }

    public static String logMinerViewQuery(String schema, String logMinerUser) {
        StringBuilder query = new StringBuilder();
        // query.append("SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME ");
        query.append("SELECT * ");
        query.append("FROM V$LOGMNR_CONTENTS ");
        query.append("WHERE ");
        query.append("SCN >= ? AND SCN < ? ");
        query.append("AND (");
        // MISSING_SCN/DDL only when not performed by excluded users
        query.append("(OPERATION_CODE IN (5,34) AND USERNAME NOT IN (").append(getExcludedUsers(logMinerUser)).append(")) ");
        // COMMIT/ROLLBACK
        query.append("OR (OPERATION_CODE IN (7,36)) ");
        // INSERT/UPDATE/DELETE
        query.append("OR ");
        query.append("(OPERATION_CODE IN (1,2,3) ");
        query.append(" AND SEG_OWNER NOT IN ('APPQOSSYS','AUDSYS','CTXSYS','DVSYS','DBSFWUSER','DBSNMP','GSMADMIN_INTERNAL','LBACSYS','MDSYS','OJVMSYS','OLAPSYS','ORDDATA','ORDSYS','OUTLN','SYS','SYSTEM','WMSYS','XDB') ");

        if (StringUtil.isNotBlank(schema)) {
            query.append(String.format(" AND (REGEXP_LIKE(SEG_OWNER,'^%s$','i')) ", schema));
        }

        query.append(" ))");

        return query.toString();
    }

    public static String getNextValidScnAfter() {
        StringBuilder query = new StringBuilder();

        query.append("SELECT MIN(SCN) AS NEXT_VALID_SCN ");
        query.append("FROM V$LOGMNR_CONTENTS ");
        query.append("WHERE SCN >= ? AND operation IS NOT NULL");  // 这里就是参数占位符，需要传入上一个SCN

        return query.toString();
    }

    public static String getBacklogCount() {
        StringBuilder query = new StringBuilder();

        query.append("SELECT COUNT(SCN) AS BACKLOG_COUNT ");
        query.append("FROM V$LOGMNR_CONTENTS ");
        query.append("WHERE SCN >= ? AND SCN<= ? AND operation IS NOT NULL");  // 这里就是参数占位符，需要传入上一个SCN

        return query.toString();
    }

    private static String getExcludedUsers(String logMinerUser) {
        return "'SYS','SYSTEM','" + logMinerUser.toUpperCase() + "'";
    }

    public static void setSessionParameter(Connection connection) throws SQLException {
        executeCallableStatement(connection, LOG_MINER_SQL_ALTER_NLS_SESSION_PARAMETERS);
    }

    public static void startLogMiner(Connection connection, long startScn, long endScn) throws SQLException {
        addLogFiles(connection, startScn, endScn);
        startLogMinerSession(connection);
    }

    /**
     * 只负责把覆盖 [startScn, endScn) 的日志文件加入 LogMiner。
     * 注意：这里不会调用 START_LOGMNR，避免在 redo 切换场景下频繁重启会话导致 alert.log 暴涨。
     */
    public static void addLogFiles(Connection connection, long startScn, long endScn) throws SQLException {
        try (PreparedStatement addFilesStmt = connection.prepareCall(LOG_MINER_SQL_ADD_LOG_FILES)) {
            addFilesStmt.setString(1, String.valueOf(startScn));
            addFilesStmt.setString(2, String.valueOf(endScn));
            addFilesStmt.execute();
        }
    }

    /**
     * 启动 LogMiner 会话（假设日志文件已经 add 完成）。
     */
    public static void startLogMinerSession(Connection connection) throws SQLException {
        executeCallableStatement(connection, LOG_MINER_SQL_START_LOG_MINER);
    }

    public static long getCurrentScn(Connection connection) throws SQLException {
        try {
            return queryScn(connection, LOG_MINER_SQL_GET_CURRENT_SCN);
        } catch (SQLException e) {
            if (!isMissingDictionaryView(e)) {
                throw e;
            }
            logger.warn("无法访问 V$DATABASE 获取 SCN，尝试 DBMS_FLASHBACK 备用方案: {}", resolveSqlMessage(e));
        }
        try {
            return queryScn(connection, LOG_MINER_SQL_GET_CURRENT_SCN_FALLBACK);
        } catch (SQLException e) {
            throw new SQLException("无法获取当前 SCN，请为 LogMiner 用户授予 V$DATABASE 访问权限或 EXECUTE ON DBMS_FLASHBACK", e);
        }
    }

    private static long queryScn(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }
            return rs.getLong(1);
        }
    }

    /**
     * CDB 检测/切换为可选步骤：11g 无 CDB；12c+ 先 USERENV 再 V$DATABASE 兜底，失败时不中断 LogMiner。
     */
    public static void setSessionContainerIfCdbMode(Connection connection, int databaseMajorVersion) {
        if (databaseMajorVersion < 12) {
            return;
        }
        try {
            if (!detectCdbMode(connection, databaseMajorVersion)) {
                return;
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute(LOG_MINER_SQL_ALTER_SESSION_CONTAINER);
            }
        } catch (Exception e) {
            logger.warn("CDB 容器检测/切换未完成，将在当前会话继续 LogMiner: {}", e.getMessage());
        }
    }

    /**
     * 判断是否为 CDB 模式：12c+ 优先 USERENV(CDB)，11g 或 ORA-02003 时回退 V$DATABASE.CDB。
     */
    private static boolean detectCdbMode(Connection connection, int databaseMajorVersion) {
        if (databaseMajorVersion < 12) {
            return false;
        }
        Boolean byUserEnv = detectCdbModeByUserEnv(connection);
        if (byUserEnv != null) {
            return byUserEnv;
        }
        return detectCdbModeByVDatabase(connection);
    }

    private static Boolean detectCdbModeByUserEnv(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(LOG_MINER_SQL_IS_CDB)) {
            if (!rs.next()) {
                return false;
            }
            return isCdbFlag(rs.getString(1));
        } catch (Exception e) {
            if (isUnsupportedCdbUserEnv(e)) {
                logger.debug("USERENV(CDB) 不可用(通常为 11g 或非 CDB 环境)，跳过容器切换: {}", resolveExceptionMessage(e));
                return false;
            }
            logger.debug("USERENV(CDB) 检测失败，尝试 V$DATABASE.CDB: {}", resolveExceptionMessage(e));
            return null;
        }
    }

    private static boolean detectCdbModeByVDatabase(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(LOG_MINER_SQL_IS_CDB_VDATABASE)) {
            if (!rs.next()) {
                return false;
            }
            return isCdbFlag(rs.getString(1));
        } catch (Exception e) {
            if (isMissingDictionaryViewException(e)) {
                logger.debug("无法查询 V$DATABASE.CDB，按非 CDB 继续 LogMiner: {}", resolveExceptionMessage(e));
                return false;
            }
            logger.warn("V$DATABASE.CDB 检测失败，按非 CDB 继续 LogMiner: {}", resolveExceptionMessage(e));
            return false;
        }
    }

    private static boolean isCdbFlag(String cdb) {
        return cdb != null && "YES".equalsIgnoreCase(cdb.trim());
    }

    private static boolean isUnsupportedCdbUserEnv(Exception e) {
        Throwable current = e;
        while (current != null) {
            if (current instanceof SQLException) {
                if (((SQLException) current).getErrorCode() == 2003) {
                    return true;
                }
            }
            String message = current.getMessage();
            if (message != null && message.contains("ORA-02003")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isMissingDictionaryViewException(Exception e) {
        if (e instanceof SQLException) {
            return isMissingDictionaryView((SQLException) e);
        }
        Throwable current = e;
        while (current != null) {
            if (current instanceof SQLException && isMissingDictionaryView((SQLException) current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String resolveExceptionMessage(Exception e) {
        if (e == null) {
            return "";
        }
        if (e instanceof SQLException) {
            return resolveSqlMessage((SQLException) e);
        }
        String message = e.getMessage();
        return StringUtil.isNotBlank(message) ? message : e.getClass().getSimpleName();
    }

    private static boolean isMissingDictionaryView(SQLException e) {
        Throwable current = e;
        while (current != null) {
            if (current instanceof SQLException) {
                SQLException sqlException = (SQLException) current;
                int errorCode = sqlException.getErrorCode();
                if (errorCode == 942 || errorCode == 1031 || errorCode == 904) {
                    return true;
                }
            }
            String message = current.getMessage();
            if (message != null && (message.contains("ORA-00942") || message.contains("ORA-01031") || message.contains("ORA-00904"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String resolveSqlMessage(SQLException e) {
        if (e == null) {
            return "";
        }
        String message = e.getMessage();
        if (StringUtil.isNotBlank(message)) {
            return message;
        }
        return e.getClass().getSimpleName();
    }

    private static List<String> queryList(Connection connection, String querySql, String key) throws SQLException {
        List<String> list = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(querySql)) {
                while (rs.next()) {
                    String k = rs.getString(key);
                    if (StringUtil.isNotBlank(k)) {
                        list.add(k.toUpperCase());
                    }
                }
            }
        }
        return list;
    }

    public static void checkPermissions(Connection connection, int version) throws SQLException {
        List<String> roles = queryList(connection, LOG_MINER_SQL_QUERY_ROLES, "GRANTED_ROLE");
        if (CollectionUtils.isEmpty(roles)) {
            throw new RuntimeException("No permissions");
        }

        // DBA
        if (roles.contains("DBA")) {
            return;
        }
        // SELECT_CATALOG_ROLE 允许用户访问数据字典视图（如 DBA_, V$, ALL_* 等），用于查询数据库的元数据
        if (!roles.contains(LOG_MINER_SELECT_CATALOG_ROLE_ROLE)) {
            throw new IllegalArgumentException(String.format("No permission, please execute sql authorization：GRANT %s TO USERNAME;", LOG_MINER_SELECT_CATALOG_ROLE_ROLE));
        }

        List<String> privileges = queryList(connection, LOG_MINER_SQL_QUERY_PRIVILEGES, "PRIVILEGE");
        if (CollectionUtils.isEmpty(privileges)) {
            throw new RuntimeException("No permissions");
        }
        List<String> checkPrivileges = version <= 11 ? LOG_MINER_ORACLE_11_PRIVILEGES_NEEDED : LOG_MINER_PRIVILEGES_NEEDED;
        long count = privileges.stream().filter(checkPrivileges::contains).count();
        if (count != checkPrivileges.size()) {
            String log = StringUtil.join(Collections.singleton(checkPrivileges), StringUtil.COMMA);
            throw new IllegalArgumentException(String.format("No permission, please execute sql authorization：GRANT %s TO USERNAME;", log));
        }
    }
}