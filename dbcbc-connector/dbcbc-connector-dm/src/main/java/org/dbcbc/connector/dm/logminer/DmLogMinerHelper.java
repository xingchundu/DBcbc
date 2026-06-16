/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.logminer;

import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.dm.DmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 达梦 DBMS_LOGMNR 归档日志挖掘辅助类。
 */
public final class DmLogMinerHelper {

    private static final Logger logger = LoggerFactory.getLogger(DmLogMinerHelper.class);

    public static final int LOG_MINER_OC_INSERT = 1;
    public static final int LOG_MINER_OC_DELETE = 2;
    public static final int LOG_MINER_OC_UPDATE = 3;
    public static final int LOG_MINER_OC_DDL = 5;
    public static final int LOG_MINER_OC_COMMIT = 7;
    public static final int LOG_MINER_OC_MISSING_SCN = 34;
    public static final int LOG_MINER_OC_ROLLBACK = 36;

    private static final String LOG_MINER_SQL_ENSURE_PACKAGE = "SP_CREATE_SYSTEM_PACKAGES(1,'DBMS_LOGMNR')";
    private static final String LOG_MINER_SQL_GET_CURRENT_LSN = "SELECT CUR_LSN FROM V$RLOG";
    private static final String LOG_MINER_SQL_GET_CURRENT_LSN_FALLBACK =
            "SELECT MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL";
    /** 达梦 CDC 需始终装载最新若干归档（同一归档文件会持续增长，不会触发 SEQUENCE# 变化）。 */
    private static final String LOG_MINER_SQL_QUERY_RECENT_ARCHIVES =
            "SELECT NAME FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL "
                    + "AND SEQUENCE# >= (SELECT MAX(SEQUENCE#) - ? FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL) "
                    + "ORDER BY FIRST_CHANGE#";
    private static final String LOG_MINER_SQL_ARCHIVE_SNAPSHOT =
            "SELECT MAX(SEQUENCE#), MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL";
    private static final int RECENT_ARCHIVE_WINDOW = 4;
    private static final int LOG_MINER_START_OPTIONS = 2130;
    /** 达梦官方示例为单参数 ADD_LOGFILE，默认 ADDFILE；JDBC 传第二参数 2 可能触发 -2849。 */
    private static final String LOG_MINER_SQL_ADD_LOG_FILE = "{call DBMS_LOGMNR.ADD_LOGFILE(?)}";
    private static final String LOG_MINER_SQL_REMOVE_LOG_FILE = "{call DBMS_LOGMNR.REMOVE_LOGFILE(?)}";
    private static final String LOG_MINER_SQL_LIST_LOGS = "SELECT FILENAME FROM V$LOGMNR_LOGS";
    private static final String LOG_MINER_SQL_QUERY_COVERING_ARCHIVE =
            "SELECT NAME FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL "
                    + "AND FIRST_CHANGE# < ? AND (NEXT_CHANGE# > ? OR NEXT_CHANGE# IS NULL) "
                    + "ORDER BY FIRST_CHANGE# DESC FETCH FIRST 1 ROWS ONLY";
    private static final String LOG_MINER_SQL_QUERY_LATEST_ARCHIVE =
            "SELECT NAME FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL "
                    + "ORDER BY SEQUENCE# DESC FETCH FIRST 1 ROWS ONLY";
    private static final String LOG_MINER_SQL_START_LOG_MINER = "CALL DBMS_LOGMNR.START_LOGMNR(OPTIONS => " + LOG_MINER_START_OPTIONS + ")";
    private static final String LOG_MINER_SQL_END_LOG_MINER = "{call DBMS_LOGMNR.END_LOGMNR()}";
    /** 达梦 ALTER SESSION 一次只能设置一个参数，不能与 Oracle 一样合并多条。 */
    private static final String LOG_MINER_SQL_ALTER_NLS_DATE_FORMAT =
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'";
    private static final String LOG_MINER_SQL_ALTER_NLS_TIMESTAMP_FORMAT =
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'";
    private static final String LOG_MINER_SQL_ALTER_NLS_DATE_LANGUAGE =
            "ALTER SESSION SET NLS_DATE_LANGUAGE = 'ENGLISH'";

    private DmLogMinerHelper() {
    }

    public static void executeCallableStatement(Connection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.prepareCall(statement)) {
            s.execute();
        }
    }

    public static void ensureLogMinerPackage(Connection connection) throws SQLException {
        try (CallableStatement s = connection.prepareCall("{call " + LOG_MINER_SQL_ENSURE_PACKAGE + "}")) {
            s.execute();
        } catch (SQLException e) {
            logger.debug("DBMS_LOGMNR package may already exist: {}", e.getMessage());
        }
    }

    public static List<String> listArchivePaths(Connection connection, long startScn, long endScn) throws SQLException {
        Set<String> paths = new LinkedHashSet<>();
        try (PreparedStatement ps = connection.prepareStatement(LOG_MINER_SQL_QUERY_COVERING_ARCHIVE)) {
            ps.setLong(1, endScn);
            ps.setLong(2, startScn);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    addArchivePath(paths, rs.getString(1));
                }
            }
        }
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(LOG_MINER_SQL_QUERY_LATEST_ARCHIVE)) {
            if (rs.next()) {
                addArchivePath(paths, rs.getString(1));
            }
        }
        try (PreparedStatement ps = connection.prepareStatement(LOG_MINER_SQL_QUERY_RECENT_ARCHIVES)) {
            ps.setInt(1, RECENT_ARCHIVE_WINDOW);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    addArchivePath(paths, rs.getString(1));
                }
            }
        } catch (SQLException e) {
            logger.debug("Recent archive query unavailable, skip: {}", e.getMessage());
        }
        if (paths.isEmpty()) {
            try (PreparedStatement ps = connection.prepareStatement(LOG_MINER_SQL_QUERY_RECENT_ARCHIVES)) {
                ps.setInt(1, RECENT_ARCHIVE_WINDOW);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        addArchivePath(paths, rs.getString(1));
                    }
                }
            } catch (SQLException e) {
                logger.debug("Recent archive fallback unavailable, skip: {}", e.getMessage());
            }
        }
        return new ArrayList<>(paths);
    }

    private static void addArchivePath(Set<String> paths, String path) {
        if (StringUtil.isBlank(path)) {
            return;
        }
        String normalized = normalizeArchivePath(path);
        boolean duplicated = paths.stream().anyMatch(existing -> normalizeArchivePath(existing).equals(normalized));
        if (!duplicated) {
            paths.add(path.trim());
        }
    }

    private static String normalizeArchivePath(String path) {
        if (path == null) {
            return StringUtil.EMPTY;
        }
        String normalized = path.trim().replace('\\', '/');
        int slash = normalized.lastIndexOf('/');
        if (slash >= 0 && slash < normalized.length() - 1) {
            normalized = normalized.substring(slash + 1);
        }
        return normalized.toUpperCase();
    }

    public static ArchiveSnapshot getArchiveSnapshot(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(LOG_MINER_SQL_ARCHIVE_SNAPSHOT)) {
            if (rs.next()) {
                long maxSequence = rs.getLong(1);
                if (rs.wasNull()) {
                    maxSequence = 0;
                }
                long maxNextChange = rs.getLong(2);
                if (rs.wasNull()) {
                    maxNextChange = 0;
                }
                return new ArchiveSnapshot(maxSequence, maxNextChange);
            }
        }
        return new ArchiveSnapshot(0, 0);
    }

    public static final class ArchiveSnapshot {
        private final long maxSequence;
        private final long maxNextChange;

        public ArchiveSnapshot(long maxSequence, long maxNextChange) {
            this.maxSequence = maxSequence;
            this.maxNextChange = maxNextChange;
        }

        public boolean changed(ArchiveSnapshot other) {
            if (other == null) {
                return true;
            }
            return maxSequence != other.maxSequence || maxNextChange != other.maxNextChange;
        }

        @Override
        public String toString() {
            return "ArchiveSnapshot{maxSequence=" + maxSequence + ", maxNextChange=" + maxNextChange + '}';
        }
    }

    public static void endLogMiner(Connection connection) {
        if (connection != null) {
            resetLogMinerSession(connection);
        }
    }

    /**
     * 安全清理 LogMiner 会话：先 REMOVE 已登记文件，再 END；忽略无会话/未列出等错误（-2846/-2849）。
     */
    public static void resetLogMinerSession(Connection connection) {
        if (connection == null) {
            return;
        }
        for (String filename : listLogMinerLogFiles(connection)) {
            try (CallableStatement cs = connection.prepareCall(LOG_MINER_SQL_REMOVE_LOG_FILE)) {
                cs.setString(1, filename);
                cs.execute();
            } catch (SQLException e) {
                if (!isIgnorableLogMinerResetError(e)) {
                    logger.warn("REMOVE_LOGFILE failed for {}: {}", filename, e.getMessage());
                }
            }
        }
        try {
            executeCallableStatement(connection, LOG_MINER_SQL_END_LOG_MINER);
        } catch (SQLException e) {
            if (!isIgnorableLogMinerResetError(e)) {
                logger.warn("END_LOGMNR failed: {}", e.getMessage());
            }
        }
    }

    private static List<String> listLogMinerLogFiles(Connection connection) {
        List<String> files = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(LOG_MINER_SQL_LIST_LOGS)) {
            while (rs.next()) {
                String filename = rs.getString(1);
                if (StringUtil.isNotBlank(filename)) {
                    files.add(filename.trim());
                }
            }
        } catch (SQLException e) {
            logger.debug("V$LOGMNR_LOGS unavailable, skip remove before END: {}", e.getMessage());
        }
        return files;
    }

    private static boolean isIgnorableLogMinerResetError(SQLException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        return message.contains("-2846")
                || message.contains("-2849")
                || message.contains("2846")
                || message.contains("2849")
                || message.contains("无活动的 LogMiner")
                || message.contains("未列出的日志文件");
    }

    public static String logMinerViewQuery(String schema, String logMinerUser, List<String> tableNames) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT * ");
        query.append("FROM V$LOGMNR_CONTENTS ");
        query.append("WHERE ");
        query.append("SCN >= ? AND SCN < ? ");
        query.append("AND (");
        query.append("(OPERATION_CODE IN (5,34) AND USERNAME NOT IN (").append(getExcludedUsers(logMinerUser)).append(")) ");
        query.append("OR (OPERATION_CODE IN (7,36)) ");
        query.append("OR ");
        query.append("(OPERATION_CODE IN (1,2,3) ");
        query.append(" AND SEG_OWNER NOT IN ('SYS','SYSDBA','SYSSSO','SYSAUDITOR','SYSJOB','SYSTS','CTISYS') ");
        appendDmlScopeFilter(query, schema, tableNames);
        query.append(" ))");
        return query.toString();
    }

    private static void appendDmlScopeFilter(StringBuilder query, String schema, List<String> tableNames) {
        query.append(" AND (");
        boolean hasScope = false;
        if (StringUtil.isNotBlank(schema)) {
            query.append(String.format("REGEXP_LIKE(SEG_OWNER,'^%s$','i')", escapeRegexLiteral(schema)));
            hasScope = true;
        }
        if (!CollectionUtils.isEmpty(tableNames)) {
            if (hasScope) {
                query.append(" OR ");
            }
            query.append("TABLE_NAME IN (");
            for (int i = 0; i < tableNames.size(); i++) {
                if (i > 0) {
                    query.append(',');
                }
                query.append('\'').append(tableNames.get(i).toUpperCase().replace("'", "''")).append('\'');
            }
            query.append(')');
            hasScope = true;
        }
        if (!hasScope) {
            query.append("1=1");
        }
        query.append(')');
    }

    private static String escapeRegexLiteral(String value) {
        return value.replace("\\", "\\\\").replace("'", "''");
    }

    public static String getNextValidScnAfter() {
        return "SELECT MIN(SCN) AS NEXT_VALID_SCN FROM V$LOGMNR_CONTENTS WHERE SCN >= ? AND operation IS NOT NULL";
    }

    public static String getBacklogCount() {
        return "SELECT COUNT(SCN) AS BACKLOG_COUNT FROM V$LOGMNR_CONTENTS WHERE SCN >= ? AND SCN <= ? AND operation IS NOT NULL";
    }

    private static String getExcludedUsers(String logMinerUser) {
        return "'SYS','SYSDBA','SYSSSO','" + logMinerUser.toUpperCase() + "'";
    }

    public static void setSessionParameter(Connection connection) throws SQLException {
        executeSessionAlter(connection, LOG_MINER_SQL_ALTER_NLS_DATE_FORMAT);
        executeSessionAlter(connection, LOG_MINER_SQL_ALTER_NLS_TIMESTAMP_FORMAT);
        try {
            executeSessionAlter(connection, LOG_MINER_SQL_ALTER_NLS_DATE_LANGUAGE);
        } catch (SQLException e) {
            logger.debug("NLS_DATE_LANGUAGE not supported, skip: {}", e.getMessage());
        }
    }

    private static void executeSessionAlter(Connection connection, String sql) throws SQLException {
        Objects.requireNonNull(sql);
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    public static void startLogMiner(Connection connection, long startScn, long endScn) throws SQLException {
        addLogFiles(connection, startScn, endScn);
        startLogMinerSession(connection);
    }

    public static void addLogFiles(Connection connection, long startScn, long endScn) throws SQLException {
        addLogFiles(connection, startScn, endScn, true);
    }

    public static void addLogFiles(Connection connection, long startScn, long endScn, boolean resetSession) throws SQLException {
        ensureLogMinerPackage(connection);
        if (resetSession) {
            resetLogMinerSession(connection);
        }
        List<String> paths = listArchivePaths(connection, startScn, endScn);
        if (CollectionUtils.isEmpty(paths)) {
            logger.warn("No archived logs found for SCN range [{}, {}), please check ARCH_INI and RLOG_APPEND_LOGIC", startScn, endScn);
            return;
        }
        logger.info("Load {} DM archive log file(s) for SCN range [{}, {}): {}", paths.size(), startScn, endScn, paths);
        for (String path : paths) {
            addArchiveLogFile(connection, path);
        }
    }

    private static void addArchiveLogFile(Connection connection, String path) throws SQLException {
        try {
            try (CallableStatement cs = connection.prepareCall(LOG_MINER_SQL_ADD_LOG_FILE)) {
                cs.setString(1, path);
                cs.execute();
            }
        } catch (SQLException e) {
            if (isDuplicateLogFileError(e)) {
                logger.debug("Archive already listed, skip: {}", path);
                return;
            }
            logger.debug("ADD_LOGFILE callable failed ({}), retry CALL syntax for {}", e.getMessage(), path);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CALL DBMS_LOGMNR.ADD_LOGFILE('" + escapeSqlLiteral(path) + "')");
            }
        }
    }

    private static String escapeSqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static boolean isDuplicateLogFileError(SQLException e) {
        String message = e.getMessage();
        return message != null && (message.contains("-2848") || message.contains("2848") || message.contains("duplicate logfile"));
    }

    public static void startLogMinerSession(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(LOG_MINER_SQL_START_LOG_MINER);
        }
    }

    public static long getCurrentScn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(LOG_MINER_SQL_GET_CURRENT_LSN)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            } catch (SQLException e) {
                logger.debug("CUR_LSN unavailable, fallback to archived log SCN: {}", e.getMessage());
            }
            try (ResultSet rs = statement.executeQuery(LOG_MINER_SQL_GET_CURRENT_LSN_FALLBACK)) {
                if (rs.next()) {
                    long val = rs.getLong(1);
                    if (!rs.wasNull()) {
                        return val;
                    }
                }
            }
            throw new IllegalStateException("Couldn't get current LSN/SCN from DM database");
        }
    }

    public static void checkPermissions(Connection connection) throws SQLException {
        List<String> roles = queryList(connection, "SELECT GRANTED_ROLE FROM USER_ROLE_PRIVS", "GRANTED_ROLE");
        if (CollectionUtils.isEmpty(roles)) {
            throw new DmException("达梦账号缺少 LogMiner 权限，请授予 DBA 或 SELECT ANY TABLE 等权限");
        }
        if (roles.stream().anyMatch(r -> "DBA".equalsIgnoreCase(r))) {
            return;
        }
        throw new DmException("达梦 LogMiner 需要 DBA 权限，请执行：GRANT DBA TO " + connection.getMetaData().getUserName());
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
}
