/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.logminer;

import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.dm.DmException;

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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
    private static final String LOG_MINER_SQL_GET_CURRENT_LSN_FALLBACK = "SELECT MAX(NEXT_CHANGE#) FROM V$ARCHIVED_LOG WHERE STATUS = 'A'";
    private static final String LOG_MINER_SQL_QUERY_ARCHIVES =
            "SELECT PATH FROM V$ARCHIVED_LOG WHERE STATUS = 'A' AND FIRST_CHANGE# < ? AND NEXT_CHANGE# > ? ORDER BY FIRST_CHANGE#";
    private static final String LOG_MINER_SQL_ADD_LOG_FILE_NEW = "BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR.NEW); END;";
    private static final String LOG_MINER_SQL_ADD_LOG_FILE = "BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR.ADDFILE); END;";
    private static final String LOG_MINER_SQL_START_LOG_MINER = "BEGIN DBMS_LOGMNR.START_LOGMNR(OPTIONS => 2128); END;";
    private static final String LOG_MINER_SQL_END_LOG_MINER = "BEGIN DBMS_LOGMNR.END_LOGMNR(); END;";
    private static final String LOG_MINER_SQL_ALTER_NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
            + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
            + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
            + "  NLS_NUMERIC_CHARACTERS = '.,'"
            + "  TIME_ZONE = '00:00'";

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
        List<String> paths = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(LOG_MINER_SQL_QUERY_ARCHIVES)) {
            ps.setLong(1, endScn);
            ps.setLong(2, startScn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String path = rs.getString(1);
                    if (StringUtil.isNotBlank(path)) {
                        paths.add(path);
                    }
                }
            }
        }
        return paths;
    }

    public static List<BigInteger> getCurrentArchiveSequences(Connection connection, long startScn, long endScn) throws SQLException {
        List<BigInteger> sequences = new ArrayList<>();
        for (String path : listArchivePaths(connection, startScn, endScn)) {
            sequences.add(BigInteger.valueOf(path.hashCode() & 0x7FFFFFFFL));
        }
        return sequences;
    }

    public static void endLogMiner(Connection connection) {
        if (connection != null) {
            try {
                executeCallableStatement(connection, LOG_MINER_SQL_END_LOG_MINER);
            } catch (Exception e) {
                logger.debug("Cannot close log miner session gracefully: {}", e.getMessage());
            }
        }
    }

    public static String logMinerViewQuery(String schema, String logMinerUser) {
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
        if (StringUtil.isNotBlank(schema)) {
            query.append(String.format(" AND (REGEXP_LIKE(SEG_OWNER,'^%s$','i')) ", schema));
        }
        query.append(" ))");
        return query.toString();
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
        executeCallableStatement(connection, LOG_MINER_SQL_ALTER_NLS_SESSION_PARAMETERS);
    }

    public static void startLogMiner(Connection connection, long startScn, long endScn) throws SQLException {
        addLogFiles(connection, startScn, endScn);
        startLogMinerSession(connection);
    }

    public static void addLogFiles(Connection connection, long startScn, long endScn) throws SQLException {
        ensureLogMinerPackage(connection);
        endLogMiner(connection);
        List<String> paths = listArchivePaths(connection, startScn, endScn);
        if (CollectionUtils.isEmpty(paths)) {
            logger.warn("No archived logs found for SCN range [{}, {}), please check ARCH_INI and RLOG_APPEND_LOGIC", startScn, endScn);
            return;
        }
        boolean first = true;
        for (String path : paths) {
            String sql = first ? LOG_MINER_SQL_ADD_LOG_FILE_NEW : LOG_MINER_SQL_ADD_LOG_FILE;
            try (CallableStatement cs = connection.prepareCall(sql)) {
                cs.setString(1, path);
                cs.execute();
            }
            first = false;
        }
    }

    public static void startLogMinerSession(Connection connection) throws SQLException {
        executeCallableStatement(connection, LOG_MINER_SQL_START_LOG_MINER);
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
