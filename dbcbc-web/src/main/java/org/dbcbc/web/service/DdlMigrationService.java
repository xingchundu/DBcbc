/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.web.service;

import database.ddl.transfer.Transfer;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.MigrationSummary;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.object.ObjectMigrateOptions;
import database.ddl.transfer.object.ObjectMigrateResult;

import org.dbcbc.biz.ConnectorService;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.sdk.config.DatabaseConfig;
import org.dbcbc.sdk.model.ConnectorConfig;

import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 * DDL 迁移服务：支持表结构迁移 + 数据库对象迁移（两阶段）
 */
@Service
public class DdlMigrationService {

    private static final String MYSQL      = "MySQL";
    private static final String POSTGRESQL = "PostgreSQL";
    private static final String ORACLE     = "Oracle";
    private static final String DM         = "DM";
    private static final String SQLSERVER  = "SqlServer";

    @Resource
    private ConnectorService connectorService;

    /** 阶段一：仅迁移表结构（原有逻辑） */
    public MigrationSummary transfer(String sourceConnectorId, String targetConnectorId) throws Throwable {
        Connector src = connectorService.getConnector(sourceConnectorId);
        Connector tgt = connectorService.getConnector(targetConnectorId);
        if (src == null || tgt == null) {
            throw new IllegalArgumentException("连接不存在，请从连接管理中选择有效数据源");
        }
        return Transfer.transferRDBMS(toSettings(src), toSettings(tgt));
    }

    /**
     * 阶段二：仅迁移数据库对象（表结构已完成）
     *
     * @param objectTypesCsv 逗号分隔的对象类型名，如 "PROCEDURE,FUNCTION,VIEW"
     */
    public ObjectMigrateResult transferObjects(String sourceConnectorId, String targetConnectorId,
            String objectTypesCsv) throws Throwable {
        Connector src = connectorService.getConnector(sourceConnectorId);
        Connector tgt = connectorService.getConnector(targetConnectorId);
        if (src == null || tgt == null) {
            throw new IllegalArgumentException("连接不存在，请从连接管理中选择有效数据源");
        }
        ObjectMigrateOptions options = parseOptions(objectTypesCsv);
        return Transfer.transferObjects(toSettings(src), toSettings(tgt), options);
    }

    private ObjectMigrateOptions parseOptions(String csv) {
        if (csv == null || csv.trim().isEmpty()) return ObjectMigrateOptions.none();
        if ("ALL".equalsIgnoreCase(csv.trim())) return ObjectMigrateOptions.all();
        List<String> names = Arrays.asList(csv.split(","));
        return ObjectMigrateOptions.of(names);
    }

    private DBSettings toSettings(Connector connector) {
        ConnectorConfig cfg = connector.getConfig();
        if (!(cfg instanceof DatabaseConfig)) {
            throw new IllegalArgumentException(
                    "DDL 迁移仅支持 MySQL / PostgreSQL / Oracle / DM / SqlServer 连接，请在连接管理中配置");
        }
        DatabaseConfig dc = (DatabaseConfig) cfg;
        String type = dc.getConnectorType();
        DataBaseType dbType = parseDbType(type);
        DBSettings s = new DBSettings();
        s.setDataBaseType(dbType);
        s.setDriverClass(dc.getDriverClassName());
        s.setIpAddress(dc.getHost());
        s.setPort(String.valueOf(dc.getPort()));
        s.setUserName(dc.getUsername());
        s.setUserPassword(dc.getPassword());
        if (dbType == DataBaseType.ORACLE && StringUtil.isNotBlank(dc.getServiceName())) {
            s.setOracleServiceName(dc.getServiceName());
            if (StringUtil.isNotBlank(dc.getDatabase())) {
                s.setDataBaseName(dc.getDatabase());
            }
        } else {
            s.setDataBaseName(dc.getDatabase());
        }
        return s;
    }

    private static DataBaseType parseDbType(String connectorType) {
        if (MYSQL.equals(connectorType))      return DataBaseType.MYSQL;
        if (POSTGRESQL.equals(connectorType)) return DataBaseType.POSTGRESQL;
        if (ORACLE.equals(connectorType))     return DataBaseType.ORACLE;
        if (DM.equals(connectorType))         return DataBaseType.DM;
        if (SQLSERVER.equals(connectorType))  return DataBaseType.SQLSERVER;
        throw new IllegalArgumentException(
                "不支持的连接类型: " + connectorType + "（仅支持 MySQL、PostgreSQL、Oracle、DM、SqlServer）");
    }
}
