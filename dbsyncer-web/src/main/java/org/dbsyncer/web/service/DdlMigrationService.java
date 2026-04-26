/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.web.service;

import database.ddl.transfer.Transfer;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.consts.DataBaseType;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.model.ConnectorConfig;

import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * 使用连接管理中已配置的关系型数据源执行 DDL 结构迁移
 */
@Service
public class DdlMigrationService {

    private static final String MYSQL = "MySQL";
    private static final String POSTGRESQL = "PostgreSQL";
    private static final String ORACLE = "Oracle";

    @Resource
    private ConnectorService connectorService;

    public void transfer(String sourceConnectorId, String targetConnectorId) throws Throwable {
        Connector src = connectorService.getConnector(sourceConnectorId);
        Connector tgt = connectorService.getConnector(targetConnectorId);
        if (src == null || tgt == null) {
            throw new IllegalArgumentException("连接不存在，请从连接管理中选择有效数据源");
        }
        Transfer.transferRDBMS(toSettings(src), toSettings(tgt));
    }

    private DBSettings toSettings(Connector connector) {
        ConnectorConfig cfg = connector.getConfig();
        if (!(cfg instanceof DatabaseConfig)) {
            throw new IllegalArgumentException("DDL 迁移仅支持 MySQL / PostgreSQL / Oracle 连接，请在连接管理中配置");
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
        if (MYSQL.equals(connectorType)) {
            return DataBaseType.MYSQL;
        }
        if (POSTGRESQL.equals(connectorType)) {
            return DataBaseType.POSTGRESQL;
        }
        if (ORACLE.equals(connectorType)) {
            return DataBaseType.ORACLE;
        }
        throw new IllegalArgumentException("不支持的连接类型: " + connectorType + "（仅支持 MySQL、PostgreSQL、Oracle）");
    }
}
