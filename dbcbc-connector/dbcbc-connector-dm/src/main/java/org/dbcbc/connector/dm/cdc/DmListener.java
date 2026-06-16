/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.cdc;

import org.dbcbc.common.QueueOverflowException;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.dm.DmException;
import org.dbcbc.connector.dm.logminer.DmLogMiner;
import org.dbcbc.connector.dm.logminer.DmLogMinerParser;
import org.dbcbc.connector.dm.logminer.RedoEvent;
import org.dbcbc.connector.dm.logminer.parser.DmDeleteSql;
import org.dbcbc.connector.dm.logminer.parser.DmInsertSql;
import org.dbcbc.connector.dm.logminer.parser.DmUpdateSql;
import org.dbcbc.sdk.config.DatabaseConfig;
import org.dbcbc.sdk.connector.database.Database;
import org.dbcbc.sdk.connector.database.DatabaseConnectorInstance;
import org.dbcbc.sdk.constant.ConnectorConstant;
import org.dbcbc.sdk.listener.AbstractDatabaseListener;
import org.dbcbc.sdk.listener.ChangedEvent;
import org.dbcbc.sdk.listener.event.DDLChangedEvent;
import org.dbcbc.sdk.listener.event.RowChangedEvent;
import org.dbcbc.sdk.model.ChangedOffset;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.model.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 达梦归档日志增量监听（LogMiner），用于 DM -> PostgreSQL 等目标库的日志增量同步。
 */
public class DmListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String REDO_POSITION = "position";
    private final Map<String, List<Field>> tableFiledMap = new ConcurrentHashMap<>();
    private final Map<String, String> mappingTableNameMap = new ConcurrentHashMap<>();
    private DmLogMiner logMiner;

    @Override
    public void init() {
        super.init();
        sourceTable.forEach(table -> registerTable(table.getName(), table.getColumn()));
    }

    private void registerTable(String tableName, List<Field> columns) {
        String key = normalizeTableKey(tableName);
        tableFiledMap.put(key, columns);
        mappingTableNameMap.put(key, tableName);
        if (StringUtil.isNotBlank(schema)) {
            String schemaKey = normalizeTableKey(schema + "." + tableName);
            tableFiledMap.put(schemaKey, columns);
            mappingTableNameMap.put(schemaKey, tableName);
        }
    }

    @Override
    public void start() {
        try {
            final DatabaseConfig config = getConnectorInstance().getConfig();
            String driverClassName = config.getDriverClassName();
            String username = config.getUsername();
            String password = config.getPassword();
            String url = config.getUrl();
            List<String> monitorTableNames = sourceTable.stream().map(Table::getName).collect(Collectors.toList());
            boolean containsPos = snapshot.containsKey(REDO_POSITION);
            logMiner = new DmLogMiner(username, password, url, schema, driverClassName, monitorTableNames);
            logMiner.setStartScn(containsPos ? Long.parseLong(snapshot.get(REDO_POSITION)) : 0);
            logMiner.registerEventListener(event -> {
                try {
                    parseEvent(event);
                } catch (JSQLParserException e) {
                    logger.warn("不支持sql:{}", event.getRedoSql());
                } catch (Exception e) {
                    logger.error("解析sql异常:{}", event.getRedoSql(), e);
                }
            });
            logMiner.start();
        } catch (Exception e) {
            logger.error("达梦 LogMiner 启动失败:{}", e.getMessage(), e);
            throw new DmException(e);
        }
    }

    private void trySendEvent(ChangedEvent event) {
        try {
            while (logMiner.isConnected()) {
                try {
                    sendChangedEvent(event);
                    break;
                } catch (QueueOverflowException e) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.error(ex.getMessage(), ex);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void parseEvent(RedoEvent event) throws Exception {
        Statement statement = CCJSqlParserUtil.parse(event.getRedoSql());
        if (statement instanceof Update) {
            Update update = (Update) statement;
            String tableName = resolveMappingTableName(update.getTable(), event);
            List<Field> fields = resolveFields(tableName);
            if (fields != null) {
                DmUpdateSql parser = new DmUpdateSql(update, fields);
                List<Object> data = parser.parseColumns();
                enrichUpdateRow(tableName, fields, data);
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_UPDATE, data, null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            String tableName = resolveMappingTableName(insert.getTable(), event);
            List<Field> fields = resolveFields(tableName);
            if (fields != null) {
                DmInsertSql parser = new DmInsertSql(insert, fields);
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_INSERT, parser.parseColumns(), null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Delete) {
            Delete delete = (Delete) statement;
            String tableName = resolveMappingTableName(delete.getTable(), event);
            List<Field> fields = resolveFields(tableName);
            if (fields != null) {
                DmDeleteSql parser = new DmDeleteSql(delete, fields);
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_DELETE, parser.parseColumns(), null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Alter) {
            Alter alter = (Alter) statement;
            String tableName = resolveMappingTableName(alter.getTable(), event);
            if (resolveFields(tableName) != null) {
                logger.info("sql:{}", event.getRedoSql());
                trySendEvent(new DDLChangedEvent(tableName, ConnectorConstant.OPERTION_ALTER, event.getRedoSql(), null, event.getScn()));
            }
        }
    }

    @Override
    public void close() {
        if (logMiner != null) {
            logMiner.close();
        }
    }

    @Override
    public void refreshEvent(ChangedOffset offset) {
        snapshot.put(REDO_POSITION, String.valueOf(offset.getPosition()));
    }

    private void enrichUpdateRow(String tableName, List<Field> fields, List<Object> data) {
        if (!(connectorService instanceof Database) || !(getConnectorInstance() instanceof DatabaseConnectorInstance)) {
            return;
        }
        DmRowEnricher.fillMissingColumns((DatabaseConnectorInstance) getConnectorInstance(), (Database) connectorService, schema, tableName, fields,
                data);
    }

    private List<Field> resolveFields(String tableName) {
        if (StringUtil.isBlank(tableName)) {
            return null;
        }
        List<Field> fields = tableFiledMap.get(normalizeTableKey(tableName));
        return fields == null ? null : DmLogMinerParser.toUpperCaseFields(fields);
    }

    private String resolveMappingTableName(net.sf.jsqlparser.schema.Table table, RedoEvent event) {
        String parsedTableName = getTableName(table);
        String parsedSchema = table == null ? null : normalizeTableKey(getSchemaName(table));
        if (StringUtil.isNotBlank(parsedSchema)) {
            String schemaTableKey = parsedSchema + "." + normalizeTableKey(parsedTableName);
            if (resolveFields(schemaTableKey) != null && matchConfiguredSchema(parsedSchema, event)) {
                return mappingTableNameMap.getOrDefault(schemaTableKey, parsedTableName);
            }
        }
        if (resolveFields(parsedTableName) != null && matchConfiguredSchema(parsedSchema, event)) {
            return mappingTableNameMap.getOrDefault(normalizeTableKey(parsedTableName), parsedTableName);
        }
        String logTableName = event.getObjectName();
        String logSchema = normalizeTableKey(event.getObjectOwner());
        if (StringUtil.isNotBlank(logSchema) && StringUtil.isNotBlank(logTableName)) {
            String schemaTableKey = logSchema + "." + normalizeTableKey(logTableName);
            if (resolveFields(schemaTableKey) != null && matchConfiguredSchema(logSchema, event)) {
                return mappingTableNameMap.getOrDefault(schemaTableKey, logTableName);
            }
        }
        if (StringUtil.isNotBlank(logTableName) && resolveFields(logTableName) != null && matchConfiguredSchema(logSchema, event)) {
            return mappingTableNameMap.getOrDefault(normalizeTableKey(logTableName), logTableName);
        }
        return parsedTableName;
    }

    private boolean matchConfiguredSchema(String owner, RedoEvent event) {
        if (StringUtil.isBlank(schema)) {
            return true;
        }
        if (StringUtil.isNotBlank(owner)) {
            return schema.equalsIgnoreCase(owner);
        }
        if (StringUtil.isNotBlank(event.getObjectOwner())) {
            return schema.equalsIgnoreCase(event.getObjectOwner());
        }
        // LogMiner 行缺少 owner 时，仍允许按表名匹配已映射表
        return true;
    }

    private String normalizeTableKey(String tableName) {
        return tableName == null ? StringUtil.EMPTY : tableName.toUpperCase();
    }

    private String getTableName(net.sf.jsqlparser.schema.Table table) {
        return table == null ? StringUtil.EMPTY : StringUtil.replace(table.getName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
    }

    private String getSchemaName(net.sf.jsqlparser.schema.Table table) {
        if (table == null) {
            return StringUtil.EMPTY;
        }
        String schemaName = table.getSchemaName();
        if (StringUtil.isBlank(schemaName) && table.getDatabase() != null) {
            schemaName = table.getDatabase().getDatabaseName();
        }
        return StringUtil.replace(schemaName, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
    }
}
