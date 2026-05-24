/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.cdc;

import org.dbcbc.common.QueueOverflowException;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.dm.DmException;
import org.dbcbc.connector.dm.logminer.DmLogMiner;
import org.dbcbc.connector.dm.logminer.RedoEvent;
import org.dbcbc.connector.oracle.logminer.parser.impl.DeleteSql;
import org.dbcbc.connector.oracle.logminer.parser.impl.InsertSql;
import org.dbcbc.connector.oracle.logminer.parser.impl.UpdateSql;
import org.dbcbc.sdk.config.DatabaseConfig;
import org.dbcbc.sdk.constant.ConnectorConstant;
import org.dbcbc.sdk.listener.AbstractDatabaseListener;
import org.dbcbc.sdk.listener.ChangedEvent;
import org.dbcbc.sdk.listener.event.DDLChangedEvent;
import org.dbcbc.sdk.listener.event.RowChangedEvent;
import org.dbcbc.sdk.model.ChangedOffset;
import org.dbcbc.sdk.model.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 达梦归档日志增量监听（LogMiner），用于 DM -> PostgreSQL 等目标库的日志增量同步。
 */
public class DmListener extends AbstractDatabaseListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String REDO_POSITION = "position";
    private final Map<String, List<Field>> tableFiledMap = new ConcurrentHashMap<>();
    private DmLogMiner logMiner;

    @Override
    public void init() {
        super.init();
        sourceTable.forEach(table -> tableFiledMap.put(table.getName(), table.getColumn()));
    }

    @Override
    public void start() {
        try {
            final DatabaseConfig config = getConnectorInstance().getConfig();
            String driverClassName = config.getDriverClassName();
            String username = config.getUsername();
            String password = config.getPassword();
            String url = config.getUrl();
            boolean containsPos = snapshot.containsKey(REDO_POSITION);
            logMiner = new DmLogMiner(username, password, url, schema, driverClassName);
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
            String tableName = getTableName(update.getTable());
            if (tableFiledMap.containsKey(tableName)) {
                UpdateSql parser = new UpdateSql(update, tableFiledMap.get(tableName));
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_UPDATE, parser.parseColumns(), null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            String tableName = getTableName(insert.getTable());
            if (tableFiledMap.containsKey(tableName)) {
                InsertSql parser = new InsertSql(insert, tableFiledMap.get(tableName));
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_INSERT, parser.parseColumns(), null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Delete) {
            Delete delete = (Delete) statement;
            String tableName = getTableName(delete.getTable());
            if (tableFiledMap.containsKey(tableName)) {
                DeleteSql parser = new DeleteSql(delete, tableFiledMap.get(tableName));
                trySendEvent(new RowChangedEvent(tableName, ConnectorConstant.OPERTION_DELETE, parser.parseColumns(), null, event.getScn()));
            }
            return;
        }

        if (statement instanceof Alter) {
            Alter alter = (Alter) statement;
            String tableName = getTableName(alter.getTable());
            if (tableFiledMap.containsKey(tableName)) {
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

    private String getTableName(Table table) {
        return table == null ? StringUtil.EMPTY : StringUtil.replace(table.getName(), StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
    }
}
