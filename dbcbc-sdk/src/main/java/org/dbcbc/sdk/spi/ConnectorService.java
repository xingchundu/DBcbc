/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.sdk.spi;

import org.dbcbc.common.model.Result;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.sdk.SdkException;
import org.dbcbc.sdk.config.CommandConfig;
import org.dbcbc.sdk.config.DDLConfig;
import org.dbcbc.sdk.connector.ConfigValidator;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.connector.ConnectorServiceContext;
import org.dbcbc.sdk.enums.ListenerTypeEnum;
import org.dbcbc.sdk.enums.TableTypeEnum;
import org.dbcbc.sdk.listener.Listener;
import org.dbcbc.sdk.model.ConnectorConfig;
import org.dbcbc.sdk.model.MetaInfo;
import org.dbcbc.sdk.model.Table;
import org.dbcbc.sdk.plugin.MetaContext;
import org.dbcbc.sdk.plugin.PluginContext;
import org.dbcbc.sdk.plugin.ReaderContext;
import org.dbcbc.sdk.schema.SchemaResolver;
import org.dbcbc.sdk.storage.StorageService;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 连接器基础功能
 *
 * @param <I> ConnectorInstance
 * @param <C> ConnectorConfig
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-19 23:24
 */
public interface ConnectorService<I extends ConnectorInstance, C extends ConnectorConfig> {

    /**
     * 连接器类型
     */
    String getConnectorType();

    /**
     * 可扩展的表类型
     *
     * @return
     */
    TableTypeEnum getExtendedTableType();

    /**
     * 获取配置对象
     *
     * @return
     */
    Class<C> getConfigClass();

    /**
     * 建立连接
     *
     * @param connectorConfig
     * @param context
     * @return
     */
    ConnectorInstance connect(C connectorConfig, ConnectorServiceContext context);

    /**
     * 连接器配置校验器
     *
     * @return
     */
    ConfigValidator getConfigValidator();

    /**
     * 断开连接
     *
     * @param connectorInstance
     */
    void disconnect(I connectorInstance);

    /**
     * 检查连接器是否连接正常
     *
     * @param connectorInstance
     * @return
     */
    boolean isAlive(I connectorInstance);

    /**
     * 获取所有的数据库
     *
     * @param connectorInstance
     * @return
     */
    default List<String> getDatabases(I connectorInstance) {
        return Collections.emptyList();
    }

    /**
     * 获取指定数据库名的Schema
     *
     * @param connectorInstance
     * @param catalog
     * @return
     */
    default List<String> getSchemas(I connectorInstance, String catalog) {
        return Collections.emptyList();
    }

    /**
     * 获取所有表名
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    List<Table> getTable(I connectorInstance, ConnectorServiceContext context);

    /**
     * 获取表元信息
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    List<MetaInfo> getMetaInfo(I connectorInstance, ConnectorServiceContext context);

    /**
     * 获取总数
     *
     * @param connectorInstance
     * @param metaContext
     * @return
     */
    long getCount(I connectorInstance, MetaContext metaContext);

    /**
     * 分页获取数据源数据
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    Result reader(I connectorInstance, ReaderContext context);

    /**
     * 批量写入目标源数据
     *
     * @param connectorInstance
     * @param context
     * @return
     */
    Result writer(I connectorInstance, PluginContext context);

    /**
     * 执行DDL命令
     *
     * @param connectorInstance
     * @param ddlConfig
     * @return
     */
    default Result writerDDL(I connectorInstance, DDLConfig ddlConfig) {
        throw new SdkException("Unsupported method.");
    }

    /**
     * 获取数据源同步参数
     *
     * @param commandConfig
     * @return
     */
    Map<String, String> getSourceCommand(CommandConfig commandConfig);

    /**
     * 获取目标源同步参数
     *
     * @param commandConfig
     * @return
     */
    Map<String, String> getTargetCommand(CommandConfig commandConfig);

    /**
     * 获取监听器
     *
     * @param listenerType {@link ListenerTypeEnum}
     * @return
     */
    Listener getListener(String listenerType);

    /**
     * 获取存储服务
     *
     * @return
     */
    default StorageService getStorageService() {
        return null;
    }

    /**
     * 获取标准数据类型解析器
     *
     * @return
     */
    SchemaResolver getSchemaResolver();

    /**
     * 获取指定时间的位点信息
     *
     * @param connectorInstance
     * @return
     */
    default Object getPosition(I connectorInstance) {
        return StringUtil.EMPTY;
    }
}
