/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.manager.impl;

import org.dbcbc.common.rsa.RsaManager;
import org.dbcbc.common.scheduled.ScheduledTaskJob;
import org.dbcbc.common.scheduled.ScheduledTaskService;
import org.dbcbc.connector.base.ConnectorFactory;
import org.dbcbc.manager.AbstractPuller;
import org.dbcbc.manager.ManagerException;
import org.dbcbc.parser.LogService;
import org.dbcbc.parser.LogType;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.TableGroupContext;
import org.dbcbc.parser.consumer.ParserConsumer;
import org.dbcbc.parser.event.RefreshOffsetEvent;
import org.dbcbc.parser.flush.impl.BufferActuatorRouter;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.Meta;
import org.dbcbc.parser.model.Picker;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.util.ConnectorInstanceUtil;
import org.dbcbc.parser.util.PickerUtil;
import org.dbcbc.plugin.PluginFactory;
import org.dbcbc.sdk.config.ListenerConfig;
import org.dbcbc.sdk.constant.ConnectorConstant;
import org.dbcbc.sdk.enums.ListenerTypeEnum;
import org.dbcbc.sdk.enums.TableTypeEnum;
import org.dbcbc.sdk.listener.AbstractListener;
import org.dbcbc.sdk.listener.AbstractQuartzListener;
import org.dbcbc.sdk.listener.Listener;
import org.dbcbc.sdk.model.ChangedOffset;
import org.dbcbc.sdk.model.ConnectorConfig;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.model.Table;
import org.dbcbc.sdk.model.TableGroupQuartzCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 增量同步
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2020-04-26 15:28
 */
@Component
public final class IncrementPuller extends AbstractPuller implements ApplicationListener<RefreshOffsetEvent>, ScheduledTaskJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private BufferActuatorRouter bufferActuatorRouter;

    @Resource
    private ScheduledTaskService scheduledTaskService;

    @Resource
    private RsaManager rsaManager;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private LogService logService;

    @Resource
    private TableGroupContext tableGroupContext;

    private final Map<String, Listener> map = new ConcurrentHashMap<>();

    @PostConstruct
    private void init() {
        scheduledTaskService.start(3000, this);
    }

    @Override
    public void start(Mapping mapping) {
        final String mappingId = mapping.getId();
        final String metaId = mapping.getMetaId();
        Connector connector = profileComponent.getConnector(mapping.getSourceConnectorId());
        Assert.notNull(connector, "连接器不能为空.");
        Connector targetConnector = profileComponent.getConnector(mapping.getTargetConnectorId());
        Assert.notNull(targetConnector, "目标连接器不能为空.");
        List<TableGroup> list = profileComponent.getSortedTableGroupAll(mappingId);
        Assert.notEmpty(list, "表映射关系不能为空，请先添加源表到目标表关系.");
        Meta meta = profileComponent.getMeta(metaId);
        Assert.notNull(meta, "Meta不能为空.");

        Thread worker = new Thread(()-> {
            try {
                map.computeIfAbsent(metaId, k-> {
                    logger.info("开始增量同步：{}, {}", metaId, mapping.getName());
                    long now = Instant.now().toEpochMilli();
                    meta.setBeginTime(now);
                    meta.setEndTime(now);
                    profileComponent.editConfigModel(meta);
                    tableGroupContext.put(mapping, list);
                    return getListener(mapping, connector, targetConnector, list, meta);
                }).start();
            } catch (Exception e) {
                close(metaId);
                logService.log(LogType.TableGroupLog.INCREMENT_FAILED, String.format("启动驱动失败：[%s], %s", mapping.getName(), e.getMessage()));
                logger.error("运行异常，结束增量同步：{}", metaId, e);
            }
        });
        worker.setName("increment-worker-" + mapping.getId());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public void close(String metaId) {
        map.compute(metaId, (k, listener)-> {
            if (listener != null) {
                listener.close();
            }
            bufferActuatorRouter.unbind(metaId);
            tableGroupContext.clear(metaId);
            publishClosedEvent(metaId);
            logger.info("关闭成功:{}", metaId);
            return null;
        });
    }

    @Override
    public void onApplicationEvent(RefreshOffsetEvent event) {
        ChangedOffset offset = event.getChangedOffset();
        if (offset != null && map.containsKey(offset.getMetaId())) {
            map.get(offset.getMetaId()).refreshEvent(offset);
        }
    }

    @Override
    public void run() {
        // 定时同步增量信息
        map.values().forEach(Listener::flushEvent);
    }

    private Listener getListener(Mapping mapping, Connector connector, Connector targetConnector, List<TableGroup> list, Meta meta) {
        ConnectorConfig connectorConfig = connector.getConfig();
        ListenerConfig listenerConfig = mapping.getListener();
        String listenerType = listenerConfig.getListenerType();

        Listener listener = connectorFactory.getListener(connectorConfig.getConnectorType(), listenerType);
        if (null == listener) {
            throw new ManagerException(String.format("Unsupported listener type \"%s\".", connectorConfig.getConnectorType()));
        }
        listener.register(new ParserConsumer(bufferActuatorRouter, profileComponent, pluginFactory, logService, meta.getId(), list));

        // 默认定时抽取
        if (ListenerTypeEnum.isTiming(listenerType) && listener instanceof AbstractQuartzListener) {
            AbstractQuartzListener quartzListener = (AbstractQuartzListener) listener;
            List<TableGroupQuartzCommand> quartzCommands = list.stream().map(t-> {
                final TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, t);
                final Picker picker = new Picker(group);
                List<Field> fields = picker.getSourceFields();
                Assert.notEmpty(fields, "表字段映射关系不能为空：" + group.getSourceTable().getName() + " > " + group.getTargetTable().getName());
                return new TableGroupQuartzCommand(t.getSourceTable(), fields, t.getTargetTable(), t.getCommand(), group.getPlugin(), group.getPluginExtInfo());
            }).collect(Collectors.toList());
            quartzListener.setMappingName(mapping.getName());
            quartzListener.setReadNum(mapping.getReadNum());
            quartzListener.setCommands(quartzCommands);
        }

        if (listener instanceof AbstractListener) {
            AbstractListener abstractListener = (AbstractListener) listener;
            Set<String> filterTable = new HashSet<>();
            List<Table> sourceTable = new ArrayList<>();
            List<Table> customTable = new ArrayList<>();
            list.forEach(t->addSourceTable(sourceTable, customTable, filterTable, t.getSourceTable()));
            abstractListener.setDatabase(mapping.getSourceDatabase());
            abstractListener.setSchema(mapping.getSourceSchema());
            abstractListener.setConnectorService(connectorFactory.getConnectorService(connectorConfig.getConnectorType()));
            String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), connector.getId(), ConnectorInstanceUtil.SOURCE_SUFFIX);
            String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), targetConnector.getId(), ConnectorInstanceUtil.TARGET_SUFFIX);
            abstractListener.setConnectorInstance(connectorFactory.connect(sourceInstanceId));
            abstractListener.setTargetConnectorInstance(connectorFactory.connect(targetInstanceId));
            abstractListener.setScheduledTaskService(scheduledTaskService);
            setRsaConfig(abstractListener);
            abstractListener.setConnectorConfig(connectorConfig);
            abstractListener.setListenerConfig(listenerConfig);
            abstractListener.setFilterTable(filterTable);
            abstractListener.setSourceTable(sourceTable);
            abstractListener.setCustomTable(customTable);
            abstractListener.setSnapshot(meta.getSnapshot());
            abstractListener.setMetaId(meta.getId());
        }

        listener.init();
        return listener;
    }

    private void setRsaConfig(AbstractListener listener) {
        if (profileComponent.getSystemConfig().isEnableOpenAPI()) {
            listener.setRsaManager(rsaManager);
            listener.setRsaConfig(profileComponent.getSystemConfig().getRsaConfig());
        }
    }

    private void addSourceTable(List<Table> sourceTable, List<Table> customTable, Set<String> filterTable, Table table) {
        switch (TableTypeEnum.getTableType(table.getType())) {
            case TABLE:
            case VIEW:
            case MATERIALIZED_VIEW:
                if (!filterTable.contains(table.getName())) {
                    sourceTable.add(table);
                    filterTable.add(table.getName());
                }
                break;
            case SQL:
            case SEMI:
                if (!filterTable.contains(table.getName())) {
                    customTable.add(table);
                    filterTable.add(table.getName());
                    Object mainTable = table.getExtInfo().get(ConnectorConstant.CUSTOM_TABLE_MAIN);
                    if (mainTable instanceof String) {
                        filterTable.add(String.valueOf(mainTable));
                    }
                }
                break;
            default:
                break;
        }
    }

}