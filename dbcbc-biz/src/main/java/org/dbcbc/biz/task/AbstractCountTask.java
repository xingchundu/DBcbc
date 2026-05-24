/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbcbc.biz.task;

import org.dbcbc.biz.TableGroupService;
import org.dbcbc.common.dispatch.AbstractDispatchTask;
import org.dbcbc.common.rsa.RsaManager;
import org.dbcbc.connector.base.ConnectorFactory;
import org.dbcbc.parser.ParserComponent;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.util.ConnectorInstanceUtil;
import org.dbcbc.parser.util.PickerUtil;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.connector.DefaultMetaContext;
import org.dbcbc.sdk.enums.ModelEnum;
import org.dbcbc.sdk.model.ConnectorConfig;
import org.dbcbc.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.Map;

/**
 * 抽象类统计驱动总数任务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-06-25 01:00
 */
public abstract class AbstractCountTask extends AbstractDispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected String mappingId;

    protected ParserComponent parserComponent;

    protected ProfileComponent profileComponent;

    protected TableGroupService tableGroupService;

    protected ConnectorFactory connectorFactory;

    private RsaManager rsaManager;

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public void setParserComponent(ParserComponent parserComponent) {
        this.parserComponent = parserComponent;
    }

    public void setProfileComponent(ProfileComponent profileComponent) {
        this.profileComponent = profileComponent;
    }

    public void setTableGroupService(TableGroupService tableGroupService) {
        this.tableGroupService = tableGroupService;
    }

    public void setConnectorFactory(ConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    public void setRsaManager(RsaManager rsaManager) {
        this.rsaManager = rsaManager;
    }

    protected void updateTableGroupCount(Mapping mapping, TableGroup tableGroup) {
        long now = Instant.now().toEpochMilli();
        TableGroup group = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
        Map<String, String> command = parserComponent.getCommand(mapping, group);
        String sourceConnectorId = mapping.getSourceConnectorId();
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mappingId, sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
        ConnectorConfig config = profileComponent.getConnector(sourceConnectorId).getConfig();
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        Assert.notNull(command, "command can not null");
        ConnectorService connectorService = connectorFactory.getConnectorService(config);

        DefaultMetaContext metaContext = new DefaultMetaContext();
        metaContext.setCommand(command);
        metaContext.setSourceTable(group.getSourceTable());
        metaContext.setSourceConnectorInstance(connectorInstance);
        setRsaConfig(metaContext);

        long count = connectorService.getCount(connectorInstance, metaContext);
        tableGroup.getSourceTable().setCount(count);
        profileComponent.editConfigModel(tableGroup);
        logger.info("{}表{}, 总数:{}, {}ms", mapping.getName(), tableGroup.getSourceTable().getName(), count, (Instant.now().toEpochMilli() - now));
    }

    protected boolean shouldStop(Mapping mapping) {
        return !isRunning() || !ModelEnum.isFull(mapping.getModel());
    }

    private void setRsaConfig(DefaultMetaContext context) {
        if (profileComponent.getSystemConfig().isEnableOpenAPI()) {
            context.setRsaManager(rsaManager);
            context.setRsaConfig(profileComponent.getSystemConfig().getRsaConfig());
        }
    }
}
