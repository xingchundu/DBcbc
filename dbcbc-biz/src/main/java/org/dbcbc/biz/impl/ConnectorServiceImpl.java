/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.biz.impl;

import org.dbcbc.biz.BizException;
import org.dbcbc.biz.ConnectorService;
import org.dbcbc.biz.checker.Checker;
import org.dbcbc.biz.checker.impl.connector.ConnectorChecker;
import org.dbcbc.biz.util.ConnectionErrorUtil;
import org.dbcbc.biz.vo.ConnectorVO;
import org.dbcbc.common.model.Paging;
import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.JsonUtil;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.base.ConnectorFactory;
import org.dbcbc.parser.LogService;
import org.dbcbc.parser.LogType;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.model.ConfigModel;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.util.ConnectorInstanceUtil;
import org.dbcbc.sdk.config.DatabaseConfig;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.constant.ConfigConstant;
import org.dbcbc.sdk.model.ConnectorConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.util.Properties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class ConnectorServiceImpl extends BaseServiceImpl implements ConnectorService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, Boolean> health = new ConcurrentHashMap<>();

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private LogService logService;

    @Resource
    private Checker connectorChecker;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.INSERT, model);
        logConnectionErrorIfPresent(model);

        return profileComponent.addConfigModel(model);
    }

    @Override
    public String copy(String id) {
        Connector connector = profileComponent.getConnector(id);
        Assert.notNull(connector, "The connector id is invalid.");

        ConnectorConfig config = connector.getConfig();
        Map params = JsonUtil.parseMap(config);
        params.put("properties", config.getPropertiesText());
        params.put("extInfo", JsonUtil.objToJson(config.getExtInfo()));
        params.put(ConfigConstant.CONFIG_MODEL_NAME, connector.getName() + "(复制)");
        ConfigModel model = connectorChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.COPY, model);
        logConnectionErrorIfPresent(model);

        return profileComponent.addConfigModel(model);
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = connectorChecker.checkEditConfigModel(params);
        log(LogType.ConnectorLog.UPDATE, model);
        logConnectionErrorIfPresent(model);

        return profileComponent.editConfigModel(model);
    }

    @Override
    public String remove(String id) {
        List<Mapping> mappingAll = profileComponent.getMappingAll();
        if (!CollectionUtils.isEmpty(mappingAll)) {
            mappingAll.forEach(mapping-> {
                if (StringUtil.equals(mapping.getSourceConnectorId(), id) || StringUtil.equals(mapping.getTargetConnectorId(), id)) {
                    String error = String.format("驱动“%s”正在使用，请先删除", mapping.getName());
                    logger.error(error);
                    throw new BizException(error);
                }
            });
        }

        Connector connector = profileComponent.getConnector(id);
        if (connector != null) {
            connectorFactory.disconnect(connector.getId());
            log(LogType.ConnectorLog.DELETE, connector);
            profileComponent.removeConfigModel(id);
        }
        return "删除连接器成功!";
    }

    @Override
    public Connector getConnector(String id) {
        return profileComponent.getConnector(id);
    }

    @Override
    public List<String> getDatabase(String id) {
        Connector connector = profileComponent.getConnector(id);
        return connector != null ? connector.getDatabases() : Collections.emptyList();
    }

    @Override
    public String refreshConnectorDatabases(String id) {
        Assert.hasText(id, "连接器 id 不能为空");
        Connector connector = profileComponent.getConnector(id);
        Assert.notNull(connector, "连接不存在");
        ConnectorConfig config = connector.getConfig();
        if (!(config instanceof DatabaseConfig)) {
            return String.format("[%s] 非关系型库连接，已跳过", connector.getName());
        }
        org.dbcbc.sdk.spi.ConnectorService connectorSpi = connectorFactory.getConnectorService(config.getConnectorType());
        ConnectorInstance instance = connectorFactory.connect(connector.getId());
        List<String> databases = connectorSpi.getDatabases(instance);
        connector.setDatabases(databases != null ? databases : Collections.emptyList());
        profileComponent.editConfigModel(connector);
        int n = connector.getDatabases() == null ? 0 : connector.getDatabases().size();
        return String.format("[%s] 已从数据源重新加载库列表（共 %d 个）", connector.getName(), n);
    }

    @Override
    public List<String> getSchema(String id, String database) {
        Connector connector = profileComponent.getConnector(id);
        if (connector != null) {
            ConnectorConfig config = connector.getConfig();
            org.dbcbc.sdk.spi.ConnectorService connectorService = connectorFactory.getConnectorService(config.getConnectorType());
            ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId());
            return connectorService.getSchemas(connectorInstance, database);
        }
        return Collections.emptyList();
    }

    @Override
    public List<ConnectorVO> getConnectorAll() {
        return profileComponent.getConnectorAll().stream().map(this::convertConnector2Vo).sorted(Comparator.comparing(Connector::getUpdateTime).reversed()).collect(Collectors.toList());
    }

    @Override
    public Paging<ConnectorVO> search(Map<String, String> params) {
        return searchConfigModel(params, getConnectorAll());
    }

    @Override
    public List<String> getConnectorTypeAll() {
        ArrayList<String> connectorTypes = new ArrayList<>(connectorFactory.getConnectorTypeAll());
        connectorTypes.sort(Comparator.comparing(String::toString));
        return connectorTypes;
    }

    @Override
    public void refreshHealth() {
        List<Connector> list = profileComponent.getConnectorAll();
        if (CollectionUtils.isEmpty(list)) {
            if (!CollectionUtils.isEmpty(health)) {
                health.clear();
            }
            return;
        }

        // 更新连接器状态
        Set<String> exist = new HashSet<>();
        list.forEach(c-> {
            health.put(c.getId(), isAlive(c.getId(), c.getConfig()));
            exist.add(c.getId());
        });

        // 移除删除的连接器
        Set<String> remove = new HashSet<>();
        for (Map.Entry<String, Boolean> entry : health.entrySet()) {
            if (!exist.contains(entry.getKey())) {
                remove.add(entry.getKey());
            }
        }

        if (!CollectionUtils.isEmpty(remove)) {
            remove.forEach(health::remove);
        }
    }

    @Override
    public boolean isAlive(String id) {
        return health.getOrDefault(id, false);
    }

    @Override
    public String testConnection(String id) {
        Connector connector = profileComponent.getConnector(id);
        Assert.notNull(connector, "连接不存在");
        ConnectorConfig config = connector.getConfig();
        Assert.notNull(config, "连接配置不存在");
        try {
            ConnectorInstance instance = connectorFactory.connect(connector.getId(), config, StringUtil.EMPTY, StringUtil.EMPTY);
            org.dbcbc.sdk.spi.ConnectorService connectorSpi = connectorFactory.getConnectorService(config.getConnectorType());
            boolean alive = connectorSpi.isAlive(instance);
            if (!alive) {
                String error = String.format("连接测试失败[%s]：数据源无响应", connector.getName());
                onTestConnectionFailed(connector, error);
                return error;
            }
            clearConnectionError(connector);
            health.put(connector.getId(), true);
            return null;
        } catch (Exception e) {
            String error = String.format("连接测试失败[%s]：%s", connector.getName(), ConnectionErrorUtil.describe(e, connector.getConfig()));
            onTestConnectionFailed(connector, error);
            safeDisconnect(connector.getId());
            health.put(connector.getId(), false);
            return error;
        }
    }

    private void onTestConnectionFailed(Connector connector, String error) {
        persistConnectionError(connector, error);
        LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
        logService.log(logType, "%s%s:%s %s", logType.getMessage(), logType.getName(), connector.getName(), error);
        logger.warn("Connector test failed: {}", error);
    }

    private void clearConnectionError(Connector connector) {
        ConnectorConfig config = connector.getConfig();
        if (config == null || config.getExtInfo() == null) {
            return;
        }
        if (config.getExtInfo().remove(ConnectorChecker.CONNECTION_ERROR_KEY) != null) {
            profileComponent.editConfigModel(connector);
        }
    }

    private void persistConnectionError(Connector connector, String error) {
        ConnectorConfig config = connector.getConfig();
        Properties extInfo = config.getExtInfo();
        if (extInfo == null) {
            extInfo = new Properties();
            config.setExtInfo(extInfo);
        }
        extInfo.setProperty(ConnectorChecker.CONNECTION_ERROR_KEY, error);
        profileComponent.editConfigModel(connector);
    }

    private void safeDisconnect(String connectorId) {
        try {
            connectorFactory.disconnect(connectorId);
        } catch (Exception e) {
            logger.debug("Disconnect after test failed: {}", e.getMessage());
        }
    }

    private void logConnectionErrorIfPresent(ConfigModel model) {
        if (!(model instanceof Connector)) {
            return;
        }
        Connector connector = (Connector) model;
        String connectionError = ConnectorChecker.getConnectionError(connector);
        if (StringUtil.isBlank(connectionError)) {
            return;
        }
        LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
        logService.log(logType, "%s%s:%s %s", logType.getMessage(), logType.getName(), connector.getName(), connectionError);
        logger.warn("Connector save with connection error: [{}] {}", connector.getName(), connectionError);
    }

    @Override
    public Object getPosition(String mappingId) {
        Mapping mapping = profileComponent.getMapping(mappingId);
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX);
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        return connectorFactory.getPosition(connectorInstance);
    }

    private boolean isAlive(String connectorConfigId, ConnectorConfig config) {
        try {
            return connectorFactory.isAlive(connectorConfigId, config);
        } catch (Exception e) {
            LogType.ConnectorLog logType = LogType.ConnectorLog.FAILED;
            logService.log(logType, "%s%s", logType.getName(), e.getMessage());
            return false;
        }
    }

    private ConnectorVO convertConnector2Vo(Connector connector) {
        ConnectorVO vo = new ConnectorVO(isAlive(connector.getId()));
        BeanUtils.copyProperties(connector, vo);
        vo.setConnectionError(ConnectorChecker.getConnectionError(connector));
        return vo;
    }
}