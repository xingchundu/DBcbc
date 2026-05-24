/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.biz.checker.impl.connector;

import org.dbcbc.biz.BizException;
import org.dbcbc.biz.checker.AbstractChecker;
import org.dbcbc.biz.util.ConnectionErrorUtil;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.base.ConnectorFactory;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.model.ConfigModel;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.sdk.connector.ConfigValidator;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.constant.ConfigConstant;
import org.dbcbc.sdk.model.ConnectorConfig;
import org.dbcbc.sdk.spi.ConnectorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
@Component
public class ConnectorChecker extends AbstractChecker {

    public static final String CONNECTION_ERROR_KEY = "connectionError";

    private static final int MAX_CONNECT_ATTEMPTS = 3;

    private static final long CONNECT_TIMEOUT_MS = 30_000L;

    private final ExecutorService connectExecutor = Executors.newCachedThreadPool();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        printParams(params);
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        String connectorType = params.get("connectorType");
        Assert.hasText(name, "connector name is empty.");
        Assert.hasText(connectorType, "connector connectorType is empty.");

        Connector connector = new Connector();
        connector.setName(name);
        ConnectorConfig config = getConfig(connectorType);
        connector.setConfig(config);
        // 修改基本配置
        this.modifyConfigModel(connector, params);
        // 校验并修改配置
        validateAndModifyConfig(config, params);
        // 连接并获取数据库列表
        connectAndLoadDatabases(connector, config);

        return connector;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        printParams(params);
        Assert.notEmpty(params, "ConnectorChecker check params is null.");
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Connector connector = profileComponent.getConnector(id);
        Assert.notNull(connector, "Can not find connector.");
        ConnectorConfig config = connector.getConfig();
        // 修改基本配置
        this.modifyConfigModel(connector, params);
        // 校验并修改配置
        validateAndModifyConfig(config, params);
        // 获取数据库列表
        connectAndLoadDatabases(connector, config);

        return connector;
    }

    /**
     * 校验并修改配置
     */
    private void validateAndModifyConfig(ConnectorConfig config, Map<String, String> params) {
        ConnectorService connectorService = connectorFactory.getConnectorService(config.getConnectorType());
        ConfigValidator configValidator = connectorService.getConfigValidator();
        Assert.notNull(configValidator, "ConfigValidator can not be null.");
        configValidator.modify(connectorService, config, params);
    }

    public static String getConnectionError(Connector connector) {
        if (connector == null || connector.getConfig() == null) {
            return null;
        }
        Properties extInfo = connector.getConfig().getExtInfo();
        if (extInfo == null) {
            return null;
        }
        return extInfo.getProperty(CONNECTION_ERROR_KEY);
    }

    /**
     * 连接并加载数据库列表（最多重试 3 次，总耗时不超过 30 秒；失败仍保存配置并记录错误）
     */
    private void connectAndLoadDatabases(Connector connector, ConnectorConfig config) {
        Properties extInfo = config.getExtInfo();
        if (extInfo == null) {
            extInfo = new Properties();
            config.setExtInfo(extInfo);
        }
        extInfo.remove(CONNECTION_ERROR_KEY);

        long deadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
        String lastError = null;
        int attemptsMade = 0;

        for (int attempt = 1; attempt <= MAX_CONNECT_ATTEMPTS; attempt++) {
            if (System.currentTimeMillis() >= deadline) {
                lastError = ConnectionErrorUtil.formatTimeoutMessage(config);
                break;
            }
            attemptsMade = attempt;
            long remainingMs = deadline - System.currentTimeMillis();
            if (remainingMs <= 0) {
                lastError = ConnectionErrorUtil.formatTimeoutMessage(config);
                break;
            }
            try {
                List<String> databases = tryConnectAndLoadDatabases(connector, config, remainingMs);
                connector.setDatabases(databases != null ? databases : Collections.emptyList());
                return;
            } catch (TimeoutException e) {
                lastError = ConnectionErrorUtil.formatTimeoutMessage(config);
                safeDisconnect(connector.getId());
                break;
            } catch (Exception e) {
                lastError = ConnectionErrorUtil.describe(e, config);
                logger.warn("Connector connect attempt {}/{} failed: {}", attempt, MAX_CONNECT_ATTEMPTS, lastError);
                safeDisconnect(connector.getId());
            }
        }

        connector.setDatabases(Collections.emptyList());
        String message;
        if (lastError != null && lastError.startsWith("连接超时（30秒）")) {
            message = lastError + "，已保存配置但未获取数据库列表";
        } else {
            message = String.format("连接失败（已尝试%d次）：%s", attemptsMade > 0 ? attemptsMade : MAX_CONNECT_ATTEMPTS, lastError);
        }
        extInfo.setProperty(CONNECTION_ERROR_KEY, message);
    }

    private List<String> tryConnectAndLoadDatabases(Connector connector, ConnectorConfig config, long timeoutMs)
            throws Exception {
        Callable<List<String>> task = () -> {
            ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), config, StringUtil.EMPTY, StringUtil.EMPTY);
            ConnectorService connectorService = connectorFactory.getConnectorService(config.getConnectorType());
            return connectorService.getDatabases(connectorInstance);
        };
        Future<List<String>> future = connectExecutor.submit(task);
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    private void safeDisconnect(String connectorId) {
        if (StringUtil.isBlank(connectorId)) {
            return;
        }
        try {
            connectorFactory.disconnect(connectorId);
        } catch (Exception e) {
            logger.debug("Disconnect after failed connect: {}", e.getMessage());
        }
    }

    private ConnectorConfig getConfig(String connectorType) {
        try {
            ConnectorService connectorService = connectorFactory.getConnectorService(connectorType);
            Class<ConnectorConfig> configClass = connectorService.getConfigClass();
            ConnectorConfig config = configClass.newInstance();
            config.setConnectorType(connectorService.getConnectorType());
            return config;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new BizException("获取连接器配置异常.");
        }
    }
}
