/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.biz.util;

import org.dbcbc.common.util.StringUtil;
import org.dbcbc.sdk.config.DatabaseConfig;
import org.dbcbc.sdk.model.ConnectorConfig;

/**
 * 解析 JDBC / 连接器异常，输出可读的连接失败原因。
 */
public final class ConnectionErrorUtil {

    private ConnectionErrorUtil() {
    }

    public static String describe(Throwable e) {
        return describe(e, null);
    }

    public static String describe(Throwable e, ConnectorConfig config) {
        String reason = resolveReason(e);
        String detail = collectMessages(e);
        String endpoint = formatEndpoint(config);

        if (StringUtil.isNotBlank(reason)) {
            if (StringUtil.isNotBlank(endpoint)) {
                return String.format("%s（%s），详情：%s", reason, endpoint, detail);
            }
            return String.format("%s，详情：%s", reason, detail);
        }
        if (StringUtil.isNotBlank(endpoint)) {
            return String.format("%s，详情：%s", endpoint, detail);
        }
        return detail;
    }

    public static String formatTimeoutMessage(ConnectorConfig config) {
        String endpoint = formatEndpoint(config);
        if (StringUtil.isNotBlank(endpoint)) {
            return String.format("连接超时（30秒），%s 无响应，请检查主机地址、端口及网络是否可达", endpoint);
        }
        return "连接超时（30秒），请检查主机地址、端口及网络是否可达";
    }

    private static String formatEndpoint(ConnectorConfig config) {
        if (!(config instanceof DatabaseConfig)) {
            return null;
        }
        DatabaseConfig dbConfig = (DatabaseConfig) config;
        if (StringUtil.isBlank(dbConfig.getHost())) {
            return null;
        }
        if (dbConfig.getPort() > 0) {
            return String.format("目标 %s:%d", dbConfig.getHost(), dbConfig.getPort());
        }
        return String.format("目标 %s", dbConfig.getHost());
    }

    private static String resolveReason(Throwable e) {
        StringBuilder combined = new StringBuilder();
        Throwable current = e;
        while (current != null) {
            String className = current.getClass().getSimpleName();
            if (className.contains("UnknownHost")) {
                return "主机名无法解析，请检查主机地址是否正确";
            }
            if (StringUtil.isNotBlank(current.getMessage())) {
                combined.append(' ').append(current.getMessage().toLowerCase());
            }
            current = current.getCause();
        }

        String text = combined.toString();
        if (text.contains("connection refused")) {
            return "端口不通或服务未启动，无法建立连接";
        }
        if (text.contains("connect timed out") || text.contains("connection timed out")
                || text.contains("read timed out")) {
            return "连接超时，请检查主机地址、端口及网络是否可达";
        }
        if (text.contains("communications link failure")) {
            return "网络通信失败，请检查主机地址、端口及防火墙设置";
        }
        if (text.contains("no route to host")) {
            return "无法路由至目标主机，请检查网络或主机地址";
        }
        if (text.contains("network is unreachable")) {
            return "网络不可达，请检查主机地址及网络配置";
        }
        if (text.contains("access denied") || text.contains("authentication failed")
                || text.contains("password authentication failed")) {
            return "用户名或密码错误，认证失败";
        }
        if (text.contains("unknown database")) {
            return "数据库不存在，请检查库名是否正确";
        }
        if (text.contains("invalid connection") || text.contains("connection is closed")) {
            return "连接无效或已关闭，请检查连接参数";
        }
        if (text.contains("too many connections")) {
            return "数据库连接数已达上限，请稍后重试或调整最大连接数";
        }
        if (text.contains("ssl") && (text.contains("handshake") || text.contains("certificate"))) {
            return "SSL 握手失败，请检查 SSL 相关配置";
        }
        if (text.contains("could not create connection to database server")) {
            return "无法连接数据库服务器，请检查主机地址、端口及服务是否可用";
        }
        return null;
    }

    private static String collectMessages(Throwable e) {
        StringBuilder sb = new StringBuilder();
        Throwable current = e;
        while (current != null) {
            if (StringUtil.isNotBlank(current.getMessage())) {
                if (sb.length() > 0) {
                    sb.append(" | ");
                }
                sb.append(current.getMessage());
            }
            current = current.getCause();
        }
        if (sb.length() == 0 && e != null) {
            sb.append(e.getClass().getSimpleName());
        }
        return sb.toString();
    }
}
