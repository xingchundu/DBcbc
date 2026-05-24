package org.dbcbc.biz.vo;

import org.dbcbc.parser.model.Connector;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/10 17:20
 */
public class ConnectorVO extends Connector {

    // 是否运行
    private boolean running;

    /** 最近一次保存时的连接错误（如有） */
    private String connectionError;

    public ConnectorVO(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public String getConnectionError() {
        return connectionError;
    }

    public void setConnectionError(String connectionError) {
        this.connectionError = connectionError;
    }
}
