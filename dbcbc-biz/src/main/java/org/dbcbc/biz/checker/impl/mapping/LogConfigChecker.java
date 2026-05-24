package org.dbcbc.biz.checker.impl.mapping;

import org.dbcbc.biz.checker.MappingConfigChecker;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.sdk.config.ListenerConfig;
import org.dbcbc.sdk.enums.ListenerTypeEnum;

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * 日志配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class LogConfigChecker implements MappingConfigChecker {

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        ListenerConfig listener = mapping.getListener();
        Assert.notNull(listener, "ListenerConfig can not be null.");

        listener.setListenerType(ListenerTypeEnum.LOG.getType());
    }
}
