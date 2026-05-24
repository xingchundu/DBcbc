/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.sdk.connector;

import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.sdk.SdkException;
import org.dbcbc.sdk.constant.ConnectorConstant;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.plugin.PluginContext;
import org.dbcbc.sdk.schema.SchemaResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public void convertProcessBeforeWriter(PluginContext context, SchemaResolver targetResolver) {
        if (CollectionUtils.isEmpty(context.getTargetFields()) || CollectionUtils.isEmpty(context.getTargetList()) || targetResolver == null) {
            return;
        }

        for (Map row : context.getTargetList()) {
            for (Field f : context.getTargetFields()) {
                if (null == f) {
                    continue;
                }
                try {
                    row.compute(f.getName(), (k, v)->targetResolver.convert(v, f));
                } catch (Exception e) {
                    logger.error(String.format("convert value error: (%s, %s, %s)", context.getTargetTable().getName(), f.getName(), row.get(f.getName())), e);
                    throw new SdkException(e);
                }
            }
        }
    }

    protected boolean isUpdate(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_UPDATE, event);
    }

    protected boolean isInsert(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_INSERT, event);
    }

    protected boolean isDelete(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_DELETE, event);
    }
}
