/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.parser.util;

import org.dbcbc.parser.model.Mapping;
import org.dbcbc.sdk.connector.DefaultConnectorServiceContext;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-05 22:56
 */
public abstract class ConnectorServiceContextUtil {

    public static DefaultConnectorServiceContext buildConnectorServiceContext(Mapping mapping, boolean isSource) {
        DefaultConnectorServiceContext context = new DefaultConnectorServiceContext();
        context.setCatalog(isSource ? mapping.getSourceDatabase() : mapping.getTargetDatabase());
        context.setSchema(isSource ? mapping.getSourceSchema() : mapping.getTargetSchema());
        context.setMappingId(mapping.getId());
        context.setConnectorId(isSource ? mapping.getSourceConnectorId() : mapping.getTargetConnectorId());
        context.setSuffix(isSource ? ConnectorInstanceUtil.SOURCE_SUFFIX : ConnectorInstanceUtil.TARGET_SUFFIX);
        return context;
    }
}
