/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.parser.strategy.impl;

import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.strategy.GroupStrategy;
import org.dbcbc.sdk.constant.ConfigConstant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:53
 */
public final class TableGroupStrategy implements GroupStrategy<TableGroup> {

    @Override
    public String getGroupId(TableGroup model) {
        String mappingId = model.getMappingId();
        return new StringBuilder(ConfigConstant.TABLE_GROUP).append("_").append(mappingId).toString();
    }
}
