/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbcbc.parser;

import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.model.TableGroupPicker;

import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-01-16 23:48
 */
public interface TableGroupContext {

    void put(Mapping mapping, List<TableGroup> tableGroups);

    void update(Mapping mapping, List<TableGroup> tableGroups);

    List<TableGroupPicker> getTableGroupPickers(String metaId, String tableName);

    void clear(String metaId);
}
