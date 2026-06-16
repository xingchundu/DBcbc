/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.logminer;

import org.dbcbc.sdk.model.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * 达梦 LogMiner SQL 解析辅助：列名大小写与 LogMiner 输出对齐。
 */
public final class DmLogMinerParser {

    private DmLogMinerParser() {
    }

    public static List<Field> toUpperCaseFields(List<Field> fields) {
        List<Field> normalized = new ArrayList<>(fields.size());
        for (Field field : fields) {
            Field copy = new Field();
            copy.setName(field.getName() == null ? null : field.getName().toUpperCase());
            copy.setTypeName(field.getTypeName());
            copy.setType(field.getType());
            copy.setPk(field.isPk());
            copy.setLabelName(field.getLabelName());
            copy.setColumnSize(field.getColumnSize());
            copy.setRatio(field.getRatio());
            normalized.add(copy);
        }
        return normalized;
    }
}
