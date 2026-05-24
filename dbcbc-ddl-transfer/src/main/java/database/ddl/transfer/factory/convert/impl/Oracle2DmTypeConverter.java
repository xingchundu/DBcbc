package database.ddl.transfer.factory.convert.impl;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.factory.convert.BaseTypeConverter;
import database.ddl.transfer.utils.StringUtil;

import java.util.Map;

/**
 * Oracle 至达梦（DM）的数据类型转换（DDL 结构迁移，类型高度兼容 Oracle）
 */
public class Oracle2DmTypeConverter extends BaseTypeConverter {

    public Oracle2DmTypeConverter(Map<String, String> typeMapping, Map<String, String> typeProperties) {
        super(typeMapping, typeProperties);
    }

    @Override
    protected String getOracleUnmappedFallbackType() {
        return "VARCHAR2";
    }

    @Override
    protected String getUnmappedFallbackType(Column column) {
        return preserveSourceType(column);
    }

    @Override
    public String convert(Column column) {
        if (column == null) {
            return null;
        }
        String mapped = typeMapping.get(column.getColumnType() == null ? null : column.getColumnType().toUpperCase());
        return mapped != null ? mapped : preserveSourceType(column);
    }

    private String preserveSourceType(Column column) {
        if (!StringUtil.isBlank(column.getColumnType())) {
            return column.getColumnType().toUpperCase();
        }
        StringBuilder sb = new StringBuilder(column.getDataType().toUpperCase());
        if (column.isStringType() && column.hasStrLength()) {
            sb.append("(").append(column.getStrLength()).append(")");
        } else if (column.hasPrecision()) {
            sb.append("(").append(column.getPrecision());
            if (column.hasScale()) {
                sb.append(",").append(column.getScale());
            }
            sb.append(")");
        }
        return sb.toString();
    }
}
