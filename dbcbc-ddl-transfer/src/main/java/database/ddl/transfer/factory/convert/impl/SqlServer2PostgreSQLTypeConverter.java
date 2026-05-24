package database.ddl.transfer.factory.convert.impl;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.factory.convert.BaseTypeConverter;

import java.util.Map;

/**
 * SQL Server → PostgreSQL 数据类型转换器（用于表结构迁移阶段）
 */
public class SqlServer2PostgreSQLTypeConverter extends BaseTypeConverter {

    public SqlServer2PostgreSQLTypeConverter(Map<String, String> typeMapping,
                                              Map<String, String> typeProperties) {
        super(typeMapping, typeProperties);
    }

    @Override
    protected String getOracleUnmappedFallbackType() {
        return "TEXT";
    }

    @Override
    public String convert(Column column) {
        // 优先使用 columnType，退回到 dataType
        String type = column.getColumnType();
        if (type == null || type.trim().isEmpty()) {
            type = column.getDataType();
        }
        if (type == null || type.trim().isEmpty()) return "TEXT";
        String mapped = typeMapping.get(type.toUpperCase().trim());
        return mapped != null ? mapped : "TEXT";
    }
}
