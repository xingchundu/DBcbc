package database.ddl.transfer.factory.convert.impl;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.factory.convert.BaseTypeConverter;

import java.util.Locale;
import java.util.Map;

/**
 * Mysql至PostgreSQL的数据类型转换
 *
 * @author gs
 */
public class MySQL2PostgreSQLTypeConverter extends BaseTypeConverter {

    /**
     * MySQL VARCHAR 长度达到该阈值时映射为 PostgreSQL TEXT，避免长文本写入 varchar(n) 失败
     */
    private static final int VARCHAR_TEXT_THRESHOLD = 1024;

    public MySQL2PostgreSQLTypeConverter(Map<String, String> typeMapping, Map<String, String> typeProperties) {
        super(typeMapping, typeProperties);
    }

    @Override
    public DataBaseDefine convert(DataBaseDefine dataBaseDefine) {
        DataBaseDefine converted = super.convert(dataBaseDefine);
        for (Table table : converted.getTablesMap().values()) {
            for (Column column : table.getColumns()) {
                expandLongVarcharToText(column);
            }
        }
        return converted;
    }

    @Override
    public String convert(Column column) {
        return typeMapping.get(column.getColumnType().toUpperCase());
    }

    private void expandLongVarcharToText(Column column) {
        String finalType = column.getFinalConvertDataType();
        if (finalType == null) {
            return;
        }
        String upper = finalType.trim().toUpperCase(Locale.ROOT);
        if (!upper.startsWith("VARCHAR(")) {
            return;
        }
        int end = upper.indexOf(')');
        if (end <= 8) {
            return;
        }
        try {
            int length = Integer.parseInt(upper.substring(8, end).trim());
            if (length >= VARCHAR_TEXT_THRESHOLD) {
                column.setFinalConvertDataType("TEXT");
                column.setDataType("TEXT");
            }
        } catch (NumberFormatException ignored) {
            // 保持原映射类型
        }
    }
}
