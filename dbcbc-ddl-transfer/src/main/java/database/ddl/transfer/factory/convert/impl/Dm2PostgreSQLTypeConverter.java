package database.ddl.transfer.factory.convert.impl;

import java.util.Map;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.factory.convert.BaseTypeConverter;

public class Dm2PostgreSQLTypeConverter extends BaseTypeConverter {

    public Dm2PostgreSQLTypeConverter(Map<String, String> typeMapping, Map<String, String> typeProperties) {
        super(typeMapping, typeProperties);
    }

    @Override
    protected String getOracleUnmappedFallbackType() {
        return "TEXT";
    }

    @Override
    public String convert(Column column) {
        return typeMapping.get(column.getColumnType().toUpperCase());
    }
}
