package database.ddl.transfer.factory.convert.impl;

import java.util.Map;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.factory.convert.BaseTypeConverter;

/**
 * Oracle 至 MySQL 的数据类型转换（DDL 结构迁移）
 */
public class Oracle2MySQLTypeConverter extends BaseTypeConverter {

	public Oracle2MySQLTypeConverter(Map<String, String> typeMapping, Map<String, String> typeProperties) {
		super(typeMapping, typeProperties);
	}

	@Override
	protected String getOracleUnmappedFallbackType() {
		return "LONGTEXT";
	}

	@Override
	public String convert(Column column) {
		if (column == null || column.getColumnType() == null) {
			return null;
		}
		return typeMapping.get(column.getColumnType().toUpperCase());
	}
}
