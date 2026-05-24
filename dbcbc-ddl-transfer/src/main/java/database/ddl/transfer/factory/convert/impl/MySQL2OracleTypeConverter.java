package database.ddl.transfer.factory.convert.impl;

import java.util.Map;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.factory.convert.BaseTypeConverter;

/**
 * MySQL 至 Oracle 的数据类型转换（DDL 结构迁移）
 */
public class MySQL2OracleTypeConverter extends BaseTypeConverter {

	public MySQL2OracleTypeConverter(Map<String, String> typeMapping, Map<String, String> typeProperties) {
		super(typeMapping, typeProperties);
	}

	@Override
	public String convert(Column column) {
		if (column == null || column.getColumnType() == null) {
			return null;
		}
		return typeMapping.get(column.getColumnType().toUpperCase());
	}

	@Override
	protected String getUnmappedFallbackType(Column column) {
		// 目标为 Oracle 时，未映射的 MySQL 源类型不宜使用 TEXT
		if (DataBaseType.MYSQL.equals(column.getDataBaseType())) {
			return "CLOB";
		}
		return super.getUnmappedFallbackType(column);
	}
}
