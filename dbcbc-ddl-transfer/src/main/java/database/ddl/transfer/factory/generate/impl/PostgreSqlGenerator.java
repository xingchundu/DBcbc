package database.ddl.transfer.factory.generate.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.PrimaryKey;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.factory.generate.Generator;
import database.ddl.transfer.utils.StringUtil;

/**
 * 针对PostgreSQL数据库的结构生成
 *
 * @author gs
 */
public class PostgreSqlGenerator extends Generator {

	public PostgreSqlGenerator(Connection connection, DataBaseDefine dataBaseDefine, DBSettings targetDBSettings) {
		super(connection, dataBaseDefine, targetDBSettings);
	}

	@Override
	protected String getTableDDL(Table tableDefine) {
		StringBuilder stringBuilder = new StringBuilder("create table ");
		stringBuilder.append("\"").append(tableDefine.getTableName()).append("\"").append("(");

		List<Column> columnList = tableDefine.getColumns();
		for (Column column : columnList) {
			stringBuilder.append(this.getColumnDefineDDL(column));
			stringBuilder.append(",");
		}

		PrimaryKey primaryKey = tableDefine.getPrimaryKey();
		if (primaryKey != null) {
			stringBuilder.append(this.getPrimaryKeyDefineDDL(primaryKey)).append(",");
		}

		stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		stringBuilder.append(");");
		stringBuilder.append(this.getCommentDefineDDL(tableDefine));

		return stringBuilder.toString().toLowerCase();
	}

	/**
	 * 生成主键定义的DDL语句
	 * 
	 * @param primaryKey 主键定义
	 * @return DDL语句
	 */
	private String getPrimaryKeyDefineDDL(PrimaryKey primaryKey) {
		StringBuilder stringBuilder = new StringBuilder("");
		if (!StringUtil.isBlank(primaryKey.getPkName())) {
			stringBuilder.append("constraint ").append("\"" + primaryKey.getPkName() + "\"").append(" primary key(");
		} else {
			stringBuilder.append("primary key(");
		}
		List<String> columnNames = primaryKey.getColumns();
		for (String columnName : columnNames) {
			stringBuilder.append("\"" + columnName + "\"").append(",");
		}
		stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		stringBuilder.append(")");

		return stringBuilder.toString();
	}

	/**
	 * 生成字段定义的DDL语句
	 * 
	 * @param column 字段定义
	 * @return DDL语句
	 */
	private String getColumnDefineDDL(Column column) {
		StringBuilder stringBuilder = new StringBuilder("\"" + column.getColumnName() + "\"");

		String type = column.getFinalConvertDataType();
		stringBuilder.append(" ");
		stringBuilder.append(type);
		if (!column.isNullAble()) {
			stringBuilder.append(" ").append("not null");
		}

		boolean isAutoIncrement = column.getExtra() != null && column.getExtra().toLowerCase().contains("auto_increment");
		if (isAutoIncrement) {
			stringBuilder.append(" GENERATED ALWAYS AS IDENTITY");
		} else if (column.hasDefault()) {
			stringBuilder.append(" default ").append(column.getDefaultDefine());
		}

		return stringBuilder.toString();
	}

	private String getCommentDefineDDL(Table tableDefine) {
		StringBuilder stringBuilder = new StringBuilder("");
		List<Column> columns = tableDefine.getColumns();
		for (Column column : columns) {
			if (!StringUtil.isBlank(column.getColumnComment())) {
				String escapedComment = column.getColumnComment().replace("'", "''");
				stringBuilder.append("COMMENT ON COLUMN \"").append(tableDefine.getTableName()).append("\".\"").append(column.getColumnName()).append("\" IS '").append(escapedComment)
						.append("';");
			}
		}
		if (!StringUtil.isBlank(tableDefine.getTableComment())) {
			String escapedComment = tableDefine.getTableComment().replace("'", "''");
			stringBuilder.append("COMMENT ON TABLE \"").append(tableDefine.getTableName()).append("\" IS '").append(escapedComment).append("'");
		}
		return stringBuilder.toString();
	}

	@Override
	protected String getDataBaseDDL(DataBaseDefine dataBaseDefine) {
		String sql = "select u.datname from pg_catalog.pg_database u where u.datname = '" + dataBaseDefine.getCatalog() + "';";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		boolean flag = false;
		try {
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			if(resultSet.next()) {
				flag = true;
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取postgresql表定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		StringBuilder stringBuilder = new StringBuilder("");
		if (!flag) {
			stringBuilder.append("CREATE DATABASE ").append("\"").append(dataBaseDefine.getCatalog()).append("\" ")
					.append("ENCODING '").append(resolvePostgreSqlEncoding(dataBaseDefine.getCharacterSetDataBase())).append("';");
		}

		return stringBuilder.toString();
	}

	/**
	 * MySQL 源库字符集（如 utf8mb3、utf8mb4）需映射为 PostgreSQL 合法 ENCODING 名（如 UTF8）。
	 */
	private String resolvePostgreSqlEncoding(String characterSet) {
		if (StringUtil.isBlank(characterSet)) {
			return "UTF8";
		}
		String lower = characterSet.trim().toLowerCase();
		if (lower.contains("utf8") || lower.contains("utf-8")) {
			return "UTF8";
		}
		if (lower.contains("gbk") || lower.contains("gb2312") || lower.contains("gb18030")) {
			return "UTF8";
		}
		if (lower.contains("latin1") || lower.equals("cp1252") || lower.equals("iso88591")) {
			return "LATIN1";
		}
		if (lower.equals("sql_ascii")) {
			return "SQL_ASCII";
		}
		if (lower.equals("euc_cn") || lower.equals("euc_jp") || lower.equals("euc_kr") || lower.equals("euc_tw")) {
			return lower.toUpperCase();
		}
		String upper = characterSet.trim().toUpperCase();
		if (upper.equals("UTF8") || upper.equals("LATIN1") || upper.equals("SQL_ASCII")
				|| upper.startsWith("EUC_") || upper.startsWith("WIN")) {
			return upper;
		}
		logger.warn("未识别的字符集 {}，CREATE DATABASE 将使用 UTF8", characterSet);
		return "UTF8";
	}

	@Override
	protected List<String> getModifiedColumnDDL(Table sourceTableDefine, Table targetTableDefine) {
		StringBuilder stringBuilder = null;
		List<String> resultList = new LinkedList<>();
		// 主键列名（postgresql主键列，修改类型会直接报错，放弃主键列类型修改）
		List<String> primaryKeys = new ArrayList<>();
		if(targetTableDefine.getPrimaryKey() != null) {
			primaryKeys = targetTableDefine.getPrimaryKey().getColumns();
		}
		for (Column sourceColumn : sourceTableDefine.getColumns()) {
			stringBuilder = new StringBuilder("");
			String columnName = sourceColumn.getColumnName();
			Column targetColumn = targetTableDefine.getColumnsMap().get(columnName);
			if (targetColumn == null) {
				// 字段不存在直接添加
				stringBuilder.append("ALTER TABLE \"").append(sourceTableDefine.getTableName()).append("\" ADD COLUMN ").append(columnName).append(" ").append(sourceColumn.getFinalConvertDataType())
						.append(" ");
				if (!sourceColumn.isNullAble()) {
					stringBuilder.append("NOT NULL");
				} else {
					stringBuilder.append("NULL");
				}
				stringBuilder.append(";");

				if (!StringUtil.isBlank(sourceColumn.getColumnComment())) {
					String escapedComment = sourceColumn.getColumnComment().replace("'", "''");
					stringBuilder.append("COMMENT ON COLUMN \"").append(sourceTableDefine.getTableName()).append("\".\"").append(columnName).append("\" IS '").append(escapedComment).append("';");
				}
			} else {
				if (sourceColumn.equals(targetColumn)) {
					continue;
				} else {
					// 由于不同数据库类型转换后与实际查询的类型存在不一致，导致不应该修改类型的字段也会再次执行类型修改操作，表数据量大时影响性能，暂关闭类型修改功能
//					if(!sourceColumn.getDataType().equals(targetColumn.getDataType()) && !primaryKeys.contains(targetColumn.getColumnName())) {
//						stringBuilder.append("ALTER TABLE \"").append(sourceDataBaseDefine.getCatalog()).append("\".\"public\".\"").append(sourceTableDefine.getTableName()).append("\" ALTER COLUMN ").append(columnName).append(" type ").append(sourceColumn.getFinalConvertDataType())
//						.append(";");
//					}
//
//					if (!StringUtil.isBlank(sourceColumn.getColumnComment()) && !sourceColumn.getColumnComment().equals(targetColumn.getColumnComment())) {
//						stringBuilder.append("COMMENT ON COLUMN \"").append(sourceDataBaseDefine.getCatalog()).append("\".\"public\".\"").append(sourceTableDefine.getTableName()).append("\" IS '").append(sourceColumn.getColumnComment()).append("';");
//					}
//					
//					if(!sourceColumn.isNullAble() == targetColumn.isNullAble() && !targetColumn.isNullAble()) {
//						// 删除非空校验
//						stringBuilder.append("ALTER TABLE \"").append(sourceDataBaseDefine.getCatalog()).append("\".\"public\".\"").append(sourceTableDefine.getTableName()).append("\" ALTER COLUMN ").append(columnName).append(" drop not null;");
//					}
				}
			}
			if(!StringUtil.isBlank(stringBuilder.toString())) {
				resultList.add(stringBuilder.toString());
			}
		}
		return resultList;
	}
}
