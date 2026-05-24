package database.ddl.transfer.factory.generate.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.PrimaryKey;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.consts.DataBaseTypeProperties;
import database.ddl.transfer.factory.generate.Generator;
import database.ddl.transfer.utils.StringUtil;

public class OracleSqlGenerator extends Generator {

	public OracleSqlGenerator(Connection connection, DataBaseDefine sourceDataBaseDefine, DBSettings targetDBSettings) {
		super(connection, sourceDataBaseDefine, targetDBSettings);
	}

	/**
	 * Oracle 双引号标识符，避免与保留字（如 MODE、ORDER、UID 等）冲突导致 ORA-00904；名中双引号写为两连续双引号
	 */
	private static String q(String name) {
		if (name == null) {
			return "\"\"";
		}
		return "\"" + name.replace("\"", "\"\"") + "\"";
	}

	/**
	 * JDBC {@link Statement#execute(String)} 对单条语句不宜带末尾分号，否则易 ORA-00922；建用户后需 GRANT 方可正常建连。
	 */
	@Override
	protected boolean createDataBase() {
		String userUpper = sourceDataBaseDefine.getCatalog().toUpperCase();
		String dataBaseDDL = this.getDataBaseDDL(sourceDataBaseDefine);
		if (StringUtil.isBlank(dataBaseDDL)) {
			logger.info("库(用户){}已存在，无需再次创建", userUpper);
			ensureTargetSchemaUserGrants(userUpper);
			return false;
		}
		String grantSession = buildUserGrantSql(userUpper);
		try (Statement statement = this.connection.createStatement()) {
			statement.execute(dataBaseDDL);
			statement.execute(grantSession);
			logger.info("库(用户){}创建成功并已授权: {}", userUpper, grantSession);
			return true;
		} catch (Throwable e) {
			logger.error(String.format("创建库失败，DDL：%s", dataBaseDDL), e);
			return false;
		}
	}

	private String buildUserGrantSql(String userUpper) {
		if (targetDBSettings != null && targetDBSettings.getDataBaseType() == DataBaseType.DM) {
			return "GRANT CREATE SESSION, CREATE TABLE, CREATE INDEX, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE, CREATE TRIGGER, UNLIMITED TABLESPACE TO "
					+ userUpper;
		}
		return "GRANT CREATE SESSION, UNLIMITED TABLESPACE, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE, CREATE TRIGGER TO "
				+ userUpper;
	}

	/**
	 * 达梦目标：在 SYSDBA 等管理账号连接下，为与源 Oracle 同名的 schema 用户补充 DDL 所需权限。
	 * 用户已存在时 createDataBase 不会执行 GRANT，此处必须显式授权后再切换到该用户建表/索引。
	 */
	private void ensureTargetSchemaUserGrants(String userUpper) {
		if (targetDBSettings == null || targetDBSettings.getDataBaseType() != DataBaseType.DM) {
			return;
		}
		String grantSql = buildUserGrantSql(userUpper);
		try (Statement statement = this.connection.createStatement()) {
			statement.execute(grantSql);
			logger.info("达梦用户 {} 权限已确认/补充: {}", userUpper, grantSql);
		} catch (Throwable e) {
			logger.warn("达梦用户 {} 权限补充失败: {} — {}", userUpper, grantSql, e.getMessage());
		}
	}

	@Override
	protected String getDataBaseDDL(DataBaseDefine dataBaseDefine) {
		String sql = "select * from all_users where username = ?";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		boolean flag = false;
		try {
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, dataBaseDefine.getCatalog().toUpperCase());
			resultSet = preparedStatement.executeQuery();
			if(resultSet.next()) {
				flag = true;
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取Oracle表定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		StringBuilder stringBuilder = new StringBuilder("");
		if (!flag) {
			String name = dataBaseDefine.getCatalog().toUpperCase();
			String pwd = resolveDefaultUserPassword(name);
			stringBuilder.append("CREATE USER ").append(name).append(" IDENTIFIED BY ");
			if (targetDBSettings != null && targetDBSettings.getDataBaseType() == DataBaseType.DM) {
				stringBuilder.append("\"").append(pwd).append("\"");
			} else {
				stringBuilder.append(pwd);
			}
		}

		return stringBuilder.toString();
	}

	/**
	 * 新建 Oracle/DM 用户时的默认口令（DM 不允许与登录名相同或过于简单）
	 */
	protected String resolveDefaultUserPassword(String userUpper) {
		if (targetDBSettings != null && targetDBSettings.getDataBaseType() == DataBaseType.DM) {
			return DataBaseTypeProperties.DM_DEFAULT_USER_PASSWORD;
		}
		String catalog = sourceDataBaseDefine == null ? null : sourceDataBaseDefine.getCatalog();
		return catalog == null ? userUpper : catalog;
	}

	@Override
	protected String getTableDDL(Table tableDefine) {
		StringBuilder stringBuilder = new StringBuilder("create table ");
		stringBuilder.append(q(tableDefine.getTableName())).append(" (");

		PrimaryKey primaryKey = tableDefine.getPrimaryKey();
		List<Column> columnList = tableDefine.getColumns();
		for (Column column : columnList) {
			stringBuilder.append(this.getColumnDefineDDL(column, primaryKey));
			stringBuilder.append(",");
		}

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
			stringBuilder.append("constraint ").append(q(primaryKey.getPkName())).append(" primary key(");
		} else {
			stringBuilder.append("primary key(");
		}
		List<String> columnNames = primaryKey.getColumns();
		for (String columnName : columnNames) {
			stringBuilder.append(q(columnName)).append(",");
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
	private String getColumnDefineDDL(Column column, PrimaryKey primaryKey) {
		StringBuilder stringBuilder = new StringBuilder(q(column.getColumnName()));

		String type = column.getFinalConvertDataType();
		stringBuilder.append(" ");
		stringBuilder.append(type);
		if (!column.isNullAble() || isPrimaryKeyColumn(column, primaryKey)) {
			stringBuilder.append(" ").append("not null");
		}

		// 暂时注释掉默认值
//		if (column.hasDefault() && column.getDefaultDefine().contains("now")) {
//			stringBuilder.append(" default ").append("CURRENT_TIMESTAMP");
//		}

		return stringBuilder.toString();
	}

	private boolean isPrimaryKeyColumn(Column column, PrimaryKey primaryKey) {
		if (primaryKey == null || primaryKey.getColumns() == null || column == null) {
			return false;
		}
		for (String pkCol : primaryKey.getColumns()) {
			if (pkCol != null && pkCol.equalsIgnoreCase(column.getColumnName())) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected String getAddPrimaryKeyDDL(Table sourceTable, Table targetTable) {
		PrimaryKey sourcePk = sourceTable == null ? null : sourceTable.getPrimaryKey();
		if (sourcePk == null || sourcePk.getColumns() == null || sourcePk.getColumns().isEmpty()) {
			return null;
		}
		if (targetTable != null && targetTable.getPrimaryKey() != null
				&& targetTable.getPrimaryKey().getColumns() != null
				&& !targetTable.getPrimaryKey().getColumns().isEmpty()) {
			return null;
		}
		StringBuilder stringBuilder = new StringBuilder("ALTER TABLE ");
		stringBuilder.append(q(sourceTable.getTableName())).append(" ADD CONSTRAINT ")
				.append(q(sourcePk.getPkName())).append(" PRIMARY KEY(");
		for (String columnName : sourcePk.getColumns()) {
			stringBuilder.append(q(columnName)).append(",");
		}
		stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		stringBuilder.append(")");
		return stringBuilder.toString().toLowerCase();
	}
	
	private String getCommentDefineDDL(Table tableDefine) {
		StringBuilder stringBuilder = new StringBuilder("");
		String tq = q(tableDefine.getTableName());
		List<Column> columns = tableDefine.getColumns();
		for (Column column : columns) {
			if (!StringUtil.isBlank(column.getColumnComment())) {
				// 注释中若含单引号，按 Oracle 规则加倍
				String cmt = column.getColumnComment().replace("'", "''");
				stringBuilder.append("COMMENT ON COLUMN ").append(tq).append(".").append(q(column.getColumnName())).append(" IS '").append(cmt)
						.append("';");
			}
		}
		if (!StringUtil.isBlank(tableDefine.getTableComment())) {
			String tc = tableDefine.getTableComment().replace("'", "''");
			stringBuilder.append("COMMENT ON TABLE ").append(tq).append(" IS '").append(tc).append("'");
		}
		return stringBuilder.toString();
	}

	@Override
	protected List<String> getModifiedColumnDDL(Table sourceTableDefine, Table targetTableDefine) {
		StringBuilder stringBuilder = null;
		List<String> resultList = new LinkedList<>();
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
				stringBuilder.append("ALTER TABLE ").append(q(sourceTableDefine.getTableName())).append(" ADD (").append(q(columnName)).append(" ").append(sourceColumn.getFinalConvertDataType())
						.append(" ");
				if (!sourceColumn.isNullAble()) {
					stringBuilder.append("NOT NULL");
				} else {
					stringBuilder.append("NULL");
				}
				stringBuilder.append(");");

				if (!StringUtil.isBlank(sourceColumn.getColumnComment())) {
					String cmt = sourceColumn.getColumnComment().replace("'", "''");
					stringBuilder.append("COMMENT ON COLUMN ").append(q(sourceTableDefine.getTableName())).append(".").append(q(columnName)).append(" IS '").append(cmt).append("';");
				}
			} else {
				if (sourceColumn.equals(targetColumn)) {
					continue;
				} else {
					// 由于不同数据库类型转换后与实际查询的类型存在不一致，导致不应该修改类型的字段也会再次执行类型修改操作，表数据量大时影响性能，暂关闭类型修改功能
//					if(!sourceColumn.getDataType().equals(targetColumn.getDataType()) && !primaryKeys.contains(targetColumn.getColumnName())) {
//						stringBuilder.append("ALTER TABLE ").append(sourceTableDefine.getTableName()).append(" MODIFY (").append(columnName).append(" ").append(sourceColumn.getFinalConvertDataType())
//						.append(");");
//					}
//
//					if (!StringUtil.isBlank(sourceColumn.getColumnComment()) && !sourceColumn.getColumnComment().equals(targetColumn.getColumnComment())) {
//						stringBuilder.append("COMMENT ON COLUMN ").append(sourceDataBaseDefine.getTableName()).append(".").append(columnName).append(" IS '").append(sourceColumn.getColumnComment()).append("';");
//					}
//					
//					if(!sourceColumn.isNullAble() == targetColumn.isNullAble() && !targetColumn.isNullAble()) {
//						// 删除非空校验
//						stringBuilder.append("ALTER TABLE ").append(sourceTableDefine.getTableName()).append(" MODIFY ").append(columnName).append(" null;");
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
