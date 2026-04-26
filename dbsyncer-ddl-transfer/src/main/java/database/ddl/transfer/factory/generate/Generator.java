package database.ddl.transfer.factory.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.IndexDefinition;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.factory.analyse.Analyser;
import database.ddl.transfer.factory.analyse.AnalyserFactory;
import database.ddl.transfer.utils.DBUrlUtil;
import database.ddl.transfer.utils.IndexDdlFactory;
import database.ddl.transfer.utils.StringUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * DDL构造类
 *
 * @author gs
 */
public abstract class Generator {
	/**
	 * 数据库连接
	 */
	protected Connection connection;

	/**
	 * 数据库结构中的元素定义
	 */
	protected DataBaseDefine sourceDataBaseDefine;

	/**
	 * 目标数据库配置参数
	 */
	protected DBSettings targetDBSettings;

	protected Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	public Generator(Connection connection, DataBaseDefine sourceDataBaseDefine, DBSettings targetDBSettings) {
		this.connection = connection;
		this.sourceDataBaseDefine = sourceDataBaseDefine;
		this.targetDBSettings = targetDBSettings;
	}

	/**
	 * 构造数据库结构
	 * 
	 * @throws SQLException
	 */
	public void generateStructure() throws SQLException {
		boolean createDataBase = this.createDataBase();
		if(!targetDBSettings.getDataBaseType().equals(DataBaseType.ORACLE)) {
			if (createDataBase || !sourceDataBaseDefine.getCatalog().equals(targetDBSettings.getDataBaseName())) {
				// 创建了新库或则连接库和源库不一样，关闭原有连接，切换新连接
				try {
					connection.close();
				} catch (Exception e) {
					logger.error("由于创建了新库，需关闭原有库连接，出现异常", e);
				}
				// 创建基于新库的连接
				targetDBSettings.setDataBaseName(sourceDataBaseDefine.getCatalog());
				String url = DBUrlUtil.generateDataBaseUrl(targetDBSettings);
				connection = DriverManager.getConnection(url, targetDBSettings.getUserName(), targetDBSettings.getUserPassword());
			}
		}else {
			// Oracle数据库为用户名
			if(createDataBase || !sourceDataBaseDefine.getCatalog().equals(targetDBSettings.getUserName())) {
				// 创建了新库或则连接库和源库不一样，关闭原有连接，切换新连接
				try {
					connection.close();
				} catch (Exception e) {
					logger.error("由于创建了新库，需关闭原有库连接，出现异常", e);
				}
				// 创建基于新库的连接
				targetDBSettings.setUserName(sourceDataBaseDefine.getCatalog());
				targetDBSettings.setUserPassword(sourceDataBaseDefine.getCatalog());
				String url = DBUrlUtil.generateDataBaseUrl(targetDBSettings);
				connection = DriverManager.getConnection(url, targetDBSettings.getUserName(), targetDBSettings.getUserPassword());
			}
		}
		this.createTable(sourceDataBaseDefine.getTablesMap().values());
	}

	/**
	 * 创建库结构
	 * @throws SQLException 
	 */
	protected boolean createDataBase() {
		boolean result = false;
		String dataBaseDDL = this.getDataBaseDDL(sourceDataBaseDefine);
		if (!StringUtil.isBlank(dataBaseDDL)) {
			try (Statement statement = this.connection.createStatement();) {
				statement.execute(dataBaseDDL);
				logger.info("库{}创建成功", sourceDataBaseDefine.getCatalog());
				result = true;
			} catch (Throwable e) {
				logger.error(String.format("创建库失败，DDL：%s", dataBaseDDL), e);
			}
		} else {
			logger.info("库{}已存在，无需再次创建", sourceDataBaseDefine.getCatalog());
		}
		return result;
	}

	/**
	 * 创建表结构
	 * 
	 * @param tableDefines 表结构定义
	 * @throws SQLException 
	 */
	protected void createTable(Collection<Table> sourceTableDefines) throws SQLException {
		Analyser analyser = AnalyserFactory.getInstance(connection);
		DataBaseDefine targetDataBaseDefine = analyser.getDataBaseDefine(connection);
		Set<String> targetTableNames = targetDataBaseDefine.getTablesMap().keySet();
		if (sourceTableDefines != null && !sourceTableDefines.isEmpty()) {
			String tableDDL = null;
			List<String> modifiedColumnDDLList = new LinkedList<>();
			try (Statement statement = this.connection.createStatement();) {
				for (Table sourceTable : sourceTableDefines) {
					String tableName = sourceTable.getTableName();
					if(!targetTableNames.contains(tableName)) {
						tableDDL = this.getTableDDL(sourceTable);
						if(targetDBSettings.getDataBaseType().equals(DataBaseType.ORACLE)) {
							String[] sqls = tableDDL.split(";");
							for (String singleSql : sqls) {
								statement.execute(singleSql);
							}
						}else {
							statement.execute(tableDDL);
						}
						logger.info("表{}创建成功", tableName);
						// 新表落库后,按源端抽取的 secondaryIndexes 在目标端建二级索引(失败仅告警,不阻塞整库迁移)
						this.executeSecondaryIndexStatements(statement, sourceTable, tableName);
					}else {
						Table targetTable = targetDataBaseDefine.getTablesMap().get(tableName);
						modifiedColumnDDLList = this.getModifiedColumnDDL(sourceTable, targetTable);
						for (String modifiedColumnDDL : modifiedColumnDDLList) {
							statement.execute(modifiedColumnDDL);
							logger.info("表{}字段修改成功,执行DDL：{}", tableName, modifiedColumnDDL);
						}
						// 已存在表: 按索引名(小写)对比目标库已抽取的 secondaryIndexes,仅补建缺失项
						this.executeSecondaryIndexStatementsForMissing(statement, sourceTable, targetTable, tableName);
					}
				}
			} catch (Throwable e) {
				logger.error(String.format("表创建或修改失败，DDL：%s", StringUtil.isBlank(tableDDL) ? modifiedColumnDDLList.toString() : tableDDL), e);
			}

		}
	}

	/**
	 * 在目标库执行源表上的二级索引 DDL(可被子类覆写;默认用 IndexDdlFactory 与列上的源库 DataBaseType)
	 */
	protected List<String> getIndexCreationDdls(Table sourceTable) {
		List<String> r = new ArrayList<>();
		if (sourceTable.getSecondaryIndexes() == null || sourceTable.getSecondaryIndexes().isEmpty()) {
			return r;
		}
		DataBaseType target = targetDBSettings.getDataBaseType();
		DataBaseType source = null;
		if (sourceTable.getColumns() != null && !sourceTable.getColumns().isEmpty()) {
			source = sourceTable.getColumns().get(0).getDataBaseType();
		}
		for (IndexDefinition id : sourceTable.getSecondaryIndexes()) {
			String sql = IndexDdlFactory.buildCreateIndex(target, source, sourceTable, id);
			if (!StringUtil.isBlank(sql)) {
				r.add(sql);
			}
		}
		return r;
	}

	/**
	 * 仅包含「目标上尚无同名(忽略大小写)」的建索引 SQL; 用于表已存在时的补建(目标侧索引用 Analyser 在连接上已抽取)
	 */
	protected List<String> getIndexCreationDdlsMissingOnTarget(Table sourceTable, Table targetTable) {
		List<String> r = new ArrayList<>();
		if (sourceTable.getSecondaryIndexes() == null || sourceTable.getSecondaryIndexes().isEmpty()) {
			return r;
		}
		Set<String> existingLower = new HashSet<>();
		if (targetTable != null && targetTable.getSecondaryIndexes() != null) {
			for (IndexDefinition tix : targetTable.getSecondaryIndexes()) {
				if (tix.getIndexName() != null) {
					existingLower.add(tix.getIndexName().toLowerCase());
				}
			}
		}
		DataBaseType target = targetDBSettings.getDataBaseType();
		DataBaseType source = null;
		if (sourceTable.getColumns() != null && !sourceTable.getColumns().isEmpty()) {
			source = sourceTable.getColumns().get(0).getDataBaseType();
		}
		for (IndexDefinition id : sourceTable.getSecondaryIndexes()) {
			if (id.getIndexName() == null) {
				continue;
			}
			if (existingLower.contains(id.getIndexName().toLowerCase())) {
				continue;
			}
			String sql = IndexDdlFactory.buildCreateIndex(target, source, sourceTable, id);
			if (!StringUtil.isBlank(sql)) {
				r.add(sql);
			}
		}
		return r;
	}

	/**
	 * 与 createTable 中“新建表”分支配合:逐条执行 CREATE INDEX 类语句(Oracle 按分号拆分以兼容多语句脚本风格)
	 */
	private void executeSecondaryIndexStatements(Statement statement, Table sourceTable, String tableName) {
		this.executeSecondaryIndexDdlList(statement, this.getIndexCreationDdls(sourceTable), tableName);
	}

	/**
	 * 与 createTable 中“表已存在”分支配合:只执行 {@link #getIndexCreationDdlsMissingOnTarget} 的 DDL
	 */
	private void executeSecondaryIndexStatementsForMissing(Statement statement, Table sourceTable, Table targetTable, String tableName) {
		this.executeSecondaryIndexDdlList(statement, this.getIndexCreationDdlsMissingOnTarget(sourceTable, targetTable), tableName);
	}

	/**
	 * 逐条执行二级索引 CREATE 语句(Oracle 按分号拆分)
	 */
	private void executeSecondaryIndexDdlList(Statement statement, List<String> indexDdls, String tableName) {
		if (indexDdls == null || indexDdls.isEmpty()) {
			return;
		}
		for (String indexSql : indexDdls) {
			if (StringUtil.isBlank(indexSql)) {
				continue;
			}
			try {
				if (targetDBSettings.getDataBaseType().equals(DataBaseType.ORACLE)) {
					for (String part : indexSql.split(";")) {
						if (!StringUtil.isBlank(part.trim())) {
							statement.execute(part);
						}
					}
				} else {
					statement.execute(indexSql);
				}
				logger.info("表{} 二级索引已执行: {}", tableName, indexSql);
			} catch (Throwable ix) {
				logger.warn("表{} 二级索引未创建(可能已存在或方言限制): {} — {}", tableName, indexSql, ix.getMessage());
			}
		}
	}

	/**
	 * 释放数据库操作资源
	 * 
	 * @param preparedStatement PreparedStatement
	 * @param resultSet         ResultSet
	 */
	protected void releaseResources(PreparedStatement preparedStatement, ResultSet resultSet) {
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (Throwable e) {
			}
		}

		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (Throwable e) {
			}
		}
	}

	/**
	 * 获取创建数据库的DDL语句
	 * 
	 * @param dataBaseDefine 数据库结构定义
	 * @return 创建数据库的DDL语句
	 */
	protected abstract String getDataBaseDDL(DataBaseDefine dataBaseDefine);

	/**
	 * 获取创建表的DDL语句
	 * 
	 * @param tableDefine 表结构定义
	 * @return 创建表的DDL语句
	 */
	protected abstract String getTableDDL(Table tableDefine);
	
	/**
	 * 表已存在，比较两个表不同的列，并生成列变化的DDL
	 * 
	 * @param sourceTableDefine 源表
	 * @param targetTableDefine 目标表
	 * @return
	 */
	protected abstract List<String> getModifiedColumnDDL(Table sourceTableDefine, Table targetTableDefine);
}
