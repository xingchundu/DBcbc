package database.ddl.transfer.factory.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.consts.DataBaseTypeProperties;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.IndexDefinition;
import database.ddl.transfer.bean.MigrationIssue;
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

	/** 迁移过程中未能完成的表/索引及原因 */
	private final List<MigrationIssue> migrationIssues = new ArrayList<>();

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
		if (targetDBSettings.getDataBaseType().equals(DataBaseType.ORACLE)
				|| targetDBSettings.getDataBaseType().equals(DataBaseType.DM)) {
			// Oracle / 达梦：以用户名作为 schema；目标若用 SYSDBA 等管理账号连接，须切到源库同名用户后再建表/索引
			this.switchConnectionToSourceSchemaUser();
		} else if (createDataBase || !sourceDataBaseDefine.getCatalog().equals(targetDBSettings.getDataBaseName())) {
			// MySQL / PostgreSQL / SqlServer：按库名切换连接
			try {
				connection.close();
			} catch (Exception e) {
				logger.error("由于创建了新库，需关闭原有库连接，出现异常", e);
			}
			targetDBSettings.setDataBaseName(sourceDataBaseDefine.getCatalog());
			String url = DBUrlUtil.generateDataBaseUrl(targetDBSettings);
			connection = DriverManager.getConnection(url, targetDBSettings.getUserName(), targetDBSettings.getUserPassword());
		}
		this.createTable(sourceDataBaseDefine.getTablesMap().values());
	}

	public List<MigrationIssue> getMigrationIssues() {
		return new ArrayList<>(migrationIssues);
	}

	protected void recordTableIssue(String tableName, String reason) {
		if (StringUtil.isBlank(tableName)) {
			return;
		}
		migrationIssues.add(MigrationIssue.table(tableName, reason));
	}

	protected void recordIndexIssue(String tableName, String indexName, String reason) {
		if (StringUtil.isBlank(tableName)) {
			return;
		}
		migrationIssues.add(MigrationIssue.index(tableName, indexName, reason));
	}

	private boolean hasTableIssue(String tableName) {
		for (MigrationIssue issue : migrationIssues) {
			if (issue.getKind() == MigrationIssue.Kind.TABLE
					&& tableName.equalsIgnoreCase(issue.getTableName())) {
				return true;
			}
		}
		return false;
	}

	private boolean hasIndexIssue(String tableName, String indexName) {
		for (MigrationIssue issue : migrationIssues) {
			if (issue.getKind() == MigrationIssue.Kind.INDEX
					&& tableName.equalsIgnoreCase(issue.getTableName())
					&& indexName != null && indexName.equalsIgnoreCase(issue.getIndexName())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Oracle / 达梦目标：在管理账号（如 SYSDBA）下完成建用户、授权后，切换到与源库同名的 schema 用户执行 DDL。
	 * 以当前 JDBC 会话用户为准，而非连接配置中的 userName（配置可能与实际登录账号不一致）。
	 */
	protected void switchConnectionToSourceSchemaUser() throws SQLException {
		if (sourceDataBaseDefine == null || StringUtil.isBlank(sourceDataBaseDefine.getCatalog())) {
			throw new SQLException("源库 schema 用户名为空，无法切换目标库连接");
		}
		String schemaUser = sourceDataBaseDefine.getCatalog().toUpperCase();
		String defaultPwd = DataBaseTypeProperties.oracleSchemaUserPassword(schemaUser);
		if (targetDBSettings.getDataBaseType().equals(DataBaseType.DM)) {
			defaultPwd = DataBaseTypeProperties.DM_DEFAULT_USER_PASSWORD;
		}
		String currentUser = connection.getMetaData().getUserName();
		if (currentUser != null && schemaUser.equalsIgnoreCase(currentUser)) {
			logger.info("目标库已以用户 {} 连接，直接在此用户下执行 DDL", schemaUser);
			return;
		}
		logger.info("目标库当前会话用户 {} 与源库用户 {} 不一致，将在 {} 用户下执行 DDL", currentUser, schemaUser, schemaUser);
		try {
			connection.close();
		} catch (Exception e) {
			logger.error("切换目标库用户前关闭原连接出现异常", e);
		}
		targetDBSettings.setUserName(schemaUser);
		targetDBSettings.setUserPassword(defaultPwd);
		String url = DBUrlUtil.generateDataBaseUrl(targetDBSettings);
		connection = DriverManager.getConnection(url, schemaUser, defaultPwd);
		logger.info("已切换至目标用户 {} 连接，后续建表/索引均在该用户下执行", schemaUser);
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
		Set<String> targetTableNames = new HashSet<>(targetDataBaseDefine.getTablesMap().keySet());
		if (sourceTableDefines != null && !sourceTableDefines.isEmpty()) {
			String tableDDL = null;
			List<String> modifiedColumnDDLList = new LinkedList<>();
			try (Statement statement = this.connection.createStatement();) {
				for (Table sourceTable : sourceTableDefines) {
					String tableName = sourceTable.getTableName();
					try {
						if (!targetTableNames.contains(tableName)) {
							tableDDL = this.getTableDDL(sourceTable);
							executeDdlStatements(statement, tableDDL);
							logger.info("表{}创建成功", tableName);
							targetTableNames.add(tableName);
							this.executeSecondaryIndexStatements(statement, sourceTable, tableName);
						} else {
							Table targetTable = targetDataBaseDefine.getTablesMap().get(tableName);
							modifiedColumnDDLList = this.getModifiedColumnDDL(sourceTable, targetTable);
							for (String modifiedColumnDDL : modifiedColumnDDLList) {
								executeDdlStatements(statement, modifiedColumnDDL);
								logger.info("表{}字段修改成功,执行DDL：{}", tableName, modifiedColumnDDL);
							}
							this.executeAddPrimaryKeyIfMissing(statement, sourceTable, targetTable, tableName);
							this.executeSecondaryIndexStatementsForMissing(statement, sourceTable, targetTable, tableName);
						}
					} catch (Throwable e) {
						String reason = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
						if (!hasTableIssue(tableName)) {
							recordTableIssue(tableName, reason);
						}
						logger.error("表{}创建或修改失败：{}", tableName, reason, e);
					}
				}
			} catch (Throwable e) {
				logger.error("表结构迁移会话异常", e);
			}

		}
	}

	/**
	 * Oracle / 达梦 的建表 DDL 常含 COMMENT 等多条语句，需按分号拆分逐条执行
	 */
	private void executeDdlStatements(Statement statement, String ddl) throws SQLException {
		if (targetDBSettings.getDataBaseType().equals(DataBaseType.ORACLE)
				|| targetDBSettings.getDataBaseType().equals(DataBaseType.DM)
				|| targetDBSettings.getDataBaseType().equals(DataBaseType.POSTGRESQL)) {
			for (String singleSql : ddl.split(";")) {
				if (StringUtil.isBlank(singleSql)) {
					continue;
				}
				statement.execute(singleSql.trim());
			}
		} else {
			statement.execute(ddl);
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
			} else if (id.getIndexName() != null && !hasIndexIssue(sourceTable.getTableName(), id.getIndexName())) {
				recordIndexIssue(sourceTable.getTableName(), id.getIndexName(),
						"目标方言不支持或含不可迁移列(如Oracle函数索引/系统列)");
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
			} else if (!hasIndexIssue(sourceTable.getTableName(), id.getIndexName())) {
				recordIndexIssue(sourceTable.getTableName(), id.getIndexName(),
						"目标方言不支持或含不可迁移列(如Oracle函数索引/系统列)");
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
				if (targetDBSettings.getDataBaseType().equals(DataBaseType.ORACLE)
						|| targetDBSettings.getDataBaseType().equals(DataBaseType.DM)) {
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
				String indexName = extractIndexName(indexSql);
				String reason = ix.getMessage() != null ? ix.getMessage() : ix.getClass().getSimpleName();
				if (!hasIndexIssue(tableName, indexName)) {
					recordIndexIssue(tableName, indexName, reason);
				}
				logger.warn("表{} 二级索引未创建(可能已存在或方言限制): {} — {}", tableName, indexSql, reason);
			}
		}
	}

	/**
	 * 目标表缺少主键时补建（Oracle / 达梦 等由子类实现）
	 */
	protected void executeAddPrimaryKeyIfMissing(Statement statement, Table sourceTable, Table targetTable, String tableName) {
		String ddl = getAddPrimaryKeyDDL(sourceTable, targetTable);
		if (StringUtil.isBlank(ddl)) {
			return;
		}
		try {
			executeDdlStatements(statement, ddl);
			logger.info("表{}主键已补建,执行DDL：{}", tableName, ddl);
		} catch (Throwable e) {
			logger.warn("表{}主键补建失败: {} — {}", tableName, ddl, e.getMessage());
		}
	}

	/**
	 * 源表有主键、目标表无主键时返回 ALTER TABLE ADD PRIMARY KEY DDL；默认不处理
	 */
	protected String getAddPrimaryKeyDDL(Table sourceTable, Table targetTable) {
		return null;
	}

	private static String extractIndexName(String indexSql) {
		if (StringUtil.isBlank(indexSql)) {
			return null;
		}
		String lower = indexSql.toLowerCase();
		int pos = lower.indexOf("index");
		if (pos < 0) {
			return null;
		}
		String tail = indexSql.substring(pos + 5).trim();
		if (tail.startsWith("\"")) {
			int end = tail.indexOf('"', 1);
			return end > 0 ? tail.substring(1, end) : null;
		}
		int space = tail.indexOf(' ');
		return space > 0 ? tail.substring(0, space) : tail;
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
