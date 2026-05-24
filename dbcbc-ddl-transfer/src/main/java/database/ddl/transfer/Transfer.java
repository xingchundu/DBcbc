package database.ddl.transfer;

import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.MigrationIssue;
import database.ddl.transfer.bean.MigrationSummary;
import database.ddl.transfer.bean.ObjectCounts;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.bean.IndexDefinition;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.consts.DataBaseTypeProperties;
import database.ddl.transfer.factory.analyse.Analyser;
import database.ddl.transfer.factory.analyse.AnalyserFactory;
import database.ddl.transfer.factory.convert.TypeConvertFactory;
import database.ddl.transfer.factory.generate.Generator;
import database.ddl.transfer.factory.generate.GeneratorFactory;
import database.ddl.transfer.utils.DBUrlUtil;
import database.ddl.transfer.utils.StringUtil;
import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.ObjectMigrateOptions;
import database.ddl.transfer.object.ObjectMigrateResult;
import database.ddl.transfer.object.extractor.AbstractObjectExtractor;
import database.ddl.transfer.object.extractor.ObjectExtractorFactory;
import database.ddl.transfer.object.migrator.AbstractObjectMigrator;
import database.ddl.transfer.object.migrator.ObjectMigratorFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 数据库结构转换（由 DBCDC 连接管理中的数据源驱动连接参数）
 */
public final class Transfer {

    private static final Logger logger = LoggerFactory.getLogger(Transfer.class);

    private Transfer() {
    }

    /**
     * 关系型数据库表结构传输 （如果库不存在，则会创建库和所有表；库存在，则只会创建所有表）
     *
     * @param sourceDB 源库配置
     * @param targetDB 目标库配置
     */
    public static MigrationSummary transferRDBMS(DBSettings sourceDB, DBSettings targetDB) throws Throwable {
        logger.info("开始连接至数据库");
        try (Connection sourceConnection = getConnection(sourceDB); Connection targetConnection = getConnection(targetDB)) {
            logger.info("成功连接至数据库");

            logger.info("开始获取源数据库结构定义");
            Analyser analyser = AnalyserFactory.getInstance(sourceConnection);
            DataBaseDefine dataBaseDefine = analyser.getDataBaseDefine(sourceConnection);
            ObjectCounts sourceCounts = countFromDefine(dataBaseDefine);
            logger.info("源数据库结构定义获取完毕");

            String sourceDataBaseName = sourceConnection.getMetaData().getDatabaseProductName();
            String targetDataBaseName = targetConnection.getMetaData().getDatabaseProductName();
            if (!sourceDataBaseName.equals(targetDataBaseName)) {
                logger.info("开始转换为目标数据库类型");
                // 规范化数据库名为无空格大写（如 "Microsoft SQL Server" → "MICROSOFTSQLSERVER"）
                String srcNorm = sourceDataBaseName.toUpperCase().replaceAll("\\s+","");
                String tgtNorm = targetDataBaseName.toUpperCase().replaceAll("\\s+","");
                // 统一别名
                srcNorm = normalizeDbName(srcNorm);
                tgtNorm = normalizeDbName(tgtNorm);
                String convertType = srcNorm + "2" + tgtNorm;
                try {
                    database.ddl.transfer.factory.convert.BaseTypeConverter converter =
                            TypeConvertFactory.getInstance(convertType);
                    converter.convert(dataBaseDefine);
                    logger.info("目标数据库类型转换完成");
                } catch (IllegalArgumentException | IllegalStateException noConverter) {
                    logger.warn("未找到类型转换器 [{}]，将尝试直接使用源库类型名（如已有兼容映射可忽略此警告）", convertType);
                }
            }

            logger.info("开始构造目标数据库结构");
            Generator generator = GeneratorFactory.getInstance(targetConnection, dataBaseDefine, targetDB);
            generator.generateStructure();
            logger.info("目标数据库结构构造完成");

            List<MigrationIssue> issues = new ArrayList<>(generator.getMigrationIssues());
            ObjectCounts targetCounts;
            DBSettings verifyTargetSettings = resolveTargetVerifySettings(targetDB, dataBaseDefine);
            try (Connection verifyTarget = getConnection(verifyTargetSettings)) {
                Analyser targetAnalyser = AnalyserFactory.getInstance(verifyTarget);
                DataBaseDefine targetDefine = targetAnalyser.getDataBaseDefine(verifyTarget);
                targetCounts = countFromDefine(targetDefine);
                appendDiffIssues(dataBaseDefine, targetDefine, issues);
            }
            MigrationSummary summary = new MigrationSummary(sourceCounts, targetCounts, issues);
            logger.info(summary.formatMessage());
            return summary;
        }
    }

    /**
     * 仅迁移数据库对象（表结构已完成，单独执行对象迁移阶段）
     *
     * @param sourceDB 源库配置
     * @param targetDB 目标库配置
     * @param options  用户选择的对象类型
     */
    public static ObjectMigrateResult transferObjects(
            DBSettings sourceDB, DBSettings targetDB, ObjectMigrateOptions options) throws Throwable {
        logger.info("===== 对象迁移阶段开始 =====");
        try (Connection srcConn = getConnection(sourceDB);
             Connection tgtConn = getConnection(targetDB)) {
            AbstractObjectExtractor extractor = ObjectExtractorFactory.getInstance(srcConn);
            List<DbObject> objects = extractor.extract(options);
            logger.info("共提取到 {} 个待迁移对象", objects.size());
            AbstractObjectMigrator migrator = ObjectMigratorFactory.getInstance(srcConn, tgtConn);
            ObjectMigrateResult result = migrator.migrate(objects);
            logger.info("===== 对象迁移阶段结束 =====");
            return result;
        }
    }

    /** 将数据库产品名规范化为 TypeConvertFactory 中的 key 前缀 */
    private static String normalizeDbName(String upper) {
        if (upper == null) return "";
        if (upper.contains("MICROSOFTSQLSERVER") || upper.contains("SQLSERVER")) return "SQLSERVER";
        if (upper.contains("ORACLE"))     return "ORACLE";
        if (upper.contains("POSTGRESQL") || upper.contains("POSTGRES")) return "POSTGRESQL";
        if (upper.contains("MYSQL"))      return "MYSQL";
        if (upper.contains("DM") || upper.contains("DAMENG")) return "DM";
        return upper.replaceAll("\\s+", "");
    }

    /**
     * 目标库校验连接：Oracle/DM 切 schema 用户；PostgreSQL/MySQL/SqlServer 切到源库同名 database（DDL 新建库后须与迁移会话一致）。
     */
    private static DBSettings resolveTargetVerifySettings(DBSettings targetDB, DataBaseDefine sourceDefine) {
        if (targetDB == null || sourceDefine == null || StringUtil.isBlank(sourceDefine.getCatalog())) {
            return targetDB;
        }
        DataBaseType targetType = targetDB.getDataBaseType();
        DBSettings verify = copySettings(targetDB);
        if (targetType == DataBaseType.ORACLE || targetType == DataBaseType.DM) {
            String schemaUser = sourceDefine.getCatalog().toUpperCase();
            if (!schemaUser.equalsIgnoreCase(targetDB.getUserName())) {
                verify.setUserName(schemaUser);
                if (targetType == DataBaseType.DM) {
                    verify.setUserPassword(DataBaseTypeProperties.DM_DEFAULT_USER_PASSWORD);
                } else {
                    verify.setUserPassword(sourceDefine.getCatalog());
                }
            }
            return verify;
        }
        if (targetType == DataBaseType.POSTGRESQL || targetType == DataBaseType.MYSQL
                || targetType == DataBaseType.SQLSERVER) {
            String targetCatalog = sourceDefine.getCatalog();
            if (!targetCatalog.equalsIgnoreCase(targetDB.getDataBaseName())) {
                verify.setDataBaseName(targetCatalog);
            }
        }
        return verify;
    }

    private static DBSettings copySettings(DBSettings source) {
        DBSettings verify = new DBSettings();
        verify.setDataBaseType(source.getDataBaseType());
        verify.setDriverClass(source.getDriverClass());
        verify.setIpAddress(source.getIpAddress());
        verify.setPort(source.getPort());
        verify.setDataBaseName(source.getDataBaseName());
        verify.setOracleServiceName(source.getOracleServiceName());
        verify.setUserName(source.getUserName());
        verify.setUserPassword(source.getUserPassword());
        return verify;
    }

    /** 对比源/目标结构，补充迁移后仍缺失且尚未记录的表与索引 */
    private static void appendDiffIssues(DataBaseDefine sourceDefine, DataBaseDefine targetDefine,
            List<MigrationIssue> issues) {
        if (sourceDefine == null || sourceDefine.getTablesMap() == null) {
            return;
        }
        Map<String, Table> targetTables = targetDefine == null || targetDefine.getTablesMap() == null
                ? new HashMap<>() : targetDefine.getTablesMap();
        Set<String> recordedTables = new HashSet<>();
        Set<String> recordedIndexes = new HashSet<>();
        for (MigrationIssue issue : issues) {
            if (issue.getKind() == MigrationIssue.Kind.TABLE) {
                recordedTables.add(issue.getTableName().toLowerCase());
            } else if (issue.getIndexName() != null) {
                recordedIndexes.add(issue.getTableName().toLowerCase() + "\0" + issue.getIndexName().toLowerCase());
            }
        }
        for (Table sourceTable : sourceDefine.getTablesMap().values()) {
            String tableName = sourceTable.getTableName();
            if (tableName == null) {
                continue;
            }
            if (!targetTables.containsKey(tableName)) {
                if (!recordedTables.contains(tableName.toLowerCase())) {
                    issues.add(MigrationIssue.table(tableName, "目标库中未找到该表(可能建表失败或校验库不一致)"));
                }
                continue;
            }
            Map<String, IndexDefinition> targetIdx = indexMapForTable(targetTables.get(tableName));
            if (sourceTable.getSecondaryIndexes() == null) {
                continue;
            }
            for (IndexDefinition idx : sourceTable.getSecondaryIndexes()) {
                if (idx.getIndexName() == null) {
                    continue;
                }
                String key = tableName.toLowerCase() + "\0" + idx.getIndexName().toLowerCase();
                if (recordedIndexes.contains(key)) {
                    continue;
                }
                if (!targetIdx.containsKey(idx.getIndexName().toLowerCase())) {
                    issues.add(MigrationIssue.index(tableName, idx.getIndexName(),
                            "目标库中未找到该索引(可能创建失败或方言不支持)"));
                }
            }
        }
    }

    private static Map<String, IndexDefinition> indexMapForTable(Table table) {
        Map<String, IndexDefinition> map = new LinkedHashMap<>();
        if (table == null || table.getSecondaryIndexes() == null) {
            return map;
        }
        for (IndexDefinition idx : table.getSecondaryIndexes()) {
            if (idx.getIndexName() != null) {
                map.put(idx.getIndexName().toLowerCase(), idx);
            }
        }
        return map;
    }

    private static ObjectCounts countFromDefine(DataBaseDefine dataBaseDefine) {
        int tableCount = dataBaseDefine.getTablesMap() == null ? 0 : dataBaseDefine.getTablesMap().size();
        int indexCount = 0;
        if (dataBaseDefine.getTablesMap() != null) {
            for (Table table : dataBaseDefine.getTablesMap().values()) {
                if (table.getSecondaryIndexes() != null) {
                    indexCount += table.getSecondaryIndexes().size();
                }
            }
        }
        return new ObjectCounts(tableCount, indexCount);
    }

    private static Connection getConnection(DBSettings settings)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        String driverClass = resolveDriverClassName(settings);
        if (StringUtil.isBlank(driverClass)) {
            throw new IllegalStateException("JDBC 驱动类名为空；请在「连接管理」中填写该数据库的 driverClassName，"
                    + "或确保已设置 dataBaseType 以便使用内置默认驱动类名。");
        }
        Class.forName(driverClass).newInstance();
        String url = DBUrlUtil.generateDataBaseUrl(settings);
        return DriverManager.getConnection(url, settings.getUserName(), settings.getUserPassword());
    }

    /**
     * 当连接元数据中未存 driverClass 时，按关系型库类型回退为常用 JDBC 驱动，避免 Class.forName(null)。
     */
    private static String resolveDriverClassName(DBSettings settings) {
        if (settings == null) {
            return null;
        }
        String configured = settings.getDriverClass();
        if (!StringUtil.isBlank(configured)) {
            return configured.trim();
        }
        DataBaseType t = settings.getDataBaseType();
        if (t == null) {
            return null;
        }
        switch (t) {
        case MYSQL:
            return "com.mysql.cj.jdbc.Driver";
        case ORACLE:
            return "oracle.jdbc.OracleDriver";
        case POSTGRESQL:
            return "org.postgresql.Driver";
        case DM:
            return "dm.jdbc.driver.DmDriver";
        case SQLSERVER:
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        default:
            return null;
        }
    }
}
