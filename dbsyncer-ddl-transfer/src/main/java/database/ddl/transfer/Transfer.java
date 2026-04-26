package database.ddl.transfer;

import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.factory.analyse.Analyser;
import database.ddl.transfer.factory.analyse.AnalyserFactory;
import database.ddl.transfer.factory.convert.TypeConvertFactory;
import database.ddl.transfer.factory.generate.Generator;
import database.ddl.transfer.factory.generate.GeneratorFactory;
import database.ddl.transfer.utils.DBUrlUtil;

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
    public static void transferRDBMS(DBSettings sourceDB, DBSettings targetDB) throws Throwable {
        logger.info("开始连接至数据库");
        try (Connection sourceConnection = getConnection(sourceDB); Connection targetConnection = getConnection(targetDB)) {
            logger.info("成功连接至数据库");

            logger.info("开始获取源数据库结构定义");
            Analyser analyser = AnalyserFactory.getInstance(sourceConnection);
            DataBaseDefine dataBaseDefine = analyser.getDataBaseDefine(sourceConnection);
            logger.info("源数据库结构定义获取完毕");

            String sourceDataBaseName = sourceConnection.getMetaData().getDatabaseProductName();
            String targetDataBaseName = targetConnection.getMetaData().getDatabaseProductName();
            if (!sourceDataBaseName.equals(targetDataBaseName)) {
                logger.info("开始转换为目标数据库类型");
                String convertType = sourceDataBaseName.toUpperCase() + "2" + targetDataBaseName.toUpperCase();
                TypeConvertFactory.getInstance(convertType).convert(dataBaseDefine);
                logger.info("目标数据库类型转换完成");
            }

            logger.info("开始构造目标数据库结构");
            Generator generator = GeneratorFactory.getInstance(targetConnection, dataBaseDefine, targetDB);
            generator.generateStructure();
            logger.info("目标数据库结构构造完成");
        }
    }

    private static Connection getConnection(DBSettings settings)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        Class.forName(settings.getDriverClass()).newInstance();
        String url = DBUrlUtil.generateDataBaseUrl(settings);
        return DriverManager.getConnection(url, settings.getUserName(), settings.getUserPassword());
    }
}
