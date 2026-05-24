package database.ddl.transfer.factory.convert;

import database.ddl.transfer.consts.ConvertType;
import database.ddl.transfer.consts.DataBaseTypeProperties;
import database.ddl.transfer.factory.convert.impl.Dm2PostgreSQLTypeConverter;
import database.ddl.transfer.factory.convert.impl.SqlServer2PostgreSQLTypeConverter;
import database.ddl.transfer.factory.convert.impl.MySQL2OracleTypeConverter;
import database.ddl.transfer.factory.convert.impl.MySQL2PostgreSQLTypeConverter;
import database.ddl.transfer.factory.convert.impl.PostgreSql2MySQLTypeConverter;
import database.ddl.transfer.utils.JsonUtil;
import database.ddl.transfer.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.factory.convert.BaseTypeConverter;
import database.ddl.transfer.factory.convert.TypeConvertFactory;
import database.ddl.transfer.factory.convert.impl.Oracle2DmTypeConverter;
import database.ddl.transfer.factory.convert.impl.Oracle2MySQLTypeConverter;
import database.ddl.transfer.factory.convert.impl.Oracle2PostgreSQLTypeConverter;

import java.util.Map;

/**
 * 转换工厂类，用于转化数据类型，方法等
 *
 * @author gs
 */
public final class TypeConvertFactory {

	private static Logger logger = LoggerFactory.getLogger(TypeConvertFactory.class);

	/**
	 * 无需实例化
	 */
	private TypeConvertFactory() {
	}

	public static BaseTypeConverter getInstance(String convertType) {
		BaseTypeConverter typeConverter = null;
		Map<String, String> mapping = null;
		Map<String, String> typeProperties = null;
		try {
			Map<String, Map<String, String>> typeMapping = JsonUtil.readJsonData("TypeMapping.json");
			if (ConvertType.MYSQL2POSTGRESQL.equals(convertType)) {
				mapping = typeMapping.get("mysql2pg");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.POSTGRE_TYPE_SCALA);
				typeConverter = new MySQL2PostgreSQLTypeConverter(mapping, typeProperties);
			} else if (ConvertType.POSTGRESQL2MYSQL.equals(convertType)) {
				mapping = typeMapping.get("pg2mysql");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.MYSQL_TYPE_SCALA);
				typeConverter = new PostgreSql2MySQLTypeConverter(mapping, typeProperties);
			} else if (ConvertType.ORACLE2POSTGRESQL.equals(convertType)) {
				mapping = typeMapping.get("oracle2pg");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.POSTGRE_TYPE_SCALA);
				typeConverter = new Oracle2PostgreSQLTypeConverter(mapping, typeProperties);
			} else if (ConvertType.ORACLE2MYSQL.equals(convertType)) {
				mapping = typeMapping.get("oracle2mysql");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.MYSQL_TYPE_SCALA);
				typeConverter = new Oracle2MySQLTypeConverter(mapping, typeProperties);
			} else if (ConvertType.MYSQL2ORACLE.equals(convertType)) {
				mapping = typeMapping.get("mysql2oracle");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.ORACLE_TYPE_SCALA);
				typeConverter = new MySQL2OracleTypeConverter(mapping, typeProperties);
			} else if(ConvertType.POSTGRESQL2ORACLE.equals(convertType)) {
				mapping = typeMapping.get("pg2oracle");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.ORACLE_TYPE_SCALA);
				typeConverter = new Oracle2PostgreSQLTypeConverter(mapping, typeProperties);
			}else if(ConvertType.DM2POSTGRESQL.equals(convertType)) {
				mapping = typeMapping.get("dm2pg");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.POSTGRE_TYPE_SCALA);
				typeConverter = new Dm2PostgreSQLTypeConverter(mapping, typeProperties);
			}else if(ConvertType.ORACLE2DM.equals(convertType)) {
				mapping = typeMapping.get("oracle2dm");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.DM_TYPE_SCALA);
				typeConverter = new Oracle2DmTypeConverter(mapping, typeProperties);
			}else if(ConvertType.SQLSERVER2POSTGRESQL.equals(convertType)) {
				mapping = typeMapping.get("sqlserver2pg");
				typeProperties = StringUtil.str2Map(DataBaseTypeProperties.POSTGRE_TYPE_SCALA);
				typeConverter = new SqlServer2PostgreSQLTypeConverter(mapping, typeProperties);
			}else {
				throw new IllegalArgumentException(String.format("无法识别的数据库类型：%s", convertType));
			}
		} catch (Exception e) {
			logger.error("读取TypeMapping.json文件并初始化转换器出现异常", e);
		}
		if (typeConverter == null) {
			throw new IllegalStateException(
					String.format("不支持的源/目标库类型组合或转换器初始化失败：%s（请在 TypeConvertFactory 与 TypeMapping.json 中补充映射）", convertType));
		}
		return typeConverter;
	}
}
