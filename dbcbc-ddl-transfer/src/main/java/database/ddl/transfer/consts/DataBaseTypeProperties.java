package database.ddl.transfer.consts;

/**
 * @ClassName DataBaseTypeProperties
 * @Description TODO
 * @Author luoyuntian
 * @Date 2019-12-31 09:39
 * @Version
 **/
public class DataBaseTypeProperties {
	// 配置内容为需要添加长度或精度的类型
	public static final String MYSQL_TYPE_SCALA = "int,float,double,decimal,char,varchar,binary,varbinary,date,datetime,timestamp";
	public static final String POSTGRE_TYPE_SCALA = "bit,bit varying,character,char,character varying,varchar,interval,numeric,decimal,time,timestamp";
	public static final String ORACLE_TYPE_SCALA = "decimal,numeric,dec,number,char,varchar2";
	public static final String DM_TYPE_SCALA = "decimal,numeric,dec,number,char,varchar,varchar2";
	/** 达梦新建用户默认口令（需满足复杂度规则，且不能与登录名相同） */
	public static final String DM_DEFAULT_USER_PASSWORD = "Dameng123";

	/**
	 * Oracle 新建 schema 用户时的默认口令：与用户名相同（大写），建用户与切换 JDBC 连接须保持一致。
	 */
	public static String oracleSchemaUserPassword(String schemaUserUpper) {
		return schemaUserUpper == null ? "" : schemaUserUpper.toUpperCase();
	}

}
