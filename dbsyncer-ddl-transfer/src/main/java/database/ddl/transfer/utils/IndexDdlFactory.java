package database.ddl.transfer.utils;

import database.ddl.transfer.bean.IndexDefinition;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.consts.DataBaseType;

/**
 * 二级索引在目标库上的 CREATE 语句(不改变既有表/列/主键逻辑,仅本类为新增)
 */
public final class IndexDdlFactory {

	private IndexDdlFactory() {
	}

	/**
	 * 生成单条建索引 SQL; 不支持的类型或跨库过复杂时返回 null(调用方日志中跳过)
	 */
	public static String buildCreateIndex(DataBaseType targetType, DataBaseType sourceType, Table table, IndexDefinition idx) {
		if (idx.getColumnNames() == null || idx.getColumnNames().isEmpty() || StringUtil.isBlank(idx.getIndexName())) {
			return null;
		}
		String tb = table.getTableName();
		String in = idx.getIndexName();
		String m = idx.getAccessMethod() != null ? idx.getAccessMethod().toLowerCase() : "btree";

		if (DataBaseType.POSTGRESQL.equals(targetType)) {
			return buildPostgreSqlDdl(sourceType, idx, tb, in, m);
		}
		if (DataBaseType.MYSQL.equals(targetType)) {
			return buildMySqlDdl(idx, tb, in, m);
		}
		if (DataBaseType.ORACLE.equals(targetType)) {
			return buildOracleDdl(idx, tb, in, m);
		}
		return null;
	}

	// ---------- PostgreSQL 目标 ----------

	/** 目标为 PostgreSQL: btree / hash / GIN/ GiST 等,以及 MySQL FULLTEXT 迁 PG 时的 GIN 简化 */
	private static String buildPostgreSqlDdl(DataBaseType sourceType, IndexDefinition idx, String tb, String in, String m) {
		String uq = idx.isUnique() ? "UNIQUE " : "";
		// 源 MySQL FULLTEXT,目标 PG: 单列表用 gin + to_tsvector(多列不自动生成)
		if ("fulltext".equals(m) && sourceTypeOrMatch(sourceType, DataBaseType.MYSQL)) {
			if (idx.getColumnNames().size() == 1) {
				return String.format("create index %s on %s using gin (to_tsvector('simple', %s::text))", qpgIdent(in), qpgIdent(tb),
						qpgIdent(idx.getColumnNames().get(0)));
			}
			return null;
		}
		// 源/目标侧非 btree 的访问方法(如 PG 的 gin、MySQL 的 hash 等)
		if (isPgNonBtreeMethod(m) && (sourceTypeOrMatch(sourceType, DataBaseType.POSTGRESQL) || "hash".equals(m))) {
			return String.format("create %sindex %s on %s using %s (%s)", uq, qpgIdent(in), qpgIdent(tb), m, joinPgColumns(idx));
		}
		return String.format("create %sindex %s on %s (%s)", uq, qpgIdent(in), qpgIdent(tb), joinPgColumns(idx));
	}

	private static boolean isPgNonBtreeMethod(String m) {
		return "gin".equals(m) || "gist".equals(m) || "spgist".equals(m) || "brin".equals(m) || "hash".equals(m);
	}

	private static String joinPgColumns(IndexDefinition idx) {
		StringBuilder b = new StringBuilder();
		for (String c : idx.getColumnNames()) {
			if (b.length() > 0) {
				b.append(", ");
			}
			b.append(qpgIdent(c));
		}
		return b.toString();
	}

	private static String qpgIdent(String name) {
		if (name == null) {
			return "\"\"";
		}
		return "\"" + name.replace("\"", "\"\"") + "\"";
	}

	// ---------- MySQL 目标 ----------

	/** 目标为 MySQL: 普通/唯一/FULLTEXT/SPATIAL */
	private static String buildMySqlDdl(IndexDefinition idx, String tb, String in, String m) {
		// 源为 PG 的 gin 等到 MySQL 无对等全文类时退化为普通索引(或单列尽量用 fulltext 需列类型,此处保守为 btree 风格)
		if ("fulltext".equals(m) && idx.getColumnNames().size() == 1) {
			return String.format("CREATE FULLTEXT INDEX `%s` ON `%s` (`%s`)", myEsc(in), myEsc(tb), myEsc(idx.getColumnNames().get(0)));
		}
		if ("spatial".equals(m) && idx.getColumnNames().size() == 1) {
			return String.format("CREATE SPATIAL INDEX `%s` ON `%s` (`%s`)", myEsc(in), myEsc(tb), myEsc(idx.getColumnNames().get(0)));
		}
		if (idx.isUnique()) {
			return String.format("CREATE UNIQUE INDEX `%s` ON `%s` (%s)", myEsc(in), myEsc(tb), joinMysqlColumns(idx));
		}
		return String.format("CREATE INDEX `%s` ON `%s` (%s)", myEsc(in), myEsc(tb), joinMysqlColumns(idx));
	}

	private static String myEsc(String c) {
		if (c == null) {
			return "";
		}
		return c.replace("`", "``");
	}

	private static String joinMysqlColumns(IndexDefinition idx) {
		StringBuilder b = new StringBuilder();
		for (String c : idx.getColumnNames()) {
			if (b.length() > 0) {
				b.append(", ");
			}
			b.append("`").append(myEsc(c)).append("`");
		}
		return b.toString();
	}

	// ---------- Oracle 目标 ----------

	/** 目标为 Oracle: 普通/唯一/BITMAP(与 getTableDDL 一致输出小写无引号) */
	private static String buildOracleDdl(IndexDefinition idx, String tb, String in, String m) {
		if ("bitmap".equals(m)) {
			return String.format("create bitmap index %s on %s (%s)", in, tb, joinOracleColumns(idx)).toLowerCase();
		}
		if (idx.isUnique()) {
			return String.format("create unique index %s on %s (%s)", in, tb, joinOracleColumns(idx)).toLowerCase();
		}
		return String.format("create index %s on %s (%s)", in, tb, joinOracleColumns(idx)).toLowerCase();
	}

	private static String joinOracleColumns(IndexDefinition idx) {
		StringBuilder b = new StringBuilder();
		for (String c : idx.getColumnNames()) {
			if (b.length() > 0) {
				b.append(", ");
			}
			if (c != null) {
				b.append(c);
			}
		}
		return b.toString();
	}

	private static boolean sourceTypeOrMatch(DataBaseType a, DataBaseType b) {
		return a != null && a.equals(b);
	}
}
