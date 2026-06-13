package database.ddl.transfer.utils;

import java.util.ArrayList;
import java.util.List;

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
	 * 生成单条删索引 SQL; 不支持时返回 null
	 */
	public static String buildDropIndex(DataBaseType targetType, Table table, IndexDefinition idx) {
		if (idx == null || StringUtil.isBlank(idx.getIndexName()) || table == null) {
			return null;
		}
		String tb = table.getTableName();
		String in = idx.getIndexName();

		if (DataBaseType.POSTGRESQL.equals(targetType)) {
			return String.format("DROP INDEX IF EXISTS \"%s\"", in.replace("\"", "\"\""));
		}
		if (DataBaseType.MYSQL.equals(targetType)) {
			return String.format("DROP INDEX `%s` ON `%s`", myEsc(in), myEsc(tb));
		}
		if (DataBaseType.ORACLE.equals(targetType)) {
			return String.format("drop index %s", in).toLowerCase();
		}
		if (DataBaseType.DM.equals(targetType)) {
			return String.format("DROP INDEX %s", qDmIdent(in));
		}
		if (DataBaseType.SQLSERVER.equals(targetType)) {
			return String.format("DROP INDEX [%s] ON [%s]", in, tb);
		}
		return null;
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
		if (DataBaseType.DM.equals(targetType)) {
			idx = sanitizeIndexColumns(idx, table);
			if (idx == null || idx.getColumnNames() == null || idx.getColumnNames().isEmpty()) {
				return null;
			}
			return buildDmDdl(idx, tb, in, m);
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
				return String.format("create index %s on %s using gin (to_tsvector('simple', %s::text))", qpgIdent(in), qpgTable(tb),
						qpgIdent(idx.getColumnNames().get(0)));
			}
			return null;
		}
		// 源/目标侧非 btree 的访问方法(如 PG 的 gin、MySQL 的 hash 等)
		if (isPgNonBtreeMethod(m) && (sourceTypeOrMatch(sourceType, DataBaseType.POSTGRESQL) || "hash".equals(m))) {
			return String.format("create %sindex %s on %s using %s (%s)", uq, qpgIdent(in), qpgTable(tb), m, joinPgColumns(idx));
		}
		return String.format("create %sindex %s on %s (%s)", uq, qpgIdent(in), qpgTable(tb), joinPgColumns(idx));
	}

	private static String qpgTable(String tableName) {
		if (tableName == null) {
			return "public.\"\"";
		}
		return "public.\"" + tableName.replace("\"", "\"\"") + "\"";
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

	// ---------- 达梦（DM）目标（与 Oracle 兼容，表/列使用双引号标识符） ----------

	private static String buildDmDdl(IndexDefinition idx, String tb, String in, String m) {
		String qt = qDmIdent(tb);
		String qi = qDmIdent(in);
		String cols = joinDmColumns(idx);
		if ("bitmap".equals(m)) {
			return String.format("create bitmap index %s on %s (%s)", qi, qt, cols);
		}
		if (idx.isUnique()) {
			return String.format("create unique index %s on %s (%s)", qi, qt, cols);
		}
		return String.format("create index %s on %s (%s)", qi, qt, cols);
	}

	private static String joinDmColumns(IndexDefinition idx) {
		StringBuilder b = new StringBuilder();
		for (String c : idx.getColumnNames()) {
			if (b.length() > 0) {
				b.append(", ");
			}
			if (c != null) {
				b.append(qDmIdent(c));
			}
		}
		return b.toString();
	}

	private static String qDmIdent(String name) {
		if (name == null) {
			return "\"\"";
		}
		return "\"" + name.replace("\"", "\"\"") + "\"";
	}

	private static IndexDefinition sanitizeIndexColumns(IndexDefinition idx, Table table) {
		if (idx == null || idx.getColumnNames() == null) {
			return null;
		}
		List<String> cols = new ArrayList<>();
		for (String col : idx.getColumnNames()) {
			if (col == null || col.toLowerCase().matches("sys_nc\\d+\\$")) {
				continue;
			}
			if (table != null && table.getColumnsMap() != null && !table.getColumnsMap().isEmpty()
					&& !table.getColumnsMap().containsKey(col)) {
				continue;
			}
			cols.add(col);
		}
		if (cols.isEmpty()) {
			return null;
		}
		IndexDefinition copy = new IndexDefinition();
		copy.setTableName(idx.getTableName());
		copy.setIndexName(idx.getIndexName());
		copy.setUnique(idx.isUnique());
		copy.setAccessMethod(idx.getAccessMethod());
		copy.setColumnNames(cols);
		return copy;
	}

	private static boolean sourceTypeOrMatch(DataBaseType a, DataBaseType b) {
		return a != null && a.equals(b);
	}
}
