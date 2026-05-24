package database.ddl.transfer.utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import database.ddl.transfer.bean.IndexDefinition;
import database.ddl.transfer.bean.Table;

/**
 * 通过 JDBC {@link DatabaseMetaData#getIndexInfo} 抽取二级索引列序,避免各版本 pg_index/Oracle 字典型 SQL 差异
 */
public final class JdbcSecondaryIndexReader {

	private JdbcSecondaryIndexReader() {
	}

	/**
	 * @param tableNameForMetadata 对 Oracle 等库传大写表名,对 PostgreSQL 等传小写/实际名
	 */
	public static List<IndexDefinition> readForTable(Connection connection, String catalog, String schema,
			Table table, String tableNameForMetadata) throws SQLException {
		List<IndexDefinition> out = new ArrayList<>();
		DatabaseMetaData md = connection.getMetaData();
		// 列顺序 key=索引名(小写) -> 序号->列名
		Map<String, TreeMap<Integer, String>> byIndex = new LinkedHashMap<>();
		Map<String, Boolean> byIndexNonUnique = new HashMap<>();
		try (ResultSet irs = md.getIndexInfo(catalog, schema, tableNameForMetadata, false, false)) {
			while (irs.next()) {
				if (irs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
					continue;
				}
				String iname = irs.getString("INDEX_NAME");
				if (iname == null) {
					continue;
				}
				String cname = irs.getString("COLUMN_NAME");
				if (cname == null) {
					continue;
				}
				if (isOracleSystemColumn(cname)) {
					continue;
				}
				int ord = irs.getInt("ORDINAL_POSITION");
				if (ord <= 0) {
					continue;
				}
				String inameL = iname.toLowerCase();
				byIndex.putIfAbsent(inameL, new TreeMap<>());
				byIndex.get(inameL).put(ord, cname.toLowerCase());
				byIndexNonUnique.put(inameL, irs.getBoolean("NON_UNIQUE"));
			}
		}
		String tkey = table.getTableName();
		for (Map.Entry<String, TreeMap<Integer, String>> e : byIndex.entrySet()) {
			if (isOracleSystemIndexName(e.getKey())) {
				continue;
			}
			List<String> cols = new ArrayList<>(e.getValue().values());
			cols = filterMigratableColumns(cols, table);
			if (cols.isEmpty() || table.isSameIndexColumnsAsPrimaryKey(cols)) {
				continue;
			}
			IndexDefinition def = new IndexDefinition();
			def.setTableName(tkey);
			def.setIndexName(e.getKey());
			def.setUnique(!byIndexNonUnique.getOrDefault(e.getKey(), true));
			def.setColumnNames(cols);
			// 访问法 JDBC 不通用,建索引阶段目标库用 btree 为默认(全文等见各 Generator 分支)
			def.setAccessMethod("btree");
			out.add(def);
		}
		return out;
	}

	/** Oracle 函数索引/虚拟列内部名，不可迁移 */
	private static boolean isOracleSystemColumn(String columnName) {
		return columnName != null && columnName.toLowerCase().matches("sys_nc\\d+\\$");
	}

	/** Oracle 系统生成的约束索引名（常与唯一约束/主键重复） */
	private static boolean isOracleSystemIndexName(String indexName) {
		return indexName != null && indexName.toLowerCase().matches("sys_c\\d+");
	}

	private static List<String> filterMigratableColumns(List<String> cols, Table table) {
		List<String> filtered = new ArrayList<>();
		for (String col : cols) {
			if (col == null || isOracleSystemColumn(col)) {
				continue;
			}
			if (table != null && table.getColumnsMap() != null && !table.getColumnsMap().isEmpty()
					&& !table.getColumnsMap().containsKey(col)) {
				continue;
			}
			filtered.add(col);
		}
		return filtered;
	}
}
