package database.ddl.transfer.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * 二级索引元数据(非主键): 普通/唯一/全文等,用于跨库结构迁移
 */
public class IndexDefinition {

	private String tableName;
	private String indexName;
	/** 是否为唯一索引(或唯一约束在部分库中对应的唯一索引) */
	private boolean unique;
	/** 索引列(有序); 仅支持普通列,表达式/函数类索引在源端抽取时过滤 */
	private List<String> columnNames;
	/**
	 * 源库索访问/类型提示,小写: btree, hash, bit, gin, gist, spgist, brin, fulltext(全文), spatial(空间), bitmap(Oracle), context(Oracle 域索引/全文) 等
	 */
	private String accessMethod;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public boolean isUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	public void addColumnName(String col) {
		if (this.columnNames == null) {
			this.columnNames = new ArrayList<>();
		}
		this.columnNames.add(col);
	}

	public String getAccessMethod() {
		return accessMethod;
	}

	public void setAccessMethod(String accessMethod) {
		this.accessMethod = accessMethod;
	}
}
