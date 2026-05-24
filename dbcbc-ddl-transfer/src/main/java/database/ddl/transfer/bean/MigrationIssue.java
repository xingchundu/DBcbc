package database.ddl.transfer.bean;

/**
 * DDL 迁移中未能完成的对象及原因（表 / 二级索引）
 */
public class MigrationIssue {

    public enum Kind {
        TABLE, INDEX
    }

    private final Kind kind;
    private final String tableName;
    private final String indexName;
    private final String reason;

    public MigrationIssue(Kind kind, String tableName, String indexName, String reason) {
        this.kind = kind;
        this.tableName = tableName;
        this.indexName = indexName;
        this.reason = reason == null ? "未知原因" : reason;
    }

    public static MigrationIssue table(String tableName, String reason) {
        return new MigrationIssue(Kind.TABLE, tableName, null, reason);
    }

    public static MigrationIssue index(String tableName, String indexName, String reason) {
        return new MigrationIssue(Kind.INDEX, tableName, indexName, reason);
    }

    public Kind getKind() {
        return kind;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getReason() {
        return reason;
    }

    public String formatBrief() {
        if (kind == Kind.TABLE) {
            return tableName + "(" + reason + ")";
        }
        String in = indexName == null ? "" : indexName;
        return tableName + "." + in + "(" + reason + ")";
    }
}
