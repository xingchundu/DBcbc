package database.ddl.transfer.bean;

/**
 * 数据库对象数量统计（表、二级索引）
 */
public class ObjectCounts {

    private final int tableCount;
    private final int indexCount;

    public ObjectCounts(int tableCount, int indexCount) {
        this.tableCount = tableCount;
        this.indexCount = indexCount;
    }

    public int getTableCount() {
        return tableCount;
    }

    public int getIndexCount() {
        return indexCount;
    }
}
