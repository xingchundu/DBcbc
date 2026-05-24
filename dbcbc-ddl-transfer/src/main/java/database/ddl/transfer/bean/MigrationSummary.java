package database.ddl.transfer.bean;

import database.ddl.transfer.object.ObjectMigrateResult;

/**
 * DDL 迁移完成后的汇总（表结构阶段 + 可选的对象迁移阶段）
 */
public class MigrationSummary {

    private final ObjectCounts source;
    private final ObjectCounts target;
    /** 对象迁移阶段结果，若未执行则为 null */
    private ObjectMigrateResult objectResult;

    public MigrationSummary(ObjectCounts source, ObjectCounts target) {
        this.source = source;
        this.target = target;
    }

    public ObjectCounts getSource() { return source; }
    public ObjectCounts getTarget() { return target; }

    public ObjectMigrateResult getObjectResult() { return objectResult; }
    public void setObjectResult(ObjectMigrateResult r) { this.objectResult = r; }

    public String formatMessage() {
        String base = String.format(
            "【表结构】迁移完成：源库 — 表 %d，索引 %d；目标库 — 表 %d，索引 %d",
            source.getTableCount(), source.getIndexCount(),
            target.getTableCount(), target.getIndexCount());
        if (objectResult != null) {
            base += "\n" + objectResult.formatMessage();
        }
        return base;
    }
}
