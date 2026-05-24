package database.ddl.transfer.object;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 对象迁移阶段整体结果（汇总 + 明细列表）
 */
public class ObjectMigrateResult {

    private int successCount;
    private int warnCount;
    private int failedCount;
    private int skippedCount;
    private final List<ObjectMigrateItem> items = new ArrayList<>();

    public void add(ObjectMigrateItem item) {
        items.add(item);
        switch (item.getStatus()) {
            case SUCCESS: successCount++; break;
            case WARN:    warnCount++;    break;
            case FAILED:  failedCount++;  break;
            case SKIPPED: skippedCount++; break;
        }
    }

    public int getSuccessCount() { return successCount; }
    public int getWarnCount()    { return warnCount;    }
    public int getFailedCount()  { return failedCount;  }
    public int getSkippedCount() { return skippedCount; }
    public int getTotalCount()   { return successCount + warnCount + failedCount + skippedCount; }

    public List<ObjectMigrateItem> getItems() {
        return Collections.unmodifiableList(items);
    }

    public String formatMessage() {
        return String.format(
            "【对象迁移】完成：成功 %d，警告 %d，失败 %d，跳过 %d，共 %d 个",
            successCount, warnCount, failedCount, skippedCount, getTotalCount());
    }
}
