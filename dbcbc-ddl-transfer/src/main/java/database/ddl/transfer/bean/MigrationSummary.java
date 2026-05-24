package database.ddl.transfer.bean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import database.ddl.transfer.object.ObjectMigrateResult;

/**
 * DDL 迁移完成后的汇总（表结构阶段 + 可选的对象迁移阶段）
 */
public class MigrationSummary {

    private final ObjectCounts source;
    private final ObjectCounts target;
    private final List<MigrationIssue> issues;
    /** 对象迁移阶段结果，若未执行则为 null */
    private ObjectMigrateResult objectResult;

    public MigrationSummary(ObjectCounts source, ObjectCounts target) {
        this(source, target, Collections.emptyList());
    }

    public MigrationSummary(ObjectCounts source, ObjectCounts target, List<MigrationIssue> issues) {
        this.source = source;
        this.target = target;
        this.issues = issues == null ? new ArrayList<>() : new ArrayList<>(issues);
    }

    public ObjectCounts getSource() { return source; }
    public ObjectCounts getTarget() { return target; }

    public List<MigrationIssue> getIssues() {
        return Collections.unmodifiableList(issues);
    }

    public ObjectMigrateResult getObjectResult() { return objectResult; }
    public void setObjectResult(ObjectMigrateResult r) { this.objectResult = r; }

    public String formatMessage() {
        String base = String.format(
            "【表结构】迁移完成：源库 — 表 %d，索引 %d；目标库 — 表 %d，索引 %d",
            source.getTableCount(), source.getIndexCount(),
            target.getTableCount(), target.getIndexCount());
        int missingTables = countKind(MigrationIssue.Kind.TABLE);
        int missingIndexes = countKind(MigrationIssue.Kind.INDEX);
        if (missingTables > 0 || missingIndexes > 0) {
            base += String.format("；未迁移 — 表 %d，索引 %d", missingTables, missingIndexes);
        }
        if (!issues.isEmpty()) {
            base += "\n" + formatIssueDetails();
        }
        if (objectResult != null) {
            base += "\n" + objectResult.formatMessage();
        }
        return base;
    }

    private int countKind(MigrationIssue.Kind kind) {
        int n = 0;
        for (MigrationIssue issue : issues) {
            if (issue.getKind() == kind) {
                n++;
            }
        }
        return n;
    }

    private String formatIssueDetails() {
        StringBuilder sb = new StringBuilder();
        List<String> tableLines = new ArrayList<>();
        List<String> indexLines = new ArrayList<>();
        for (MigrationIssue issue : issues) {
            if (issue.getKind() == MigrationIssue.Kind.TABLE) {
                tableLines.add(issue.formatBrief());
            } else {
                indexLines.add(issue.formatBrief());
            }
        }
        if (!tableLines.isEmpty()) {
            sb.append("未迁移表：").append(String.join("；", tableLines));
        }
        if (!indexLines.isEmpty()) {
            if (sb.length() > 0) {
                sb.append("\n");
            }
            sb.append("未迁移索引：").append(String.join("；", indexLines));
        }
        return sb.toString();
    }
}
