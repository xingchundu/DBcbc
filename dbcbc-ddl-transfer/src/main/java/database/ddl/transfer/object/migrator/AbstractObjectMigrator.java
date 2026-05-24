package database.ddl.transfer.object.migrator;

import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.DbObjectType;
import database.ddl.transfer.object.ObjectMigrateItem;
import database.ddl.transfer.object.ObjectMigrateResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 * 对象迁移器抽象基类：将源对象 DDL 转换后执行到目标库
 */
public abstract class AbstractObjectMigrator {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final Connection targetConnection;

    protected AbstractObjectMigrator(Connection targetConnection) {
        this.targetConnection = targetConnection;
    }

    /**
     * 批量迁移对象列表
     */
    public final ObjectMigrateResult migrate(List<DbObject> objects) {
        ObjectMigrateResult result = new ObjectMigrateResult();
        if (objects == null || objects.isEmpty()) {
            logger.info("  无可迁移的数据库对象");
            return result;
        }
        logger.info("  共需迁移对象 {} 个", objects.size());
        for (DbObject obj : objects) {
            ObjectMigrateItem item = migrateOne(obj);
            result.add(item);
            String statusStr = item.getStatus().name();
            if (item.getMessage() != null) {
                logger.info("    [{}][{}] {} → {}：{}",
                        obj.getType().getDisplayName(), statusStr, obj.getName(), statusStr, item.getMessage());
            } else {
                logger.info("    [{}][{}] {}",
                        obj.getType().getDisplayName(), statusStr, obj.getName());
            }
        }
        logger.info(result.formatMessage());
        return result;
    }

    private ObjectMigrateItem migrateOne(DbObject obj) {
        try {
            // Job 和 权限/角色 仅记录日志，不执行
            if (obj.getType() == DbObjectType.JOB || obj.getType() == DbObjectType.ROLE_GRANT) {
                logger.info("    [提示] {} DDL 已记录到日志，请手动在目标库执行：\n{}",
                        obj.getType().getDisplayName(), obj.getDdl());
                return ObjectMigrateItem.warn(obj.getType(), obj.getName(),
                        "已记录到日志，需手动执行");
            }
            // 转换 DDL
            ConvertResult cvt = convert(obj);
            if (cvt == null || cvt.getDdl() == null || cvt.getDdl().trim().isEmpty()) {
                return ObjectMigrateItem.skipped(obj.getType(), obj.getName(),
                        "目标库不支持此对象类型或转换后 DDL 为空");
            }
            if (cvt.isWarnOnly()) {
                logger.warn("    [警告] {} [{}] DDL 已转换但需人工检查：\n{}",
                        obj.getType().getDisplayName(), obj.getName(), cvt.getDdl());
                return ObjectMigrateItem.warn(obj.getType(), obj.getName(), cvt.getWarnMsg());
            }
            // 执行 DDL
            executeDdl(cvt.getDdl());
            // 若有额外 DDL（如 Package Body）
            if (obj.getExtra() != null && !obj.getExtra().trim().isEmpty()) {
                ConvertResult cvtExtra = convertExtra(obj);
                if (cvtExtra != null && !cvtExtra.getDdl().trim().isEmpty()) {
                    try { executeDdl(cvtExtra.getDdl()); } catch (Exception e) {
                        logger.warn("    执行附加 DDL 失败：{}", e.getMessage());
                    }
                }
            }
            return ObjectMigrateItem.success(obj.getType(), obj.getName());
        } catch (Exception e) {
            return ObjectMigrateItem.failed(obj.getType(), obj.getName(), e.getMessage());
        }
    }

    /**
     * 将源 DDL 转换为目标库方言。子类实现。
     * 返回 null 或空 DDL 表示跳过该对象。
     */
    protected abstract ConvertResult convert(DbObject obj);

    /** 转换附加 DDL（如 Package Body），默认不转换 */
    protected ConvertResult convertExtra(DbObject obj) {
        return new ConvertResult(obj.getExtra());
    }

    /** 执行单条 DDL（自动去掉末尾分号，某些驱动不支持） */
    protected void executeDdl(String ddl) throws Exception {
        String sql = ddl.trim();
        // 去掉末尾的单个分号（如有）
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        try (Statement st = targetConnection.createStatement()) {
            st.execute(sql);
        }
    }

    // ─── 内部 DTO ───

    protected static class ConvertResult {
        private final String ddl;
        private boolean warnOnly;
        private String warnMsg;

        public ConvertResult(String ddl) {
            this.ddl = ddl;
        }

        public ConvertResult(String ddl, String warnMsg) {
            this.ddl      = ddl;
            this.warnOnly = true;
            this.warnMsg  = warnMsg;
        }

        public String  getDdl()      { return ddl;      }
        public boolean isWarnOnly()  { return warnOnly; }
        public String  getWarnMsg()  { return warnMsg;  }
    }
}
