package database.ddl.transfer.object.migrator.impl;

import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.migrator.AbstractObjectMigrator;

import java.sql.Connection;

/**
 * Oracle → 达梦(DM) 对象迁移器
 *
 * 达梦高度兼容 Oracle 语法，绝大多数对象可以直接执行；
 * 仅处理少数细微差异（如 FORCE VIEW 关键字）。
 */
public class Oracle2DmObjectMigrator extends AbstractObjectMigrator {

    public Oracle2DmObjectMigrator(Connection targetConnection) {
        super(targetConnection);
    }

    @Override
    protected ConvertResult convert(DbObject obj) {
        if (obj == null || obj.getDdl() == null) return null;
        String ddl = obj.getDdl().trim();
        switch (obj.getType()) {
            case VIEW:
                // DM 不支持 FORCE 关键字
                ddl = ddl.replaceAll("(?i)CREATE\\s+OR\\s+REPLACE\\s+FORCE\\s+VIEW",
                        "CREATE OR REPLACE VIEW");
                return new ConvertResult(ddl);
            case SEQUENCE:
            case SYNONYM:
            case PROCEDURE:
            case FUNCTION:
            case TRIGGER:
            case PACKAGE:
                // DM 兼容 Oracle PL/SQL，直接使用
                return new ConvertResult(ddl);
            default:
                return new ConvertResult(ddl);
        }
    }
}
