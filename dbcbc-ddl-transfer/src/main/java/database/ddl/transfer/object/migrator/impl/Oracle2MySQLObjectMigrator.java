package database.ddl.transfer.object.migrator.impl;

import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.migrator.AbstractObjectMigrator;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Oracle → MySQL 对象迁移器
 *
 * <ul>
 *   <li>VIEW        —— 语法自动转换
 *   <li>SEQUENCE    —— MySQL 无原生序列，输出注释提示
 *   <li>SYNONYM     —— MySQL 无 SYNONYM，跳过
 *   <li>PACKAGE     —— MySQL 无 PACKAGE，跳过
 *   <li>PROCEDURE/FUNCTION/TRIGGER —— 最大努力替换 + 警告
 * </ul>
 */
public class Oracle2MySQLObjectMigrator extends AbstractObjectMigrator {

    public Oracle2MySQLObjectMigrator(Connection targetConnection) {
        super(targetConnection);
    }

    @Override
    protected ConvertResult convert(DbObject obj) {
        if (obj == null || obj.getDdl() == null) return null;
        String ddl = obj.getDdl().trim();
        switch (obj.getType()) {
            case VIEW:
                return convertView(ddl);
            case SEQUENCE:
                logger.warn("    MySQL 无原生序列，[{}] 跳过，建议改用 AUTO_INCREMENT", obj.getName());
                return null;
            case SYNONYM:
                logger.warn("    MySQL 不支持 SYNONYM，[{}] 跳过", obj.getName());
                return null;
            case PACKAGE:
                logger.warn("    MySQL 不支持 PACKAGE，[{}] 跳过，请拆分为独立存储过程/函数", obj.getName());
                return null;
            case PROCEDURE:
                return convertProcedure(ddl, obj.getName());
            case FUNCTION:
                return convertFunction(ddl, obj.getName());
            case TRIGGER:
                return convertTrigger(ddl, obj.getName());
            default:
                return new ConvertResult(ddl);
        }
    }

    /** MySQL DDL 需要 DELIMITER 语义，直接通过 Statement 执行 */
    @Override
    protected void executeDdl(String ddl) throws Exception {
        String sql = ddl.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        try (Statement st = targetConnection.createStatement()) {
            st.execute(sql);
        }
    }

    private ConvertResult convertView(String ddl) {
        String r = ddl
            .replaceAll("(?i)CREATE\\s+OR\\s+REPLACE\\s+FORCE\\s+VIEW", "CREATE OR REPLACE VIEW")
            .replaceAll("\"([A-Za-z_][A-Za-z0-9_]*)\"", "`$1`")
            .replaceAll("(?i)\\bSYSDATE\\b", "NOW()")
            .replaceAll("(?i)\\s+WITH\\s+READ\\s+ONLY\\s*;?\\s*$", "")
            .trim();
        if (!r.endsWith(";")) r += ";";
        return new ConvertResult(r);
    }

    private ConvertResult convertProcedure(String ddl, String name) {
        String r = oracleToMysqlCommon(ddl);
        return new ConvertResult(r,
                "存储过程 [" + name + "] 已做基础语法替换，PL/SQL→MySQL SP 差异较大，请人工核查");
    }

    private ConvertResult convertFunction(String ddl, String name) {
        String r = oracleToMysqlCommon(ddl);
        return new ConvertResult(r,
                "函数 [" + name + "] 已做基础语法替换，请人工核查");
    }

    private ConvertResult convertTrigger(String ddl, String name) {
        String r = ddl
            .replaceAll(":NEW\\.", "NEW.")
            .replaceAll(":OLD\\.", "OLD.")
            .replaceAll("(?i)\\bNUMBER\\b",   "DECIMAL(18,6)")
            .replaceAll("(?i)\\bVARCHAR2\\b", "VARCHAR")
            .replaceAll("(?i)\\bSYSDATE\\b",  "NOW()");
        return new ConvertResult(r,
                "触发器 [" + name + "] 已做基础替换，请人工核查");
    }

    private String oracleToMysqlCommon(String ddl) {
        return ddl
            .replaceAll("(?i)\\bNUMBER\\b",                "DECIMAL(18,6)")
            .replaceAll("(?i)\\bVARCHAR2\\b",              "VARCHAR")
            .replaceAll("(?i)\\bSYSDATE\\b",               "NOW()")
            .replaceAll("(?i)\\bNVL\\s*\\(",               "IFNULL(")
            .replaceAll("(?i)\\bDBMS_OUTPUT\\.PUT_LINE\\s*\\(([^)]+)\\)", "SELECT $1")
            .replaceAll("(?i)\\bEXCEPTION\\s+WHEN\\s+OTHERS\\b", "-- EXCEPTION WHEN OTHERS")
            .replaceAll("\"([A-Za-z_][A-Za-z0-9_]*)\"", "`$1`");
    }
}
