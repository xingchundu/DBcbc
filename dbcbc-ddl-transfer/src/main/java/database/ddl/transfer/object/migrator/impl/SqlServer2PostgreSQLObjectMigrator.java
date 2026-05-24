package database.ddl.transfer.object.migrator.impl;

import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.migrator.AbstractObjectMigrator;

import java.sql.Connection;

/**
 * SQL Server → PostgreSQL 对象迁移器
 *
 * <ul>
 *   <li>VIEW        —— 自动转换语法
 *   <li>SEQUENCE    —— 自动转换
 *   <li>PROCEDURE / FUNCTION / TRIGGER —— 最大努力 T-SQL→PL/pgSQL 替换 + 警告
 *   <li>JOB、ROLE_GRANT —— 由基类输出日志，不执行
 * </ul>
 */
public class SqlServer2PostgreSQLObjectMigrator extends AbstractObjectMigrator {

    public SqlServer2PostgreSQLObjectMigrator(Connection targetConnection) {
        super(targetConnection);
    }

    @Override
    protected ConvertResult convert(DbObject obj) {
        if (obj == null || obj.getDdl() == null) return null;
        String ddl = obj.getDdl().trim();
        switch (obj.getType()) {
            case VIEW:      return convertView(ddl);
            case SEQUENCE:  return convertSequence(ddl);
            case PROCEDURE: return convertProcedure(ddl, obj.getName());
            case FUNCTION:  return convertFunction(ddl, obj.getName());
            case TRIGGER:   return convertTrigger(ddl, obj.getName());
            default:        return new ConvertResult(ddl);
        }
    }

    // ─────────────────── VIEW ───────────────────

    private ConvertResult convertView(String ddl) {
        String r = tsqlToPgsqlCommon(ddl)
            .replaceAll("(?i)\\bCREATE\\s+VIEW\\b",           "CREATE OR REPLACE VIEW")
            .replaceAll("(?i)\\bWITH\\s+SCHEMABINDING\\b",    "")
            .trim();
        if (!r.endsWith(";")) r += ";";
        return new ConvertResult(r);
    }

    // ─────────────────── SEQUENCE ───────────────────

    private ConvertResult convertSequence(String ddl) {
        String r = ddl
            // 去掉方括号标识符
            .replaceAll("\\[([^\\]]+)\\]", "$1")
            // AS BIGINT → (PG 序列不需要 AS 类型)
            .replaceAll("(?i)\\bAS\\s+(BIGINT|INT|SMALLINT)\\b", "")
            .replaceAll("(?i)\\bNO\\s+CYCLE\\b", "NO CYCLE")
            .replaceAll("(?i)\\bCREATE\\s+SEQUENCE\\b", "CREATE SEQUENCE IF NOT EXISTS")
            .trim();
        if (!r.endsWith(";")) r += ";";
        return new ConvertResult(r);
    }

    // ─────────────────── PROCEDURE ───────────────────

    private ConvertResult convertProcedure(String ddl, String name) {
        String r = tsqlToPgsqlCommon(ddl)
            .replaceAll("(?i)\\bCREATE\\s+PROCEDURE\\b",    "CREATE OR REPLACE PROCEDURE")
            .replaceAll("(?i)\\bCREATE\\s+PROC\\b",         "CREATE OR REPLACE PROCEDURE")
            .replaceAll("(?i)\\bSET\\s+NOCOUNT\\s+ON\\s*;?","")
            .replaceAll("@(\\w+)",                           "p_$1");
        if (!r.toUpperCase().contains("LANGUAGE")) {
            r += "\nLANGUAGE plpgsql;";
        }
        return new ConvertResult(r,
                "存储过程 [" + name + "] 已做基础 T-SQL→PL/pgSQL 替换，差异较大，请人工核查");
    }

    // ─────────────────── FUNCTION ───────────────────

    private ConvertResult convertFunction(String ddl, String name) {
        String r = tsqlToPgsqlCommon(ddl)
            .replaceAll("(?i)\\bCREATE\\s+FUNCTION\\b", "CREATE OR REPLACE FUNCTION")
            .replaceAll("@(\\w+)",                        "p_$1")
            .replaceAll("(?i)\\bRETURNS\\s+TABLE\\b",    "RETURNS TABLE");
        if (!r.toUpperCase().contains("LANGUAGE")) {
            r += "\nLANGUAGE plpgsql;";
        }
        return new ConvertResult(r,
                "函数 [" + name + "] 已做基础替换，请人工核查");
    }

    // ─────────────────── TRIGGER ───────────────────

    private ConvertResult convertTrigger(String ddl, String name) {
        String r = tsqlToPgsqlCommon(ddl)
            .replaceAll("(?i)\\bINSERTED\\b", "NEW")
            .replaceAll("(?i)\\bDELETED\\b",  "OLD");
        return new ConvertResult(r,
                "触发器 [" + name + "] 已做基础替换，PG 触发器需独立函数，请人工完善");
    }

    // ─────────────────── 公共替换 ───────────────────

    private String tsqlToPgsqlCommon(String ddl) {
        return ddl
            // 去掉方括号标识符
            .replaceAll("\\[([^\\]]+)\\]",                 "\"$1\"")
            // 数据类型
            .replaceAll("(?i)\\bNVARCHAR\\b",              "VARCHAR")
            .replaceAll("(?i)\\bNTEXT\\b",                 "TEXT")
            .replaceAll("(?i)\\bDATETIME2?\\b",            "TIMESTAMP")
            .replaceAll("(?i)\\bSMALLDATETIME\\b",        "TIMESTAMP")
            .replaceAll("(?i)\\bMONEY\\b",                 "NUMERIC(19,4)")
            .replaceAll("(?i)\\bBIT\\b",                   "BOOLEAN")
            .replaceAll("(?i)\\bTINYINT\\b",               "SMALLINT")
            .replaceAll("(?i)\\bUNIQUEIDENTIFIER\\b",     "UUID")
            // 函数替换
            .replaceAll("(?i)\\bGETDATE\\s*\\(\\s*\\)",   "CURRENT_TIMESTAMP")
            .replaceAll("(?i)\\bISNULL\\s*\\(",            "COALESCE(")
            .replaceAll("(?i)\\bLEN\\s*\\(",               "LENGTH(")
            .replaceAll("(?i)\\bCHARINDEX\\s*\\(",        "POSITION(")
            .replaceAll("(?i)\\bCONVERT\\s*\\([^,]+,\\s*([^)]+)\\)", "CAST($1 AS TEXT)")
            .replaceAll("(?i)\\bPRINT\\s+",               "RAISE NOTICE ")
            // TOP N → LIMIT N
            .replaceAll("(?i)\\bTOP\\s+(\\d+)\\b",        "LIMIT $1")
            ;
    }
}
