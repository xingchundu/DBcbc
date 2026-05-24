package database.ddl.transfer.object.migrator.impl;

import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.DbObjectType;
import database.ddl.transfer.object.migrator.AbstractObjectMigrator;

import java.sql.Connection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Oracle → PostgreSQL 对象迁移器
 *
 * <p>迁移策略：
 * <ul>
 *   <li>VIEW、SEQUENCE、SYNONYM(→VIEW) —— 自动转换语法
 *   <li>PROCEDURE、FUNCTION、TRIGGER   —— 最大努力替换 + 警告提示
 *   <li>PACKAGE   —— 拆分为注释提示（PG 无包概念）
 *   <li>JOB、ROLE_GRANT —— 由基类输出日志，不执行
 * </ul>
 */
public class Oracle2PostgreSQLObjectMigrator extends AbstractObjectMigrator {

    public Oracle2PostgreSQLObjectMigrator(Connection targetConnection) {
        super(targetConnection);
    }

    @Override
    protected ConvertResult convert(DbObject obj) {
        if (obj == null || obj.getDdl() == null) return null;
        switch (obj.getType()) {
            case VIEW:      return convertView(obj.getDdl());
            case SEQUENCE:  return convertSequence(obj.getDdl(), obj.getName());
            case SYNONYM:   return convertSynonymToView(obj);
            case PROCEDURE: return convertProcedure(obj.getDdl(), obj.getName());
            case FUNCTION:  return convertFunction(obj.getDdl(), obj.getName());
            case TRIGGER:   return convertTrigger(obj.getDdl(), obj.getName());
            case PACKAGE:
                logger.warn("    [PACKAGE] PostgreSQL 不支持 Package，[{}] 跳过，请手动拆分为独立函数", obj.getName());
                return null;
            default:
                return new ConvertResult(obj.getDdl());
        }
    }

    // ─────────────────── VIEW ───────────────────

    private ConvertResult convertView(String ddl) {
        String r = ddl
            .replaceAll("(?i)CREATE\\s+OR\\s+REPLACE\\s+FORCE\\s+VIEW", "CREATE OR REPLACE VIEW")
            .replaceAll("(?i)\\s+WITH\\s+READ\\s+ONLY\\s*;?\\s*$", "")
            .trim();
        if (!r.endsWith(";")) r += ";";
        return new ConvertResult(r);
    }

    // ─────────────────── SEQUENCE ───────────────────

    private ConvertResult convertSequence(String ddl, String name) {
        String r = ddl
            .replaceAll("(?i)\\bNOCYCLE\\b",  "NO CYCLE")
            .replaceAll("(?i)\\bNOCACHE\\b",  "CACHE 1")
            .replaceAll("(?i)\\bNOORDER\\b",  "")
            .replaceAll("(?i)\\bORDER\\b",    "")
            // Oracle MAXVALUE 极大值 → PG BIGINT 最大值
            .replaceAll("(?i)MAXVALUE\\s+9{15,}", "MAXVALUE 9223372036854775807")
            .replaceAll("(?i)CREATE\\s+SEQUENCE", "CREATE SEQUENCE IF NOT EXISTS");
        if (!r.trim().endsWith(";")) r = r.trim() + ";";
        return new ConvertResult(r);
    }

    // ─────────────────── SYNONYM → VIEW ───────────────────

    private ConvertResult convertSynonymToView(DbObject obj) {
        // CREATE OR REPLACE SYNONYM syn FOR schema.table
        Matcher m = Pattern.compile("(?i)\\bFOR\\s+([\\w.\"]+)\\s*$").matcher(obj.getDdl().trim());
        if (m.find()) {
            String target = m.group(1);
            String ddl = "CREATE OR REPLACE VIEW " + obj.getName() + " AS SELECT * FROM " + target + ";";
            return new ConvertResult(ddl,
                    "SYNONYM 已转换为 VIEW，请确认 " + target + " 在 PG 中可访问");
        }
        logger.warn("    无法解析同义词 [{}] 的目标，已跳过", obj.getName());
        return null;
    }

    // ─────────────────── PROCEDURE ───────────────────

    private ConvertResult convertProcedure(String ddl, String name) {
        String r = bestEffortPlsqlToPgsql(ddl)
            .replaceAll("(?i)\\bCREATE\\s+OR\\s+REPLACE\\s+PROCEDURE\\b",
                        "CREATE OR REPLACE PROCEDURE");
        if (!r.toUpperCase().contains("LANGUAGE")) {
            r += "\nLANGUAGE plpgsql;";
        }
        return new ConvertResult(r,
                "存储过程 [" + name + "] 已做基础语法替换，PL/SQL→PL/pgSQL 差异较大，请人工核查后再执行");
    }

    // ─────────────────── FUNCTION ───────────────────

    private ConvertResult convertFunction(String ddl, String name) {
        String r = bestEffortPlsqlToPgsql(ddl)
            .replaceAll("(?i)\\bCREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\b",
                        "CREATE OR REPLACE FUNCTION");
        if (!r.toUpperCase().contains("LANGUAGE")) {
            r += "\nLANGUAGE plpgsql;";
        }
        return new ConvertResult(r,
                "函数 [" + name + "] 已做基础语法替换，请人工核查");
    }

    // ─────────────────── TRIGGER ───────────────────

    private ConvertResult convertTrigger(String ddl, String name) {
        String r = ddl
            .replaceAll(":NEW\\.", "NEW.")
            .replaceAll(":OLD\\.", "OLD.")
            .replaceAll("(?i)\\bREFERENCING\\s+NEW\\s+AS\\s+NEW\\s+OLD\\s+AS\\s+OLD\\b", "")
            .replaceAll("(?i)\\bNUMBER\\b",   "NUMERIC")
            .replaceAll("(?i)\\bVARCHAR2\\b", "VARCHAR")
            .replaceAll("(?i)\\bSYSDATE\\b",  "CURRENT_TIMESTAMP");
        return new ConvertResult(r,
                "触发器 [" + name + "] 已做基础替换，PG 触发器需调用独立函数，请人工完善");
    }

    // ─────────────────── 公共替换 ───────────────────

    private String bestEffortPlsqlToPgsql(String ddl) {
        return ddl
            .replaceAll("(?i)\\bVARCHAR2\\b",             "VARCHAR")
            .replaceAll("(?i)\\bNUMBER\\b",               "NUMERIC")
            .replaceAll("(?i)\\bDATE\\b",                  "TIMESTAMP")
            .replaceAll("(?i)\\bSYSDATE\\b",              "CURRENT_TIMESTAMP")
            .replaceAll("(?i)\\bNVL\\s*\\(",              "COALESCE(")
            .replaceAll("(?i)\\bTO_DATE\\s*\\(([^,]+),([^)]+)\\)", "TO_TIMESTAMP($1,$2)")
            .replaceAll("(?i)\\bDBMS_OUTPUT\\.PUT_LINE\\b","RAISE NOTICE")
            .replaceAll("(?i)\\bEND\\s+(\\w+)\\s*;",     "END;")
            .replaceAll("(?i)\\bIS\\b(?=\\s*\\n)",        "AS $$")
            ;
    }
}
