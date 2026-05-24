package database.ddl.transfer.object.extractor;

import database.ddl.transfer.object.DbObject;
import database.ddl.transfer.object.DbObjectType;
import database.ddl.transfer.object.ObjectMigrateOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 从源数据库提取各类对象 DDL 的抽象基类
 */
public abstract class AbstractObjectExtractor {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final Connection connection;

    protected AbstractObjectExtractor(Connection connection) {
        this.connection = connection;
    }

    /**
     * 按用户选项提取所有选中的对象类型
     */
    public final List<DbObject> extract(ObjectMigrateOptions options) {
        List<DbObject> all = new ArrayList<>();
        for (DbObjectType type : DbObjectType.values()) {
            if (!options.isEnabled(type)) continue;
            logger.info("  正在提取 {}...", type.getDisplayName());
            try {
                List<DbObject> list = extractByType(type);
                if (list != null) all.addAll(list);
                logger.info("  {} 提取完成，共 {} 个", type.getDisplayName(),
                        list == null ? 0 : list.size());
            } catch (Exception e) {
                logger.warn("  {} 提取失败（已跳过）：{}", type.getDisplayName(), e.getMessage());
            }
        }
        return all;
    }

    /** 按对象类型分发提取，子类覆写不支持的可直接返回空列表 */
    protected List<DbObject> extractByType(DbObjectType type) {
        switch (type) {
            case PROCEDURE:  return extractProcedures();
            case FUNCTION:   return extractFunctions();
            case TRIGGER:    return extractTriggers();
            case PACKAGE:    return extractPackages();
            case VIEW:       return extractViews();
            case SYNONYM:    return extractSynonyms();
            case SEQUENCE:   return extractSequences();
            case JOB:        return extractJobs();
            case ROLE_GRANT: return extractRoleGrants();
            default:         return Collections.emptyList();
        }
    }

    protected List<DbObject> extractProcedures() { return Collections.emptyList(); }
    protected List<DbObject> extractFunctions()  { return Collections.emptyList(); }
    protected List<DbObject> extractTriggers()   { return Collections.emptyList(); }
    protected List<DbObject> extractPackages()   { return Collections.emptyList(); }
    protected List<DbObject> extractViews()      { return Collections.emptyList(); }
    protected List<DbObject> extractSynonyms()   { return Collections.emptyList(); }
    protected List<DbObject> extractSequences()  { return Collections.emptyList(); }
    protected List<DbObject> extractJobs()       { return Collections.emptyList(); }
    protected List<DbObject> extractRoleGrants() { return Collections.emptyList(); }

    // ─── JDBC 工具方法 ───

    protected void close(PreparedStatement ps, ResultSet rs) {
        if (rs != null) { try { rs.close(); } catch (Exception ignored) {} }
        if (ps != null) { try { ps.close(); } catch (Exception ignored) {} }
    }

    protected List<String> queryNames(String sql, Object... params) {
        List<String> names = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            rs = ps.executeQuery();
            while (rs.next()) names.add(rs.getString(1));
        } catch (Exception e) {
            logger.warn("queryNames 失败：{}", e.getMessage());
        } finally {
            close(ps, rs);
        }
        return names;
    }

    protected String querySingleString(String sql, Object... params) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            rs = ps.executeQuery();
            if (rs.next()) {
                Object v = rs.getObject(1);
                return v != null ? v.toString().trim() : null;
            }
        } catch (Exception e) {
            logger.warn("querySingleString 失败：{}", e.getMessage());
        } finally {
            close(ps, rs);
        }
        return null;
    }
}
