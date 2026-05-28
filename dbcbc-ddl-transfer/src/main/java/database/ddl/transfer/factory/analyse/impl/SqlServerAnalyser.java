package database.ddl.transfer.factory.analyse.impl;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.IndexDefinition;
import database.ddl.transfer.bean.PrimaryKey;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.factory.analyse.Analyser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL Server 数据库结构分析器（用于表结构迁移的源/目标端）
 */
public class SqlServerAnalyser extends Analyser {

    public SqlServerAnalyser(Connection connection) {
        super(connection);
    }

    @Override
    protected DataBaseDefine getDataBaseDefines(Connection connection) {
        DataBaseDefine def = new DataBaseDefine();
        try {
            def.setCatalog(connection.getCatalog());
            def.setCharacterSetDataBase("utf8");
            def.setCollationDataBase("utf8_general_ci");
        } catch (Exception e) {
            throw new RuntimeException("获取 SQL Server 库定义失败", e);
        }
        return def;
    }

    @Override
    protected List<Table> getTableDefines(Connection connection, String catalog, String schema) {
        List<Table> list = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql =
                "SELECT t.name AS table_name, ep.value AS table_comment " +
                "FROM sys.tables t " +
                "LEFT JOIN sys.extended_properties ep " +
                "  ON ep.major_id = t.object_id AND ep.minor_id = 0 " +
                "  AND ep.name = 'MS_Description' " +
                "WHERE t.schema_id = SCHEMA_ID() " +
                "ORDER BY t.name";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                Table tbl = new Table();
                tbl.setTableName(rs.getString("table_name").toLowerCase());
                Object cmt = rs.getObject("table_comment");
                if (cmt != null) tbl.setTableComment(cmt.toString());
                list.add(tbl);
            }
        } catch (Exception e) {
            throw new RuntimeException("获取 SQL Server 表定义失败", e);
        } finally {
            releaseResources(ps, rs);
        }
        return list;
    }

    @Override
    protected List<Column> getColumnDefines(Connection connection, String catalog, String schema) {
        List<Column> list = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql =
                "SELECT c.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION, " +
                "  c.COLUMN_DEFAULT, c.IS_NULLABLE, c.DATA_TYPE, " +
                "  c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE " +
                "FROM INFORMATION_SCHEMA.COLUMNS c " +
                "WHERE c.TABLE_SCHEMA = SCHEMA_NAME() " +
                "ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                Column col = new Column();
                col.setDataBaseType(DataBaseType.SQLSERVER);
                col.setTableName(rs.getString("TABLE_NAME").toLowerCase());
                col.setColumnName(rs.getString("COLUMN_NAME").toLowerCase());
                col.setColumnOrder(rs.getInt("ORDINAL_POSITION"));
                col.setDefaultDefine(rs.getString("COLUMN_DEFAULT"));
                col.setNullAble("YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                String dt = rs.getString("DATA_TYPE").toUpperCase();
                col.setDataType(dt);
                col.setColumnType(dt);  // TypeConverter 通过 getColumnType() 做映射
                Object charLen = rs.getObject("CHARACTER_MAXIMUM_LENGTH");
                if (charLen != null) {
                    long len = ((Number) charLen).longValue();
                    if (len > 0 && len <= Integer.MAX_VALUE) col.setStrLength((int) len);
                }
                Object prec  = rs.getObject("NUMERIC_PRECISION");
                Object scale = rs.getObject("NUMERIC_SCALE");
                if (prec  != null) col.setPrecision(((Number) prec).intValue());
                if (scale != null) col.setScale(((Number) scale).intValue());
                list.add(col);
            }
        } catch (Exception e) {
            throw new RuntimeException("获取 SQL Server 列定义失败", e);
        } finally {
            releaseResources(ps, rs);
        }
        return list;
    }

    @Override
    protected List<PrimaryKey> getPrimaryKeyDefines(Connection connection, String catalog, String schema) {
        List<PrimaryKey> list = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql =
                "SELECT t.name AS table_name, c.name AS column_name, ic.key_ordinal " +
                "FROM sys.indexes i " +
                "JOIN sys.tables t ON t.object_id = i.object_id " +
                "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
                "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id " +
                "WHERE i.is_primary_key = 1 AND t.schema_id = SCHEMA_ID() " +
                "ORDER BY t.name, ic.key_ordinal";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            Map<String, PrimaryKey> pkMap = new LinkedHashMap<>();
            while (rs.next()) {
                String tableName = rs.getString("table_name").toLowerCase();
                PrimaryKey pk = pkMap.get(tableName);
                if (pk == null) {
                    pk = new PrimaryKey();
                    pk.setTableName(tableName);
                    pkMap.put(tableName, pk);
                    list.add(pk);
                }
                pk.addColumn(rs.getString("column_name").toLowerCase());
            }
        } catch (Exception e) {
            throw new RuntimeException("获取 SQL Server 主键定义失败", e);
        } finally {
            releaseResources(ps, rs);
        }
        return list;
    }

    @Override
    protected List<IndexDefinition> getSecondaryIndexDefines(Connection connection,
            String catalog, String schema, Map<String, Table> tableMap) {
        List<IndexDefinition> list = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql =
                "SELECT t.name AS table_name, i.name AS index_name, i.is_unique, " +
                "  c.name AS column_name, ic.key_ordinal " +
                "FROM sys.indexes i " +
                "JOIN sys.tables t ON t.object_id = i.object_id " +
                "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
                "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id " +
                "WHERE i.is_primary_key = 0 AND i.type > 0 AND t.schema_id = SCHEMA_ID() " +
                "ORDER BY t.name, i.name, ic.key_ordinal";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            Map<String, IndexDefinition> idxMap = new LinkedHashMap<>();
            while (rs.next()) {
                String tblName = rs.getString("table_name").toLowerCase();
                String idxName = rs.getString("index_name").toLowerCase();
                String key = tblName + "." + idxName;
                IndexDefinition idx = idxMap.get(key);
                if (idx == null) {
                    idx = new IndexDefinition();
                    idx.setTableName(tblName);
                    idx.setIndexName(idxName);
                    idx.setUnique(rs.getBoolean("is_unique"));
                    idxMap.put(key, idx);
                    list.add(idx);
                }
                idx.addColumnName(rs.getString("column_name").toLowerCase());
            }
        } catch (Exception e) {
            logger.warn("获取 SQL Server 二级索引失败：{}", e.getMessage());
        } finally {
            releaseResources(ps, rs);
        }
        return list;
    }
}
