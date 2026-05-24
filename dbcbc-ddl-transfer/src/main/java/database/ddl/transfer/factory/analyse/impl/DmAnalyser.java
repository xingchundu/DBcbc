package database.ddl.transfer.factory.analyse.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.IndexDefinition;
import database.ddl.transfer.bean.PrimaryKey;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.factory.analyse.Analyser;
import database.ddl.transfer.utils.StringUtil;

public class DmAnalyser extends Analyser {

    private final String CONSTRAINT_NAME_PRIMARY_KEY = "P";

    public DmAnalyser(Connection connection) {
        super(connection);
    }

    @Override
    protected DataBaseDefine getDataBaseDefines(Connection connection) {
        DataBaseDefine dataBaseDefine = new DataBaseDefine();
        try {
            dataBaseDefine.setCatalog(connection.getMetaData().getUserName());
            dataBaseDefine.setCharacterSetDataBase("utf8");
            dataBaseDefine.setCollationDataBase("UTF8");
        } catch (SQLException e) {
            throw new RuntimeException("获取DM库定义失败", e);
        }
        return dataBaseDefine;
    }

    @Override
    protected List<Table> getTableDefines(Connection connection, String catalog, String schema) {
        String sql = "SELECT TABLE_NAME, COMMENTS FROM USER_TAB_COMMENTS WHERE TABLE_TYPE = 'TABLE'";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Table> tableList = new ArrayList<>();
        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Table table = new Table();
                table.setTableName(resultSet.getString("TABLE_NAME").toLowerCase());
                table.setTableComment(resultSet.getString("COMMENTS"));
                tableList.add(table);
            }
        } catch (Throwable e) {
            throw new RuntimeException("获取DM表定义失败", e);
        } finally {
            this.releaseResources(preparedStatement, resultSet);
        }
        return tableList;
    }

    @Override
    protected List<Column> getColumnDefines(Connection connection, String catalog, String schema) {
        String sql = "SELECT a.TABLE_NAME, a.COLUMN_NAME, b.COMMENTS, a.COLUMN_ID, a.DATA_DEFAULT, a.NULLABLE, "
                + "a.DATA_TYPE, a.CHAR_LENGTH, a.DATA_PRECISION, a.DATA_SCALE, d.CONSTRAINT_TYPE "
                + "FROM USER_TAB_COLS a "
                + "INNER JOIN USER_COL_COMMENTS b ON b.TABLE_NAME = a.TABLE_NAME AND b.COLUMN_NAME = a.COLUMN_NAME "
                + "LEFT JOIN USER_CONS_COLUMNS c ON c.TABLE_NAME = a.TABLE_NAME AND c.COLUMN_NAME = a.COLUMN_NAME "
                + "LEFT JOIN USER_CONSTRAINTS d ON d.TABLE_NAME = c.TABLE_NAME AND d.CONSTRAINT_NAME = c.CONSTRAINT_NAME "
                + "ORDER BY a.COLUMN_ID";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Column> columnList = new ArrayList<>();
        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                columnList.add(recordColumn(resultSet));
            }
        } catch (Throwable e) {
            throw new RuntimeException("获取DM表字段定义失败", e);
        } finally {
            this.releaseResources(preparedStatement, resultSet);
        }
        return columnList;
    }

    private Column recordColumn(ResultSet resultSet) throws SQLException {
        Column column = new Column();
        column.setDataBaseType(DataBaseType.DM);
        column.setTableName(resultSet.getString("TABLE_NAME").toLowerCase());
        column.setColumnName(resultSet.getString("COLUMN_NAME").toLowerCase());
        column.setColumnComment(resultSet.getString("COMMENTS"));
        column.setColumnOrder(resultSet.getInt("COLUMN_ID"));
        column.setDefaultDefine(resultSet.getString("DATA_DEFAULT"));
        column.setNullAble(!"N".equalsIgnoreCase(resultSet.getString("NULLABLE")));
        column.setColumnType(null);
        column.setColumnKey(resultSet.getString("CONSTRAINT_TYPE"));
        column.setExtra(null);
        column.setDataType(resultSet.getString("DATA_TYPE"));

        if (column.notTextType() && column.notBlobType() && column.notClobType()) {
            if (resultSet.getObject("DATA_PRECISION") != null) {
                column.setPrecision(resultSet.getInt("DATA_PRECISION"));
            } else if (resultSet.getObject("CHAR_LENGTH") != null) {
                column.setStrLength(resultSet.getInt("CHAR_LENGTH"));
            }
            if (resultSet.getObject("DATA_SCALE") != null) {
                column.setScale(resultSet.getInt("DATA_SCALE"));
            }
        }
        return column;
    }

    @Override
    protected List<PrimaryKey> getPrimaryKeyDefines(Connection connection, String catalog, String schema) {
        String sql = "SELECT ucc.TABLE_NAME, ucc.COLUMN_NAME, ucc.POSITION, uc.CONSTRAINT_NAME "
                + "FROM USER_CONS_COLUMNS ucc "
                + "INNER JOIN USER_CONSTRAINTS uc ON uc.CONSTRAINT_NAME = ucc.CONSTRAINT_NAME AND uc.TABLE_NAME = ucc.TABLE_NAME "
                + "WHERE uc.CONSTRAINT_TYPE = ? "
                + "ORDER BY ucc.TABLE_NAME, ucc.POSITION";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<PrimaryKey> primaryKeyList = new ArrayList<>();
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, CONSTRAINT_NAME_PRIMARY_KEY);
            resultSet = preparedStatement.executeQuery();

            Map<String, PrimaryKey> primaryKeyMap = new HashMap<>();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME").toLowerCase();
                PrimaryKey primaryKey = primaryKeyMap.get(tableName);
                if (primaryKey == null) {
                    primaryKey = new PrimaryKey();
                    primaryKey.setTableName(tableName);
                    String constraintName = resultSet.getString("CONSTRAINT_NAME");
                    if (constraintName != null) {
                        primaryKey.setPkName(constraintName.toLowerCase());
                    }
                    primaryKeyList.add(primaryKey);
                    primaryKeyMap.put(tableName, primaryKey);
                }
                primaryKey.addColumn(resultSet.getString("COLUMN_NAME").toLowerCase());
            }
        } catch (Throwable e) {
            throw new RuntimeException("获取DM表主键定义失败", e);
        } finally {
            this.releaseResources(preparedStatement, resultSet);
        }
        return primaryKeyList;
    }

    @Override
    protected List<IndexDefinition> getSecondaryIndexDefines(Connection connection, String catalog, String schema,
            Map<String, Table> tableMap) {
        // 达梦 JDBC DatabaseMetaData#getIndexInfo 常无法返回二级索引，改查 USER_INDEXES / USER_IND_COLUMNS
        String sql = "SELECT ui.TABLE_NAME, ui.INDEX_NAME, ui.UNIQUENESS, ui.INDEX_TYPE, "
                + "uic.COLUMN_NAME, uic.COLUMN_POSITION "
                + "FROM USER_INDEXES ui "
                + "INNER JOIN USER_IND_COLUMNS uic ON ui.INDEX_NAME = uic.INDEX_NAME AND ui.TABLE_NAME = uic.TABLE_NAME "
                + "WHERE NOT EXISTS ("
                + "  SELECT 1 FROM USER_CONSTRAINTS uc "
                + "  WHERE uc.CONSTRAINT_TYPE = 'P' AND uc.INDEX_NAME = ui.INDEX_NAME AND uc.TABLE_NAME = ui.TABLE_NAME"
                + ") "
                + "ORDER BY ui.TABLE_NAME, ui.INDEX_NAME, uic.COLUMN_POSITION";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Map<String, IndexDefinition> byKey = new LinkedHashMap<>();
        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String indexName = resultSet.getString("INDEX_NAME");
                if (tableName == null || indexName == null) {
                    continue;
                }
                tableName = tableName.toLowerCase();
                indexName = indexName.toLowerCase();
                if (isDmSystemIndexName(indexName)) {
                    continue;
                }
                String columnName = resultSet.getString("COLUMN_NAME");
                if (columnName == null || isDmSystemColumn(columnName)) {
                    continue;
                }
                columnName = columnName.toLowerCase();
                Table table = tableMap == null ? null : tableMap.get(tableName);
                if (table != null && table.getColumnsMap() != null && !table.getColumnsMap().isEmpty()
                        && !table.getColumnsMap().containsKey(columnName)) {
                    continue;
                }
                String key = tableName + "\0" + indexName;
                IndexDefinition def = byKey.get(key);
                if (def == null) {
                    def = new IndexDefinition();
                    def.setTableName(tableName);
                    def.setIndexName(indexName);
                    def.setUnique("UNIQUE".equalsIgnoreCase(resultSet.getString("UNIQUENESS")));
                    String indexType = resultSet.getString("INDEX_TYPE");
                    def.setAccessMethod(mapDmIndexType(indexType));
                    byKey.put(key, def);
                }
                def.addColumnName(columnName);
            }
        } catch (Throwable e) {
            throw new RuntimeException("获取 DM 二级索引失败", e);
        } finally {
            this.releaseResources(preparedStatement, resultSet);
        }
        List<IndexDefinition> all = new ArrayList<>();
        for (IndexDefinition def : byKey.values()) {
            if (def.getColumnNames() == null || def.getColumnNames().isEmpty()) {
                continue;
            }
            Table table = tableMap == null ? null : tableMap.get(def.getTableName());
            if (table != null && table.isSameIndexColumnsAsPrimaryKey(def.getColumnNames())) {
                continue;
            }
            all.add(def);
        }
        return all;
    }

    private static boolean isDmSystemColumn(String columnName) {
        return columnName != null && columnName.toLowerCase().matches("sys_nc\\d+\\$");
    }

    private static boolean isDmSystemIndexName(String indexName) {
        return indexName != null && indexName.toLowerCase().matches("sys_c\\d+");
    }

    private static String mapDmIndexType(String indexType) {
        if (StringUtil.isBlank(indexType)) {
            return "btree";
        }
        String t = indexType.toLowerCase();
        if (t.contains("bitmap")) {
            return "bitmap";
        }
        return "btree";
    }
}
