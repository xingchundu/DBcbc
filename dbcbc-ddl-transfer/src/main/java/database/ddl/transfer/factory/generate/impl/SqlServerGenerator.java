package database.ddl.transfer.factory.generate.impl;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.PrimaryKey;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.factory.generate.Generator;
import database.ddl.transfer.utils.StringUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * SQL Server 目标库 DDL 生成器
 * （主要用于 SqlServer → SqlServer 同库结构迁移；
 *  SqlServer → PG 路径的目标端使用 PostgreSqlGenerator）
 */
public class SqlServerGenerator extends Generator {

    public SqlServerGenerator(Connection connection, DataBaseDefine dataBaseDefine,
                               DBSettings targetDBSettings) {
        super(connection, dataBaseDefine, targetDBSettings);
    }

    @Override
    protected String getDataBaseDDL(DataBaseDefine dataBaseDefine) {
        // SQL Server 通常不需要在迁移时创建 Database；返回空表示库已存在
        return "";
    }

    @Override
    protected String getTableDDL(Table tableDefine) {
        StringBuilder sb = new StringBuilder("CREATE TABLE [")
                .append(tableDefine.getTableName()).append("] (\n");
        List<Column> columns = tableDefine.getColumns();
        for (Column col : columns) {
            sb.append("  ").append(getColumnDDL(col)).append(",\n");
        }
        PrimaryKey pk = tableDefine.getPrimaryKey();
        if (pk != null) {
            sb.append("  CONSTRAINT [PK_").append(tableDefine.getTableName())
              .append("] PRIMARY KEY (");
            List<String> pkCols = pk.getColumns();
            for (int i = 0; i < pkCols.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append("[").append(pkCols.get(i)).append("]");
            }
            sb.append(")\n");
        } else {
            // 删掉最后一列的逗号
            int lastComma = sb.lastIndexOf(",\n");
            if (lastComma >= 0) sb.delete(lastComma, lastComma + 2).append("\n");
        }
        sb.append(");");
        return sb.toString();
    }

    private String getColumnDDL(Column column) {
        StringBuilder sb = new StringBuilder("[").append(column.getColumnName()).append("] ")
                .append(mapType(column));
        if (!column.isNullAble()) sb.append(" NOT NULL");
        return sb.toString();
    }

    /** 将通用/Oracle/PG 类型名映射到 SQL Server 类型 */
    private String mapType(Column col) {
        String dt = col.getDataType() == null ? "" : col.getDataType().toUpperCase();
        // finalConvertDataType 优先（经过 TypeConverter 处理）
        String converted = col.getFinalConvertDataType();
        if (!StringUtil.isBlank(converted)) dt = converted.toUpperCase();
        switch (dt) {
            case "VARCHAR":
            case "VARCHAR2":
            case "CHARACTER VARYING":
                return col.getStrLength() != null ? "NVARCHAR(" + col.getStrLength() + ")" : "NVARCHAR(255)";
            case "CHAR":
            case "NCHAR":
                return col.getStrLength() != null ? "NCHAR(" + col.getStrLength() + ")" : "NCHAR(1)";
            case "TEXT":
            case "CLOB":
            case "NCLOB":
            case "LONG":
                return "NVARCHAR(MAX)";
            case "BLOB":
            case "BYTEA":
            case "RAW":
            case "LONG RAW":
            case "VARBINARY":
                return "VARBINARY(MAX)";
            case "INT":
            case "INTEGER":
            case "INT4":
                return "INT";
            case "BIGINT":
            case "INT8":
            case "NUMERIC(20)":
                return "BIGINT";
            case "SMALLINT":
            case "INT2":
                return "SMALLINT";
            case "TINYINT":
                return "TINYINT";
            case "NUMBER":
            case "NUMERIC":
            case "DECIMAL":
                if (col.getPrecision() != null && col.getScale() != null) {
                    return "DECIMAL(" + col.getPrecision() + "," + col.getScale() + ")";
                }
                if (col.getPrecision() != null) {
                    return "DECIMAL(" + col.getPrecision() + ",0)";
                }
                return "DECIMAL(18,6)";
            case "FLOAT":
            case "FLOAT8":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "REAL":
                return "FLOAT";
            case "BOOLEAN":
            case "BOOL":
            case "BIT":
                return "BIT";
            case "DATE":
                return "DATE";
            case "TIMESTAMP":
            case "DATETIME":
            case "DATETIME2":
                return "DATETIME2";
            case "TIME":
                return "TIME";
            default:
                return "NVARCHAR(255)";
        }
    }

    @Override
    protected List<String> getModifiedColumnDDL(Table sourceTable, Table targetTable) {
        List<String> ddls = new LinkedList<>();
        if (sourceTable == null || targetTable == null) return ddls;
        for (Column srcCol : sourceTable.getColumns()) {
            if (targetTable.getColumnsMap().get(srcCol.getColumnName()) == null) {
                ddls.add("ALTER TABLE [" + sourceTable.getTableName() + "] ADD " +
                         getColumnDDL(srcCol) + ";");
            }
        }
        return ddls;
    }
}
