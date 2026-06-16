package org.dbcbc.connector.dm.logminer.parser;

import org.dbcbc.sdk.model.Field;
import org.junit.Assert;
import org.junit.Test;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public class DmDeleteSqlTest {

    @Test
    public void shouldParseDeleteWithQuotedColumns() throws Exception {
        Delete delete = (Delete) CCJSqlParserUtil.parse(
                "DELETE FROM \"MONITOR\".\"t2\" WHERE \"id\" = 61 AND \"log_date\" = DATE'2026-06-24'");
        List<Field> fields = Arrays.asList(field("id", Types.INTEGER), field("log_date", Types.DATE), field("content", Types.VARCHAR));
        List<Object> values = new DmDeleteSql(delete, fields).parseColumns();
        Assert.assertEquals(new BigDecimal("61"), values.get(0));
        Assert.assertNotNull(values.get(1));
        Assert.assertNull(values.get(2));
    }

    @Test
    public void shouldParseDeleteWithParenthesisWhere() throws Exception {
        Delete delete = (Delete) CCJSqlParserUtil.parse(
                "DELETE FROM \"MONITOR\".\"t2\" WHERE (\"id\" = 62 AND \"content\" = 'pd1')");
        List<Field> fields = Arrays.asList(field("id", Types.INTEGER), field("log_date", Types.DATE), field("content", Types.VARCHAR));
        List<Object> values = new DmDeleteSql(delete, fields).parseColumns();
        Assert.assertEquals(new BigDecimal("62"), values.get(0));
        Assert.assertEquals("pd1", values.get(2));
    }

    private Field field(String name, int type) {
        Field field = new Field();
        field.setName(name.toUpperCase());
        field.setType(type);
        return field;
    }
}
