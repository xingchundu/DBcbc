package database.ddl.transfer.object;

/**
 * 从源库提取的单个数据库对象（DDL + 元信息）
 */
public class DbObject {

    private final DbObjectType type;
    private final String name;
    /** 原始 DDL 文本 */
    private final String ddl;
    /** 附加信息（如 PACKAGE BODY 等） */
    private String extra;

    public DbObject(DbObjectType type, String name, String ddl) {
        this.type = type;
        this.name = name;
        this.ddl = ddl;
    }

    public DbObjectType getType()  { return type; }
    public String getName()        { return name; }
    public String getDdl()         { return ddl;  }
    public String getExtra()       { return extra; }
    public void setExtra(String e) { this.extra = e; }
}
