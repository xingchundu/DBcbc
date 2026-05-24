package database.ddl.transfer.object;

/**
 * 单个数据库对象迁移的结果记录
 */
public class ObjectMigrateItem {

    public enum Status { SUCCESS, WARN, FAILED, SKIPPED }

    private final DbObjectType type;
    private final String name;
    private final Status status;
    private final String message;

    private ObjectMigrateItem(DbObjectType type, String name, Status status, String message) {
        this.type    = type;
        this.name    = name;
        this.status  = status;
        this.message = message;
    }

    public static ObjectMigrateItem success(DbObjectType type, String name) {
        return new ObjectMigrateItem(type, name, Status.SUCCESS, null);
    }

    public static ObjectMigrateItem warn(DbObjectType type, String name, String msg) {
        return new ObjectMigrateItem(type, name, Status.WARN, msg);
    }

    public static ObjectMigrateItem failed(DbObjectType type, String name, String msg) {
        return new ObjectMigrateItem(type, name, Status.FAILED, msg);
    }

    public static ObjectMigrateItem skipped(DbObjectType type, String name, String msg) {
        return new ObjectMigrateItem(type, name, Status.SKIPPED, msg);
    }

    public DbObjectType getType()   { return type;    }
    public String       getName()   { return name;    }
    public Status       getStatus() { return status;  }
    public String       getMessage(){ return message; }
}
