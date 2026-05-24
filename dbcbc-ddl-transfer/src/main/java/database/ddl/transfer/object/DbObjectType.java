package database.ddl.transfer.object;

/**
 * 可迁移的数据库对象类型枚举
 */
public enum DbObjectType {

    PROCEDURE("存储过程"),
    FUNCTION("函数"),
    TRIGGER("触发器"),
    PACKAGE("包"),
    VIEW("视图"),
    SYNONYM("同义词"),
    SEQUENCE("序列"),
    JOB("Job/调度任务"),
    ROLE_GRANT("权限/角色");

    private final String displayName;

    DbObjectType(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    /**
     * 根据名称解析，忽略大小写，不匹配返回 null
     */
    public static DbObjectType fromName(String name) {
        if (name == null) return null;
        for (DbObjectType t : values()) {
            if (t.name().equalsIgnoreCase(name.trim())) {
                return t;
            }
        }
        return null;
    }
}
