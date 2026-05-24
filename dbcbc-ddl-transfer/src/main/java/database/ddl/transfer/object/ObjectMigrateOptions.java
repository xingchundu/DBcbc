package database.ddl.transfer.object;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * 对象迁移选项：记录用户选择要迁移哪些对象类型
 */
public class ObjectMigrateOptions {

    private final Set<DbObjectType> types;

    private ObjectMigrateOptions(Set<DbObjectType> types) {
        this.types = Collections.unmodifiableSet(types);
    }

    /** 不迁移任何对象 */
    public static ObjectMigrateOptions none() {
        return new ObjectMigrateOptions(EnumSet.noneOf(DbObjectType.class));
    }

    /** 迁移所有支持的对象类型 */
    public static ObjectMigrateOptions all() {
        return new ObjectMigrateOptions(EnumSet.allOf(DbObjectType.class));
    }

    /**
     * 从字符串列表构造（前端复选框传入，逗号分隔或列表）
     * 每项为 DbObjectType.name()，忽略无效项。
     */
    public static ObjectMigrateOptions of(List<String> typeNames) {
        if (typeNames == null || typeNames.isEmpty()) {
            return none();
        }
        Set<DbObjectType> set = EnumSet.noneOf(DbObjectType.class);
        for (String s : typeNames) {
            DbObjectType t = DbObjectType.fromName(s);
            if (t != null) set.add(t);
        }
        return new ObjectMigrateOptions(set);
    }

    public boolean isEnabled(DbObjectType type) {
        return types.contains(type);
    }

    public boolean hasAny() {
        return !types.isEmpty();
    }

    public Set<DbObjectType> getTypes() {
        return types;
    }
}
