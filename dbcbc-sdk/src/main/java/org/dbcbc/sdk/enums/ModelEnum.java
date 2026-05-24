/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.sdk.enums;

import org.dbcbc.common.util.StringUtil;
import org.dbcbc.sdk.SdkException;

/**
 * 驱动同步方式枚举
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-11-16 00:51
 */
public enum ModelEnum {

    /**
     * 全量
     */
    FULL("full", "全量"),
    /**
     * 增量
     */
    INCREMENT("increment", "增量");

    private final String code;
    private final String name;

    ModelEnum(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public static ModelEnum getModelEnum(String code) throws SdkException {
        for (ModelEnum e : ModelEnum.values()) {
            if (StringUtil.equals(code, e.getCode())) {
                return e;
            }
        }
        throw new SdkException(String.format("Model code \"%s\" does not exist.", code));
    }

    public static boolean isFull(String model) {
        return StringUtil.equals(FULL.getCode(), model);
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
