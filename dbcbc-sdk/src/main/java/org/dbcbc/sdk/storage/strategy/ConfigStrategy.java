/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.sdk.storage.strategy;

import org.dbcbc.sdk.enums.StorageEnum;
import org.dbcbc.sdk.storage.Strategy;

/**
 * 配置：Connector、Mapping、TableGroup、Meta、ProjectGroup、SystemConfig、UserConfig
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-16 23:22
 */
public final class ConfigStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.CONFIG.getType();
    }
}
