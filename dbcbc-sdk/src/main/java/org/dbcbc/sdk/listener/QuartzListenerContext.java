/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.sdk.listener;

import org.dbcbc.sdk.enums.ModelEnum;
import org.dbcbc.sdk.plugin.AbstractPluginContext;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-05 01:07
 */
public final class QuartzListenerContext extends AbstractPluginContext {

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.INCREMENT;
    }
}
