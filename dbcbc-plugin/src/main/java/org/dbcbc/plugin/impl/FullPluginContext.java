package org.dbcbc.plugin.impl;

import org.dbcbc.sdk.enums.ModelEnum;
import org.dbcbc.sdk.plugin.AbstractPluginContext;

/**
 * @author AE86
 * @version 1.0.0s
 * @date 2022/6/30 16:04
 */
public final class FullPluginContext extends AbstractPluginContext {

    @Override
    public ModelEnum getModelEnum() {
        return ModelEnum.FULL;
    }
}
