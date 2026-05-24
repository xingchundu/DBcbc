/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.parser.command.impl;

import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.command.Command;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.Meta;
import org.dbcbc.parser.model.SystemConfig;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.model.UserConfig;

/**
 * 预加载接口
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
public final class PreloadCommand implements Command {

    private ProfileComponent profileComponent;

    private String json;

    public PreloadCommand(ProfileComponent profileComponent, String json) {
        this.profileComponent = profileComponent;
        this.json = json;
    }

    @Override
    public SystemConfig parseSystemConfig() {
        return profileComponent.parseObject(json, SystemConfig.class);
    }

    @Override
    public UserConfig parseUserConfig() {
        return profileComponent.parseObject(json, UserConfig.class);
    }

    @Override
    public Connector parseConnector() {
        return profileComponent.parseConnector(json);
    }

    @Override
    public Mapping parseMapping() {
        return profileComponent.parseObject(json, Mapping.class);
    }

    @Override
    public TableGroup parseTableGroup() {
        return profileComponent.parseObject(json, TableGroup.class);
    }

    @Override
    public Meta parseMeta() {
        return profileComponent.parseObject(json, Meta.class);
    }
}
