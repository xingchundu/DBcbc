/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.biz.impl;

import org.dbcbc.biz.AppConfigService;
import org.dbcbc.biz.vo.VersionVO;
import org.dbcbc.common.config.AppConfig;

import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-12 01:08
 */
@Component
public class AppConfigServiceImpl implements AppConfigService {

    @Resource
    private AppConfig appConfig;

    @Override
    public VersionVO getVersionInfo(String username) {
        return new VersionVO(appConfig.getName(), appConfig.getCopyright());
    }
}
