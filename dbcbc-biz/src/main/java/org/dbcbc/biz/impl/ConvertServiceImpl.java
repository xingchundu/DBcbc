/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.biz.impl;

import org.dbcbc.biz.ConvertService;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.enums.ConvertEnum;

import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/14 0:03
 */
@Component
public class ConvertServiceImpl implements ConvertService {

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public List<ConvertEnum> getConvertEnumAll() {
        return profileComponent.getConvertEnumAll();
    }
}
