/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.biz.impl;

import org.dbcbc.biz.ConditionService;
import org.dbcbc.biz.vo.ConditionVO;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.sdk.enums.FilterEnum;
import org.dbcbc.sdk.enums.OperationEnum;
import org.dbcbc.sdk.enums.QuartzFilterEnum;

import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import java.util.List;

/**
 * 支持的条件和运算符类型
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-19 16:02
 */
@Component
public class ConditionServiceImpl implements ConditionService {

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public ConditionVO getCondition() {
        List<OperationEnum> operationEnumAll = profileComponent.getOperationEnumAll();
        List<QuartzFilterEnum> quartzFilterEnumAll = profileComponent.getQuartzFilterEnumAll();
        List<FilterEnum> filterEnumAll = profileComponent.getFilterEnumAll();
        return new ConditionVO(operationEnumAll, quartzFilterEnumAll, filterEnumAll);
    }
}