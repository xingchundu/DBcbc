/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbcbc.biz.metric;

import org.dbcbc.biz.vo.MetricResponseVO;

import java.util.List;

/**
 * 合并分组指标
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-04-15 23:13
 */
public interface MetricGroupProcessor {

    List<MetricResponseVO> process(List<MetricResponseVO> metrics);
}