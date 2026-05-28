/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.biz;

import org.dbcbc.biz.vo.MappingVO;
import org.dbcbc.biz.vo.VerifyProgressVO;
import org.dbcbc.common.model.Paging;

import java.util.Map;

/**
 * 数据验证服务接口
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-05-26
 */
public interface DataVerifyService {

    /**
     * 搜索增量同步驱动列表
     *
     * @param params
     * @return
     */
    Paging<MappingVO> searchIncrementalMappings(Map<String, String> params);

    /**
     * 启动数据验证任务
     *
     * @param mappingId  驱动ID
     * @param verifyType 验证类型: count / sampling / checksum
     * @return
     */
    String startVerify(String mappingId, String verifyType);

    /**
     * 停止数据验证任务
     *
     * @param mappingId
     * @return
     */
    String stopVerify(String mappingId);

    /**
     * 获取验证进度
     *
     * @param mappingId
     * @return
     */
    VerifyProgressVO getProgress(String mappingId);

    /**
     * 查询验证历史记录
     *
     * @param params
     * @return
     */
    Paging<Map> getHistory(Map<String, String> params);

    /**
     * 删除验证历史记录
     *
     * @param id
     * @return
     */
    String deleteHistory(String id);
}
