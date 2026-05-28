/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.biz.impl;

import org.dbcbc.biz.DataVerifyService;
import org.dbcbc.biz.vo.MappingVO;
import org.dbcbc.biz.vo.MetaVO;
import org.dbcbc.biz.vo.VerifyProgressVO;
import org.dbcbc.common.dispatch.DispatchTaskService;
import org.dbcbc.common.model.Paging;
import org.dbcbc.common.rsa.RsaManager;
import org.dbcbc.common.util.NumberUtil;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.base.ConnectorFactory;
import org.dbcbc.parser.ParserComponent;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.Meta;
import org.dbcbc.sdk.constant.ConfigConstant;
import org.dbcbc.sdk.enums.ModelEnum;
import org.dbcbc.sdk.enums.StorageEnum;
import org.dbcbc.sdk.filter.Query;
import org.dbcbc.sdk.storage.StorageService;
import org.dbcbc.storage.impl.SnowflakeIdWorker;
import org.dbcbc.biz.task.DataVerifyTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 数据验证服务实现
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-05-26
 */
@Service
public class DataVerifyServiceImpl extends BaseServiceImpl implements DataVerifyService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ConcurrentHashMap<String, VerifyProgressVO> progressMap = new ConcurrentHashMap<>();

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private DispatchTaskService dispatchTaskService;

    @Resource
    private RsaManager rsaManager;

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Override
    public Paging<MappingVO> searchIncrementalMappings(java.util.Map<String, String> params) {
        List<MappingVO> list = profileComponent.getMappingAll().stream()
                .map(this::convertMapping2Vo)
                .sorted(Comparator.comparing(MappingVO::getUpdateTime).reversed())
                .collect(Collectors.toList());
        return searchConfigModel(params, list);
    }

    @Override
    public String startVerify(String mappingId, String verifyType) {
        Mapping mapping = profileComponent.getMapping(mappingId);
        Assert.notNull(mapping, "驱动不存在.");

        if (dispatchTaskService.isRunning(mappingId)) {
            throw new org.dbcbc.biz.BizException("验证任务正在运行中.");
        }

        if (StringUtil.isBlank(verifyType)) {
            verifyType = "count";
        }

        VerifyProgressVO progressVO = new VerifyProgressVO();
        progressVO.setMappingId(mappingId);
        progressVO.setMappingName(mapping.getName());
        progressVO.setVerifyType(verifyType);
        progressVO.setStatus("pending");
        progressMap.put(mappingId, progressVO);

        DataVerifyTask task = new DataVerifyTask();
        task.setMappingId(mappingId);
        task.setVerifyType(verifyType);
        task.setProfileComponent(profileComponent);
        task.setConnectorFactory(connectorFactory);
        task.setParserComponent(parserComponent);
        task.setRsaManager(rsaManager);
        task.setProgressVO(progressVO);
        task.setStorageService(storageService);
        task.setSnowflakeIdWorker(snowflakeIdWorker);
        task.onDestroy(t -> progressMap.remove(mappingId));

        dispatchTaskService.execute(task);
        logger.info("启动数据验证:{}, 类型:{}", mapping.getName(), verifyType);
        return "验证任务已启动";
    }

    @Override
    public String stopVerify(String mappingId) {
        dispatchTaskService.stop(mappingId);
        VerifyProgressVO progressVO = progressMap.get(mappingId);
        if (progressVO != null) {
            progressVO.setStatus("stopped");
        }
        return "验证任务已停止";
    }

    @Override
    public VerifyProgressVO getProgress(String mappingId) {
        return progressMap.get(mappingId);
    }

    @Override
    public Paging<Map> getHistory(Map<String, String> params) {
        String taskId = params.get("taskId");
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
        Query query = new Query(pageNum, pageSize);
        query.setType(StorageEnum.TASK_DATA_VERIFICATION_DETAIL);
        if (StringUtil.isNotBlank(taskId)) {
            query.addFilter(ConfigConstant.TASK_ID, taskId);
        }
        return storageService.query(query);
    }

    @Override
    public String deleteHistory(String id) {
        storageService.remove(StorageEnum.TASK_DATA_VERIFICATION_DETAIL, id);
        return "删除成功";
    }

    private MappingVO convertMapping2Vo(Mapping mapping) {
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "Meta can not be null.");
        MetaVO metaVo = new MetaVO(ModelEnum.getModelEnum(mapping.getModel()).getName(), mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);
        metaVo.setCounting(dispatchTaskService.isRunning(mapping.getId()));

        Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
        Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
        MappingVO vo = new MappingVO(s, t, metaVo);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
    }
}
