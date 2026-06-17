/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.biz.impl;

import org.dbcbc.biz.BizException;
import org.dbcbc.biz.MappingService;
import org.dbcbc.biz.MonitorService;
import org.dbcbc.biz.RepeatedTableGroupException;
import org.dbcbc.biz.TableGroupService;
import org.dbcbc.biz.checker.impl.mapping.MappingChecker;
import org.dbcbc.biz.task.MappingCountTask;
import org.dbcbc.biz.vo.LogVO;
import org.dbcbc.biz.vo.MappingCustomTableVO;
import org.dbcbc.biz.vo.MappingVO;
import org.dbcbc.biz.vo.MetaVO;
import org.dbcbc.biz.vo.TableVO;
import org.dbcbc.common.dispatch.DispatchTaskService;
import org.dbcbc.common.model.Paging;
import org.dbcbc.common.util.NumberUtil;
import org.dbcbc.common.rsa.RsaManager;
import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.JsonUtil;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.connector.base.ConnectorFactory;
import org.dbcbc.manager.ManagerFactory;
import org.dbcbc.manager.impl.PreloadTemplate;
import org.dbcbc.parser.LogService;
import org.dbcbc.parser.LogType;
import org.dbcbc.parser.ParserComponent;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.enums.MetaEnum;
import org.dbcbc.parser.TableGroupContext;
import org.dbcbc.parser.model.ConfigModel;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.Meta;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.util.ConnectorInstanceUtil;
import org.dbcbc.parser.util.ConnectorServiceContextUtil;
import org.dbcbc.sdk.SdkException;
import org.dbcbc.sdk.connector.ConfigValidator;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.connector.DefaultConnectorServiceContext;
import org.dbcbc.sdk.constant.ConfigConstant;
import org.dbcbc.sdk.enums.ModelEnum;
import org.dbcbc.sdk.enums.StorageEnum;
import org.dbcbc.sdk.enums.TableTypeEnum;
import org.dbcbc.sdk.filter.Query;
import org.dbcbc.sdk.storage.StorageService;
import org.dbcbc.sdk.model.ConnectorConfig;
import org.dbcbc.sdk.model.MetaInfo;
import org.dbcbc.sdk.model.Table;
import org.dbcbc.sdk.spi.ConnectorService;
import org.dbcbc.storage.impl.SnowflakeIdWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class MappingServiceImpl extends BaseServiceImpl implements MappingService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private MappingChecker mappingChecker;

    @Resource
    private TableGroupService tableGroupService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MonitorService monitorService;

    @Resource
    private DispatchTaskService dispatchTaskService;

    @Resource
    private ManagerFactory managerFactory;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private TableGroupContext tableGroupContext;

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private PreloadTemplate preloadTemplate;

    @Resource
    private RsaManager rsaManager;

    @Resource
    private LogService logService;

    @Resource
    private StorageService storageService;

    private static final int SYNC_ERROR_LOG_SCAN_SIZE = 2000;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = mappingChecker.checkAddConfigModel(params);
        log(LogType.MappingLog.INSERT, model);

        String id = profileComponent.addConfigModel(model);
        // 加载驱动表
        refreshMappingTables(id);

        // 匹配相似表 on
        if (StringUtil.isNotBlank(params.get("autoMatchTable"))) {
            matchSimilarTableGroups(model);
            submitMappingCountTask((Mapping) model, null);
            return id;
        }

        // 自定义表映射关系
        String tableGroups = params.get("tableGroups");
        if (StringUtil.isNotBlank(tableGroups)) {
            matchCustomizedTableGroups(model, tableGroups);
        }
        // 统计总数
        submitMappingCountTask((Mapping) model, null);
        return id;
    }

    @Override
    public String copy(String id) {
        Mapping mapping = profileComponent.getMapping(id);
        Assert.notNull(mapping, "The mapping id is invalid.");

        String json = JsonUtil.objToJson(mapping);
        Mapping newMapping = JsonUtil.jsonToObj(json, Mapping.class);
        newMapping.setName(mapping.getName() + "(复制)");
        newMapping.setId(String.valueOf(snowflakeIdWorker.nextId()));
        newMapping.setUpdateTime(Instant.now().toEpochMilli());
        mappingChecker.addMeta(newMapping);

        profileComponent.addConfigModel(newMapping);
        preloadTemplate.reConnect(newMapping);
        log(LogType.MappingLog.COPY, newMapping);

        // 复制映射表关系
        List<TableGroup> groupList = profileComponent.getTableGroupAll(mapping.getId());
        if (!CollectionUtils.isEmpty(groupList)) {
            groupList.forEach(tableGroup-> {
                String tableGroupJson = JsonUtil.objToJson(tableGroup);
                TableGroup newTableGroup = JsonUtil.jsonToObj(tableGroupJson, TableGroup.class);
                newTableGroup.setId(String.valueOf(snowflakeIdWorker.nextId()));
                newTableGroup.setMappingId(newMapping.getId());
                profileComponent.addTableGroup(newTableGroup);
                log(LogType.TableGroupLog.COPY, newTableGroup);
            });
        }
        return newMapping.getId();
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        String metaSnapshot = params.get("metaSnapshot");
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            Mapping model = (Mapping) mappingChecker.checkEditConfigModel(params);
            log(LogType.MappingLog.UPDATE, model);

            // 更新meta
            tableGroupService.updateMeta(mapping, metaSnapshot);
            profileComponent.editConfigModel(model);
        }
        // 统计总数
        submitMappingCountTask(mapping, metaSnapshot);
        return id;
    }

    @Override
    public String remove(String id) {
        Mapping mapping = assertMappingExist(id);
        String metaId = mapping.getMetaId();
        Meta meta = profileComponent.getMeta(metaId);
        synchronized (LOCK) {
            assertRunning(metaId);

            // 删除数据
            monitorService.clearData(metaId);
            log(LogType.MetaLog.CLEAR, meta);

            // 删除meta
            profileComponent.removeConfigModel(metaId);
            log(LogType.MetaLog.DELETE, meta);

            // 删除tableGroup
            List<TableGroup> groupList = profileComponent.getTableGroupAll(id);
            if (!CollectionUtils.isEmpty(groupList)) {
                groupList.forEach(t->profileComponent.removeTableGroup(t.getId()));
            }

            // 删除驱动表映射关系
            tableGroupContext.clear(metaId);

            // 释放连接池
            String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getSourceConnectorId(), ConnectorInstanceUtil.SOURCE_SUFFIX);
            String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), mapping.getTargetConnectorId(), ConnectorInstanceUtil.TARGET_SUFFIX);
            connectorFactory.disconnect(sourceInstanceId);
            connectorFactory.disconnect(targetInstanceId);

            // 删除驱动
            profileComponent.removeConfigModel(id);
            log(LogType.MappingLog.DELETE, mapping);
        }
        return "驱动删除成功";
    }

    @Override
    public MappingVO getMapping(String id) {
        Mapping mapping = profileComponent.getMapping(id);
        return convertMapping2Vo(mapping);
    }

    @Override
    public MappingCustomTableVO getMappingCustomTable(String id, String type) {
        Mapping mapping = profileComponent.getMapping(id);
        MappingCustomTableVO vo = new MappingCustomTableVO();
        vo.setId(mapping.getId());
        vo.setName(mapping.getName());
        boolean isSource = StringUtil.equals("source", type);
        List<Table> tables = isSource ? mapping.getSourceTable() : mapping.getTargetTable();
        String connectorId = isSource ? mapping.getSourceConnectorId() : mapping.getTargetConnectorId();
        ConnectorConfig config = profileComponent.getConnector(connectorId).getConfig();
        ConnectorService<?, ?> connectorService = connectorFactory.getConnectorService(config);
        vo.setConnectorConfig(config);
        vo.setExtendedType(connectorService.getExtendedTableType().getCode());

        // 只返回自定义表类型
        if (!CollectionUtils.isEmpty(tables)) {
            List<Table> mainTables = new ArrayList<>();
            List<TableVO> customTables = new ArrayList<>();
            tables.forEach(t-> {
                switch (TableTypeEnum.getTableType(t.getType())) {
                    case SQL:
                    case SEMI:
                        TableVO tableVO = new TableVO();
                        BeanUtils.copyProperties(t, tableVO);
                        customTables.add(tableVO);
                        break;
                    case TABLE:
                        mainTables.add(t);
                    default:
                        break;
                }
            });
            vo.setCustomTables(customTables.stream().sorted(Comparator.comparing(Table::getName)).collect(Collectors.toList()));
            vo.setMainTables(mainTables.stream().sorted(Comparator.comparing(Table::getName)).collect(Collectors.toList()));
        }
        // 元信息
        vo.setMeta(profileComponent.getMeta(mapping.getMetaId()));
        return vo;
    }

    @Override
    public MappingVO getMapping(String id, Integer exclude) {
        Mapping mapping = profileComponent.getMapping(id);
        // 显示所有表
        if (exclude != null && exclude == 1) {
            return convertMapping2Vo(mapping);
        }
        // 过滤已映射的表
        MappingVO vo = convertMapping2Vo(mapping);
        List<TableGroup> tableGroupAll = tableGroupService.getTableGroupAll(id);
        if (!CollectionUtils.isEmpty(tableGroupAll)) {
            final Set<String> sTables = new HashSet<>();
            final Set<String> tTables = new HashSet<>();
            tableGroupAll.forEach(tableGroup-> {
                sTables.add(tableGroup.getSourceTable().getName());
                tTables.add(tableGroup.getTargetTable().getName());
            });
            vo.setSourceTable(mapping.getSourceTable().stream().filter(t->!CollectionUtils.isEmpty(sTables) && !sTables.contains(t.getName())).collect(Collectors.toList()));
            vo.setTargetTable(mapping.getTargetTable().stream().filter(t->!CollectionUtils.isEmpty(tTables) && !tTables.contains(t.getName())).collect(Collectors.toList()));
            sTables.clear();
            tTables.clear();
        }
        return vo;
    }

    @Override
    public List<MappingVO> getMappingAll() {
        return profileComponent.getMappingAll().stream().map(this::convertMapping2Vo).sorted(Comparator.comparing(MappingVO::getUpdateTime).reversed()).collect(Collectors.toList());
    }

    @Override
    public Paging<MappingVO> search(Map<String, String> params) {
        List<MappingVO> list = getMappingAll();
        String searchState = params.get("searchState");
        if (StringUtil.isNotBlank(searchState)) {
            list = list.stream().filter(m -> matchSearchState(m, searchState)).collect(Collectors.toList());
        }
        return searchConfigModel(params, list);
    }

    private boolean matchSearchState(MappingVO mapping, String searchState) {
        int state = mapping.getMeta() != null ? mapping.getMeta().getState() : MetaEnum.READY.getCode();
        if ("1".equals(searchState)) {
            return MetaEnum.isRunning(state);
        }
        if ("0".equals(searchState)) {
            return !MetaEnum.isRunning(state);
        }
        return true;
    }

    @Override
    public String start(String id) {
        Mapping mapping = assertMappingExist(id);
        final String metaId = mapping.getMetaId();
        // 如果已经完成了，重置状态
        clearMetaIfFinished(metaId);

        synchronized (LOCK) {
            assertRunning(metaId);

            // 建立/刷新源与目标连接实例（mappingId@connectorId@S/T），避免启动时连接池无实例导致立即退出
            preloadTemplate.reConnect(mapping);

            try {
                managerFactory.start(mapping);
            } catch (Exception e) {
                logSyncStartError(mapping, e);
                if (e instanceof BizException) {
                    throw (BizException) e;
                }
                throw new BizException(e.getMessage() != null ? e.getMessage() : "启动驱动失败");
            }

            log(LogType.MappingLog.RUNNING, mapping);
        }
        return "驱动启动成功";
    }

    @Override
    public Paging<LogVO> querySyncErrors(Map<String, String> params) {
        String id = params.get("id");
        Mapping mapping = assertMappingExist(id);
        int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
        int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);

        Set<String> sourceTables = profileComponent.getSortedTableGroupAll(id).stream()
                .map(tg -> tg.getSourceTable().getName())
                .filter(StringUtil::isNotBlank)
                .collect(Collectors.toSet());

        List<Map> rows = querySyncErrorLogRows();

        List<LogVO> matched = rows.stream()
                .filter(row -> matchSyncErrorLog(row, mapping, sourceTables))
                .map(this::toLogVO)
                .sorted(Comparator.comparing(LogVO::getCreateTime).reversed())
                .collect(Collectors.toList());

        Paging<LogVO> paging = new Paging<>(pageNum, pageSize);
        paging.setTotal(matched.size());
        int offset = Math.max(0, (pageNum - 1) * pageSize);
        paging.setData(matched.stream().skip(offset).limit(pageSize).collect(Collectors.toList()));
        return paging;
    }

    private void logSyncStartError(Mapping mapping, Exception e) {
        LogType logType = ModelEnum.INCREMENT.getCode().equals(mapping.getModel())
                ? LogType.TableGroupLog.INCREMENT_FAILED : LogType.TableGroupLog.FULL_FAILED;
        String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        logService.log(logType, "启动驱动失败：[%s][id=%s], %s", mapping.getName(), mapping.getId(), message);
        logger.warn("启动驱动失败：{}, {}", mapping.getName(), message, e);
    }

    /**
     * 按同步异常类型拉取日志后在内存中匹配驱动，避免 Lucene TermQuery 无法按 json 子串检索。
     */
    private List<Map> querySyncErrorLogRows() {
        Map<String, Map> rowsById = new LinkedHashMap<>();
        appendLogRows(rowsById, LogType.TableGroupLog.INCREMENT_FAILED.getType());
        appendLogRows(rowsById, LogType.TableGroupLog.FULL_FAILED.getType());
        appendLogRows(rowsById, LogType.TableGroupLog.INCREMENT_STATS.getType());
        appendLogRows(rowsById, LogType.TableGroupLog.FULL_STATS.getType());
        return new ArrayList<>(rowsById.values());
    }

    private void appendLogRows(Map<String, Map> rowsById, String logType) {
        Query query = new Query(1, SYNC_ERROR_LOG_SCAN_SIZE);
        query.setType(StorageEnum.LOG);
        query.addFilter(ConfigConstant.CONFIG_MODEL_TYPE, logType);
        Paging raw = storageService.query(query);
        List<Map> rows = raw.getData() == null ? Collections.emptyList() : (List<Map>) raw.getData();
        for (Map row : rows) {
            if (row == null) {
                continue;
            }
            String id = String.valueOf(row.get(ConfigConstant.CONFIG_MODEL_ID));
            rowsById.putIfAbsent(id, row);
        }
    }

    private boolean matchSyncErrorLog(Map row, Mapping mapping, Set<String> sourceTables) {
        if (row == null) {
            return false;
        }
        String type = String.valueOf(row.get(ConfigConstant.CONFIG_MODEL_TYPE));
        String json = String.valueOf(row.get(ConfigConstant.CONFIG_MODEL_JSON));
        if (StringUtil.isBlank(json) || "null".equals(json)) {
            return false;
        }
        boolean syncType = LogType.TableGroupLog.INCREMENT_FAILED.getType().equals(type)
                || LogType.TableGroupLog.FULL_FAILED.getType().equals(type)
                || LogType.TableGroupLog.INCREMENT_STATS.getType().equals(type)
                || LogType.TableGroupLog.FULL_STATS.getType().equals(type);
        if (!syncType && !json.contains("同步失败") && !json.contains("同步异常") && !json.contains("启动驱动失败")) {
            return false;
        }
        String name = mapping.getName();
        String mappingId = mapping.getId();
        if (StringUtil.isNotBlank(mappingId) && json.contains("[id=" + mappingId + "]")) {
            return true;
        }
        if (json.contains("[" + name + "]")) {
            return true;
        }
        if (json.contains(":" + name + "(")) {
            return true;
        }
        if (json.contains("启动驱动失败") && json.contains(name)) {
            return true;
        }
        for (String table : sourceTables) {
            if (json.contains("表" + table)) {
                return true;
            }
            if (json.startsWith(table + ":")) {
                return true;
            }
        }
        return syncType && json.contains(name);
    }

    private LogVO toLogVO(Map row) {
        return JsonUtil.jsonToObj(JsonUtil.objToJson(row), LogVO.class);
    }

    @Override
    public String stop(String id) {
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            if (!isRunning(mapping.getMetaId())) {
                throw new BizException("驱动已停止.");
            }
            managerFactory.close(mapping);

            log(LogType.MappingLog.STOP, mapping);

            // 发送关闭驱动通知消息
            String model = ModelEnum.getModelEnum(mapping.getModel()).getName();
            sendNotifyMessage("停止驱动", String.format("手动停止驱动：%s(%s)", mapping.getName(), model));
        }
        return "驱动停止成功";
    }

    @Override
    public String batchStart(String ids) {
        Assert.hasText(ids, "驱动ID不能为空");
        String[] idArray = StringUtil.split(ids, ",");
        int success = 0;
        int fail = 0;
        for (String id : idArray) {
            try {
                start(id);
                success++;
            } catch (Exception e) {
                fail++;
                logger.warn("批量启动驱动失败：{}, {}", id, e.getMessage());
            }
        }
        return String.format("批量启动完成：成功%d，失败%d", success, fail);
    }

    @Override
    public String batchStop(String ids) {
        Assert.hasText(ids, "驱动ID不能为空");
        String[] idArray = StringUtil.split(ids, ",");
        int success = 0;
        int fail = 0;
        for (String id : idArray) {
            try {
                stop(id);
                success++;
            } catch (Exception e) {
                fail++;
                logger.warn("批量停止驱动失败：{}, {}", id, e.getMessage());
            }
        }
        return String.format("批量停止完成：成功%d，失败%d", success, fail);
    }

    @Override
    public String refreshMappingTables(String id) {
        Mapping mapping = profileComponent.getMapping(id);
        Assert.notNull(mapping, "The mapping id is invalid.");
        preloadTemplate.reConnect(mapping);
        mapping.setSourceTable(updateConnectorTables(mapping, ConnectorInstanceUtil.SOURCE_SUFFIX));
        mapping.setTargetTable(updateConnectorTables(mapping, ConnectorInstanceUtil.TARGET_SUFFIX));
        profileComponent.editConfigModel(mapping);
        return "刷新驱动表成功";
    }

    @Override
    public List<Table> searchCustomTable(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String searchKey = params.get("searchKey");
        boolean isSource = StringUtil.equals("source", type);
        List<Table> tables = getMappingTables(mapping, isSource);
        if (!CollectionUtils.isEmpty(tables) && StringUtil.isNotBlank(searchKey)) {
            return tables.stream().filter(t -> t.getName().contains(searchKey)).collect(Collectors.toList());
        }
        return tables;
    }

    @Override
    public String saveCustomTable(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            saveCustomTable(mapping, params);
            profileComponent.editConfigModel(mapping);
            log(LogType.MappingLog.UPDATE, mapping);
        }
        return id;
    }

    @Override
    public String removeCustomTable(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Mapping mapping = assertMappingExist(id);
        synchronized (LOCK) {
            assertRunning(mapping.getMetaId());
            removeCustomTable(mapping, params);
            profileComponent.editConfigModel(mapping);
            log(LogType.MappingLog.UPDATE, mapping);
        }
        return id;
    }

    /**
     * 提交统计驱动总数任务
     */
    private void submitMappingCountTask(Mapping mapping, String metaSnapshot) {
        MappingCountTask task = new MappingCountTask();
        task.setMappingId(mapping.getId());
        task.setMetaSnapshot(metaSnapshot);
        task.setParserComponent(parserComponent);
        task.setProfileComponent(profileComponent);
        task.setTableGroupService(tableGroupService);
        task.setConnectorFactory(connectorFactory);
        task.setRsaManager(rsaManager);
        dispatchTaskService.execute(task);
    }

    private List<Table> updateConnectorTables(Mapping mapping, String suffix) {
        boolean isSource = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(mapping, isSource);

        // 合并自定义表
        List<Table> customTables = new ArrayList<>();
        List<Table> tables = getMappingTables(mapping, isSource);
        tables.forEach(t-> {
            switch (TableTypeEnum.getTableType(t.getType())) {
                case SQL:
                case SEMI:
                    customTables.add(t);
                    break;
                default:
                    break;
            }
        });

        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(context.getMappingId(), context.getConnectorId(), context.getSuffix());
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        tables = connectorFactory.getTables(connectorInstance, context);
        tables.addAll(customTables);
        // 按升序展示表
        Collections.sort(tables, Comparator.comparing(Table::getName));
        return tables;
    }

    private MappingVO convertMapping2Vo(Mapping mapping) {
        Assert.notNull(mapping, "Mapping can not be null.");
        String model = mapping.getModel();

        // 元信息
        Meta meta = profileComponent.getMeta(mapping.getMetaId());
        Assert.notNull(meta, "Meta can not be null.");
        MetaVO metaVo = new MetaVO(ModelEnum.getModelEnum(model).getName(), mapping.getName());
        BeanUtils.copyProperties(meta, metaVo);
        metaVo.setCounting(dispatchTaskService.isRunning(mapping.getId()));

        Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
        Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
        MappingVO vo = new MappingVO(s, t, metaVo);
        BeanUtils.copyProperties(mapping, vo);
        return vo;
    }

    /**
     * 检查是否存在驱动
     *
     * @param mappingId
     * @return
     */
    private Mapping assertMappingExist(String mappingId) {
        Mapping mapping = profileComponent.getMapping(mappingId);
        Assert.notNull(mapping, "驱动不存在.");
        return mapping;
    }

    /**
     * 匹配相似表
     *
     * @param model
     */
    private void matchSimilarTableGroups(ConfigModel model) {
        Mapping mapping = (Mapping) model;
        List<Table> sourceTables = mapping.getSourceTable();
        List<Table> targetTables = mapping.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            return;
        }
        // 优化匹配性能
        Map<String, Table> targetTableMap = targetTables.stream().collect(Collectors.toMap(table->table.getName().toUpperCase(), table->table));

        // 匹配相似表
        for (Table sourceTable : sourceTables) {
            if (StringUtil.isBlank(sourceTable.getName())) {
                continue;
            }
            targetTableMap.computeIfPresent(sourceTable.getName().toUpperCase(), (k, targetTable)-> {
                // 仅支持表类型
                if (TableTypeEnum.isTable(targetTable.getType())) {
                    addTableGroup(mapping.getId(), sourceTable.getName(), targetTable.getName(), StringUtil.EMPTY);
                }
                return targetTable;
            });
        }
    }

    /**
     * 自定义配置表映射关系
     *
     * @param model
     * @param tableGroups
     */
    private void matchCustomizedTableGroups(ConfigModel model, String tableGroups) {
        Mapping mapping = (Mapping) model;
        List<Table> sourceTables = mapping.getSourceTable();
        List<Table> targetTables = mapping.getTargetTable();
        if (CollectionUtils.isEmpty(sourceTables) || CollectionUtils.isEmpty(targetTables)) {
            return;
        }
        String[] lines = StringUtil.split(tableGroups, StringUtil.BREAK_LINE);
        // 数据源表|目标源表=源表字段A1*|目标字段A2*
        for (String line : lines) {
            String[] tableGroup = StringUtil.split(line, StringUtil.EQUAL);
            String[] tableGroupNames = StringUtil.split(tableGroup[0], StringUtil.VERTICAL_LINE);
            if (tableGroupNames.length == 2) {
                addTableGroup(mapping.getId(), tableGroupNames[0], tableGroupNames[1], tableGroup.length == 2 ? tableGroup[1] : StringUtil.EMPTY);
            }
        }
    }

    private void addTableGroup(String id, String sourceTableName, String targetTableName, String fieldMappings) {
        try {
            Map<String, String> params = new HashMap<>();
            params.put("mappingId", id);
            params.put("sourceTable", sourceTableName);
            params.put("targetTable", targetTableName);
            params.put("sourceType", TableTypeEnum.TABLE.getCode());
            params.put("targetType", TableTypeEnum.TABLE.getCode());
            // A1*|A2*,B1|B2,|C3
            if (StringUtil.isNotBlank(fieldMappings)) {
                String[] mappings = StringUtil.split(fieldMappings, StringUtil.COMMA);
                StringJoiner fms = new StringJoiner(StringUtil.COMMA);
                StringJoiner sPk = new StringJoiner(StringUtil.COMMA);
                StringJoiner tPk = new StringJoiner(StringUtil.COMMA);
                for (String mapping : mappings) {
                    String[] m = StringUtil.split(mapping, "\\" + StringUtil.VERTICAL_LINE);
                    if (m.length == 2) {
                        fms.add(replaceStar(m[0], sPk) + StringUtil.VERTICAL_LINE + replaceStar(m[1], tPk));
                        continue;
                    }
                    // |C2,C3|
                    if (m.length == 1) {
                        String name = replaceStar(m[0], tPk);
                        if (StringUtil.startsWith(mapping, StringUtil.VERTICAL_LINE)) {
                            fms.add(StringUtil.VERTICAL_LINE + name);
                            continue;
                        }
                        fms.add(name + StringUtil.VERTICAL_LINE);
                    }
                }
                params.put("fieldMappings", fms.toString());
                if (StringUtil.isNotBlank(sPk.toString())) {
                    params.put("sourceTablePK", sPk.toString());
                }
                if (StringUtil.isNotBlank(tPk.toString())) {
                    params.put("targetTablePK", tPk.toString());
                }
            }
            tableGroupService.add(params);
        } catch (RepeatedTableGroupException | SdkException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private String replaceStar(String name, StringJoiner joiner) {
        if (StringUtil.endsWith(name, StringUtil.STAR)) {
            name = StringUtil.replace(name.trim(), StringUtil.STAR, StringUtil.EMPTY);
            joiner.add(name);
        }
        return name;
    }

    private void clearMetaIfFinished(String metaId) {
        Meta meta = profileComponent.getMeta(metaId);
        Assert.notNull(meta, "Mapping meta can not be null.");
        // 完成任务则重置状态
        if (meta.getTotal().get() <= (meta.getSuccess().get() + meta.getFail().get())) {
            meta.getFail().set(0);
            meta.getSuccess().set(0);
            profileComponent.editConfigModel(meta);
        }
    }

    private void saveCustomTable(Mapping mapping, Map<String, String> params) {
        String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String operator = params.get("operator");
        String customTable = params.get("customTable");
        boolean isSource = StringUtil.equals("source", type);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(mapping, isSource);

        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(context.getMappingId(), context.getConnectorId(), context.getSuffix());
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        ConnectorService connectorService = connectorFactory.getConnectorService(connectorInstance.getConfig());
        ConfigValidator configValidator = connectorService.getConfigValidator();
        Assert.notNull(configValidator, "ConfigValidator can not be null.");
        Table newTable = configValidator.modifyExtendedTable(connectorService, params);
        Assert.notNull(newTable, "解析自定义表异常");

        context.addTablePattern(newTable);
        List<MetaInfo> metaInfos = connectorService.getMetaInfo(connectorInstance, context);
        Assert.notEmpty(metaInfos, "执行SQL异常");
        Assert.notEmpty(metaInfos.get(0).getColumn(), "获取字段信息异常");

        List<Table> tables = getMappingTables(mapping, isSource);
        // 首次添加
        if (CollectionUtils.isEmpty(tables)) {
            tables.add(newTable);
            return;
        }

        // 新增操作
        Set<String> exist = tables.stream().map(Table::getName).collect(Collectors.toSet());
        String newTableName = newTable.getName();
        if (StringUtil.equals("add", operator)) {
            if (exist.contains(newTableName)) {
                throw new BizException(String.format("%s或自定义表名重复，请更换", isSource ? "数据源" : "目标源"));
            }
            tables.add(newTable);
            // 按升序展示表
            Collections.sort(tables, Comparator.comparing(Table::getName));
            return;
        }

        // 修改操作，更改表名
        if (!StringUtil.equals(customTable, newTableName)) {
            if (exist.contains(newTableName)) {
                throw new BizException(String.format("%s或自定义表名重复，请更换", isSource ? "数据源" : "目标源"));
            }
        }
        for (Table t : tables) {
            if (StringUtil.equals(t.getName(), customTable)) {
                t.setName(newTable.getName());
                t.setColumn(newTable.getColumn());
                t.setExtInfo(newTable.getExtInfo());
                break;
            }
        }

        // 按升序展示表
        Collections.sort(tables, Comparator.comparing(Table::getName));
    }

    private List<Table> getMappingTables(Mapping mapping, boolean isSource) {
        mapping.setSourceTable(!CollectionUtils.isEmpty(mapping.getSourceTable()) ? mapping.getSourceTable() : new ArrayList<>());
        mapping.setTargetTable(!CollectionUtils.isEmpty(mapping.getTargetTable()) ? mapping.getTargetTable() : new ArrayList<>());
        return isSource ? mapping.getSourceTable() : mapping.getTargetTable();
    }

    private void removeCustomTable(Mapping mapping, Map<String, String> params) {
        String type = params.get(ConfigConstant.CONFIG_MODEL_TYPE);
        String tableName = params.get("customTable");
        Assert.hasText(tableName, "无自定义表.");
        List<Table> tables = StringUtil.equals("source", type) ? mapping.getSourceTable() : mapping.getTargetTable();
        if (!CollectionUtils.isEmpty(tables)) {
            Iterator<Table> iterator = tables.iterator();
            while (iterator.hasNext()) {
                Table t = iterator.next();
                switch (TableTypeEnum.getTableType(t.getType())) {
                    case SQL:
                    case SEMI:
                        if (StringUtil.equals(t.getName(), tableName)) {
                            iterator.remove();
                            return;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

}