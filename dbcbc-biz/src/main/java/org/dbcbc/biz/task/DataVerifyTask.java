/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.biz.task;

import org.dbcbc.biz.vo.CascadeTableResultVO;
import org.dbcbc.biz.vo.FieldMismatchVO;
import org.dbcbc.biz.vo.TableVerifyResultVO;
import org.dbcbc.biz.vo.VerifyProgressVO;
import org.dbcbc.common.dispatch.AbstractDispatchTask;
import org.dbcbc.common.enums.DispatchTaskEnum;
import org.dbcbc.common.model.Result;
import org.dbcbc.common.rsa.RsaManager;
import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.JsonUtil;
import org.dbcbc.connector.base.ConnectorFactory;
import org.dbcbc.parser.ParserComponent;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.model.FieldMapping;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.Picker;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.util.ConnectorInstanceUtil;
import org.dbcbc.parser.util.PickerUtil;
import org.dbcbc.plugin.impl.FullPluginContext;
import org.dbcbc.sdk.config.CommandConfig;
import org.dbcbc.sdk.constant.ConfigConstant;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.connector.DefaultMetaContext;
import org.dbcbc.sdk.constant.ConnectorConstant;
import org.dbcbc.sdk.enums.StorageEnum;
import org.dbcbc.sdk.model.ConnectorConfig;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.spi.ConnectorService;
import org.dbcbc.sdk.storage.StorageService;
import org.dbcbc.sdk.util.PrimaryKeyUtil;
import org.dbcbc.storage.impl.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据验证异步任务
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-05-26
 */
public class DataVerifyTask extends AbstractDispatchTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String mappingId;
    private String verifyType;
    private ProfileComponent profileComponent;
    private ConnectorFactory connectorFactory;
    private ParserComponent parserComponent;
    private RsaManager rsaManager;
    private VerifyProgressVO progressVO;
    private StorageService storageService;
    private SnowflakeIdWorker snowflakeIdWorker;

    @Override
    public String getUniqueId() {
        return mappingId;
    }

    @Override
    public DispatchTaskEnum getType() {
        return DispatchTaskEnum.DATA_VERIFY;
    }

    @Override
    public void execute() {
        Mapping mapping = profileComponent.getMapping(mappingId);
        if (mapping == null) {
            progressVO.setStatus("error");
            progressVO.setErrorMessage("驱动不存在");
            return;
        }

        List<TableGroup> groupAll = profileComponent.getTableGroupAll(mappingId);
        progressVO.setTotalTables(groupAll.size());
        progressVO.setStatus("running");
        progressVO.setStartTime(Instant.now().toEpochMilli());
        logger.info("开始数据验证:{}, {}张表, 类型:{}", mapping.getName(), groupAll.size(), verifyType);

        for (int i = 0; i < groupAll.size(); i++) {
            if (!isRunning()) {
                progressVO.setStatus("stopped");
                progressVO.setEndTime(Instant.now().toEpochMilli());
                return;
            }

            TableGroup tableGroup = groupAll.get(i);

            if ("cascade".equals(verifyType)) {
                CascadeTableResultVO cascadeResult = new CascadeTableResultVO();
                cascadeResult.setTableGroupId(tableGroup.getId());
                cascadeResult.setSourceTableName(tableGroup.getSourceTable().getName());
                cascadeResult.setTargetTableName(tableGroup.getTargetTable().getName());
                cascadeResult.setStatus("running");
                progressVO.getCascadeResults().add(cascadeResult);

                try {
                    TableGroup merged = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
                    Map<String, String> command = parserComponent.getCommand(mapping, merged);
                    verifyByCascade(mapping, merged, command, cascadeResult);
                    cascadeResult.setStatus("completed");
                } catch (Exception e) {
                    cascadeResult.setStatus("error");
                    logger.error("分层校验异常:{} >> {}", cascadeResult.getSourceTableName(), cascadeResult.getTargetTableName(), e);
                }
                persistResult(mappingId, verifyType, cascadeResult.getSourceTableName(), cascadeResult.getTargetTableName(), cascadeResult);
            } else {
                TableVerifyResultVO result = new TableVerifyResultVO();
                result.setTableGroupId(tableGroup.getId());
                result.setSourceTableName(tableGroup.getSourceTable().getName());
                result.setTargetTableName(tableGroup.getTargetTable().getName());
                result.setStatus("running");
                progressVO.getResults().add(result);

                try {
                    TableGroup merged = PickerUtil.mergeTableGroupConfig(mapping, tableGroup);
                    Map<String, String> command = parserComponent.getCommand(mapping, merged);

                    switch (verifyType) {
                        case "sampling":
                            verifyBySampling(mapping, merged, command, result);
                            break;
                        case "checksum":
                            verifyByChecksum(mapping, merged, command, result);
                            break;
                        default:
                            verifyByCount(mapping, merged, command, result);
                            break;
                    }
                    result.setStatus("completed");
                } catch (Exception e) {
                    result.setStatus("error");
                    result.setErrorMessage(e.getMessage());
                    logger.error("验证表异常:{} >> {}", result.getSourceTableName(), result.getTargetTableName(), e);
                }
                persistResult(mappingId, verifyType, result.getSourceTableName(), result.getTargetTableName(), result);
            }
            progressVO.setCompletedTables(i + 1);
        }

        progressVO.setStatus(isRunning() ? "completed" : "stopped");
        progressVO.setEndTime(Instant.now().toEpochMilli());
        logger.info("完成数据验证:{}, 状态:{}", mapping.getName(), progressVO.getStatus());
    }

    private void persistResult(String taskId, String verifyType, String sourceTableName, String targetTableName, Object resultVO) {
        if (storageService == null || snowflakeIdWorker == null) {
            return;
        }
        try {
            Map<String, Object> params = new HashMap<>();
            params.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
            params.put(ConfigConstant.TASK_ID, taskId);
            params.put(ConfigConstant.CONFIG_MODEL_TYPE, verifyType);
            params.put(ConfigConstant.TASK_SOURCE_TABLE_NAME, sourceTableName);
            params.put(ConfigConstant.DATA_TARGET_TABLE_NAME, targetTableName);
            params.put(ConfigConstant.TASK_CONTENT, JsonUtil.objToJson(resultVO));
            params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, Instant.now().toEpochMilli());
            params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, Instant.now().toEpochMilli());
            storageService.add(StorageEnum.TASK_DATA_VERIFICATION_DETAIL, params);
        } catch (Exception e) {
            logger.error("持久化验证结果失败:{}", sourceTableName, e);
        }
    }

    // ==================== 行数对比 ====================

    private void verifyByCount(Mapping mapping, TableGroup merged, Map<String, String> command, TableVerifyResultVO result) {
        long sourceCount = getCount(mapping, merged, command, true);
        long targetCount = getCount(mapping, merged, command, false);
        result.setSourceCount(sourceCount);
        result.setTargetCount(targetCount);
        result.setCountMatch(sourceCount == targetCount);
    }

    // ==================== 行级全量对比 ====================

    @SuppressWarnings("unchecked")
    private void verifyBySampling(Mapping mapping, TableGroup merged, Map<String, String> command, TableVerifyResultVO result) {
        long sourceCount = getCount(mapping, merged, command, true);
        long targetCount = getCount(mapping, merged, command, false);
        result.setSourceCount(sourceCount);
        result.setTargetCount(targetCount);
        result.setCountMatch(sourceCount == targetCount);

        String sourceConnectorId = mapping.getSourceConnectorId();
        String targetConnectorId = mapping.getTargetConnectorId();
        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), targetConnectorId, ConnectorInstanceUtil.TARGET_SUFFIX);

        ConnectorConfig sConfig = profileComponent.getConnector(sourceConnectorId).getConfig();
        ConnectorConfig tConfig = profileComponent.getConnector(targetConnectorId).getConfig();
        ConnectorInstance sourceInstance = connectorFactory.connect(sourceInstanceId);
        ConnectorInstance targetInstance = connectorFactory.connect(targetInstanceId);

        List<FieldMapping> fieldMapping = merged.getFieldMapping();
        List<String> sourceFieldNames = new ArrayList<>();
        List<String> targetFieldNames = new ArrayList<>();
        if (!CollectionUtils.isEmpty(fieldMapping)) {
            for (FieldMapping fm : fieldMapping) {
                if (fm.getSource() != null) sourceFieldNames.add(fm.getSource().getName());
                if (fm.getTarget() != null) targetFieldNames.add(fm.getTarget().getName());
            }
        }

        List<String> sourcePks = PrimaryKeyUtil.findTablePrimaryKeys(merged.getSourceTable());
        List<String> targetPks = PrimaryKeyUtil.findTablePrimaryKeys(merged.getTargetTable());
        String srcPkName = CollectionUtils.isEmpty(sourcePks) ? null : sourcePks.get(0);
        String tgtPkName = CollectionUtils.isEmpty(targetPks) ? null : targetPks.get(0);

        String srcFullTable = buildFullTableName(mapping.getSourceSchema(), merged.getSourceTable().getName());
        String tgtFullTable = buildFullTableName(mapping.getTargetSchema(), merged.getTargetTable().getName());

        List<String> srcTypeNames = buildTypeNames(merged.getSourceTable(), sourceFieldNames);
        List<String> tgtTypeNames = buildTypeNames(merged.getTargetTable(), targetFieldNames);

        // Fetch (pk, row_signature) from both sides — full table
        List<Map<String, Object>> srcRowSigs = fetchRowSignaturesWithPk(sourceInstance, sConfig.getConnectorType(), srcFullTable, sourceFieldNames, srcPkName, 0, Long.MAX_VALUE, srcTypeNames);
        List<Map<String, Object>> tgtRowSigs = fetchRowSignaturesWithPk(targetInstance, tConfig.getConnectorType(), tgtFullTable, targetFieldNames, tgtPkName, 0, Long.MAX_VALUE, tgtTypeNames);

        if (srcRowSigs == null) srcRowSigs = new ArrayList<>();
        if (tgtRowSigs == null) tgtRowSigs = new ArrayList<>();

        // Build target PK → signature map
        Map<String, String> tgtSigMap = new LinkedHashMap<>();
        for (Map<String, Object> row : tgtRowSigs) {
            tgtSigMap.put(String.valueOf(row.get("pk_val")), String.valueOf(row.get("row_sig")));
        }

        Set<String> srcPkSet = new HashSet<>();
        List<FieldMismatchVO> mismatches = new ArrayList<>();
        Set<String> mismatchedPkSet = new HashSet<>();

        // Compare source rows against target
        for (Map<String, Object> srcRow : srcRowSigs) {
            String srcPk = String.valueOf(srcRow.get("pk_val"));
            srcPkSet.add(srcPk);
            String srcSig = String.valueOf(srcRow.get("row_sig"));
            String tgtSig = tgtSigMap.get(srcPk);

            if (tgtSig == null) {
                FieldMismatchVO mismatch = new FieldMismatchVO();
                Map<String, Object> pkMap = new LinkedHashMap<>();
                pkMap.put(srcPkName != null ? srcPkName : "pk", srcPk);
                mismatch.setPrimaryKey(pkMap);
                mismatch.setFieldName("*");
                mismatch.setSourceValue("存在");
                mismatch.setTargetValue("不存在");
                mismatches.add(mismatch);
                mismatchedPkSet.add(srcPk);
            } else if (!srcSig.equals(tgtSig)) {
                // Field-level comparison
                Map<String, Object> srcData = fetchRowData(sourceInstance, sConfig.getConnectorType(), srcFullTable, sourceFieldNames, srcPkName, srcPk);
                Map<String, Object> tgtData = fetchRowData(targetInstance, tConfig.getConnectorType(), tgtFullTable, targetFieldNames, tgtPkName, srcPk);
                if (srcData != null && tgtData != null) {
                    List<FieldMismatchVO> fieldDiffs = compareFields(srcData, tgtData, fieldMapping, srcPkName, srcPk);
                    mismatches.addAll(fieldDiffs);
                    if (!fieldDiffs.isEmpty()) mismatchedPkSet.add(srcPk);
                }
            }
        }

        // Check for extra rows in target
        for (Map<String, Object> tgtRow : tgtRowSigs) {
            String tgtPk = String.valueOf(tgtRow.get("pk_val"));
            if (!srcPkSet.contains(tgtPk)) {
                FieldMismatchVO mismatch = new FieldMismatchVO();
                Map<String, Object> pkMap = new LinkedHashMap<>();
                pkMap.put(tgtPkName != null ? tgtPkName : "pk", tgtPk);
                mismatch.setPrimaryKey(pkMap);
                mismatch.setFieldName("*");
                mismatch.setSourceValue("不存在");
                mismatch.setTargetValue("存在");
                mismatches.add(mismatch);
                mismatchedPkSet.add(tgtPk);
            }
        }

        result.setSampledRows(srcRowSigs.size() + tgtRowSigs.size());
        result.setMismatchedRows(mismatchedPkSet.size());
        result.setMismatches(mismatches.size() > MAX_DISPLAY_ITEMS ? mismatches.subList(0, MAX_DISPLAY_ITEMS) : mismatches);
    }

    @SuppressWarnings("unchecked")
    private Map lookupTargetRow(ConnectorInstance targetInstance, String connectorType,
                                String schema, org.dbcbc.sdk.model.Table table,
                                List<String> primaryKeys, Map sourceRow) {
        try {
            if (CollectionUtils.isEmpty(primaryKeys)) return null;

            String fullTableName = (schema != null && !schema.isEmpty()) ? schema + "." + table.getName() : table.getName();
            StringBuilder sb = new StringBuilder("SELECT * FROM ").append(fullTableName).append(" WHERE ");
            List<Object> args = new ArrayList<>();
            for (int k = 0; k < primaryKeys.size(); k++) {
                if (k > 0) sb.append(" AND ");
                sb.append(primaryKeys.get(k)).append(" = ?");
                args.add(sourceRow.get(primaryKeys.get(k)));
            }
            appendDbSpecificLimit(sb, connectorType);

            String lookupSql = sb.toString();
            List<Map<String, Object>> rows = ((org.dbcbc.sdk.connector.database.DatabaseConnectorInstance) targetInstance)
                    .execute(dt -> dt.queryForList(lookupSql, args.toArray()));
            return (rows != null && !rows.isEmpty()) ? rows.get(0) : null;
        } catch (Exception e) {
            logger.debug("目标行查找失败:{}", e.getMessage());
            return null;
        }
    }

    private void appendDbSpecificLimit(StringBuilder sb, String connectorType) {
        if ("Oracle".equalsIgnoreCase(connectorType) || "DM".equalsIgnoreCase(connectorType)) {
            sb.insert(0, "SELECT * FROM (");
            sb.append(") WHERE ROWNUM <= 1");
        } else if ("SqlServer".equalsIgnoreCase(connectorType)) {
            sb.insert(0, "SELECT TOP 1 * FROM (");
            sb.append(") _t");
        } else {
            sb.append(" LIMIT 1");
        }
    }

    private boolean valuesEqual(Object src, Object tgt) {
        if (src == null && tgt == null) return true;
        if (src == null || tgt == null) return false;
        String s = String.valueOf(src).trim();
        String t = String.valueOf(tgt).trim();
        return s.equals(t);
    }

    // ==================== 校验和对比 ====================

    private void verifyByChecksum(Mapping mapping, TableGroup merged, Map<String, String> command, TableVerifyResultVO result) {
        long sourceCount = getCount(mapping, merged, command, true);
        long targetCount = getCount(mapping, merged, command, false);
        result.setSourceCount(sourceCount);
        result.setTargetCount(targetCount);
        result.setCountMatch(sourceCount == targetCount);

        String sourceConnectorId = mapping.getSourceConnectorId();
        String targetConnectorId = mapping.getTargetConnectorId();
        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), targetConnectorId, ConnectorInstanceUtil.TARGET_SUFFIX);

        ConnectorConfig sConfig = profileComponent.getConnector(sourceConnectorId).getConfig();
        ConnectorConfig tConfig = profileComponent.getConnector(targetConnectorId).getConfig();
        ConnectorInstance sourceInstance = connectorFactory.connect(sourceInstanceId);
        ConnectorInstance targetInstance = connectorFactory.connect(targetInstanceId);

        List<FieldMapping> fieldMapping = merged.getFieldMapping();
        List<String> sourceFieldNames = new ArrayList<>();
        List<String> targetFieldNames = new ArrayList<>();
        if (!CollectionUtils.isEmpty(fieldMapping)) {
            for (FieldMapping fm : fieldMapping) {
                if (fm.getSource() != null) sourceFieldNames.add(fm.getSource().getName());
                if (fm.getTarget() != null) targetFieldNames.add(fm.getTarget().getName());
            }
        }

        String srcFullTable = buildFullTableName(mapping.getSourceSchema(), merged.getSourceTable().getName());
        String tgtFullTable = buildFullTableName(mapping.getTargetSchema(), merged.getTargetTable().getName());

        List<String> srcTypeNames = buildTypeNames(merged.getSourceTable(), sourceFieldNames);
        List<String> tgtTypeNames = buildTypeNames(merged.getTargetTable(), targetFieldNames);

        // Fetch concatenated row values from both sides
        List<String> srcSignatures = fetchRowSignatures(sourceInstance, sConfig.getConnectorType(), srcFullTable, sourceFieldNames, srcTypeNames);
        List<String> tgtSignatures = fetchRowSignatures(targetInstance, tConfig.getConnectorType(), tgtFullTable, targetFieldNames, tgtTypeNames);

        // Sort and compute MD5 in Java — database-agnostic
        String sourceChecksum = computeSignatureChecksum(srcSignatures);
        String targetChecksum = computeSignatureChecksum(tgtSignatures);

        result.setSourceChecksum(sourceChecksum);
        result.setTargetChecksum(targetChecksum);
        result.setChecksumMatch(sourceChecksum != null && sourceChecksum.equals(targetChecksum));
    }

    // ==================== 分层校验 ====================

    private static final int CHUNK_SIZE = 10000;
    private static final int MAX_DISPLAY_ITEMS = 20;

    @SuppressWarnings("unchecked")
    private void verifyByCascade(Mapping mapping, TableGroup merged, Map<String, String> command, CascadeTableResultVO result) {
        // === Layer 1: COUNT ===
        long sourceCount = getCount(mapping, merged, command, true);
        long targetCount = getCount(mapping, merged, command, false);
        result.setSourceCount(sourceCount);
        result.setTargetCount(targetCount);

        if (sourceCount != targetCount) {
            result.setLayer1Pass(false);
            result.setMissingPks(findMissingPks(mapping, merged));
            return;
        }
        result.setLayer1Pass(true);

        if (!isRunning()) return;

        // === Layer 2: Checksum ===
        String sourceConnectorId = mapping.getSourceConnectorId();
        String targetConnectorId = mapping.getTargetConnectorId();
        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), targetConnectorId, ConnectorInstanceUtil.TARGET_SUFFIX);

        ConnectorConfig sConfig = profileComponent.getConnector(sourceConnectorId).getConfig();
        ConnectorConfig tConfig = profileComponent.getConnector(targetConnectorId).getConfig();
        ConnectorInstance sourceInstance = connectorFactory.connect(sourceInstanceId);
        ConnectorInstance targetInstance = connectorFactory.connect(targetInstanceId);

        List<String> sourcePks = PrimaryKeyUtil.findTablePrimaryKeys(merged.getSourceTable());
        List<String> targetPks = PrimaryKeyUtil.findTablePrimaryKeys(merged.getTargetTable());

        List<FieldMapping> fieldMapping = merged.getFieldMapping();
        List<String> sourceFieldNames = new ArrayList<>();
        List<String> targetFieldNames = new ArrayList<>();
        if (!CollectionUtils.isEmpty(fieldMapping)) {
            for (FieldMapping fm : fieldMapping) {
                if (fm.getSource() != null) sourceFieldNames.add(fm.getSource().getName());
                if (fm.getTarget() != null) targetFieldNames.add(fm.getTarget().getName());
            }
        }

        String srcPkName = CollectionUtils.isEmpty(sourcePks) ? null : sourcePks.get(0);
        String tgtPkName = CollectionUtils.isEmpty(targetPks) ? null : targetPks.get(0);

        // Build chunks based on source table PK range
        List<long[]> chunks = buildChunks(sourceInstance, mapping.getSourceSchema(), merged.getSourceTable(), srcPkName);

        if (chunks.isEmpty()) {
            // Empty table or no numeric PK — do a single whole-table checksum
            chunks = new ArrayList<>();
            chunks.add(new long[]{0, Long.MAX_VALUE});
        }

        result.setTotalChunks(chunks.size());

        String srcConnectorType = sConfig.getConnectorType();
        String tgtConnectorType = tConfig.getConnectorType();
        String srcFullTable = buildFullTableName(mapping.getSourceSchema(), merged.getSourceTable().getName());
        String tgtFullTable = buildFullTableName(mapping.getTargetSchema(), merged.getTargetTable().getName());

        List<String> srcTypeNames = buildTypeNames(merged.getSourceTable(), sourceFieldNames);
        List<String> tgtTypeNames = buildTypeNames(merged.getTargetTable(), targetFieldNames);

        List<Integer> abnormalChunkIndexes = new ArrayList<>();

        for (int ci = 0; ci < chunks.size(); ci++) {
            if (!isRunning()) return;

            long[] chunk = chunks.get(ci);
            List<String> srcSigs = fetchRowSignatures(sourceInstance, srcConnectorType, srcFullTable, sourceFieldNames, srcPkName, chunk[0], chunk[1], srcTypeNames);
            List<String> tgtSigs = fetchRowSignatures(targetInstance, tgtConnectorType, tgtFullTable, targetFieldNames, tgtPkName, chunk[0], chunk[1], tgtTypeNames);

            String srcChecksum = computeSignatureChecksum(srcSigs);
            String tgtChecksum = computeSignatureChecksum(tgtSigs);

            if (srcChecksum == null || tgtChecksum == null || !srcChecksum.equals(tgtChecksum)) {
                abnormalChunkIndexes.add(ci);
            }
        }

        result.setAbnormalChunks(abnormalChunkIndexes.size());

        if (abnormalChunkIndexes.isEmpty()) {
            result.setLayer2Pass(true);
            return;
        }
        result.setLayer2Pass(false);

        if (!isRunning()) return;

        // === Layer 3: Row-level comparison in abnormal chunks ===
        List<FieldMismatchVO> allMismatches = new ArrayList<>();
        int totalMismatchedRows = 0;
        int totalSampledRows = 0;

        for (int ci : abnormalChunkIndexes) {
            if (!isRunning()) return;

            long[] chunk = chunks.get(ci);
            // Fetch (pk, row_signature) from both sides
            List<Map<String, Object>> srcRowSigs = fetchRowSignaturesWithPk(sourceInstance, srcConnectorType, srcFullTable, sourceFieldNames, srcPkName, chunk[0], chunk[1], srcTypeNames);
            List<Map<String, Object>> tgtRowSigs = fetchRowSignaturesWithPk(targetInstance, tgtConnectorType, tgtFullTable, targetFieldNames, tgtPkName, chunk[0], chunk[1], tgtTypeNames);

            Map<String, Map<String, Object>> tgtRowMap = new LinkedHashMap<>();
            if (tgtRowSigs != null) {
                for (Map<String, Object> row : tgtRowSigs) {
                    String pk = String.valueOf(row.get("pk_val"));
                    tgtRowMap.put(pk, row);
                }
            }

            if (srcRowSigs != null) {
                totalSampledRows += srcRowSigs.size();
                for (Map<String, Object> srcRow : srcRowSigs) {
                    String srcPk = String.valueOf(srcRow.get("pk_val"));
                    Map<String, Object> tgtRow = tgtRowMap.get(srcPk);

                    if (tgtRow == null) {
                        FieldMismatchVO mismatch = new FieldMismatchVO();
                        Map<String, Object> pkMap = new LinkedHashMap<>();
                        pkMap.put(srcPkName != null ? srcPkName : "pk", srcPk);
                        mismatch.setPrimaryKey(pkMap);
                        mismatch.setFieldName("*");
                        mismatch.setSourceValue("存在");
                        mismatch.setTargetValue("不存在");
                        allMismatches.add(mismatch);
                        totalMismatchedRows++;
                    } else {
                        String srcSig = String.valueOf(srcRow.get("row_sig"));
                        String tgtSig = String.valueOf(tgtRow.get("row_sig"));
                        if (!srcSig.equals(tgtSig)) {
                            // Field-level comparison
                            Map<String, Object> srcData = fetchRowData(sourceInstance, srcConnectorType, srcFullTable, sourceFieldNames, srcPkName, srcPk);
                            Map<String, Object> tgtData = fetchRowData(targetInstance, tgtConnectorType, tgtFullTable, targetFieldNames, tgtPkName, srcPk);
                            if (srcData != null && tgtData != null) {
                                List<FieldMismatchVO> fieldDiffs = compareFields(srcData, tgtData, fieldMapping, srcPkName, srcPk);
                                allMismatches.addAll(fieldDiffs);
                                if (!fieldDiffs.isEmpty()) totalMismatchedRows++;
                            }
                        }
                    }
                }
            }

            // Also check for extra rows in target
            if (srcRowSigs != null) {
                Set<String> srcPkSet = new HashSet<>();
                for (Map<String, Object> srcRow : srcRowSigs) {
                    srcPkSet.add(String.valueOf(srcRow.get("pk_val")));
                }
                for (Map<String, Object> tgtRow : tgtRowSigs != null ? tgtRowSigs : new ArrayList<Map<String, Object>>()) {
                    String tgtPk = String.valueOf(tgtRow.get("pk_val"));
                    if (!srcPkSet.contains(tgtPk)) {
                        FieldMismatchVO mismatch = new FieldMismatchVO();
                        Map<String, Object> pkMap = new LinkedHashMap<>();
                        pkMap.put(tgtPkName != null ? tgtPkName : "pk", tgtPk);
                        mismatch.setPrimaryKey(pkMap);
                        mismatch.setFieldName("*");
                        mismatch.setSourceValue("不存在");
                        mismatch.setTargetValue("存在");
                        allMismatches.add(mismatch);
                        totalMismatchedRows++;
                    }
                }
            }
        }

        result.setSampledRows(totalSampledRows);
        result.setMismatchedRows(totalMismatchedRows);
        result.setLayer3Pass(allMismatches.isEmpty());
        result.setMismatches(allMismatches.size() > MAX_DISPLAY_ITEMS ? allMismatches.subList(0, MAX_DISPLAY_ITEMS) : allMismatches);
    }

    @SuppressWarnings("unchecked")
    private List<long[]> buildChunks(ConnectorInstance connectorInstance, String schema, org.dbcbc.sdk.model.Table table, String pkName) {
        List<long[]> chunks = new ArrayList<>();
        if (pkName == null) return chunks;

        String fullTableName = buildFullTableName(schema, table.getName());
        String minMaxSql = "SELECT MIN(" + pkName + ") AS min_pk, MAX(" + pkName + ") AS max_pk FROM " + fullTableName;

        try {
            List<Map<String, Object>> rows = ((org.dbcbc.sdk.connector.database.DatabaseConnectorInstance) connectorInstance)
                    .execute(dt -> dt.queryForList(minMaxSql));
            if (rows == null || rows.isEmpty()) return chunks;

            Map<String, Object> row = rows.get(0);
            Object minObj = row.get("min_pk");
            Object maxObj = row.get("max_pk");
            if (minObj == null || maxObj == null) return chunks;

            long minPk = Long.parseLong(String.valueOf(minObj));
            long maxPk = Long.parseLong(String.valueOf(maxObj));

            for (long start = minPk; start <= maxPk; start += CHUNK_SIZE) {
                chunks.add(new long[]{start, start + CHUNK_SIZE});
            }
        } catch (NumberFormatException e) {
            // Non-numeric PK, cannot chunk — fall back to single chunk
            logger.debug("主键非数值类型，退化为整表校验:{}", table.getName());
        } catch (Exception e) {
            logger.warn("获取chunk范围失败:{}", e.getMessage());
        }
        return chunks;
    }

    /**
     * Build SQL that selects concatenated field values as a single "row_sig" string.
     * Database-agnostic: uses COALESCE + CAST on each field, separated by '|'.
     */
    private String buildConcatSelect(String connectorType, String tableName, List<String> fieldNames,
                                      String pkName, long chunkStart, long chunkEnd, boolean includePk,
                                      List<String> typeNames) {
        if (CollectionUtils.isEmpty(fieldNames)) {
            return includePk ? "SELECT 1 AS pk_val, '' AS row_sig WHERE 1=0" : "SELECT '' AS row_sig WHERE 1=0";
        }

        String type = connectorType != null ? connectorType.toLowerCase() : "";
        StringBuilder concatExpr = new StringBuilder();

        for (int i = 0; i < fieldNames.size(); i++) {
            if (i > 0) concatExpr.append(" || '|' || ");
            String f = fieldNames.get(i);
            switch (type) {
                case "mysql":
                    concatExpr.append("COALESCE(CAST(`").append(f).append("` AS CHAR), '~')");
                    break;
                case "postgresql":
                case "pg":
                    concatExpr.append("COALESCE(\"").append(f).append("\"::text, '~')");
                    break;
                case "oracle":
                case "dm":
                    if (isOracleLobType(typeNames.get(i))) {
                        concatExpr.append("NVL(DBMS_LOB.SUBSTR(\"").append(f).append("\", 4000, 1), '~')");
                    } else {
                        concatExpr.append("NVL(TO_CHAR(\"").append(f).append("\"), '~')");
                    }
                    break;
                case "sqlserver":
                    concatExpr.append("COALESCE(CAST(\"").append(f).append("\" AS NVARCHAR(MAX)), '~')");
                    break;
                default:
                    concatExpr.append("COALESCE(CAST(\"").append(f).append("\" AS CHAR), '~')");
                    break;
            }
        }

        StringBuilder sb = new StringBuilder("SELECT ");
        if (includePk && pkName != null) {
            sb.append(pkName).append(" AS pk_val, ");
        }
        sb.append(concatExpr).append(" AS row_sig FROM ").append(tableName);

        if (pkName != null && chunkEnd != Long.MAX_VALUE) {
            sb.append(" WHERE ").append(pkName).append(" >= ").append(chunkStart)
              .append(" AND ").append(pkName).append(" < ").append(chunkEnd);
        }

        return sb.toString();
    }

    private static boolean isOracleLobType(String typeName) {
        if (typeName == null) return false;
        String upper = typeName.toUpperCase();
        return upper.contains("CLOB") || upper.contains("BLOB") || upper.contains("NCLOB");
    }

    private List<String> buildTypeNames(org.dbcbc.sdk.model.Table table, List<String> fieldNames) {
        List<String> typeNames = new ArrayList<>();
        if (table == null || table.getColumn() == null) {
            for (int i = 0; i < fieldNames.size(); i++) typeNames.add(null);
            return typeNames;
        }
        Map<String, String> fieldTypeMap = new HashMap<>();
        for (Field f : table.getColumn()) {
            fieldTypeMap.put(f.getName(), f.getTypeName());
        }
        for (String fn : fieldNames) {
            typeNames.add(fieldTypeMap.get(fn));
        }
        return typeNames;
    }

    /**
     * Fetch row signatures (concatenated field values) for checksum comparison.
     * Returns list of row_sig strings.
     */
    @SuppressWarnings("unchecked")
    private List<String> fetchRowSignatures(ConnectorInstance connectorInstance, String connectorType,
                                             String tableName, List<String> fieldNames, List<String> typeNames) {
        return fetchRowSignatures(connectorInstance, connectorType, tableName, fieldNames, null, 0, Long.MAX_VALUE, typeNames);
    }

    @SuppressWarnings("unchecked")
    private List<String> fetchRowSignatures(ConnectorInstance connectorInstance, String connectorType,
                                             String tableName, List<String> fieldNames,
                                             String pkName, long chunkStart, long chunkEnd, List<String> typeNames) {
        try {
            String sql = buildConcatSelect(connectorType, tableName, fieldNames, pkName, chunkStart, chunkEnd, false, typeNames);
            List<Map<String, Object>> rows = ((org.dbcbc.sdk.connector.database.DatabaseConnectorInstance) connectorInstance)
                    .execute(dt -> dt.queryForList(sql));
            if (rows == null) return new ArrayList<>();
            List<String> sigs = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                Object val = row.values().iterator().next();
                sigs.add(val != null ? String.valueOf(val) : "");
            }
            return sigs;
        } catch (Exception e) {
            logger.warn("获取行签名失败:{}", e.getMessage());
            return null;
        }
    }

    /**
     * Fetch (pk, row_signature) pairs for row-level comparison.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> fetchRowSignaturesWithPk(ConnectorInstance connectorInstance, String connectorType,
                                                                String tableName, List<String> fieldNames,
                                                                String pkName, long chunkStart, long chunkEnd,
                                                                List<String> typeNames) {
        try {
            String sql = buildConcatSelect(connectorType, tableName, fieldNames, pkName, chunkStart, chunkEnd, true, typeNames);
            return ((org.dbcbc.sdk.connector.database.DatabaseConnectorInstance) connectorInstance)
                    .execute(dt -> dt.queryForList(sql));
        } catch (Exception e) {
            logger.warn("获取行签名(含PK)失败:{}", e.getMessage());
            return null;
        }
    }

    /**
     * Compute a single checksum from a list of row signatures.
     * Sorts all signatures then computes MD5 — database-agnostic.
     */
    private String computeSignatureChecksum(List<String> signatures) {
        if (signatures == null || signatures.isEmpty()) return "d41d8cd98f00b204e9800998ecf8427e"; // MD5 of empty
        try {
            Collections.sort(signatures);
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            for (String sig : signatures) {
                md.update(sig.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                md.update((byte) '\n');
            }
            byte[] digest = md.digest();
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b & 0xff));
            }
            return hex.toString();
        } catch (Exception e) {
            logger.warn("计算签名校验和失败:{}", e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fetchRowData(ConnectorInstance connectorInstance, String connectorType,
                                              String tableName, List<String> fieldNames, String pkName, String pkValue) {
        try {
            if (pkName == null || CollectionUtils.isEmpty(fieldNames)) return null;

            StringBuilder sb = new StringBuilder("SELECT ");
            for (int i = 0; i < fieldNames.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(fieldNames.get(i));
            }
            sb.append(" FROM ").append(tableName).append(" WHERE ").append(pkName).append(" = ?");
            appendDbSpecificLimit(sb, connectorType);

            List<Map<String, Object>> rows = ((org.dbcbc.sdk.connector.database.DatabaseConnectorInstance) connectorInstance)
                    .execute(dt -> dt.queryForList(sb.toString(), pkValue));
            return (rows != null && !rows.isEmpty()) ? rows.get(0) : null;
        } catch (Exception e) {
            logger.debug("获取行数据失败:{}", e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> findMissingPks(Mapping mapping, TableGroup merged) {
        List<Map<String, Object>> missingPks = new ArrayList<>();
        try {
            String sourceConnectorId = mapping.getSourceConnectorId();
            String targetConnectorId = mapping.getTargetConnectorId();
            String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
            String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), targetConnectorId, ConnectorInstanceUtil.TARGET_SUFFIX);

            ConnectorConfig sConfig = profileComponent.getConnector(sourceConnectorId).getConfig();
            ConnectorConfig tConfig = profileComponent.getConnector(targetConnectorId).getConfig();
            ConnectorInstance sourceInstance = connectorFactory.connect(sourceInstanceId);
            ConnectorInstance targetInstance = connectorFactory.connect(targetInstanceId);

            List<String> sourcePks = PrimaryKeyUtil.findTablePrimaryKeys(merged.getSourceTable());
            if (CollectionUtils.isEmpty(sourcePks)) return missingPks;

            String srcPk = sourcePks.get(0);
            String tgtPk = PrimaryKeyUtil.findTablePrimaryKeys(merged.getTargetTable()).get(0);
            String srcTable = buildFullTableName(mapping.getSourceSchema(), merged.getSourceTable().getName());
            String tgtTable = buildFullTableName(mapping.getTargetSchema(), merged.getTargetTable().getName());

            String sql = "SELECT s." + srcPk + " FROM " + srcTable + " s WHERE NOT EXISTS (SELECT 1 FROM " + tgtTable + " t WHERE t." + tgtPk + " = s." + srcPk + ")";

            // For NOT EXISTS queries, use a subquery approach with LIMIT
            String limitSql;
            String connectorType = sConfig.getConnectorType();
            if ("Oracle".equalsIgnoreCase(connectorType) || "DM".equalsIgnoreCase(connectorType)) {
                limitSql = "SELECT * FROM (" + sql + ") WHERE ROWNUM <= " + MAX_DISPLAY_ITEMS;
            } else if ("SqlServer".equalsIgnoreCase(connectorType)) {
                limitSql = "SELECT TOP " + MAX_DISPLAY_ITEMS + " * FROM (" + sql + ") _t";
            } else {
                limitSql = sql + " LIMIT " + MAX_DISPLAY_ITEMS;
            }

            List<Map<String, Object>> rows = ((org.dbcbc.sdk.connector.database.DatabaseConnectorInstance) sourceInstance).execute(dt -> dt.queryForList(limitSql));
            if (rows != null) {
                for (Map<String, Object> row : rows) {
                    Map<String, Object> pkMap = new LinkedHashMap<>();
                    pkMap.put(srcPk, row.get(srcPk));
                    missingPks.add(pkMap);
                }
            }
        } catch (Exception e) {
            logger.warn("查找缺失主键失败:{}", e.getMessage());
        }
        return missingPks;
    }

    private List<FieldMismatchVO> compareFields(Map<String, Object> srcData, Map<String, Object> tgtData,
                                                  List<FieldMapping> fieldMapping, String pkName, String pkValue) {
        List<FieldMismatchVO> diffs = new ArrayList<>();
        for (FieldMapping fm : fieldMapping) {
            if (fm.getSource() == null || fm.getTarget() == null) continue;
            String srcFieldName = fm.getSource().getName();
            String tgtFieldName = fm.getTarget().getName();
            Object srcVal = srcData.get(srcFieldName);
            Object tgtVal = tgtData.get(tgtFieldName);
            if (!valuesEqual(srcVal, tgtVal)) {
                FieldMismatchVO mismatch = new FieldMismatchVO();
                Map<String, Object> pkMap = new LinkedHashMap<>();
                pkMap.put(pkName != null ? pkName : "pk", pkValue);
                mismatch.setPrimaryKey(pkMap);
                mismatch.setFieldName(tgtFieldName);
                mismatch.setSourceValue(srcVal);
                mismatch.setTargetValue(tgtVal);
                diffs.add(mismatch);
            }
        }
        return diffs;
    }

    private String buildFullTableName(String schema, String tableName) {
        return (schema != null && !schema.isEmpty()) ? schema + "." + tableName : tableName;
    }

    // ==================== 公共方法 ====================

    private long getCount(Mapping mapping, TableGroup merged, Map<String, String> command, boolean isSource) {
        String connectorId = isSource ? mapping.getSourceConnectorId() : mapping.getTargetConnectorId();
        String suffix = isSource ? ConnectorInstanceUtil.SOURCE_SUFFIX : ConnectorInstanceUtil.TARGET_SUFFIX;
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(mapping.getId(), connectorId, suffix);
        ConnectorConfig config = profileComponent.getConnector(connectorId).getConfig();
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        ConnectorService connectorService = connectorFactory.getConnectorService(config);

        Map<String, String> countCommand;
        if (isSource) {
            countCommand = command;
        } else {
            CommandConfig targetCmdConfig = new CommandConfig(
                    config.getConnectorType(), mapping.getTargetSchema(),
                    merged.getTargetTable(), connectorInstance, null
            );
            countCommand = connectorService.getSourceCommand(targetCmdConfig);
        }

        DefaultMetaContext metaContext = new DefaultMetaContext();
        metaContext.setCommand(countCommand);
        metaContext.setSourceTable(isSource ? merged.getSourceTable() : merged.getTargetTable());
        metaContext.setSourceConnectorInstance(connectorInstance);
        setRsaConfig(metaContext);

        return connectorService.getCount(connectorInstance, metaContext);
    }

    private void setRsaConfig(DefaultMetaContext context) {
        if (profileComponent.getSystemConfig().isEnableOpenAPI()) {
            context.setRsaManager(rsaManager);
            context.setRsaConfig(profileComponent.getSystemConfig().getRsaConfig());
        }
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public void setVerifyType(String verifyType) {
        this.verifyType = verifyType;
    }

    public void setProfileComponent(ProfileComponent profileComponent) {
        this.profileComponent = profileComponent;
    }

    public void setConnectorFactory(ConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    public void setParserComponent(ParserComponent parserComponent) {
        this.parserComponent = parserComponent;
    }

    public void setRsaManager(RsaManager rsaManager) {
        this.rsaManager = rsaManager;
    }

    public void setProgressVO(VerifyProgressVO progressVO) {
        this.progressVO = progressVO;
    }

    public void setStorageService(StorageService storageService) {
        this.storageService = storageService;
    }

    public void setSnowflakeIdWorker(SnowflakeIdWorker snowflakeIdWorker) {
        this.snowflakeIdWorker = snowflakeIdWorker;
    }
}
