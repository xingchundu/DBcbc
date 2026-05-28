/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.biz.vo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 分层校验单表结果VO
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-05-27
 */
public class CascadeTableResultVO implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableGroupId;
    private String sourceTableName;
    private String targetTableName;
    private String status; // running / completed / error

    // Layer 1: 行数对比
    private long sourceCount;
    private long targetCount;
    private boolean layer1Pass;
    private List<Map<String, Object>> missingPks;

    // Layer 2: 校验和
    private int totalChunks;
    private int abnormalChunks;
    private boolean layer2Pass;
    private String sourceChecksum;
    private String targetChecksum;

    // Layer 3: 行级采样
    private int sampledRows;
    private int mismatchedRows;
    private boolean layer3Pass;
    private List<FieldMismatchVO> mismatches;

    public String getTableGroupId() { return tableGroupId; }
    public void setTableGroupId(String tableGroupId) { this.tableGroupId = tableGroupId; }

    public String getSourceTableName() { return sourceTableName; }
    public void setSourceTableName(String sourceTableName) { this.sourceTableName = sourceTableName; }

    public String getTargetTableName() { return targetTableName; }
    public void setTargetTableName(String targetTableName) { this.targetTableName = targetTableName; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public long getSourceCount() { return sourceCount; }
    public void setSourceCount(long sourceCount) { this.sourceCount = sourceCount; }

    public long getTargetCount() { return targetCount; }
    public void setTargetCount(long targetCount) { this.targetCount = targetCount; }

    public boolean isLayer1Pass() { return layer1Pass; }
    public void setLayer1Pass(boolean layer1Pass) { this.layer1Pass = layer1Pass; }

    public List<Map<String, Object>> getMissingPks() { return missingPks; }
    public void setMissingPks(List<Map<String, Object>> missingPks) { this.missingPks = missingPks; }

    public int getTotalChunks() { return totalChunks; }
    public void setTotalChunks(int totalChunks) { this.totalChunks = totalChunks; }

    public int getAbnormalChunks() { return abnormalChunks; }
    public void setAbnormalChunks(int abnormalChunks) { this.abnormalChunks = abnormalChunks; }

    public boolean isLayer2Pass() { return layer2Pass; }
    public void setLayer2Pass(boolean layer2Pass) { this.layer2Pass = layer2Pass; }

    public String getSourceChecksum() { return sourceChecksum; }
    public void setSourceChecksum(String sourceChecksum) { this.sourceChecksum = sourceChecksum; }

    public String getTargetChecksum() { return targetChecksum; }
    public void setTargetChecksum(String targetChecksum) { this.targetChecksum = targetChecksum; }

    public int getSampledRows() { return sampledRows; }
    public void setSampledRows(int sampledRows) { this.sampledRows = sampledRows; }

    public int getMismatchedRows() { return mismatchedRows; }
    public void setMismatchedRows(int mismatchedRows) { this.mismatchedRows = mismatchedRows; }

    public boolean isLayer3Pass() { return layer3Pass; }
    public void setLayer3Pass(boolean layer3Pass) { this.layer3Pass = layer3Pass; }

    public List<FieldMismatchVO> getMismatches() { return mismatches; }
    public void setMismatches(List<FieldMismatchVO> mismatches) { this.mismatches = mismatches; }
}
