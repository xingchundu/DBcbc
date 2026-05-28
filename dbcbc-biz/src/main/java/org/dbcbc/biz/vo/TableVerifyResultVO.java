/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.biz.vo;

import java.io.Serializable;
import java.util.List;

/**
 * 单表验证结果VO
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-05-26
 */
public class TableVerifyResultVO implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableGroupId;
    private String sourceTableName;
    private String targetTableName;
    private long sourceCount;
    private long targetCount;
    private boolean countMatch;
    private String status;
    private String errorMessage;

    // 行级采样
    private int sampledRows;
    private int mismatchedRows;
    private List<FieldMismatchVO> mismatches;

    // 校验和
    private String sourceChecksum;
    private String targetChecksum;
    private boolean checksumMatch;

    public String getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(String tableGroupId) {
        this.tableGroupId = tableGroupId;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public long getSourceCount() {
        return sourceCount;
    }

    public void setSourceCount(long sourceCount) {
        this.sourceCount = sourceCount;
    }

    public long getTargetCount() {
        return targetCount;
    }

    public void setTargetCount(long targetCount) {
        this.targetCount = targetCount;
    }

    public boolean isCountMatch() {
        return countMatch;
    }

    public void setCountMatch(boolean countMatch) {
        this.countMatch = countMatch;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public int getSampledRows() {
        return sampledRows;
    }

    public void setSampledRows(int sampledRows) {
        this.sampledRows = sampledRows;
    }

    public int getMismatchedRows() {
        return mismatchedRows;
    }

    public void setMismatchedRows(int mismatchedRows) {
        this.mismatchedRows = mismatchedRows;
    }

    public List<FieldMismatchVO> getMismatches() {
        return mismatches;
    }

    public void setMismatches(List<FieldMismatchVO> mismatches) {
        this.mismatches = mismatches;
    }

    public String getSourceChecksum() {
        return sourceChecksum;
    }

    public void setSourceChecksum(String sourceChecksum) {
        this.sourceChecksum = sourceChecksum;
    }

    public String getTargetChecksum() {
        return targetChecksum;
    }

    public void setTargetChecksum(String targetChecksum) {
        this.targetChecksum = targetChecksum;
    }

    public boolean isChecksumMatch() {
        return checksumMatch;
    }

    public void setChecksumMatch(boolean checksumMatch) {
        this.checksumMatch = checksumMatch;
    }
}
