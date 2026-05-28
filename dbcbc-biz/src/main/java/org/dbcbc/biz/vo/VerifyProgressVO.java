/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.biz.vo;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 数据验证进度VO
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-05-26
 */
public class VerifyProgressVO implements Serializable {

    private static final long serialVersionUID = 1L;

    private String mappingId;
    private String mappingName;
    private String verifyType;
    private volatile String status;
    private volatile int totalTables;
    private volatile int completedTables;
    private volatile long startTime;
    private volatile long endTime;
    private volatile String errorMessage;
    private final List<TableVerifyResultVO> results = new CopyOnWriteArrayList<>();
    private final List<CascadeTableResultVO> cascadeResults = new CopyOnWriteArrayList<>();

    public String getMappingId() {
        return mappingId;
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public String getMappingName() {
        return mappingName;
    }

    public void setMappingName(String mappingName) {
        this.mappingName = mappingName;
    }

    public String getVerifyType() {
        return verifyType;
    }

    public void setVerifyType(String verifyType) {
        this.verifyType = verifyType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getTotalTables() {
        return totalTables;
    }

    public void setTotalTables(int totalTables) {
        this.totalTables = totalTables;
    }

    public int getCompletedTables() {
        return completedTables;
    }

    public void setCompletedTables(int completedTables) {
        this.completedTables = completedTables;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public List<TableVerifyResultVO> getResults() {
        return results;
    }

    public List<CascadeTableResultVO> getCascadeResults() {
        return cascadeResults;
    }
}
