/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.biz.vo;

import java.io.Serializable;
import java.util.Map;

/**
 * 字段不匹配详情VO
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-05-26
 */
public class FieldMismatchVO implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, Object> primaryKey;
    private String fieldName;
    private Object sourceValue;
    private Object targetValue;

    public Map<String, Object> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(Map<String, Object> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Object getSourceValue() {
        return sourceValue;
    }

    public void setSourceValue(Object sourceValue) {
        this.sourceValue = sourceValue;
    }

    public Object getTargetValue() {
        return targetValue;
    }

    public void setTargetValue(Object targetValue) {
        this.targetValue = targetValue;
    }
}
