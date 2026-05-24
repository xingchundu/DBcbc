/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.sdk.schema.support;

import org.dbcbc.sdk.enums.DataTypeEnum;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.AbstractDataType;

import java.sql.Time;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-21 23:56
 */
public abstract class TimeType extends AbstractDataType<Time> {

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.TIME;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Time) {
            return val;
        }
        return throwUnsupportedException(val, field);
    }
}
