/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.sdk.schema.support;

import org.dbcbc.sdk.enums.DataTypeEnum;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.schema.AbstractDataType;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-21 23:56
 */
public abstract class FloatType extends AbstractDataType<Float> {

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.FLOAT;
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof Number) {
            return ((Number) val).floatValue();
        }
        if (val instanceof String) {
            return Float.parseFloat((String) val);
        }
        return throwUnsupportedException(val, field);
    }
}
