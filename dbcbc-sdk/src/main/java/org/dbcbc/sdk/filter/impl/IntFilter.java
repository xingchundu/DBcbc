/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.sdk.filter.impl;

import org.dbcbc.sdk.enums.FilterEnum;
import org.dbcbc.sdk.enums.FilterTypeEnum;
import org.dbcbc.sdk.filter.AbstractFilter;

import java.util.Objects;

public class IntFilter extends AbstractFilter {

    public IntFilter(String name, int value) {
        setName(name);
        setFilter(FilterEnum.EQUAL.getName());
        setValue(Objects.toString(value));
    }

    @Override
    public FilterTypeEnum getFilterTypeEnum() {
        return FilterTypeEnum.INT;
    }
}
