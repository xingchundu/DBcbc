/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.sdk.filter.impl;

import org.dbcbc.sdk.enums.FilterEnum;
import org.dbcbc.sdk.enums.FilterTypeEnum;
import org.dbcbc.sdk.filter.AbstractFilter;

public class StringFilter extends AbstractFilter {

    public StringFilter(String name, FilterEnum filterEnum, String value, boolean enableHighLightSearch) {
        setName(name);
        setFilter(filterEnum.getName());
        setValue(value);
        setEnableHighLightSearch(enableHighLightSearch);
    }

    @Override
    public FilterTypeEnum getFilterTypeEnum() {
        return FilterTypeEnum.STRING;
    }
}
