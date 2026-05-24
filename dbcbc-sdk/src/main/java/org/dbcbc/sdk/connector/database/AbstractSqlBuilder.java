/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.sdk.connector.database;

import org.dbcbc.sdk.SdkException;
import org.dbcbc.sdk.config.SqlBuilderConfig;
import org.dbcbc.sdk.connector.database.sqlbuilder.SqlBuilder;

public abstract class AbstractSqlBuilder implements SqlBuilder {

    @Override
    public String buildQuerySql(SqlBuilderConfig config) {
        throw new SdkException("Not implemented");
    }
}
