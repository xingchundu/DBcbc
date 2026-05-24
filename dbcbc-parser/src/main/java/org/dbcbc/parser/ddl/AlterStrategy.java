/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbcbc.parser.ddl;

import org.dbcbc.sdk.config.DDLConfig;

import net.sf.jsqlparser.statement.alter.AlterExpression;

import net.sf.jsqlparser.statement.alter.AlterExpression;

/**
 * Alter策略
 *
 * @version 1.0.0
 * @Author life
 * @Date 2023-09-24 14:24
 */
public interface AlterStrategy {

    /**
     * 解析DDLConfig
     *
     * @param expression
     * @param ddlConfig
     */
    void parse(AlterExpression expression, DDLConfig ddlConfig);
}
