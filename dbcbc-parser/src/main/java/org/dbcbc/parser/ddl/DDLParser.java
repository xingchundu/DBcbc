/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.parser.ddl;

import net.sf.jsqlparser.JSQLParserException;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.sdk.config.DDLConfig;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.spi.ConnectorService;

public interface DDLParser {

    DDLConfig parse(ConnectorInstance connectorInstance, ConnectorService connectorService, TableGroup tableGroup, String sql) throws JSQLParserException;

    void refreshFiledMappings(TableGroup tableGroup, DDLConfig targetDDLConfig);
}
