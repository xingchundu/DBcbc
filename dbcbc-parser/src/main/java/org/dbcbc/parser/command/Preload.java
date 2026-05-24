/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.parser.command;

import org.dbcbc.parser.ParserException;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.Meta;
import org.dbcbc.parser.model.SystemConfig;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.model.UserConfig;

/**
 * 预加载接口
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
public interface Preload {

    default SystemConfig parseSystemConfig() {
        throw new ParserException("Unsupported method parseSystemConfig");
    }

    default UserConfig parseUserConfig() {
        throw new ParserException("Unsupported method parseUserConfig");
    }

    default Connector parseConnector() {
        throw new ParserException("Unsupported method parseConnector");
    }

    default Mapping parseMapping() {
        throw new ParserException("Unsupported method parseMapping");
    }

    default TableGroup parseTableGroup() {
        throw new ParserException("Unsupported method parseTableGroup");
    }

    default Meta parseMeta() {
        throw new ParserException("Unsupported method parseMeta");
    }
}
