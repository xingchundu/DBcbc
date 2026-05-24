package org.dbcbc.parser.convert.handler;

import org.dbcbc.common.util.UUIDUtil;
import org.dbcbc.parser.convert.Handler;

/**
 * UUID
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 23:05
 */
public class UUIDHandler implements Handler {

    @Override
    public Object handle(String args, Object value) {
        return UUIDUtil.getUUID();
    }
}
