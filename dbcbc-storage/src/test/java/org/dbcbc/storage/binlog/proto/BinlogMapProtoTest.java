package org.dbcbc.storage.binlog.proto;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class BinlogMapProtoTest {

    @Test
    public void shouldBuildBinlogMap() {
        BinlogMap map = BinlogMap.newBuilder().putRow("k", ByteString.copyFromUtf8("v")).build();
        Assert.assertEquals(1, map.getRowCount());
    }
}
