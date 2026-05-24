/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbcbc.web;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2025-02-09 15:06
 */
public class Version {

    /**
     * 大版本-小版本-编号-年-月-日-序号
     */
    private final long version;

    public static final Version V_2_0_5 = new Version(20_00_05_2025_02_19_00L);
    public static final Version V_2_0_6 = new Version(20_00_06_2025_00_00_00L);
    public static final Version V_2_0_7 = new Version(20_00_07_2025_07_08_00L);
    public static final Version V_2_0_8 = new Version(20_00_08_2026_01_30_00L);
    public static final Version V_2_0_9 = new Version(20_00_08_2026_02_26_00L);

    /** 2.0.0 发行线 */
    public static final Version V_2_0_0 = new Version(20_00_00_2026_04_28_00L);

    public static final Version CURRENT = V_2_0_0;

    public Version(long version) {
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return String.valueOf(version);
    }
}
