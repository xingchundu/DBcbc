/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.sdk.spi;

import org.dbcbc.sdk.enums.EditionEnum;
import org.dbcbc.sdk.model.ProductInfo;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-13 01:17
 */
public interface LicenseService {

    /**
     * 获取版本号
     */
    EditionEnum getEditionEnum();

    /**
     * 获取License上传路径 dbcbc/conf/
     */
    String getLicensePath();

    /**
     * 获取授权KEY
     *
     * @return
     */
    String getKey();

    /**
     * 获取授权信息
     *
     * @return
     */
    ProductInfo getProductInfo();

    /**
     * 更新授权
     */
    void updateLicense();
}
