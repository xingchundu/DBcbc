/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.biz.impl;

import org.apache.commons.io.FileUtils;
import org.dbcbc.biz.SystemConfigService;
import org.dbcbc.biz.UserConfigService;
import org.dbcbc.biz.checker.Checker;
import org.dbcbc.biz.vo.SystemConfigVO;
import org.dbcbc.common.config.AppConfig;
import org.dbcbc.common.enums.FileSuffixEnum;
import org.dbcbc.common.model.RsaVersion;
import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.RSAUtil;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.manager.impl.PreloadTemplate;
import org.dbcbc.parser.LogService;
import org.dbcbc.parser.LogType;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.model.ConfigModel;
import org.dbcbc.parser.model.SystemConfig;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class SystemConfigServiceImpl implements SystemConfigService {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private PreloadTemplate preloadTemplate;

    @Resource
    private Checker systemConfigChecker;

    @Resource
    private LogService logService;

    @Resource
    private UserConfigService userConfigService;

    @Resource
    private AppConfig appConfig;

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = systemConfigChecker.checkEditConfigModel(params);
        profileComponent.editConfigModel(model);
        return "修改成功.";
    }

    @Override
    public SystemConfigVO getSystemConfigVo() {
        SystemConfigVO systemConfigVo = new SystemConfigVO();
        BeanUtils.copyProperties(getSystemConfig(), systemConfigVo);
        systemConfigVo.setWatermark(getWatermark(systemConfigVo));
        return systemConfigVo;
    }

    @Override
    public SystemConfig getSystemConfig() {
        SystemConfig config = profileComponent.getSystemConfig();
        if (null != config) {
            return config;
        }

        synchronized (this) {
            config = profileComponent.getSystemConfig();
            if (null == config) {
                config = (SystemConfig) systemConfigChecker.checkAddConfigModel(new HashMap<>());
            }
            return config;
        }
    }

    @Override
    public List<ConfigModel> getConfigModelAll() {
        List<ConfigModel> list = new ArrayList<>();
        list.add(getSystemConfig());
        list.add(userConfigService.getUserConfig());
        list.addAll(profileComponent.getConnectorAll().stream().limit(5).collect(Collectors.toList()));
        list.addAll(profileComponent.getMappingAll().stream().limit(5).collect(Collectors.toList()));
        list.addAll(profileComponent.getMetaAll().stream().limit(5).collect(Collectors.toList()));
        return list;
    }

    @Override
    public void checkFileSuffix(String filename) {
        Assert.hasText(filename, "the config filename is null.");
        String suffix = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
        FileSuffixEnum fileSuffix = FileSuffixEnum.getFileSuffix(suffix);
        Assert.notNull(fileSuffix, "Illegal file suffix");
        Assert.isTrue(FileSuffixEnum.JSON == fileSuffix, String.format("不正确的文件扩展名 \"%s\"，只支持 \"%s\" 的文件扩展名。", filename, FileSuffixEnum.JSON.getName()));
    }

    @Override
    public void refreshConfig(File file) {
        Assert.notNull(file, "the config file is null.");
        try {
            List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
            if (!CollectionUtils.isEmpty(lines)) {
                StringBuilder json = new StringBuilder();
                lines.forEach(json::append);
                preloadTemplate.reload(json.toString());
            }
        } catch (IOException e) {
            logService.log(LogType.CacheLog.IMPORT_ERROR);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Override
    public String getWatermark(SystemConfig systemConfig) {
        return StringUtil.isNotBlank(systemConfig.getWatermark()) ? systemConfig.getWatermark() : appConfig.getName() + "-${username}<br />" + appConfig.getCompany();
    }

    @Override
    public RsaVersion createRSAConfig(int keyLength) {
        Assert.isTrue(keyLength >= 1024 && keyLength <= 8192, "密钥长度支持的范围[1024-8192]");
        return RSAUtil.createKeys(keyLength);
    }

    @Override
    public String generateApiSecret() {
        byte[] bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }
}