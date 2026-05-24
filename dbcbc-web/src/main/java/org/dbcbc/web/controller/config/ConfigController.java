package org.dbcbc.web.controller.config;

import org.dbcbc.biz.SystemConfigService;
import org.dbcbc.biz.vo.RestResult;
import org.dbcbc.common.config.AppConfig;
import org.dbcbc.common.model.VersionInfo;
import org.dbcbc.common.util.JsonUtil;
import org.dbcbc.manager.impl.PreloadTemplate;
import org.dbcbc.parser.CacheService;
import org.dbcbc.parser.LogService;
import org.dbcbc.parser.LogType;
import org.dbcbc.storage.impl.SnowflakeIdWorker;
import org.dbcbc.web.Version;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/config")
public class ConfigController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private CacheService cacheService;

    @Resource
    private LogService logService;

    @Resource
    private AppConfig appConfig;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("config", systemConfigService.getConfigModelAll());
        model.put("fileSize", JsonUtil.objToJson(cacheService.getAll()).getBytes(Charset.defaultCharset()).length);
        return "config/list";
    }

    @PostMapping(value = "/upload")
    @ResponseBody
    public RestResult upload(MultipartFile[] files) {
        try {
            if (files != null) {
                for (MultipartFile file : files) {
                    if (file == null) {
                        continue;
                    }
                    String filename = file.getOriginalFilename();
                    systemConfigService.checkFileSuffix(filename);
                    String tmpdir = System.getProperty("java.io.tmpdir");
                    File dest = new File(tmpdir + filename);
                    FileUtils.deleteQuietly(dest);
                    FileUtils.copyInputStreamToFile(file.getInputStream(), dest);
                    systemConfigService.refreshConfig(dest);
                    String msg = String.format("导入配置文件%s", filename);
                    logger.info(msg);
                    logService.log(LogType.CacheLog.IMPORT, msg);
                }
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/download")
    public void download(HttpServletResponse response) {
        String fileName = String.format("%s-%s-%s.json", appConfig.getName(), appConfig.getVersion(), snowflakeIdWorker.nextId());
        response.setHeader("content-type", "application/octet-stream");
        response.setHeader("Content-Disposition", String.format("attachment; filename=%s", fileName));
        response.setContentType("application/octet-stream");
        OutputStream outputStream = null;
        try {
            outputStream = response.getOutputStream();
            String cache = JsonUtil.objToJson(getConfig());
            byte[] bytes = cache.getBytes(Charset.defaultCharset());
            int length = bytes.length;
            String msg = String.format("导出配置文件%s，大小%dKB", fileName, (length / 1024));
            logger.info(msg);
            logService.log(LogType.CacheLog.EXPORT, msg);
            outputStream.write(bytes, 0, length);
            outputStream.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
    }

    private Map<String, Object> getConfig() {
        Map<String, Object> map = new HashMap<>();
        VersionInfo info = new VersionInfo();
        info.setVersion(Version.CURRENT.getVersion());
        info.setAppName(appConfig.getName());
        info.setCreateTime(Instant.now().toEpochMilli());
        map.put(PreloadTemplate.DBS_VERSION_INFO, info);
        map.putAll(cacheService.getAll());
        return map;
    }
}
