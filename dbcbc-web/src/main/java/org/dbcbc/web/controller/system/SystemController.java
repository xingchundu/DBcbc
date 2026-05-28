/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.web.controller.system;

import org.dbcbc.biz.SystemConfigService;
import org.dbcbc.biz.vo.RestResult;
import org.dbcbc.parser.LogService;
import org.dbcbc.parser.LogType;
import org.dbcbc.web.controller.BaseController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import java.util.Map;

@Controller
@RequestMapping(value = "/system")
public class SystemController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private LogService logService;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("config", systemConfigService.getSystemConfigVo());
        return "system/list";
    }

    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            logService.log(LogType.SystemLog.INFO, "修改系统配置");
            return RestResult.restSuccess(systemConfigService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/generateRSA")
    @ResponseBody
    public RestResult generateRSA(HttpServletRequest request, @RequestParam(value = "keyLength") int keyLength) {
        try {
            logService.log(LogType.SystemLog.INFO, "生成RSA密钥对, 长度:%s", keyLength);
            return RestResult.restSuccess(systemConfigService.createRSAConfig(keyLength));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 生成 API 密钥（OpenAPI 登录凭证）
     */
    @PostMapping("/generateApiSecret")
    @ResponseBody
    public RestResult generateApiSecret(HttpServletRequest request) {
        try {
            logService.log(LogType.SystemLog.INFO, "生成API密钥");
            return RestResult.restSuccess(systemConfigService.generateApiSecret());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}