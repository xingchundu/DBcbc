/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.web.controller.verify;

import org.dbcbc.biz.DataVerifyService;
import org.dbcbc.biz.MappingService;
import org.dbcbc.biz.vo.RestResult;
import org.dbcbc.parser.LogService;
import org.dbcbc.parser.LogType;
import org.dbcbc.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * 数据验证控制器
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-05-26
 */
@Controller
@RequestMapping("/verify")
public class DataVerifyController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private DataVerifyService dataVerifyService;

    @Resource
    private MappingService mappingService;

    @Resource
    private LogService logService;

    @GetMapping("/list")
    public String list(ModelMap model) {
        return "verify/list";
    }

    @GetMapping("/detail")
    public String detail(ModelMap model, @RequestParam("id") String id) {
        model.put("mapping", mappingService.getMapping(id, 1));
        return "verify/detail";
    }

    @PostMapping("/search")
    @ResponseBody
    public RestResult search(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(dataVerifyService.searchIncrementalMappings(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/start")
    @ResponseBody
    public RestResult start(@RequestParam("id") String id, @RequestParam(value = "verifyType", defaultValue = "count") String verifyType) {
        try {
            logService.log(LogType.SystemLog.INFO, "启动数据验证, 驱动ID:%s, 类型:%s", id, verifyType);
            return RestResult.restSuccess(dataVerifyService.startVerify(id, verifyType));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/stop")
    @ResponseBody
    public RestResult stop(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(dataVerifyService.stopVerify(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/progress")
    @ResponseBody
    public RestResult progress(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(dataVerifyService.getProgress(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/history")
    @ResponseBody
    public RestResult history(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(dataVerifyService.getHistory(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/delete")
    @ResponseBody
    public RestResult delete(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(dataVerifyService.deleteHistory(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }
}
