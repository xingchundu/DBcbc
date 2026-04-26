/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.vo.ConnectorVO;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.web.controller.BaseController;
import org.dbsyncer.web.service.DdlMigrationService;
import org.dbsyncer.web.support.ddl.DdlMigrationLogCapture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DDL 结构迁移页面（数据源来自连接管理）
 */
@Controller
@RequestMapping("/ddl")
public class DdlMigrationController extends BaseController {

    private static final String MYSQL = "MySQL";
    private static final String POSTGRESQL = "PostgreSQL";
    private static final String ORACLE = "Oracle";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @Resource
    private DdlMigrationService ddlMigrationService;

    @GetMapping("/migration")
    public String page(ModelMap model) {
        List<ConnectorVO> ddlConnectors =
                connectorService.getConnectorAll().stream().filter(this::isDdlRdbmsConnector).collect(Collectors.toList());
        model.put("ddlConnectors", ddlConnectors);
        return "ddl/migration";
    }

    @PostMapping("/transfer")
    @ResponseBody
    public RestResult transfer(HttpServletRequest request) {
        Map<String, String> params = getParams(request);
        String sourceId = params.get("sourceConnectorId");
        String targetId = params.get("targetConnectorId");
        if (sourceId == null || targetId == null || sourceId.trim().isEmpty() || targetId.trim().isEmpty()) {
            return RestResult.restFail("请选择源连接与目标连接");
        }
        if (sourceId.equals(targetId)) {
            return RestResult.restFail("源连接与目标连接不能相同");
        }
        final String src = sourceId.trim();
        final String tgt = targetId.trim();
        DdlMigrationLogCapture.CaptureResult cap =
                DdlMigrationLogCapture.runCapturing(() -> ddlMigrationService.transfer(src, tgt));
        List<Map<String, String>> logs = new java.util.ArrayList<>(cap.getLogs());
        if (cap.getError() != null) {
            Throwable e = cap.getError();
            logger.error(e.getMessage(), e);
            Map<String, String> errRow = new LinkedHashMap<>(2);
            errRow.put("level", "ERROR");
            errRow.put("message", (e.getMessage() != null ? e.getMessage() : e.getClass().getName()) + "（请求未完全成功，请查看上方日志）");
            logs.add(errRow);
            Map<String, Object> data = new HashMap<>(2);
            data.put("logs", logs);
            RestResult r = RestResult.restFail(e.getMessage() != null ? e.getMessage() : "迁移失败");
            r.setData(data);
            return r;
        }
        Map<String, Object> data = new HashMap<>(2);
        data.put("logs", logs);
        RestResult r = RestResult.restSuccess(data);
        r.setMessage("DDL 结构迁移已执行完成");
        return r;
    }

    /**
     * 对当前选择的源/目标连接器重新从数据源拉取库列表并持久化，便于 DDL 新建库后在驱动管理等页面选到新库、再拉表。
     */
    @PostMapping("/refreshConnectorMeta")
    @ResponseBody
    public RestResult refreshConnectorMeta(HttpServletRequest request) {
        Map<String, String> params = getParams(request);
        String sourceId = trimToNull(params.get("sourceConnectorId"));
        String targetId = trimToNull(params.get("targetConnectorId"));
        Set<String> ids = new LinkedHashSet<>();
        if (sourceId != null) {
            ids.add(sourceId);
        }
        if (targetId != null) {
            ids.add(targetId);
        }
        if (ids.isEmpty()) {
            return RestResult.restFail("请至少选择源连接或目标连接");
        }
        List<String> parts = new ArrayList<>();
        try {
            for (String id : ids) {
                parts.add(connectorService.refreshConnectorDatabases(id));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return RestResult.restFail(e.getMessage() != null ? e.getMessage() : "刷新失败");
        }
        String msg = String.join("；", parts);
        RestResult r = RestResult.restSuccess(msg);
        r.setMessage(msg);
        return r;
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private boolean isDdlRdbmsConnector(ConnectorVO vo) {
        ConnectorConfig cfg = vo.getConfig();
        if (!(cfg instanceof DatabaseConfig)) {
            return false;
        }
        String t = cfg.getConnectorType();
        return MYSQL.equals(t) || POSTGRESQL.equals(t) || ORACLE.equals(t);
    }
}
