/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.web.controller.index;

import database.ddl.transfer.bean.MigrationSummary;
import database.ddl.transfer.bean.ObjectCounts;
import database.ddl.transfer.object.DbObjectType;
import database.ddl.transfer.object.ObjectMigrateItem;
import database.ddl.transfer.object.ObjectMigrateResult;

import org.dbcbc.biz.ConnectorService;
import org.dbcbc.biz.vo.ConnectorVO;
import org.dbcbc.biz.vo.RestResult;
import org.dbcbc.sdk.config.DatabaseConfig;
import org.dbcbc.sdk.model.ConnectorConfig;
import org.dbcbc.web.controller.BaseController;
import org.dbcbc.web.service.DdlMigrationService;
import org.dbcbc.web.support.ddl.DdlMigrationLogCapture;

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
 * DDL 迁移页面控制器
 * <p>支持两阶段迁移：① 表结构迁移 → ② 数据库对象迁移</p>
 * <p>支持迁移路径：Oracle→PG、Oracle→DM、Oracle→MySQL、SqlServer→PG</p>
 */
@Controller
@RequestMapping("/ddl")
public class DdlMigrationController extends BaseController {

    private static final String MYSQL      = "MySQL";
    private static final String POSTGRESQL = "PostgreSQL";
    private static final String ORACLE     = "Oracle";
    private static final String DM         = "DM";
    private static final String SQLSERVER  = "SqlServer";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @Resource
    private DdlMigrationService ddlMigrationService;

    // ─────────────────────────────────────────────────────────────
    // GET /ddl/migration — 页面入口
    // ─────────────────────────────────────────────────────────────

    @GetMapping("/migration")
    public String page(ModelMap model) {
        List<ConnectorVO> ddlConnectors = connectorService.getConnectorAll()
                .stream().filter(this::isDdlRdbmsConnector).collect(Collectors.toList());
        model.put("ddlConnectors", ddlConnectors);
        return "ddl/migration";
    }

    // ─────────────────────────────────────────────────────────────
    // POST /ddl/transfer — 阶段一：迁移表结构（保持原有接口不变）
    // ─────────────────────────────────────────────────────────────

    @PostMapping("/transfer")
    @ResponseBody
    public RestResult transfer(HttpServletRequest request) {
        Map<String, String> params = getParams(request);
        String sourceId = params.get("sourceConnectorId");
        String targetId = params.get("targetConnectorId");
        if (isBlank(sourceId) || isBlank(targetId)) {
            return RestResult.restFail("请选择源连接与目标连接");
        }
        if (sourceId.trim().equals(targetId.trim())) {
            return RestResult.restFail("源连接与目标连接不能相同");
        }
        final String src = sourceId.trim();
        final String tgt = targetId.trim();
        final MigrationSummary[] holder = new MigrationSummary[1];
        DdlMigrationLogCapture.CaptureResult cap =
                DdlMigrationLogCapture.runCapturing(() -> holder[0] = ddlMigrationService.transfer(src, tgt));
        return buildTableResult(cap, holder[0]);
    }

    // ─────────────────────────────────────────────────────────────
    // POST /ddl/transferObjects — 阶段二：迁移数据库对象
    // ─────────────────────────────────────────────────────────────

    @PostMapping("/transferObjects")
    @ResponseBody
    public RestResult transferObjects(HttpServletRequest request) {
        Map<String, String> params = getParams(request);
        String sourceId = params.get("sourceConnectorId");
        String targetId = params.get("targetConnectorId");
        // objectTypes: 逗号分隔的 DbObjectType 名称，如 "PROCEDURE,FUNCTION,VIEW"
        String objectTypes = params.getOrDefault("objectTypes", "");

        if (isBlank(sourceId) || isBlank(targetId)) {
            return RestResult.restFail("请选择源连接与目标连接");
        }
        if (sourceId.trim().equals(targetId.trim())) {
            return RestResult.restFail("源连接与目标连接不能相同");
        }
        if (isBlank(objectTypes)) {
            return RestResult.restFail("请至少选择一种迁移对象类型");
        }
        final String src = sourceId.trim();
        final String tgt = targetId.trim();
        final String objTypes = objectTypes.trim();
        final ObjectMigrateResult[] holder = new ObjectMigrateResult[1];
        DdlMigrationLogCapture.CaptureResult cap =
                DdlMigrationLogCapture.runCapturing(
                        () -> holder[0] = ddlMigrationService.transferObjects(src, tgt, objTypes));
        return buildObjectResult(cap, holder[0]);
    }

    // ─────────────────────────────────────────────────────────────
    // POST /ddl/refreshConnectorMeta — 刷新连接库列表（原有逻辑不变）
    // ─────────────────────────────────────────────────────────────

    @PostMapping("/refreshConnectorMeta")
    @ResponseBody
    public RestResult refreshConnectorMeta(HttpServletRequest request) {
        Map<String, String> params = getParams(request);
        String sourceId = trimToNull(params.get("sourceConnectorId"));
        String targetId = trimToNull(params.get("targetConnectorId"));
        Set<String> ids = new LinkedHashSet<>();
        if (sourceId != null) ids.add(sourceId);
        if (targetId != null) ids.add(targetId);
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

    // ─────────────────────────────────────────────────────────────
    // 内部工具方法
    // ─────────────────────────────────────────────────────────────

    private RestResult buildTableResult(DdlMigrationLogCapture.CaptureResult cap,
                                        MigrationSummary summary) {
        List<Map<String, String>> logs = new ArrayList<>(cap.getLogs());
        if (cap.getError() != null) {
            Throwable e = cap.getError();
            logger.error(e.getMessage(), e);
            Map<String, String> errRow = new LinkedHashMap<>(2);
            errRow.put("level", "ERROR");
            errRow.put("message", (e.getMessage() != null ? e.getMessage() : e.getClass().getName())
                    + "（请求未完全成功，请查看上方日志）");
            logs.add(errRow);
            Map<String, Object> data = new HashMap<>(2);
            data.put("logs", logs);
            RestResult r = RestResult.restFail(e.getMessage() != null ? e.getMessage() : "迁移失败");
            r.setData(data);
            return r;
        }
        Map<String, Object> data = new HashMap<>(3);
        data.put("logs", logs);
        if (summary != null) {
            data.put("summary", toSummaryMap(summary));
        }
        RestResult r = RestResult.restSuccess(data);
        r.setMessage(summary != null ? summary.formatMessage() : "DDL 表结构迁移已执行完成");
        return r;
    }

    private RestResult buildObjectResult(DdlMigrationLogCapture.CaptureResult cap,
                                         ObjectMigrateResult result) {
        List<Map<String, String>> logs = new ArrayList<>(cap.getLogs());
        if (cap.getError() != null) {
            Throwable e = cap.getError();
            logger.error(e.getMessage(), e);
            Map<String, String> errRow = new LinkedHashMap<>(2);
            errRow.put("level", "ERROR");
            errRow.put("message", e.getMessage() != null ? e.getMessage() : e.getClass().getName());
            logs.add(errRow);
        }
        // 追加对象迁移明细日志
        if (result != null) {
            for (ObjectMigrateItem item : result.getItems()) {
                Map<String, String> row = new LinkedHashMap<>(2);
                String level = item.getStatus() == ObjectMigrateItem.Status.FAILED ? "ERROR"
                        : item.getStatus() == ObjectMigrateItem.Status.WARN ? "WARN" : "INFO";
                row.put("level", level);
                String msg = "[" + item.getType().getDisplayName() + "] " + item.getName()
                        + " → " + item.getStatus().name();
                if (item.getMessage() != null && !item.getMessage().isEmpty()) {
                    msg += "：" + item.getMessage();
                }
                row.put("message", msg);
                logs.add(row);
            }
            // 汇总行
            Map<String, String> summaryRow = new LinkedHashMap<>(2);
            summaryRow.put("level", result.getFailedCount() > 0 ? "WARN" : "INFO");
            summaryRow.put("message", result.formatMessage());
            logs.add(summaryRow);
        }
        Map<String, Object> data = new HashMap<>(3);
        data.put("logs", logs);
        if (result != null) {
            data.put("objectResult", toObjectResultMap(result));
        }
        boolean hasError = cap.getError() != null;
        RestResult r = hasError
                ? RestResult.restFail(cap.getError().getMessage() != null
                        ? cap.getError().getMessage() : "对象迁移失败")
                : RestResult.restSuccess(data);
        r.setData(data);
        r.setMessage(result != null ? result.formatMessage() : "对象迁移完成");
        return r;
    }

    private static Map<String, Object> toSummaryMap(MigrationSummary summary) {
        Map<String, Object> map = new LinkedHashMap<>(2);
        map.put("source", toCountsMap(summary.getSource()));
        map.put("target", toCountsMap(summary.getTarget()));
        return map;
    }

    private static Map<String, Integer> toCountsMap(ObjectCounts counts) {
        Map<String, Integer> map = new LinkedHashMap<>(2);
        map.put("tables",  counts.getTableCount());
        map.put("indexes", counts.getIndexCount());
        return map;
    }

    private static Map<String, Object> toObjectResultMap(ObjectMigrateResult r) {
        Map<String, Object> map = new LinkedHashMap<>(5);
        map.put("total",   r.getTotalCount());
        map.put("success", r.getSuccessCount());
        map.put("warn",    r.getWarnCount());
        map.put("failed",  r.getFailedCount());
        map.put("skipped", r.getSkippedCount());
        return map;
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private boolean isDdlRdbmsConnector(ConnectorVO vo) {
        ConnectorConfig cfg = vo.getConfig();
        if (!(cfg instanceof DatabaseConfig)) return false;
        String t = cfg.getConnectorType();
        return MYSQL.equals(t) || POSTGRESQL.equals(t) || ORACLE.equals(t)
                || DM.equals(t) || SQLSERVER.equals(t);
    }
}
