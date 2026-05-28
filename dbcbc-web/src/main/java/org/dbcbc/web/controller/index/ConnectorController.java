package org.dbcbc.web.controller.index;

import org.dbcbc.biz.ConnectorService;
import org.dbcbc.biz.checker.impl.connector.ConnectorChecker;
import org.dbcbc.biz.vo.RestResult;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.parser.model.Connector;
import org.dbcbc.web.controller.BaseController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/connector")
public class ConnectorController extends BaseController {

    private static final List<String> ADD_CONNECTOR_TYPES = Arrays.asList(
            "DM", "MySQL", "PostgreSQL", "Oracle", "SqlServer", "File");

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @GetMapping("/list")
    public String pageList(HttpServletRequest request, ModelMap model) {
        return "connector/list";
    }

    @GetMapping("/page/add")
    public String pageAdd(HttpServletRequest request, ModelMap model) {
        List<String> registeredTypes = connectorService.getConnectorTypeAll();
        List<String> connectorTypes = ADD_CONNECTOR_TYPES.stream()
                .filter(registeredTypes::contains)
                .collect(Collectors.toList());
        model.put("connectorTypes", connectorTypes);
        return "connector/add";
    }

    @GetMapping("/page/add{page}")
    public String page(HttpServletRequest request, ModelMap model, @PathVariable("page") String page) {
        return "connector/add" + page;
    }

    @GetMapping("/page/edit")
    public String pageEdit(HttpServletRequest request, ModelMap model, String id) {
        model.put("connector", connectorService.getConnector(id));
        return "connector/edit";
    }

    @PostMapping("/search")
    @ResponseBody
    public RestResult search(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(connectorService.search(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/refreshStatus")
    @ResponseBody
    public RestResult refreshStatus() {
        try {
            connectorService.refreshHealth();
            return RestResult.restSuccess("连接状态已刷新");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/copy")
    @ResponseBody
    public RestResult add(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(connectorService.copy(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            String id = connectorService.add(params);
            Connector connector = connectorService.getConnector(id);
            String connectionError = ConnectorChecker.getConnectionError(connector);
            Map<String, Object> data = new HashMap<>(2);
            data.put("id", id);
            if (StringUtil.isNotBlank(connectionError)) {
                data.put("connectionError", connectionError);
            }
            return RestResult.restSuccess(data);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(connectorService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/get")
    @ResponseBody
    public RestResult get(HttpServletRequest request, String id) {
        try {
            return RestResult.restSuccess(connectorService.getConnector(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(HttpServletRequest request, @RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(connectorService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping(value = "/getPosition")
    @ResponseBody
    public RestResult getPosition(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(connectorService.getPosition(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/test")
    @ResponseBody
    public RestResult test(@RequestParam(value = "id") String id) {
        try {
            String error = connectorService.testConnection(id);
            if (error == null) {
                return RestResult.restSuccess("连接测试成功");
            }
            return RestResult.restFail(error);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    @PostMapping("/getDatabase")
    @ResponseBody
    public RestResult getDatabase(String id) {
        try {
            return RestResult.restSuccess(connectorService.getDatabase(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/getSchema")
    @ResponseBody
    public RestResult getSchema(String id, String database) {
        try {
            return RestResult.restSuccess(connectorService.getSchema(id, database));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }
}
