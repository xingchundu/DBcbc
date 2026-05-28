/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.parser.ddl.impl;

import com.alibaba.fastjson2.JSON;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.parser.ddl.AlterStrategy;
import org.dbcbc.parser.ddl.DDLParser;
import org.dbcbc.parser.ddl.alter.AddStrategy;
import org.dbcbc.parser.ddl.alter.ChangeStrategy;
import org.dbcbc.parser.ddl.alter.DropStrategy;
import org.dbcbc.parser.ddl.alter.ModifyStrategy;
import org.dbcbc.parser.model.FieldMapping;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.sdk.config.DDLConfig;
import org.dbcbc.sdk.connector.ConnectorInstance;
import org.dbcbc.sdk.connector.database.Database;
import org.dbcbc.sdk.connector.database.DatabaseConnectorInstance;
import org.dbcbc.sdk.enums.DDLOperationEnum;
import org.dbcbc.sdk.model.Field;
import org.dbcbc.sdk.spi.ConnectorService;
import org.dbcbc.sdk.util.SqlParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ddl解析器, 支持类型参考：{@link DDLOperationEnum}
 *
 * @author life
 * @version 1.0.0
 * @date 2023/9/19 22:38
 */
@Component
public class DDLParserImpl implements DDLParser {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<AlterOperation, AlterStrategy> STRATEGIES = new ConcurrentHashMap();

    @PostConstruct
    private void init() {
        STRATEGIES.putIfAbsent(AlterOperation.MODIFY, new ModifyStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.ADD, new AddStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.CHANGE, new ChangeStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.DROP, new DropStrategy());
    }

    @Override
    public DDLConfig parse(ConnectorInstance connectorInstance, ConnectorService connectorService, TableGroup tableGroup, String sql) throws JSQLParserException {
        return parse(connectorInstance, connectorService, tableGroup, sql, null);
    }

    @Override
    public DDLConfig parse(ConnectorInstance connectorInstance, ConnectorService connectorService, TableGroup tableGroup, String sql, String sourceConnectorType) throws JSQLParserException {
        DDLConfig ddlConfig = new DDLConfig();
        logger.info("ddl:{}", sql);
        Statement statement = SqlParserUtil.parse(sql);
        if (statement instanceof Alter && connectorService instanceof Database) {
            Alter alter = (Alter) statement;
            Database database = (Database) connectorService;
            // 跨库DDL：转换列类型
            if (sourceConnectorType != null) {
                String targetConnectorType = connectorService.getConnectorType();
                if (!StringUtil.equals(sourceConnectorType, targetConnectorType)) {
                    convertColumnTypes(alter, sourceConnectorType, targetConnectorType);
                }
            }
            // 替换成目标表名
            String newTableName = database.buildWithQuotation(tableGroup.getTargetTable().getName());
            alter.getTable().setName(newTableName);
            // 替换目标数据库名
            sql = database.buildAlterCatalog((DatabaseConnectorInstance) connectorInstance, alter);
            ddlConfig.setSql(sql);
            for (AlterExpression expression : alter.getAlterExpressions()) {
                STRATEGIES.computeIfPresent(expression.getOperation(), (k, strategy)-> {
                    strategy.parse(expression, ddlConfig);
                    return strategy;
                });
            }
        }
        return ddlConfig;
    }

    @Override
    public void refreshFiledMappings(TableGroup tableGroup, DDLConfig targetDDLConfig) {
        switch (targetDDLConfig.getDdlOperationEnum()) {
            case ALTER_MODIFY:
                updateFieldMapping(tableGroup, targetDDLConfig.getModifiedFieldNames());
                break;
            case ALTER_ADD:
                appendFieldMappings(tableGroup, targetDDLConfig.getAddedFieldNames());
                break;
            case ALTER_CHANGE:
                renameFieldMapping(tableGroup, targetDDLConfig.getChangedFieldNames());
                break;
            case ALTER_DROP:
                removeFieldMappings(tableGroup, targetDDLConfig.getDroppedFieldNames());
                break;
            default:
                break;
        }
    }

    private void updateFieldMapping(TableGroup tableGroup, List<String> modifiedFieldNames) {
        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed->filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed->filed));
        for (FieldMapping fieldMapping : tableGroup.getFieldMapping()) {
            Field source = fieldMapping.getSource();
            Field target = fieldMapping.getTarget();
            // 支持1对多场景
            if (source != null) {
                String modifiedName = source.getName();
                if (!modifiedFieldNames.contains(modifiedName)) {
                    continue;
                }
                sourceFiledMap.computeIfPresent(modifiedName, (k, field)-> {
                    fieldMapping.setSource(field);
                    return field;
                });
                if (target != null && StringUtil.equals(modifiedName, target.getName())) {
                    targetFiledMap.computeIfPresent(modifiedName, (k, field)-> {
                        fieldMapping.setTarget(field);
                        return field;
                    });
                }
            }
        }
    }

    private void renameFieldMapping(TableGroup tableGroup, Map<String, String> changedFieldNames) {
        Set<String> oldNames = changedFieldNames.keySet();
        for (FieldMapping fieldMapping : tableGroup.getFieldMapping()) {
            Field source = fieldMapping.getSource();
            Field target = fieldMapping.getTarget();
            // 支持1对多场景
            if (source != null) {
                String oldFieldName = source.getName();
                if (!oldNames.contains(oldFieldName)) {
                    continue;
                }
                changedFieldNames.computeIfPresent(oldFieldName, (k, newName)-> {
                    source.setName(newName);
                    if (target != null && StringUtil.equals(oldFieldName, target.getName())) {
                        target.setName(newName);
                    }
                    return newName;
                });
            }
        }
    }

    private void removeFieldMappings(TableGroup tableGroup, List<String> removeFieldNames) {
        Iterator<FieldMapping> iterator = tableGroup.getFieldMapping().iterator();
        while (iterator.hasNext()) {
            FieldMapping fieldMapping = iterator.next();
            Field source = fieldMapping.getSource();
            if (source != null && removeFieldNames.contains(source.getName())) {
                iterator.remove();
            }
        }
    }

    private void appendFieldMappings(TableGroup tableGroup, List<String> addedFieldNames) {
        List<FieldMapping> fieldMappings = tableGroup.getFieldMapping();
        Iterator<String> iterator = addedFieldNames.iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            for (FieldMapping fieldMapping : fieldMappings) {
                Field source = fieldMapping.getSource();
                Field target = fieldMapping.getTarget();
                // 检查重复字段
                if (source != null && target != null && StringUtil.equals(source.getName(), name) && StringUtil.equals(target.getName(), name)) {
                    iterator.remove();
                }
            }
        }
        if (CollectionUtils.isEmpty(addedFieldNames)) {
            return;
        }

        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed->filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed->filed));
        if (CollectionUtils.isEmpty(sourceFiledMap) || CollectionUtils.isEmpty(targetFiledMap)) {
            return;
        }
        addedFieldNames.forEach(newFieldName-> {
            if (sourceFiledMap.containsKey(newFieldName) && targetFiledMap.containsKey(newFieldName)) {
                fieldMappings.add(new FieldMapping(sourceFiledMap.get(newFieldName), targetFiledMap.get(newFieldName)));
            }
        });
    }

    // ==================== 跨库DDL类型转换 ====================

    private static volatile Map<String, Map<String, String>> typeMappingCache;

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, String>> loadTypeMapping() {
        if (typeMappingCache == null) {
            synchronized (DDLParserImpl.class) {
                if (typeMappingCache == null) {
                    try {
                        ClassLoader cl = Thread.currentThread().getContextClassLoader();
                        if (cl == null) cl = DDLParserImpl.class.getClassLoader();
                        try (InputStream in = cl.getResourceAsStream("TypeMapping.json")) {
                            if (in != null) {
                                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                byte[] buf = new byte[4096];
                                int len;
                                while ((len = in.read(buf)) != -1) {
                                    bos.write(buf, 0, len);
                                }
                                String json = bos.toString("UTF-8");
                                typeMappingCache = JSON.parseObject(json, Map.class);
                            }
                        }
                    } catch (Exception e) {
                        LoggerFactory.getLogger(DDLParserImpl.class).warn("加载TypeMapping.json失败:{}", e.getMessage());
                    }
                    if (typeMappingCache == null) {
                        typeMappingCache = new ConcurrentHashMap<>();
                    }
                }
            }
        }
        return typeMappingCache;
    }

    private static String normalizeConnectorType(String connectorType) {
        if (connectorType == null) return "";
        switch (connectorType) {
            case "DM": return "dm";
            case "PostgreSQL": return "pg";
            case "MySQL": return "mysql";
            case "Oracle": return "oracle";
            case "SqlServer": return "sqlserver";
            default: return connectorType.toLowerCase();
        }
    }

    private void convertColumnTypes(Alter alter, String sourceConnectorType, String targetConnectorType) {
        String srcKey = normalizeConnectorType(sourceConnectorType);
        String tgtKey = normalizeConnectorType(targetConnectorType);
        String routeKey = srcKey + "2" + tgtKey;

        Map<String, Map<String, String>> allMappings = loadTypeMapping();
        Map<String, String> mapping = allMappings.get(routeKey);
        if (mapping == null || mapping.isEmpty()) {
            return;
        }

        for (AlterExpression expression : alter.getAlterExpressions()) {
            if (expression.getColDataTypeList() == null) continue;
            for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
                if (columnDataType.getColDataType() == null) continue;
                String srcType = columnDataType.getColDataType().getDataType();
                if (srcType == null) continue;
                String mappedType = mapping.get(srcType.toUpperCase());
                if (mappedType != null) {
                    columnDataType.getColDataType().setDataType(mappedType);
                }
            }
        }
    }

}