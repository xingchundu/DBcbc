/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.manager.impl;

import org.dbcbc.common.util.NumberUtil;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.manager.AbstractPuller;
import org.dbcbc.parser.LogService;
import org.dbcbc.parser.LogType;
import org.dbcbc.parser.ParserComponent;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.enums.ParserEnum;
import org.dbcbc.parser.event.FullRefreshEvent;
import org.dbcbc.parser.model.Mapping;
import org.dbcbc.parser.model.Meta;
import org.dbcbc.parser.model.TableGroup;
import org.dbcbc.parser.model.Task;
import org.dbcbc.sdk.util.PrimaryKeyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 全量同步
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2020-04-26 15:28
 */
@Component
public final class FullPuller extends AbstractPuller implements ApplicationListener<FullRefreshEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ParserComponent parserComponent;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    private final Map<String, Task> map = new ConcurrentHashMap<>();

    @Override
    public void start(Mapping mapping) {
        List<TableGroup> list = profileComponent.getSortedTableGroupAll(mapping.getId());
        Assert.notEmpty(list, "映射关系不能为空");
        Thread worker = new Thread(()-> {
            final String metaId = mapping.getMetaId();
            ExecutorService executor = Executors.newFixedThreadPool(mapping.getThreadNum());
            try {
                Task task = map.computeIfAbsent(metaId, k->new Task(metaId));
                logger.info("开始全量同步：{}, {}", metaId, mapping.getName());
                doTask(task, mapping, list, executor);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                logService.log(LogType.SystemLog.ERROR, e.getMessage());
            } finally {
                try {
                    executor.shutdown();
                } catch (Exception e) {
                    logService.log(LogType.SystemLog.ERROR, e.getMessage());
                }
                map.remove(metaId);
                publishClosedEvent(metaId);
                logger.info("结束全量同步：{}, {}", metaId, mapping.getName());
            }
        });
        worker.setName("full-worker-" + mapping.getId());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public void close(String metaId) {
        map.computeIfPresent(metaId, (k, task)-> {
            task.stop();
            return null;
        });
    }

    @Override
    public void onApplicationEvent(FullRefreshEvent event) {
        // 异步监听任务刷新事件
        flush(event.getTask());
    }

    private void doTask(Task task, Mapping mapping, List<TableGroup> list, Executor executor) {
        // 记录开始时间
        long now = Instant.now().toEpochMilli();
        task.setBeginTime(now);
        task.setEndTime(now);

        // 获取上次同步点
        Meta meta = profileComponent.getMeta(task.getId());
        Map<String, String> snapshot = meta.getSnapshot();
        task.setPageIndex(NumberUtil.toInt(snapshot.get(ParserEnum.PAGE_INDEX.getCode()), ParserEnum.PAGE_INDEX.getDefaultValue()));
        // 反序列化游标值类型(通常为数字或字符串类型)
        task.setCursors(PrimaryKeyUtil.getLastCursors(snapshot.get(ParserEnum.CURSOR.getCode())));
        task.setTableGroupIndex(NumberUtil.toInt(snapshot.get(ParserEnum.TABLE_GROUP_INDEX.getCode()), ParserEnum.TABLE_GROUP_INDEX.getDefaultValue()));
        flush(task);

        int i = task.getTableGroupIndex();
        int maxRetries = 3;
        long[] delays = {5000, 10000, 20000};
        while (i < list.size()) {
            TableGroup tableGroup = list.get(i);
            boolean success = false;
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    parserComponent.execute(task, mapping, tableGroup, executor);
                    success = true;
                    break;
                } catch (Exception e) {
                    if (attempt < maxRetries) {
                        logger.warn("表{}同步失败，第{}次重试：{}", tableGroup.getSourceTable().getName(), attempt + 1, e.getMessage());
                        logService.log(LogType.TableGroupLog.FULL_FAILED,
                            String.format("表%s同步失败（第%d次重试）：%s", tableGroup.getSourceTable().getName(), attempt + 1, e.getMessage()));
                        try { Thread.sleep(delays[attempt]); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); i = list.size(); break; }
                    } else {
                        logger.error("表{}重试{}次后仍失败", tableGroup.getSourceTable().getName(), maxRetries, e);
                        logService.log(LogType.TableGroupLog.FULL_FAILED,
                            String.format("表%s同步失败（已重试%d次）：%s", tableGroup.getSourceTable().getName(), maxRetries, e.getMessage()));
                    }
                }
            }
            if (!task.isRunning()) {
                break;
            }
            task.setPageIndex(ParserEnum.PAGE_INDEX.getDefaultValue());
            task.setCursors(null);
            task.setTableGroupIndex(++i);
            flush(task);
        }

        // 记录结束时间
        task.setEndTime(Instant.now().toEpochMilli());
        task.setTableGroupIndex(ParserEnum.TABLE_GROUP_INDEX.getDefaultValue());
        flush(task);
    }

    private void flush(Task task) {
        Meta meta = profileComponent.getMeta(task.getId());
        Assert.notNull(meta, "检查meta为空.");

        // 全量的过程中，有新数据则更新总数
        long finished = meta.getSuccess().get() + meta.getFail().get();
        if (meta.getTotal().get() < finished) {
            meta.getTotal().set(finished);
        }

        meta.setBeginTime(task.getBeginTime());
        meta.setEndTime(task.getEndTime());
        meta.setUpdateTime(Instant.now().toEpochMilli());
        Map<String, String> snapshot = meta.getSnapshot();
        snapshot.put(ParserEnum.PAGE_INDEX.getCode(), String.valueOf(task.getPageIndex()));
        snapshot.put(ParserEnum.CURSOR.getCode(), StringUtil.getIfBlank(StringUtil.join(task.getCursors(), StringUtil.COMMA), StringUtil.EMPTY));
        snapshot.put(ParserEnum.TABLE_GROUP_INDEX.getCode(), String.valueOf(task.getTableGroupIndex()));
        profileComponent.editConfigModel(meta);
    }

}