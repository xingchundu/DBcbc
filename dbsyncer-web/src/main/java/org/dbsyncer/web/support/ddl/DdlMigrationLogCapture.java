/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.web.support.ddl;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 在单次 DDL 迁移执行期间挂载到 Root Logger，收集 {@code database.ddl.transfer} 包下的日志供页面展示。
 */
public final class DdlMigrationLogCapture extends AbstractAppender {

    private static final String LOGGER_PREFIX = "database.ddl.transfer";
    private static final ReentrantLock LOCK = new ReentrantLock();
    private static final int MAX_STACK_LINES = 48;

    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private final List<Map<String, String>> lines = new ArrayList<>();

    private DdlMigrationLogCapture() {
        super("DdlMigrationMemoryAppender", null,
                PatternLayout.newBuilder().withPattern("%m%n").build(), false, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
        String loggerName = event.getLoggerName();
        if (loggerName == null || !loggerName.startsWith(LOGGER_PREFIX)) {
            return;
        }
        String level = event.getLevel().name();
        StringBuilder text = new StringBuilder();
        text.append(TIME_FMT.format(Instant.ofEpochMilli(event.getTimeMillis()))).append(" [").append(level).append("] ");
        text.append(event.getMessage().getFormattedMessage());
        if (event.getThrownProxy() != null) {
            Throwable t = event.getThrownProxy().getThrowable();
            if (t != null) {
                text.append('\n').append(t.toString());
                StackTraceElement[] st = t.getStackTrace();
                int n = Math.min(st.length, MAX_STACK_LINES);
                for (int i = 0; i < n; i++) {
                    text.append('\n').append("    at ").append(st[i]);
                }
                if (st.length > MAX_STACK_LINES) {
                    text.append("\n    ... (").append(st.length - MAX_STACK_LINES).append(" more)");
                }
            }
        }
        Map<String, String> row = new LinkedHashMap<>(2);
        row.put("level", level);
        row.put("message", text.toString());
        synchronized (lines) {
            lines.add(row);
        }
    }

    /**
     * 执行任务并收集 DDL 模块日志；异常不会向上抛出，而是通过 {@link CaptureResult#getError()} 返回。
     */
    public static CaptureResult runCapturing(ThrowableRunnable task) {
        LOCK.lock();
        DdlMigrationLogCapture appender = new DdlMigrationLogCapture();
        appender.start();
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration cfg = ctx.getConfiguration();
        LoggerConfig root = cfg.getRootLogger();
        root.addAppender(appender, Level.DEBUG, null);
        ctx.updateLoggers();
        Throwable error = null;
        try {
            task.run();
        } catch (Throwable t) {
            error = t;
        } finally {
            root.removeAppender(appender.getName());
            appender.stop();
            ctx.updateLoggers();
            LOCK.unlock();
        }
        List<Map<String, String>> snapshot;
        synchronized (appender.lines) {
            snapshot = Collections.unmodifiableList(new ArrayList<>(appender.lines));
        }
        return new CaptureResult(snapshot, error);
    }

    @FunctionalInterface
    public interface ThrowableRunnable {
        void run() throws Throwable;
    }

    public static final class CaptureResult {
        private final List<Map<String, String>> logs;
        private final Throwable error;

        private CaptureResult(List<Map<String, String>> logs, Throwable error) {
            this.logs = logs;
            this.error = error;
        }

        public List<Map<String, String>> getLogs() {
            return logs;
        }

        public Throwable getError() {
            return error;
        }
    }
}
