package org.dbcbc.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.util.concurrent.RejectedExecutionHandler;

/**
 * @author yjwang
 * @date 2022/4/29 10:27
 */
@Configuration
@ConfigurationProperties(prefix = "dbcbc.web.scheduler")
public class SchedulerConfig implements SchedulingConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerConfig.class);

    /**
     * 工作线程数
     */
    private int poolSize = Runtime.getRuntime().availableProcessors();

    @Override
    public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
        scheduledTaskRegistrar.setTaskScheduler(taskScheduler());
    }

    @Bean(name = "taskScheduler", destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        // 核心线程池大小
        scheduler.setPoolSize(poolSize);
        // 线程名字前缀
        scheduler.setThreadNamePrefix("taskScheduler-");
        // 设置线程池关闭的时候等待所有任务都完成再继续销毁其他的Bean
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        // 设置线程池中任务的等待时间，如果超过这个时候还没有销毁就强制销毁，以确保应用最后能够被关闭，而不是阻塞住
        scheduler.setAwaitTerminationSeconds(30);
        // 线程池满，拒绝策略
        scheduler.setRejectedExecutionHandler(rejectedExecutionHandler());
        return scheduler;
    }

    private RejectedExecutionHandler rejectedExecutionHandler() {
        return (r, executor)-> {
            if (!executor.isShutdown()) {
                logger.warn("调度器队列已满，任务被丢弃，调度器: {}", executor);
            }
        };
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }
}
