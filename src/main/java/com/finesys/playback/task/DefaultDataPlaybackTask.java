package com.finesys.playback.task;

import com.finesys.playback.consumer.IConsumer;
import com.finesys.playback.producer.IProducer;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>Title: DefaultDataPlaybackTask</p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 17:30</p>
 */
@Slf4j
public class DefaultDataPlaybackTask implements Runnable {
    private IProducer producer;
    private IConsumer consumer;

    public DefaultDataPlaybackTask(IProducer producer, IConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
        this.consumer.producer(producer);
    }

    @Override
    public void run() {
        if (producer == null || consumer == null) {
            log.error("producer或consumer为空, 任务未运行退出");
            return;
        }

        if (!producer.init()) {
            log.error("producer.init失败, 任务未运行退出");
            return;
        }

        if (!consumer.init()) {
            log.error("consumer.init失败, 任务未运行退出");
            return;
        }

        consumer.beforeProcess();
        while (!consumer.workDone()) {
            consumer.process();
        }

        consumer.afterProcess();
    }
}
