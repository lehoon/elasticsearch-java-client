package com.finesys.playback.task;

import com.finesys.playback.consumer.IConsumer;
import com.finesys.playback.producer.IProducer;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 17:30</p>
 */
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
        if (producer == null || consumer == null) return;

        while (!consumer.workDone()) {
            consumer.process();
        }
    }
}
