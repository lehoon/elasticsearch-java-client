package com.finesys.playback;

import com.finesys.playback.consumer.IConsumer;
import com.finesys.playback.producer.IProducer;
import com.finesys.playback.task.DefaultDataPlaybackTask;

import java.util.concurrent.*;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 16:38</p>
 */
public final class FinesysDataPlayBack {
    private ExecutorService playBackThreadPool = null;

    private static FinesysDataPlayBack instance = new FinesysDataPlayBack();

    private FinesysDataPlayBack() {}

    public static FinesysDataPlayBack getInstance() {
        return instance;
    }

    public boolean submitTask(IProducer producer, IConsumer consumer) {
        if (playBackThreadPool == null) return false;
        if (producer == null) return false;
        if (consumer == null) return false;

        try {
            playBackThreadPool.submit(new DefaultDataPlaybackTask(producer, consumer));
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    public void setPlayBackThreadPool(ExecutorService playBackThreadPool) {
        this.playBackThreadPool = playBackThreadPool;
    }
}
