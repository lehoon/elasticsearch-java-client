package com.finesys.playback.consumer;

import com.finesys.playback.producer.IProducer;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 16:40</p>
 */
public interface IConsumer<T> {
    void producer(IProducer<T> producer);
    void process();
    boolean workDone();
}

