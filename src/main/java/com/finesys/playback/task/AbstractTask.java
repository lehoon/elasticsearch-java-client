package com.finesys.playback.task;

import java.util.concurrent.Callable;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/9/7 10:37</p>
 */
public interface AbstractTask extends Callable<String> {
    void setTaskId(String taskId);
    String taskId();
}
