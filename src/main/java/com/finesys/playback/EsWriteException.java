package com.finesys.playback;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 16:39</p>
 */
public class EsWriteException extends Throwable {
    public EsWriteException(String message) {
        super("写入es回放数据失败.");
    }

    public EsWriteException(String message, Throwable cause) {
        super("写入es回放数据失败.", cause);
    }

    public EsWriteException(Throwable cause) {
        super("写入es回放数据失败.", cause);
    }

    public EsWriteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super("写入es回放数据失败.", cause, enableSuppression, writableStackTrace);
    }
}
