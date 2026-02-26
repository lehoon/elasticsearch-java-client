package com.finesys.playback;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 16:39</p>
 */
public class EsReaderException extends Throwable {
    public EsReaderException(String message) {
        super("读取es回放数据失败.");
    }

    public EsReaderException(String message, Throwable cause) {
        super("读取es回放数据失败.", cause);
    }

    public EsReaderException(Throwable cause) {
        super("读取es回放数据失败.", cause);
    }
}
