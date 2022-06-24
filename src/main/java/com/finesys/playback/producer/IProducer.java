package com.finesys.playback.producer;

import com.finesys.playback.EsReaderException;

import java.io.IOException;
import java.util.List;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 16:39</p>
 */
public interface IProducer<T> {
    boolean hasNext();
    List<T> next() throws EsReaderException;

    void setPage(final int page);
    void setSize(final int size);
}
