package com.finesys.playback.producer;

import com.finesys.elasticsearch.client.domain.DepthModel;
import com.finesys.elasticsearch.client.domain.EsDataModel;
import com.finesys.playback.EsReaderException;
import com.finesys.playback.EsWriteException;

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
    void setIndexName(final String indexName);
    T lastOfToday(final String type, final String today) throws EsReaderException;
    void send(String id, String symbol, String market, String type, String time, String data) throws EsWriteException;
    void send(String id, String symbol, String market, String []types, String time, String data) throws EsWriteException;
    void send(final EsDataModel dataModel) throws EsWriteException;
    void send(List<EsDataModel<DepthModel>> depthList) throws EsWriteException;

    //get
    int page();
    int size();
    String symbol();
    String market();
    String beginTime();
    String endTime();
    String request();
    String indexName();
    boolean init();
}