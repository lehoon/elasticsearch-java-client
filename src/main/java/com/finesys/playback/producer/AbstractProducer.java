package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.finesys.playback.EsReaderException;
import com.finesys.playback.EsWriteException;
import com.finesys.playback.es.client.AbstractEsDepthClient;
import com.finesys.playback.es.domain.DepthModel;
import com.finesys.playback.es.domain.EsDataModel;
import com.finesys.playback.es.utils.DateUtils;

import java.util.List;

/**
 * <p>Title: AbstractProducer</p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 16:41</p>
 */
public abstract class AbstractProducer implements IProducer<DepthModel> {
    protected boolean init = false;
    protected volatile int page = 0;
    protected int size = 100;
    protected String indexName;
    protected long readIndex = 0L;
    protected long dataTotalCount = 0L;
    protected ElasticsearchClient elasticsearchClient;
    protected AbstractEsDepthClient esDepthClient = null;

    public AbstractProducer(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public List<DepthModel> next() throws EsReaderException {
        page += 1;
        readIndex += size;
        return esDepthClient.searchPage(page, size);
    }

    @Override
    public boolean hasNext() {
        return readIndex <= dataTotalCount;
    }

    @Override
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public void setPage(int page) {
        this.page = page;
    }

    @Override
    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public DepthModel lastOfToday(final String type, final String today) throws EsReaderException {
        return esDepthClient.searchLastOfToDay(type, today);
    }

    @Override
    public int page() {
        return page;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public String indexName() {
        return indexName;
    }

    @Override
    public void rewind() {
        this.page = 0;
        this.readIndex = 0;
    }

    @Override
    public void send(String id, String symbol, String market, String type, String time, String data) throws EsWriteException {
        DepthModel depthModel = new DepthModel();
        depthModel.setSymbol(symbol);
        depthModel.setMarket(market);
        depthModel.setTime(time);
        depthModel.setData(data);
        depthModel.setType(type);
        depthModel.setCreated(DateUtils.formatDepthDate(depthModel.getTime()));
        EsDataModel dataModel = new EsDataModel();
        dataModel.setId(id);
        dataModel.setData(depthModel);
        esDepthClient.pushData(dataModel);
    }

    @Override
    public void send(String id, String symbol, String market, String[] types, String time, String data) throws EsWriteException {
        for (String type : types) {
            send(id, symbol, market, type, time, data);
        }
    }

    @Override
    public void send(final EsDataModel dataModel) throws EsWriteException {
        esDepthClient.pushData(dataModel);
    }

    @Override
    public void send(List<EsDataModel<DepthModel>> depthList) throws EsWriteException {
        for (EsDataModel esData : depthList) {
            send(esData);
        }
    }

    @Override
    public boolean init() {
        return init;
    }

    @Override
    public long totalCount() {
        return dataTotalCount;
    }

    @Override
    public String[] symbols() {
        return esDepthClient.symbol();
    }

    @Override
    public String[] markets() {
        return esDepthClient.market();
    }

    @Override
    public String[] types() {
        return esDepthClient.type();
    }

    @Override
    public String beginTime() {
        return esDepthClient.beginTime();
    }

    @Override
    public String endTime() {
        return esDepthClient.endTime();
    }

    @Override
    public String request() {
        StringBuilder sb = new StringBuilder(128);
        sb.append(indexName)
                .append(",")
                .append(arr2String(esDepthClient.symbol()))
                .append(arr2String(esDepthClient.market()))
                .append(arr2String(esDepthClient.type()))
                .append(beginTime())
                .append(",")
                .append(endTime());
        return sb.toString();
    }


    protected String arr2String(String [] in) {
        if (in == null || in.length == 0) return "";

        StringBuilder sb = new StringBuilder(in.length * 20);
        for (String s : in) {
            sb.append(s).append(",");
        }

        return sb.toString();
    }
}
