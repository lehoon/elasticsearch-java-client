package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.finesys.elasticsearch.client.client.AbstractEsDepthClient;
import com.finesys.elasticsearch.client.domain.DepthModel;
import com.finesys.elasticsearch.client.domain.EsDataModel;
import com.finesys.elasticsearch.client.utils.DateUtils;
import com.finesys.playback.EsReaderException;
import com.finesys.playback.EsWriteException;

import java.io.IOException;
import java.util.List;

/**
 * <p>Title: </p>
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
        try {
            return esDepthClient.searchPage(page, size);
        } catch (IOException e) {
            throw new EsReaderException(e);
        }
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
        try {
            return esDepthClient.searchLastOfToDay(type, today);
        } catch (IOException e) {
            throw new EsReaderException("读取最后一条行情数据异常", e);
        }
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
    public void send(String id, String symbol, String market, String type, String time, String data) throws EsWriteException {
        DepthModel depthModel = new DepthModel();
        depthModel.setSymbol(symbol);
        depthModel.setMarket(market);
        depthModel.setTime(time);
        depthModel.setData(data);
        depthModel.setType(type);
        depthModel.setCreated(DateUtils.formatDepthDate(depthModel.getTime()));

        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.operations(op -> op
                .index(idx -> idx
                        .index(indexName)
                        .document(depthModel)
                        .id(id)));

        try {
            BulkResponse response = elasticsearchClient.bulk(builder.build());
            if (response.errors()) {
                throw new EsWriteException(String.format("写入行情数据失败,[%s]", response.toString()));
            }
        } catch (ElasticsearchException e) {
            throw new EsWriteException("写入行情数据失败", e);
        } catch (IOException e) {
            throw new EsWriteException("写入行情数据失败", e);
        }
    }

    @Override
    public void send(String id, String symbol, String market, String[] types, String time, String data) throws EsWriteException {
        for (String type : types) {
            send(id, symbol, market, type, time, data);
        }
    }

    @Override
    public void send(final EsDataModel dataModel) throws EsWriteException {
        try {
            esDepthClient.pushData(dataModel);
        } catch (IOException e) {
            throw new EsWriteException(e);
        }
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
}
