package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.finesys.elasticsearch.client.client.AbstractEsDepthClient;
import com.finesys.elasticsearch.client.domain.DepthModel;
import com.finesys.elasticsearch.client.domain.EsDataModel;
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
    protected volatile int page = 0;
    protected int size = 100;
    protected long readIndex = 0l;
    protected long dataTotalCount = 0l;
    protected ElasticsearchClient elasticsearchClient;
    protected AbstractEsDepthClient esDepthClient = null;

    public AbstractProducer(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public void send(final EsDataModel dataModel) throws EsWriteException {
        try {
            esDepthClient.pushData(dataModel);
        } catch (IOException e) {
            throw new EsWriteException(e);
        }
    }

    public void sendBatch(List<EsDataModel<DepthModel>> depthList) throws EsWriteException {
        for (EsDataModel depth : depthList) {
            send(depth);
        }
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
        return readIndex < dataTotalCount;
    }

    @Override
    public void setPage(int page) {
        this.page = page;
    }

    @Override
    public void setSize(int size) {
        this.size = size;
    }
}
