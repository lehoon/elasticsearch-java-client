package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.finesys.playback.EsReaderException;
import com.finesys.playback.es.client.EsDepthDefaultClient;
import com.finesys.playback.es.domain.DepthModel;

import java.util.Date;

/**
 * <p>Title: EsPlaybackDefaultProducer</p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/30 11:06</p>
 */
public class EsPlaybackDefaultProducer extends AbstractProducer {

    public EsPlaybackDefaultProducer(ElasticsearchClient elasticsearchClient) {
        super(elasticsearchClient);
    }

    public void init(String [] symbols, String [] markets, String [] types,
                     Date beginTime, Date endTime) throws EsReaderException {
        init(indexName, symbols, markets, types, beginTime, endTime);
    }

    public void init(String indexName, String [] symbols, String [] markets, String [] types,
                     Date beginTime, Date endTime) throws EsReaderException {
        esDepthClient = new EsDepthDefaultClient(indexName, symbols, markets, types, beginTime, endTime, elasticsearchClient);
        dataTotalCount = esDepthClient.count();
        this.init = true;
    }

    public long searchDepthCount(final String day) throws EsReaderException {
        return esDepthClient.count(day);
    }

    public long searchDepthCount() throws EsReaderException {
        return esDepthClient.count();
    }

    public DepthModel lastDepthOfDay(final String type, final String day) throws EsReaderException {
        return esDepthClient.searchLastOfToDay(type, day);
    }

    public DepthModel lastDepth() throws EsReaderException{
        return esDepthClient.searchLast();
    }
}
