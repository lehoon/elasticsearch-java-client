package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.finesys.playback.EsReaderException;
import com.finesys.playback.es.client.EsDepthDefaultClient;
import com.finesys.playback.es.domain.DepthModel;

import java.util.Date;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/9/27 16:09</p>
 */
public class EsPlaybackOnceDayProducer extends AbstractProducer {
    private Date beginDate;
    private Date endDate;

    public EsPlaybackOnceDayProducer(ElasticsearchClient elasticsearchClient) {
        super(elasticsearchClient);
    }

    public void init(String [] symbols, String [] markets, String [] types,
                     Date beginTime, Date endTime) throws EsReaderException {
        init(indexName, symbols, markets, types, beginTime, endTime);
    }

    public void init(String indexName, String [] symbols, String [] markets, String [] types,
                     Date beginTime, Date endTime) throws EsReaderException {
        esDepthClient = new EsDepthDefaultClient(indexName, symbols, markets, types, beginTime, endTime, elasticsearchClient);
        setMarketTime(beginTime, endTime);
        this.init = true;
    }

    public void setMarketTime(Date beginTime, Date endTime) throws EsReaderException{
        this.beginDate = beginTime;
        this.endDate = endTime;
        esDepthClient.setMarketTime(beginTime, endTime);
        dataTotalCount = esDepthClient.count();
        rewind();
    }

    public DepthModel searchLastOfToDay(final String market,
                                        final String type,
                                        final String symbol) throws EsReaderException {
        return esDepthClient.searchLastOfToDay(market, type, symbol, beginDate, endDate);
    }

}
