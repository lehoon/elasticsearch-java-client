package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.finesys.elasticsearch.client.client.EsDepthClient;
import com.finesys.elasticsearch.client.domain.DepthModel;

import java.io.IOException;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 16:40</p>
 */
public class EsPlaybackProducer01 extends AbstractProducer {
    public EsPlaybackProducer01(ElasticsearchClient elasticsearchClient) {
        super(elasticsearchClient);
    }

    public void init(String symbol, String market, String type,
                     String beginTime, String endTime) throws IOException {
        init(indexName, symbol, market, type, beginTime, endTime);
    }

    public void init(String indexName, String symbol, String market, String type,
                     String beginTime, String endTime) throws IOException {
        esDepthClient = new EsDepthClient(indexName, symbol, market, type, beginTime, endTime, elasticsearchClient);
        dataTotalCount = esDepthClient.count();
        this.init = true;
    }

    public String type() {
        return ((EsDepthClient) esDepthClient).getType();
    }

    public DepthModel lastDepthOfDay(final String day) throws IOException {
        return esDepthClient.searchLastOfToDay(type(), day);
    }

    @Override
    public String symbol() {
        return esDepthClient.symbol();
    }

    @Override
    public String market() {
        return esDepthClient.market();
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
                .append(symbol())
                .append(",")
                .append(market())
                .append(",")
                .append(type())
                .append(",")
                .append(beginTime())
                .append(",")
                .append(endTime());
        return sb.toString();
    }
}