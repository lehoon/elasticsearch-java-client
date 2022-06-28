package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.finesys.elasticsearch.client.client.EsDepthMutiTypeClient;
import com.finesys.elasticsearch.client.domain.DepthModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 17:26</p>
 */
public class EsPlaybackProducer02 extends AbstractProducer {
    public EsPlaybackProducer02(ElasticsearchClient elasticsearchClient) {
        super(elasticsearchClient);
    }

    public void init(String symbol, String market, String [] types,
                     String beginTime, String endTime) throws IOException {
        init(indexName, symbol, market, types, beginTime, endTime);
    }

    public void init(String indexName, String symbol, String market, String [] types,
                     String beginTime, String endTime) throws IOException {
        esDepthClient = new EsDepthMutiTypeClient(indexName, symbol, market, types, beginTime, endTime, elasticsearchClient);
        dataTotalCount = esDepthClient.count();
        this.init = true;
    }

    public String[] types() {
        return ((EsDepthMutiTypeClient) esDepthClient).getTypes();
    }

    public List<DepthModel> lastDepthOfDay(final String day) throws IOException{
        List<DepthModel> depthList = new ArrayList<DepthModel>(types().length);
        for (String type : types()) {
            depthList.add(esDepthClient.searchLastOfToDay(type, day));
        }

        return depthList;
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
                .append(types())
                .append(",")
                .append(beginTime())
                .append(",")
                .append(endTime());
        return sb.toString();
    }
}
