package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.finesys.elasticsearch.client.client.EsDepthClient;
import com.finesys.elasticsearch.client.client.EsDepthMutiTypeClient;

import java.io.IOException;

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

    public void init(String indexName, String symbol, String market, String [] types,
                     String beginTime, String endTime) throws IOException {
        esDepthClient = new EsDepthMutiTypeClient(indexName, symbol, market, types, beginTime, endTime, elasticsearchClient);
        dataTotalCount = esDepthClient.count();
    }
}
