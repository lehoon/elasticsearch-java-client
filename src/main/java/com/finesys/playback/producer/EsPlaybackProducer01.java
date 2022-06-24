package com.finesys.playback.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.finesys.elasticsearch.client.client.EsDepthClient;

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

    public void init(String indexName, String symbol, String market, String type,
                     String beginTime, String endTime) throws IOException {
        esDepthClient = new EsDepthClient(indexName, symbol, market, type, beginTime, endTime, elasticsearchClient);
        dataTotalCount = esDepthClient.count();
    }
}