package com.finesys.elasticsearch.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.apama.marketdata.Depth;
import com.apama.marketdata.DepthBuilder;
import com.finesys.elasticsearch.client.domain.DepthModel;
import com.finesys.elasticsearch.client.utils.DateUtils;
import com.finesys.elasticsearch.client.utils.JacksonMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/28 17:54</p>
 */
public class MakeTestDepthData {
    //rest client
    private RestClient restClient = null;
    //es search client
    private ElasticsearchClient client = null;

    //初始化es client对象
    public void init(final String hostName, final int port) throws IOException {
        // Create the low-level client
        restClient = RestClient.builder(
                new HttpHost(hostName, port)).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        client = new ElasticsearchClient(transport);
        System.out.println(client.info().toString());
    }

    public static void main(String args[]) throws IOException {
        MakeTestDepthData searchClient = new MakeTestDepthData();
        //初始化es查询客户端对象
        searchClient.init("192.168.1.58", 9200);
        System.out.println("写入数据开始============");
        searchClient.batchPushDepthData(1000000);
        System.out.println("写入数据结束============");
        searchClient.shutdown();
    }

    /**
     * 写入单条数据
     * @param id
     * @param depthModel
     * @return
     * @throws IOException
     */
    public boolean pushDepthData(final String id, final DepthModel depthModel) throws IOException {
        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.operations(op -> op
                .index(idx -> idx
                        .index("finesys_depth")
                        .document(depthModel)
                        .id(id)));

        BulkResponse response = client.bulk(builder.build());
        return response.errors();
    }

    /**
     * 批量写入数据
     * @param count  数量
     * @throws IOException
     */
    public void batchPushDepthData(final int count) throws IOException {
        JacksonMapper jacksonMapper = new JacksonMapper();
        DepthModel depthModel = new DepthModel();
        DepthBuilder depthBuilder = new DepthBuilder();

        Map<Integer, String> symbolMap = new HashMap<Integer,String>(10);
        symbolMap.put(0, "ag2204");
        symbolMap.put(1, "ag2205");
        symbolMap.put(2, "ag2206");
        symbolMap.put(3, "ag2207");
        symbolMap.put(4, "ag2208");
        symbolMap.put(5, "ag2209");

        Map<Integer, String> marketMap = new HashMap<Integer,String>(10);
        marketMap.put(0, "SH");
        marketMap.put(1, "SZ");
        marketMap.put(2, "SHFE");

        Map<Integer, String> typeMap = new HashMap<Integer,String>(10);
        typeMap.put(0, "L1");
        typeMap.put(0, "L2");

        long successCount = 0;
        final int totalCount = count <= 0 ? 1 : count;
        for (int i = 0; i < totalCount; i++) {
            depthModel.setSymbol(symbolMap.get(i % symbolMap.size()));
            depthModel.setMarket(marketMap.get(i % marketMap.size()));
            depthModel.setType(typeMap.get(i % typeMap.size()));
            depthModel.setCreated(DateUtils.getNow());
            depthModel.setTime(DateUtils.getDepthTime(depthModel.getCreated()));
            Depth depth = depthBuilder.defaultDepth(depthModel.getSymbol()).build();
            depthModel.setData(jacksonMapper.toJson(depth));

            if (pushDepthData(DateUtils.getDepthIdTime(), depthModel)) {
                successCount++;
            }

            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
            }
        }

        System.out.println(String.format("批量写入数据完成, 成功数量[%d],失败数量[%d]", successCount, totalCount - successCount));
    }

    public void shutdown() throws IOException {
        client.shutdown();
        restClient.close();
    }
}
