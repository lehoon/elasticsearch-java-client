package com.finesys.elasticsearch.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.finesys.elasticsearch.client.client.EsDepthDefaultClient;
import com.finesys.elasticsearch.client.utils.DateUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

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
        searchClient.testMutilCount();
        System.out.println("写入数据结束============");
        searchClient.shutdown();
    }

    public void testMutilCount() {
        String [] symbols = new String[] {"ag2208", "ag2203", "ag2205"};
        String [] markets = new String[] {"SZ", "SHFE"};
        String [] types = new String[] {"L2"};

        String beginTime = DateUtils.depthPlaybackBeginTime("20220628");
        String endTime = DateUtils.depthPlaybackBeginTime("20220629");
        EsDepthDefaultClient defaultClient = new EsDepthDefaultClient("finesys_depth",
                symbols, markets, types, beginTime, endTime, client);

        try {
            long count = defaultClient.count();
            System.out.println("mutil count quest count is " + count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void shutdown() throws IOException {
        client.shutdown();
        restClient.close();
    }
}
