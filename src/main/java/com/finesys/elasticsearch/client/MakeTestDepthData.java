package com.finesys.elasticsearch.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.finesys.elasticsearch.client.client.EsDepthDefaultClient;
import com.finesys.elasticsearch.client.domain.DepthModel;
import com.finesys.elasticsearch.client.utils.DateUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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
    public void init() throws IOException {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("", ""));

        // Create the low-level client
        restClient = RestClient.builder(
                new HttpHost("192.168.1.200", 9200))
                .setHttpClientConfigCallback(cb -> cb.setDefaultCredentialsProvider(credentialsProvider))
                .build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        client = new ElasticsearchClient(transport);
        System.out.println(client.ping().value());
        //System.out.println(client.cluster().state().toString());
        System.out.println(client.info().toString());
    }

    public static void main(String args[]) throws IOException {
        MakeTestDepthData searchClient = new MakeTestDepthData();
        //初始化es查询客户端对象
        searchClient.init();
        System.out.println("写入数据开始============");
        searchClient.testMutilCount();
        //searchClient.testFunction();
        System.out.println("写入数据结束============");
        searchClient.shutdown();
    }

    public void testFunction() {
        Function<Integer, Integer> function = i -> i + 1;
        Function<Integer, Integer> function1 = i -> i + 1;
        System.out.println(function.apply(function1.apply(1)));
    }

    public void testMutilCount() throws IOException {
        String [] symbols = new String[] {"AG2112", "000001", "600000"};
        String [] markets = new String[] {"SHFE", "SZ", "SH"};
        String [] types = new String[] {"KLine_1day"};

        Date beginTime = DateUtils.formatDepthPlaybackDate("20211101");
        Date endTime = DateUtils.formatDepthPlaybackDate("20211102");
        EsDepthDefaultClient defaultClient = new EsDepthDefaultClient("finesys_marketdata",
                symbols, markets, types, beginTime, endTime);

        defaultClient.setClient(client);

        try {
            long count = defaultClient.count();
            System.out.println("mutil count quest count is " + count);
        } catch (Exception e) {
            e.printStackTrace();
        }

        int page = 1;
        AtomicLong consumerCount = new AtomicLong(0L);
        while (true) {
            List<DepthModel> dataList = defaultClient.searchPage(page, 9000);
            if (dataList == null || dataList.size() == 0) {
                System.out.println("查询数据为空,退出本次查询" + page);
                break;
            }

            int index = 0;
            for (DepthModel depthModel : dataList) {
                System.out.println(depthModel.getTime() + "<<<<=====>>>>" + depthModel.getCreated().getTime());
            }

            consumerCount.addAndGet(dataList.size());
            page += 1;
        }

        System.out.println("本次一共查询数据条数" + consumerCount.get());


        DepthModel lastDepth = defaultClient.searchLast();
        System.out.println(lastDepth.getTime() + "<<<<==lastDepth===>>>>" + lastDepth.getTime());


        String currentDayTime = DateUtils.playbackCurrentDateTime("20211101");
        String nextDayTime = DateUtils.nextDay(currentDayTime);
        DepthModel lastTodayDepth = defaultClient.searchLastOfToDay("", currentDayTime);
        System.out.println(lastTodayDepth.getTime() + "<<<<==lastTodayDepth===>>>>" + lastTodayDepth.getCreated().getTime());


        System.out.println(String.format("当前任务最后一条当天[%s]行情数据[%s] [%d]",
                currentDayTime,
                lastTodayDepth.toString(),
                lastTodayDepth.getCreated().getTime()));



        System.out.println(Long.MAX_VALUE);
        System.out.println(System.currentTimeMillis());


        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(1623167088500L);
        System.out.println(calendar.get(Calendar.YEAR) + "-" +
                calendar.get(Calendar.MONTH) + "-" +
                calendar.get(Calendar.DAY_OF_MONTH) + " " +
                calendar.get(Calendar.HOUR) + ":" +
                calendar.get(Calendar.MINUTE) + ":" +
                calendar.get(Calendar.SECOND) + ":");
    }


    public void shutdown() throws IOException {
        client.shutdown();
        restClient.close();
    }
}
