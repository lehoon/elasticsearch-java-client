package com.finesys.playback;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.finesys.playback.consumer.IConsumer;
import com.finesys.playback.es.utils.DateUtils;
import com.finesys.playback.producer.EsPlaybackDefaultProducer;
import com.finesys.playback.producer.EsPlaybackOnceDayProducer;
import com.finesys.playback.producer.IProducer;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Logger;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/9/23 10:12</p>
 */
public class PlaybackTest {
    private Logger logger = Logger.getLogger(PlaybackTest.class.getName());


    //rest client
    private RestClient restClient = null;
    //es search client
    private ElasticsearchClient client = null;

    public void init() throws IOException {
        logger.info("======================");
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
        /*ByteBuffer buffer1 = ByteBuffer.allocate(8);
        buffer1.putDouble(100.5);
        buffer1.rewind();
        System.out.println(buffer1.getDouble());*/


    public static void main11111(String args[]) throws IOException {
        byte[] buf = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x34};
        ByteBuffer buffer = ByteBuffer.wrap(buf);
        String memoryByteString = Arrays.toString(buffer.array());
        System.out.println("默认字节序：" + buffer.order().toString() + "," + "内存数据：" + memoryByteString);
        double d = buffer.getDouble();
        System.out.println("double=" + d);
    }

    public static void main(String args[]) throws IOException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(1643731200000L);
        System.out.println(calendar.get(Calendar.YEAR) + "-" +
                calendar.get(Calendar.MONTH) + 1 + "-" +
                calendar.get(Calendar.DAY_OF_MONTH) + " " +
                calendar.get(Calendar.HOUR_OF_DAY) + ":" +
                calendar.get(Calendar.MINUTE) + ":" +
                calendar.get(Calendar.SECOND));

        calendar.setTimeInMillis(1643817599000L);
        System.out.println(calendar.get(Calendar.YEAR) + "-" +
                calendar.get(Calendar.MONTH) + 1 + "-" +
                calendar.get(Calendar.DAY_OF_MONTH) + " " +
                calendar.get(Calendar.HOUR_OF_DAY) + ":" +
                calendar.get(Calendar.MINUTE) + ":" +
                calendar.get(Calendar.SECOND));
        //System.exit(1);


        PlaybackTest playbackTest = new PlaybackTest();
        playbackTest.init();

        String symbols[] = {"000001"};
        String markets[] = {"SZ"};
        String types[] = {"KLine_1day"};
        Date begineTime = DateUtils.getSdfTime("2022-02-01 00:00:00.000");
        Date endTime = DateUtils.getSdfTime("2022-02-15 23:59:59.000");

        IProducer producer = playbackTest.playBackProducer(symbols,
                markets,
                types,
                begineTime,
                endTime);

        IConsumer consumer = new BtpSendDepth2ApamaDefaultConsumer("Cl202203281506018668-1.0-zzh-20220922111836",
                "Cl202203281506018668-1.0-zzh-20220922111836",
                "Cl202203281506018668-1.0-zzh-20220922111836",
                "Cl202203281506018668-1.0-zzh-20220922111836",
                100000);

        ((BtpSendDepth2ApamaDefaultConsumer) consumer).setServerChannel("xxxxxxxxxxxxxxxxxxx");
        ((BtpSendDepth2ApamaDefaultConsumer) consumer).setServerHost("192.168.1.180");
        ((BtpSendDepth2ApamaDefaultConsumer) consumer).setServerPort("14001");
        consumer.producer(producer);

        if (!producer.init()) {
            System.out.println("[sleep task] producer.init失败, 任务未运行退出");
            return;
        }

        if (!consumer.init()) {
            System.out.println("[sleep task] consumer.init失败, 任务未运行退出");
            return;
        }

        consumer.beforeProcess();

        while (!consumer.workDone()) {
            consumer.process();
        }

        consumer.afterProcess();
        playbackTest.shutdown();
    }

    private IProducer playBackProducer(String [] symbols,
                                       String [] markets,
                                       String [] types,
                                       Date beginTime,
                                       Date endTime) {
        if (client == null) return null;

        //IProducer producer = new EsPlaybackDefaultProducer(client);
        IProducer producer = new EsPlaybackOnceDayProducer(client);
        if (producer == null) return producer;

        try {
            producer.setSize(1000);
            producer.setIndexName("finesys_marketdata");
            ((EsPlaybackOnceDayProducer) producer).init(symbols, markets, types, beginTime, endTime);
            return producer;
        } catch (EsReaderException e) {
            System.out.println(e);
        }

        return null;
    }


    public void shutdown() throws IOException {
        client.shutdown();
        restClient.close();
    }
}
