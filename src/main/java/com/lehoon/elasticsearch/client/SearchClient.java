package com.lehoon.elasticsearch.client;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.apama.marketdata.Depth;
import com.apama.marketdata.DepthBuilder;
import com.lehoon.elasticsearch.client.domain.DepthModel;
import com.lehoon.elasticsearch.client.utils.DateUtils;
import com.lehoon.elasticsearch.client.utils.JacksonMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>Title: es search client</p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: lehoon Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/17 16:41</p>
 */
public class SearchClient {
    //rest client
    private RestClient restClient = null;
    //es search client
    private ElasticsearchClient client = null;
    //search response last _id
    private String lastId;

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
        SearchClient searchClient = new SearchClient();
        //初始化es查询客户端对象
        searchClient.init("192.168.1.58", 9200);
        System.out.println("checkClusterRunning=" + searchClient.checkClusterRunning());
        System.out.println("searchCount=" + searchClient.searchCount("", ""));

        System.out.println("写入数据开始============");
        //searchClient.batchPushDepthData(1000000);
        System.out.println("写入数据结束============");

        //根据symbol查询符合条件的数量
        //long count = searchClient.searchCount("finesys_depth01", "typename");
        //System.out.println("查询符合条件的数量=" + count);
        //根据symbol查询第一条数据
        //searchClient.searchBySymbolFirst("ag2204");

        //分页查询--------------------begin--------------------------
        final String indexName = "finesys_depth01";
        final String symbol = "ag2204";
        final String market = "SH";
        final String type = "KLine_30sec";
        final String beginTime = "2022-06-22 10:58:05.527";
        final String endTime = "2022-06-22 11:58:05.527";
        int page = 0;
        int size = 10;

        searchClient.searchDepathPage(indexName, symbol, market, type, beginTime, endTime, page, size);
        page++;
        searchClient.searchDepathPage(indexName, symbol, market, type, beginTime, endTime, page, size);
        page++;
        searchClient.searchDepathPage(indexName, symbol, market, type, beginTime, endTime, page, size);
        page++;
        searchClient.searchDepathPage(indexName, symbol, market, type, beginTime, endTime, page, size);
        //分页查询--------------------end----------------------------
        searchClient.shutdown();
    }

    //检查es集群是否运行状态
    public boolean checkClusterRunning() throws IOException {
        return client.ping().value();
    }

    //检查索引名称是否存在
    public boolean checkIndexExist(final String indexName) throws IOException {
        if (client == null) return false;
        CountRequest.Builder countBuilder = new CountRequest.Builder();
        countBuilder.index(indexName);
        CountResponse countResponse = client.count(countBuilder.build());
        return countResponse.count() > 0;


        /*ExistsRequest.Builder builder = new ExistsRequest.Builder();
        builder.index(indexName)
                .type("depth");
        ExistsRequest request = builder.build();
        BooleanResponse response = client.exists(request);
        System.out.println(response.toString());
        return response.value();*/


        /*return client.exists(b ->
                b.
                        id("111111111111111111")
                        .index(indexName)
        ).value();*/
    }

    //创建索引对象
    public boolean createIndex(final String indexName) throws IOException {
        if (client == null) return false;
        IndexSettings.Builder builder = new IndexSettings.Builder();
        builder.numberOfReplicas("3");
        builder.numberOfShards("3");

        CreateIndexResponse createIndexResponse = client.indices()
                .create(c -> c
                .index(indexName)
                .settings(builder.build()));

        return createIndexResponse.acknowledged();
    }

    //异步方法使用类型
    public void asyncClient() {
        // Create the low-level client
        restClient = RestClient.builder(
                new HttpHost("192.168.1.58", 9200)).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // Asynchronous non-blocking client
        ElasticsearchAsyncClient asyncClient =
                new ElasticsearchAsyncClient(transport);

        asyncClient
                .exists(b -> b.index("products").id("foo"))
                .whenComplete((response, exception) -> {
                    if (exception != null) {
                        System.out.println("Failed to index" + exception);
                    } else {
                        System.out.println("Product exists");
                    }
                });
    }

    //查询符合添加的数据数量
    public long searchCount(final String indexName, final String typeName) throws IOException {
        //开始时间
        final String begineTime = "2022-06-21 15:04:56.188";
        //结束时间
        final String endTime = "2022-06-21 15:05:57.188";
        //market
        final String market = "SH";
        //type
        final String type = "KLine_30sec";
        //转换时间long
        final long begineCreated = DateUtils.fromStringFormat(begineTime);
        final long endCreated = DateUtils.fromStringFormat(endTime);
        //symbol
        final String symbol = "ag2204";

        CountResponse response = client.count(c -> c
                .index(indexName)
                .query(q -> q.bool( t -> t
                        .must(q1 -> q1.range(t1 -> t1.field("created").gte(JsonData.of(begineCreated)).lte(JsonData.of(endCreated)))) //时间区间
                        .must(ss -> ss.term(s1 -> s1.field("symbol").value(v -> v.stringValue(symbol))))   //symbol=symbol
                //.must(m -> m.term(m1 -> m1.field("market").value(v -> v.stringValue(market))))     //market=martket
                //.must(ty -> ty.term(t1 -> t1.field("type").value(v -> v.stringValue(type)))) //type=type
                ))
        );

        return response.count();
    }

    /**
     * 分页查询
     */
    public List<DepthModel> searchDepathPage(final String indexName,
                                             final String symbol,
                                             final String market,
                                             final String type,
                                             final String beginTime,
                                             final String endTime,
                                             final int page,
                                             final int size) throws IOException {

        //根据created排序
        SearchRequest searchRequest = makeSearchRequest(indexName, symbol, market, type, beginTime, endTime, page, size);
        return searchFromEs(searchRequest);
    }

    /**
     * 分页查询
     */
    public List<DepthModel> searchDepathPageWithSearchAfter(final String indexName,
                                             final String symbol,
                                             final String market,
                                             final String type,
                                             final String beginTime,
                                             final String endTime,
                                             final int page,
                                             final int size,
                                             final String searchAfter) throws IOException {

        //根据created排序
        SearchRequest searchRequest = makeSearchRequestWithSearchAfter(indexName, symbol, market, type, beginTime, endTime, lastId, size);
        System.out.println("search page with searchafter.");
        return searchFromEs(searchRequest);
    }

    private List<DepthModel> searchFromEs(final SearchRequest request) throws IOException {
        SearchResponse<DepthModel> response = client.search(request, DepthModel.class);
        System.out.println("search page with searchafter result count is " + response.hits().hits().size());

        if (response.hits().hits().size() > 0) {
            List<Hit<DepthModel>> hits = response.hits().hits();
            lastId = hits.get(hits.size() - 1).id();
            return hits.stream().map((e) -> e.source()).collect(Collectors.toList());
        }

        return new ArrayList<DepthModel>();
    }

    /**
     * 分页查询数据
     * @param indexName
     * @param symbol
     * @param market
     * @param type
     * @param beginTime
     * @param endTime
     * @param page
     * @param size
     * @return
     */
    private SearchRequest makeSearchRequest(final String indexName,
                                            final String symbol,
                                            final String market,
                                            final String type,
                                            final String beginTime,
                                            final String endTime,
                                            final int page,
                                            final int size) {
        //转换开始时间long类型
        final long begineCreated = DateUtils.fromStringFormat(beginTime);
        //结束时间到long类型
        final long endCreated = DateUtils.fromStringFormat(endTime);
        //request builder instance
        SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
        requestBuilder.index(indexName)
                .sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Asc)))
                .query(q -> q.bool( q1 -> q1
                        .must(m1 -> m1.range(c1 -> c1.field("created").gte(JsonData.of(begineCreated)).lte(JsonData.of(endCreated))))
                        .must(m2 -> m2.term(s1 -> s1.field("symbol").value(v -> v.stringValue(symbol))))
                        .must(m -> m.term(m1 -> m1.field("market").value(v -> v.stringValue(market))))
                        .must(s -> s.term(t1 -> t1.field("type").value(v -> v.stringValue(type))))
                        ))
                .from(page == 0 ? 0 : page * size)
                .size(size)
                //.searchAfter("")
        ;

        return requestBuilder.build();
    }

    //根据searchAfter查询分页数据
    private SearchRequest makeSearchRequestWithSearchAfter(final String indexName,
                                            final String symbol,
                                            final String market,
                                            final String type,
                                            final String beginTime,
                                            final String endTime,
                                            final String searchAfter,
                                            final int size) {
        //转换开始时间long类型
        final long begineCreated = DateUtils.fromStringFormat(beginTime);
        //结束时间到long类型
        final long endCreated = DateUtils.fromStringFormat(endTime);
        //request builder instance
        SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
        requestBuilder.index(indexName)
                .sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Asc)))
                .query(q -> q.bool( q1 -> q1
                        .must(m1 -> m1.range(c1 -> c1.field("created").gte(JsonData.of(begineCreated)).lte(JsonData.of(endCreated))))
                        .must(m2 -> m2.term(s1 -> s1.field("symbol").value(v -> v.stringValue(symbol))))
                        .must(m -> m.term(m1 -> m1.field("market").value(v -> v.stringValue(market))))
                        .must(s -> s.term(t1 -> t1.field("type").value(v -> v.stringValue(type))))
                ))
                .from(0)
                .size(size)
                .searchAfter(searchAfter)
        ;

        return requestBuilder.build();
    }

    //多个type查询 返回termquery条件
    private List<TermQuery> queryByTypes(final List<String> typeList) {
        TermQuery.Builder builder = new TermQuery.Builder();
        List<TermQuery> queryList = new ArrayList<TermQuery>(typeList.size());
        for (String type : typeList) {
            queryList.add(builder.field("type").value(s -> s.stringValue(type)).build());
        }
        return queryList;
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
                        .index("finesys_depth01")
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
        Depth depth = depthBuilder.defaultDepth("ag2203").build();

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
        typeMap.put(0, "Depth");
        typeMap.put(1, "Tick");
        typeMap.put(2, "KLine_30sec");
        typeMap.put(3, "KLine_5min");
        typeMap.put(4, "KLine_1hour");
        typeMap.put(5, "KLine_1day");
        typeMap.put(6, "KLine_1week");
        typeMap.put(7, "KLine_1mon");

        long successCount = 0;
        final int totalCount = count <= 0 ? 1 : count;
        for (int i = 0; i < totalCount; i++) {
            depthModel.setSymbol(symbolMap.get(i % 6));
            depthModel.setMarket(marketMap.get(i % 3));
            depthModel.setType(typeMap.get(i % 8));
            depthModel.setCreated(DateUtils.getNow());
            depthModel.setTime(DateUtils.getDepthTime(depthModel.getCreated()));
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

    //按照symbol查询第一条
    public void searchBySymbolFirst(final String symbol) throws IOException {
        //开始时间
        final String begineTime = "2022-06-21 15:04:56.188";
        //结束时间
        final String endTime = "2022-06-21 15:05:57.188";
        //market
        final String market = "SH";
        //type
        final String type = "KLine_30sec";
        //转换时间long
        final long begineCreated = DateUtils.fromStringFormat(begineTime);
        final long endCreated = DateUtils.fromStringFormat(endTime);

        //根据created排序
        SearchResponse<DepthModel> response = client.search(s -> s.index("finesys_depth01")
                        .sort(s1 -> s1.field( f -> f.field("created").order(SortOrder.Asc)))
                        //.scroll(t -> t.offset(0))
                        .from(0)   //offset
                        .size(10)   //size
                        .query(q -> q.bool( t -> t
                            .must(q1 -> q1.range(t1 -> t1.field("created").gte(JsonData.of(begineCreated)).lte(JsonData.of(endCreated)))) //时间区间
                            .must(ss -> ss.term(s1 -> s1.field("symbol").value(v -> v.stringValue(symbol))))   //symbol=symbol
                            //.must(m -> m.term(m1 -> m1.field("market").value(v -> v.stringValue(market))))     //market=martket
                            //.must(ty -> ty.term(t1 -> t1.field("type").value(v -> v.stringValue(type)))) //type=type
                        )), DepthModel.class);

        if (response.hits().hits().size() > 0) {
            List<Hit<DepthModel>> hits = response.hits().hits();

            for (Hit<DepthModel> hit : hits) {
                System.out.println("hit.id()======" + hit.id());
                System.out.println(hit.source());//拿到数据
                //DepthModel depthModel = hit.source();
            }
        }

        System.out.println("search result count is " + response.hits().hits().size());
    }

    //按照多symbol查询第一条
    public void searchByMutilSymbolFirst(final String [] symbol) {
    }

    public void searchBySymbolLast(final String symbol) {
    }

    public RestClient getRestClient() {
        return restClient;
    }

    public ElasticsearchClient getClient() {
        return client;
    }
}
